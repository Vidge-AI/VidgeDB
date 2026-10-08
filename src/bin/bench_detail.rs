//! Fine-grained instrumentation of the costliest benchmark scenarios — Phase 9
//! performance audit (task contract: MEASURE FIRST, no blind optimizing).
//!
//! Scenarios instrumented (spec §38 workloads, same shapes as bench.rs):
//! - `relation_insertion` (§38.1: 3000 relations in 10 tx of 300) — the pire.
//! - `ts_aggregate` (§38.2: 50 full-series aggregates over 100k pts).
//! - `recovery` (§38.5 cold reopen of the synthetic twin fixture).
//!
//! Stage timers (std::time::Instant, release build): every stage prints
//! total ms + % of total + µs/call; per-op percentiles via manual sort.
//! A CRC32 micro-bench (bitwise vs table-based) quantifies candidate (a).

use std::time::Instant;

use vidgedb::engine::Engine;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;

// ---------------------------------------------------------------------------
// crc micro-bench (candidate (a): bitwise vs table-based)
// ---------------------------------------------------------------------------

/// Software CRC-32 (IEEE 802.3), table-less bitwise — the current Phase 1
/// implementation (wal.rs).
fn crc32_bitwise(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Software CRC-32 (IEEE 802.3), table-based (256 u32 entries baked at
/// compile time, deterministic across platforms, zero allocation).
fn crc32_table(data: &[u8]) -> u32 {
    const fn make_table() -> [u32; 256] {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    }
    static TABLE: [u32; 256] = make_table();
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = (crc >> 8) ^ TABLE[((crc ^ (b as u32)) & 0xFF) as usize];
    }
    !crc
}

fn bench_crc() {
    let page: Vec<u8> = (0..4096usize).map(|i| (i * 31 + 7) as u8).collect();
    let rounds = 3_000usize; // 3000 page frames (the relation_insertion workload)
    for chk in [1u8, 42, 255] {
        let a = crc32_bitwise(&page[..32 + chk as usize]);
        let b = crc32_table(&page[..32 + chk as usize]);
        assert_eq!(a, b, "crc32 bitwise != crc32 table");
    }
    for _ in 0..3 {
        let t0 = Instant::now();
        let mut acc: u64 = 0;
        for r in 0..rounds {
            let mut data = page.clone();
            data[0] = r as u8;
            acc += crc32_bitwise(&data) as u64;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "crc32 BITWISE: 3000 x 4KiB = {:.3} ms ({:.1} µs/frame) [acc {:x}]",
            ms,
            ms * 1000.0 / rounds as f64,
            acc % 0xFFFF
        );
    }
    for _ in 0..3 {
        let t0 = Instant::now();
        let mut acc: u64 = 0;
        for r in 0..rounds {
            let mut data = page.clone();
            data[0] = r as u8;
            acc += crc32_table(&data) as u64;
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "crc32 TABLE:   3000 x 4KiB = {:.3} ms ({:.1} µs/frame) [acc {:x}]",
            ms,
            ms * 1000.0 / rounds as f64,
            acc % 0xFFFF
        );
    }
}

// ---------------------------------------------------------------------------
// Stage accounting
// ---------------------------------------------------------------------------

struct Stage {
    name: &'static str,
    ns: u64,
    calls: usize,
    samples: Vec<f64>,
}

struct Stages {
    st: Vec<Stage>,
}

impl Stages {
    fn new(names: &[&'static str]) -> Stages {
        Stages {
            st: names
                .iter()
                .map(|n| Stage {
                    name: n,
                    ns: 0,
                    calls: 0,
                    samples: Vec::new(),
                })
                .collect(),
        }
    }
    #[inline]
    fn time<T>(&mut self, name: &'static str, sample: bool, f: impl FnOnce() -> T) -> T {
        let t0 = Instant::now();
        let out = f();
        let ns = t0.elapsed().as_nanos() as u64;
        let s = self.st.iter_mut().find(|s| s.name == name).expect("stage");
        s.ns += ns;
        s.calls += 1;
        if sample {
            s.samples.push(ns as f64 / 1e6);
        }
        out
    }
    fn total_ms(&self) -> f64 {
        self.st.iter().map(|s| s.ns as f64 / 1e6).sum()
    }
    fn print(&self, title: &str) {
        let total = self.total_ms();
        println!("\n### {title} — total {:.2} ms", total);
        println!("| stage | total ms | % | calls | µs/call |");
        println!("|---|---:|---:|---:|---:|");
        for s in &self.st {
            let ms = s.ns as f64 / 1e6;
            let pct = if total > 0.0 { 100.0 * ms / total } else { 0.0 };
            let us_per = if s.calls > 0 {
                format!("{:.1}", s.ns as f64 / s.calls as f64 / 1e3)
            } else {
                "(n/a)".into()
            };
            println!(
                "| {} | {:.3} | {:.1} | {} | {} |",
                s.name, ms, pct, s.calls, us_per
            );
        }
    }
}

fn percentile(mut v: Vec<f64>, p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = ((p / 100.0) * v.len() as f64).ceil() as usize;
    v[rank.clamp(1, v.len()) - 1]
}

fn print_pct(title: &str, mut v: Vec<f64>) {
    if v.is_empty() {
        return;
    }
    println!(
        "{}: n={} p50={:.3}ms p95={:.3}ms p99={:.3}ms sum={:.1}ms mean={:.3}ms",
        title,
        v.len(),
        percentile(v.clone(), 50.0),
        percentile(v.clone(), 95.0),
        percentile(v.clone(), 99.0),
        v.iter().sum::<f64>(),
        v.iter().sum::<f64>() / v.len() as f64
    );
    v.clear();
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn tmp_db(tag: &str) -> String {
    let mut p = std::env::temp_dir();
    p.push(format!("vidgedb_detail_{}_{}.vdg", tag, std::process::id()));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

fn file_size(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn seed_entities(eng: &mut Engine, gs: &mut GraphStore) {
    let mut tx = eng.begin().unwrap();
    for i in 0..1000usize {
        gs.add_entity(eng, &mut tx, "Node", &format!("Node_{:05}", i), &[])
            .unwrap();
    }
    eng.commit(tx).unwrap();
    gs.persist(eng).unwrap();
}

fn rel_args(i: usize) -> (u32, u32, &'static str) {
    let src = (i % 1000) as u32;
    let dst = ((i * 7 + 13) % 1000) as u32;
    let topo = ["electrical:feeds", "mechanical:drives", "network:profinet"][i % 3];
    (src, dst, topo)
}

// ---------------------------------------------------------------------------
// Scenario 1 — relation_insertion: coarse + zoom + commit-step V2
// ---------------------------------------------------------------------------

const REL_N: usize = 3000;
const REL_STEP: usize = 300;

/// V1 coarse: add_relation whole / persist / Engine::commit whole.
fn bench_rel_insert_v1() {
    let path = tmp_db("rel");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    seed_entities(&mut eng, &mut gs);
    let mut st = Stages::new(&[
        "add_relation (in-tx, whole)",
        "persist (meta write_in_tx)",
        "Engine::commit (whole)",
    ]);
    let mut tx_durs: Vec<f64> = Vec::new();
    for chunk_start in (0..REL_N).step_by(REL_STEP) {
        let mut tx = eng.begin().unwrap();
        let end = (chunk_start + REL_STEP).min(REL_N);
        let t0 = Instant::now();
        for i in chunk_start..end {
            let (src, dst, topo) = rel_args(i);
            st.time("add_relation (in-tx, whole)", true, || {
                gs.add_relation(&mut eng, &mut tx, src, dst, topo, 0, -1, 0)
                    .unwrap()
            });
        }
        st.time("persist (meta write_in_tx)", true, || {
            gs.persist(&mut eng).unwrap()
        });
        tx_durs.push(t0.elapsed().as_secs_f64() * 1000.0);
        st.time("Engine::commit (whole)", true, || eng.commit(tx).unwrap());
    }
    st.print("relation_insertion V1 (add_relation whole | commit whole)");
    print_pct("tx durations (300 relations each)", tx_durs);
    drop(gs);
    drop(eng);
    cleanup(&path);
}

/// Zoom: inside one tx, measure the FIRST add_relation (alloc path: fresh
/// slab page chaining) vs MEAN of the other 299 (chain-walk + intern + stage).
fn bench_rel_zoom() {
    let path = tmp_db("relz");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    seed_entities(&mut eng, &mut gs);
    let mut first_us: Vec<f64> = Vec::new();
    let mut rest_mean_us: Vec<f64> = Vec::new();
    for tx_i in 0..10 {
        let mut tx = eng.begin().unwrap();
        let mut total_rest_ns = 0u64;
        for i in 0..REL_STEP {
            let global = tx_i * REL_STEP + i;
            let (src, dst, topo) = rel_args(global);
            let t0 = Instant::now();
            gs.add_relation(&mut eng, &mut tx, src, dst, topo, 0, -1, 0)
                .unwrap();
            let ns = t0.elapsed().as_nanos() as u64;
            if i == 0 {
                first_us.push(ns as f64 / 1e3);
            } else {
                total_rest_ns += ns;
            }
        }
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        rest_mean_us.push(total_rest_ns as f64 / (REL_STEP - 1) as f64 / 1e3);
    }
    println!("\n### relation zoom — first call (page alloc+chain) vs steady calls");
    print_pct("first add_relation of tx (µs)", first_us);
    print_pct("mean of steady add_relation (µs)", rest_mean_us);
    drop(gs);
    drop(eng);
    cleanup(&path);
}

/// V2: commit protocol split per step using the Engine audit hooks —
/// SAME operations in the SAME order as Engine::commit.
fn bench_rel_insert_v2() {
    let path = tmp_db("rel2");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    seed_entities(&mut eng, &mut gs);
    let mut st = Stages::new(&[
        "in-tx add_relation",
        "in-tx persist",
        "commit: fold + validation (compute_commit_sb)",
        "commit: 1) WAL append SetPage frames",
        "commit: 2) WAL append sb frame",
        "commit: 3) WAL append commit marker",
        "commit: 4) WAL sync_all (fsync)",
        "commit: 5a) pager apply (copy pages)",
        "commit: 5b) pager flush write_all + fsync",
        "commit: 5c) pending retain",
        "commit: 6) stat wal_len",
    ]);
    let mut tx_durs: Vec<f64> = Vec::new();
    for chunk_start in (0..REL_N).step_by(REL_STEP) {
        let t0 = Instant::now();
        let mut tx = eng.begin().unwrap();
        let end = (chunk_start + REL_STEP).min(REL_N);
        for i in chunk_start..end {
            let (src, dst, topo) = rel_args(i);
            st.time("in-tx add_relation", true, || {
                gs.add_relation(&mut eng, &mut tx, src, dst, topo, 0, -1, 0)
                    .unwrap()
            });
        }
        st.time("in-tx persist", true, || gs.persist(&mut eng).unwrap());
        let (writes_raw, taken_raw, freed_raw) = tx.parts();
        let writes: Vec<(u32, [u8; 4096])> = writes_raw.to_vec();
        let taken: Vec<u32> = taken_raw.to_vec();
        let freed: Vec<u32> = freed_raw.to_vec();
        let final_sb = st.time(
            "commit: fold + validation (compute_commit_sb)",
            false,
            || eng.expose_commit_fold(&writes, &freed, &taken),
        );
        for (id, data) in &writes {
            st.time("commit: 1) WAL append SetPage frames", false, || {
                eng.expose_wal_setpage(*id, data).unwrap()
            });
        }
        st.time("commit: 2) WAL append sb frame", false, || {
            eng.expose_wal_sb(final_sb[0], final_sb[1]).unwrap()
        });
        st.time("commit: 3) WAL append commit marker", false, || {
            eng.expose_wal_commit().unwrap()
        });
        st.time("commit: 4) WAL sync_all (fsync)", true, || {
            eng.expose_wal_sync().unwrap()
        });
        st.time("commit: 5a) pager apply (copy pages)", false, || {
            eng.expose_pager_apply(&writes)
        });
        st.time("commit: 5b) pager flush write_all + fsync", false, || {
            eng.expose_pager_flush().unwrap()
        });
        st.time("commit: 5c) pending retain", false, || {
            eng.expose_clear_pending(&taken, &freed)
        });
        st.time("commit: 6) stat wal_len", false, || {
            let _ = eng.expose_stat_wal_len();
        });
        // The Tx was committed step-by-step above; dropping it is a no-op.
        drop(tx);
        tx_durs.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    st.print("relation_insertion V2 (commit split per step)");
    print_pct("tx durations (300 relations each)", tx_durs);
    drop(gs);
    drop(eng);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Scenario 2 — ts_aggregate: query-phases split
// ---------------------------------------------------------------------------

fn bench_ts_aggregate_detail(points: usize, aggs: usize) {
    use vidgedb::timeseries::BATCH;
    let path = tmp_db("agg");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut tx = eng.begin().unwrap();
    let sid = ts.create_series(&mut eng, &mut tx, "agg.current").unwrap();
    for i in 0..points as i64 {
        ts.append(
            &mut eng,
            &mut tx,
            sid,
            1_700_000_000 + i * 10,
            (i % 1000) as f64 * 0.01,
        )
        .unwrap();
    }
    ts.flush_series(&mut eng, &mut tx, sid).unwrap();
    eng.commit(tx).unwrap();
    ts.persist(&mut eng).unwrap();
    drop(ts);
    drop(gs);
    drop(eng);

    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let n_chunks = ts.committed_chunks(sid);
    println!(
        "\n### ts_aggregate detail — {} points, {} chunks ({} pts/chunk)",
        points, n_chunks, BATCH
    );
    let mut st = Stages::new(&[
        "ts.query whole (chunk scan+stream_read+decode+sort)",
        "fold sum/min/max (on decoded pts)",
    ]);
    let mut durs: Vec<f64> = Vec::new();
    let mut verified = 0usize;
    for _ in 0..aggs {
        let t0 = Instant::now();
        let pts = st.time(
            "ts.query whole (chunk scan+stream_read+decode+sort)",
            true,
            || ts.query(&mut eng, sid, 0, i64::MAX / 2).unwrap(),
        );
        let mut sum = 0.0f64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        st.time("fold sum/min/max (on decoded pts)", true, || {
            for &(_, v) in &pts {
                sum += v;
                if v < min {
                    min = v;
                }
                if v > max {
                    max = v;
                }
            }
        });
        durs.push(t0.elapsed().as_secs_f64() * 1000.0);
        let want_sum: f64 = (0..points as i64).map(|i| (i % 1000) as f64 * 0.01).sum();
        if (sum - want_sum).abs() < 1e-6 * want_sum.abs().max(1.0) {
            verified += 1;
        }
    }
    println!(
        "aggregates verified {}/{} over {} pts each",
        verified, aggs, points
    );
    st.print("ts_aggregate (50 full aggregates)");
    print_pct("aggregate full duration", durs);
    drop(ts);
    drop(gs);
    drop(eng);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Scenario 3 — recovery: cold reopen x5 with correctness anchors
// ---------------------------------------------------------------------------

fn bench_recovery_detail(machines: usize, signals: usize, pps: usize) {
    use vidgedb::benchgen::{self, BenchConfig};
    let path = tmp_db("twin");
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let cfg = BenchConfig {
            machines,
            signals,
            points_per_signal: pps,
            seed: benchgen::DEFAULT_SEED,
        };
        benchgen::generate(&mut eng, &mut gs, &mut ts, &cfg).unwrap();
    }
    println!(
        "\n### recovery detail — fixture: {} entities, {} series x {} pts, .vdg={} B, WAL={} B",
        machines * 4,
        machines * signals,
        pps,
        file_size(&path),
        file_size(&format!("{}-wal", path))
    );
    let mut opens: Vec<f64> = Vec::new();
    for _ in 0..5 {
        let t0 = Instant::now();
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        assert_eq!(gs.entity_count() as usize, machines * 4);
        let total_points: u64 = ts.series.iter().map(|s| s.total_points).sum();
        assert_eq!(total_points as usize, machines * signals * pps);
        opens.push(t0.elapsed().as_secs_f64() * 1000.0);
        drop(ts);
        drop(gs);
        drop(eng);
    }
    print_pct("recovery cold reopen (Engine+stores, verified)", opens);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let points: usize = args
        .iter()
        .position(|a| a == "--points")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    println!("# bench_detail — Phase 9 performance audit (release build)");
    bench_crc();
    bench_rel_insert_v1();
    bench_rel_zoom();
    bench_rel_insert_v2();
    bench_ts_aggregate_detail(points, 50);
    bench_recovery_detail(50, 1, points / 50);
    println!("\nbench_detail: done");
}

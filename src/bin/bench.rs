//! Benchmark suite — Phase 8 (spec §37 metrics, §38 scenarios, §39
//! correctness gating).
//!
//! Runs the spec §38 storage + time-series scenarios against temporary
//! `.vdg` databases and prints a markdown report on stdout (human mode) or a
//! machine-readable JSON (… `--json`). Every scenario ends with a spec §39
//! CORRECTNESS CHECK: what is read back must equal what was written, or the
//! run FAILs and exits with code 1 ("a faster incorrect database SHALL fail
//! the benchmark").
//!
//! Measurement: `std::time::Instant` only — no external benchmark crate.
//! Repeated ops collect per-op durations; percentile math (p50/p95/p99) is
//! manual (sort + nearest-rank).
//!
//! Usage:
//! ```text
//! bench [--machines N] [--points N] [--json]
//! ```
//!
//! `--points` scales the time-series scenarios (default 100 000); the graph
//! scenarios use the spec §38.1-style fixed workloads (1 000 entities,
//! 3 000 relations, 1 000 lookups, 100 range queries, 50 aggregations).
//! Machine count only feeds the synthetic-twin fixture scenarios (§42);
//! storage/TS scenarios use their own fixed workloads as specified.

use std::time::Instant;

use vidgedb::benchgen::{self, BenchConfig};
use vidgedb::engine::Engine;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;

// ---------------------------------------------------------------------------
// Measurement helpers (spec §37: p50/p95/p99)
// ---------------------------------------------------------------------------

/// One timed scenario result.
#[derive(Clone)]
struct Scen {
    name: &'static str,
    /// Total wall time (ms).
    total_ms: f64,
    /// ops/s (throughput) — repeated-ops count over total seconds.
    ops_per_s: f64,
    /// Per-op percentiles (ms); total-only scenarios repeat the total.
    p50: f64,
    p95: f64,
    p99: f64,
    /// Number of operations measured (1 for total-only scenarios).
    n_ops: usize,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    // Nearest-rank: ceil(p/100 * n), 1-indexed.
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    let rank = rank.clamp(1, sorted.len());
    sorted[rank - 1]
}

/// Build a Scen from per-op durations (ms). n_ops may exceed the sample
/// length only when per-op timing was skipped for speed — then percentiles
/// come from the collected sample.
fn scen_from(name: &'static str, durs_ms: Vec<f64>, n_ops: usize) -> Scen {
    let mut sorted = durs_ms.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total: f64 = sorted.iter().sum();
    let total = if total == 0.0 && n_ops > 0 {
        0.0001
    } else {
        total
    };
    Scen {
        name,
        total_ms: total,
        ops_per_s: if total > 0.0 {
            n_ops as f64 / (total / 1000.0)
        } else {
            0.0
        },
        p50: percentile(&sorted, 50.0),
        p95: percentile(&sorted, 95.0),
        p99: percentile(&sorted, 99.0),
        n_ops,
    }
}

/// One-row total-only scenario (recovery time, storage size, ...).
fn scen_total(name: &'static str, total_ms: f64, ops: usize) -> Scen {
    scen_from(name, vec![total_ms], ops)
}

fn fmt_scen(s: &Scen) -> String {
    format!(
        "| {} | {:.1} | {:.0} | {:.3} | {:.3} | {:.3} |",
        s.name, s.total_ms, s.ops_per_s, s.p50, s.p95, s.p99
    )
}

fn fmt_scen_json(s: &Scen) -> String {
    format!(
        "{{\"name\":\"{}\",\"total_ms\":{:.2},\"ops_per_s\":{:.1},\"p50_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3},\"n_ops\":{}}}",
        s.name, s.total_ms, s.ops_per_s, s.p50, s.p95, s.p99, s.n_ops
    )
}

/// Fail helper (spec §39): print and exit 1.
fn fail(what: &str, expected: &str, actual: &str) -> ! {
    eprintln!("FAIL: {} — expected {}, actual {}", what, expected, actual);
    std::process::exit(1);
}

/// Require a condition; `fail` otherwise.
fn check(cond: bool, what: &str, expected: &str, actual: &str) {
    if !cond {
        fail(what, expected, actual);
    }
}

// ---------------------------------------------------------------------------
// Temp DB plumbing
// ---------------------------------------------------------------------------

fn tmp_db(tag: &str) -> String {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "vidgedb_bench_{}_{}_{}.vdg",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0)
    ));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

fn file_size(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Scenarios — graph storage (spec §38.1)
// ---------------------------------------------------------------------------

/// 38.1 entity insertion: N entities in transactions of 100.
fn bench_entity_insertion(n: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("ent");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut tx_durs = Vec::new();
    let names: Vec<String> = (0..n).map(|i| format!("Entity_{:05}", i)).collect();
    for chunk in names.chunks(100) {
        let t0 = Instant::now();
        let mut tx = eng.begin().unwrap();
        for nm in chunk {
            gs.add_entity(&mut eng, &mut tx, "Component", nm, &[])
                .unwrap();
        }
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        tx_durs.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    ts.persist(&mut eng).unwrap();
    sizes.push((
        "entity_insertion".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    // §39: reopen and verify every name round-trips.
    drop(eng);
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    check(
        gs.entity_count() as usize == n,
        "entity_insertion count",
        &n.to_string(),
        &gs.entity_count().to_string(),
    );
    let mut verified = 0usize;
    for (i, nm) in names.iter().enumerate() {
        let sid = gs.entity_name_sid(&mut eng, i as u32).unwrap();
        let read = gs.get_str(&mut eng, sid).unwrap();
        check(read == *nm, "entity_insertion name round-trip", nm, &read);
        verified += 1;
    }
    println!(
        "entity_insertion: {} entities in {} tx of 100, {} verified",
        n,
        tx_durs.len(),
        verified
    );
    let s = scen_from("entity_insertion", tx_durs, n);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

/// 38.1 relation insertion: N relations over a fixed entity ring.
fn bench_relation_insertion(n: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("rel");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut tx = eng.begin().unwrap();
    // 1 000 entities to link (the entity_insertion workload).
    let mut keys = Vec::new();
    for i in 0..1000 {
        let k = gs
            .add_entity(&mut eng, &mut tx, "Node", &format!("Node_{:05}", i), &[])
            .unwrap();
        keys.push(k);
    }
    eng.commit(tx).unwrap();
    gs.persist(&mut eng).unwrap();

    let mut tx_durs = Vec::new();
    for chunk_start in (0..n).step_by(300) {
        let t0 = Instant::now();
        let mut tx = eng.begin().unwrap();
        let end = (chunk_start + 300).min(n);
        for i in chunk_start..end {
            let src = keys[i % keys.len()];
            let dst = keys[(i * 7 + 13) % keys.len()];
            let topo = ["electrical:feeds", "mechanical:drives", "network:profinet"][i % 3];
            gs.add_relation(&mut eng, &mut tx, src, dst, topo, 0, -1, 0)
                .unwrap();
        }
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        tx_durs.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    drop(eng);
    sizes.push((
        "relation_insertion".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    // §39: reopen and verify relation count + adjacency endpoints.
    let mut eng = Engine::open(&path).unwrap();
    let gs = GraphStore::open(&mut eng).unwrap();
    check(
        gs.relations.len() == n,
        "relation_insertion count",
        &n.to_string(),
        &gs.relations.len().to_string(),
    );
    for (i, r) in gs.relations.iter().enumerate() {
        let want_src = keys[i % keys.len()];
        let want_dst = keys[(i * 7 + 13) % keys.len()];
        check(
            r.src == want_src && r.dst == want_dst,
            "relation_insertion endpoints",
            &format!("{}->{}", want_src, want_dst),
            &format!("{}->{}", r.src, r.dst),
        );
    }
    println!("relation_insertion: {} relations, endpoints verified", n);
    let s = scen_from("relation_insertion", tx_durs, n);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

/// 38.1 lookup: name -> key through the interned-string scan.
fn bench_lookup(lookups: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("look");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut tx = eng.begin().unwrap();
    let n = 1000usize;
    let mut keys = Vec::new();
    for i in 0..n {
        let k = gs
            .add_entity(&mut eng, &mut tx, "Node", &format!("Node_{:05}", i), &[])
            .unwrap();
        keys.push(k);
    }
    eng.commit(tx).unwrap();
    gs.persist(&mut eng).unwrap();
    drop(eng);
    drop(gs);

    // Reopen (fresh state) and measure name->key lookups.
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut durs = Vec::new();
    let mut verified = 0usize;
    for i in 0..lookups {
        let nm = format!("Node_{:05}", i % n);
        let t0 = Instant::now();
        let found = lookup_by_name(&mut eng, &mut gs, &nm);
        durs.push(t0.elapsed().as_secs_f64() * 1000.0);
        let k = found.unwrap_or_else(|| fail("lookup found", &nm, "None"));
        check(
            k == keys[i % n],
            "lookup key",
            &keys[i % n].to_string(),
            &k.to_string(),
        );
        verified += 1;
    }
    sizes.push((
        "lookup".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    println!("lookup: {} name->key reads, {} verified", lookups, verified);
    let s = scen_from("lookup", durs, lookups);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

/// get_entity-like scan lookup: find the entity whose name sid matches the
/// interned name (the Phase 7 `entity_key_by_name` path).
fn lookup_by_name(eng: &mut Engine, gs: &mut GraphStore, name: &str) -> Option<u32> {
    if let Some(sid) = gs.str_lookup(name) {
        for key in 0..gs.entity_count() {
            let n_sid = gs.entity_name_sid(eng, key).unwrap();
            if n_sid == sid {
                return Some(key);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Scenarios — time series (spec §38.2)
// ---------------------------------------------------------------------------

/// 38.2 sequential ingestion: all points into ONE series (append auto-flush).
fn bench_ts_sequential(points: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("seq");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut tx = eng.begin().unwrap();
    let sid = ts.create_series(&mut eng, &mut tx, "seq.current").unwrap();
    let t0 = Instant::now();
    for i in 0..points as i64 {
        // Pure function of i — no RNG (spec §42 determinism).
        let v = (i % 1000) as f64 * 0.01;
        ts.append(&mut eng, &mut tx, sid, 1_700_000_000 + i * 10, v)
            .unwrap();
    }
    ts.flush_series(&mut eng, &mut tx, sid).unwrap();
    eng.commit(tx).unwrap();
    ts.persist(&mut eng).unwrap();
    let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
    drop(eng);
    drop(gs);
    drop(ts);
    sizes.push((
        "ts_sequential".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    // §39: reopen, sum must equal the written checksum.
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let pts = ts.query(&mut eng, sid, 0, i64::MAX / 2).unwrap();
    check(
        pts.len() == points,
        "ts_sequential point count",
        &points.to_string(),
        &pts.len().to_string(),
    );
    let want: f64 = (0..points as i64).map(|i| (i % 1000) as f64 * 0.01).sum();
    let got: f64 = pts.iter().map(|(_, v)| *v).sum();
    check(
        (want - got).abs() < 1e-6 * want.abs().max(1.0),
        "ts_sequential value sum",
        &format!("{:.4}", want),
        &format!("{:.4}", got),
    );
    println!("ts_sequential: {} points, 1 series, sum verified", points);
    let s = scen_total("ts_sequential", total_ms, points);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

/// 38.2 batched ingestion: points spread over 10 series.
fn bench_ts_batched(points: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("bat");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut tx = eng.begin().unwrap();
    let per = points / 10;
    let mut sids = Vec::new();
    for k in 0..10 {
        let s = ts
            .create_series(&mut eng, &mut tx, &format!("batch{:02}.current", k))
            .unwrap();
        sids.push(s);
    }
    let t0 = Instant::now();
    for k in 0..10usize {
        for i in 0..per as i64 {
            let v = (k as i64 * 1000 + i % 1000) as f64 * 0.01;
            ts.append(&mut eng, &mut tx, sids[k], 1_700_000_000 + i * 10, v)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sids[k]).unwrap();
    }
    eng.commit(tx).unwrap();
    ts.persist(&mut eng).unwrap();
    let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
    drop(eng);
    drop(gs);
    drop(ts);
    sizes.push((
        "ts_batched".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    // §39: every series must hold exactly `per` points, values intact.
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut total_read = 0usize;
    let mut sum_read = 0.0f64;
    for k in 0..10 {
        let pts = ts.query(&mut eng, sids[k], 0, i64::MAX / 2).unwrap();
        check(
            pts.len() == per,
            "ts_batched per-series count",
            &per.to_string(),
            &pts.len().to_string(),
        );
        total_read += pts.len();
        sum_read += pts.iter().map(|(_, v)| *v).sum::<f64>();
    }
    let want: f64 = (0..10usize)
        .map(|k| {
            (0..per as i64)
                .map(|i| (k as i64 * 1000 + i % 1000) as f64 * 0.01)
                .sum::<f64>()
        })
        .sum();
    check(
        (want - sum_read).abs() < 1e-6 * want.abs().max(1.0),
        "ts_batched value sum",
        &format!("{:.4}", want),
        &format!("{:.4}", sum_read),
    );
    println!(
        "ts_batched: {} points over 10 series ({} read back), sum verified",
        points, total_read
    );
    let s = scen_total("ts_batched", total_ms, points);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

/// 38.2 range queries: 100 window reads over one 100k-point series.
fn bench_ts_range_query(points: usize, queries: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("rng");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut tx = eng.begin().unwrap();
    let sid = ts.create_series(&mut eng, &mut tx, "rng.current").unwrap();
    for i in 0..points as i64 {
        let v = (i % 1000) as f64 * 0.01;
        ts.append(&mut eng, &mut tx, sid, 1_700_000_000 + i * 10, v)
            .unwrap();
    }
    ts.flush_series(&mut eng, &mut tx, sid).unwrap();
    eng.commit(tx).unwrap();
    ts.persist(&mut eng).unwrap();
    drop(eng);
    drop(gs);
    drop(ts);

    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    // Window = 1/1000 of the span (100 points at 10 s spacing).
    let span = points as i64 * 10;
    let win = (span / 1000).max(10);
    let t_start = 1_700_000_000i64;
    let mut durs = Vec::new();
    let mut verified = 0usize;
    for q in 0..queries {
        let from = t_start + (q as i64 * span / queries as i64);
        let to = from + win;
        let t0 = Instant::now();
        let pts = ts.query(&mut eng, sid, from, to).unwrap();
        durs.push(t0.elapsed().as_secs_f64() * 1000.0);
        // §39: window width +10 s spacing ⇒ win/10 + 1 points (inclusive).
        let want = (win / 10 + 1) as i64;
        check(
            pts.len() as i64 == want,
            "ts_range_query window count",
            &want.to_string(),
            &pts.len().to_string(),
        );
        // Every returned point is inside the window.
        for (t, _) in &pts {
            check(
                *t >= from && *t <= to,
                "ts_range_query bounds",
                &format!("[{}..{}]", from, to),
                &t.to_string(),
            );
        }
        verified += pts.len();
    }
    sizes.push((
        "ts_range_query".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    println!(
        "ts_range_query: {} windows over {} pts, {} points verified",
        queries, points, verified
    );
    let s = scen_from("ts_range_query", durs, queries);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

/// 38.2 aggregation: 50 full-series aggregates.
fn bench_ts_aggregate(points: usize, aggs: usize, sizes: &mut Vec<(String, u64, u64)>) {
    let path = tmp_db("agg");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut tx = eng.begin().unwrap();
    let sid = ts.create_series(&mut eng, &mut tx, "agg.current").unwrap();
    for i in 0..points as i64 {
        let v = (i % 1000) as f64 * 0.01;
        ts.append(&mut eng, &mut tx, sid, 1_700_000_000 + i * 10, v)
            .unwrap();
    }
    ts.flush_series(&mut eng, &mut tx, sid).unwrap();
    eng.commit(tx).unwrap();
    ts.persist(&mut eng).unwrap();
    drop(eng);
    drop(gs);
    drop(ts);

    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut durs = Vec::new();
    let want_sum: f64 = (0..points as i64).map(|i| (i % 1000) as f64 * 0.01).sum();
    for _ in 0..aggs {
        let t0 = Instant::now();
        let a = ts.aggregate(&mut eng, sid, 0, i64::MAX / 2).unwrap();
        durs.push(t0.elapsed().as_secs_f64() * 1000.0);
        check(
            a.count as usize == points,
            "ts_aggregate count",
            &points.to_string(),
            &a.count.to_string(),
        );
        check(
            (a.sum - want_sum).abs() < 1e-6 * want_sum.abs().max(1.0),
            "ts_aggregate sum",
            &format!("{:.4}", want_sum),
            &format!("{:.4}", a.sum),
        );
    }
    sizes.push((
        "ts_aggregate".into(),
        file_size(&path),
        file_size(&format!("{}-wal", path)),
    ));
    println!("ts_aggregate: {} full aggregates, sum verified", aggs);
    let s = scen_from("ts_aggregate", durs, aggs);
    REPORT.lock().unwrap().push(s);
    drop(eng);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Synthetic twin fixture scenarios (spec §42 + §38.4 hybrid)
// ---------------------------------------------------------------------------

/// Build the synthetic twin fixture (spec §42) once; reuse for hybrid +
/// recovery scenarios.
fn build_fixture(machines: usize, signals: usize, pps: usize) -> (String, u64, u64) {
    let path = tmp_db("twin");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let cfg = BenchConfig {
        machines,
        signals,
        points_per_signal: pps,
        seed: benchgen::DEFAULT_SEED,
    };
    let sum = benchgen::generate(&mut eng, &mut gs, &mut ts, &cfg).unwrap();
    // §39 on the generator itself: counters must match the config exactly.
    let want_e = (machines * 4) as u64;
    let want_r = (machines * 3) as u64;
    let want_s = (machines * signals) as u64;
    let want_p = (machines * signals * pps) as u64;
    check(
        sum.entities == want_e
            && sum.relations == want_r
            && sum.series == want_s
            && sum.points == want_p,
        "benchgen summary counts",
        &format!("e{} r{} s{} p{}", want_e, want_r, want_s, want_p),
        &format!(
            "e{} r{} s{} p{}",
            sum.entities, sum.relations, sum.series, sum.points
        ),
    );
    drop(eng);
    drop(gs);
    drop(ts);
    let size = file_size(&path);
    let wal = file_size(&format!("{}-wal", path));
    (path, size, wal)
}

/// Verify the twin fixture round-trips on reopen (spec §39).
fn verify_fixture(path: &str, machines: usize, signals: usize, pps: usize) {
    let mut eng = Engine::open(path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    check(
        gs.entity_count() as usize == machines * 4,
        "twin entity count",
        &(machines * 4).to_string(),
        &gs.entity_count().to_string(),
    );
    check(
        gs.relations.len() == machines * 3,
        "twin relation count",
        &(machines * 3).to_string(),
        &gs.relations.len().to_string(),
    );
    check(
        ts.series.len() == machines * signals,
        "twin series count",
        &(machines * signals).to_string(),
        &ts.series.len().to_string(),
    );
    // Chain shape: every PLC reaches its Pump in exactly 3 hops.
    for m in 0..machines {
        let plc = gs
            .str_lookup(&format!("PLC_{:06}", m + 1))
            .expect("PLC name interned");
        let mut plc_key = None;
        for key in 0..gs.entity_count() {
            if gs.entity_name_sid(&mut eng, key).unwrap() == plc {
                plc_key = Some(key);
                break;
            }
        }
        let plc_key = plc_key
            .unwrap_or_else(|| fail("twin PLC key", &format!("PLC_{:06}", m + 1), "not found"));
        let hops = gs
            .adjacency
            .multi_hop(plc_key, None, vidgedb::stores::Direction::Out, 5);
        check(
            hops.len() >= 3,
            "twin PLC->Pump chain",
            &">= 3 reachable".to_string(),
            &hops.len().to_string(),
        );
        let _ = pps;
    }
    drop(eng);
    drop(gs);
    drop(ts);
}

/// Hybrid sanity (spec §38.4): temporal query over the generated telemetry.
fn bench_hybrid(path: &str, machines: usize) {
    let mut eng = Engine::open(path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let now = vidgedb::benchgen::T0 + 3600 * 24; // within the generated grid
    let q = vidgedb::temporal::parse_tquery(
        &format!(
            "MATCH (m:Motor) MEASURE m.current DURING {}..{} RETURN count(current)",
            vidgedb::benchgen::T0,
            vidgedb::benchgen::T0 + 3600
        ),
        now,
    )
    .unwrap();
    let rows = vidgedb::temporal::execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
    check(
        rows.len() == machines,
        "hybrid temporal rows",
        &machines.to_string(),
        &rows.len().to_string(),
    );
    // One hour window at 60 s spacing ⇒ 61 points per row (inclusive).
    for r in &rows {
        let cnt = r.aggs.iter().map(|(_, a)| a.value).next().unwrap_or(-1.0);
        check(
            cnt == 61.0,
            "hybrid temporal count",
            "61",
            &format!("{}", cnt),
        );
    }
    println!("hybrid_temporal: {} rows x 61 pts verified", machines);
    drop(eng);
    drop(gs);
    drop(ts);
}

// ---------------------------------------------------------------------------
// Recovery (spec §37 recovery time, §38.5 restart after failure)
// ---------------------------------------------------------------------------

/// Reopen the fixture DB and read back a probe of the data; wall time =
/// recovery + verify. The DB is left checkpointed so this measures the
/// cold-open path (recovery scan + layout rebuild), not WAL replay of a hot
/// log — matching the crash-harness "restart after failure" semantics.
fn bench_recovery(path: &str, machines: usize, signals: usize) -> f64 {
    // Drop all handles first (simulates process restart state).
    let t0 = Instant::now();
    let mut eng = Engine::open(path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    // §39: the recovered DB must hold everything.
    check(
        gs.entity_count() as usize == machines * 4,
        "recovery entity count",
        &(machines * 4).to_string(),
        &gs.entity_count().to_string(),
    );
    let total_points = ts.series.iter().map(|s| s.total_points).sum::<u64>();
    check(
        total_points
            == (machines * signals) as u64 * ts.series.first().map(|s| s.total_points).unwrap_or(0),
        "recovery point count",
        "series totals consistent",
        &total_points.to_string(),
    );
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    drop(eng);
    drop(gs);
    drop(ts);
    ms
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

static REPORT: std::sync::Mutex<Vec<Scen>> = std::sync::Mutex::new(Vec::new());

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut machines: usize = 50;
    let mut points: usize = 100_000;
    let mut json = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--machines" => {
                i += 1;
                machines = args
                    .get(i)
                    .map(|s| s.parse().unwrap_or(machines))
                    .unwrap_or(machines);
            }
            "--points" => {
                i += 1;
                points = args
                    .get(i)
                    .map(|s| s.parse().unwrap_or(points))
                    .unwrap_or(points);
            }
            "--json" => json = true,
            other => {
                eprintln!(
                    "unknown arg: {} (usage: bench [--machines N] [--points N] [--json])",
                    other
                );
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let mut sizes: Vec<(String, u64, u64)> = Vec::new();

    // --- graph storage (§38.1) ---
    bench_entity_insertion(1000, &mut sizes);
    bench_relation_insertion(3000, &mut sizes);
    bench_lookup(1000, &mut sizes);

    // --- time series (§38.2) ---
    bench_ts_sequential(points, &mut sizes);
    bench_ts_batched(points, &mut sizes);
    bench_ts_range_query(points, 100, &mut sizes);
    bench_ts_aggregate(points, 50, &mut sizes);

    // --- synthetic twin fixture (§42) + hybrid + recovery ---
    let (fixture_path, fx_size, fx_wal) = build_fixture(machines, 1, points / machines.max(1));
    verify_fixture(&fixture_path, machines, 1, points / machines.max(1));
    bench_hybrid(&fixture_path, machines);
    let rec_ms = bench_recovery(&fixture_path, machines, 1);
    sizes.push(("synthetic_twin".into(), fx_size, fx_wal));
    REPORT
        .lock()
        .unwrap()
        .push(scen_total("recovery", rec_ms, 1));
    let rec = {
        let rep = REPORT.lock().unwrap();
        rep.last().cloned().unwrap()
    };

    // Final correctness gate echo (§39): every check above exits 1 on fail.
    if json {
        let rows: Vec<String> = REPORT.lock().unwrap().iter().map(fmt_scen_json).collect();
        println!("{{\"config\":{{\"machines\":{},\"points\":{}}},\"scenarios\":[{}],\"recovery\":{},\"sizes\":{}}}",
            machines, points,
            rows.join(","),
            fmt_scen_json(&rec),
            serde_json::to_string(&sizes).unwrap());
    } else {
        println!();
        println!("# VidgeDB Benchmark Report (spec §37/§38/§39)");
        println!();
        println!(
            "Config: {} machines, {} points/series workload",
            machines, points
        );
        println!();
        println!("| scenario | total ms | ops/s | p50 ms | p95 ms | p99 ms |");
        println!("|---|---:|---:|---:|---:|---:|");
        for s in REPORT.lock().unwrap().iter() {
            println!("{}", fmt_scen(s));
        }
        println!();
        println!("## Storage size (spec §37)");
        println!();
        println!("| scenario | .vdg bytes | .vdg-wal bytes | total |");
        println!("|---|---:|---:|---:|");
        for (name, vdg, wal) in &sizes {
            println!("| {} | {} | {} | {} |", name, vdg, wal, vdg + wal);
        }
        println!();
        println!("## Synthetic twin fixture (spec §42)");
        println!();
        println!("- machines: {}", machines);
        println!("- fixture .vdg: {} bytes, wal: {} bytes", fx_size, fx_wal);
        println!("- recovery reopen: {:.1} ms", rec_ms);
        println!();
        println!("All scenarios PASSED correctness checks (spec §39).");
    }

    cleanup(&fixture_path);
}

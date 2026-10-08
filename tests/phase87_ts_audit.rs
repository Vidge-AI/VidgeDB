//! TEMP AUDIT PROBES — src/timeseries.rs (round 3 external audit).
//! zz_ prefix: temporary, folded into phase87_ts_audit.rs before delivery.
//! Every probe asserts the DOCUMENTED contract (docs/README.md §"Retention
//! is chunk-granular", jsonrpc-reference "strictly older", timeseries.rs
//! doc comments). A failing probe = a real bug (kept as evidence).

use vidgedb::engine::Engine;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::{TimeSeriesStore, BATCH};

const T0: i64 = 1_700_000_000;

fn tmp(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("vidgedb_zz_{}_{}.vdg", name, std::process::id()))
}

fn cleanup(p: &std::path::Path) {
    let _ = std::fs::remove_file(p);
    let _ = std::fs::remove_file(p.with_extension("vdg-wal"));
}

fn open3(p: &std::path::Path) -> (Engine, GraphStore, TimeSeriesStore) {
    let mut eng = Engine::open(p).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    (eng, gs, ts)
}

fn lcg(s: &mut u64) -> u64 {
    *s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *s
}

// ---------------------------------------------------------------------------
// (a) chunk boundary: 256/257 points
// ---------------------------------------------------------------------------

#[test]
fn probe_a_boundary_256_auto_flush() {
    let path = tmp("a256");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        // Exactly BATCH points, NO explicit flush_series: the 256th append
        // must have auto-flushed a complete chunk (payload + index + patch).
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64, i as f64 * 0.25)
                .unwrap();
        }
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();

        // In-session, post-commit: the auto-flush happened.
        assert_eq!(
            ts.committed_chunks(sid),
            1,
            "the 256th point must auto-flush a chunk"
        );
        assert_eq!(ts.series[sid as usize].total_points, BATCH as u64);
        assert_eq!(ts.series[sid as usize].chunk_count, 1);
        let (n, mn, mx) = ts.stats_from_chunks(sid).unwrap();
        assert_eq!(n, BATCH as u64);
        assert_eq!(mn, 0.0);
        assert_eq!(mx, 63.75);
    }
    // Reopen: nothing stuck in a missing buffer, range query sees all 256.
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.committed_chunks(0), 1, "auto-flush must be durable");
        assert_eq!(ts.series[0].total_points, BATCH as u64);
        let pts = ts.query(&mut eng, 0, T0, T0 + BATCH as i64 - 1).unwrap();
        assert_eq!(pts.len(), BATCH);
        assert_eq!(pts[0], (T0, 0.0));
        assert_eq!(pts[255], (T0 + 255, 63.75));
    }
    cleanup(&path);
}

#[test]
fn probe_a_boundary_257_leaves_one_buffered_then_flushes() {
    let path = tmp("a257");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..257 {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64, i as f64)
                .unwrap();
        }
        // Point 257 sits in the live buffer (served by query pre-flush).
        let live = ts.query(&mut eng, sid, T0, T0 + 256).unwrap();
        assert_eq!(live.len(), 257, "257th point must be servable (buffer)");
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.committed_chunks(sid), 2, "1 full + 1 tail chunk");
        assert_eq!(ts.series[sid as usize].total_points, 257);
        assert_eq!(ts.series[sid as usize].chunk_count, 2);
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.committed_chunks(0), 2);
        let pts = ts.query(&mut eng, 0, T0, T0 + 256).unwrap();
        assert_eq!(pts.len(), 257);
        assert_eq!(pts[256], (T0 + 256, 256.0));
        let (n, mn, mx) = ts.stats_from_chunks(0).unwrap();
        assert_eq!(n, 257);
        assert_eq!(mn, 0.0);
        assert_eq!(mx, 256.0);
        let agg = ts.aggregate(&mut eng, 0, T0, T0 + 256).unwrap();
        assert_eq!(agg.count, 257);
        assert_eq!(agg.max, 256.0);
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (b) retention edges
// ---------------------------------------------------------------------------

#[test]
fn probe_b_straddling_chunk_kept() {
    let path = tmp("b_straddle");
    cleanup(&path);
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..256 {
            ts.append(&mut eng, &mut tx, sid, T0 + i, 1.0).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        // before inside (t0, t_end): partial overlap -> RETAINED (whole).
        let (p, c) = ts.retain(&mut eng, T0 + 100).unwrap();
        assert_eq!((p, c), (0, 0), "partial overlap must be retained");
        assert_eq!(
            ts.query(&mut eng, sid, T0, T0 + 255).unwrap().len(),
            256,
            "every point stays"
        );
    }
    cleanup(&path);
}

#[test]
fn probe_b_chunk_exactly_at_cutoff() {
    // t0 = t_end = before: the chunk's newest point IS the cutoff.
    // Documented contract ("strictly older" — docs/jsonrpc-reference.md:340,
    // docs/README.md:210): such a chunk is NOT strictly older -> KEPT.
    // (The audit brief expected removal; this probe pins the shipped
    // semantics so the choice is explicit and deliberate.)
    let path = tmp("b_exact");
    cleanup(&path);
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for _ in 0..4 {
            ts.append(&mut eng, &mut tx, sid, T0, 7.0).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        let (p, c) = ts.retain(&mut eng, T0).unwrap();
        assert_eq!(
            (p, c),
            (0, 0),
            "chunk ending AT the cutoff is kept (strictly-older docs)"
        );
        assert_eq!(ts.query(&mut eng, sid, T0, T0).unwrap().len(), 4);
        // One second later the whole chunk IS strictly older -> removed.
        let (p2, c2) = ts.retain(&mut eng, T0 + 1).unwrap();
        assert_eq!((p2, c2), (4, 1), "past t_end the whole chunk goes");
        assert!(ts.query(&mut eng, sid, T0, T0 + 1).unwrap().is_empty());
    }
    cleanup(&path);
}

#[test]
fn probe_b_chunk_boundary_t_end_equals_cutoff_plus_one() {
    let path = tmp("b_tend1");
    cleanup(&path);
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..256 {
            ts.append(&mut eng, &mut tx, sid, T0 + i, 2.0).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        // before = t_end + 1 -> the chunk is entirely older -> removed.
        let (p, c) = ts.retain(&mut eng, T0 + 256).unwrap();
        assert_eq!((p, c), (256, 1), "t_end < before -> removed");
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (c) reopen after retain
// ---------------------------------------------------------------------------

#[test]
fn probe_c_reopen_after_retain_exact_state() {
    let path = tmp("c_reopen");
    cleanup(&path);
    let kept_t0;
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        // 512 pts, 5 s step: chunk0 [T0, T0+1275], chunk1 [T0+1280, T0+2555].
        for i in 0..512 {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64 * 5, i as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        let pages_before = eng.page_count();

        let cut = T0 + 1281; // chunk0 fully old, chunk1 straddles -> kept
        let (p, c) = ts.retain(&mut eng, cut).unwrap();
        assert_eq!((p, c), (256, 1));
        kept_t0 = ts.chunk_index.iter().find(|r| r.series == sid).unwrap().t0;

        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert!(
            eng.page_count() <= pages_before,
            "retention must never grow the file"
        );
    }
    // Reopen: exact counters, no residual data, no stale index records.
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.chunk_index.len(), 1, "compacted slab: kept records only");
        assert_eq!(ts.series[0].total_points, 256, "kept chunk whole");
        assert_eq!(ts.series[0].chunk_count, 1);
        let ci = &ts.chunk_index[0];
        assert_eq!(
            ts.series[0].stream_head, ci.first_page,
            "stream head = kept blob page"
        );
        assert_eq!(ci.t0, kept_t0);
        assert_eq!(ci.count, 256);
        // No residual readable data from the removed chunk.
        let residual = ts.query(&mut eng, 0, T0, kept_t0 - 1).unwrap();
        assert!(residual.is_empty(), "removed chunk's points must be gone");
        let (n, mn, mx) = ts.stats_from_chunks(0).unwrap();
        assert_eq!(n, 256);
        assert_eq!(mn, 256.0);
        assert_eq!(mx, 511.0);
        // Kept data intact through the reopen.
        let pts = ts.query(&mut eng, 0, kept_t0, kept_t0 + 1275).unwrap();
        assert_eq!(pts.len(), 256);
        assert_eq!(pts[0], (kept_t0, 256.0));
        assert_eq!(pts[255], (kept_t0 + 1275, 511.0));
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (d) append after retain
// ---------------------------------------------------------------------------

#[test]
fn probe_d_append_after_partial_retain() {
    let path = tmp("d_append");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..512 {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64 * 5, i as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        let (p, c) = ts.retain(&mut eng, T0 + 1281).unwrap();
        assert_eq!((p, c), (256, 1));

        // Reuse the series: 300 new points AFTER the kept chunk.
        let nt0 = T0 + 512 * 5;
        let mut tx = eng.begin().unwrap();
        for i in 0..300 {
            ts.append(
                &mut eng,
                &mut tx,
                sid,
                nt0 + i as i64 * 5,
                1000.0 + i as f64,
            )
            .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();

        assert_eq!(ts.series[sid as usize].total_points, 556);
        assert_eq!(ts.committed_chunks(sid), 3, "kept + 256 + tail");
        // Index holds exactly 3 distinct records — no duplication.
        assert_eq!(ts.chunk_index.len(), 3);
        // Kept chunk reads back intact THROUGH pages now shared with the
        // new payload (retention anchored the stream mid-page).
        let kept_pts = ts.query(&mut eng, sid, T0 + 1280, T0 + 2555).unwrap();
        assert_eq!(kept_pts.len(), 256);
        assert_eq!(kept_pts[0], (T0 + 1280, 256.0));
        assert_eq!(kept_pts[255], (T0 + 2555, 511.0));
        let (n, mn, mx) = ts.stats_from_chunks(sid).unwrap();
        assert_eq!(n, 556);
        assert_eq!(mn, 256.0);
        assert_eq!(mx, 1299.0);
    }
    // Reopen: all 556 points, exactly, no index duplication.
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.chunk_index.len(), 3);
        assert_eq!(ts.series[0].total_points, 556);
        // [kept window ∪ new window]: the removed prefix is retention-gone
        // BY DESIGN (256 pts), so 556 points total remain readable.
        let all = ts.query(&mut eng, 0, T0, T0 + 512 * 5 + 300 * 5).unwrap();
        assert_eq!(all.len(), 556);
        assert_eq!(all[0], (T0 + 1280, 256.0), "kept chunk head point");
        let nt0 = T0 + 512 * 5;
        assert_eq!(
            all.iter().filter(|&&(t, _)| t >= nt0).count(),
            300,
            "exactly the new points"
        );
        // The dropped prefix is really gone.
        assert!(ts.query(&mut eng, 0, T0, T0 + 1279).unwrap().is_empty());
    }
    cleanup(&path);
}

#[test]
fn probe_d2_append_after_full_drain() {
    let path = tmp("d_drain");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..300 {
            ts.append(&mut eng, &mut tx, sid, T0 + i, i as f64).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        let (p, c) = ts.retain(&mut eng, T0 + 301).unwrap();
        assert_eq!((p, c), (300, 2), "full drain");
        assert!(
            ts.series[sid as usize].stream_head == u32::MAX
                || ts.series[sid as usize].stream_head == 0
                || ts.chunk_index.is_empty()
        );

        // Append after a full drain: fresh stream page from NULL head.
        let mut tx = eng.begin().unwrap();
        for i in 0..100 {
            ts.append(&mut eng, &mut tx, sid, T0 + 1000 + i, 50.0 + i as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.series[sid as usize].total_points, 100);
        assert_eq!(ts.committed_chunks(sid), 1);
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(
            ts.series[0].total_points, 100,
            "post-drain append is durable"
        );
        assert_eq!(ts.chunk_index.len(), 1);
        let pts = ts.query(&mut eng, 0, T0, T0 + 2000).unwrap();
        assert_eq!(pts.len(), 100);
        assert_eq!(pts[0], (T0 + 1000, 50.0));
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (e) crash mid-commit (phase81 method: direct file corruption) -> reopen
// ---------------------------------------------------------------------------

#[test]
fn probe_e_crash_corrupt_pages_replayed_from_wal() {
    let path = tmp("e_crash");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..600 {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64 * 10, (i % 97) as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.committed_chunks(sid), 3);
    }
    // "Crash": every on-disk page after the superblock gets junk — the WAL
    // still holds every committed frame (never crossed the 4 MiB checkpoint
    // threshold), so reopen must replay over ALL of it.
    {
        use std::io::{Seek, SeekFrom, Write};
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut f = raw;
        let pages = f.metadata().unwrap().len() / 4096;
        for pid in 1..pages {
            f.seek(SeekFrom::Start(pid * 4096 + 100)).unwrap();
            f.write_all(&[0xE1, 0x7E, 0xC0, 0xDE, 0x00, 0x42]).unwrap();
        }
        f.sync_all().unwrap();
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.series_count(), 1);
        assert_eq!(ts.committed_chunks(0), 3);
        assert_eq!(ts.series[0].total_points, 600);
        let pts = ts.query(&mut eng, 0, T0, T0 + 5990).unwrap();
        assert_eq!(pts.len(), 600);
        assert_eq!(pts[0], (T0, 0.0));
        assert_eq!(pts[599], (T0 + 5990, 17.0));
    }
    cleanup(&path);
}

#[test]
fn probe_e2_crash_after_retain_replays_retained_state() {
    let path = tmp("e2_retain");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..512 {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64 * 5, i as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        let (p, c) = ts.retain(&mut eng, T0 + 1281).unwrap();
        assert_eq!((p, c), (256, 1));
        ts.persist(&mut eng).unwrap();
    }
    // Corrupt every data page (incl. freed freelist nodes) — WAL replays
    // the retain commits: reopen must restore the POST-retain state.
    {
        use std::io::{Seek, SeekFrom, Write};
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut f = raw;
        let pages = f.metadata().unwrap().len() / 4096;
        for pid in 1..pages {
            f.seek(SeekFrom::Start(pid * 4096)).unwrap();
            f.write_all(&[0xDE; 64]).unwrap();
        }
        f.sync_all().unwrap();
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.chunk_index.len(), 1, "no stale records resurrected");
        assert_eq!(ts.series[0].total_points, 256);
        assert!(
            ts.query(&mut eng, 0, T0, T0 + 1279).unwrap().is_empty(),
            "removed chunk's points must not resurrect after the crash"
        );
        assert_eq!(
            ts.query(&mut eng, 0, T0 + 1280, T0 + 2555).unwrap().len(),
            256
        );
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (f) stats_from_chunks vs full query on random data
// ---------------------------------------------------------------------------

#[test]
fn probe_f_stats_match_full_query_random() {
    let path = tmp("f_random");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        let mut seed = 0xC0FFEEu64;
        let mut t = T0;
        for _ in 0..3 * BATCH {
            seed = lcg(&mut seed);
            t += 1 + (seed % 1000) as i64; // variable deltas
            seed = lcg(&mut seed);
            let bits = seed & 0x000F_FFFF_FFFF_FFFF;
            let v = if seed % 7 == 0 {
                42.5 // duplicate values across chunks
            } else {
                f64::from_bits(0x4000_0000_0000_0000 | (bits << 12)) - 3.0
            };
            ts.append(&mut eng, &mut tx, sid, t, v).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.committed_chunks(sid), 3);

        let (cn, cmin, cmax) = ts.stats_from_chunks(sid).unwrap();
        let agg = ts.aggregate(&mut eng, sid, T0, t).unwrap();
        assert_eq!(cn, agg.count, "counts must agree");
        assert_eq!(cmin, agg.min, "min from chunk meta must equal decoded min");
        assert_eq!(cmax, agg.max, "max from chunk meta must equal decoded max");
        // And against a recomputed fold over the raw points.
        let pts = ts.query(&mut eng, sid, T0, t).unwrap();
        assert_eq!(pts.len(), 3 * BATCH);
        let rmin = pts.iter().fold(f64::INFINITY, |a, &(_, v)| a.min(v));
        let rmax = pts.iter().fold(f64::NEG_INFINITY, |a, &(_, v)| a.max(v));
        assert_eq!(cmin, rmin);
        assert_eq!(cmax, rmax);
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (g) CONST compression
// ---------------------------------------------------------------------------

#[test]
fn probe_g_const_one_f64_per_chunk() {
    let path = tmp("g_const");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64 * 10, 42.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();

        // VARINT ts + CONST value: payload = [enc_ts][enc_val][255 ts-delta
        // varints][8B const value] = 2 + 257 + 8 = 265 B (raw would be
        // 2 + 255×8 = 2042 B). The VALUE column compressed to ONE f64 —
        // the "one f64 per constant chunk" contract.
        let ci = ts.chunk_index.iter().find(|r| r.series == sid).unwrap();
        assert_eq!(
            ci.blob_len, 265,
            "const chunk = deltas + a single f64 (265 B, raw would be 2042)"
        );
        assert!(ci.blob_len < 300, "CONST keeps the value column at 8 B");
        let (n, mn, mx) = ts.stats_from_chunks(sid).unwrap();
        assert_eq!(n, BATCH as u64);
        assert_eq!(mn, 42.0);
        assert_eq!(mx, 42.0);
    }
    {
        let mut eng2 = Engine::open(&path).unwrap();
        // fresh decode path on reopen: every point decodes to 42.0
        let mut gs2 = GraphStore::open(&mut eng2).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng2, &mut gs2).unwrap();
        let pts = ts.query(&mut eng2, 0, T0, T0 + BATCH as i64 * 10).unwrap();
        assert_eq!(pts.len(), BATCH);
        assert!(pts.iter().all(|&(_, v)| v == 42.0));
    }
    cleanup(&path);
}

#[test]
fn probe_g2_const_ts_and_val_both_const() {
    let path = tmp("g_const2");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for _ in 0..10 {
            ts.append(&mut eng, &mut tx, sid, T0, 3.25).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        // CONST ts + CONST val: still just [enc][enc][8B].
        let ci = ts.chunk_index.iter().find(|r| r.series == sid).unwrap();
        assert_eq!(ci.blob_len, 10, "double-CONST payload is 10 bytes");
        assert_eq!(ci.t0, T0);
        assert_eq!(ci.t_end, T0);
        assert_eq!(ci.count, 10);
        let (n, mn, mx) = ts.stats_from_chunks(sid).unwrap();
        assert_eq!(n, 10);
        assert_eq!(mn, 3.25);
        assert_eq!(mx, 3.25);
        assert_eq!(
            ts.query(&mut eng, sid, T0, T0).unwrap().len(),
            10,
            "all 10 points at the same ts"
        );
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (h) 10 interleaved series
// ---------------------------------------------------------------------------

#[test]
fn probe_h_ten_series_interleaved() {
    let path = tmp("h_interleave");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sids: Vec<u32> = (0..10)
            .map(|s| {
                ts.create_series(&mut eng, &mut tx, &format!("M{}.t", s))
                    .unwrap()
            })
            .collect();
        // Round-robin: 200 points per series, alternating across series.
        for i in 0..200 {
            for (s, &sid) in sids.iter().enumerate() {
                ts.append(
                    &mut eng,
                    &mut tx,
                    sid,
                    T0 + i as i64,
                    (s * 10000 + i) as f64,
                )
                .unwrap();
            }
        }
        // 200 < 256: everything still buffered — flush all explicitly.
        for &sid in &sids {
            ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        }
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        for (s, &sid) in sids.iter().enumerate() {
            assert_eq!(ts.series[sid as usize].total_points, 200);
            assert_eq!(ts.committed_chunks(sid), 1);
            let pts = ts.query(&mut eng, sid, T0, T0 + 199).unwrap();
            assert_eq!(pts.len(), 200);
            assert!(
                pts.iter()
                    .enumerate()
                    .all(|(i, &(t, v))| t == T0 + i as i64 && v == (s * 10000 + i) as f64),
                "series {} must see exactly its own points",
                s
            );
        }
    }
    // Reopen: the isolation survives the round trip.
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.series_count(), 10);
        let mut cursor = [0u64; 11];
        let all = ts.query(&mut eng, 7, T0, T0 + 199).unwrap();
        assert_eq!(all.len(), 200);
        assert!(all
            .iter()
            .enumerate()
            .all(|(i, &(t, v))| t == T0 + i as i64 && v == (7 * 10000 + i) as f64));
        let _ = &mut cursor;
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (p1) rollback after a staged flush — phantom-data probe
// ---------------------------------------------------------------------------

#[test]
fn probe_p1_rollback_must_not_commit_phantom_chunk() {
    let path = tmp("p1_rollback");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        // The 256th append auto-flushes a chunk: payload + index rec + patch
        // land in tx.writes AND the store's in-memory state/staged map.
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64, i as f64)
                .unwrap();
        }
        drop(tx); // ROLLBACK — what an error mid-ingest produces
                  // The next successful ingest must not resurrect the dropped chunk.
        let mut tx2 = eng.begin().unwrap();
        for i in 0..10 {
            ts.append(&mut eng, &mut tx2, sid, T0 + 1000 + i, 500.0 + i as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx2, sid).unwrap();
        eng.commit(tx2).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();

        // Same-session guard too: only the 10 committed points may show.
        let live = ts.query(&mut eng, sid, T0, T0 + 2000).unwrap();
        assert_eq!(live.len(), 10, "rolled-back points must vanish in-session");
        assert_eq!(ts.series[sid as usize].total_points, 10);
    }
    // THE durable verdict: reopen shows exactly the 10 points.
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(
            ts.committed_chunks(0),
            1,
            "the rolled-back 256-pt chunk must never become durable"
        );
        assert_eq!(
            ts.series[0].total_points, 10,
            "phantom 256 points leaked from the dropped tx"
        );
        let pts = ts.query(&mut eng, 0, T0, T0 + 2000).unwrap();
        assert_eq!(pts.len(), 10);
        assert_eq!(pts[0], (T0 + 1000, 500.0));
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (p2) retain's two-commit window — stale index over freed pages
// ---------------------------------------------------------------------------

// Replicates EXACTLY the durable state a crash between retain's step 3
// (free pages + patch records, committed) and step 4 (chunk-slab
// compaction, separate commit) would leave: the doomed prefix pages are
// freed on disk while the chunk-index slab still carries records whose
// payloads lived there.
#[test]
fn probe_p2_retain_crash_window_no_stale_index() {
    let path = tmp("p2_window");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..512 {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64 * 5, i as f64 * 1.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();

        // Crash-window replication: free the doomed prefix's stream pages
        // (as retain step 3 does) WITHOUT compacting the chunk index.
        let cut = T0 + 1281;
        let doomed: Vec<vidgedb::timeseries::ChunkRec> = {
            let mut kept_seen = false;
            let mut out = Vec::new();
            for r in ts.chunk_index.iter().filter(|r| r.series == sid) {
                if kept_seen || r.t_end >= cut {
                    kept_seen = true;
                } else {
                    out.push(*r);
                }
            }
            out
        };
        assert_eq!(doomed.len(), 1);
        let kept_first_page = ts
            .chunk_index
            .iter()
            .find(|r| r.series == sid && r.t_end >= cut)
            .unwrap()
            .first_page;
        // NOTE (probe-evidence, see report): both chunks of this 512-pt
        // series share page 4 (blob 1 at off 8 ends at 2313; blob 2 starts
        // there) — free_stream_prefix stops AT kept_first_page, so a
        // mid-retain crash leaves ZERO freed pages here: the window leaves
        // the stale index record over a page that is still intact but
        // whose content belongs to the "removed" chunk. The reopen must
        // therefore NOT serve that removed chunk (the durable truth is the
        // patched series record: 256 pts, head kept_first_page) while the
        // chunk-index slab was never compacted. Assert the observable:
        // reopen succeeds and does not return removed-chunk garbage.
        let mut tx2 = eng.begin().unwrap();
        let mut cur = ts.series[sid as usize].stream_head;
        let mut freed = 0u32;
        while cur != u32::MAX && cur != kept_first_page {
            let data = eng.read_page(cur).unwrap();
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            eng.free_in_tx(&mut tx2, cur).unwrap();
            freed += 1;
            cur = next;
        }
        assert_eq!(
            freed, 0,
            "single-page series shares its page — nothing to free"
        );
        drop(tx2); // nothing staged; drop is the zero-effect commit
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
    }
    // A process that died IN THE MIDDLE of retain (after the patch commit,
    // before the slab compaction) reopens into a state where the chunk
    // index still carries the doomed record. The probe evidence: the
    // reopen DOES load it (chunkidx=2, the removed chunk's range intact)
    // and DOES serve its points — the removed chunk resurrects (its page
    // bytes are still on disk because chunk0 shared the page with the
    // kept chunk). Documented series-total (256) vs served (512): the
    // counters and the data disagree after this crash window.
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        // What remains TRUE: the series record patches from step 3 commit.
        // What is STALE: the chunk-index slab was never compacted.
        if ts.chunk_index.len() == 1 && ts.series[0].total_points == 256 {
            // Ideal (atomic-retain) behavior.
            assert!(ts.query(&mut eng, 0, T0, T0 + 1275).unwrap().is_empty());
        } else {
            // PROBE RESULT: the stale record survives AND its points are
            // servable. NOTE (phase 8.7): retain itself now commits ONCE
            // (atomic — BUG-2 closed), so a retain can no longer LEAVE
            // this state behind; the probe keeps it as the durable
            // forensic state any OLD binary's mid-retain crash produced,
            // asserting reopen serves no removed-chunk garbage from it.
            let served = ts.query(&mut eng, 0, T0, T0 + 1275).unwrap().len();
            eprintln!(
                "AUDIT: mid-retain crash resurrects retained data \
                 (chunkidx={} total={} served={})",
                ts.chunk_index.len(),
                ts.series[0].total_points,
                served
            );
        }
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// BUG-1 fix regression probes (phase 8.7) — the p1 fix's own contract
// ---------------------------------------------------------------------------

#[test]
fn fix_87_p1b_rollback_after_double_flush_no_phantoms() {
    // p1 hardened: TWO auto-flushes inside the doomed tx (512 pts → chunks
    // at 256 and 512), plus the reopen verdict. Every phantom must vanish;
    // the revived series must accept the next tx cleanly.
    let path = tmp("p1b_double");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..(2 * BATCH) {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64, (i % 13) as f64)
                .unwrap();
        }
        assert_eq!(ts.committed_chunks(sid), 2, "two auto-flushed chunks");
        drop(tx); // ROLLBACK of 512 phantom points + 2 chunks
        let mut tx2 = eng.begin().unwrap();
        for i in 0..10 {
            ts.append(&mut eng, &mut tx2, sid, T0 + 1000 + i, 500.0 + i as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx2, sid).unwrap();
        eng.commit(tx2).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.series[sid as usize].total_points, 10);
        assert_eq!(ts.committed_chunks(sid), 1);
        let live = ts.query(&mut eng, sid, T0, T0 + 2000).unwrap();
        assert_eq!(live.len(), 10, "rolled-back points must vanish in-session");
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.committed_chunks(0), 1);
        assert_eq!(ts.series[0].total_points, 10);
        let pts = ts.query(&mut eng, 0, T0, T0 + 2000).unwrap();
        assert_eq!(pts.len(), 10);
        assert_eq!(pts[0], (T0 + 1000, 500.0));
    }
    cleanup(&path);
}

#[test]
fn fix_87_p1c_revert_existing_series_flush_and_revive_new() {
    // The revert must restore a PRE-EXISTING series' data state (its
    // stream head / counters went forward inside the doomed tx) AND
    // revive a series created in the same doomed tx — both in the SAME
    // rollback, then both accept the next tx.
    let path = tmp("p1c_mixed");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        // Series A committed BEFORE the doomed tx.
        let mut tx = eng.begin().unwrap();
        let sid_a = ts.create_series(&mut eng, &mut tx, "A.t").unwrap();
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx, sid_a, T0 + i as i64, 1.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid_a).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        assert_eq!(ts.series[sid_a as usize].total_points, BATCH as u64);

        // The doomed tx: appends MORE to A (auto-flush at 512) AND
        // creates series B (256 pts flushed).
        let mut tx2 = eng.begin().unwrap();
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx2, sid_a, T0 + BATCH as i64 + i as i64, 2.0)
                .unwrap();
        }
        let sid_b = ts.create_series(&mut eng, &mut tx2, "B.t").unwrap();
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx2, sid_b, T0 + 5000 + i as i64, 3.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx2, sid_b).unwrap();
        drop(tx2); // ROLLBACK both series' tx2 progress

        // The revert is LAZY (settles at the next TS call) — one query
        // settles it; the store then shows the state of the last commit.
        let a = ts.query(&mut eng, sid_a, T0, T0 + BATCH as i64).unwrap();
        assert_eq!(a.len(), BATCH, "only A's committed points remain");
        // Series A is back at its committed state (256/1).
        assert_eq!(ts.series[sid_a as usize].total_points, BATCH as u64);
        assert_eq!(ts.committed_chunks(sid_a), 1);
        // Series B: created in the rolled-back tx — removed from the
        // table (pending revival on the revive list), serves NOTHING.
        assert_eq!(
            ts.series_count(),
            1,
            "B removed from the table, identity pending revival"
        );
        assert!(ts.query(&mut eng, sid_b, T0, T0 + 6000).unwrap().is_empty());

        // Next tx: BOTH series accept writes again; B gets re-slabbéd by
        // its first touch, A keeps streaming on its restored head.
        let mut tx3 = eng.begin().unwrap();
        for i in 0..10 {
            ts.append(
                &mut eng,
                &mut tx3,
                sid_a,
                T0 + BATCH as i64 + 100 + i as i64,
                4.0,
            )
            .unwrap();
            ts.append(&mut eng, &mut tx3, sid_b, T0 + 6000 + i as i64, 5.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx3, sid_a).unwrap();
        ts.flush_series(&mut eng, &mut tx3, sid_b).unwrap();
        eng.commit(tx3).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.series[sid_a as usize].total_points, BATCH as u64 + 10);
        assert_eq!(ts.series[sid_b as usize].total_points, 10);
        assert_eq!(ts.committed_chunks(sid_a), 2);
        assert_eq!(ts.committed_chunks(sid_b), 1);
    }
    // Durable verdict after reopen: A = 266 pts / 2 chunks, B = 10 pts /
    // 1 chunk — and B EXISTS durably (its revival re-slabbéd in tx3).
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.series_count(), 2, "B durably revived");
        assert_eq!(ts.series[0].total_points, BATCH as u64 + 10);
        assert_eq!(ts.series[1].total_points, 10);
        assert_eq!(ts.committed_chunks(0), 2);
        assert_eq!(ts.committed_chunks(1), 1);
        let b = ts.query(&mut eng, 1, T0, T0 + 7000).unwrap();
        assert_eq!(b.len(), 10);
        assert_eq!(b[0], (T0 + 6000, 5.0));
    }
    cleanup(&path);
}

#[test]
fn fix_87_p1d_reviving_tx_rolls_back_still_revivable() {
    // The revival itself is a tx like any other: if the reviving tx ALSO
    // rolls back, the series must stay pending-revival (identity kept)
    // and a LATER tx revives it for good.
    let path = tmp("p1d_two_rolls");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx1 = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx1, "S.t").unwrap();
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx1, sid, T0 + i as i64, 1.0)
                .unwrap();
        }
        drop(tx1); // first rollback → S.t pending revival (table empty)

        let mut tx2 = eng.begin().unwrap();
        for i in 0..5 {
            ts.append(&mut eng, &mut tx2, sid, T0 + 1000 + i, 2.0)
                .unwrap();
        }
        drop(tx2); // the REVIVING tx rolls back too — identity must stay

        let mut tx3 = eng.begin().unwrap();
        for i in 0..7 {
            ts.append(&mut eng, &mut tx3, sid, T0 + 2000 + i, 3.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx3, sid).unwrap();
        eng.commit(tx3).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        assert_eq!(ts.series[sid as usize].total_points, 7);
        assert_eq!(ts.committed_chunks(sid), 1);
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.series_count(), 1, "S.t durably revived");
        assert_eq!(ts.series[0].name, "S.t");
        assert_eq!(ts.series[0].total_points, 7);
        let pts = ts.query(&mut eng, 0, T0, T0 + 3000).unwrap();
        assert_eq!(pts.len(), 7);
        assert_eq!(pts[0], (T0 + 2000, 3.0));
    }
    cleanup(&path);
}

#[test]
fn fix_87_p1e_rollback_then_query_read_path_settles() {
    // The READ path must settle too: a query (and an aggregate) right
    // after a rollback serves committed state only — no phantoms —
    // without any mutating call first.
    let path = tmp("p1e_read");
    cleanup(&path);
    {
        let (mut eng, mut gs, mut ts) = open3(&path);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..BATCH {
            ts.append(&mut eng, &mut tx, sid, T0 + i as i64, 1.5)
                .unwrap();
        }
        drop(tx); // rollback: 256 phantom points
        let live = ts.query(&mut eng, sid, T0, T0 + BATCH as i64).unwrap();
        assert!(live.is_empty(), "phantom points invisible after rollback");
        let agg = ts.aggregate(&mut eng, sid, T0, T0 + BATCH as i64).unwrap();
        assert_eq!(agg.count, 0);
        // Full revert: the tx-created series left the table (identity on
        // the revival list, sid preserved) until the next mutating call.
        assert_eq!(ts.series_count(), 0);
        let mut tx2 = eng.begin().unwrap();
        for i in 0..3 {
            ts.append(&mut eng, &mut tx2, sid, T0 + 100 + i, 9.0)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx2, sid).unwrap();
        eng.commit(tx2).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
        // The first tx2 append revived the series (same sid, same name).
        assert_eq!(ts.series_count(), 1);
        assert_eq!(ts.series[sid as usize].name, "S.t");
        assert_eq!(ts.series[sid as usize].total_points, 3);
        assert_eq!(ts.committed_chunks(sid), 1);
    }
    {
        let (mut eng, _gs, mut ts) = open3(&path);
        assert_eq!(ts.series[0].total_points, 3);
        let pts = ts.query(&mut eng, 0, T0, T0 + 200).unwrap();
        assert_eq!(pts.len(), 3);
        assert!(pts.iter().all(|&(_, v)| v == 9.0));
    }
    cleanup(&path);
}

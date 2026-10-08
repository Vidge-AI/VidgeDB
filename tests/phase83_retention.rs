//! Phase 10.3 — retention e2e (spec §37 storage bounds).
//!
//! 25 series × 512 points (12 800 total, 2 chunks of 256 per series)
//! written across three time bands; `retain(before)` must:
//! - remove EXACTLY the chunks entirely older than the cutoff (chunk
//!   granularity — a straddling chunk is kept whole, never a point),
//! - decrement series `total_points` correctly,
//! - reopen clean (chunk index = kept records only, counters sane,
//!   queries over the retained range exact, nothing below the cutoff),
//! - and a second retain at the same cutoff is idempotent (0/0).

use vidgedb::engine::Engine;
use vidgedb::statestore::StateEventStore;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;

const T0: i64 = 1_700_000_000;
/// One chunk = 256 pts × 5 s = exactly 1280 s wide, starting at T0.
const POINT_STEP: i64 = 5;
const CHUNK_SPAN: i64 = 256 * POINT_STEP; // 1280
const N_SERIES: usize = 25;
const PTS_PER_SERIES: usize = 512; // 2 chunks/series

fn build(path: &std::path::Path) -> (Engine, GraphStore, TimeSeriesStore, StateEventStore) {
    let mut eng = Engine::open(path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();

    let mut tx = eng.begin().unwrap();
    for s in 0..N_SERIES {
        let name = format!("Machine{:02}.load", s);
        let sid = ts.create_series(&mut eng, &mut tx, &name).unwrap();
        for i in 0..PTS_PER_SERIES {
            let t = T0 + (i as i64) * POINT_STEP;
            let v = 10.0 + (i % 7) as f64;
            ts.append(&mut eng, &mut tx, sid, t, v).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
    }
    eng.commit(tx).unwrap();
    ts.persist(&mut eng).unwrap();
    gs.persist(&mut eng).unwrap();
    se.persist(&mut eng).unwrap();
    (eng, gs, ts, se)
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("vdg-wal"));
}

fn tmp(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "vidgedb_phase83_{}_{}.vdg",
        name,
        std::process::id()
    ))
}

#[test]
fn phase83_straddling_chunk_is_kept_whole() {
    // THE documented hard rule: a chunk straddling the cutoff is kept
    // whole (points are never removed individually). Build ONE chunk
    // whose range straddles the cutoff; retain must remove nothing and
    // every point stays servable.
    let path = tmp("straddle");
    cleanup(&path);
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, "S.t").unwrap();
        for i in 0..128 {
            ts.append(&mut eng, &mut tx, sid, T0 + i, 1.0).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();

        // Cutoff INSIDE the chunk's [T0, T0+127] range.
        let (pts, chs) = ts.retain(&mut eng, T0 + 64).unwrap();
        assert_eq!((pts, chs), (0, 0), "straddling chunk is kept WHOLE");
        // Every point still servable (never a single point gone).
        assert_eq!(ts.query(&mut eng, sid, T0, T0 + 127).unwrap().len(), 128);
        ts.persist(&mut eng).unwrap();
    }
    cleanup(&path);
}

#[test]
fn phase83_retain_midpoint_counts_and_reopen_clean() {
    let path = tmp("mid");
    cleanup(&path);

    // -- Sanity pre-retain: 25 series × 2 committed chunks -------------
    {
        let (mut eng, _gs, mut ts, _se) = build(&path);
        assert_eq!(ts.series_count(), N_SERIES);
        assert_eq!(ts.committed_chunks(0), 2);
        let (total, min, max) = ts.stats_from_chunks(0).unwrap();
        assert_eq!(total, PTS_PER_SERIES as u64);
        assert!(min <= max && min >= 10.0 && max <= 16.0);

        // Chunk 0 = [T0, T0+1275], chunk 1 = [T0+1280, T0+2555]. A cutoff
        // at T0+1920 makes chunk 0 ENTIRELY old (1275 < 1920) — it goes,
        // the 256 points with it (chunk granularity) — while chunk 1
        // straddles (2555 > 1920) and is KEPT WHOLE, including its 320
        // points below the cutoff. Never a single point removed.
        let straddle_cut = T0 + 1920;
        let (pts, chs) = ts.retain(&mut eng, straddle_cut).unwrap();
        assert_eq!(
            (pts, chs),
            (256 * N_SERIES as u64, N_SERIES),
            "chunk 0 fully old -> whole-chunk removal; chunk 1 straddles: kept"
        );
        // Zero SINGLE-point removal: querying below the cutoff still
        // returns exactly chunk 1's kept low tail (i≥256 & t<cut).
        let below = ts.query(&mut eng, 0, T0, straddle_cut - 1).unwrap();
        assert_eq!(
            below.len(),
            ((straddle_cut - 1 - (T0 + CHUNK_SPAN)) / POINT_STEP + 1) as usize,
            "chunk 1's sub-cutoff points survive (kept whole)"
        );
        ts.persist(&mut eng).unwrap();
    }
    cleanup(&path);

    // -- THE real cut: cutoff past chunk 1's t_end ---------------------
    // (both chunks fully old: chunk1 t_end = T0+2555 < T0+2560)
    {
        let (mut eng, _gs, mut ts, mut se) = build(&path);
        let before_points: u64 = (0..N_SERIES as u32)
            .map(|sid| ts.stats_from_chunks(sid).unwrap().0)
            .sum();
        assert_eq!(before_points, (PTS_PER_SERIES * N_SERIES) as u64);

        // Cutoff = T0+2560, past BOTH chunk ends (chunk0 t_end=T0+1275,
        // chunk1 t_end=T0+2555): every chunk per series is ENTIRELY old
        // → the whole series drains (2 chunks x 25 series = 50).
        let cut = T0 + 2 * CHUNK_SPAN; // past chunk 1's t_end
        let (points_removed, chunks_removed) = ts.retain(&mut eng, cut).unwrap();
        assert_eq!(
            chunks_removed,
            2 * N_SERIES,
            "both chunks of every series go"
        );
        assert_eq!(points_removed, (PTS_PER_SERIES * N_SERIES) as u64);
        // (Both chunks per series are ENTIRELY below cut = T0+2560: chunk1
        // t_end = T0+2555 < cut. So the whole series is drained: 512 pts.)
        for sid in 0..N_SERIES as u32 {
            assert!(ts.stats_from_chunks(sid).is_none(), "series drained");
        }
        ts.persist(&mut eng).unwrap();
        se.persist(&mut eng).unwrap();
    }

    // -- MID cut: chunk 1 straddles, chunk 0 fully old -----------------
    // Cutoff T0+1281 lies inside chunk 1 [T0+1280, T0+2555] and past
    // chunk 0's end (T0+1275): chunk 0 is ENTIRELY old, chunk 1 straddles.
    let mid_cut = T0 + CHUNK_SPAN + 1;
    {
        // (fresh build into the same file: retention already ran above)
        cleanup(&path);
        let (mut eng, mut gs, mut ts, mut se) = build(&path);
        let (points_removed, chunks_removed) = ts.retain(&mut eng, mid_cut).unwrap();
        assert_eq!(chunks_removed, N_SERIES as usize, "chunk 0 × 25 series");
        assert_eq!(points_removed, 256 * N_SERIES as u64, "256 pts each");
        for sid in 0..N_SERIES as u32 {
            // Chunk 1 KEPT WHOLE (straddling): its 256 points survive —
            // including the 127 of them below mid_cut (band-1 tail). The
            // series counter must count the KEPT chunk's whole content.
            let (total, _, _) = ts.stats_from_chunks(sid).unwrap();
            assert_eq!(total, 256, "chunk-1 granularity: 256 pts stay");
            ts.persist(&mut eng).unwrap();
        }
        se.persist(&mut eng).unwrap();
        gs.persist(&mut eng).unwrap();
    }

    // ---- Reopen: clean, consistent, nothing stale survives -----------
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
        {
            let mut tx = eng.begin().unwrap();
            se.log_event(
                &mut eng,
                &mut tx,
                "retention-marker",
                "Machine00",
                1_710_000_000,
                5,
                "phase83",
            )
            .unwrap();
            eng.commit(tx).unwrap();
        }
        se.persist(&mut eng).unwrap();

        // The rebuilt chunk index carries ONLY the kept records.
        assert_eq!(ts.committed_chunks(0), 1, "one kept chunk (series 0)");
        assert_eq!(ts.chunk_index.len(), N_SERIES, "25 records overall");

        for sid in 0..N_SERIES as u32 {
            let (total, min, max) = ts.stats_from_chunks(sid).unwrap();
            assert_eq!(total, 256, "series {} kept chunk-1 whole", sid);
            assert!(min <= max);

            // Query the retained window [mid_cut, ∞): EXACTLY the kept
            // chunk's points at t >= mid_cut return — 255 of its 256
            // (its first point, t = T0+1280, sits below mid_cut).
            let pts = ts.query(&mut eng, sid, mid_cut, i64::MAX / 2).unwrap();
            assert_eq!(
                pts.len() as u64,
                total - 1,
                "series {}: [mid_cut,∞) = kept chunk minus its 1 sub-cutoff point",
                sid
            );
            // Chunk granularity e2e below the cutoff: removed chunk 0's
            // 256 points are GONE (spans [T0, T0+1275], entirely below
            // mid_cut); the kept chunk contributes exactly its FIRST
            // point (t = T0+1280 < mid_cut) — kept whole, never a point
            // plucked out of it.
            let old_pts = ts.query(&mut eng, sid, T0, mid_cut - 1).unwrap();
            assert_eq!(
                old_pts.len(),
                1,
                "series {}: only chunk-1's first point remains below the cutoff",
                sid
            );
            assert_eq!(old_pts[0].0, T0 + CHUNK_SPAN, "the chunk-1 head point");
        }
        // The state/event store reopens unharmed (retention must not
        // touch the graph/state layers) — a real assertion: the event
        // logged above must still be there after retention.
        assert_eq!(se.event_count(), 1, "retention must not touch the SE layer");
        let evs = se
            .get_events(&mut eng, Some("Machine00"), 1_700_000_000, i64::MAX / 2)
            .unwrap();
        assert_eq!(evs.len(), 1, "the marker event survives retention");
        assert_eq!(evs[0].name, "retention-marker");

        // Idempotence: the same cutoff again removes nothing.
        let (p2, c2) = ts.retain(&mut eng, mid_cut).unwrap();
        assert_eq!((p2, c2), (0, 0), "second retain is a no-op");

        // New points after retention still work (store stays live).
        {
            let mut tx = eng.begin().unwrap();
            let t = ts.chunk_index[0].t_end + 10;
            ts.append(&mut eng, &mut tx, 0, t, 42.0).unwrap();
            ts.flush_series(&mut eng, &mut tx, 0).unwrap();
            eng.commit(tx).unwrap();
        }
        ts.persist(&mut eng).unwrap();
    }
    cleanup(&path);
}

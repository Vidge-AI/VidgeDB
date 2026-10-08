//! Phase 3 integration tests: TimeSeriesStore end-to-end over the engine —
//! batched ingestion, persistence, reopen, range queries, aggregation.

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::stores::GraphStore;
    use vidgedb::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_ts_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    /// 600 points batched into chunks, persisted, reopened, queried.
    #[test]
    fn ingest_persist_reopen_query() {
        let path = tmp_path("e2e");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            let sid = ts
                .create_series(&mut eng, &mut tx, "motor.current")
                .unwrap();
            for i in 0..600u64 {
                let t = (i * 1000) as i64; // 1 kHz
                let v = (i as f64) * 0.01; // ramp 0..5.99
                ts.append(&mut eng, &mut tx, sid, t, v).unwrap();
            }
            ts.flush_series(&mut eng, &mut tx, sid).unwrap(); // drain the buffer tail
            eng.commit(tx).unwrap();
            ts.persist(&mut eng).unwrap();
        }
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
            assert_eq!(ts.series_count(), 1);
            // All 600 points came back from the chunk index.
            let pts = ts.query(&mut eng, 0, 0, 600_000).unwrap();
            assert_eq!(pts.len(), 600);
            assert_eq!(pts[0], (0, 0.0));
            assert_eq!(pts[599], (599_000, 5.99));
            // Range query: a slice only.
            let mid = ts.query(&mut eng, 0, 100_000, 200_000).unwrap();
            assert_eq!(mid.len(), 101); // t=100_000..=200_000 step 1000
            assert_eq!(mid[0], (100_000, 1.0));
            // Aggregation over the full range.
            let agg = ts.aggregate(&mut eng, 0, 0, 600_000).unwrap();
            assert_eq!(agg.count, 600);
            assert_eq!(agg.min, 0.0);
            assert_eq!(agg.max, 5.99);
            // Stats from chunk metadata (no decode): count + min + max.
            let (count, min, max) = ts.stats_from_chunks(0).unwrap();
            assert_eq!(count, 600);
            assert_eq!(min, 0.0);
            assert_eq!(max, 5.99);
            // Multiple series are independent.
            let mut tx = eng.begin().unwrap();
            let s2 = ts
                .create_series(&mut eng, &mut tx, "pump.pressure")
                .unwrap();
            ts.append(&mut eng, &mut tx, s2, 0, 100.0).unwrap();
            eng.commit(tx).unwrap();
            ts.persist(&mut eng).unwrap();
            let pts2 = ts.query(&mut eng, s2, 0, 0).unwrap();
            assert_eq!(pts2, vec![(0, 100.0)]);
        }
        cleanup(&path);
    }

    /// Constant-valued telemetry compresses to one f64 per chunk.
    #[test]
    fn constant_compression() {
        let path = tmp_path("const");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            let sid = ts.create_series(&mut eng, &mut tx, "state.flag").unwrap();
            for i in 0..300u64 {
                ts.append(&mut eng, &mut tx, sid, (i * 100) as i64, 42.0)
                    .unwrap();
            }
            ts.flush_series(&mut eng, &mut tx, sid).unwrap();
            eng.commit(tx).unwrap();
            ts.persist(&mut eng).unwrap();
        }
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let (_, min, max) = ts.stats_from_chunks(0).unwrap();
        assert_eq!(min, 42.0);
        assert_eq!(max, 42.0);
        let pts = ts.query(&mut eng, 0, 0, i64::MAX / 2).unwrap();
        assert_eq!(pts.len(), 300);
        assert!(pts.iter().all(|&(_, v)| v == 42.0));
        cleanup(&path);
    }
}

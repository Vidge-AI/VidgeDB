//! Phase 6.2 integration tests: temporal VidgeQL (MEASURE/DURING + RETURN
//! aggregates) joining graph bindings with the Phase 3 time-series store.
//!
//! Spec §23 example:
//! ```text
//! MATCH (m:Motor) MEASURE m.current DURING last(24h) RETURN max(current), avg(current)
//! ```

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::stores::GraphStore;
    use vidgedb::temporal::parse_tquery;
    use vidgedb::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_tq_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    fn build(path: &str, now: i64) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut tx = eng.begin().unwrap();
        // Graph: Motor42 -> mechanical -> Pump17 (spec §58).
        let motor = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[])
            .unwrap();
        let _pump = gs
            .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[])
            .unwrap();
        // Telemetry: Motor42.current ramp 5..15 A over the last 24 h.
        let sid = ts
            .create_series(&mut eng, &mut tx, "Motor42.current")
            .unwrap();
        for i in 0..600i64 {
            let t = now - (600 - i) * 60; // one point per minute
            let v = 5.0 + (i as f64) * 0.01; // 5.00 .. 10.99
            ts.append(&mut eng, &mut tx, sid, t, v).unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        let _ = motor;
    }

    /// The spec §23 hybrid example: graph match + temporal aggregation.
    #[test]
    fn measure_during_aggregates() {
        let path = tmp_path("e2e");
        let now = 1_760_000_000i64;
        build(&path, now);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let q = parse_tquery(
            "MATCH (m:Motor) MEASURE m.current DURING last(24h) RETURN max(current), avg(current), count(current)",
            now,
        )
        .unwrap();
        assert!(q.measure.is_some());
        assert_eq!(q.ret.len(), 3);
        let rows = vidgedb::temporal::execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 1); // one Motor bound
        let r = &rows[0];
        assert!(r.bindings.iter().any(|(v, _)| v == "m"));
        // max = 10.99, avg = (5.00+10.99)/2 = 7.995, count = 600.
        let get = |name: &str| r.aggs.iter().find(|(l, _)| l == name).unwrap().1.value;
        assert_eq!(get("max"), 10.99);
        assert!((get("avg") - 7.995).abs() < 1e-9);
        assert_eq!(get("count"), 600.0);
        cleanup(&path);
    }

    /// Absolute window `t1..t2` selects a slice.
    #[test]
    fn absolute_window_slice() {
        let path = tmp_path("win");
        let now = 1_760_000_000i64;
        build(&path, now);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let from = now - 300 * 60; // last 300 minutes
        let q = parse_tquery(
            &format!(
                "MATCH (m:Motor) MEASURE m.current DURING {}..{} RETURN count(current)",
                from,
                now - 240 * 60
            ),
            now,
        )
        .unwrap();
        let rows = vidgedb::temporal::execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 1);
        let count = rows[0].aggs.iter().find(|(_, _)| true).unwrap().1.value;
        assert_eq!(count, 61.0); // inclusive bounds, 60-minute span + 1
        cleanup(&path);
    }

    /// Entity without telemetry yields no aggregate (binding still returned).
    #[test]
    fn no_series_no_agg() {
        let path = tmp_path("noseries");
        let now = 1_760_000_000i64;
        build(&path, now);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        {
            let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
            // Add a second motor WITHOUT telemetry.
            gs.add_entity(&mut eng, &mut tx, "Motor", "Motor43", &[])
                .unwrap();
            eng.commit(tx).unwrap();
            ts.persist(&mut eng).unwrap();
        }
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let q = parse_tquery(
            "MATCH (m:Motor) MEASURE m.current DURING last(1h) RETURN max(current)",
            now,
        )
        .unwrap();
        let rows = vidgedb::temporal::execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 2); // both motors bound
                                   // Only Motor42 has aggregates.
        let with_agg = rows.iter().filter(|r| !r.aggs.is_empty()).count();
        assert_eq!(with_agg, 1);
        cleanup(&path);
    }
}

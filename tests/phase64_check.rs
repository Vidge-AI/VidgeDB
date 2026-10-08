//! Phase 6.4 integration tests: CHECK/FOR deviation engine (spec §25).
//!
//! ```text
//! EXPECTED -> OBSERVED -> COMPARE -> DEVIATION
//! ```
//!
//! Expected values are entity properties (`spec.<signal>.max`), observations
//! live in the Phase 3 time-series store (`<entity_name>.<signal>` series).
//! The free function `check_entity` is the priority API; `parse_check` +
//! `execute_check` add the spec §25 VidgeQL statement shape.

#[cfg(test)]
mod tests {
    use vidgedb::check::{check_entity, execute_check, parse_check, CheckStatus};
    use vidgedb::engine::Engine;
    use vidgedb::model::Provenance;
    use vidgedb::stores::GraphStore;
    use vidgedb::temporal::parse_last;
    use vidgedb::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_p64_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    const NOW: i64 = 1_760_000_000;

    /// Build a graph + telemetry fixture and reopen it (committed state).
    ///
    /// - MotorOK:  spec current max = 10 A; readings 9.0 / 10.0 (within spec,
    ///   one point exactly at the limit — strict inequality keeps it OK).
    /// - MotorBad: spec current max = 10 A; readings 8.0 / 12.5 (peak 12.5).
    /// - MotorNoSpec: current readings 5.0 but NO spec property.
    /// - MotorNoSeries: spec property but NO series at all.
    fn build(path: &str) -> (u32, u32, u32, u32) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut tx = eng.begin().unwrap();
        let ok = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "MotorOK",
                &[("spec.current.max", "10"), ("spec.current.unit", "A")],
            )
            .unwrap();
        let bad = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "MotorBad",
                &[("spec.current.max", "10")],
            )
            .unwrap();
        let nospec = gs
            .add_entity(&mut eng, &mut tx, "Motor", "MotorNoSpec", &[])
            .unwrap();
        let noseries = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "MotorNoSeries",
                &[("spec.current.max", "10")],
            )
            .unwrap();
        let s_ok = ts
            .create_series(&mut eng, &mut tx, "MotorOK.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s_ok, NOW - 600, 9.0).unwrap();
        ts.append(&mut eng, &mut tx, s_ok, NOW - 300, 10.0).unwrap();
        let s_bad = ts
            .create_series(&mut eng, &mut tx, "MotorBad.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s_bad, NOW - 600, 8.0).unwrap();
        ts.append(&mut eng, &mut tx, s_bad, NOW - 300, 12.5)
            .unwrap();
        let s_ns = ts
            .create_series(&mut eng, &mut tx, "MotorNoSpec.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s_ns, NOW - 300, 5.0).unwrap();
        for s in [s_ok, s_bad, s_ns] {
            ts.flush_series(&mut eng, &mut tx, s).unwrap();
        }
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        (ok, bad, nospec, noseries)
    }

    /// Reopen the fixture fresh (checks persistence of the CHECK inputs).
    fn reopen(path: &str) -> (Engine, GraphStore, TimeSeriesStore) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        (eng, gs, ts)
    }

    /// Spec §25 conformant reading: observed max <= expected max => OK,
    /// deviation = observed - expected (negative or zero), provenance kept.
    #[test]
    fn check_conformant_series_is_ok() {
        let path = tmp_path("ok");
        cleanup(&path);
        let (ok, _, _, _) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);
        let win = parse_last("1h", NOW).unwrap();
        let r = check_entity(&mut eng, &mut gs, &mut ts, ok, "current", win).unwrap();
        assert_eq!(r.status, CheckStatus::Ok);
        assert_eq!(r.entity_name, "MotorOK");
        assert_eq!(r.expected_max, Some(10.0));
        assert_eq!(r.observed, Some(10.0)); // worst case = window max
        assert_eq!(r.deviation, Some(0.0)); // strict: equal is still OK
        assert_eq!(r.points_checked, 2);
        // Provenance preserved: expected=Specification, observed=Observation.
        assert_eq!(r.expected_prov, Some(Provenance::Specification));
        assert_eq!(r.observed_prov, Some(Provenance::Observation));
        assert_eq!(r.unit, Some("A".to_string()));
        cleanup(&path);
    }

    /// Spec §25 violation example shape: observed > expected => VIOLATION
    /// with the exact deviation (observed - expected_max).
    #[test]
    fn check_overshoot_is_violation_with_exact_deviation() {
        let path = tmp_path("viol");
        cleanup(&path);
        let (_, bad, _, _) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);
        let win = parse_last("1h", NOW).unwrap();
        let r = check_entity(&mut eng, &mut gs, &mut ts, bad, "current", win).unwrap();
        assert_eq!(r.status, CheckStatus::Violation);
        assert_eq!(r.observed, Some(12.5)); // worst case, not the average
        assert_eq!(r.deviation, Some(2.5)); // 12.5 - 10, exact
        assert_eq!(r.expected_max, Some(10.0));
        // §24-style negative deviation inside spec (expected 24, read 21.8):
        let path2 = tmp_path("neg");
        cleanup(&path2);
        let mut eng2 = Engine::open(&path2).unwrap();
        let mut gs2 = GraphStore::open(&mut eng2).unwrap();
        let mut ts2 = TimeSeriesStore::open(&mut eng2, &mut gs2).unwrap();
        let mut tx = eng2.begin().unwrap();
        let plc = gs2
            .add_entity(
                &mut eng2,
                &mut tx,
                "PLC",
                "PLC01",
                &[("spec.voltage.max", "24")],
            )
            .unwrap();
        let s = ts2
            .create_series(&mut eng2, &mut tx, "PLC01.voltage")
            .unwrap();
        ts2.append(&mut eng2, &mut tx, s, NOW - 60, 21.8).unwrap();
        ts2.flush_series(&mut eng2, &mut tx, s).unwrap();
        gs2.persist(&mut eng2).unwrap();
        eng2.commit(tx).unwrap();
        ts2.persist(&mut eng2).unwrap();
        let r2 = check_entity(&mut eng2, &mut gs2, &mut ts2, plc, "voltage", win).unwrap();
        assert_eq!(r2.status, CheckStatus::Ok);
        assert!((r2.deviation.unwrap() - (-2.2)).abs() < 1e-9); // spec §25 example value
        cleanup(&path2);
        cleanup(&path);
    }

    /// Spec present but no series / no points in the window => NO_DATA.
    #[test]
    fn check_missing_series_is_no_data() {
        let path = tmp_path("nodata");
        cleanup(&path);
        let (_, _, _, noseries) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);
        let win = parse_last("1h", NOW).unwrap();
        let r = check_entity(&mut eng, &mut gs, &mut ts, noseries, "current", win).unwrap();
        assert_eq!(r.status, CheckStatus::NoData);
        assert_eq!(r.expected_max, Some(10.0)); // spec known...
        assert_eq!(r.observed, None); // ...nothing observed
        assert_eq!(r.deviation, None);
        assert_eq!(r.points_checked, 0);
        // Same status when the series exists but the window misses the points.
        let old_win = parse_last("1h", NOW - 86_400).unwrap(); // yesterday
        let r2 = check_entity(&mut eng, &mut gs, &mut ts, noseries, "current", old_win).unwrap();
        assert_eq!(r2.status, CheckStatus::NoData);
        cleanup(&path);
    }

    /// No `spec.<signal>.max` property => NO_SPEC (observations still read).
    #[test]
    fn check_missing_spec_is_no_spec() {
        let path = tmp_path("nospec");
        cleanup(&path);
        let (_, _, nospec, _) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);
        let win = parse_last("1h", NOW).unwrap();
        let r = check_entity(&mut eng, &mut gs, &mut ts, nospec, "current", win).unwrap();
        assert_eq!(r.status, CheckStatus::NoSpec);
        assert_eq!(r.expected_max, None);
        assert_eq!(r.expected_prov, None);
        assert_eq!(r.observed, Some(5.0)); // observations are still reported
        assert_eq!(r.deviation, None);
        cleanup(&path);
    }

    /// The spec §25 example as a VidgeQL statement:
    /// CHECK m.current FOR (m:Motor) WHERE m.name = "..." DURING last(1h)
    /// RETURN status, deviation — parses and executes over the fixture.
    #[test]
    fn check_statement_parses_and_executes() {
        let path = tmp_path("stmt");
        cleanup(&path);
        let (_, _, _, _) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);
        let q = parse_check(
            "CHECK m.current FOR (m:Motor) WHERE m.name = \"MotorBad\" DURING last(1h) RETURN status, deviation, expected, observed",
            NOW,
        )
        .unwrap();
        assert_eq!(q.var, "m");
        assert_eq!(q.signal, "current");
        assert_eq!(
            q.ret_fields,
            vec!["status", "deviation", "expected", "observed"]
        );
        let rows = execute_check(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.result.status, CheckStatus::Violation);
        assert_eq!(row.result.deviation, Some(2.5));
        let get = |name: &str| {
            row.fields
                .iter()
                .find(|(f, _)| f == name)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        assert_eq!(get("status"), "VIOLATION");
        assert_eq!(get("deviation"), "2.5");
        assert_eq!(get("expected"), "10");
        assert_eq!(get("observed"), "12.5");
        // §25 output carries expected/observed/status (provenance classes on
        // the CheckResult are Specification/Observation).
        assert_eq!(row.result.expected_prov, Some(Provenance::Specification));
        assert_eq!(row.result.observed_prov, Some(Provenance::Observation));
        cleanup(&path);
    }

    /// CHECK without WHERE binds every entity matching the FOR pattern, each
    /// row carrying its own verdict.
    #[test]
    fn check_statement_without_where_returns_all_verdicts() {
        let path = tmp_path("all");
        cleanup(&path);
        let (_, _, _, _) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);
        let q = parse_check(
            "CHECK m.current FOR (m:Motor) DURING last(1h) RETURN status",
            NOW,
        )
        .unwrap();
        let rows = execute_check(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 4);
        let mut statuses: Vec<(String, String)> = rows
            .iter()
            .map(|r| {
                (
                    r.result.entity_name.clone(),
                    r.fields[0].1.clone(), // RETURN status only
                )
            })
            .collect();
        statuses.sort();
        assert_eq!(
            statuses,
            vec![
                ("MotorBad".to_string(), "VIOLATION".to_string()),
                ("MotorNoSeries".to_string(), "NO_DATA".to_string()),
                ("MotorNoSpec".to_string(), "NO_SPEC".to_string()),
                ("MotorOK".to_string(), "OK".to_string()),
            ]
        );
        cleanup(&path);
    }
}

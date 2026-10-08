//! Phase 7.1 integration tests: DIAGNOSE high-level diagnostic query
//! (spec §26) — the 8-step pipeline over the spec §58 machine.
//!
//! Fixture: PLC01 -network-> Drive12 -electrical-> Motor42
//!          -mechanical-> Pump17, with telemetry on Motor42:
//!   - `current`   (spec max 10 A) peaks at 12.5  -> VIOLATION;
//!   - `vibration` (spec max 5.0)  stays at 3.0   -> OK.
//!
//! The report must contain the component, its resolved dependencies, the
//! measurements, one VIOLATION + one OK check, exactly one anomaly and one
//! candidate causal hypothesis (provenance Hypothesis, upstream = Drive12).
//! A second entity without series must yield empty measurements and no
//! anomaly. Hypotheses are report outputs only: no store may change.

#[cfg(test)]
mod tests {
    use vidgedb::diagnose::{diagnose, ProvClass};
    use vidgedb::engine::Engine;
    use vidgedb::stores::GraphStore;
    use vidgedb::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_p71_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    const NOW: i64 = 1_760_000_000;
    /// last(30m) as an absolute span (spec §26 example window).
    const FROM: i64 = NOW - 1800;
    const TO: i64 = NOW;

    /// Spec §58 machine + Motor42 telemetry, persisted and committed.
    /// Returns (plc, drive, motor, pump) entity keys.
    fn build(path: &str) -> (u32, u32, u32, u32) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut tx = eng.begin().unwrap();
        let plc = gs
            .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[("vendor", "Siemens")])
            .unwrap();
        let drive = gs
            .add_entity(&mut eng, &mut tx, "Drive", "Drive12", &[])
            .unwrap();
        let motor = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "Motor42",
                &[("spec.current.max", "10"), ("spec.vibration.max", "5.0")],
            )
            .unwrap();
        let pump = gs
            .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[])
            .unwrap();
        // PLC01 -[:network:profinet]-> Drive12 -[:electrical:feeds]-> Motor42
        // Motor42 -[:mechanical:drives]-> Pump17
        gs.add_relation(&mut eng, &mut tx, plc, drive, "network:profinet", 0, -1, 0)
            .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            drive,
            motor,
            "electrical:feeds",
            0,
            -1,
            0,
        )
        .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            motor,
            pump,
            "mechanical:drives",
            0,
            -1,
            0,
        )
        .unwrap();
        // Telemetry: current overshoots the spec (peak 12.5 > 10),
        // vibration stays within spec (<= 5.0).
        let s_cur = ts
            .create_series(&mut eng, &mut tx, "Motor42.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s_cur, NOW - 1200, 8.0)
            .unwrap();
        ts.append(&mut eng, &mut tx, s_cur, NOW - 600, 12.5)
            .unwrap();
        let s_vib = ts
            .create_series(&mut eng, &mut tx, "Motor42.vibration")
            .unwrap();
        ts.append(&mut eng, &mut tx, s_vib, NOW - 1200, 2.0)
            .unwrap();
        ts.append(&mut eng, &mut tx, s_vib, NOW - 600, 3.0).unwrap();
        // A foreign series that must NOT appear in Motor42's measurements.
        let s_other = ts
            .create_series(&mut eng, &mut tx, "Pump17.pressure")
            .unwrap();
        ts.append(&mut eng, &mut tx, s_other, NOW - 600, 1.5)
            .unwrap();
        for s in [s_cur, s_vib, s_other] {
            ts.flush_series(&mut eng, &mut tx, s).unwrap();
        }
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
        (plc, drive, motor, pump)
    }

    fn reopen(path: &str) -> (Engine, GraphStore, TimeSeriesStore) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        (eng, gs, ts)
    }

    /// Full report on the violating motor: all 8 steps present, provenance
    /// preserved, one anomaly + one hypothesis with upstream names.
    #[test]
    fn diagnose_motor42_full_report() {
        let path = tmp_path("full");
        cleanup(&path);
        let (_, drive, motor, pump) = build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);

        let r = diagnose(&mut eng, &mut gs, &mut ts, "Motor42", FROM, TO).unwrap();

        // -- Step 1: identify component ----------------------------------
        let comp = r.component.as_ref().expect("Motor42 must resolve");
        assert_eq!(comp.key, motor);
        assert_eq!(comp.name, "Motor42");
        assert_eq!(comp.type_name, "Motor");
        assert_eq!(comp.provenance, ProvClass::Observed);
        assert!(comp
            .props
            .iter()
            .any(|p| p.name == "spec.current.max" && p.value == "10"));

        // -- Step 2: identify dependencies -------------------------------
        // Amont (relations_in): Drive12 feeds Motor42.
        assert_eq!(r.dependencies.upstream.len(), 1);
        let up = &r.dependencies.upstream[0];
        assert_eq!(up.name, "Drive12");
        assert_eq!(up.key, drive);
        assert_eq!(up.type_name, "Drive");
        assert_eq!(up.topology, "electrical");
        assert_eq!(up.relation_type, "feeds");
        assert_eq!(up.relation_provenance, "Fact");
        // Aval (relations_out): Motor42 drives Pump17.
        assert_eq!(r.dependencies.downstream.len(), 1);
        let down = &r.dependencies.downstream[0];
        assert_eq!(down.name, "Pump17");
        assert_eq!(down.key, pump);
        assert_eq!(down.topology, "mechanical");
        assert_eq!(down.relation_type, "drives");

        // -- Step 3: inspect topology ------------------------------------
        assert_eq!(r.topology.len(), 2);
        let elec = r
            .topology
            .iter()
            .find(|t| t.topology == "electrical")
            .expect("electrical topology");
        assert_eq!(elec.relation_types, vec!["feeds"]);
        assert_eq!(elec.edge_count, 1);
        let mech = r
            .topology
            .iter()
            .find(|t| t.topology == "mechanical")
            .expect("mechanical topology");
        assert_eq!(mech.relation_types, vec!["drives"]);

        // -- Step 4: inspect events (v0: empty + explicit note) ----------
        assert!(r.events.is_empty());
        assert!(r.events_note.contains("no event store"));

        // -- Step 5: inspect measurements --------------------------------
        assert_eq!(r.measurements.len(), 2);
        let cur = r
            .measurements
            .iter()
            .find(|m| m.signal == "current")
            .expect("current measurement");
        assert_eq!(cur.series, "Motor42.current");
        assert_eq!(cur.points, 2);
        assert_eq!(cur.min, Some(8.0));
        assert_eq!(cur.max, Some(12.5));
        assert_eq!(cur.provenance, ProvClass::Observed);
        let vib = r
            .measurements
            .iter()
            .find(|m| m.signal == "vibration")
            .expect("vibration measurement");
        assert_eq!(vib.points, 2);
        assert_eq!(vib.max, Some(3.0));
        // Pump17.pressure must NOT leak into Motor42's measurements.
        assert!(!r.measurements.iter().any(|m| m.signal == "pressure"));

        // -- Step 6: compare specifications ------------------------------
        assert_eq!(r.checks.len(), 2);
        let cur_check = r
            .checks
            .iter()
            .find(|c| c.signal == "current")
            .expect("current check");
        assert_eq!(cur_check.status, "VIOLATION");
        assert_eq!(cur_check.expected_max, Some(10.0));
        assert_eq!(cur_check.observed, Some(12.5));
        assert_eq!(cur_check.deviation, Some(2.5));
        assert_eq!(cur_check.expected_prov.as_deref(), Some("Specification"));
        assert_eq!(cur_check.observed_prov.as_deref(), Some("Observation"));
        let vib_check = r
            .checks
            .iter()
            .find(|c| c.signal == "vibration")
            .expect("vibration check");
        assert_eq!(vib_check.status, "OK");
        assert_eq!(vib_check.deviation, Some(-2.0));

        // -- Step 7: identify anomalies ----------------------------------
        assert_eq!(r.anomalies.len(), 1);
        assert_eq!(r.anomalies[0].signal, "current");
        assert_eq!(r.anomalies[0].deviation, Some(2.5));

        // -- Step 8: candidate causal paths (hypotheses ONLY) ------------
        assert_eq!(r.causal_paths.len(), 1);
        let h = &r.causal_paths[0];
        assert!(h.hypothesis.contains("current"));
        assert!(h.hypothesis.contains("exceeded expected max"));
        assert_eq!(h.confidence, 0.5);
        assert_eq!(h.provenance, "Hypothesis");
        assert_eq!(h.source, "vidgedb_diagnose_v0");
        assert_eq!(h.upstream, vec!["Drive12".to_string()]);

        // Report-level provenance policy is spelled out.
        assert!(r.provenance_note.contains("provenance"));
        assert!(r.provenance_note.contains("never promoted"));
        assert_eq!(r.window.from, FROM);
        assert_eq!(r.window.to, TO);

        // The report serializes to JSON (AI-native consumption, spec §27).
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["component"]["name"], "Motor42");
        assert_eq!(json["anomalies"][0]["signal"], "current");
        assert_eq!(json["causal_paths"][0]["provenance"], "Hypothesis");
        assert_eq!(json["causal_paths"][0]["upstream"][0], "Drive12");
        assert_eq!(json["measurements"][0]["provenance"], "observed");
    }

    /// Hypotheses are report outputs only (spec §29): diagnose must not
    /// write anything — relation slab, entity slab, series and point counts
    /// are unchanged, and two runs return identical reports.
    #[test]
    fn diagnose_is_read_only_and_hypotheses_never_persisted() {
        let path = tmp_path("readonly");
        cleanup(&path);
        build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);

        let rels_before = gs.relations.len();
        let ents_before = gs.entity_count();
        let series_before = ts.series.len();
        let points_before: u64 = ts.series.iter().map(|s| s.total_points as u64).sum();

        let r1 = diagnose(&mut eng, &mut gs, &mut ts, "Motor42", FROM, TO).unwrap();
        let r2 = diagnose(&mut eng, &mut gs, &mut ts, "Motor42", FROM, TO).unwrap();

        assert_eq!(r1, r2, "diagnose must be deterministic and read-only");
        assert_eq!(gs.relations.len(), rels_before);
        assert_eq!(gs.entity_count(), ents_before);
        assert_eq!(ts.series.len(), series_before);
        assert_eq!(
            ts.series.iter().map(|s| s.total_points as u64).sum::<u64>(),
            points_before
        );
        // No hypothesis entity/series was created behind the scenes.
        assert!(!ts.series.iter().any(|s| s.name.contains("hypothes")));
    }

    /// Unknown entity: structured "not found" — empty sections, no error.
    #[test]
    fn diagnose_unknown_entity_is_structured_empty() {
        let path = tmp_path("unknown");
        cleanup(&path);
        build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);

        let r = diagnose(&mut eng, &mut gs, &mut ts, "Motor99", FROM, TO).unwrap();
        assert!(r.component.is_none());
        assert!(r.dependencies.upstream.is_empty());
        assert!(r.dependencies.downstream.is_empty());
        assert!(r.topology.is_empty());
        assert!(r.measurements.is_empty());
        assert!(r.checks.is_empty());
        assert!(r.anomalies.is_empty());
        assert!(r.causal_paths.is_empty());
        assert!(r.events.is_empty());
        assert_eq!(r.entity, "Motor99");
    }

    /// Entities with no telemetry of their own: no measurements, hence no
    /// checks / anomalies / hypotheses. Two shapes are covered:
    /// - Pump17 has a downstream measurement series in the fixture
    ///   (`Pump17.pressure`), so it shows exactly one measurement;
    /// - Drive12 has NO series at all — measurements fully empty.
    #[test]
    fn diagnose_entity_without_series_has_no_measurements_no_anomalies() {
        let path = tmp_path("noseries");
        cleanup(&path);
        build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);

        let r = diagnose(&mut eng, &mut gs, &mut ts, "Pump17", FROM, TO).unwrap();
        let comp = r.component.as_ref().expect("Pump17 must resolve");
        assert_eq!(comp.name, "Pump17");
        assert_eq!(comp.type_name, "Pump");
        // Aval is empty (Pump17 drives nothing), amont holds Motor42.
        assert!(r.dependencies.downstream.is_empty());
        assert_eq!(r.dependencies.upstream.len(), 1);
        assert_eq!(r.dependencies.upstream[0].name, "Motor42");
        // Pump17.pressure is measured but has no spec prop: one measurement,
        // no checks, no anomalies, no hypotheses.
        assert_eq!(r.measurements.len(), 1);
        assert_eq!(r.measurements[0].signal, "pressure");
        assert!(r.checks.is_empty());
        assert!(r.anomalies.is_empty());
        assert!(r.causal_paths.is_empty());

        // Drive12: strictly no series -> empty measurements, no anomalies.
        // Topology: PLC01 -network-> Drive12 -electrical-> Motor42.
        let r2 = diagnose(&mut eng, &mut gs, &mut ts, "Drive12", FROM, TO).unwrap();
        assert_eq!(
            r2.component.as_ref().expect("Drive12 resolves").name,
            "Drive12"
        );
        assert!(r2.measurements.is_empty());
        assert!(r2.checks.is_empty());
        assert!(r2.anomalies.is_empty());
        assert!(r2.causal_paths.is_empty());
        assert_eq!(r2.dependencies.upstream.len(), 1);
        assert_eq!(r2.dependencies.upstream[0].name, "PLC01");
        assert_eq!(r2.dependencies.upstream[0].topology, "network");
        assert_eq!(r2.dependencies.downstream.len(), 1);
        assert_eq!(r2.dependencies.downstream[0].name, "Motor42");
        assert_eq!(r2.dependencies.downstream[0].topology, "electrical");
    }

    /// Window filtering: points outside [from, to] are excluded from the
    /// measurement stats and from the comparison (spec §26 DURING).
    #[test]
    fn diagnose_window_filters_points() {
        let path = tmp_path("window");
        cleanup(&path);
        build(&path);
        let (mut eng, mut gs, mut ts) = reopen(&path);

        // Window covering only the 8.0 current point (no violation).
        let r = diagnose(&mut eng, &mut gs, &mut ts, "Motor42", NOW - 1500, NOW - 900).unwrap();
        let cur = r
            .measurements
            .iter()
            .find(|m| m.signal == "current")
            .expect("current measurement");
        assert_eq!(cur.points, 1);
        assert_eq!(cur.max, Some(8.0));
        assert!(r.anomalies.is_empty());
        assert!(r.causal_paths.is_empty());
        assert_eq!(
            r.checks
                .iter()
                .find(|c| c.signal == "current")
                .unwrap()
                .status,
            "OK"
        );
    }
}

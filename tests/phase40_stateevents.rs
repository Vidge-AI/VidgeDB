//! Phase 4 integration tests: StateEventStore end-to-end over the engine —
//! spec §10 state pairs with bitemporal history and spec §15 events with
//! provenance. Everything is verified against a FRESHLY REOPENED database
//! (reconstruction from the slabs — same-session reads after commit are
//! stale, a known Phase-2 engine bug class).

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::statestore::{StateEventStore, DETAILS_INLINE_MAX};
    use vidgedb::stores::GraphStore;
    use vidgedb::ttemporal::VALID_TO_OPEN;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_p40_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    /// Open (eng, gs, se) fresh on the committed database.
    fn reopen(path: &str) -> (Engine, GraphStore, StateEventStore) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let se = StateEventStore::open(&mut eng, &mut gs).unwrap();
        (eng, gs, se)
    }

    // -----------------------------------------------------------------------
    // Spec §10: state pairs + history
    // -----------------------------------------------------------------------

    /// set_state x3 at t=100/200/300; then everything is read back from a
    /// REOPENED file: current state (at=None), point-in-time resolution,
    /// and the full history with [100,200) [200,300) [300,open) windows.
    #[test]
    fn state_history_and_point_in_time_reopen() {
        let path = tmp_path("state");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "STOPPED", 100)
                .unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "RUNNING", 200)
                .unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "OVERLOAD", 300)
                .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        let (mut eng, _gs, mut se) = reopen(&path);
        // Current state (at = None -> now) = the t=300 value, and the
        // returned valid_from anchors it to the last write.
        let (value, from) = se
            .get_state(&mut eng, "Motor42", "state", None)
            .unwrap()
            .expect("current state must exist");
        assert_eq!(value, "OVERLOAD");
        assert_eq!(from, 300);
        // Point-in-time: at=250 falls inside the [200,300) window.
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(250))
                .unwrap(),
            Some(("RUNNING".to_string(), 200))
        );
        // Full history survives the reopen (reconstructed from the slabs):
        // 3 entries, windows [100,200), [200,300), [300, open).
        let hist = se.state_history(&mut eng, "Motor42", "state").unwrap();
        assert_eq!(hist.len(), 3, "history: {:?}", hist);
        assert_eq!(
            hist,
            vec![
                ("STOPPED".to_string(), 100, 200),
                ("RUNNING".to_string(), 200, 300),
                ("OVERLOAD".to_string(), 300, VALID_TO_OPEN),
            ]
        );
        // Half-open boundary semantics (shared convention, spec §9): the
        // closing instant belongs to the NEXT pair, not to the closed one.
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(100))
                .unwrap(),
            Some(("STOPPED".to_string(), 100))
        );
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(200))
                .unwrap(),
            Some(("RUNNING".to_string(), 200))
        );
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(300))
                .unwrap(),
            Some(("OVERLOAD".to_string(), 300))
        );
        // Before the first pair and for unknown keys: no state.
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(99))
                .unwrap(),
            None
        );
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "fault", Some(150))
                .unwrap(),
            None
        );
        assert_eq!(
            se.get_state(&mut eng, "Nobody", "state", None).unwrap(),
            None
        );
        // Several keys per entity are independent histories.
        let mut tx = eng.begin().unwrap();
        se.set_state(&mut eng, &mut tx, "Motor42", "enabled", "true", 150)
            .unwrap();
        eng.commit(tx).unwrap();
        se.persist(&mut eng).unwrap();
        let (mut eng2, _gs2, se2) = reopen(&path);
        let (v, f) = se2
            .get_state(&mut eng2, "Motor42", "enabled", None)
            .unwrap()
            .unwrap();
        assert_eq!((v.as_str(), f), ("true", 150));
        // …and the `state` history is untouched by the second key.
        assert_eq!(
            se2.state_history(&mut eng2, "Motor42", "state")
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            se2.get_state(&mut eng2, "Motor42", "state", Some(250))
                .unwrap(),
            Some(("RUNNING".to_string(), 200))
        );
        cleanup(&path);
    }

    /// Backdated writes: inserting a pair BEFORE the current open pair
    /// still preserves every existing pair (append-only history) and the
    /// point-in-time query resolves overlaps by most-recent valid_from.
    #[test]
    fn backdated_write_preserves_history() {
        let path = tmp_path("backdated");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            se.set_state(&mut eng, &mut tx, "V1", "pos", "OPEN", 300)
                .unwrap();
            // A late-arriving correction at t=100 (the pair [300, open)
            // stays in the slab — nothing is overwritten).
            se.set_state(&mut eng, &mut tx, "V1", "pos", "CLOSING", 100)
                .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        let (mut eng, _gs, se) = reopen(&path);
        let hist = se.state_history(&mut eng, "V1", "pos").unwrap();
        assert_eq!(hist.len(), 2, "nothing is erased: {:?}", hist);
        assert_eq!(
            hist,
            vec![
                ("OPEN".to_string(), 300, 100),
                ("CLOSING".to_string(), 100, VALID_TO_OPEN),
            ]
        );
        // The correction wins from t=100 (nothing else covers [100,∞)):
        // set_state closes the previously-open pair mechanically, so the
        // backdated write left [300,100) — an empty window — but BOTH
        // pairs stay in the slab (nothing erased, append-only history).
        assert_eq!(
            se.get_state(&mut eng, "V1", "pos", Some(150)).unwrap(),
            Some(("CLOSING".to_string(), 100))
        );
        assert_eq!(
            se.get_state(&mut eng, "V1", "pos", Some(400)).unwrap(),
            Some(("CLOSING".to_string(), 100))
        );
        assert_eq!(
            se.get_state(&mut eng, "V1", "pos", Some(299)).unwrap(),
            Some(("CLOSING".to_string(), 100))
        );
        // Before the correction: nothing covers t<100.
        assert_eq!(se.get_state(&mut eng, "V1", "pos", Some(50)).unwrap(), None);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // Spec §15: events
    // -----------------------------------------------------------------------

    /// log_event xN across two entities; get_events filters by entity and
    /// by window; the provenance byte and details round-trip; all read
    /// back from a REOPENED file.
    #[test]
    fn events_filter_window_provenance_reopen() {
        let path = tmp_path("events");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            se.log_event(
                &mut eng,
                &mut tx,
                "MotorStarted",
                "Motor42",
                100,
                5,
                "auto mode",
            )
            .unwrap();
            se.log_event(
                &mut eng,
                &mut tx,
                "AlarmRaised",
                "Motor42",
                200,
                1,
                "overcurrent",
            )
            .unwrap();
            se.log_event(&mut eng, &mut tx, "ValveOpened", "Pump17", 250, 0, "")
                .unwrap();
            se.log_event(
                &mut eng,
                &mut tx,
                "MotorStopped",
                "Motor42",
                300,
                5,
                "manual stop",
            )
            .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        let (mut eng, _gs, mut se) = reopen(&path);
        assert_eq!(se.event_count(), 4);
        // All entities, full window: 4 events, sorted by timestamp.
        let all = se.get_events(&mut eng, None, 0, 1000).unwrap();
        assert_eq!(all.len(), 4);
        let stamps: Vec<i64> = all.iter().map(|e| e.timestamp).collect();
        assert_eq!(stamps, vec![100, 200, 250, 300]);
        // Entity filter: Motor42 only (3), Pump17 only (1).
        let m42 = se.get_events(&mut eng, Some("Motor42"), 0, 1000).unwrap();
        assert_eq!(m42.len(), 3);
        assert!(m42.iter().all(|e| e.entity == "Motor42"));
        let p17 = se.get_events(&mut eng, Some("Pump17"), 0, 1000).unwrap();
        assert_eq!(p17.len(), 1);
        assert_eq!(p17[0].name, "ValveOpened");
        assert_eq!(p17[0].provenance, 0); // Fact
                                          // Unknown entity: empty.
        assert!(se
            .get_events(&mut eng, Some("Nope"), 0, 1000)
            .unwrap()
            .is_empty());
        // Window filter is inclusive on both bounds: [150, 250] catches
        // t=200 and t=250 but not t=100 / t=300.
        let win = se.get_events(&mut eng, None, 150, 250).unwrap();
        assert_eq!(
            win.iter().map(|e| e.timestamp).collect::<Vec<i64>>(),
            vec![200, 250]
        );
        let edge = se.get_events(&mut eng, None, 200, 200).unwrap();
        assert_eq!(edge.len(), 1);
        // Provenance byte round-trip (Event=5 on the Motor events) and
        // details round-trip.
        let started = &se.get_events(&mut eng, Some("Motor42"), 0, 150).unwrap()[0];
        assert_eq!(started.name, "MotorStarted");
        assert_eq!(started.provenance, 5);
        assert_eq!(started.details, "auto mode");
        let stopped = &se.get_events(&mut eng, Some("Motor42"), 250, 400).unwrap()[0];
        assert_eq!(stopped.name, "MotorStopped");
        assert_eq!(stopped.provenance, 5);
        assert_eq!(stopped.details, "manual stop");
        // Empty details stay empty.
        assert_eq!(p17[0].details, "");
        // Every spec §14 provenance class value 0..=7 survives the wire.
        let mut tx = eng.begin().unwrap();
        for p in 0u8..=7 {
            se.log_event(
                &mut eng,
                &mut tx,
                "Probe",
                "Probe",
                500 + p as i64,
                p,
                "prov",
            )
            .unwrap();
        }
        eng.commit(tx).unwrap();
        se.persist(&mut eng).unwrap();
        let (mut eng2, _gs2, se2) = reopen(&path);
        let probes = se2.get_events(&mut eng2, Some("Probe"), 500, 600).unwrap();
        assert_eq!(probes.len(), 8);
        for (i, e) in probes.iter().enumerate() {
            assert_eq!(e.provenance, i as u8, "provenance byte round-trip");
        }
        cleanup(&path);
    }

    /// Event details longer than DETAILS_INLINE_MAX bytes are stored
    /// truncated (at a UTF-8 char boundary) and survive the reopen.
    #[test]
    fn event_details_truncated_but_roundtrip() {
        let path = tmp_path("details");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            let long = "d".repeat(200);
            se.log_event(
                &mut eng,
                &mut tx,
                "ConfigurationChanged",
                "PLC01",
                42,
                7,
                &long,
            )
            .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        let (mut eng, _gs, se) = reopen(&path);
        let ev = &se.get_events(&mut eng, Some("PLC01"), 0, 100).unwrap()[0];
        assert_eq!(ev.name, "ConfigurationChanged");
        assert_eq!(ev.details.len(), DETAILS_INLINE_MAX);
        assert_eq!(ev.details, "d".repeat(DETAILS_INLINE_MAX));
        cleanup(&path);
    }

    /// THE reopen test: state pairs, history windows and events all
    /// survive a full close/reopen (reconstruction from the slabs), and
    /// the graph meta still points at the same SE layout page (single
    /// source of truth, `se_meta_page`).
    #[test]
    fn full_reopen_state_history_events_survive() {
        let path = tmp_path("reopen");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "OFF", 10)
                .unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "RUN", 20)
                .unwrap();
            se.set_state(&mut eng, &mut tx, "Pump17", "state", "IDLE", 15)
                .unwrap();
            se.log_event(&mut eng, &mut tx, "MotorStarted", "Motor42", 20, 5, "s1")
                .unwrap();
            se.log_event(&mut eng, &mut tx, "AlarmRaised", "Pump17", 25, 1, "dry run")
                .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        // Reopen 1.
        let (mut eng, _gs, se) = reopen(&path);
        assert_eq!(se.state_count(), 3);
        assert_eq!(se.event_count(), 2);
        let h = se.state_history(&mut eng, "Motor42", "state").unwrap();
        assert_eq!(
            h,
            vec![
                ("OFF".to_string(), 10, 20),
                ("RUN".to_string(), 20, VALID_TO_OPEN),
            ]
        );
        let cur = se
            .get_state(&mut eng, "Motor42", "state", Some(25))
            .unwrap();
        assert_eq!(cur, Some(("RUN".to_string(), 20)));
        let evs = se.get_events(&mut eng, None, 0, 100).unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].name, "MotorStarted");
        assert_eq!(evs[1].name, "AlarmRaised");
        assert_eq!(evs[1].provenance, 1);
        // Reopen 2: the reconstruction is stable (idempotent across opens).
        let (mut eng3, _gs3, mut se3) = reopen(&path);
        assert_eq!(se3.state_count(), 3);
        assert_eq!(se3.event_count(), 2);
        assert_eq!(se3.state_history(&mut eng3, "Motor42", "state").unwrap(), h);
        assert_eq!(
            se3.get_events(&mut eng3, None, 0, 100).unwrap(),
            se.get_events(&mut eng, None, 0, 100).unwrap()
        );
        // A second session continues the history append-only.
        let mut tx = eng3.begin().unwrap();
        se3.set_state(&mut eng3, &mut tx, "Motor42", "state", "FAULT", 30)
            .unwrap();
        eng3.commit(tx).unwrap();
        se3.persist(&mut eng3).unwrap();
        let (mut eng4, _gs4, se4) = reopen(&path);
        assert_eq!(se4.state_count(), 4);
        let h4 = se4.state_history(&mut eng4, "Motor42", "state").unwrap();
        assert_eq!(
            h4,
            vec![
                ("OFF".to_string(), 10, 20),
                ("RUN".to_string(), 20, 30),
                ("FAULT".to_string(), 30, VALID_TO_OPEN),
            ]
        );
        // The earlier history entries are preserved — only RUN's window
        // was closed by the new write, exactly as set_state contracts.
        assert_eq!(h4[0], h[0]);
        assert_eq!(h4[1].0, h[1].0);
        assert_eq!(h4[1].1, h[1].1);
        assert_eq!(h4[1].2, 30);
        assert_eq!(
            se4.get_state(&mut eng4, "Motor42", "state", Some(25))
                .unwrap(),
            Some(("RUN".to_string(), 20))
        );
        cleanup(&path);
    }

    /// The SE layout page pointer lives in the graph Meta (`se_meta_page`,
    /// 11th field): reopening the GRAPH alone gives a Meta that, after the
    /// SE store opens, resolves to the same layout page — and legacy
    /// Meta-bytes (0 in the field) decode as NULL_PAGE, not page 0
    /// (invariant 1: heads/pointers are never 0).
    #[test]
    fn se_meta_page_is_single_source_of_truth() {
        let path = tmp_path("meta");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            se.log_event(&mut eng, &mut tx, "CableReplaced", "W17", 5, 0, "r1")
                .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        // Raw graph meta bytes: the 11th u32 (offset 40) is se_meta_page.
        let mut eng = Engine::open(&path).unwrap();
        let raw = *eng.read_page(1).unwrap();
        let se_meta = u32::from_le_bytes(raw[40..44].try_into().unwrap());
        assert_ne!(se_meta, 0, "layout page is real, and never page 0");
        assert_ne!(se_meta, u32::MAX, "field was persisted");
        // The store rebuilt from that page still finds the event.
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let se = StateEventStore::open(&mut eng, &mut gs).unwrap();
        let evs = se.get_events(&mut eng, Some("W17"), 0, 10).unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].name, "CableReplaced");
        assert_eq!(evs[0].provenance, 0);
        cleanup(&path);
    }
}

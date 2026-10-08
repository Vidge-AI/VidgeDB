//! Phase 7 integration tests: Agent Tool API (spec §27/§28/§56).
//!
//! Fixture: a machine PLC -> Wire -> Motor -> Pump (electrical + mechanical
//! topology) with current telemetry on the Motor and a spec property
//! (`spec.current.max`), then every AgentApi method is exercised against a
//! FRESHLY REOPENED database (the committed state — same-session reads after
//! commit are stale, a known Phase-2 engine bug class).

#[cfg(test)]
mod tests {
    use vidgedb::tools::{err_json, provenance_class_names, AgentApi, Role};

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_p70_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    const NOW: i64 = 1_760_000_000;

    /// Build the machine fixture (committed + persisted).
    ///
    /// Graph: PLC01 -electrical:connected_via-> W17 -electrical:feeds->
    /// Motor42 -mechanical:drives-> Pump17.
    /// Telemetry: Motor42.current with a 12.5 A overshoot inside the window
    /// (spec: max 10 A) and a 9.0 A baseline; Pump17.pressure telemetry
    /// without a spec property (NO_SPEC path).
    fn build(path: &str) {
        use vidgedb::engine::Engine;
        use vidgedb::stores::GraphStore;
        use vidgedb::timeseries::TimeSeriesStore;

        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut tx = eng.begin().unwrap();
        let plc = gs
            .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[("vendor", "Siemens")])
            .unwrap();
        let wire = gs
            .add_entity(&mut eng, &mut tx, "Wire", "W17", &[])
            .unwrap();
        let motor = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "Motor42",
                // Phase 2 caps inline props at 44 bytes: spec.max (19) +
                // spec.unit (20) = 39 fits; a third prop would overflow.
                &[("spec.current.max", "10"), ("spec.current.unit", "A")],
            )
            .unwrap();
        let pump = gs
            .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[("pressure", "183")])
            .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            plc,
            wire,
            "electrical:connected_via",
            0,
            -1,
            0, // Fact
        )
        .unwrap();
        gs.add_relation(&mut eng, &mut tx, wire, motor, "electrical:feeds", 0, -1, 0)
            .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            motor,
            pump,
            "mechanical:drives",
            0,
            -1,
            1, // Observation (sensor-confirmed drive link)
        )
        .unwrap();
        let s = ts
            .create_series(&mut eng, &mut tx, "Motor42.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s, NOW - 600, 9.0).unwrap();
        ts.append(&mut eng, &mut tx, s, NOW - 300, 12.5).unwrap(); // overshoot
        ts.flush_series(&mut eng, &mut tx, s).unwrap();
        let s2 = ts
            .create_series(&mut eng, &mut tx, "Pump17.pressure")
            .unwrap();
        ts.append(&mut eng, &mut tx, s2, NOW - 300, 183.0).unwrap();
        ts.flush_series(&mut eng, &mut tx, s2).unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();
    }

    /// Open the API fresh on the committed database.
    fn api(path: &str) -> AgentApi {
        AgentApi::open(path, "test-agent-v7").unwrap()
    }

    // -----------------------------------------------------------------------
    // schema()
    // -----------------------------------------------------------------------

    #[test]
    fn schema_lists_types_topologies_series_provenance() {
        let path = tmp_path("schema");
        build(&path);
        let mut a = api(&path);
        let s = a.schema().unwrap();
        assert_eq!(
            s["entity_types"],
            serde_json::json!(["Motor", "PLC", "Pump", "Wire"])
        );
        assert_eq!(
            s["relation_topologies"],
            serde_json::json!(["electrical", "mechanical"])
        );
        assert_eq!(
            s["series"],
            serde_json::json!(["Motor42.current", "Pump17.pressure"])
        );
        // The 8 spec §14 classes, in byte order.
        assert_eq!(
            s["provenance_classes"],
            serde_json::json!(provenance_class_names())
        );
        assert_eq!(s["provenance_classes"].as_array().unwrap().len(), 8);
        assert_eq!(s["provenance_classes"][0], serde_json::json!("Fact"));
        assert_eq!(s["provenance_classes"][4], serde_json::json!("Hypothesis"));
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // query()
    // -----------------------------------------------------------------------

    #[test]
    fn query_match_with_resolved_bindings() {
        let path = tmp_path("query");
        build(&path);
        let mut a = api(&path);
        let r = a
            .query(
                "MATCH (p:PLC) -[:ELECTRICAL]-> (w:Wire) -[:ELECTRICAL]-> (m:Motor) RETURN p, w, m",
            )
            .unwrap();
        assert!(r.get("error").is_none(), "unexpected: {}", r);
        assert_eq!(r["n"], 1);
        let row = &r["rows"][0];
        assert_eq!(row["p"]["name"], "PLC01");
        assert_eq!(row["p"]["type"], "PLC");
        assert_eq!(row["p"]["properties"]["vendor"], "Siemens");
        assert_eq!(row["w"]["name"], "W17");
        assert_eq!(row["m"]["name"], "Motor42");
        assert_eq!(row["m"]["properties"]["spec.current.max"], "10");
        assert_eq!(row["m"]["properties"]["spec.current.unit"], "A");
        // WHERE by name + LIMIT path.
        let r2 = a
            .query("MATCH (m:Motor) WHERE m.name = \"Motor42\" RETURN m")
            .unwrap();
        assert_eq!(r2["n"], 1);
        assert_eq!(r2["rows"][0]["m"]["key"], row["m"]["key"]);
        cleanup(&path);
    }

    #[test]
    fn query_invalid_vql_returns_error_value_not_panic() {
        let path = tmp_path("qerr");
        build(&path);
        let mut a = api(&path);
        for bad in [
            "MATCH (m:Motor RETURN m",        // unbalanced
            "MATCH (m:Motor) -[:]-> (x) RET", // broken arrow + keyword
            "totally not vql",
            "MATCH (m:Motor) WHERE m.name = RETURN m",
        ] {
            let r = a.query(bad).unwrap();
            assert!(
                r.get("error").is_some(),
                "expected {{error}} for {:?}, got {}",
                bad,
                r
            );
            assert_eq!(r, err_json(r["error"].as_str().unwrap()));
        }
        // The API still works afterwards (no poisoned state).
        let ok = a.query("MATCH (p:PLC) RETURN p").unwrap();
        assert_eq!(ok["n"], 1);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // query_temporal()
    // -----------------------------------------------------------------------

    #[test]
    fn query_temporal_measure_during_aggregates() {
        let path = tmp_path("tq");
        build(&path);
        let mut a = api(&path);
        let r = a
            .query_temporal(
                "MATCH (m:Motor) WHERE m.name = \"Motor42\" MEASURE m.current DURING last(1h) RETURN max(current), avg(current)",
                NOW,
            )
            .unwrap();
        assert!(r.get("error").is_none(), "unexpected: {}", r);
        assert_eq!(r["n"], 1);
        let row = &r["rows"][0];
        assert_eq!(row["m"]["name"], "Motor42");
        assert_eq!(row["max"]["kind"], "Max");
        assert_eq!(row["max"]["value"], 12.5);
        assert_eq!(row["avg"]["kind"], "Avg");
        assert_eq!(row["avg"]["value"], 10.75); // (9.0 + 12.5) / 2
                                                // Broken temporal query -> {"error"} too.
        let bad = a
            .query_temporal("MATCH (m:Motor) MEASURE m.current DURING last(1q)", NOW)
            .unwrap();
        assert!(bad.get("error").is_some(), "got {}", bad);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // get_entity()
    // -----------------------------------------------------------------------

    #[test]
    fn get_entity_returns_props_and_relations() {
        let path = tmp_path("ent");
        build(&path);
        let mut a = api(&path);
        // Motor42 is key 2 by insertion order (PLC, Wire, Motor, Pump).
        let e = a.get_entity(2).unwrap();
        assert_eq!(e["name"], "Motor42");
        assert_eq!(e["type"], "Motor");
        assert_eq!(e["key"], 2);
        assert_eq!(e["properties"]["spec.current.max"], "10");
        assert_eq!(e["properties"]["spec.current.unit"], "A");
        // 1 in (W17 -electrical:feeds->) and 1 out (-mechanical:drives->).
        assert_eq!(e["relations_in"].as_array().unwrap().len(), 1);
        assert_eq!(e["relations_out"].as_array().unwrap().len(), 1);
        let rin = &e["relations_in"][0];
        assert_eq!(rin["from"], "W17");
        assert_eq!(rin["type"], "feeds");
        assert_eq!(rin["topology"], "electrical");
        assert_eq!(rin["provenance"], "Fact");
        let rout = &e["relations_out"][0];
        assert_eq!(rout["to"], "Pump17");
        assert_eq!(rout["type"], "drives");
        assert_eq!(rout["topology"], "mechanical");
        assert_eq!(rout["provenance"], "Observation");
        // PLC has only outgoing relations.
        let plc = a.get_entity(0).unwrap();
        assert_eq!(plc["relations_in"].as_array().unwrap().len(), 0);
        assert_eq!(plc["relations_out"].as_array().unwrap().len(), 1);
        // Unknown key -> structured error, no panic.
        let miss = a.get_entity(999).unwrap();
        assert!(miss.get("error").is_some(), "got {}", miss);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // get_measurements()
    // -----------------------------------------------------------------------

    #[test]
    fn get_measurements_returns_points_min_max() {
        let path = tmp_path("meas");
        build(&path);
        let mut a = api(&path);
        let m = a
            .get_measurements("Motor42", "current", NOW - 3600, NOW)
            .unwrap();
        assert!(m.get("error").is_none(), "unexpected: {}", m);
        assert_eq!(m["count"], 2);
        assert_eq!(m["min"], 9.0);
        assert_eq!(m["max"], 12.5);
        let pts = m["points"].as_array().unwrap();
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0]["t"], NOW - 600);
        assert_eq!(pts[0]["value"], 9.0);
        assert_eq!(pts[1]["value"], 12.5);
        // Window that excludes everything -> count 0, null min/max.
        let empty = a.get_measurements("Motor42", "current", 0, 1).unwrap();
        assert_eq!(empty["count"], 0);
        assert!(empty["min"].is_null());
        assert!(empty["max"].is_null());
        // Unknown series -> {"error"}.
        let miss = a.get_measurements("Motor42", "vibration", 0, NOW).unwrap();
        assert!(miss.get("error").is_some(), "got {}", miss);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // check() (spec §25)
    // -----------------------------------------------------------------------

    #[test]
    fn check_reports_violation_with_deviation() {
        let path = tmp_path("chk");
        build(&path);
        let mut a = api(&path);
        let c = a.check("Motor42", "current", NOW - 3600, NOW).unwrap();
        assert!(c.get("error").is_none(), "unexpected: {}", c);
        assert_eq!(c["status"], "VIOLATION");
        assert_eq!(c["entity"], "Motor42");
        assert_eq!(c["expected_max"], 10.0);
        assert_eq!(c["observed"], 12.5);
        assert_eq!(c["deviation"], 2.5);
        assert_eq!(c["unit"], "A");
        assert_eq!(c["expected_provenance"], "Specification");
        assert_eq!(c["observed_provenance"], "Observation");
        assert_eq!(c["points_checked"], 2);
        // Unknown entity -> {"error"}.
        let miss = a.check("Ghost", "current", 0, NOW).unwrap();
        assert!(miss.get("error").is_some(), "got {}", miss);
        cleanup(&path);
    }

    #[test]
    fn check_no_spec_and_no_data_statuses() {
        let path = tmp_path("chk2");
        build(&path);
        let mut a = api(&path);
        // Pump17.pressure: telemetry exists, no spec.*.max property.
        let ns = a.check("Pump17", "pressure", NOW - 3600, NOW).unwrap();
        assert_eq!(ns["status"], "NO_SPEC");
        // Motor42.vibration: spec exists? No — no spec prop and no series.
        let nd = a.check("Motor42", "vibration", NOW - 3600, NOW).unwrap();
        assert_eq!(nd["status"], "NO_SPEC");
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // trace()
    // -----------------------------------------------------------------------

    #[test]
    fn trace_finds_plc_to_pump_path() {
        let path = tmp_path("trace");
        build(&path);
        let mut a = api(&path);
        let t = a.trace("PLC01", "Pump17", 5).unwrap();
        assert!(t.get("error").is_none(), "unexpected: {}", t);
        assert_eq!(t["found"], true);
        assert_eq!(t["n_hops"], 3);
        let steps = t["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0]["from"], "PLC01");
        assert_eq!(steps[0]["to"], "W17");
        assert_eq!(steps[0]["topology"], "electrical");
        assert_eq!(steps[0]["relation_type"], "connected_via");
        assert_eq!(steps[1]["from"], "W17");
        assert_eq!(steps[1]["to"], "Motor42");
        assert_eq!(steps[1]["relation_type"], "feeds");
        assert_eq!(steps[2]["from"], "Motor42");
        assert_eq!(steps[2]["to"], "Pump17");
        assert_eq!(steps[2]["relation_type"], "drives");
        assert_eq!(steps[2]["topology"], "mechanical");
        // The path carries resolved node summaries.
        let path_nodes = t["path"].as_array().unwrap();
        assert_eq!(path_nodes.len(), 4);
        assert_eq!(path_nodes[0]["name"], "PLC01");
        assert_eq!(path_nodes[3]["name"], "Pump17");
        // max_hops too small -> found=false, no panic.
        let short = a.trace("PLC01", "Pump17", 2).unwrap();
        assert_eq!(short["found"], false);
        // Unknown endpoints -> {"error"}.
        let miss = a.trace("PLC01", "Ghost", 3).unwrap();
        assert!(miss.get("error").is_some(), "got {}", miss);
        cleanup(&path);
    }

    #[test]
    fn trace_unreachable_returns_found_false() {
        let path = tmp_path("trace2");
        build(&path);
        let mut a = api(&path);
        // Pump has no outgoing edges: Pump -> PLC is unreachable.
        let r = a.trace("Pump17", "PLC01", 5).unwrap();
        assert_eq!(r["found"], false);
        assert_eq!(r["steps"].as_array().unwrap().len(), 0);
        // A node traces to itself trivially.
        let self1 = a.trace("Motor42", "Motor42", 3).unwrap();
        assert_eq!(self1["found"], true);
        assert_eq!(self1["path"][0]["name"], "Motor42");
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // provenance() + the write refusal (spec §14/§29)
    // -----------------------------------------------------------------------

    #[test]
    fn provenance_lists_relations_with_bytes() {
        let path = tmp_path("prov");
        build(&path);
        let mut a = api(&path);
        let p = a.provenance().unwrap();
        assert!(p.get("error").is_none(), "unexpected: {}", p);
        assert!(p["note"]
            .as_str()
            .unwrap()
            .contains("never promotes Hypothesis"));
        let rels = p["relations"].as_array().unwrap();
        assert_eq!(rels.len(), 3); // all live relations
        assert_eq!(rels[0]["src"], "PLC01");
        assert_eq!(rels[0]["provenance_byte"], 0);
        assert_eq!(rels[0]["provenance"], "Fact");
        assert_eq!(rels[2]["provenance_byte"], 1);
        assert_eq!(rels[2]["provenance"], "Observation");
        cleanup(&path);
    }

    #[test]
    fn set_hypothesis_is_refused_with_clear_error() {
        let path = tmp_path("refuse");
        build(&path);
        let mut a = api(&path);
        let r = a
            .set_hypothesis("Motor42", "bearing wear suspected")
            .unwrap();
        assert!(r.get("error").is_some(), "got {}", r);
        let msg = r["error"].as_str().unwrap();
        assert!(msg.contains("refused"), "message: {}", msg);
        // Nothing was written: relation count unchanged (provenance view).
        let p = a.provenance().unwrap();
        assert_eq!(p["relations"].as_array().unwrap().len(), 3);
        // Phase 11: the refusal applies to EVERY role (a Writer gets the
        // same refusal — hypotheses are never agent-writable, spec §29).
        let mut w = AgentApi::open_with_role(&path, "writer-x", Role::Writer).unwrap();
        let rw = w.set_hypothesis("Motor42", "h").unwrap();
        assert!(rw.get("error").is_some(), "got {}", rw);
        let audit = w.audit_log();
        assert_eq!(audit.last().unwrap().method, "audit_log"); // (audit_log self-audits)
        assert!(audit
            .iter()
            .any(|e| e.method == "set_hypothesis" && e.agent_id == "writer-x"));
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // Audit log (spec §56)
    // -----------------------------------------------------------------------

    #[test]
    fn audit_log_records_every_call_with_agent_id() {
        let path = tmp_path("audit");
        build(&path);
        let mut a = AgentApi::open(&path, "agent-x").unwrap();
        let _ = a.schema();
        let _ = a.query("MATCH (m:Motor) WHERE m.name = \"Motor42\" RETURN m");
        let _ = a.check("Motor42", "current", NOW - 3600, NOW);
        let _ = a.set_hypothesis("Motor42", "h"); // refusals are audited too
        let log = a.audit_log();
        // 4 calls + the audit_log() call itself.
        assert_eq!(log.len(), 5);
        assert!(log.iter().all(|e| e.agent_id == "agent-x"));
        let methods: Vec<&str> = log.iter().map(|e| e.method.as_str()).collect();
        assert_eq!(
            methods,
            vec!["schema", "query", "check", "set_hypothesis", "audit_log"]
        );
        // Params summaries carry the call arguments.
        assert!(log[1].params_summary.contains("Motor42"));
        assert!(log[2].params_summary.contains("current"));
        assert!(log[3].params_summary.contains("REFUSED"));
        // Timestamps are sane unix seconds.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(log
            .iter()
            .all(|e| e.timestamp > 1_700_000_000 && e.timestamp <= now));
        // A second agent gets its own trail.
        let mut b = AgentApi::open(&path, "agent-y").unwrap();
        assert!(b.audit_log().iter().all(|e| e.agent_id == "agent-y"));
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // Read-only guarantee (spec §55)
    // -----------------------------------------------------------------------

    #[test]
    fn api_is_read_only_no_new_entities_or_relations() {
        let path = tmp_path("ro");
        build(&path);
        let mut a = api(&path);
        let before = a.schema().unwrap();
        // Throw every (allowed) method at it, including refused writes.
        let _ = a.query("MATCH (x:PLC) RETURN x");
        let _ = a.get_entity(0);
        let _ = a.get_measurements("Motor42", "current", 0, NOW);
        let _ = a.trace("PLC01", "Pump17", 5);
        let _ = a.set_hypothesis("PLC01", "nope");
        let after = a.schema().unwrap();
        assert_eq!(before, after); // same inventory: nothing added
                                   // And the on-disk file gained no new entity slab records either.
        let mut eng = vidgedb::engine::Engine::open(&path).unwrap();
        let gs = vidgedb::stores::GraphStore::open(&mut eng).unwrap();
        assert_eq!(gs.entity_count(), 4);
        assert_eq!(gs.relations.len(), 3);
        cleanup(&path);
    }
}

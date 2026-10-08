//! Phase 11 — the ingestion path e2e: roles (spec §55 derrogation),
//! `ingest_points`, `upsert_entity`, `set_state`, and the -32003 Reader
//! refusal, exercised both at the AgentApi level AND through the spawned
//! `vidgedb --service` binary (JSON-RPC 2.0, exactly like a Phase 12 SDK).
//!
//! Fixture convention (phase70/82 lineage): a fresh `.vdg` per test
//! (tmp_path + cleanup), and every READ-BACK goes through a freshly
//! REOPENED API (same-session reads after commit are stale — the known
//! Phase 2 engine bug class; ingest commits, then the verifier reopens).

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::stores::GraphStore;
    use vidgedb::tools::{AgentApi, RelationSpec, Role};

    // -- small tempfile helper (tmp_path + cleanup convention) ------------

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("vidgedb_p84_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    const T0: i64 = 1_700_000_000;

    /// An EXISTING machine seed: PLC01 -electrical:feeds-> Motor42 (built
    /// through the low-level stores — the base topology a Phase 11 ingest
    /// then EXTENDS with telemetry + more entities).
    fn build_base(path: &str) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let plc = gs
            .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[("vendor", "Siemens")])
            .unwrap();
        let _motor = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[])
            .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            plc,
            1,
            "electrical:feeds",
            0,
            -1,
            vidgedb::model::Provenance::Fact as u8,
        )
        .unwrap();
        eng.commit(tx).unwrap();
        gs.persist(&mut eng).unwrap();
    }

    // -----------------------------------------------------------------------
    // (1) Reader attempts ingest -> clear -32003 error + AUDIT TRAIL ENTRY
    // -----------------------------------------------------------------------

    #[test]
    fn phase84_reader_write_refused_with_32003_and_audited() {
        let path = tmp_path("reader_refused");
        build_base(&path);
        let mut a = AgentApi::open_with_role(&path, "nosy-agent", Role::Reader).unwrap();
        assert_eq!(a.role(), Role::Reader);

        // ingest_points -> Err carrying the documented -32003 marker.
        let err = a
            .ingest_points("Motor42", "current", &[(T0, 5.0)])
            .unwrap_err();
        assert!(
            err.error.contains("-32003"),
            "refusal must carry the documented code, got {}",
            err.error
        );
        assert!(
            err.error.contains("Reader"),
            "refusal must name the role, got {}",
            err.error
        );

        // The refusal left an audit entry (spec §56: even forbidden
        // attempts are traceable) — and NOTHING was written.
        let log = a.audit_log();
        let refusal = log
            .iter()
            .find(|e| e.method == "ingest_points")
            .expect("the write attempt must be audited even when refused");
        assert!(refusal.params_summary.contains("REFUSED"));
        assert_eq!(refusal.agent_id, "nosy-agent");
        let m = a
            .get_measurements("Motor42", "current", 0, T0 + 10)
            .unwrap();
        assert!(m.get("error").is_some(), "no series may exist: {}", m);
        cleanup(&path);
    }

    /// The -32003 gate covers EVERY write method for a Reader (each
    /// refusal audited) — five refusals, zero writes, zero deletions.
    #[test]
    fn phase84_refuses_every_write_method_for_reader() {
        let path = tmp_path("reader_all");
        build_base(&path);
        let mut a = AgentApi::open_with_role(&path, "reader-x", Role::Reader).unwrap();
        let before_schema = a.schema().unwrap();

        for (method, res) in [
            (
                "ingest_points",
                a.ingest_points("Motor42", "current", &[(T0, 1.0)])
                    .err()
                    .unwrap()
                    .error,
            ),
            (
                "upsert_entity",
                a.upsert_entity("Ghost", "Motor", &[], &[], "plc")
                    .err()
                    .unwrap()
                    .error,
            ),
            (
                "set_state",
                a.set_state("Motor42", "running", "yes", Some(T0))
                    .err()
                    .unwrap()
                    .error,
            ),
            (
                "log_event",
                a.log_event("boot", "Motor42", T0, 5, "x")
                    .err()
                    .unwrap()
                    .error,
            ),
            ("retain", a.retain(T0).err().unwrap().error),
        ] {
            assert!(
                res.contains("-32003"),
                "{} must refuse with -32003, got {}",
                method,
                res
            );
        }

        // Nothing was written: identical inventory.
        let after_schema = a.schema().unwrap();
        assert_eq!(before_schema["series"], after_schema["series"]);
        assert_eq!(before_schema["entity_types"], after_schema["entity_types"]);

        // Each refusal audited (five in total).
        let log = a.audit_log();
        let refused: Vec<_> = log
            .iter()
            .filter(|e| e.params_summary.contains("REFUSED"))
            .collect();
        assert_eq!(refused.len(), 5, "every refused write leaves a trail");
        // A Reader can still READ state/events (Phase 11 routing rule:
        // only the writes are gated).
        let st = a.get_state("Motor42", "running", None).unwrap();
        assert!(st.is_null(), "read access stays open for a Reader");
        let ev = a.get_events(None, 0, T0 + 10).unwrap();
        assert_eq!(ev["n"], 0);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // (2) Ingest role -> ingest_points -> points re-read IMMEDIATELY via
    //     get_measurements (freshly reopened API — validated e2e shape)
    // -----------------------------------------------------------------------

    #[test]
    fn phase84_ingest_points_roundtrip_via_get_measurements() {
        let path = tmp_path("ingest_roundtrip");
        build_base(&path);
        {
            let mut a = AgentApi::open_with_role(&path, "plc-bridge", Role::Ingest).unwrap();
            let pts: Vec<(i64, f64)> = (0..300)
                .map(|i| (T0 + i as i64, 5.0 + (i % 60) as f64 * 0.1))
                .collect();
            let r = a.ingest_points("Motor42", "current", &pts).unwrap();
            assert!(r.get("error").is_none(), "unexpected error {}", r);
            assert_eq!(r["accepted"], 300);
            // 300 points / 256-pt auto-batch => 2 chunk flushes INSIDE the
            // call (the final flush_series drains the 44-pt remainder too —
            // so 2 chunks total: ceil(300/256) = 2).
            assert_eq!(r["chunks_flushed"], 2, "1 tx, auto-flush every 256");
        }
        // Immediate read-back via a FRESH API (what any external reader
        // sees; the engine pager view is fully committed at this point).
        let mut b = AgentApi::open_with_role(&path, "observer", Role::Reader).unwrap();
        let m = b
            .get_measurements("Motor42", "current", T0, T0 + 299)
            .unwrap();
        assert!(m.get("error").is_none(), "unexpected error {}", m);
        assert_eq!(
            m["count"], 300,
            "every ingested point is immediately visible"
        );
        assert_eq!(m["min"], 5.0);
        assert_eq!(m["max"], 10.9); // 5.0 + 59 * 0.1
        let pts = m["points"].as_array().unwrap();
        assert_eq!(pts[0]["t"], T0);
        assert_eq!(pts[0]["value"], 5.0);
        assert_eq!(pts[299]["t"], T0 + 299);

        // A second ingest into the SAME series appends (no duplicate series
        // created) — and stays visible.
        let mut c = AgentApi::open_with_role(&path, "plc-bridge", Role::Ingest).unwrap();
        let r2 = c
            .ingest_points("Motor42", "current", &[(T0 + 300, 4.2)])
            .unwrap();
        assert_eq!(r2["accepted"], 1);
        let m2 = c
            .get_measurements("Motor42", "current", T0, T0 + 400)
            .unwrap();
        assert_eq!(m2["count"], 301);
        // The series list has exactly one Motor42.current.
        let s = c.schema().unwrap();
        assert_eq!(
            s["series"].as_array().unwrap().len(),
            1,
            "no duplicate series on re-ingest: {}",
            s
        );
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // (3) upsert_entity with relations -> trace() sees the path
    // -----------------------------------------------------------------------

    #[test]
    fn phase84_upsert_entity_relations_visible_in_trace() {
        let path = tmp_path("upsert_trace");
        build_base(&path);
        {
            let mut a = AgentApi::open_with_role(&path, "plc-bridge", Role::Writer).unwrap();
            // Sequencing (relation targets must EXIST — an ingest agent
            // never invents endpoints): first create Pump17 (no relations),
            // THEN upsert Motor42 (exists => update, props merged) with the
            // relation to the now-existing Pump17.
            let pump = a
                .upsert_entity(
                    "Pump17",
                    "Pump",
                    &[("pressure.rating".into(), Some("183".into()))],
                    &[],
                    "plc",
                )
                .unwrap();
            assert!(pump.get("error").is_none(), "unexpected {}", pump);
            assert_eq!(pump["created"], true);

            let motor = a
                .upsert_entity(
                    "Motor42",
                    "Motor",
                    &[
                        ("spec.current.max".into(), Some("10".into())),
                        // A null prop on a NEW key records the key (value "")…
                        ("note.placeholder".into(), None),
                    ],
                    &[RelationSpec {
                        to: "Pump17".into(),
                        relation_type: "mechanical:drives".into(),
                        valid_from: Some(T0),
                    }],
                    "plc", // official machine topology => Fact
                )
                .unwrap();
            assert!(motor.get("error").is_none(), "unexpected {}", motor);
            assert_eq!(
                motor["created"], false,
                "Motor42 already existed (update path)"
            );
            assert_eq!(motor["relations_added"], 1);

            // Idempotent upsert: same relation again -> deduped (0 added).
            let again = a
                .upsert_entity(
                    "Motor42",
                    "Motor",
                    &[("spec.current.max".into(), Some("11".into()))], // non-null overwrites
                    &[RelationSpec {
                        to: "Pump17".into(),
                        relation_type: "mechanical:drives".into(),
                        valid_from: Some(T0),
                    }],
                    "plc",
                )
                .unwrap();
            assert_eq!(again["relations_added"], 0, "dedup: no duplicate topology");
            assert_eq!(again["created"], false);
        }

        // Reopen (Reader) — trace() sees the FULL ingested path, with the
        // provenance the ingest declared.
        let mut b = AgentApi::open_with_role(&path, "observer", Role::Reader).unwrap();
        let t = b.trace("PLC01", "Pump17", 5).unwrap();
        assert!(t.get("error").is_none(), "unexpected {}", t);
        assert_eq!(t["found"], true);
        assert_eq!(t["n_hops"], 2); // PLC01 -> Motor42 -> Pump17
        let steps = t["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0]["from"], "PLC01");
        assert_eq!(steps[0]["to"], "Motor42");
        assert_eq!(steps[0]["topology"], "electrical");
        assert_eq!(steps[0]["relation_type"], "feeds");
        assert_eq!(steps[0]["provenance"], "Fact"); // the Phase 2 seed
        assert_eq!(steps[1]["from"], "Motor42");
        assert_eq!(steps[1]["to"], "Pump17");
        assert_eq!(steps[1]["topology"], "mechanical");
        assert_eq!(steps[1]["relation_type"], "drives");
        assert_eq!(
            steps[1]["provenance"], "Fact",
            "source=plc => Fact (spec §29)"
        );
        assert_eq!(steps[1]["to"], "Pump17");

        // The prop merge is visible (and the null prop did not erase).
        let motor_key = entity_key_by_name(&mut b, "Motor");
        let motor_val = b.get_entity(motor_key).unwrap();
        assert_eq!(motor_val["name"], "Motor42");
        // Merged props: the seed had no spec prop; the upsert OVERWROTE the
        // (existing, from upsert 1) one with 11 — visible now.
        assert_eq!(motor_val["properties"]["spec.current.max"], "11");
        assert_eq!(motor_val["properties"]["note.placeholder"], "");
        // source=plc relations carry Fact provenance in get_entity too.
        let rout = &motor_val["relations_out"][0];
        assert_eq!(rout["to"], "Pump17");
        assert_eq!(rout["provenance"], "Fact");
        cleanup(&path);
    }

    /// Lookup helper: entity key by exact name through the public surface.
    fn entity_key_by_name(api: &mut AgentApi, entity_type: &str) -> u32 {
        // The name is unique in the fixture; VidgeQL binds it by name.
        let vql = format!(
            "MATCH (e:{}) WHERE e.name = \"{}\" RETURN e",
            entity_type, "Motor42"
        );
        let rows = api.query(&vql).unwrap();
        rows["rows"][0]["e"]["key"]
            .as_u64()
            .expect("fixture entity must exist") as u32
    }

    // -----------------------------------------------------------------------
    // (4) 10K-point batch -> chunks flushed correctly (256-pt granularity)
    // -----------------------------------------------------------------------

    #[test]
    fn phase84_ten_k_points_chunk_flush() {
        let path = tmp_path("big_batch");
        build_base(&path);
        let mut a = AgentApi::open_with_role(&path, "plc-bridge", Role::Ingest).unwrap();
        let n = 10_000;
        let pts: Vec<(i64, f64)> = (0..n).map(|i| (T0 + i as i64, (i % 7) as f64)).collect();
        let r = a.ingest_points("Motor42", "vibration", &pts).unwrap();
        assert!(r.get("error").is_none(), "unexpected {}", r);
        assert_eq!(r["accepted"], n as u64);
        // ceil(10_000 / 256) = 40 chunks flushed by the auto-batch (the
        // final flush_series is a no-op on an evenly-divided buffer).
        assert_eq!(
            r["chunks_flushed"], 40,
            "exactly ceil(n/BATCH) chunks persisted in ONE tx"
        );
        // Read-back over the FULL range (fresh API): every point present,
        // chronological, correct values at the chunk seams.
        drop(a);
        let mut b = AgentApi::open_with_role(&path, "observer", Role::Reader).unwrap();
        let m = b
            .get_measurements("Motor42", "vibration", T0, T0 + n as i64)
            .unwrap();
        assert_eq!(m["count"], n as u64);
        assert_eq!(m["min"], 0.0);
        assert_eq!(m["max"], 6.0);
        let pts_out = m["points"].as_array().unwrap();
        assert_eq!(pts_out.len(), n);
        assert_eq!(pts_out[0]["t"], T0);
        assert_eq!(pts_out[255]["t"], T0 + 255); // first chunk boundary
        assert_eq!(pts_out[256]["t"], T0 + 256); // second chunk start
        assert_eq!(pts_out[9999]["t"], T0 + 9999);
        assert_eq!(pts_out[9999]["value"], 9999.0 % 7.0);
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // (5) reopen -> EVERYTHING persists (telemetry + topology + props)
    // -----------------------------------------------------------------------

    #[test]
    fn phase84_reopen_persists_ingested_everything() {
        let path = tmp_path("reopen_persist");
        build_base(&path);
        {
            let mut a = AgentApi::open_with_role(&path, "plc-bridge", Role::Ingest).unwrap();
            // Pump17 FIRST (relation target), then sensor feed, then
            // topology with an "agent" source (=> Observation provenance).
            // Prop-seed, then sensor telemetry: TWO signals (level +
            // temperature), one call each (auto-create_series for both).
            a.upsert_entity(
                "Relay3",
                "Relay",
                &[("coil".into(), Some("24V".into()))],
                &[],
                "sensor",
            )
            .unwrap();
            a.ingest_points("Relay3", "coil_voltage", &[(T0, 24.1), (T0 + 1, 24.0)])
                .unwrap();
            a.upsert_entity(
                "Relay3",
                "Relay",
                &[("mounted.on".into(), Some("panel-A".into()))],
                &[RelationSpec {
                    to: "PLC01".into(),
                    relation_type: "network:profinet".into(),
                    valid_from: None,
                }],
                "agent", // agent-observed link => Observation (NOT Fact)
            )
            .unwrap();
            a.set_state("Relay3", "coil_state", "energized", Some(T0))
                .unwrap();
            a.log_event("coil_pulled", "Relay3", T0 + 2, 5, "24V applied")
                .unwrap();
        }
        // Full reopen (drop everything first).
        let mut b = AgentApi::open_with_role(&path, "observer", Role::Reader).unwrap();
        // Telemetry persisted.
        let mv = b
            .get_measurements("Relay3", "coil_voltage", T0, T0 + 5)
            .unwrap();
        assert_eq!(mv["count"], 2);
        assert_eq!(mv["max"], 24.1);
        // Topology persisted + provenance kept.
        let t = b.trace("Relay3", "PLC01", 3).unwrap();
        assert_eq!(t["found"], true);
        assert_eq!(t["steps"][0]["topology"], "network");
        assert_eq!(t["steps"][0]["relation_type"], "profinet");
        // Props merged and persisted.
        let relay = b.get_entity(2).unwrap(); // Relay3 = 3rd entity (PLC, Motor, Relay)
        assert_eq!(relay["name"], "Relay3");
        assert_eq!(relay["properties"]["mounted.on"], "panel-A");
        // State + event persisted.
        let st = b.get_state("Relay3", "coil_state", None).unwrap();
        assert_eq!(st["value"], "energized");
        let evs = b.get_events(Some("Relay3"), 0, T0 + 10).unwrap();
        assert_eq!(evs["n"], 1);
        assert_eq!(evs["events"][0]["name"], "coil_pulled");
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // (6) the audit trail records the WRITES (spec §56) — full trail order
    // -----------------------------------------------------------------------

    #[test]
    fn phase84_audit_trail_records_writes() {
        let path = tmp_path("audit_writes");
        build_base(&path);
        let mut a = AgentApi::open_with_role(&path, "writer-84", Role::Writer).unwrap();
        a.ingest_points("Motor42", "current", &[(T0, 3.5), (T0 + 1, 3.6)])
            .unwrap();
        a.upsert_entity("Pump17", "Pump", &[], &[], "plc").unwrap();
        a.set_state("Motor42", "running", "yes", Some(T0)).unwrap();
        a.log_event("boot", "Motor42", T0, 5, "audit test").unwrap();
        a.retain(T0 + 100).unwrap(); // nothing old enough to drop, but audited

        let log = a.audit_log();
        let methods: Vec<&str> = log.iter().map(|e| e.method.as_str()).collect();
        // Writes are audited IN ORDER (plus the audit_log() self-entry).
        assert_eq!(
            methods,
            vec![
                "ingest_points",
                "upsert_entity",
                "set_state",
                "log_event",
                "retain",
                "audit_log"
            ]
        );
        assert!(log.iter().all(|e| e.agent_id == "writer-84"));
        assert!(log[0].params_summary.contains("Motor42"));
        assert!(log[0].params_summary.contains("n=2"));
        // Every entry has a sane unix timestamp.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(log.iter().all(|e| e.timestamp > T0 && e.timestamp <= now));
        // Reopen: the AUDIT TRAIL is in-memory by design (spec §56 v0) —
        // the DATA persists (covered by test 5); the trail is per-session.
        cleanup(&path);
    }

    // -----------------------------------------------------------------------
    // (7) spawn `vidgedb --service` + JSON-RPC ingest_points E2E
    // -----------------------------------------------------------------------

    mod e2e {
        use std::io::Write as _;
        use std::process::{Command, Stdio};

        /// One round-trip: write `reqs` lines, read one response per line.
        /// (reqs.len() == number of expected responses, like phase82.)
        fn roundtrip(child: &mut std::process::Child, reqs: &[&str]) -> Vec<serde_json::Value> {
            let stdin = child.stdin.as_mut().unwrap();
            for r in reqs {
                writeln!(stdin, "{}", r).unwrap();
            }
            stdin.flush().unwrap();
            let stdout = child.stdout.as_mut().unwrap();
            use std::io::BufRead;
            let mut reader = std::io::BufReader::new(stdout);
            let mut out = Vec::new();
            for _ in 0..reqs.len() {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(!line.is_empty(), "EOF before expected response");
                out.push(serde_json::from_str(line.trim()).unwrap());
            }
            out
        }

        #[test]
        fn phase84_service_jsonrpc_ingest_e2e() {
            let base = std::env::temp_dir().join(format!("vidgedb_p84_e2e_{}", std::process::id()));
            std::fs::create_dir_all(&base).unwrap();
            let db = base.join("e2e.vdg");

            // The service CREATES a fresh db on open (bookkeeping) — an
            // ingest agent can boot a database from scratch (the real PLC
            // bridge flow on a fresh machine).
            let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
                .args([
                    "--service",
                    db.to_str().unwrap(),
                    "--agent-id",
                    "plc-e2e",
                    "--role",
                    "ingest",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn vidgedb --service (ingest role)");

            // 1) upsert the machine (PLC => Fact topology).
            let resp = roundtrip(
                &mut child,
                &[r#"{"jsonrpc":"2.0","id":1,"method":"upsert_entity","params":{"name":"Motor77","type":"Motor","props":{"vendor":"ABB"},"relations":[],"source":"plc"}}"#],
            )
            .pop()
            .unwrap();
            assert_eq!(resp["result"]["created"], true, "got {}", resp);

            // 2) ingest a REAL telemetry batch (1000 points, one call).
            let ts: Vec<String> = (0..1000)
                .map(|i| {
                    format!(
                        "[{},{}]",
                        1_700_000_000 + i as i64,
                        4.0 + (i % 5) as f64 * 0.5
                    )
                })
                .collect();
            let req = format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"ingest_points","params":{{"entity":"Motor77","signal":"current","points":[{}]}}}}"#,
                ts.join(",")
            );
            let resp = roundtrip(&mut child, &[&req]).pop().unwrap();
            assert_eq!(resp["result"]["accepted"], 1000, "got {}", resp);
            // ceil(1000/256) = 4 chunks.
            assert_eq!(resp["result"]["chunks_flushed"], 4);

            // 3) a READER verifies the ingestion is immediately visible.
            //    (Same service can't change role mid-stream; verify with a
            //    second spawned service on the same file.)
            drop(resp);
            let mut reader = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
                .args([
                    "--service",
                    db.to_str().unwrap(),
                    "--agent-id",
                    "phase84-reader",
                ])
                // default role: reader (no flag at all = Phase 7 strictness)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let resp = roundtrip(
                &mut reader,
                &[
                    r#"{"jsonrpc":"2.0","id":10,"method":"get_measurements","params":{"entity":"Motor77","signal":"current","from":1700000000,"to":1700000999}}"#,
                    r#"{"jsonrpc":"2.0","id":11,"method":"query","params":{"vql":"MATCH (m:Motor) RETURN m"}}"#,
                ],
            );
            let meas = &resp[0]["result"];
            assert_eq!(
                meas["count"], 1000,
                "e2e-ingested points visible: {}",
                resp[0]
            );
            assert_eq!(meas["min"], 4.0);
            assert_eq!(meas["max"], 6.0);
            assert_eq!(resp[1]["result"]["n"], 1, "reader: {}", resp[1]);

            // 4) THE READER cannot write over the wire: -32003, and the
            //    stream keeps flowing (JSON-RPC failure is not fatal).
            let resp2 = roundtrip(
                &mut reader,
                &[
                    r#"{"jsonrpc":"2.0","id":12,"method":"ingest_points","params":{"entity":"Motor77","signal":"current","points":[[1700001000,1.0]]}}"#,
                    r#"{"jsonrpc":"2.0","id":13,"method":"schema"}"#,
                ],
            );
            assert_eq!(resp2[0]["error"]["code"], -32003, "got {}", resp2[0]);
            assert!(resp2[1]["result"].is_object(), "stream survives a refusal");

            // 5) Empty points array -> clean -32602 (never a panic).
            let resp3 = roundtrip(
                &mut reader,
                &[
                    r#"{"jsonrpc":"2.0","id":14,"method":"ingest_points","params":{"entity":"Motor77","signal":"current","points":[]}}"#,
                ],
            );
            assert_eq!(resp3[0]["error"]["code"], -32602);

            drop(reader);

            // 6) The writer session persists after `exit` (clean EOF).
            child.stdin.as_mut().unwrap().write_all(b"exit\n").unwrap();
            child.stdin.take();
            let status = child.wait().unwrap();
            assert!(status.success(), "ingest service exited cleanly");
        }
    }
}

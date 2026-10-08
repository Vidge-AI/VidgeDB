//! Phase 10.2 — service mode e2e: spawn the `vidgedb --service` binary,
//! speak line-delimited JSON-RPC 2.0 over stdin/stdout exactly like an
//! external AI agent would (Python/Node/shell), and assert:
//! - `schema` lists the entity types it ingested,
//! - `query` (VidgeQL MATCH) returns the expected rows,
//! - an INVALID request gets a JSON-RPC `error` — and the process is
//!   still alive afterwards (ZERO panic contract),
//! - a notification produces NO response line (JSON-RPC 2.0 §4).
//!
//! Spawned via [`std::process::Command`] on the freshly built binary
//! (`CARGO_BIN_EXE_vidgedb` re-links the right artifact per test run).

use serde_json::Value;
use std::io::Write as _;
use std::process::{Command, Stdio};

const AGENT: &str = "phase82-agent";

/// Build the fixture database through the very same AgentApi surface the
/// service drives (writes here are the documented bookkeeping + ingest).
fn build_fixture(path: &std::path::Path) {
    use vidgedb::engine::Engine;

    use vidgedb::stores::GraphStore;
    use vidgedb::timeseries::TimeSeriesStore;

    // The AgentApi is read-only; storage creation happens through the
    // engine + stores directly (the service binary does exactly this on
    // first open too).
    let mut eng = Engine::open(path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut tx = eng.begin().unwrap();
    let m1 = gs
        .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[("spec.max", "10")])
        .unwrap();
    let p1 = gs
        .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[])
        .unwrap();
    gs.add_relation(
        &mut eng,
        &mut tx,
        m1,
        p1,
        "mechanical:drives",
        0,
        -1,
        vidgedb::model::Provenance::Fact as u8,
    )
    .unwrap();
    eng.commit(tx).unwrap();
    {
        let mut tx2 = eng.begin().unwrap();
        gs.flush_meta(&mut eng, &mut tx2).unwrap();
        eng.commit(tx2).unwrap();
    }
    gs.persist(&mut eng).unwrap();

    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    // ONE tx for the whole series build (the validated phase70 shape):
    // a Tx dropped before commit RECLAIMS its writes, so the earlier
    // per-point txs were silently discarding nothing — but the series
    // record itself was created in a tx that got dropped here, which is
    // why schema saw an empty series list.
    let _sid = {
        let mut tx = eng.begin().unwrap();
        let sid = ts
            .create_series(&mut eng, &mut tx, "Motor42.current")
            .unwrap();
        for i in 0..256 {
            let t = 1_700_000_000 + i;
            ts.append(&mut eng, &mut tx, sid, t, 5.0 + ((i % 10) as f64))
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
        sid
    };
    ts.persist(&mut eng).unwrap();
    gs.persist(&mut eng).unwrap();
    let mut se = vidgedb::statestore::StateEventStore::open(&mut eng, &mut gs).unwrap();
    {
        let mut tx = eng.begin().unwrap();
        se.log_event(
            &mut eng,
            &mut tx,
            "boot",
            "Motor42",
            1_700_000_100,
            5,
            "phase82 fixture",
        )
        .unwrap();
        eng.commit(tx).unwrap();
    }
    se.persist(&mut eng).unwrap();
}

/// One round-trip: write `reqs` lines, read `n_expected` response lines.
fn roundtrip(child: &mut std::process::Child, reqs: &[&str], n_expected: usize) -> Vec<Value> {
    let stdin = child.stdin.as_mut().unwrap();
    for r in reqs {
        writeln!(stdin, "{}", r).unwrap();
    }
    stdin.flush().unwrap();
    let stdout = child.stdout.as_mut().unwrap();
    use std::io::BufRead;
    let mut reader = std::io::BufReader::new(stdout);
    let mut out = Vec::new();
    for _ in 0..n_expected {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(!line.is_empty(), "EOF before expected response");
        out.push(serde_json::from_str(line.trim()).unwrap());
    }
    out
}

#[test]
fn phase82_schema_query_error_no_panic() {
    let tmp = tempfile_dir();
    let db = tmp.join("phase82.vdg");
    build_fixture(&db);

    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        // Phase 11: this test exercises the WRITE methods (log_event,
        // retain) over the wire — a writer-role service is required.
        .args([
            "--service",
            db.to_str().unwrap(),
            "--agent-id",
            AGENT,
            "--role",
            "writer",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vidgedb --service");

    // -- schema: types list --------------------------------------------
    let resp = roundtrip(
        &mut child,
        &[r#"{"jsonrpc":"2.0","id":1,"method":"schema"}"#],
        1,
    )
    .pop()
    .unwrap();
    assert_eq!(resp["jsonrpc"], "2.0", "JSON-RPC version echoed");
    assert_eq!(resp["id"], 1);
    let types = resp["result"]["entity_types"].as_array().unwrap();
    let types: Vec<&str> = types.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(
        types.contains(&"Motor"),
        "schema lists Motor, got {:?}",
        types
    );
    assert!(types.contains(&"Pump"), "schema lists Pump");
    // A series exists and is listed (AgentApi reads the chunk index).
    let series = resp["result"]["series"].as_array().unwrap();
    assert!(
        series.iter().any(|s| s.as_str() == Some("Motor42.current")),
        "schema lists the time series, got {:?}",
        series
    );
    // Provenance classes surfaced (spec §56 the agent sees classes).
    let prov = resp["result"]["provenance_classes"].as_array().unwrap();
    assert!(prov.contains(&serde_json::json!("Hypothesis")));

    // -- query MATCH -> rows -------------------------------------------
    let resp = roundtrip(
        &mut child,
        &[r#"{"jsonrpc":"2.0","id":2,"method":"query","params":{"vql":"MATCH (m:Motor) WHERE m.name = \"Motor42\" RETURN m"}}"#],
        1,
    )
    .pop()
    .unwrap();
    assert_eq!(resp["id"], 2, "query must answer the right id");
    let rows = resp["result"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "one Motor in the fixture");
    assert_eq!(rows[0]["m"]["name"], "Motor42");
    assert_eq!(rows[0]["m"]["type"], "Motor");

    // -- INVALID request -> error, NO panic, service keeps living ------
    let resp = roundtrip(
        &mut child,
        &[
            "this is not json at all",
            r#"{"jsonrpc":"2.0","id":3,"method":"nosuchmethod"}"#,
        ],
        2,
    );
    let parse_err = &resp[0];
    assert_eq!(
        parse_err["error"]["code"], -32700,
        "bad JSON maps to the standard parse error"
    );
    assert_eq!(parse_err["id"], Value::Null);
    let unknown = &resp[1];
    assert_eq!(unknown["error"]["code"], -32601);
    assert!(unknown["error"]["message"]
        .as_str()
        .unwrap()
        .contains("nosuchmethod"));

    // -- invalid PARAMS on a real method --------------------------------
    let resp = roundtrip(
        &mut child,
        &[r#"{"jsonrpc":"2.0","id":4,"method":"query","params":{"nope":1}}"#],
        1,
    )
    .pop()
    .unwrap();
    assert_eq!(resp["error"]["code"], -32602, "invalid params => -32602");

    // -- semantic failures stay inside result ({"error": ...} shape) ----
    let resp = roundtrip(
        &mut child,
        &[r#"{"jsonrpc":"2.0","id":5,"method":"get_entity","params":{"key":987654}}"#],
        1,
    )
    .pop()
    .unwrap();
    assert!(
        resp["result"]["error"].is_string(),
        "unknown entity key reported as a result-level error, got {}",
        resp
    );

    // -- notification: NO response line (JSON-RPC 2.0 §4) ---------------
    //    (checked indirectly below: the NEXT response carries the NEXT id)
    let resp = roundtrip(
        &mut child,
        &[
            r#"{"jsonrpc":"2.0","method":"schema"}"#, // notification
            r#"{"jsonrpc":"2.0","id":6,"method":"provenance"}"#,
        ],
        1,
    )
    .pop()
    .unwrap();
    assert_eq!(resp["id"], 6, "notification got no response line");
    assert!(resp["result"]["provenance_classes"].is_array());

    // -- retention method is reachable over the wire too ----------------
    let resp = roundtrip(
        &mut child,
        &[r#"{"jsonrpc":"2.0","id":7,"method":"retain","params":{"before":1700000200}}"#],
        1,
    )
    .pop()
    .unwrap();
    // Chunk granularity: only the chunk(s) ENTIRELY older than the cutoff
    // go; the 128-point fixture is one 256-BATCH chunk... which straddles,
    // so NOTHING is removed here (rule: never a partial chunk).
    assert_eq!(resp["result"]["chunks_removed"], 0);

    // Clean shutdown: the polite exit word ends the loop with exit 0.
    child.stdin.as_mut().unwrap().write_all(b"exit\n").unwrap();
    child.stdin.take();
    let status = child.wait().unwrap();
    assert!(status.success(), "service exited cleanly, got {:?}", status);
}

/// A second agent, same DB: audit isolation + log_event round-trip.
#[test]
fn phase82_log_event_and_audit() {
    let tmp = tempfile_dir();
    let db = tmp.join("phase82b.vdg");
    build_fixture(&db);

    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        // Writes log_event (and asserts audit) — writer role required.
        .args([
            "--service",
            db.to_str().unwrap(),
            "--agent-id",
            "ingest-82",
            "--role",
            "writer",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let resp = roundtrip(
        &mut child,
        &[
            r#"{"jsonrpc":"2.0","id":10,"method":"log_event","params":{"name":"alert","entity":"Motor42","timestamp":1700001234,"provenance":5,"details":"relay stuck"}}"#,
            r#"{"jsonrpc":"2.0","id":11,"method":"get_events"}"#,
            r#"{"jsonrpc":"2.0","id":12,"method":"audit"}"#,
        ],
        3,
    );
    assert_eq!(resp[0]["result"]["logged"], true);
    let events = resp[1]["result"]["events"].as_array().unwrap();
    assert_eq!(events.len(), 2, "fixture boot event + the logged one");
    assert!(events
        .iter()
        .any(|e| e["name"] == "alert" && e["entity"] == "Motor42" && e["provenance"] == "Event"));
    // Audit trail records the agent + method (spec §56).
    let entries = resp[2]["result"]["entries"].as_array().unwrap();
    assert!(entries
        .iter()
        .any(|e| e["agent_id"] == "ingest-82" && e["method"] == "log_event"));

    // Malformed params must yield -32602 — and NOT abort the stream.
    let resp = roundtrip(
        &mut child,
        &[
            r#"{"jsonrpc":"2.0","id":13,"method":"log_event"}"#,
            r#"{"jsonrpc":"2.0","id":14,"method":"schema"}"#,
        ],
        2,
    );
    assert_eq!(resp[0]["error"]["code"], -32602);
    assert!(resp[1]["result"].is_object());

    child.stdin.as_mut().unwrap().write_all(b"exit\n").unwrap();
    child.stdin.take();
    let status = child.wait().unwrap();
    assert!(status.success());
}

#[test]
fn phase82_invalid_db_path_is_clean_error() {
    // A nonexistent path must print a clean stderr diagnostic (exit code 2),
    // never a panic / backtrace.
    let out = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args(["--service", "/tmp/definitely-not-here-82/x.vdg"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("cannot open"), "stderr: {}", err);
    assert!(!err.contains("panicked"), "no panic on a bad path");
}

#[test]
fn phase82_version_and_help() {
    for flag in ["--version", "--help"] {
        let out = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
            .arg(flag)
            .output()
            .unwrap();
        assert!(out.status.success());
        let s = String::from_utf8_lossy(&out.stdout);
        assert!(s.contains("vidgedb"), "flag {} produced {}", flag, s);
    }
    let help = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .arg("--help")
        .output()
        .unwrap();
    let txt = String::from_utf8_lossy(&help.stdout);
    assert!(txt.contains("--service") && txt.contains("--retention-days"));
    assert!(txt.contains("retention"));
    assert!(txt.contains("VidgeQL"));
}

/// `smoke` stays accessible as a subcommand (the historical default).
#[test]
fn phase82_smoke_subcommand_still_works() {
    let out = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .arg("smoke")
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("both violations detected"));
}

// -- small tempfile helper (tmp_path + cleanup convention) --------------

fn tempfile_dir() -> std::path::PathBuf {
    let base = std::env::temp_dir().join(format!("vidgedb_phase82_{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    base
}

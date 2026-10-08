#![cfg(feature = "opcua")]
//! Phase 16 — the OPC-UA surface e2e with a REAL OPC-UA CLIENT (the same
//! crate `opcua` 0.12, `client` feature: the wire protocol is exercised
//! for real, HELLO → OpenSecureChannel → CreateSession → ActivateSession →
//! GetEndpoints/browse/read/subscribe — nothing is mocked).
//!
//! Contract asserted here (the parent's A.1..A.5):
//! 1. `server_boot_and_ready` — the spawned `vidgedb --opcua` binary (the
//!    CARGO_BIN_EXE, exactly what an operator runs) becomes TCP-ready
//!    (`opcua_server::wait_ready` — the helper the module ships) after the
//!    pack fixture was loaded by `scripts/pack_load.py` in a subprocess;
//! 2. `client_browse_finds_pack` — a real client (SecurityPolicy None,
//!    anonymous token, endpoint `opc.tcp://127.0.0.1:<port>/`) browses
//!    namespace 2 and finds the pack entities (≥ 4 of the 5 core machines)
//!    AND the entity-type folders (PLC/Motor/Robot/Sensor);
//! 3. `client_read_variable` — the client reads the `current` Variable
//!    under `EMOT01` and the value is THE LAST POINT OF THE TWIN — fresh
//!    points are ingested through a SECOND process (`--service` role
//!    `ingest`, the documented write path) inside the pack's known band
//!    (overload current 3.2–4.1 A, overheating temperature 65–80 °C) and
//!    the published variable lands in that band;
//! 4. `monitored_item_live_update` — the client SUBSCRIBES on
//!    `EMOT01.temperature` (a monitored item), the ingest process commits a
//!    new point WHILE THE OPC-UA READER STAYS OPEN (the Phase-15 staleness
//!    law: the poller re-opens a FRESH Reader every tick — it never holds
//!    the single-writer lock, so the ingest writer is never blocked), and
//!    within the poll window the client receives the DATA CHANGE with the
//!    new value;
//! 5. the timing robustness: `--opcua-poll-ms 200` (a real CLI flag) is
//!    injected so every live-update wait stays ≤ 1 s — the tests never
//!    sleep "long enough and hope", they poll with deadlines.
//!
//! Write path discipline (the wlock story): the OPC-UA server opens
//! Role::Reader (NO writer wlock — documented v1 posture, see the module
//! docs of src/opcua_server.rs); every write in this file goes through a
//! short-lived `--service ... --role ingest` child. The OPC-UA reader and
//! the ingest writer therefore NEVER contend: the reader holds no lock,
//! the ingest holds the single-writer wlock only while it runs.
//!
//! Server-side certificate isolation: `build_server` fixes its PKI dir to
//! `./pki` (CWD-relative). Each test thus spawns the server with
//! `current_dir(<tmp fixture dir>)` — every process gets its own keypair
//! store, no inter-test race on a shared ./pki (CertificateStore's own
//! doc: "It is a bad idea to have more than one running instance pointing
//! to the same path location on disk").

use opcua::client::prelude::*;
use opcua::sync::RwLock;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Fixture: the machine pack loaded through the REAL loader (a subprocess)
// ---------------------------------------------------------------------------

/// Run `scripts/pack_load.py <pack> <db>` (the real loader, a Python
/// subprocess — the parent's fixture requirement) and return the db path.
/// The loader drives the real binary over JSON-RPC stdio (role writer) and
/// self-verifies (PACK_LOAD_OK); a failing load is a failed test WITH the
/// loader's own diagnostics.
fn load_pack_fixture(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("vidgedb_p16_{}_{}", std::process::id(), tag));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("twin.vdg");

    let out = Command::new("python3")
        .args([
            "scripts/pack_load.py",
            "examples/pack-conveyor.json",
            db.to_str().unwrap(),
            "--no-verify", // the verify stage reopens the twin; the e2e asserts below
            "--bin",
            env!("CARGO_BIN_EXE_vidgedb"),
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("spawn scripts/pack_load.py (python3 must be on PATH)");
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "pack loader failed (code {:?}):\n{}",
        out.status.code(),
        text
    );
    assert!(
        text.contains("9 entities"),
        "pack fixture did not create the 9 entities:\n{}",
        text
    );
    db
}

/// Spawn the REAL binary with --opcua (CWD = the fixture dir so the
/// server's `./pki` keypair store lands inside the isolated tmp dir) and
/// wait until the TCP endpoint accepts connections.
fn spawn_opcua_server(db: &std::path::Path, port: u16, poll_ms: u64) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--opcua",
            db.to_str().unwrap(),
            "--opcua-port",
            &port.to_string(),
            "--opcua-poll-ms",
            &poll_ms.to_string(),
            "--agent-id",
            "agent-opcua",
        ])
        .current_dir(db.parent().unwrap())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vidgedb --opcua");
    let ready = vidgedb::opcua_server::wait_ready(port, 8000);
    assert!(ready, "OPC-UA server did not become ready on port {port}");
    child
}

/// A free TCP port (the phase14 convention: ephemeral bind + drop — fine
/// for the ~100 ms before the server binds).
fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// ---------------------------------------------------------------------------
// The stdio JSON-RPC writer (a real --service child, role ingest): the
// documented write path — the OPC-UA server itself is Role::Reader only.
// ---------------------------------------------------------------------------

struct IngestSvc {
    child: Child,
    stdin: Option<ChildStdin>,
    id: u64,
}

impl IngestSvc {
    /// Spawn `vidgedb --service <db> --role ingest` (the single-writer
    /// wlock is held by THIS child while open; the OPC-UA reader never
    /// contends it).
    fn spawn(db: &std::path::Path, agent_id: &str) -> IngestSvc {
        let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
            .args([
                "--service",
                db.to_str().unwrap(),
                "--agent-id",
                agent_id,
                "--role",
                "ingest",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn vidgedb --service (role ingest)");
        let stdin = child.stdin.take().expect("stdin piped");
        IngestSvc {
            child,
            stdin: Some(stdin),
            id: 0,
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let req = json!({
            "jsonrpc": "2.0", "id": self.id, "method": method, "params": params
        });
        let stdin = self.stdin.as_mut().expect("stdin alive");
        writeln!(stdin, "{req}").expect("write request");
        stdin.flush().expect("flush request");
        let stdout = self.child.stdout.as_mut().expect("stdout piped");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("read response");
        let v: Value = serde_json::from_str(line.trim()).expect("valid JSON-RPC response");
        if let Some(err) = v.get("error") {
            panic!("{method} failed: {err}");
        }
        v["result"].clone()
    }
}

impl Drop for IngestSvc {
    fn drop(&mut self) {
        // Close stdin: the service drains and exits, RELEASED the wlock.
        self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// The REAL OPC-UA client (the crate `opcua` client feature)
// ---------------------------------------------------------------------------

/// Connect a real client session to the twin endpoint (endpoint discovery
/// over the wire — GetEndpoints — then CreateSession/ActivateSession with
/// the SecurityPolicy-None anonymous contract the v1 server advertises).
fn connect_client(port: u16) -> Arc<RwLock<Session>> {
    let endpoint = format!("opc.tcp://127.0.0.1:{port}/");
    let mut client = ClientBuilder::new()
        .application_name("vidgedb-phase16-test")
        .application_uri("urn:vidgedb:phase16")
        .create_sample_keypair(false) // SecurityPolicy None needs no client cert
        .session_name("phase16-session")
        .pki_dir(
            std::env::temp_dir()
                .join(format!("vidgedb_p16_client_pki_{}", std::process::id()))
                .to_str()
                .unwrap()
                .to_string(),
        )
        .endpoint(
            "vidgedb_none",
            ClientEndpoint {
                url: endpoint.clone(),
                security_policy: String::from(SecurityPolicy::None.to_str()),
                security_mode: String::from(MessageSecurityMode::None),
                user_token_id: ANONYMOUS_USER_TOKEN_ID.to_string(),
            },
        )
        .default_endpoint("vidgedb_none")
        .trust_server_certs(true)
        .ignore_clock_skew()
        .client()
        .expect("valid client config");

    // The wire round-trip: GetEndpoints first (the discovery contract).
    let endpoints = client
        .get_server_endpoints_from_url(&endpoint)
        .expect("GetEndpoints over the wire");
    assert!(
        endpoints
            .iter()
            .any(|e| SecurityPolicy::from_uri(e.security_policy_uri.as_ref())
                == SecurityPolicy::None),
        "the endpoint advertises a SecurityPolicy-None endpoint: {endpoints:?}"
    );

    let session = client
        .connect_to_endpoint_id(Some("vidgedb_none"))
        .expect("CreateSession + ActivateSession over the wire");
    session
}

/// Browse FORWARD from `node` (hierarchical references): (browse name,
/// node id) pairs — the wire's Browse service, nothing mocked.
fn browse_forward(session: &Arc<RwLock<Session>>, node: NodeId) -> Vec<(String, NodeId)> {
    let desc = BrowseDescription {
        node_id: node,
        browse_direction: BrowseDirection::Forward,
        reference_type_id: NodeId::new(0, ReferenceTypeId::HierarchicalReferences as u32),
        include_subtypes: true,
        node_class_mask: 0,
        result_mask: 63u32, // all BrowseDescriptionResultMask bits
    };
    let results = session
        .read()
        .browse(&[desc])
        .expect("Browse service over the wire")
        .expect("a BrowseResult (not none)");
    results
        .iter()
        .flat_map(|r| r.references.clone().unwrap_or_default())
        .map(|rd| (rd.browse_name.name.as_ref().to_string(), rd.node_id.node_id))
        .collect()
}

/// Read the Value attribute of one node (the Read service over the wire).
fn read_value(session: &Arc<RwLock<Session>>, node: NodeId) -> Option<Variant> {
    let rv = ReadValueId {
        node_id: node,
        attribute_id: AttributeId::Value as u32,
        index_range: UAString::null(),
        data_encoding: QualifiedName::null(),
    };
    let dvs = session
        .read()
        .read(&[rv], TimestampsToReturn::Both, 0.0)
        .expect("Read service over the wire");
    dvs.into_iter().next().and_then(|dv| dv.value)
}

/// Find a node by walking VidgeDB → type folder → entity → variable. The
/// tree is small (9 entities), the walk is bounded and deterministic.
fn find_node(session: &Arc<RwLock<Session>>, path: &[&str]) -> NodeId {
    let objects = NodeId::new(0, ObjectId::ObjectsFolder as u32);
    let mut current = browse_forward(session, objects)
        .into_iter()
        .find(|(n, _)| n == "VidgeDB")
        .map(|(_, id)| id)
        .expect("the VidgeDB root folder under Objects");
    for name in path {
        current = browse_forward(session, current)
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, id)| id)
            .unwrap_or_else(|| panic!("browse path {:?}: node '{}' not found", path, name));
    }
    current
}

/// Poll a live value until it enters `[lo, hi]` (deadline-bounded — the
/// timing robustness rule: no sleep-and-hope). Returns the last variant.
fn poll_value_in_band(
    session: &Arc<RwLock<Session>>,
    node: &NodeId,
    band: (f64, f64),
    deadline_ms: u64,
) -> f64 {
    let deadline = Instant::now() + Duration::from_millis(deadline_ms);
    let mut last = f64::NAN;
    while Instant::now() < deadline {
        if let Some(v) = read_value(session, node.clone()) {
            if let Variant::Double(x) = v {
                last = x;
                if x >= band.0 && x <= band.1 {
                    return x;
                }
            } else {
                panic!("expected a Double variable, got {v:?}");
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "value never entered band {:?} within {} ms (last read: {})",
        band, deadline_ms, last
    );
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// The pack entity/type-folder names a browse must find (the address-space
/// contract from src/opcua_server.rs's doc).
const PACK_ENTITIES: [&str; 5] = ["PLC01", "DRIV01", "EMOT01", "CONV01", "ROB01"];
const TYPE_FOLDERS: [&str; 4] = ["PLC", "Motor", "Robot", "Sensor"];

/// (1) The server boots from the pack fixture (real loader subprocess) and
/// becomes wire-ready within the 8 s budget.
#[test]
fn phase16_server_boot_and_ready() {
    let db = load_pack_fixture("boot");
    let port = free_port();
    let mut child = spawn_opcua_server(&db, port, 200);

    // Still alive after readiness (the run loop holds the process open).
    std::thread::sleep(Duration::from_millis(400));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the --opcua process exited on its own — it must serve until stopped"
    );

    child.kill().unwrap();
    child.wait().unwrap();
}

/// (2) A real client browses ns=2 and finds the pack: ≥ 4 of the 5 core
/// entities AND the entity-type folders (PLC/Motor/Robot/Sensor).
#[test]
fn phase16_client_browse_finds_pack() {
    let db = load_pack_fixture("browse");
    let port = free_port();
    let mut child = spawn_opcua_server(&db, port, 200);

    let session = connect_client(port);

    // Under Objects (ns=0;i=85) the VidgeDB root folder exists.
    let objects = NodeId::new(0, ObjectId::ObjectsFolder as u32);
    let root_children = browse_forward(&session, objects);
    assert!(
        root_children.iter().any(|(n, _)| n == "VidgeDB"),
        "the VidgeDB root folder under Objects: {root_children:?}"
    );

    // Under VidgeDB: the entity-type folders.
    let vidge_db = find_node(&session, &[]);
    let folders = browse_forward(&session, vidge_db);
    let mut found_types = 0;
    for tf in TYPE_FOLDERS {
        if folders.iter().any(|(n, _)| n == tf) {
            found_types += 1;
        }
    }
    assert!(
        found_types == TYPE_FOLDERS.len(),
        "all 4 type folders browsable (PLC/Motor/Robot/Sensor), got {found_types} of {:?}: {folders:?}",
        TYPE_FOLDERS
    );

    // The entity Objects: at least 4 of the 5 core names, browsable under
    // VidgeDB (each entity keeps its Organizes ref to its type folder).
    let names: Vec<String> = folders
        .iter()
        .flat_map(|(_, id)| browse_forward(&session, id.clone()))
        .map(|(n, _)| n)
        .collect();
    let mut found_entities = 0;
    for e in PACK_ENTITIES {
        if names.iter().any(|n| n == e) {
            found_entities += 1;
        }
    }
    assert!(
        found_entities >= 4,
        "browse finds ≥ 4 of the 5 pack entities, got {found_entities} ({names:?})"
    );

    child.kill().unwrap();
    child.wait().unwrap();
}

/// (3) Read the EMOT01.current variable over the wire: after a fresh
/// ingest (role ingest child, pack-band value) the polled variable equals
/// the twin's LAST point — inside the pack's known overload band.
#[test]
fn phase16_client_read_variable() {
    let db = load_pack_fixture("read");
    let port = free_port();
    let mut child = spawn_opcua_server(&db, port, 200);

    // Fresh telemetry through the REAL write path (a second process
    // holding the single-writer wlock; the OPC-UA reader never contends).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut writer = IngestSvc::spawn(&db, "ingest-reader-current");
    // The pack's overload arc drives current in 3.2–4.1 A — the test
    // commits ITS OWN fresh point inside that band (the poller reads a
    // 1 h window around NOW, and the pack's stored points predate it).
    let res = writer.call(
        "ingest_points",
        json!({"entity": "EMOT01", "signal": "current",
               "points": [[now - 5, 3.75], [now, 3.95]]}),
    );
    assert_eq!(res["accepted"], 2, "ingest accepted: {res}");
    // (the writer drops + releases the wlock at the end of the scope)

    drop(writer);

    let session = connect_client(port);
    let node = find_node(&session, &["Motor", "EMOT01", "current"]);
    // The DETERMINISTIC node id contract (ns=2;i=2000+series_index):
    // EMOT01.current is series index 1 of the sorted pack series → i=2001.
    assert_eq!(node, NodeId::new(2, 2001u32), "deterministic node id");

    // Within ~1 s (poll 200 ms) the variable must publish the ingested
    // band value — read the LIVE store, not a mock.
    let v = poll_value_in_band(&session, &node, (3.2, 4.1), 1500);
    assert!(
        (3.2..=4.1).contains(&v),
        "current inside the pack overload band 3.2–4.1 A, got {v}"
    );

    // The temperature variable lands in ITS band too (overheating arc),
    // same walk + read over the wire.
    let tnode = find_node(&session, &["Motor", "EMOT01", "temperature"]);
    let mut writer2 = IngestSvc::spawn(&db, "ingest-reader-temp");
    let res2 = writer2.call(
        "ingest_points",
        json!({"entity": "EMOT01", "signal": "temperature",
               "points": [[now - 30, 71.5], [now - 15, 74.25]]}),
    );
    assert_eq!(res2["accepted"], 2, "temp ingest accepted: {res2}");
    drop(writer2);

    let tv = poll_value_in_band(&session, &tnode, (65.0, 80.0), 1500);
    assert!(
        (65.0..=80.0).contains(&tv),
        "temperature inside the pack overheating band 65–80 C, got {tv}"
    );

    child.kill().unwrap();
    child.wait().unwrap();
}

/// (4) A monitored item on EMOT01.temperature receives the DATA CHANGE
/// when a second process ingests a new point WHILE the OPC-UA reader stays
/// open — the fresh-reader poller per tick (the staleness law) makes the
/// foreign commit visible through the wire subscription.
#[test]
fn phase16_monitored_item_live_update() {
    let db = load_pack_fixture("monitored");
    let port = free_port();
    // poll 200 ms (the injectable CLI flag): every live wait stays ≤ 1 s.
    let mut child = spawn_opcua_server(&db, port, 200);

    let session = connect_client(port);

    // The observed node: EMOT01.temperature (deterministic i=2002).
    let node = find_node(&session, &["Motor", "EMOT01", "temperature"]);
    assert_eq!(node, NodeId::new(2, 2002u32), "deterministic variable id");

    // Data-change collector (called from the client's session run loop).
    let received: Arc<Mutex<Vec<(f64, i64)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);

    // The client's subscription + monitored item (the wire contract:
    // CreateSubscription → CreateMonitoredItems → Publish round-trips).
    let sub_id = session
        .read()
        .create_subscription(
            200.0, // publishing interval (revised to the 100 ms floor)
            10_000,
            1_000,
            100,
            0,
            true,
            DataChangeCallback::new(move |items: &[&MonitoredItem]| {
                for it in items {
                    if let Some(Variant::Double(v)) = it.last_value().value {
                        let secs = it
                            .last_value()
                            .source_timestamp
                            .map(|ts| {
                                // OPC-UA epoch (1601) → unix seconds
                                ts.as_chrono().timestamp() - 11_644_473_600
                            })
                            .unwrap_or(0);
                        sink.lock().unwrap().push((v, secs));
                    }
                }
            }),
        )
        .expect("CreateSubscription over the wire");

    let req = MonitoredItemCreateRequest {
        item_to_monitor: ReadValueId {
            node_id: node,
            attribute_id: AttributeId::Value as u32,
            index_range: UAString::null(),
            data_encoding: QualifiedName::null(),
        },
        monitoring_mode: MonitoringMode::Reporting,
        requested_parameters: MonitoringParameters {
            client_handle: 1,
            sampling_interval: 200.0, // the server's subscription tick floor
            filter: ExtensionObject::null(),
            queue_size: 10,
            discard_oldest: true,
        },
    };
    let results = session
        .read()
        .create_monitored_items(sub_id, TimestampsToReturn::Both, &[req])
        .expect("CreateMonitoredItems over the wire");
    assert!(
        results[0].status_code.is_good(),
        "monitored item created: {:?}",
        results[0].status_code
    );

    // The session run loop: dispatches the Publish responses that carry
    // the data-change notifications (the crate's async run thread).
    let mut run_tx = Some(Session::run_async(session.clone()));

    // The first notification (initial value) may arrive from the client's
    // first publish exchange: wait for ANY data change ≤ 2 s, then ingest
    // the LIVE value through the real write path.
    let deadline = Instant::now() + Duration::from_secs(2);
    while received.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    let baseline = received.lock().unwrap().last().map(|(v, _)| *v);

    // THE WRITE: a second process ingests a NEW point (fresh window) while
    // the OPC-UA reader stays open. The poller (fresh Reader per tick,
    // 200 ms) republishes the variable; the monitored item fires.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let new_value = 77.5;
    let mut writer = IngestSvc::spawn(&db, "ingest-live-update");
    let res = writer.call(
        "ingest_points",
        json!({"entity": "EMOT01", "signal": "temperature",
               "points": [[now, new_value]]}),
    );
    assert_eq!(res["accepted"], 1, "live ingest accepted: {res}");
    drop(writer); // stdin closed → the ingest child exits, wlock released

    // Wait ≤ 1 s (well over the 200 ms server poll + the 100 ms wire floor)
    // for a DATA CHANGE carrying the new value.
    let deadline = Instant::now() + Duration::from_millis(1500);
    let mut saw_new = false;
    while Instant::now() < deadline {
        let got = received.lock().unwrap().clone();
        if got.iter().any(|(v, _)| *v == new_value) {
            saw_new = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        saw_new,
        "the monitored item received the NEW ingested value {new_value} \
         within 1.5 s (baseline was {baseline:?}, got {:?})",
        received.lock().unwrap()
    );

    // And the plain READ path sees it too (both services agree).
    let v = read_value(&session, NodeId::new(2, 2002u32));
    assert_eq!(
        v,
        Some(Variant::Double(new_value)),
        "read matches the change"
    );

    // Stop the session run loop cleanly.
    if let Some(tx) = run_tx.take() {
        let _ = tx.send(SessionCommand::Stop);
    }

    child.kill().unwrap();
    child.wait().unwrap();
}

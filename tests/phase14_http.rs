//! Phase 14 — HTTP JSON-RPC endpoint e2e (spec-less: the remote
//! diagnostics use-case that needs no extra bridge).
//!
//! Spawn the `vidgedb --http` binary (exactly what an operator runs on an
//! edge machine) and speak raw HTTP/1.1 to it — the transport an AI agent,
//! a browser tab, or any generic HTTP client would use. Asserts:
//! - GET /health is a 200 carrying the agent_id,
//! - POST /rpc round-trips the SAME JSON-RPC dispatch as stdio,
//! - non-JSON body → 400 + JSON-RPC `-32700` (parse error),
//! - unknown path → 404, wrong verb → 405,
//! - a Reader role refuses writes over HTTP too (HTTP 403 + `-32003` body),
//! - `--role ingest` CAN ingest through POST /rpc,
//! - `--http-token X` gates EVERY request (401 without the header, 200 with),
//! - the default bind is loopback-only (127.0.0.1) — nothing listens on a
//!   non-loopback address unless the operator explicitly asks,
//! - 20 concurrent POSTs all succeed (one thread per connection behind the
//!   one-mutex engine — the single-writer discipline holds),
//! - data ingested over HTTP SURVIVES a server restart (reopen persistence).

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Minimal std-only HTTP client (the tests' transport; no curl dependency)
// ---------------------------------------------------------------------------

/// Write a whole buffer, retrying WouldBlock (a 10 s write timeout turns
/// a spurious EAGAIN into a panic WITH the reason instead of a hang).
fn write_all_retrying(stream: &mut TcpStream, mut buf: &[u8]) {
    while !buf.is_empty() {
        match stream.write(buf) {
            Ok(0) => panic!("zero write — socket died"),
            Ok(n) => buf = &buf[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("socket write failed: {e}"),
        }
    }
}

/// Read the whole response to EOF, retrying WouldBlock — but ONLY briefly:
/// a repeated EAGAIN means the peer will never send (bad gate, dead server);
/// bound the retries so the test fails fast with a reason instead of hanging.
fn read_response_retrying(stream: &mut TcpStream, out: &mut String) {
    let mut eagain = 0u32;
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // EOF
            Ok(n) => out.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                eagain += 1;
                if eagain > 5 {
                    panic!("read retry budget exhausted (repeated timeout, peer silent)");
                }
                continue;
            }
            Err(e) => panic!("socket read failed: {e}"),
        }
    }
}

/// One HTTP request over a fresh connection; returns (status, body).
fn http(port: u16, method: &str, path: &str, body: &str, extra_headers: &[&str]) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    // The write side needs its own timeout: a response that comes back
    // before our whole request head+body drains must not hang the test.
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let head = format!(
        // Header block MUST end with a blank line (CRLF CRLF). The joined
        // extras carry their own CRLF per line; the final "{ }\r\n" plus
        // this second one closes the header block for both the no-header
        // case (extras empty → "close\r\n\r\n") and the token case.
        "{} {} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        method,
        path,
        body.len(),
        if extra_headers.is_empty() {
            String::new()
        } else {
            format!("{}\r\n", extra_headers.join("\r\n"))
        }
    );
    write_all_retrying(&mut stream, head.as_bytes());
    write_all_retrying(&mut stream, body.as_bytes());
    let mut response = String::new();
    read_response_retrying(&mut stream, &mut response);

    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("HTTP status line");
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// POST /rpc with a raw (already-serialized) body.
fn rpc(port: u16, body: &str) -> (u16, Value) {
    let (status, text) = http(port, "POST", "/rpc", body, &[]);
    let value: Value =
        serde_json::from_str(&text).unwrap_or_else(|_| panic!("rpc body not JSON: {}", text));
    (status, value)
}

/// POST /rpc with a helper-shaped JSON-RPC request.
fn rpc_method(port: u16, id: i64, method: &str, params: Value) -> (u16, Value) {
    let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string();
    rpc(port, &body)
}

/// Wait until /health answers on `port` and echoes the expected agent_id
/// (a connect to a kernel-queued-but-unaccepted port would race the test;
/// poll instead — 10 s budget, 25 ms grain).
fn wait_healthy(port: u16, expect_agent_id: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::time::Instant::now() > deadline {
            panic!("server did not become healthy on port {}", port);
        }
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let req = "GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
            let _ = s.write_all(req.as_bytes());
            let mut resp = String::new();
            if s.read_to_string(&mut resp).is_ok()
                && resp.contains("ok")
                && resp.contains(expect_agent_id)
            {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Pick a free TCP port (the OS hands one out via an ephemeral bind+drop —
/// race-prone in theory, fine in the ~100 ms before the server binds).
fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Build a fixture DB through the engine/stores directly (the same shape
/// the phase82 fixtures use: one Motor feeding one Pump).
fn build_fixture(path: &std::path::Path) {
    use vidgedb::engine::Engine;
    use vidgedb::stores::GraphStore;

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
}

/// Every LISTEN socket on `port`, as dotted-quad strings, parsed from
/// /proc/net/tcp (state column "0A" = LISTEN). Empty => the port has NO
/// listener at all — the test asserts it DOES, and that every entry is
/// 127.0.0.1 (the loopback-only default).
fn loopback_listeners_on(port: u16) -> Vec<String> {
    let proc_tcp = std::fs::read_to_string("/proc/net/tcp").unwrap_or_default();
    let mut out = Vec::new();
    for line in proc_tcp.lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 4 || cols[3] != "0A" {
            continue; // not a LISTEN entry
        }
        let mut parts = cols[1].split(':');
        let addr_hex = match parts.next() {
            Some(a) => a,
            None => continue,
        };
        let port_hex = match parts.next() {
            Some(p) => p,
            None => continue,
        };
        if u16::from_str_radix(port_hex, 16) != Ok(port) {
            continue; // some other server's socket
        }
        // /proc shows the v4 address as an 8-hex-digit u32 whose bytes
        // are the four octets in REVERSED order (127.0.0.1 -> "0100007F",
        // i.e. last octet first). Reverse the byte pairs.
        let hexchars: Vec<char> = addr_hex.chars().collect();
        let start = hexchars.len().saturating_sub(8);
        let last8: Vec<char> = hexchars[start..].to_vec();
        let pairs: Vec<char> = last8
            .chunks(2)
            .rev() // byte order reversal
            .flat_map(|c| c.to_vec())
            .collect();
        let octets: Vec<u8> = (0..4)
            .filter_map(|i| {
                let h: String = pairs[i * 2..i * 2 + 2].iter().collect();
                u8::from_str_radix(&h, 16).ok()
            })
            .collect();
        if octets.len() == 4 {
            out.push(format!(
                "{}.{}.{}.{}:{}",
                octets[0], octets[1], octets[2], octets[3], port
            ));
        }
    }
    out
}

// -- small tempfile helper (tmp_path + cleanup convention) ----------------

fn tempfile_dir() -> std::path::PathBuf {
    let base = std::env::temp_dir().join(format!("vidgedb_phase14_{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    base
}

// ---------------------------------------------------------------------------
// (1) spawn + GET /health 200 (agent_id echoed)
// ---------------------------------------------------------------------------

#[test]
fn phase14_health_and_rpc_roundtrip() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14a.vdg");
    build_fixture(&db);
    let port = free_port();

    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--agent-id",
            "p14-agent",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vidgedb --http");

    wait_healthy(port, "p14-agent");

    let (status, body) = http(port, "GET", "/health", "", &[]);
    assert_eq!(status, 200, "health is a plain 200");
    assert!(body.contains("ok"), "health body says ok: {}", body);
    assert!(
        body.contains("p14-agent"),
        "health carries the agent_id: {}",
        body
    );

    // (2) POST /rpc — a VALID JSON-RPC round-trips to `result`.
    let (status, resp) = rpc_method(port, 1, "schema", json!({}));
    assert_eq!(status, 200);
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    let types: Vec<&str> = resp["result"]["entity_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(types.contains(&"Motor"), "schema over HTTP lists Motor");
    assert!(types.contains(&"Pump"));

    // A VidgeQL query too (the dispatch is the stdio one — full surface).
    let (status, resp) = rpc_method(
        port,
        2,
        "query",
        json!({"vql": "MATCH (m:Motor) WHERE m.name = \"Motor42\" RETURN m"}),
    );
    assert_eq!(status, 200);
    assert_eq!(resp["result"]["rows"][0]["m"]["name"], "Motor42");

    // GET /schema: the browser shortcut returns the schema result itself.
    let (status, body) = http(port, "GET", "/schema", "", &[]);
    assert_eq!(status, 200);
    let schema: Value = serde_json::from_str(&body).unwrap();
    assert!(schema["series"].as_array().unwrap().is_empty());

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (3) non-JSON body → 400 + JSON-RPC error -32700
// ---------------------------------------------------------------------------

#[test]
fn phase14_bad_json_is_parse_error_400() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14b.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args(["--http", db.to_str().unwrap(), "--port", &port.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "agent-http");

    let (status, text) = http(port, "POST", "/rpc", "this is not json at all", &[]);
    assert_eq!(status, 400, "non-JSON body → 400");
    let resp: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(resp["error"]["code"], -32700, "JSON-RPC parse error code");
    assert_eq!(resp["id"], Value::Null);

    // Non-UTF8 body: still a clean parse error (never a panic).
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\xff\xfe")
        .unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).unwrap();
    assert!(
        out.starts_with("HTTP/1.1 400"),
        "binary body → 400: {}",
        out.lines().next().unwrap_or("")
    );

    // The server is ALIVE after the malformed traffic.
    let (status, _) = http(port, "GET", "/health", "", &[]);
    assert_eq!(status, 200);

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (4) unknown path → 404, (5b) wrong verb → 405
// ---------------------------------------------------------------------------

#[test]
fn phase14_routing_404_405() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14c.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args(["--http", db.to_str().unwrap(), "--port", &port.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "agent-http");

    let (status, body) = http(port, "GET", "/definitely-not-here", "", &[]);
    assert_eq!(status, 404, "unknown path → 404");
    assert!(
        body.contains("not found") || body.starts_with("not"),
        "{}",
        body
    );

    let (status, _) = http(port, "GET", "/rpc", "", &[]);
    assert_eq!(status, 405, "GET /rpc → 405 (must be POST)");

    let (status, _) = http(port, "POST", "/health", "", &[]);
    assert_eq!(status, 405, "POST /health → 405 (must be GET)");

    let (status, _) = http(port, "POST", "/rpc", "", &[]);
    assert_eq!(
        status, 400,
        "empty POST /rpc → 400 parse error (not 4xx/5xx confusion)"
    );
    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (5) Reader write over HTTP → HTTP 403 + JSON-RPC -32003 in the body
// ---------------------------------------------------------------------------

#[test]
fn phase14_reader_write_refused_403_32003() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14d.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--role",
            "reader",
            "--agent-id",
            "must-stay-reader",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "must-stay-reader");

    let (status, resp) = rpc_method(
        port,
        7,
        "ingest_points",
        json!({"entity": "Motor42", "signal": "current", "points": [[1700000100, 11.9]]}),
    );
    assert_eq!(status, 403, "reader write → HTTP 403");
    assert_eq!(resp["error"]["code"], -32003, "JSON-RPC write-forbidden");
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Reader"));

    // Reads still work for a reader (role gating, not a service-wide block).
    let (status, resp) = rpc_method(port, 8, "schema", json!({}));
    assert_eq!(status, 200);
    assert!(resp["result"]["entity_types"].is_array());

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (6) ingest_points over HTTP with role ingest → result
// ---------------------------------------------------------------------------

#[test]
fn phase14_ingest_role_ingests_over_http() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14e.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--role",
            "ingest",
            "--agent-id",
            "plc-bridge",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "plc-bridge");

    let (status, resp) = rpc_method(
        port,
        10,
        "ingest_points",
        json!({
            "entity": "Motor42",
            "signal": "vibration",
            "points": [[1700000100, 2.5], [1700000200, 3.0], [1700000300, 2.75]]
        }),
    );
    assert_eq!(status, 200, "ingest role writes over HTTP: {}", resp);
    assert_eq!(resp["result"]["accepted"], 3);
    assert!(resp["result"]["series_id"].is_u64());

    // The write is READABLE through the same endpoint (read-after-write).
    let (status, resp) = rpc_method(
        port,
        11,
        "get_measurements",
        json!({"entity": "Motor42", "signal": "vibration"}),
    );
    assert_eq!(status, 200);
    assert_eq!(resp["result"]["count"], 3, "all 3 points recorded");
    assert_eq!(resp["result"]["max"], 3.0);
    let expected: Vec<Value> = resp["result"]["points"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| json!({"t": p["t"], "value": p["value"]}))
        .collect();
    assert_eq!(expected[2], json!({"t": 1700000300, "value": 2.75}));

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (7) --http-token: 401 without the header, 200 with it
// ---------------------------------------------------------------------------

#[test]
fn phase14_token_gate() {
    const REAL_TOKEN: &str = "s3cr3t-token";
    let tmp = tempfile_dir();
    let db = tmp.join("p14f.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--http-token",
            "s3cr3t-token",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // The token applies to /health TOO — poll with the CORRECT token: the
    // gate compares the full token constant-time, so a truncated probe
    // string ("s3cr3t...") would 401 forever and hang this poll.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::time::Instant::now() > deadline {
            panic!("server did not come up under token auth");
        }
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let req = format!(
                "GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nAuthorization: Bearer {}\r\n\r\n",
                REAL_TOKEN
            );
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let _ = s.write_all(req.as_bytes());
            // Poll-grade reader (an unanswered health poll is a retry, not
            // a test failure): drain to EOF or first error, then decide.
            let mut resp = String::new();
            let mut tmp = [0u8; 4096];
            loop {
                match s.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => resp.push_str(&String::from_utf8_lossy(&tmp[..n])),
                    Err(_) => break,
                }
            }
            if resp.contains("ok") {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    // NO header → 401 for rpc, health, and schema alike.
    let (status, _) = http(
        port,
        "POST",
        "/rpc",
        r#"{"jsonrpc":"2.0","id":1,"method":"schema"}"#,
        &[],
    );
    assert_eq!(status, 401, "no token → 401");
    let (status, _) = http(port, "GET", "/health", "", &[]);
    assert_eq!(status, 401, "health is gated too");
    let (status, _) = http(port, "GET", "/schema", "", &[]);
    assert_eq!(status, 401, "schema is gated too");

    // WRONG token → 401.
    let (status, _) = http(
        port,
        "GET",
        "/health",
        "",
        &["Authorization: Bearer wrong-token"],
    );
    assert_eq!(status, 401, "wrong token → 401");

    // RIGHT token → 200 everywhere.
    let (status, body) = http(
        port,
        "GET",
        "/health",
        "",
        &["Authorization: Bearer s3cr3t-token"],
    );
    assert_eq!(status, 200);
    assert!(body.contains("ok"));
    let (status, resp) = http(
        port,
        "POST",
        "/rpc",
        r#"{"jsonrpc":"2.0","id":1,"method":"schema"}"#,
        &["Authorization: Bearer s3cr3t-token"],
    );
    assert_eq!(status, 200, "correct token → 200 on /rpc");
    let parsed: Value = serde_json::from_str(&resp).unwrap();
    assert!(parsed["result"].is_object(), "schema result: {}", parsed);

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (8) --port is REQUIRED and the default bind is LOOPBACK-ONLY: an
// explicit 127.0.0.1 bind never opens anything non-loopback. The
// non-lookback case (0.0.0.0) stays an explicit operator act — checked
// by (a) the default bind address + (b) a clean refusal when 0.0.0.0
// IS requested but a port is already taken (no half-open state).
// ---------------------------------------------------------------------------

#[test]
fn phase14_default_bind_is_loopback_only() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14g.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args(["--http", db.to_str().unwrap(), "--port", &port.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "agent-http");

    // The bound socket must be 127.0.0.1 — NEVER 0.0.0.0.
    let bound: Vec<String> = loopback_listeners_on(port);
    assert!(
        bound.iter().all(|a| a.starts_with("127.0.0.1")),
        "default bind must be loopback-only, bound: {:?}",
        bound
    );
    assert!(
        !bound.is_empty(),
        "the server's listener port must show in /proc/net/tcp"
    );

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (9) 20 concurrent POSTs all succeed (one thread per connection, ONE
// engine mutex — the single-writer model is preserved end to end)
// ---------------------------------------------------------------------------

#[test]
fn phase14_concurrency_20_posts() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14h.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--role",
            "writer",
            "--agent-id",
            "p14-burst",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "p14-burst");

    let mut handles = Vec::new();
    for i in 0..20 {
        handles.push(std::thread::spawn(move || {
            // Each worker: one ingest POST (a WRITE — exercises serialized
            // commits) + one read POST. Both must succeed.
            let body = json!({
                "jsonrpc": "2.0", "id": i,
                "method": "ingest_points",
                "params": {"entity": "Motor42", "signal": "temp",
                           "points": [[1700000000 + i, 100.0 + i as f64]]}
            })
            .to_string();
            let (s1, r1) = rpc(port, &body);
            let body2 = json!({
                "jsonrpc": "2.0", "id": 100 + i,
                "method": "get_measurements",
                "params": {"entity": "Motor42", "signal": "temp"}
            })
            .to_string();
            let (s2, r2) = rpc(port, &body2);
            (i, s1, s2, r1, r2, body)
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        results.push(h.join().unwrap());
    }
    assert_eq!(results.len(), 20);
    for (i, s1, s2, r1, r2, body) in &results {
        assert_eq!(*s1, 200, "ingest POST #{} must 200 (req={})", i, body);
        assert_eq!(*s2, 200, "read POST #{} must 200", i);
        assert!(r1["result"]["accepted"].is_u64(), "ingest result: {}", r1);
        assert!(r2["result"]["count"].is_u64(), "read result: {}", r2);
    }
    // The total across all 20 ingests is EXACTLY 20 points (no torn
    // concurrent commits, no duplicates, no silent drops).
    let (status, resp) = rpc_method(
        port,
        999,
        "get_measurements",
        json!({"entity": "Motor42", "signal": "temp"}),
    );
    assert_eq!(status, 200);
    assert_eq!(resp["result"]["count"], 20, "20 points committed exactly");

    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------------------------------------------------------------------------
// (10) persistence: data ingested over HTTP SURVIVES a restart
// ---------------------------------------------------------------------------

#[test]
fn phase14_reopen_persistence_after_http_ingest() {
    let tmp = tempfile_dir();
    let db = tmp.join("p14i.vdg");
    build_fixture(&db);
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--role",
            "writer",
            "--agent-id",
            "p14-persist",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "p14-persist");

    let (status, resp) = rpc_method(
        port,
        20,
        "ingest_points",
        json!({
            "entity": "Motor42",
            "signal": "current",
            "points": [[1700000100, 10.5], [1700000200, 10.75]]
        }),
    );
    assert_eq!(status, 200, "ingest: {}", resp);
    assert_eq!(resp["result"]["accepted"], 2);
    child.kill().unwrap();
    child.wait().unwrap();

    // RESTART the server on the SAME .vdg file (a SIGKILL-equivalent stop:
    // the persistence layer must be crash-consistent by design).
    let mut child2 = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
        .args([
            "--http",
            db.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--role",
            "reader",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_healthy(port, "agent-http");

    // The ingested series is still there and still has its points.
    let (status, resp) = rpc_method(
        port,
        21,
        "get_measurements",
        json!({"entity": "Motor42", "signal": "current"}),
    );
    assert_eq!(status, 200, "read after restart: {}", resp);
    assert_eq!(resp["result"]["count"], 2, "points survived the restart");
    assert_eq!(resp["result"]["max"], 10.75);

    child2.kill().unwrap();
    child2.wait().unwrap();
    // The tmp fixture dir is intentionally left for post-mortem debugging
    // (same policy as the phase82 harness).
}

//! VidgeDB HTTP JSON-RPC endpoint — Phase 14 (remote diagnostics).
//!
//! A minimal **HTTP/1.1** server on [`std::net::TcpListener`], serving the
//! SAME JSON-RPC 2.0 method surface as the stdio service ([`crate::service`]
//! — ONE dispatch, one source of truth). It exists because the ecosystem
//! cartography grades the remote-diagnostics
//! use-case A: an AI agent, a browser tab, or any HTTP client should be
//! able to reach a machine twin at distance WITHOUT a local stdio bridge
//! (no subprocess, no IPC plumbing — just `curl`).
//!
//! ## Endpoints (the whole v1 surface)
//! - `POST /rpc` — one JSON-RPC 2.0 request object per POST (body),
//!   one JSON-RPC response object per POST. Same methods, same codes
//!   (-32700/-32600/-32601/-32602/-32000/-32003) as stdio — this module
//!   adds NOTHING to the method table.
//! - `GET /health` — `200` + plain text `ok <agent_id> <role>` (debug).
//! - `GET /schema` — browser-friendly shortcut: runs the `schema` method
//!   and returns its result (plain JSON, not wrapped in an id/result
//!   envelope) for humans poking at a twin from a browser tab.
//! - everything else → `404`; other verbs on those paths → `405`.
//!
//! ## Hard limits (documented, deliberately)
//! - Body cap **16 MiB** (`413` over) — a request body is fully buffered
//!   before parsing; a huge body must not be a memory attack vector.
//! - **No TLS, no custom crypto** (v1): the token check (`--http-token`)
//!   is a shared-secret gate only. Deploy loopback for trust, or wrap the
//!   port in a TCP/TLS terminator (stunnel/nginx) — the documented plan.
//! - Requests are served one per connection (`Connection: close`); an
//!   HTTP/1.1 keep-alive upgrade can come later with zero protocol change.
//!
//! ## Concurrency & the no-panic contract
//! One thread per accepted connection (`std::thread::spawn`) — cheap for
//! the single-digit connection counts remote diagnostics implies. The ONE
//! [`crate::tools::AgentApi`] stays behind a Mutex (the engine's
//! single-writer model is untouched: HTTP adds READ/WRITE *endpoints*,
//! not a second writer). The listener loop itself is panic-proof
//! (`catch_unwind`): a poisoned/panicked connection handler must never
//! kill the server (fail-closed — the connection just gets closed on
//! error, possibly without a response body).
//!
//! ## Security defaults (fail-closed)
//! - Binds **127.0.0.1:8888** by default (loopback-only). `--bind 0.0.0.0`
//!   (any non-loopback bind, in fact) is an explicit operator act.
//! - `--http-token X`: when set, every request MUST carry
//!   `Authorization: Bearer X` — `401` otherwise, checked before anything
//!   else runs (the /health and /schema debug endpoints included).

use crate::service;
use crate::tools::AgentApi;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};

/// Recover the AgentApi from a poisoned mutex (fail-open recovery: the
/// dispatch layer never panics, and the single-writer engine invariant is
/// per-call — a panic between calls left no torn tx). Not custom crypto:
/// plain std.
fn lock_api(api: &Mutex<AgentApi>) -> std::sync::MutexGuard<'_, AgentApi> {
    api.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Request-body cap: 16 MiB (a body over this limit gets 413 before
/// parsing — an ingest batch is at most a few MiB of JSON).
pub const MAX_BODY: usize = 16 * 1024 * 1024;

/// One parsed HTTP/1.1 request head (enough to route + read the body).
struct Request {
    method: String,
    /// Path WITHOUT any query string (`?a=b` dropped — v1 routes exactly).
    path: String,
    /// `Authorization` header value, verbatim (lowercased name lookup).
    authorization: Option<String>,
    /// `Content-Length` as sent (absent → 0; invalid → parse error).
    content_length: usize,
}

/// Failures of the request-head parse, one per rejectable malformation.
enum ParseFail {
    /// No line ever arrived (peer closed before a head).
    Closed,
    /// First line is not `METHOD SP PATH SP HTTP/1.x`.
    MalformedRequestLine,
    /// The path part of the request-line is unusable.
    MalformedPath,
    /// A `Content-Length` header that nohs a decimal integer.
    BadContentLength,
}

/// Read + parse the request head from `r` (byte-buffered). Never
/// panics: every malformed shape maps onto a [`ParseFail`] — and even a
/// head larger than 64 KiB is just a (slow) read that terminates when the
/// peer stops or closes.
fn read_request_head(r: &mut BufReader<std::net::TcpStream>) -> Result<Request, ParseFail> {
    let mut request_line = String::new();
    let n = r
        .read_line(&mut request_line)
        .map_err(|_| ParseFail::Closed)?;
    if n == 0 {
        return Err(ParseFail::Closed); // peer closed idle connection
    }
    let request_line = request_line.trim_end();
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or(ParseFail::MalformedRequestLine)?
        .to_uppercase();
    let raw_path = parts.next().ok_or(ParseFail::MalformedRequestLine)?;
    let version = parts.next().ok_or(ParseFail::MalformedRequestLine)?;
    if !version.starts_with("HTTP/1") {
        return Err(ParseFail::MalformedRequestLine);
    }
    let path = raw_path
        .split('?')
        .next()
        .ok_or(ParseFail::MalformedPath)?
        .to_string();

    let mut authorization = None;
    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        let n = r.read_line(&mut line).map_err(|_| ParseFail::Closed)?;
        if n == 0 {
            return Err(ParseFail::Closed);
        }
        let line = line.trim_end();
        if line.is_empty() {
            break; // end of head
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            match name.as_str() {
                "authorization" => authorization = Some(value),
                "content-length" => {
                    content_length = value.parse().map_err(|_| ParseFail::BadContentLength)?;
                }
                _ => {} // v1 ignores every other header
            }
        }
    }
    Ok(Request {
        method,
        path,
        authorization,
        content_length,
    })
}

/// A minimal, non-panicking HTTP response writer.
///
/// CORS: every response (200, 4xx, preflight) carries
/// `Access-Control-Allow-Origin: *` — the dashboard SDK story (a static
/// page on a dev port talking to the twin's HTTP endpoint) is a
/// first-class client (Phase 14+). The twin exposes no ambient authority
/// (no cookies), so `*` is the safe wildcard here — an `--http-token`
/// deployment keeps its auth in the `Authorization` header, which the
/// preflight lists in `Access-Control-Allow-Headers`.
fn write_response(
    stream: &mut std::net::TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
) {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type, Authorization\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        reason,
        content_type,
        body.len()
    );
    // A half-dead socket just ends the connection: nothing sane to do.
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

/// Serve HTTP on `listener` against the ONE `AgentApi` behind `api`.
///
/// - Thread per connection; the server never panics on a bad peer (the
///   listener loop unwraps handler panics and closes the connection
///   instead — fail-closed).
/// - `token`: when `Some`, EVERY request (health, schema, rpc) must carry
///   `Authorization: Bearer <token>` (401 otherwise) — checked FIRST.
/// - Responses are always one request/one response, no keep-alive (v1).
pub fn serve(
    listener: std::net::TcpListener,
    api: Arc<Mutex<AgentApi>>,
    token: Option<String>,
) -> std::io::Result<()> {
    eprintln!("vidgedb: http endpoint ready on {}", listener.local_addr()?);
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue }; // accept failure: try next
        let api = Arc::clone(&api);
        let token = token.clone();
        std::thread::spawn(move || {
            // A panic inside the handler is contained on the connection
            // thread (fail-closed); the listener loop keeps accepting.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Err(e) = handle_connection(&mut stream, &api, token.as_deref()) {
                    eprintln!("vidgedb: http connection error: {}", e);
                }
            }));
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });
    }
    Ok(())
}

/// The per-connection logic: parse head → auth → route → respond.
fn handle_connection(
    stream: &mut std::net::TcpStream,
    api: &Arc<Mutex<AgentApi>>,
    token: Option<&str>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // -- token gate FIRST (401 before anything else runs) -----------------
    let head = read_request_head(&mut reader);
    if let Some(expected) = token {
        let presented = head.as_ref().ok().and_then(|r| r.authorization.as_deref());
        let ok = presented
            .and_then(|a| a.strip_prefix("Bearer "))
            .map(|t| constant_time_eq(t.as_bytes(), expected.as_bytes()))
            .unwrap_or(false);
        if !ok {
            write_response(
                stream,
                401,
                "Unauthorized",
                "text/plain",
                b"missing or invalid bearer token\n",
            );
            return Ok(());
        }
    }
    let head = match head {
        Ok(h) => h,
        Err(ParseFail::Closed) => return Ok(()), // peer went away: done
        Err(ParseFail::BadContentLength) => {
            write_response(
                stream,
                400,
                "Bad Request",
                "text/plain",
                b"bad Content-Length\n",
            );
            return Ok(());
        }
        Err(_) => {
            write_response(
                stream,
                400,
                "Bad Request",
                "text/plain",
                b"malformed request line\n",
            );
            return Ok(());
        }
    };

    // -- body (cap-checked BEFORE reading: fail-closed on oversize) -------
    if head.content_length > MAX_BODY {
        write_response(
            stream,
            413,
            "Content Too Large",
            "text/plain",
            b"body exceeds 16 MiB limit\n",
        );
        return Ok(());
    }
    let mut body = vec![0u8; head.content_length];
    reader
        .read_exact(&mut body)
        .map_err(std::io::Error::other)?;

    // -- routing ----------------------------------------------------------
    match (head.method.as_str(), head.path.as_str()) {
        // -- CORS preflight (any path): browser dashboards send this before
        // a cross-origin POST /rpc; 204 + the ACAO headers write_response()
        // already emits is the correct empty preflight answer.
        ("OPTIONS", _) => {
            write_response(stream, 204, "No Content", "text/plain", b"");
        }
        ("POST", "/rpc") => {
            let raw = match std::str::from_utf8(&body) {
                Ok(s) => s,
                Err(_) => {
                    // Not UTF-8 text: it cannot be JSON-RPC over HTTP.
                    let err = json!({
                        "jsonrpc": "2.0", "id": null,
                        "error": {"code": -32700, "message": "parse error: body is not UTF-8 text"}
                    });
                    write_response(
                        stream,
                        400,
                        "Bad Request",
                        "application/json",
                        serde_json::to_vec(&err).unwrap_or_default().as_slice(),
                    );
                    return Ok(());
                }
            };
            let mut locked = lock_api(api);
            // The stdio dispatch decides protocol level (`-32700` here) —
            // the wire shape is decided by the TRANSPORT (HTTP status).
            let is_valid_json = serde_json::from_str::<Value>(raw).is_ok();
            let response = if is_valid_json {
                let req: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
                service::dispatch(&mut locked, &req).unwrap_or_else(
                    || json!({"jsonrpc": "2.0", "id": null, "result": {"noted": true}}),
                )
            } else {
                // `-32700 parse error` exactly as the stdio service emits it
                // (id null). The HTTP status (400) is the transport view of
                // the same failure.
                json!({
                    "jsonrpc": "2.0", "id": null,
                    "error": {"code": -32700, "message": "parse error: body is not valid JSON"}
                })
            };
            let body_bytes = serde_json::to_vec(&response)
                .unwrap_or_else(|_| br#"{"error":"serialize"}"#.to_vec());
            let status = if response.get("error").is_some() && !is_role_refusal(&response) {
                400
            } else if is_role_refusal(&response) {
                403
            } else {
                200
            };
            write_response(
                stream,
                status,
                status_reason(status),
                "application/json",
                &body_bytes,
            );
        }
        ("GET", "/health") => {
            let locked = lock_api(api);
            let body = format!("ok {} {:?}\n", locked.agent_id(), locked.role());
            write_response(stream, 200, "OK", "text/plain", body.as_bytes());
        }
        ("GET", "/schema") => {
            let mut locked = lock_api(api);
            match locked.schema() {
                Ok(result) => {
                    let bytes = serde_json::to_vec(&result).unwrap_or_default();
                    write_response(stream, 200, "OK", "application/json", &bytes);
                }
                Err(e) => {
                    let bytes = serde_json::to_vec(&e).unwrap_or_default();
                    write_response(
                        stream,
                        500,
                        "Internal Server Error",
                        "application/json",
                        &bytes,
                    );
                }
            }
        }
        // -- method mismatches on KNOWN paths + unknown routes -----------
        // 405 for a known path hit with the wrong verb, 404 for an
        // unknown path — the route table is intentionally tiny (v1).
        (method, "/rpc") if method != "POST" => {
            write_response(
                stream,
                405,
                "Method Not Allowed",
                "text/plain",
                b"use POST /rpc\\n",
            );
        }
        (method, "/health") if method != "GET" => {
            write_response(
                stream,
                405,
                "Method Not Allowed",
                "text/plain",
                b"use GET /health\\n",
            );
        }
        (method, "/schema") if method != "GET" => {
            write_response(
                stream,
                405,
                "Method Not Allowed",
                "text/plain",
                b"use GET /schema\\n",
            );
        }
        _ => {
            write_response(
                stream,
                404,
                "Not Found",
                "text/plain",
                b"not found (use POST /rpc or GET /health|/schema)\\n",
            );
        }
    }
    Ok(())
}

/// True when the response body is the documented `-32003` write-forbidden
/// refusal (which the HTTP transport surfaces as `403 Forbidden`).
fn is_role_refusal(response: &Value) -> bool {
    response
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(Value::as_i64)
        == Some(service::WRITE_FORBIDDEN_CODE)
}

/// HTTP reason phrase for the statuses this server emits (v1: six).
fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Content Too Large",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

/// Constant-time string comparison (token check). NOT custom crypto — a
/// branch-free byte walk so a timed token oracle is not trivially
/// readable off the wire. Full TLS remains the documented later layer.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

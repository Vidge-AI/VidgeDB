//! VidgeDB service mode — Phase 10 (spec §37 storage bounds + §28 Agent API).
//!
//! A minimal **JSON-RPC 2.0** server over stdin/stdout — the de-facto
//! standard AI agents speak (MCP-compatible): an agent (Python, Node, or a
//! shell wrapper) pilots the database WITHOUT writing any Rust.
//!
//! ## Wire protocol (line-delimited JSON-RPC — deliberately NOT
//! Content-Length framed: trivially parseable from every language)
//!
//! ```text
//! stdin  : {"jsonrpc":"2.0","id":1,"method":"schema"}            \n
//! stdout : {"jsonrpc":"2.0","id":1,"result":{"entity_types":…}}  \n
//! ```
//!
//! - One request per line, one response per line. A notification (request
//!   without `id`) produces NO response line, per JSON-RPC 2.0; a note
//!   goes to stderr instead.
//! - Transport-level failures use the standard codes: -32700 parse,
//!   -32600 invalid request, -32601 method not found, -32602 invalid
//!   params, -32000 storage. Phase 11 adds the server-defined
//!   **-32003 write-forbidden**: the service holds a Reader role and the
//!   method is a writer method (`ingest_points`, `upsert_entity`,
//!   `set_state`, `log_event`, `retain`). Method-level semantic problems
//!   ("unknown entity …") stay inside `result` as the `{"error": "..."}`
//!   shape the [`crate::tools::AgentApi`] v0 contract already defines —
//!   the JSON-RPC `error` member is reserved for the protocol layer.
//!   Either way: ZERO panic on an invalid request (Phase 7 contract
//!   holds).
//! - Params may be an object (by name), an array (positional), or absent
//!   (method defaults fill in, e.g. `now` for `DURING last(...)`).
//! - Serialization uses the already-present `serde_json` ONLY — no new
//!   dependency.
//!
//! ## Methods
//! The full [`crate::tools::AgentApi`] surface plus the write path
//! (Phase 11 role-gated): `schema`, `query`, `query_temporal`,
//! `get_entity`, `get_measurements`, `check`, `trace`, `provenance`,
//! `get_state`, `state_history`, `get_events`, `audit` — reads, every
//! role — plus the WRITES (role writer|ingest only): `ingest_points`,
//! `upsert_entity`, `set_state`, `log_event`, `retain`.

use crate::tools::AgentApi;
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 plumbing
// ---------------------------------------------------------------------------

/// Standard code: malformed JSON on stdin.
const PARSE_ERROR: i64 = -32700;
/// Standard code: not a valid Request object.
const INVALID_REQUEST: i64 = -32600;
/// Standard code: unknown method.
const METHOD_NOT_FOUND: i64 = -32601;
/// Standard code: bad/missing params.
const INVALID_PARAMS: i64 = -32602;
/// Standard code: storage/Engine error surfaced by the method.
const STORAGE_ERROR: i64 = -32000;
/// Server-defined (Phase 11, documented §55 derrogation): a WRITE method
/// was invoked by a service opened with `--role reader`. The refusal is
/// audited inside the AgentApi (`require_writer`) before this code is
/// emitted; nothing was written.
pub const WRITE_FORBIDDEN_CODE: i64 = WRITE_FORBIDDEN;

const WRITE_FORBIDDEN: i64 = crate::tools::ERR_WRITE_FORBIDDEN;

fn ok_response(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message.into() } })
}

fn unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Handle ONE parsed request against `api`. `None` = notification, and a
/// note goes to stderr. Never panics — every malformed input maps to a
/// standard JSON-RPC error object.
pub fn dispatch(api: &mut AgentApi, req: &Value) -> Option<Value> {
    // A non-object Request body is invalid per JSON-RPC 2.0 §4.
    if !req.is_object() {
        return Some(err_response(
            Value::Null,
            INVALID_REQUEST,
            "request must be a JSON object",
        ));
    }
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let notification = req.get("id").is_none();
    let method = match req.get("method").and_then(Value::as_str) {
        Some(m) => m.to_string(),
        None => {
            return Some(err_response(
                id,
                INVALID_REQUEST,
                "missing \"method\" string",
            ));
        }
    };
    let outcome = run_method(api, &method, req.get("params"));
    let response = match outcome {
        Ok(result) => ok_response(id, result),
        Err(e) => err_response(id, e.code, e.message),
    };
    if notification {
        eprintln!(
            "vidgedb: notification \"{}\" handled (no response per JSON-RPC)",
            method
        );
        None
    } else {
        Some(response)
    }
}

/// Transport-level dispatch failure (becomes the JSON-RPC `error` member).
struct TErr {
    code: i64,
    message: String,
}

impl TErr {
    fn new(code: i64, msg: impl Into<String>) -> Self {
        TErr {
            code,
            message: msg.into(),
        }
    }
}

/// Run one method against the API.
///
/// `Ok(Value)` = method result (which may itself carry the AgentApi v0
/// `{"error": …}` semantic shape); `Err(TErr)` = transport failure (bad
/// params, unknown method, storage error).
fn run_method(api: &mut AgentApi, method: &str, params: Option<&Value>) -> Result<Value, TErr> {
    match method {
        // ---- inventory / read paths -----------------------------------
        "schema" => api.schema().map_err(tstorage),
        // Phase 93 : le contrat plateforme (inventaire, graphe, diagnostic).
        "list_entities" | "entities" => {
            // `entities` = alias, parce que c'est le nom que les gens tapent.
            let type_filter = str_param_opt(params, "type", 0).map_err(tparam)?;
            let limit = limit_param(params, 1).map_err(tparam)?;
            api.list_entities(type_filter.as_deref(), limit)
                .map_err(tstorage)
        }
        "graph" => {
            let limit = limit_param(params, 0).map_err(tparam)?;
            api.graph(limit).map_err(tstorage)
        }
        "diagnose" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let from = i64_opt(params, "from", 1).map_err(tparam)?.unwrap_or(0);
            let to = i64_opt(params, "to", 2)
                .map_err(tparam)?
                .unwrap_or(i64::MAX / 2);
            api.diagnose(&entity, from, to).map_err(tstorage)
        }
        // Phase 93 : le refus existait deja (`tools.rs`), mais n'etait pas
        // branche au dispatch -> l'appelant recevait un `-32601 unknown
        // method` opaque au lieu du refus MOTIVE et audite. Le refus reste un
        // `result.error` (lisible par machine), pas une erreur de transport.
        "set_hypothesis" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let text = str_param(params, "text", 1).map_err(tparam)?;
            api.set_hypothesis(&entity, &text).map_err(tstorage)
        }
        "query" => {
            let vql = str_param(params, "vql", 0).map_err(tparam)?;
            api.query(&vql).map_err(tstorage)
        }
        "query_temporal" => {
            let vql = str_param(params, "vql", 0).map_err(tparam)?;
            let now = i64_opt(params, "now", 1)
                .map_err(tparam)?
                .unwrap_or_else(unix_secs);
            api.query_temporal(&vql, now).map_err(tstorage)
        }
        "get_entity" => {
            // Accept both shapes agents emit: {"key": 42} (number) and
            // {"key": "42"} (integer-valued string).
            let key: u32 = match params.and_then(|p| param(Some(p), "key", 0)) {
                Some(Value::Number(n)) => n
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .ok_or_else(|| TErr::new(INVALID_PARAMS, "\"key\": not a u32 integer"))?,
                Some(Value::String(s)) => s.parse::<u32>().map_err(|_| {
                    TErr::new(
                        INVALID_PARAMS,
                        format!("\"key\" must be an unsigned integer, got \"{}\"", s),
                    )
                })?,
                _ => return Err(TErr::new(INVALID_PARAMS, "\"key\": missing param")),
            };
            api.get_entity(key).map_err(tstorage)
        }
        "get_measurements" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let signal = str_param(params, "signal", 1).map_err(tparam)?;
            let from = i64_opt(params, "from", 2).map_err(tparam)?.unwrap_or(0);
            let to = i64_opt(params, "to", 3)
                .map_err(tparam)?
                .unwrap_or(i64::MAX / 2);
            api.get_measurements(&entity, &signal, from, to)
                .map_err(tstorage)
        }
        "check" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let signal = str_param(params, "signal", 1).map_err(tparam)?;
            let from = i64_opt(params, "from", 2).map_err(tparam)?.unwrap_or(0);
            let to = i64_opt(params, "to", 3)
                .map_err(tparam)?
                .unwrap_or(i64::MAX / 2);
            api.check(&entity, &signal, from, to).map_err(tstorage)
        }
        "trace" => {
            let from = str_param(params, "from", 0)
                .or_else(|_| str_param(params, "from_name", 0))
                .map_err(tparam)?;
            let to = str_param(params, "to", 1)
                .or_else(|_| str_param(params, "to_name", 1))
                .map_err(tparam)?;
            let hops = i64_opt(params, "max_hops", 2).map_err(tparam)?.unwrap_or(6);
            let hops = hops.clamp(0, 255) as usize;
            api.trace(&from, &to, hops).map_err(tstorage)
        }
        "provenance" => api.provenance().map_err(tstorage),
        // ---- state & events --------------------------------------------
        "get_state" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let key = str_param(params, "key", 1).map_err(tparam)?;
            let at = i64_opt(params, "at", 2).map_err(tparam)?;
            api.get_state(&entity, &key, at).map_err(tstorage)
        }
        "state_history" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let key = str_param(params, "key", 1).map_err(tparam)?;
            api.state_history(&entity, &key).map_err(tstorage)
        }
        "log_event" => {
            let name = str_param(params, "name", 0).map_err(tparam)?;
            let entity = str_param(params, "entity", 1).map_err(tparam)?;
            let ts = i64_opt(params, "timestamp", 2)
                .map_err(tparam)?
                .unwrap_or_else(unix_secs);
            let prov = i64_opt(params, "provenance", 3)
                .map_err(tparam)?
                .unwrap_or(1); // Observation
            let details = params
                .and_then(|p| param(Some(p), "details", 4))
                .and_then(Value::as_str)
                .unwrap_or("");
            api.log_event(&name, &entity, ts, prov as u8, details)
                .map_err(tstorage)
        }
        "get_events" => {
            let entity: Option<String> = match params.and_then(|p| param(Some(p), "entity", 0)) {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => {
                    return Err(TErr::new(
                        INVALID_PARAMS,
                        "\"entity\": must be a string or null",
                    ));
                }
            };
            let from = i64_opt(params, "from", 1)
                .map_err(tparam)?
                .unwrap_or(i64::MIN / 2);
            let to = i64_opt(params, "to", 2)
                .map_err(tparam)?
                .unwrap_or(i64::MAX / 2);
            api.get_events(entity.as_deref(), from, to)
                .map_err(tstorage)
        }
        // ---- audit & retention -----------------------------------------
        "audit" => {
            let entries = api.audit_log();
            let out: Vec<Value> = entries
                .iter()
                .map(|e| {
                    json!({
                        "timestamp": e.timestamp,
                        "agent_id": e.agent_id,
                        "method": e.method,
                        "params_summary": e.params_summary,
                    })
                })
                .collect();
            let n = out.len();
            Ok(json!({ "entries": out, "n": n }))
        }
        // ---- Phase 11 — the ingestion path (role-gated) ----------------
        "ingest_points" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let signal = str_param(params, "signal", 1).map_err(tparam)?;
            let points = points_param(params).map_err(tparam)?;
            if points.is_empty() {
                // Empty arrays are refused CLEANLY (-32602), never a
                // silent no-op with a fabricated success payload.
                return Err(TErr::new(
                    INVALID_PARAMS,
                    "\"points\": must be a non-empty array of [timestamp, value] pairs",
                ));
            }
            api.ingest_points(&entity, &signal, &points)
                .map_err(twrite_or_storage)
        }
        "upsert_entity" => {
            let name = str_param(params, "name", 0).map_err(tparam)?;
            let entity_type = str_param(params, "type", 1).map_err(tparam)?;
            let props = props_param(params).map_err(tparam)?;
            let relations = relations_param(params).map_err(tparam)?;
            let source = str_param_opt(params, "source", 4)
                .map_err(tparam)?
                // Fact by default (official machine source, spec §29)
                .unwrap_or_else(|| "plc".to_string());
            api.upsert_entity(&name, &entity_type, &props, &relations, &source)
                .map_err(twrite_or_storage)
        }
        "set_state" => {
            let entity = str_param(params, "entity", 0).map_err(tparam)?;
            let key = str_param(params, "key", 1).map_err(tparam)?;
            let value = str_param(params, "value", 2).map_err(tparam)?;
            let at = i64_opt(params, "at", 3).map_err(tparam)?;
            api.set_state(&entity, &key, &value, at)
                .map_err(twrite_or_storage)
        }
        // ---- retention (Phase 11: a WRITE — Writer/Ingest only) --------
        "retain" => {
            let before = i64_opt(params, "before", 0)
                .map_err(tparam)?
                .unwrap_or_else(unix_secs);
            api.retain(before).map_err(twrite_or_storage)
        }
        _ => Err(TErr::new(
            METHOD_NOT_FOUND,
            format!("unknown method \"{}\"", method),
        )),
    }
}

/// Storage failure -> JSON-RPC error.
fn tstorage(e: crate::tools::AgentApiError) -> TErr {
    TErr::new(STORAGE_ERROR, format!("storage error: {}", e.error))
}

/// Failure of a role-gated write method (Phase 11): a Reader refusal maps
/// onto the documented -32003 (its message carries the -32003 marker),
/// everything else is a storage error -32000.
fn twrite_or_storage(e: crate::tools::AgentApiError) -> TErr {
    if e.error.contains("-32003") {
        TErr::new(WRITE_FORBIDDEN, e.error)
    } else {
        TErr::new(STORAGE_ERROR, format!("storage error: {}", e.error))
    }
}

/// Param failure -> JSON-RPC error (standard -32602).
fn tparam(why: &'static str) -> TErr {
    TErr::new(INVALID_PARAMS, why)
}

// ---------------------------------------------------------------------------
// Params normalization
// ---------------------------------------------------------------------------

/// `params`: object (by name), array (positional), or absent => None.
fn param<'a>(params: Option<&'a Value>, name: &str, pos: usize) -> Option<&'a Value> {
    match params? {
        Value::Object(m) => m.get(name),
        Value::Array(a) => a.get(pos),
        _ => None,
    }
}

fn str_param(params: Option<&Value>, name: &str, pos: usize) -> Result<String, &'static str> {
    match params.and_then(|p| param(Some(p), name, pos)) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(_) => Err("param must be a string"),
        None => Err("missing param"),
    }
}

/// Integer (or integer-valued string) param; absent/null => Ok(None).
fn i64_opt(params: Option<&Value>, name: &str, pos: usize) -> Result<Option<i64>, &'static str> {
    match params.and_then(|p| param(Some(p), name, pos)) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => Ok(n.as_i64()),
        Some(Value::String(s)) => Ok(s.parse::<i64>().ok()),
        Some(_) => Err("param must be an integer (or integer-valued string)"),
    }
}

/// Optional string param; absent/null => Ok(None).
fn str_param_opt(
    params: Option<&Value>,
    name: &str,
    pos: usize,
) -> Result<Option<String>, &'static str> {
    match params.and_then(|p| param(Some(p), name, pos)) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err("param must be a string"),
    }
}

/// Optional result-count cap. Absent/null => 0 (no cap).
///
/// A negative value is refused rather than cast: `-1 as usize` would become
/// a gigantic bound, i.e. silently "no cap" while looking like a cap.
fn limit_param(params: Option<&Value>, pos: usize) -> Result<usize, &'static str> {
    match i64_opt(params, "limit", pos)? {
        None => Ok(0),
        Some(n) if n < 0 => Err("\"limit\" must be >= 0 (0 = no cap)"),
        Some(n) => Ok(n as usize),
    }
}
// ---------------------------------------------------------------------------
// Phase 11 — structured param normalization (strict typing, clean refusals)
// ---------------------------------------------------------------------------

/// `points`: array of `[ts, value]` pairs (object form also accepted:
/// `{"ts":…, "value":…}`). A malformed pair is a clean -32602, never a
/// panic and never a silently skipped point.
fn points_param(params: Option<&Value>) -> Result<Vec<(i64, f64)>, &'static str> {
    let arr = match params.and_then(|p| param(Some(p), "points", 2)) {
        None | Some(Value::Null) => return Err("missing \"points\" array"),
        Some(Value::Array(a)) => a,
        Some(_) => return Err("\"points\" must be an array of [ts, value] pairs"),
    };
    let mut out = Vec::with_capacity(arr.len());
    for p in arr.iter() {
        let (t, v) = match p {
            Value::Array(pair) if pair.len() == 2 => match (&pair[0], &pair[1]) {
                // NOTE: integer+number — a JSON float ts like 1700000000.5
                // is REFUSED (ts must be an integer unix time); a value
                // given as "1.5" (a string) is refused too. Clean -32602.
                (Value::Number(t), Value::Number(v)) => match (t.as_i64(), v.as_f64()) {
                    (Some(t), Some(v)) => (t, v),
                    _ => {
                        return Err(
                            "points: ts must be an integer, value must be a number (pair form [ts, value])",
                        )
                    }
                },
                _ => {
                    return Err(
                        "points: ts must be an integer, value must be a number (pair form [ts, value])",
                    )
                }
            },
            Value::Object(m) => {
                let t = m
                    .get("ts")
                    .or_else(|| m.get("timestamp"))
                    .and_then(Value::as_i64);
                let v = m.get("value").and_then(Value::as_f64);
                match (t, v) {
                    (Some(t), Some(v)) => (t, v),
                    _ => {
                        return Err(
                            "points: object pairs need integer \"ts\" + number \"value\"",
                        )
                    }
                }
            }
            _ => return Err("points: expected a [ts, value] pair"),
        };
        out.push((t, v));
    }
    Ok(out)
}

/// Properties: object `{"k":"v", "opt":null}` or array of `[k, v]` pairs
/// (`v` may be null = present-without-value, merged as "" and NEVER
/// erasing an existing prop). Returns named pairs.
fn props_param(params: Option<&Value>) -> Result<Vec<(String, Option<String>)>, &'static str> {
    let pv = match params.and_then(|p| param(Some(p), "props", 2)) {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(v) => v,
    };
    let mut out: Vec<(String, Option<String>)> = Vec::new();
    match pv {
        Value::Object(m) => {
            for (k, v) in m {
                out.push((k.clone(), v.as_str().map(|s| s.to_string())));
            }
        }
        Value::Array(a) => {
            for p in a {
                match p {
                    Value::Array(pair) if pair.len() == 2 => {
                        let k = pair[0].as_str().ok_or("props: key must be a string")?;
                        let v = pair[1].as_str().map(|s| s.to_string());
                        out.push((k.to_string(), v));
                    }
                    _ => return Err("props: array form must be [key, value] pairs"),
                }
            }
        }
        _ => return Err("props: must be an object or an array of [k, v] pairs"),
    }
    Ok(out)
}

/// Relations: array of `{to, relation_type, valid_from?}` objects (or
/// positional `[to, relation_type]` pairs). `relation_type` MUST carry the
/// topology prefix (`"electrical:feeds"` — identity rule, spec §8).
fn relations_param(
    params: Option<&Value>,
) -> Result<Vec<crate::tools::RelationSpec>, &'static str> {
    let rv = match params.and_then(|p| param(Some(p), "relations", 3)) {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(v) => v,
    };
    let arr = match rv {
        Value::Array(a) => a,
        _ => return Err("relations: must be an array of {to, relation_type} objects"),
    };
    let mut out: Vec<crate::tools::RelationSpec> = Vec::with_capacity(arr.len());
    for rel in arr {
        let (to, relation_type, valid_from) = match rel {
            Value::Object(m) => {
                let to = m
                    .get("to")
                    .and_then(Value::as_str)
                    .ok_or("relations: every entry needs a string \"to\" (target entity name)")?
                    .to_string();
                let rt = m
                    .get("relation_type")
                    .and_then(Value::as_str)
                    .ok_or(
                        "relations: every entry needs a string \"relation_type\" ('topology:type')",
                    )?
                    .to_string();
                let vf = match m.get("valid_from") {
                    None | Some(Value::Null) => None,
                    Some(Value::Number(n)) => match n.as_i64() {
                        Some(v) => Some(v),
                        None => return Err("relations: \"valid_from\" must be an integer"),
                    },
                    Some(Value::String(s)) => match s.parse::<i64>() {
                        Ok(v) => Some(v),
                        Err(_) => return Err("relations: \"valid_from\" must be an integer"),
                    },
                    Some(_) => return Err("relations: \"valid_from\" must be an integer"),
                };
                (to, rt, vf)
            }
            Value::Array(pair) if pair.len() == 2 => {
                let to = pair[0]
                    .as_str()
                    .ok_or("relations: positional form is [to, relation_type]")?;
                let relation_type = pair[1]
                    .as_str()
                    .ok_or("relations: positional form is [to, relation_type]")?;
                (to.to_string(), relation_type.to_string(), None)
            }
            _ => return Err("relations: entries must be objects {to, relation_type, valid_from?}"),
        };
        if !relation_type.contains(':') {
            return Err(
                "relations: \"relation_type\" must be 'topology:type' (e.g. 'electrical:feeds') \
— the topology prefix is part of relation identity (spec §8)",
            );
        }
        out.push(crate::tools::RelationSpec {
            to,
            relation_type,
            valid_from,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// The stdio loop
// ---------------------------------------------------------------------------

/// Serve one database over `input`/`output` (a `BufRead`/`Write` pair).
///
/// - Line-delimited JSON-RPC on `input`, one response line on `output`
///   (none for notifications).
/// - `prompt` (interactive TTY mode only): a human at the terminal gets a
///   `vidgedb> ` prompt on stderr before each read; pipes stay silent.
/// - A malformed JSON line => `-32700` with `id: null`, then the service
///   keeps listening (never aborts the stream — an agent may recover).
/// - EOF or the literal word `exit`/`quit`/`shutdown` ends the loop
///   cleanly (REPL convenience).
///
/// Returns how many requests were served (diagnostics).
pub fn serve<R: BufRead, W: Write>(
    api: &mut AgentApi,
    input: R,
    output: &mut W,
    prompt: bool,
) -> std::io::Result<usize> {
    let mut n_served: usize = 0;
    let mut lines = input.lines();
    let byemsg = json!({ "jsonrpc": "2.0", "id": null, "result": { "bye": true } });
    loop {
        if prompt {
            let mut err = std::io::stderr();
            err.write_all(b"vidgedb> ")?;
            err.flush()?;
        }
        let line = match lines.next() {
            Some(l) => l?,
            None => break, // EOF: pipe closed normally
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Human (or machine) courtesy exit words.
        if trimmed == "exit" || trimmed == "quit" || trimmed == "shutdown" {
            if !prompt {
                // Machine-visible ack, JSON-RPC-shaped.
                writeln!(output, "{}", byemsg)?;
                output.flush()?;
            }
            break;
        }
        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                // Bad JSON: respond with the parse error and CONTINUE
                // (never abort the stream — an agent may recover).
                let resp = err_response(Value::Null, PARSE_ERROR, format!("parse error: {}", e));
                writeln!(output, "{}", resp)?;
                output.flush()?;
                n_served += 1;
                continue;
            }
        };
        if let Some(resp) = dispatch(api, &req) {
            writeln!(output, "{}", resp)?;
            output.flush()?;
        }
        n_served += 1;
    }
    Ok(n_served)
}

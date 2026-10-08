//! VidgeDB OPC-UA server — Phase 16 (the machine-integration surface).
//!
//! Exposes a VidgeDB twin as an **OPC-UA server** (crate `opcua` 0.12.0):
//! an industrial client (Siemens TIA Portal, AVEVA, KEPServerEX, UaExpert…)
//! browses the twin like any machine and subscribes to live telemetry with
//! monitored items — the gateway-world integration.
//!
//! ## Browse tree (the documented address-space contract)
//!
//! Everything lives in `namespace 2` (registered as
//! `urn:vidgedb:<db-file-stem>:twin`):
//! - **Root**: folder `VidgeDB` organized under `ObjectsFolder` (ns=0;i=85);
//! - one **folder per entity type** (`PLC`, `Motor`, …) under `VidgeDB`;
//! - one **Object per entity**, browse name = the VidgeDB name (e.g.
//!   `PLC01`), organized under its type folder — the object IS the twin's
//!   topology node (node id `ns=2;i=<1000+entity_key>`, deterministic — a
//!   client can pin them);
//! - **relations become the OPC-UA hierarchy**: for every live relation
//!   `src -[topo:type]-> dst` the destination entity's Object is added
//!   under the source entity's Object with `HasComponent` — a client
//!   browsing `PLC01` finds `DRIV01` and `ROB01` beneath it (parent =
//!   the network/mechanical/electrical SOURCE, child = the DESTINATION);
//! - **signals become Variables** attached with `HasComponent` to the
//!   Object of the entity the series is ABOUT (series naming =
//!   `<owner>.<signal>`, the pack ownership rule: drive readbacks land on
//!   the motor series, sensor-local series stay sensor-local);
//!   `Double` for analog measurements, `Boolean` for discrete 0/1
//!   signals (node id `ns=2;i=<2000+series_index>`);
//! - **EngineeringUnit**: when the twin carries a `spec.<signal>.unit`
//!   entity prop, the Variable gets an `EngineeringUnit` property
//!   (`HasProperty` — EUInformation extension object, UA unit ids for the
//!   common electrical/thermal units). The pack's 44-byte props cap means
//!   v1 twins carry at most one such unit; absent unit ⇒ NO
//!   EngineeringUnit served rather than a wrong one.
//!
//! ## Telemetry (the re-open polling loop)
//!
//! A polling thread runs at `poll_ms` (CLI `--opcua-poll-ms`, default
//! 1000). Each tick it opens a FRESH `AgentApi` (Role::Reader), calls
//! `get_measurements` over a recent window and writes the LAST point of
//! every series into its Variable (source timestamp = the point's own
//! unix time). A subscribed client sees the change through monitored
//! items — the crate's subscription timer evaluates at 100 ms
//! granularity, well under any sane poll period.
//!
//! WHY a fresh open per tick: the documented Phase 15 staleness law — a
//! long-lived reader NEVER sees another process's commits (its slab /
//! chunk index is an open-time snapshot); a reopen rebuilds them from
//! the durable file and sees the whole history (`open is cheap:
//! superblock + slabs rebuild` — the SAME pattern as the CLI's own
//! hourly-retention thread). The tick's Reader open takes NO write lock,
//! so an outer `--http writer` (or a `--service ingest` loader) commits
//! unimpeded; the OPC-UA poller never contends the single-writer wlock.
//!
//! ## Security policy v1 (the DOCUMENTED posture)
//!
//! **`SecurityPolicy::None` only, anonymous user tokens only** — the
//! explicit v1 contract, same class as brownfield PLC endpoints: NO
//! encryption, NO signature, NO user auth on this endpoint. ANYONE on its
//! network can read the twin. DEPLOY RULE: keep the endpoint on an
//! ISOLATED machine LAN (cell VLAN / firewall), never route it beyond the
//! line. Certificates + signed endpoints are the v2 migration (the v1
//! boot already generates the application keypair, so v2 changes endpoint
//! config only). Read-only by design: the API opens `Role::Reader` and
//! no node is writable.
//!
//! ## Failure posture (the no-panic contract)
//!
//! Store errors never crash the server: a failed measurement read logs to
//! stderr and the poll continues ("JAMAIS panic sur une erreur store —
//! log stderr + continue"). A poisoned std mutex recovers through the
//! same `unwrap_or_else(PoisonError::into_inner)` rule as the HTTP layer.
//!
//! ## Sharing the twin with the other servers
//!
//! `vidgedb db.vdg --http db.vdg --opcua ...` runs TWO servers (HTTP
//! JSON-RPC + OPC-UA) over the SAME twin — the HTTP side holds the one
//! long-lived `Arc<Mutex<AgentApi>>` (the P14 pattern); the OPC-UA side
//! re-opens its own Reader `AgentApi` every poll tick. OPC-UA never
//! contends the single-writer lock (its opens are Reader) and never
//! blocks the HTTP writer for longer than one open+query.

use crate::tools::{AgentApi, Role};
use opcua::server::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// The crate's RwLock (parking_lot behind `opcua::sync`): the server and
/// its address space are shared this way in every opcua-server example.
type RwLock<T> = opcua::sync::RwLock<T>;

/// Recover a poisoned std mutex (same fail-open rule as the HTTP layer:
/// `dispatch` never panics between calls, so the guard is always sound).
fn lock_api(api: &Mutex<AgentApi>) -> std::sync::MutexGuard<'_, AgentApi> {
    api.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Namespace URI of the twin address space (deterministic, derived from
/// the database file stem — a client can pin it).
fn namespace_uri(db_path: &str) -> String {
    let stem = std::path::Path::new(db_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("twin");
    format!("urn:vidgedb:{}:twin", stem)
}

/// One Variable to refresh: the OPC-UA metadata for one twin series.
#[derive(Clone)]
struct SignalVar {
    /// VidgeDB series name (`<owner_entity>.<signal>`).
    series: String,
    /// Index in the sorted `schema.series` list (drives the node id).
    series_index: u32,
    node: NodeId,
    /// `true` ⇒ Boolean (discrete 0/1), `false` ⇒ Double (analog).
    discrete: bool,
    /// Declared unit (`spec.<signal>.unit` present) → EngineeringUnit.
    unit: Option<String>,
}

/// Node-id constants (deterministic address space, documented above).
const ROOT_FOLDER_ID: u32 = 1;
const TYPE_FOLDER_BASE: u32 = 100;
const ENTITY_BASE: u32 = 1000;
const VARIABLE_BASE: u32 = 2000;

/// Build the server: config + address space from the twin. The AgentApi
/// is read ONCE to lay out the tree (entities/relations/signals — values
/// start at neutral defaults); the poller refreshes values afterwards.
/// Returns `(endpoint_url, namespace_index, variables)` with the server
/// already wrapped for sharing, or an error string (never panics — the
/// caller prints and exits non-zero).
fn build_server(
    api: &Arc<Mutex<AgentApi>>,
    db_path: &str,
    port: u16,
) -> Result<(Server, String, u16, Vec<SignalVar>), String> {
    // ---- read the twin (all through the shared API) ----------------------
    let schema = {
        let mut api = lock_api(api);
        api.schema().map_err(|e| format!("schema: {}", e.error))?
    };
    let series_names: Vec<String> = schema
        .get("series")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // Entity inventory: key -> (name, type). Entity keys are dense cell
    // indices 0..entity_count; walk exactly that count.
    let entity_count = {
        let mut api = lock_api(api);
        api.entity_count()
    };
    let mut entities: Vec<(u32, String, String)> = Vec::new();
    let mut key_by_name: HashMap<String, u32> = HashMap::new();
    for k in 0..entity_count {
        let got = {
            let mut api = lock_api(api);
            api.get_entity(k)
        };
        if let Ok(v) = got {
            if v.get("error").is_some() {
                continue; // an empty cell never stops the walk
            }
            let name = v
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let ty = v
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();
            if !name.is_empty() {
                key_by_name.insert(name.clone(), k);
                entities.push((k, name, ty));
            }
        }
    }
    if entities.is_empty() {
        return Err("twin holds no entities — nothing to expose".to_string());
    }
    if series_names.is_empty() {
        eprintln!("vidgedb: opcua: twin holds no series — objects only, no variables");
    }

    // Relations (live, src -> dst) from each entity's relations_out.
    let mut edges: Vec<(String, String)> = Vec::new();
    for (_, name, _) in &entities {
        let key = key_by_name[name];
        let got = {
            let mut api = lock_api(api);
            api.get_entity(key)
        };
        if let Ok(v) = got {
            if let Some(rels) = v.get("relations_out").and_then(|r| r.as_array()) {
                for r in rels {
                    if let Some(to) = r.get("to").and_then(|t| t.as_str()) {
                        if key_by_name.contains_key(to) {
                            edges.push((name.clone(), to.to_string()));
                        }
                    }
                }
            }
        }
    }

    // Signal metadata: owner + discrete heuristic. Series naming is
    // `<owner>.<signal>`; a series whose sampled values are all 0/1 is a
    // discrete (Boolean) signal, anything else is analog (Double). Units
    // come from the owner's `spec.<signal>.unit` prop WHEN present (the
    // pack's 44-byte props cap usually keeps it out — then v1 serves NO
    // EngineeringUnit rather than a wrong one).
    let units = {
        let mut api = lock_api(api);
        signal_units(&mut api)
    };
    let mut signals: Vec<SignalVar> = Vec::new();
    for (idx, s) in series_names.iter().enumerate() {
        let Some((owner, sig)) = s.split_once('.') else {
            continue; // non-conforming name: skip (never panic)
        };
        if !key_by_name.contains_key(owner) {
            continue;
        }
        let now = unix_now();
        let discrete = {
            let mut api = lock_api(api);
            api.get_measurements(owner, sig, now.saturating_sub(86400), now + 1)
                .ok()
                .and_then(|v| v.get("points").cloned())
                .and_then(|p| serde_json::from_value::<Vec<serde_json::Value>>(p).ok())
                .map(|pts| {
                    !pts.is_empty()
                        && pts.iter().all(|pt| {
                            matches!(
                                pt.get("value").and_then(|v| v.as_f64()),
                                Some(0.0) | Some(1.0)
                            )
                        })
                })
                .unwrap_or(false)
        };
        signals.push(SignalVar {
            series: s.clone(),
            series_index: idx as u32,
            node: NodeId::null(),
            discrete,
            unit: units.get(s).cloned(),
        });
    }

    // ---- server config ----------------------------------------------------
    let uri = namespace_uri(db_path);
    let user_token_ids = vec![ANONYMOUS_USER_TOKEN_ID.to_string()];
    let def = ServerEndpoint::new_none("/", &user_token_ids);
    let endpoint_url = format!("opc.tcp://127.0.0.1:{}/", port);
    let server = ServerBuilder::new()
        .application_name("VidgeDB Twin Server")
        .application_uri(format!("urn:vidgedb:{}", uri))
        .product_uri("urn:vidgedb:server")
        // v2 migration note: the sample keypair is created so signed
        // endpoints need config changes only (see module docs).
        .create_sample_keypair(true)
        .pki_dir("./pki")
        .host_and_port("127.0.0.1", port)
        // Absolute discovery url on the SAME loopback: a client doing
        // FindServers/GetEndpoints sees exactly this URL (the crate
        // resolves a relative "/" against host_and_port, but an explicit
        // one keeps the contract deterministic — and is_valid() refuses
        // an empty discovery_urls).
        .discovery_urls(vec![format!("opc.tcp://127.0.0.1:{}/", port)])
        .endpoint("vidgedb_none", def)
        .server()
        .ok_or_else(|| "opcua server configuration invalid".to_string())?;

    // ---- address space ----------------------------------------------------
    let namespace = {
        let space = server.address_space();
        let mut space = space.write();
        let ns = space
            .register_namespace(&uri)
            .map_err(|_| "namespace registration failed".to_string())?;
        let objects = NodeId::new(0, ObjectId::ObjectsFolder as u32);
        let root = NodeId::new(ns, ROOT_FOLDER_ID);
        if !space.add_folder_with_id(&root, "VidgeDB", "VidgeDB", &objects) {
            return Err("cannot create the VidgeDB root folder".to_string());
        }

        // Entity objects, organized under per-type folders (folder ids are
        // also deterministic: derived from the FIRST key of each type).
        let mut object_of: HashMap<String, NodeId> = HashMap::new();
        let mut folder_of: HashMap<String, NodeId> = HashMap::new();
        for (key, name, ty) in &entities {
            let folder = match folder_of.get(ty.as_str()) {
                Some(f) => f.clone(),
                None => {
                    let id = NodeId::new(ns, TYPE_FOLDER_BASE + key);
                    space.add_folder_with_id(&id, ty.as_str(), ty.as_str(), &root);
                    folder_of.insert(ty.clone(), id.clone());
                    id
                }
            };
            let obj_id = NodeId::new(ns, ENTITY_BASE + *key);
            if ObjectBuilder::new(&obj_id, name.as_str(), name.as_str())
                .organized_by(folder.clone())
                .insert(&mut space)
            {
                object_of.insert(name.clone(), obj_id);
            } else {
                eprintln!("vidgedb: opcua: object {} failed to insert", name);
            }
        }

        // Relations as the hierarchy: parent = src object, child = dst
        // object (HasComponent). The dst object keeps its Organizes
        // reference to its type folder too — a client sees it under both,
        // which is exactly the twin's shape (type classification AND
        // physical topology).
        for (src, dst) in &edges {
            let (Some(a), Some(b)) = (object_of.get(src), object_of.get(dst)) else {
                continue;
            };
            space.insert_reference(a, b, ReferenceTypeId::HasComponent);
        }

        // Signal variables under their OWNER object.
        for sv in signals.iter_mut() {
            let Some((owner, sig)) = sv.series.split_once('.') else {
                continue;
            };
            let Some(owner_obj) = object_of.get(owner) else {
                continue;
            };
            let idx = sv.series_index;
            let (data_type, initial) = if sv.discrete {
                (DataTypeId::Boolean, Variant::Boolean(false))
            } else {
                (DataTypeId::Double, Variant::Double(0.0))
            };
            let node = NodeId::new(ns, VARIABLE_BASE + idx);
            let inserted = VariableBuilder::new(&node, sig, sig)
                .data_type(data_type)
                .historizing(false)
                .access_level(AccessLevel::CURRENT_READ)
                .component_of(owner_obj.clone())
                .value(initial)
                .insert(&mut space);
            if !inserted {
                eprintln!("vidgedb: opcua: variable {} failed to insert", sv.series);
                continue;
            }
            sv.node = node;

            // EngineeringUnit property when the twin declares a unit for
            // this signal (spec.<signal>.unit). Units beyond the UA table
            // fall back to a deterministic id (v1 unit display only).
            if let Some(unit) = &sv.unit {
                let eu = EUInformation {
                    namespace_uri: UAString::from("urn:vidgedb:units"),
                    unit_id: ua_unit_id(unit),
                    display_name: LocalizedText {
                        locale: UAString::from(""),
                        text: UAString::from(unit),
                    },
                    description: LocalizedText {
                        locale: UAString::from(""),
                        text: UAString::from("pack-declared unit"),
                    },
                };
                let prop_id = NodeId::new(ns, 3000 + idx);
                let eu_obj = ExtensionObject::from_encodable(
                    ObjectId::EUInformation_Encoding_DefaultBinary,
                    &eu,
                );
                let inserted = VariableBuilder::new(&prop_id, "EngineeringUnit", "EngineeringUnit")
                    .property_of(sv.node.clone())
                    .data_type(DataTypeId::EUInformation)
                    .value(Variant::from(eu_obj))
                    .insert(&mut space);
                if !inserted {
                    eprintln!(
                        "vidgedb: opcua: EngineeringUnit for {} failed to insert",
                        sv.series
                    );
                }
            }
            // (owner_obj is attached to the variable via .component_of above.)
            let _ = &owner_obj;
        }
        ns
    }; // address space write lock dropped here

    Ok((server, endpoint_url, namespace, signals))
}

/// All units declared in the twin: `series name -> unit`. Source: each
/// owner's `spec.<signal>.unit` prop (a walk over 0..entity_count). A
/// series referenced by a unit prop is confirmed via the public series
/// lookup, so a stale prop never fabricates a unit.
fn signal_units(api: &mut AgentApi) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let count = api.entity_count();
    for k in 0..count {
        let Ok(v) = api.get_entity(k) else {
            continue;
        };
        if v.get("error").is_some() {
            continue;
        }
        let Some(name) = v.get("name").and_then(|n| n.as_str()) else {
            continue;
        };
        let Some(props) = v.get("properties").and_then(|p| p.as_object()) else {
            continue;
        };
        for (pk, pv) in props {
            let Some(sig) = pk
                .strip_suffix(".unit")
                .and_then(|s| s.strip_prefix("spec."))
            else {
                continue;
            };
            if let (Some(u), Ok(Some(_sid))) = (pv.as_str(), api.find_series_public(name, sig)) {
                out.insert(format!("{}.{}", name, sig), u.to_string());
            }
        }
    }
    out
}

/// Pack unit → UA unit id (EUInformation.unit_id). Deterministic table for
/// the common electrical/thermal units, stable hash for the rest.
fn ua_unit_id(unit: &str) -> i32 {
    match unit {
        "A" => 267_608,        // Ampere
        "C" | "°C" => 259_827, // DegreeCelsius
        "m/s" => 259_875,      // MetrePerSecond
        "mm/s" => 266_899,     // MillimetrePerSecond
        "bar" => 259_853,      // Bar
        "V" => 268_041,        // Volt
        _ => 100 + (unit.bytes().map(|b| b as i32).sum::<i32>() % 100_000),
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The poller + run loop
// ---------------------------------------------------------------------------

/// Convert unix seconds into an OPC-UA `DateTime` (ticks since 1601-01-01).
fn dt_from_unix(secs: i64) -> DateTime {
    // OPC-UA epoch = 1601-01-01 UTC; 11_644_473_600 s between the epochs.
    const UA_EPOCH_TO_UNIX_SECS: i64 = 11_644_473_600;
    const TICKS_PER_SEC: i64 = 10_000_000;
    DateTime::from((secs + UA_EPOCH_TO_UNIX_SECS) * TICKS_PER_SEC)
}

/// One poller tick: refresh every variable from the twin AS SEEN THROUGH
/// A FRESH AgentApi open (the Phase 15 staleness law: a long-lived reader
/// never sees another process's commit; a reopen rebuilds the whole slab
/// index from the durable file — `open is cheap: superblock + slabs
/// rebuild`). The opened Reader holds no write lock (the wlock file is
/// only taken by writer/ingest roles), so an --http writer on the same
/// twin runs unimpeded. Store/broken-twin errors NEVER panic: the tick
/// keeps the last published values and tries again next tick.
///
/// The returned values are the ONLY writes into the address space, and
/// they happen while the previous tick's handle is already dropped.
fn poll_tick(
    db_path: &str,
    server: &Arc<RwLock<Server>>,
    vars: &[SignalVar],
    agent_id: &str,
    window: i64,
) {
    let now = unix_now();
    // Fresh open per tick (see module docs, "Telemetry"): this is what
    // makes a foreign --service/--http writer's commits visible.
    let mut api = match AgentApi::open_with_role(db_path, agent_id, Role::Reader) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("vidgedb: opcua: poll open failed (continuing): {}", e.error);
            return;
        }
    };
    let space_arc = server.read().address_space();
    let mut space = space_arc.write();
    for sv in vars {
        let Some((owner, sig)) = sv.series.split_once('.') else {
            continue;
        };
        // Read the freshest committed point: the LAST point of the window.
        let reading = api.get_measurements(owner, sig, now.saturating_sub(window), now + 1);
        let res = match reading {
            Ok(v) => v,
            // Store error: log + continue (no panic — the contract).
            Err(e) => {
                eprintln!(
                    "vidgedb: opcua: poll {}: store error {} (continuing)",
                    sv.series, e.error
                );
                continue;
            }
        };
        let Some(pt) = res
            .get("points")
            .and_then(|p| p.as_array())
            .and_then(|a| a.last())
            .cloned()
        else {
            continue;
        };
        let Some(value) = pt.get("value").and_then(|v| v.as_f64()) else {
            continue;
        };
        let t = pt.get("t").and_then(|t| t.as_i64()).unwrap_or(now);
        let _ = space.set_variable_value(
            sv.node.clone(),
            if sv.discrete {
                Variant::Boolean(value != 0.0)
            } else {
                Variant::Double(value)
            },
            &dt_from_unix(t),
            &DateTime::now(),
        );
    }
}

/// Boot the OPC-UA server:
/// - lays out the address space from the twin (fail-closed on a bad twin),
/// - spawns the poller thread at `poll_ms`,
/// - spawns the OPC-UA run loop thread,
/// - returns the advertised endpoint URL.
///
/// The server runs until the process aborts it (the CLI's Ctrl-C path).
pub fn run_opcua(
    api: Arc<Mutex<AgentApi>>,
    db_path: &str,
    port: u16,
    poll_ms: u64,
) -> Result<String, String> {
    let (server, endpoint_url, ns, vars) = build_server(&api, db_path, port)?;
    let nvars = vars.len();
    let server = Arc::new(RwLock::new(server));

    // Poller thread: FRESH OPEN + read + refresh every tick. The initial
    // `api` opened the tree once (its snapshot laid the address space
    // out); the fresh opens are what keep the values LIVE. Store errors
    // NEVER kill it (log + continue). Poll budget: 50 ms..60 000 ms —
    // faster pollers only spin the CPU (the crate publishes at 100 ms
    // granularity anyway).
    {
        let server = Arc::clone(&server);
        let vars = vars.clone();
        let agent_id = {
            let api = lock_api(&api);
            api.agent_id().to_string()
        };
        let db_path = db_path.to_string();
        const WINDOW: i64 = 3600;
        std::thread::Builder::new()
            .name("vidgedb-opcua-poll".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_millis(poll_ms.clamp(50, 60_000)));
                poll_tick(&db_path, &server, &vars, &agent_id, WINDOW);
            })
            .map_err(|e| format!("poller spawn: {}", e))?;
    }

    // Server thread: the crate's blocking run loop.
    {
        let server = Arc::clone(&server);
        std::thread::Builder::new()
            .name("vidgedb-opcua-run".into())
            .spawn(move || {
                Server::run_server(server);
            })
            .map_err(|e| format!("server spawn: {}", e))?;
    }

    eprintln!(
        "vidgedb: opcua endpoint ready on {} (ns={} — {} variables, poll={}ms, \
SecurityPolicy None v1: isolated machine LAN ONLY, no encryption/auth)",
        endpoint_url, ns, nvars, poll_ms
    );
    Ok(endpoint_url)
}

/// Wait until the OPC-UA server accepts TCP connections on `port` (test
/// helper; bounded so a dead server cannot hang a test forever).
pub fn wait_ready(port: u16, timeout_ms: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

//! VidgeDB binary — Phase 10 multi-subcommand CLI.
//!
//! ## Subcommands
//! - `vidgedb smoke` — the Phase 0 spec §58 example (also the historical
//!   `cargo run` default when no arguments are given).
//! - `vidgedb open <db.vdg>` — local REPL over the SAME JSON-RPC methods
//!   the service exposes (human inspection).
//! - `vidgedb --service <db.vdg> [--agent-id X] [--retention-days N]` —
//!   JSON-RPC 2.0 over stdin/stdout for AI agents (line-delimited).
//! - `vidgedb --http <db.vdg> [--port 8888] [--bind 127.0.0.1|0.0.0.0]
//!   [--http-token X]` — Phase 14: the SAME JSON-RPC methods over a
//!   minimal HTTP/1.1 endpoint (`POST /rpc`), for remote diagnostics
//!   without a local stdio bridge. Mutually exclusive with `--service`
//!   (either the process is a child's pipe peer, or it is an HTTP server).
//! - `vidgedb --opcua <db.vdg> [--opcua-port 4840] [--opcua-poll-ms 1000]`
//!   — Phase 16: the twin as a NATIVE OPC-UA server (`opc.tcp://bind:port/`):
//!   an industrial client (UaExpert, TIA Portal, KEPServerEX…) browses the
//!   twin and subscribes to live telemetry. Combinable with `--http` (and
//!   both flags' argument) when BOTH are given: TWO servers on the SAME
//!   twin share one `Arc<Mutex<AgentApi>>` (the P14 sharing pattern).
//! - `vidgedb --version` / `vidgedb --help`.
//!
//! The binary stays a thin driver: everything lives in the library crate
//! (`vidgedb::tools` AgentApi, `vidgedb::service` JSON-RPC dispatch,
//! `vidgedb::http` the HTTP layer).

use vidgedb::engine::Engine;
use vidgedb::http;
use vidgedb::model::Vdb;
#[cfg(feature = "opcua")]
use vidgedb::opcua_server;
use vidgedb::service;
use vidgedb::statestore::StateEventStore;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;
use vidgedb::tools::{AgentApi, Role};

use std::io::{stdin, stdout, IsTerminal};
use std::process::exit;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn usage() -> String {
    let mut s = format!(
        "VidgeDB {} - embedded temporal graph database (.vdg, VidgeQL).\n\n",
        VERSION
    );
    s.push_str("USAGE:\n");
    s.push_str("  vidgedb --service <db.vdg> [--agent-id <id>] [--role <role>] [--retention-days <days>]\n");
    s.push_str("      JSON-RPC 2.0 over stdin/stdout (one request per line, one response\n");
    s.push_str("      per line) - how an AI agent pilots the database without writing Rust.\n");
    s.push_str("      --role reader|writer|ingest  authorization role (Phase 11 spec-55\n");
    s.push_str("      derrogation; default: reader = strict read-only, every write is\n");
    s.push_str("      refused with -32003 and audited). v1 has NO cryptographic auth —\n");
    s.push_str("      single-user deployment; a TCP wrapper will authenticate later.\n");
    s.push_str("      --retention-days <days>  retention pass at startup + hourly, dropping\n");
    s.push_str("      whole time-series chunks older than <days> (spec 37) — writer role\n");
    s.push_str("      is required when set.\n\n");
    s.push_str(
        "  vidgedb --http <db.vdg> [--port 8888] [--bind 127.0.0.1|0.0.0.0] [--http-token X]\n",
    );
    s.push_str("            [--agent-id <id>] [--role <role>] [--retention-days <days>]\n");
    s.push_str("      Phase 14: the SAME JSON-RPC 2.0 methods over a minimal HTTP/1.1\n");
    s.push_str("      endpoint: POST /rpc (one request object per body, one response\n");
    s.push_str("      object per reply), GET /health (debug), GET /schema (browser\n");
    s.push_str("      shortcut). Default bind: 127.0.0.1 (loopback-only — use\n");
    s.push_str("      --bind 0.0.0.0 to open the port beyond this machine, an explicit\n");
    s.push_str("      operator act). --http-token X requires Authorization: Bearer X on\n");
    s.push_str("      EVERY request (401 otherwise). v1 stays plain TCP — no custom\n");
    s.push_str("      crypto; put a TLS wrapper (stunnel/nginx) in front for remote\n");
    s.push_str("      use. NOT combinable with --service (one transport per process).\n\n");
    // The OPC-UA help block exists ONLY when the feature is compiled in —
    // a core binary must not advertise a mode it cannot serve.
    #[cfg(feature = "opcua")]
    {
        s.push_str("  vidgedb --opcua <db.vdg> [--opcua-port 4840] [--opcua-poll-ms 1000]\n");
        s.push_str("            [--agent-id <id>]\n");
        s.push_str("      Phase 16: the twin as a NATIVE OPC-UA server (crate opcua):\n");
        s.push_str("      endpoint opc.tcp://127.0.0.1:<port>/ (bind is loopback in v1),\n");
        s.push_str("      SecurityPolicy::None + anonymous tokens ONLY (v1 contract — same\n");
        s.push_str("      class as a brownfield PLC endpoint: NO encryption, NO signature,\n");
        s.push_str("      NO user auth; ANYONE on the network can read the twin. DEPLOY\n");
        s.push_str("      RULE: keep the endpoint on an ISOLATED machine LAN (cell VLAN /\n");
        s.push_str("      firewall), never route it beyond the line; signed endpoints are\n");
        s.push_str("      the v2 migration). READ-ONLY by design: the address space opens\n");
        s.push_str("      Role::Reader, no node is writable — telemetry/topology writes go\n");
        s.push_str("      through the HTTP/stdio surfaces (role writer|ingest). Namespace\n");
        s.push_str("      2 holds the browse tree (docs/opcua-reference.md): VidgeDB →\n");
        s.push_str("      type folders → entity Objects (topology as HasComponent) →\n");
        s.push_str("      signal Variables (Double analog / Boolean discrete) refreshed\n");
        s.push_str("      every --opcua-poll-ms. COMBINABLE with --http <db.vdg> (both\n");
        s.push_str("      servers, one shared twin) when both flags carry their argument.\n\n");
    }
    #[cfg(not(feature = "opcua"))]
    {
        s.push_str("  (this build has NO OPC-UA support: rebuild with --features opcua\n");
        s.push_str("   or use the vidgedb-opcua artifact — `--opcua` will report it.)\n\n");
    }
    s.push_str("  vidgedb open <db.vdg> [--agent-id <id>] [--role <role>]\n");
    s.push_str("      Local interactive REPL (human inspection) with the same methods.\n\n");
    s.push_str("  vidgedb smoke\n");
    s.push_str("      Run the spec 58 example machine (2 violations expected).\n\n");
    s.push_str("  vidgedb --version\n  vidgedb --help\n\n");
    s.push_str("METHODS (JSON-RPC over stdin or HTTP; `exit`/`quit` ends a stdio session):\n");
    s.push_str("  READ (every role): schema, query, query_temporal, get_entity,\n");
    s.push_str("  get_measurements, check, trace, provenance, get_state, state_history,\n");
    s.push_str("  get_events, audit\n");
    s.push_str("  WRITE (role=writer|ingest only; reader -> -32003 + HTTP 403):\n");
    s.push_str("  ingest_points, upsert_entity, set_state, log_event, retain\n\n");
    s.push_str("EXAMPLES (JSON-RPC):\n");
    s.push_str("  echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"schema\"}' | vidgedb --service twin.vdg\n");
    s.push_str("  vidgedb --http twin.vdg --port 8888 --role writer &\n");
    s.push_str("  curl -s http://127.0.0.1:8888/health\n");
    #[cfg(feature = "opcua")]
    {
        s.push_str(
            "  vidgedb --http twin.vdg --opcua twin.vdg --opcua-port 4840 --role writer &\n",
        );
        s.push_str("      (two servers, one twin: HTTP JSON-RPC + OPC-UA browse/subscribe)\n");
    }
    s.push_str("  curl -s http://127.0.0.1:8888/rpc -X POST \\\n");
    s.push_str("      -H 'Content-Type: application/json' \\\n");
    s.push_str("      -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"schema\"}'\n");
    s.push_str("\n");
    s.push_str("Python agent example:\n");
    s.push_str("  import subprocess, json\n");
    s.push_str("  p = subprocess.Popen(['vidgedb', '--service', 'twin.vdg'],\n");
    s.push_str("                       stdin=subprocess.PIPE, stdout=subprocess.PIPE)\n");
    s.push_str("  p.stdin.write(bytes(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'schema'})) + b'\\n')");
    s.push_str("\n");
    s.push_str("  p.stdin.flush()\n");
    s.push_str("  print(json.loads(p.stdout.readline()))\n");
    s.replace("(.vdg,", "(.vdg,")
}

/// `--service` / `open` mode: serve JSON-RPC on stdin/stdout (prompt on
/// stderr only when an interactive TTY is attached). Optionally run one
/// retention pass at startup and then hourly in a background thread.
///
/// Phase 11: the service opens the API with the role from `--role`
/// (`reader` default | `writer` | `ingest`) — the documented §55
/// derrogation. A reader service refuses every write with -32003.
fn run_service(path: &str, agent_id: &str, retention_days: Option<u64>, role: Role) -> i32 {
    let mut api = match AgentApi::open_with_role(path, agent_id, role) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("vidgedb: cannot open {}: {}", path, e.error);
            return 2;
        }
    };

    // Startup retention + hourly thread (spec §37: keep the storage bounds
    // honest on always-on edge devices without an external cron).
    if let Some(days) = retention_days {
        let cutoff = now_secs() - (days as i64) * 86_400;
        match api.retain(cutoff) {
            Ok(res) => {
                eprintln!(
                    "vidgedb: startup retention: {} points / {} chunks removed (cutoff={})",
                    res.get("points_removed").cloned().unwrap_or_default(),
                    res.get("chunks_removed").cloned().unwrap_or_default(),
                    cutoff
                );
            }
            Err(e) => {
                eprintln!("vidgedb: startup retention failed: {}", e.error);
                return 3;
            }
        }
        // The AgentApi is not Send; the thread re-opens the database afresh
        // each pass (open is cheap: superblock + slabs rebuild).
        let path_owned = path.to_string();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
            let retention = hourly_retain(&path_owned, days);
            match retention {
                Ok((pts, chs)) => {
                    eprintln!(
                        "vidgedb: hourly retention: {} points / {} chunks removed",
                        pts, chs
                    );
                }
                Err(e) => {
                    eprintln!("vidgedb: hourly retention failed: {}", e);
                }
            }
        });
    }

    let interactive_mode = stdout().is_terminal() && stdin().is_terminal();
    let served = match service::serve(&mut api, stdin().lock(), &mut stdout(), interactive_mode) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("vidgedb: stdio error: {}", e);
            return 4;
        }
    };
    eprintln!("vidgedb: served {} requests", served);
    0
}

/// Flag bundle for --opcua (Phase 16). Defaults live HERE: port 4840 (the
/// IANA-registered OPC-UA discovery/service port), poll 1000 ms.
#[cfg(feature = "opcua")]
struct OpcuaFlags {
    port: u16,
    poll_ms: u64,
    agent_id: String,
}

#[cfg(feature = "opcua")]
impl OpcuaFlags {
    fn parse(args: &[String]) -> Self {
        // --opcua-port (integer, 1..=65535; 0 would be an ephemeral bind —
        // an OPC-UA endpoint URL must be pinnable, so 0 is refused).
        let port = match args
            .iter()
            .position(|a| a == "--opcua-port")
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<u16>().ok())
        {
            Some(p) if p != 0 => p,
            Some(_) => {
                eprintln!("vidgedb: --opcua-port requires an integer in 1..=65535");
                exit(2);
            }
            None => 4840,
        };
        // --opcua-poll-ms (integer >= 1; the server clamps to 50..60000).
        let poll_ms = match args
            .iter()
            .position(|a| a == "--opcua-poll-ms")
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<u64>().ok())
        {
            Some(v) if v >= 1 => v,
            Some(_) => {
                eprintln!("vidgedb: --opcua-poll-ms requires a positive integer (ms)");
                exit(2);
            }
            None => 1000,
        };
        let agent_id = args
            .iter()
            .position(|a| a == "--agent-id")
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| "agent-opcua".to_string());
        OpcuaFlags {
            port,
            poll_ms,
            agent_id,
        }
    }
}

/// Phase 16 OPC-UA mode: open the twin as Role::Reader (strict read-only
/// v1 — writes pass through the HTTP/stdio surfaces), boot the OPC-UA
/// server + poller, then block forever on the run loop. Returns only on
/// a boot failure (the caller prints + exits non-zero).
#[cfg(feature = "opcua")]
fn run_opcua_mode(path: &str, flags: &OpcuaFlags) -> i32 {
    // Fail-closed on a bad DB: never leave a half-open endpoint listening.
    let api = match AgentApi::open_with_role(path, &flags.agent_id, Role::Reader) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("vidgedb: cannot open {}: {}", path, e.error);
            return 2;
        }
    };
    // The address-space builder runs BEFORE the endpoint advertises —
    // browse never sees a half-laid tree.
    let _endpoint_url = match opcua_server::run_opcua(
        std::sync::Arc::new(std::sync::Mutex::new(api)),
        path,
        flags.port,
        flags.poll_ms,
    ) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("vidgedb: opcua boot failed: {}", e);
            return 7;
        }
    };
    // The crate's run loop owns its thread; this thread parks so the
    // process stays alive (Ctrl-C / SIGTERM are the documented stops).
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

/// Boot the OPC-UA side of the COMBINED mode (--http + --opcua): the
/// given twin is opened reader (the OPC-UA surface's own gate, whatever
/// the HTTP side's role is), the server+poller threads start, and the
/// caller proceeds to bind the HTTP listener. Run threads keep the
/// process alive until Ctrl-C/SIGTERM.
#[cfg(feature = "opcua")]
fn start_opcua_shared(path: &str, flags: &OpcuaFlags) -> Result<(), String> {
    let api = match AgentApi::open_with_role(path, &flags.agent_id, Role::Reader) {
        Ok(a) => a,
        Err(e) => return Err(format!("cannot open {}: {}", path, e.error)),
    };
    // Discard the endpoint URL: the HTTP `ready` line carries the run
    // report (this mode's own line is already on stderr from run_opcua).
    let _ = opcua_server::run_opcua(
        std::sync::Arc::new(std::sync::Mutex::new(api)),
        path,
        flags.port,
        flags.poll_ms,
    )?;
    Ok(())
}

/// One hourly retention pass: reopen the database and `retain(now - days)`.
fn hourly_retain(path: &str, days: u64) -> Result<(u64, usize), String> {
    let mut eng = Engine::open(path).map_err(|e| format!("engine: {:?}", e))?;
    let mut gs = GraphStore::open(&mut eng).map_err(|e| format!("graph: {:?}", e))?;
    let mut ts =
        TimeSeriesStore::open(&mut eng, &mut gs).map_err(|e| format!("timeseries: {:?}", e))?;
    let _se =
        StateEventStore::open(&mut eng, &mut gs).map_err(|e| format!("statestore: {:?}", e))?;
    // GraphStore::open(&mut eng) above already rebuilt the graph; the
    // retention only touches the TS store (chunk granularity, spec §37).
    let before = now_secs() - (days as i64) * 86_400;
    ts.retain(&mut eng, before)
        .map_err(|e| format!("retain: {:?}", e))
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Parse `--role <role>` (Phase 11). Default: Reader (the un-derrogated
/// spec §55 behavior). Unknown values exit 2 with a clean diagnostic.
fn role_from_args(args: &[String]) -> Role {
    match args
        .iter()
        .position(|a| a == "--role")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("reader")
    {
        "reader" => Role::Reader,
        "writer" => Role::Writer,
        "ingest" => Role::Ingest,
        other => {
            eprintln!(
                "vidgedb: unknown --role '{}' (expected reader | writer | ingest)",
                other
            );
            exit(2);
        }
    }
}

/// `open <db.vdg>`: local interactive REPL for a human — same loop, same
/// methods; a prompt is emitted on stderr per request (TTY only).
fn run_open(path: &str, agent_id: &str, role: Role) -> i32 {
    eprintln!(
        "vidgedb: opening {} as agent {} (role={:?}; methods: schema, query, …; `exit` to leave)",
        path, agent_id, role
    );
    run_service(path, agent_id, None, role)
}

/// The Phase 0 smoke test (spec §58) — kept as a subcommand.
fn run_smoke() -> i32 {
    let mut db = Vdb::new();

    let plc = db.add_entity("PLC01", "PLC");
    let drive = db.add_entity("Drive12", "Drive");
    let motor = db.add_entity("Motor42", "Motor");
    let pump = db.add_entity("Pump17", "Pump");

    db.add_relation(plc, drive, "network:profinet");
    db.add_relation(drive, motor, "electrical:feeds");
    db.add_relation(motor, pump, "mechanical:drives");

    db.set_spec(motor, "current", 10.0, "A");
    db.set_spec(pump, "pressure", 180.0, "bar");

    db.set_measurement(motor, "current", 11.7, "A");
    db.set_measurement(pump, "pressure", 183.0, "bar");

    let violations = db.check_constraints();
    println!(
        "VidgeDB smoke test — {} entities, {} relations",
        db.entity_count(),
        db.relation_count()
    );
    for v in &violations {
        println!(
            "VIOLATION: {}:{} observed={} {} expected_max={} {}",
            v.entity, v.signal, v.observed, v.unit, v.expected_max, v.unit
        );
    }
    if violations.len() != 2 {
        eprintln!("FAIL: spec §58 expects exactly 2 violations");
        return 1;
    }
    println!("OK: both violations detected, provenance preserved.");
    0
}

/// Installer l'arrêt ordonné AVANT tout spawn de thread : SIGINT/SIGTERM
/// sont bloqués puis consommés synchroniquement, ce qui garantit que le
/// verrou single-writer est libéré (phase 92). Sans cela, `docker stop`
/// laissait un verrou fantôme et le conteneur suivant était refusé 75 s.
fn register_http_lock_shutdown(db: &str) {
    // Le nom du verrou est `<db>-wlock` (même règle que `DbWriteLock`).
    let mut p = std::path::PathBuf::from(db);
    let mut name = p.file_name().unwrap_or_default().to_os_string();
    name.push("-wlock");
    p.set_file_name(name);
    let lock = p;
    vidgedb::signals::register_shutdown_hook(Box::new(move || {
        // Idempotent : si `Drop` a déjà fait le travail (sortie par EOF),
        // l'unlink échoue simplement en silence.
        let _ = std::fs::remove_file(&lock);
        let _ = std::fs::remove_file(
            lock.with_extension(format!("vdg-wlock.tmp{}", std::process::id())),
        );
    }));
}

fn main() {
    // Installer l'arrêt ordonné AVANT tout spawn de thread : SIGINT/SIGTERM
    // sont bloqués puis consommés synchroniquement, ce qui garantit que le
    // verrou single-writer est libéré (phase 92). Sans cela, `docker stop`
    // laissait un verrou fantôme et le conteneur suivant était refusé 75 s.
    vidgedb::signals::install_signal_handlers();

    let args: Vec<String> = std::env::args().collect();
    // No arguments at all: the historical default (Phase 0 smoke).
    if args.len() <= 1 {
        eprintln!("(no arguments: running the Phase 0 smoke test — see --help)");
        exit(run_smoke());
    }
    match args[1].as_str() {
        "--version" | "-V" | "version" => {
            println!("vidgedb {}", VERSION);
            exit(0);
        }
        "--help" | "-h" | "help" => {
            println!("vidgedb {}\n\n{}", VERSION, usage());
            exit(0);
        }
        "smoke" => {
            exit(run_smoke());
        }
        "open" => match args.get(2) {
            Some(path) => {
                let agent = args
                    .iter()
                    .position(|a| a == "--agent-id")
                    .and_then(|i| args.get(i + 1))
                    .cloned()
                    .unwrap_or_else(|| "human".to_string());
                let role = role_from_args(&args);
                exit(run_open(path, &agent, role));
            }
            None => {
                eprintln!(
                    "vidgedb: `open` requires a database path (usage: vidgedb open <db.vdg>)"
                );
                exit(2);
            }
        },
        "--service" => match args.get(2) {
            Some(path) => {
                let agent = args
                    .iter()
                    .position(|a| a == "--agent-id")
                    .and_then(|i| args.get(i + 1))
                    .cloned()
                    .unwrap_or_else(|| "agent-cli".to_string());
                let mut retention_days: Option<u64> = None;
                if let Some(i) = args.iter().position(|a| a == "--retention-days") {
                    match args.get(i + 1).and_then(|v| v.parse::<u64>().ok()) {
                        Some(days) => retention_days = Some(days),
                        None => {
                            eprintln!("vidgedb: --retention-days requires an integer (days)");
                            exit(2);
                        }
                    }
                }
                // Phase 11: a retention pass is a WRITE (chunk dropping);
                // a retention-days policy with a reader role is a config
                // error, refused up front (fail closed).
                let role = role_from_args(&args);
                if retention_days.is_some() && !matches!(role, Role::Writer | Role::Ingest) {
                    eprintln!(
                        "vidgedb: --retention-days requires --role writer|ingest \
(the retention pass writes; a reader service must not)"
                    );
                    exit(2);
                }
                exit(run_service(path, &agent, retention_days, role));
            }
            None => {
                eprintln!(
                    "vidgedb: --service requires a database path (usage: vidgedb --service <db.vdg>)"
                );
                exit(2);
            }
        },
        // Phase 14/16: HTTP endpoint mode — ONE AgentApi behind a mutex,
        // the SAME method dispatch as stdio, one thread per connection.
        // NOT combinable with --service (one transport per process).
        // COMBINABLE with --opcua (Phase 16): when `--opcua <db.vdg>` is
        // ALSO present, the process runs TWO servers over the SAME twin
        // (the P14 sharing shape — HTTP holds the long-lived
        // Arc<Mutex<AgentApi>>; the OPC-UA poller re-opens its own
        // Reader each tick).
        "--http" => {
            // --http <db.vdg> is REQUIRED (the DB is the load operand).
            let flags_http_db_missing = || {
                eprintln!(
                    "vidgedb: --http requires a database path (usage: vidgedb --http <db.vdg> [--port N])"
                );
                exit(2);
            };
            let path = match args.get(2) {
                Some(p) if !p.starts_with("--") => p.clone(),
                _ => flags_http_db_missing(),
            };
            let flags = HttpFlags::parse(&args);
            let role = role_from_args(&args);
            // Same config rule as --service: the retention pass WRITES.
            if flags.retention_days.is_some() && !matches!(role, Role::Writer | Role::Ingest) {
                eprintln!(
                    "vidgedb: --retention-days requires --role writer|ingest \
(the retention pass writes; a reader endpoint must not)"
                );
                exit(2);
            }
            // The combined twin: --opcua <db.vdg> present ⇒ boot the
            // OPC-UA server BEFORE the HTTP listener (same fail-closed
            // rule: a bad boot must leave no half-open endpoint behind).
            // The role rules stay: OPC-UA's own surface is read-only
            // regardless of the HTTP side's role.
            #[cfg(feature = "opcua")]
            {
                let opcua_path = args
                    .iter()
                    .position(|a| a == "--opcua")
                    .and_then(|i| args.get(i + 1))
                    .filter(|p| !p.starts_with("--"))
                    .cloned();
                if let Some(opath) = opcua_path {
                    let oflags = OpcuaFlags::parse(&args);
                    if let Err(e) = start_opcua_shared(&opath, &oflags) {
                        eprintln!("vidgedb: opcua boot failed: {}", e);
                        exit(7);
                    }
                }
            }
            // Le mode --http ouvre en writer|ingest : son verrou doit être
            // libéré sur signal (les threads serveur ne peuvent pas l'être).
            register_http_lock_shutdown(&path);
            exit(run_http(&path, &flags, role));
        }
        // Phase 16: standalone OPC-UA mode — the ONLY mode named by its
        // own flag; `--opcua <db.vdg>` here is the load operand (the same
        // "flag carries the DB" convention as --http/--service).
        #[cfg(feature = "opcua")]
        "--opcua" => {
            let mismatch = || {
                eprintln!("vidgedb: --opcua requires a database path (usage: vidgedb --opcua <db.vdg> [--opcua-port N] [--opcua-poll-ms N])");
                exit(2);
            };
            let path = match args.get(2) {
                Some(p) if !p.starts_with("--") => p.clone(),
                _ => mismatch(),
            };
            let flags = OpcuaFlags::parse(&args);
            exit(run_opcua_mode(&path, &flags));
        }
        // Feature OFF: keep `--opcua` a KNOWN flag (never "unknown
        // argument") so operators get an actionable message instead.
        #[cfg(not(feature = "opcua"))]
        "--opcua" => {
            eprintln!(
                "vidgedb: this binary was built without OPC-UA support \
(rebuild with --features opcua, or use the vidgedb-opcua artifact)"
            );
            exit(2);
        }
        other => {
            eprintln!("vidgedb: unknown argument \"{}\"", other);
            eprintln!("{}", usage());
            exit(2);
        }
    }
}

/// Flag bundle for --http. Defaults live HERE (loopback 127.0.0.1:8888 —
/// fail-closed: nothing listens beyond this machine unless explicitly
/// asked for, and 0.0.0.0 binds an EXPLICIT non-lookback bind).
struct HttpFlags {
    port: u16,
    bind: String,
    token: Option<String>,
    agent_id: String,
    retention_days: Option<u64>,
}

impl HttpFlags {
    fn parse(args: &[String]) -> Self {
        // --port (integer, 1..=65535)
        let port = match args
            .iter()
            .position(|a| a == "--port")
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<u16>().ok())
        {
            Some(p) if p != 0 => p,
            _ => {
                eprintln!("vidgedb: --port requires an integer in 1..=65535");
                exit(2);
            }
        };
        // --bind (127.0.0.1 by default; 0.0.0.0 is the documented opt-in).
        let bind = match args
            .iter()
            .position(|a| a == "--bind")
            .and_then(|i| args.get(i + 1))
            .map(|s| s.as_str())
        {
            Some(b) => {
                if b.parse::<std::net::IpAddr>().is_err() {
                    eprintln!("vidgedb: --bind requires an IP address (e.g. 127.0.0.1 or 0.0.0.0)");
                    exit(2);
                }
                b.to_string()
            }
            None => "127.0.0.1".to_string(),
        };
        let token = args
            .iter()
            .position(|a| a == "--http-token")
            .and_then(|i| args.get(i + 1))
            .cloned();
        let agent_id = args
            .iter()
            .position(|a| a == "--agent-id")
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| "agent-http".to_string());
        let retention_days = args
            .iter()
            .position(|a| a == "--retention-days")
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse::<u64>().ok());
        HttpFlags {
            port,
            bind,
            token,
            agent_id,
            retention_days,
        }
    }
}

/// Phase 14 HTTP mode: bind → open the DB → serve forever.
fn run_http(path: &str, flags: &HttpFlags, role: Role) -> i32 {
    // Fail-closed on a bad DB: never leave a half-open endpoint listening.
    let mut api = match AgentApi::open_with_role(path, &flags.agent_id, role) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("vidgedb: cannot open {}: {}", path, e.error);
            return 2;
        }
    };

    // Startup retention (same rule as --service) — done BEFORE bind, so a
    // failing pass never leaves the port listening.
    if let Some(days) = flags.retention_days {
        let cutoff = now_secs() - (days as i64) * 86_400;
        match api.retain(cutoff) {
            Ok(res) => eprintln!(
                "vidgedb: startup retention: {} points / {} chunks removed (cutoff={})",
                res.get("points_removed").cloned().unwrap_or_default(),
                res.get("chunks_removed").cloned().unwrap_or_default(),
                cutoff
            ),
            Err(e) => {
                eprintln!("vidgedb: startup retention failed: {}", e.error);
                return 3;
            }
        }
        // Hourly background pass (same shape as --service: a fresh open
        // each time, the single-writer discipline untouched).
        let path_owned = path.to_string();
        let days = days;
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
            match hourly_retain(&path_owned, days) {
                Ok((pts, chs)) => eprintln!(
                    "vidgedb: hourly retention: {} points / {} chunks removed",
                    pts, chs
                ),
                Err(e) => eprintln!("vidgedb: hourly retention failed: {}", e),
            }
        });
    }

    let addr = format!("{}:{}", flags.bind, flags.port);
    let listener = match std::net::TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("vidgedb: cannot bind {}: {}", addr, e);
            return 5;
        }
    };
    let bind_note = if flags.bind == "127.0.0.1" || flags.bind == "::1" {
        "loopback-only (default; --bind 0.0.0.0 to open beyond this machine)"
    } else {
        "NON-loopback bind — the endpoint is reachable from the network \
(token/TLS responsibility sits with the operator)"
    };
    eprintln!(
        "vidgedb: http mode: {} as agent {} (role={:?}) on {} — {}",
        path, flags.agent_id, role, addr, bind_note
    );
    match http::serve(
        listener,
        std::sync::Arc::new(std::sync::Mutex::new(api)),
        flags.token.clone(),
    ) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("vidgedb: http serve error: {}", e);
            6
        }
    }
}

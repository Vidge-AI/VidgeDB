//! Agent Tool API — Phase 7 (spec §27/§28/§56).
//!
//! A single façade, [`AgentApi`], wraps the storage layers (Engine +
//! GraphStore + TimeSeriesStore) behind machine-readable JSON methods for
//! AI-agent consumption (spec §27: "The database SHALL therefore expose
//! machine-readable results").
//!
//! The intended agent loop (spec §27):
//! ```text
//! User -> AI Agent -> AgentApi -> Structured Result -> AI Agent -> Explanation
//! ```
//!
//! Design rules (Phase 7 contract, amended by the Phase 11 derrogation):
//! - **Read-only by default, role-gated writes** (spec §55 + DOCUMENTED
//!   derrogation, Phase 11): [`AgentApi::open`] takes an explicit
//!   [`Role`]. `Role::Reader` (the default) keeps the Phase 7 contract —
//!   strictly no writes. `Role::Writer` / `Role::Ingest` may write
//!   TELEMETRY and TOPOLOGY through `ingest_points` / `upsert_entity` /
//!   `set_state` / `log_event` — the negotiated, documented derrogation to
//!   the §55 read-only rule, necessary so an agent can ingest a real
//!   machine (PLC/OPC-UA bridge). Enforcement: a Reader write attempt is
//!   refused with the server-defined JSON-RPC code `-32003`
//!   ([`ERR_WRITE_FORBIDDEN`]) and the refusal is STILL recorded in the
//!   audit trail (spec §56) — nothing is ever written silently.
//! - **Provenance invariant untouched** (spec §14/§29): ingested data
//!   enters as `Fact` (source "plc") or `Observation` ("agent"/"sensor")
//!   and NEVER as `Hypothesis`; there is still NO promotion path —
//!   [`AgentApi::set_hypothesis`] refuses for EVERY role, and the engine
//!   never promotes (Phase 3 invariant).
//! - **v1 authorization limit (documented)**: there is NO cryptographic
//!   authentication — a role is an explicit trust declaration for a
//!   single-user deployment (the binary's `--role` flag). A TCP/TLS
//!   wrapper around the service is the planned place for real authn/z.
//!   Opening a *fresh* database creates the storage layout pages (that is
//!   `open()` bookkeeping, not data mutation); opening an existing `.vdg`
//!   touches nothing.
//! - Every method returns a serializable [`Result`] (`serde_json::Value` or
//!   `AgentApiError`, itself JSON-serializable). Query problems are reported
//!   as `{"error": "..."}` values — never a panic, never an abort.
//! - **Provenance is surfaced, never mutated** (spec §14/§29): the engine
//!   must never promote a `Hypothesis` to Fact/Observation, and there is
//!   deliberately NO `set_hypothesis` write path in v0. Any
//!   hypothesis-to-fact promotion would have to be an explicit, audited
//!   external write (DEVELOPING.md invariant); this façade refuses it.
//!
//! Auditability (spec §56): every call is recorded in an in-memory
//! `Vec<AuditEntry>` (timestamp, agent_id, method, params_summary) and
//! served back by [`AgentApi::audit_log`].

use crate::check::check_entity;
use crate::engine::Engine;
use crate::executor;
use crate::model::Provenance;
use crate::statestore::StateEventStore;
use crate::stores::GraphStore;
use crate::temporal::Window;
use crate::timeseries::TimeSeriesStore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Single-writer guard (phase 89 fix, portable heartbeat — Phase 90)
// ---------------------------------------------------------------------------

/// The durable single-writer lock of one open `.vdg` (an O_EXCL-created
/// lockfile `<db>.vdg-wlock` holding a JSON `{pid, acquired_at,
/// heartbeat_at}` record).
///
/// WHY: the engine's durable state is single-owner BY DESIGN (one pager
/// sb mirror, one freelist, one TS layout) but `Engine::open` had no
/// guard — a second writer process could open the SAME file while a
/// live writer kept it open. Two engines then interleave commit streams
/// over one file: each commits page ids its own allocator handed out
/// and rewrites the sb its own open() snapshot produced, so the
/// sb/page_count and the TS layout alternate between two diverged
/// states — the soak's `PageOutOfBounds(…)` oracle death (a committed
/// chunk record pointing at a page the other session's sb never
/// counts) and the constant data wipe (a young session rewinding a 2h
/// twin's layout), both PERMANENT (neither state is recoverable once
/// the allocators diverged).
///
/// Contract:
/// - a Writer/Ingest open CREATES the lockfile (O_EXCL) with the
///   holder's pid and holds the handle for the API's lifetime;
/// - the holder runs a HEARTBEAT thread (Phase 90): every
///   [`WRITER_HEARTBEAT`] (default 30 s; injectable for tests via
///   [`AgentApi::open_with_role_hb`]) it rewrites the lockfile ATOMICALLY
///   (temp file + rename, never in place — a steal reads either the old
///   or the new complete record) with a fresh `heartbeat_at`;
/// - a second writer open while the lock is live FAILS CLOSED with a
///   clean, actionable error (never a half-open corrupting session);
/// - a STALE lock is STOLEN automatically (a crash must never brick the
///   file, recovery + a fresh writer takes over). The liveness rule is
///   PORTABLE (zero OS syscalls, works identically on Linux/Windows/macOS):
///   - v2 lockfile (heartbeat present): dead when `now - heartbeat_at`
///     is over [`STEAL_AFTER_HEARTBEAT_SECS`] (a live holder beats every
///     30 s — a 75 s silent gap means the holder is gone, and a negative
///     age from clock skew clamps to 0 = alive);
///   - v1 lockfile (pid-only, pre-Phase-90): dead when the file's mtime
///     age exceeds [`STEAL_AFTER_LEGACY_SECS`] (legacy writers never
///     beat, so only the file age can speak);
///   - unparsable content: dead outright (v1 behavior kept).
///   - BONUS fast path, never required: on Linux only, `/proc/<pid>`
///     absence still steals immediately (a freshly crashed holder need
///     not wait out the heartbeat window); `/proc` existence never
///     overrides the heartbeat verdict.
/// - a Reader open NEVER touches the lock (readers are concurrent by
///   contract — the pager reads whole pages, no in-place mutation).
struct DbWriteLock {
    path: std::path::PathBuf,
    /// Kept open for the lock's lifetime: the file's existence IS the
    /// lock (a dropped handle does not remove it).
    _file: std::fs::File,
    /// Identity mirror of the record in the lockfile (rebroadcast by the
    /// heartbeat thread; kept so a future holder-side re-stamp or debug
    /// dump never has to re-parse its own file).
    #[allow(dead_code)]
    pid: u32,
    /// Acquisition time, unix seconds (rewritten with every heartbeat).
    #[allow(dead_code)]
    acquired_at: i64,
    /// Heartbeat thread handle (stopped + joined on drop, in THAT order —
    /// the thread must never outlive the unlink it could resurrect).
    heartbeat: Heartbeat, // stop + join
}

/// Lockfile wire record (Phase 90, v2 JSON). `heartbeat_at` is `None`
/// only when PARSING a legacy v1 pid-only file fell all the way through
/// to the JSON fallback (a real v1 file is plain `"1234\n"` text, parsed
/// on the legacy path before serde sees it).
#[derive(Clone, Serialize, Deserialize)]
struct LockfileRecord {
    /// Holder process id (informational; NOT the liveness oracle).
    pid: u32,
    /// Acquisition time, unix seconds (stable across heartbeats).
    acquired_at: i64,
    /// Last heartbeat, unix seconds. Absent = legacy lockfile.
    #[serde(default)]
    heartbeat_at: Option<i64>,
}

/// Default heartbeat period of a live writer lock (Phase 90).
pub const WRITER_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(30);

/// v2 steal threshold, unix seconds of heartbeat silence: a live holder
/// beats every [`WRITER_HEARTBEAT`] (30 s), so 75 s of silence (2.5 missed
/// beats) means the holder died — SIGKILL, panic, power loss.
pub const STEAL_AFTER_HEARTBEAT_SECS: i64 = 75;

/// Legacy (v1 pid-only) steal threshold, unix seconds of file-mtime age:
/// a Phase-89-era writer never heartbeats, so file age is the only
/// portable signal; 15 s is short by design (v2 holders refresh
/// continuously, so a long legacy timeout would only delay steals).
pub const STEAL_AFTER_LEGACY_SECS: i64 = 15;

/// Atomic lockfile rewrite used by BOTH the holder's heartbeat thread and
/// the initial fill: temp file + rename (never in-place) so a concurrent
/// steal always reads one complete record. No fsync — the heartbeat is a
/// liveness hint, not durable data; a lost final beat only delays a steal
/// by one period. Returns `false` when the lockfile is NO LONGER OURS
/// (externally stolen/replaced): the caller must stop beating — a dead
/// holder never writes again.
fn write_heartbeat(path: &std::path::Path, record: &LockfileRecord) -> bool {
    // Read-back ownership check BEFORE the rename: if the current content
    // is not ours, we lost the lock in an external race — refuse to
    // clobber the newer holder's record.
    if let Ok(s) = std::fs::read_to_string(path) {
        match serde_json::from_str::<LockfileRecord>(s.trim()) {
            Ok(cur) if cur.pid == record.pid && cur.acquired_at == record.acquired_at => {}
            _ => return false, // foreign or unparsable content: not ours
        }
    } else {
        return false; // gone
    }
    let tmp = path.with_extension(format!("vdg-wlock.tmp{}", std::process::id()));
    let body = match serde_json::to_string(record) {
        Ok(b) => b,
        Err(_) => return false,
    };
    if std::fs::write(&tmp, body.as_bytes()).is_err() {
        return false;
    }
    // Rename IS the atomic publish (std::fs::rename replaces the target
    // on Windows too: native MOVEFILE_REPLACE_EXISTING semantics).
    std::fs::rename(&tmp, path).is_ok()
}

/// Lockfile content classification before the steal decision.
enum LockfileContent {
    /// v2: heartbeat present (the portable rule applies).
    Heartbeats(LockfileRecord),
    /// Legacy v1: pid only, mtime age is the rule.
    LegacyPid(Option<u32>),
}

fn read_lockfile_content(path: &std::path::Path) -> LockfileContent {
    let s = std::fs::read_to_string(path).unwrap_or_default();
    if let Ok(rec) = serde_json::from_str::<LockfileRecord>(s.trim()) {
        if rec.heartbeat_at.is_some() && rec.pid != 0 {
            return LockfileContent::Heartbeats(rec);
        }
        // JSON without a heartbeat (or with a neutral pid): legacy shape.
        return LockfileContent::LegacyPid(Some(rec.pid));
    }
    // Plain v1 text: the pid alone ("4194303\n").
    LockfileContent::LegacyPid(s.trim().parse::<u32>().ok())
}

/// Unix-only BONUS fast path: does `/proc/<pid>` exist? Linux answers
/// definitively (`None` everywhere else — macOS ships no /proc, Windows
/// neither: there the heartbeat rule decides alone, which is why it is
/// portable and this helper is only an optimization).
fn proc_fast_path(pid: u32) -> Option<bool> {
    if !std::path::Path::new("/proc").is_dir() {
        return None;
    }
    Some(std::path::Path::new("/proc").join(pid.to_string()).exists())
}

/// One steal verdict: WHY the holder is presumed dead (stderr warning +
/// audit entry material — a steal is never silent).
struct StealVerdict {
    reason: String,
}

fn steal_reason(
    content: &LockfileContent,
    now: i64,
    mtime_age: Option<i64>,
) -> Option<StealVerdict> {
    match content {
        LockfileContent::Heartbeats(rec) => {
            let hb = rec.heartbeat_at.unwrap_or(now);
            // Clamp at 0: a FUTURE heartbeat (clock skew, NTP step) is a
            // LIVE lock — never steal it.
            let age = (now - hb).max(0);
            if age > STEAL_AFTER_HEARTBEAT_SECS {
                return Some(StealVerdict {
                    reason: format!(
                        "dead holder pid={} (heartbeat {}s old > {}s)",
                        rec.pid, age, STEAL_AFTER_HEARTBEAT_SECS
                    ),
                });
            }
            // Heartbeat still fresh. The Unix fast path may still speak:
            // a pid verifiably gone (Linux) is dead regardless of its
            // last beat (crash seconds ago). /proc ALIVE does NOT keep a
            // dead-beat lock alive — but the fast path never STEALS on
            // its own while the heartbeat is fresh, so a beaten-by holder
            // is refused everywhere.
            match proc_fast_path(rec.pid) {
                Some(false) => Some(StealVerdict {
                    reason: format!(
                        "dead holder pid={} (process gone, heartbeat {}s old)",
                        rec.pid, age
                    ),
                }),
                _ => None,
            }
        }
        LockfileContent::LegacyPid(pid) => {
            // Unparsable/garbage content (v1 parity): dead outright —
            // v1 stole these immediately, without an mtime wait.
            if pid.is_none() {
                return Some(StealVerdict {
                    reason: "corrupt or empty lockfile content".to_string(),
                });
            }
            // Linux fast path first (a verifiably dead legacy pid steals
            // without waiting on mtime).
            if let Some(p) = pid {
                if proc_fast_path(*p) == Some(false) {
                    return Some(StealVerdict {
                        reason: format!("dead legacy holder pid={} (process gone)", p),
                    });
                }
            }
            // Portable legacy rule: the writer never beats, so the file
            // age speaks. >15 s = dead (a v2 holder refreshes long before).
            let pid_s = pid.unwrap_or(0);
            if let Some(age) = mtime_age {
                if age > STEAL_AFTER_LEGACY_SECS {
                    return Some(StealVerdict {
                        reason: format!(
                            "legacy lock pid={} (mtime {}s old > {}s)",
                            pid_s, age, STEAL_AFTER_LEGACY_SECS
                        ),
                    });
                }
            }
            None
        }
    }
}

/// mtime age of `path` in unix seconds (None: unreadable/absent).
fn mtime_age_secs(path: &std::path::Path, now: i64) -> Option<i64> {
    let m = std::fs::metadata(path).ok()?;
    let mt = m
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    Some((now - mt).max(0))
}

impl DbWriteLock {
    /// `<db>.vdg-wlock` path of `db`.
    fn lock_path(db: &std::path::Path) -> std::path::PathBuf {
        let mut p = db.as_os_str().to_os_string();
        p.push("-wlock");
        std::path::PathBuf::from(p)
    }

    /// Acquire the writer lock for `db`, or refuse cleanly when ANOTHER
    /// live writer owns it. A stale lockfile (dead holder by the
    /// portable heartbeat/legacy-mtime rules above, or garbage content)
    /// is replaced. Returns the stolen reason when a steal happened
    /// (audited by `AgentApi::open_with_role` — a steal is never silent).
    fn acquire(
        db: &std::path::Path,
        heartbeat_period: std::time::Duration,
    ) -> Result<(DbWriteLock, Option<String>), AgentApiError> {
        let path = Self::lock_path(db);
        let pid = std::process::id();
        let now = unix_now();
        let mut stolen: Option<String> = None;
        // 3 attempts: (take) — (steal stale) — (take again after steal).
        for attempt in 0..3 {
            let _ = attempt; // bounded loop
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut f) => {
                    let record = LockfileRecord {
                        pid,
                        acquired_at: now,
                        heartbeat_at: Some(now),
                    };
                    let body = serde_json::to_string(&record).map_err(|e| AgentApiError {
                        error: format!("writer lock '{}' record: {}", path.display(), e),
                    })?;
                    let _ = write!(f, "{}", body);
                    let _ = f.sync_all();
                    let hb = Heartbeat::spawn(path.clone(), record, heartbeat_period);
                    return Ok((
                        DbWriteLock {
                            path,
                            _file: f,
                            pid,
                            acquired_at: now,
                            heartbeat: hb,
                        },
                        stolen,
                    ));
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Present: classify the content and apply the
                    // PORTABLE liveness rule (heartbeat + legacy mtime),
                    // with the Linux /proc check as a bonus fast path.
                    let content = read_lockfile_content(&path);
                    let holder_pid = match &content {
                        LockfileContent::Heartbeats(rec) => Some(rec.pid),
                        LockfileContent::LegacyPid(p) => *p,
                    };
                    let verdict =
                        steal_reason(&content, unix_now(), mtime_age_secs(&path, unix_now()));
                    match verdict {
                        Some(StealVerdict { reason }) => {
                            // Never silent: stderr + (via the caller)
                            // one audit trail entry (carried out of the
                            // loop on the NEXT successful create).
                            eprintln!(
                                "vidgedb: writer lock '{}' STOLEN: {} \
(single-writer liveness breach: the previous holder must be dead; \
its writes, if any were in flight, are lost by contract)",
                                path.display(),
                                reason
                            );
                            stolen = Some(reason);
                            let _ = std::fs::remove_file(&path);
                        }
                        None => {
                            return Err(AgentApiError {
                                error: format!(
                                    "database '{}' is write-locked by process {} \
(single-writer: a second concurrent writer would fork the durable \
state — sb/freelist/TsLayout — and corrupt the file; open as a reader \
role or wait for the owner to exit)",
                                    db.display(),
                                    holder_pid.unwrap_or(0)
                                ),
                            });
                        }
                    }
                }
                Err(e) => {
                    return Err(AgentApiError {
                        error: format!("writer lock '{}' unusable: {}", path.display(), e),
                    })
                }
            }
        }
        Err(AgentApiError {
            error: format!(
                "writer lock '{}' could not be acquired (steal loop exhausted)",
                path.display()
            ),
        })
    }
}

/// The holder-side heartbeat: a stop flag + the joined thread. Phase 90.
struct Heartbeat {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Heartbeat {
    /// Spawn the lockfile rewriter: every `period` (default 30 s,
    /// injectable for tests) the lockfile is re-atomically-published with
    /// a fresh `heartbeat_at`. Sleeps in small ticks so a stop is honored
    /// within ~50 ms regardless of the period. Exits by itself if the
    /// lockfile stops being ours (externally stolen): a DEAD holder
    /// never writes again.
    fn spawn(
        path: std::path::PathBuf,
        record: LockfileRecord,
        period: std::time::Duration,
    ) -> Heartbeat {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_thread = stop.clone();
        let join = std::thread::Builder::new()
            .name("vdg-wlock-heartbeat".to_string())
            .spawn(move || {
                // Tick = min(50 ms, period): fast stops for tests
                // (period 50 ms), bounded drop latency for prod.
                let tick = period.min(std::time::Duration::from_millis(50));
                let mut owed = period;
                loop {
                    if stop_thread.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(tick);
                    owed = match owed.checked_sub(tick) {
                        Some(r) => r,
                        None => std::time::Duration::ZERO,
                    };
                    if !owed.is_zero() {
                        continue;
                    }
                    owed = period;
                    if stop_thread.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    let mut rec = record.clone();
                    rec.heartbeat_at = Some(unix_now());
                    if !write_heartbeat(&path, &rec) {
                        // Lockfile no longer ours (external steal / gone):
                        // stop beating — never overwrite a newer holder.
                        eprintln!(
                            "vidgedb: writer lock '{}' heartbeat stopped \
(lockfile no longer owned by pid {})",
                            path.display(),
                            rec.pid
                        );
                        return;
                    }
                }
            })
            .ok();
        Heartbeat { stop, join }
    }

    /// Stop the thread and JOIN it (bounded by one tick, ≤ 50 ms).
    fn stop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.join.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for DbWriteLock {
    fn drop(&mut self) {
        // Stop + join the heartbeat BEFORE the unlink: a surviving thread
        // could resurrect the lockfile (temp+rename) behind the remover.
        self.heartbeat.stop();
        // Best-effort unlink; a leaked lockfile (SIGKILL of the owner) is
        // handled by the portable steal rules on the next open.
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(
            self.path
                .with_extension(format!("vdg-wlock.tmp{}", std::process::id())),
        );
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Serializable error returned by Agent API methods.
///
/// Deliberately distinct from `EngineError`: the agent never needs the
/// storage-layer internals, only a machine-readable message. Callers that
/// want the `{"error": ...}` shape (the v0 tool contract) use
/// `err_json(message)` instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentApiError {
    pub error: String,
}

impl std::fmt::Display for AgentApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for AgentApiError {}

impl From<crate::engine::EngineError> for AgentApiError {
    fn from(e: crate::engine::EngineError) -> Self {
        AgentApiError {
            error: format!("storage error: {:?}", e),
        }
    }
}

/// The v0 error payload shape: `{"error": "..."}` (no panic contract).
pub fn err_json(msg: impl Into<String>) -> Value {
    json!({ "error": msg.into() })
}

// ---------------------------------------------------------------------------
// Phase 11 — roles (the documented spec §55 derrogation)
// ---------------------------------------------------------------------------

/// Explicit authorization role of an `agent_id` (Phase 11).
///
/// - [`Role::Reader`] (default): the Phase 7 strict read-only behavior —
///   every write is refused (-32003) but audited.
/// - [`Role::Writer`] / [`Role::Ingest`]: may write telemetry and topology
///   through the Phase 11 ingest methods. In v1 both share the same
///   capability set; the distinction is intent + audit-trail legibility
///   (a PLC bridge declares `Ingest`, a full agent declares `Writer`).
///
/// v1 limit: NO cryptographic authentication — the role is asserted by the
/// local caller (`--role` flag) in a single-user deployment. A TCP/TLS
/// wrapper around the service will provide real authn/z later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Reader,
    Writer,
    Ingest,
}

impl Default for Role {
    /// Reader is the default role: absent an explicit choice, an agent
    /// keeps the un-derrogated Phase 7 behavior.
    fn default() -> Self {
        Role::Reader
    }
}

/// JSON-RPC server-defined error code carried by a write attempt from a
/// non-writer role (Phase 11; documented in the README JSON-RPC section).
pub const ERR_WRITE_FORBIDDEN: i64 = -32003;

/// Provenance for an ingested datum by declaring its SOURCE (spec §29:
/// ingested data = Fact or Observation, NEVER Hypothesis; the official
/// machine source is Fact, a sensor/agent feed is Observation).
pub fn provenance_for_source(source: &str) -> Option<u8> {
    match source {
        "plc" => Some(Provenance::Fact as u8),
        "agent" | "sensor" => Some(Provenance::Observation as u8),
        _ => None,
    }
}

/// One relation to create with an entity (Phase 11 `upsert_entity`).
#[derive(Debug, Clone, PartialEq)]
pub struct RelationSpec {
    /// Target entity NAME — must already exist (an ingest agent never
    /// invents topology endpoints; creating the other side is a separate,
    /// separate `upsert_entity` call).
    pub to: String,
    /// `topology:relation_type` — the topology prefix is REQUIRED (it is
    /// part of relation identity, spec §8; traversal on `electrical` is a
    /// different edge than traversal on `mechanical`).
    pub relation_type: String,
    /// Validity start (unix seconds); `None` = 0 (valid since the epoch).
    pub valid_from: Option<i64>,
}

// ---------------------------------------------------------------------------
// Audit trail (spec §56)
// ---------------------------------------------------------------------------

/// One audited Agent API call (spec §56 fields; v0 keeps the subset the
/// library can actually know: timestamp, agent_id, method, params).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Call time, unix seconds (spec §56 `timestamp`).
    pub timestamp: i64,
    /// Identity passed at `AgentApi::open` (spec §56 `agent_id`).
    pub agent_id: String,
    /// Method name, e.g. `"query"` (spec §56 `action`).
    pub method: String,
    /// Compact stringification of the call arguments (spec §56 `parameters`
    /// / `query` — a VQL query string is its own params summary).
    pub params_summary: String,
}

// ---------------------------------------------------------------------------
// AgentApi
// ---------------------------------------------------------------------------

/// JSON façade over the VidgeDB engine for AI agents.
///
/// `open(path, agent_id, role)` opens the database and installs the agent
/// id used in every audit entry plus the authorization [`Role`]. The
/// TimeSeriesStore is opened internally (the agent never touches it
/// directly).
pub struct AgentApi {
    eng: Engine,
    gs: GraphStore,
    ts: TimeSeriesStore,
    se: StateEventStore,
    agent_id: String,
    role: Role,
    audit: Vec<AuditEntry>,
    /// Held for the API's whole lifetime (Writer/Ingest only): the file's
    /// existence is the single-writer guarantee. `None` for a Reader.
    _write_lock: Option<DbWriteLock>,
}

impl AgentApi {
    /// Open a `.vdg` database as agent `agent_id` with an explicit
    /// authorization [`Role`].
    ///
    /// The TimeSeriesStore is opened eagerly (internally) so measurement
    /// methods work without a separate handle.
    ///
    /// The previous two-argument form (`open(path, agent_id)`, Phase 7/10)
    /// maps to [`Role::Reader`] via the `From` impl below — call sites that
    /// never write keep compiling unchanged.
    ///
    /// Phase 89 (soak wedge fix): a Writer/Ingest open takes the database's
    /// durable single-writer lock FIRST — a second concurrent writer fails
    /// closed here, before it can fork the sb/freelist/TsLayout state
    /// against the live owner. A STALE lock (its owner was SIGKILLed) is
    /// stolen on the spot — portable Phase-90 liveness rule (heartbeat
    /// age + legacy mtime; the /proc check stays a Linux-only bonus
    /// fast path) — and the steal is AUDITED (spec §56: never silent).
    /// The holder then beats its own lockfile every [`WRITER_HEARTBEAT`]
    /// (30 s) so a crashed holder on ANY OS is steppable.
    pub fn open_with_role<P: AsRef<std::path::Path>>(
        path: P,
        agent_id: impl Into<String>,
        role: Role,
    ) -> Result<AgentApi, AgentApiError> {
        Self::open_with_role_hb(path, agent_id, role, WRITER_HEARTBEAT)
    }

    /// `open_with_role` with an INJECTABLE heartbeat period (Phase 90:
    /// the tests drive a 50 ms heartbeat — the production default is
    /// [`WRITER_HEARTBEAT`] = 30 s via [`Self::open_with_role`]).
    pub fn open_with_role_hb<P: AsRef<std::path::Path>>(
        path: P,
        agent_id: impl Into<String>,
        role: Role,
        heartbeat_period: std::time::Duration,
    ) -> Result<AgentApi, AgentApiError> {
        let agent_id = agent_id.into();
        let (write_lock, stolen) = match role {
            Role::Reader => (None, None),
            Role::Writer | Role::Ingest => {
                let (lock, stolen) = DbWriteLock::acquire(path.as_ref(), heartbeat_period)?;
                (Some(lock), stolen)
            }
        };
        let mut audit = Vec::new();
        if let Some(reason) = stolen {
            audit.push(AuditEntry {
                timestamp: unix_now(),
                agent_id: agent_id.clone(),
                method: "open_with_role".to_string(),
                params_summary: format!("STALE WRITER LOCK STOLEN: {}", reason),
            });
        }
        // NOTE: the Engine open (recovery) runs INSIDE the lock already —
        // no other writer can interleave a commit into the recovery. Every
        // error path releases the lock first (by-value drop through the
        // macro below), so a failed open can never hold the database shut.
        macro_rules! open_step {
            ($step:expr) => {
                match $step {
                    Ok(v) => v,
                    Err(e) => {
                        drop(write_lock);
                        return Err(AgentApiError {
                            error: format!("{:?}", e),
                        });
                    }
                }
            };
        }
        let mut eng = open_step!(Engine::open(path.as_ref()));
        let mut gs = open_step!(GraphStore::open(&mut eng));
        let ts = open_step!(TimeSeriesStore::open(&mut eng, &mut gs));
        let se = open_step!(StateEventStore::open(&mut eng, &mut gs));
        Ok(AgentApi {
            eng,
            gs,
            ts,
            se,
            agent_id: agent_id.into(),
            role,
            audit, // may already carry the Phase-90 steal entry
            _write_lock: write_lock,
        })
    }

    /// Back-compat constructor (Phase 7/10 signature): opens as
    /// [`Role::Reader`], the strict read-only default.
    pub fn open<P: AsRef<std::path::Path>>(
        path: P,
        agent_id: impl Into<String>,
    ) -> Result<AgentApi, AgentApiError> {
        Self::open_with_role(path, agent_id, Role::Reader)
    }

    /// The agent_id this API opens with (HTTP /health surfacing, Phase 14).
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// The role this API was opened with (surfaced for tests/CLI).
    pub fn role(&self) -> Role {
        self.role
    }

    /// Number of live entities (Phase 16: the OPC-UA address-space builder
    /// walks keys 0..entity_count). Read-only, audited as `schema`.
    pub fn entity_count(&mut self) -> u32 {
        self.audit("schema", "entity_count");
        self.gs.entity_count()
    }

    /// Public series lookup (`<entity>.<signal>` -> series id or None) —
    /// Phase 16: the OPC-UA layer needs the same resolution the private
    /// `find_series` performs; read-only, audited as `get_measurements`.
    pub fn find_series_public(
        &mut self,
        entity_name: &str,
        signal: &str,
    ) -> Result<Option<u32>, AgentApiError> {
        self.audit(
            "get_measurements",
            format!("lookup series {}.{}", entity_name, signal),
        );
        self.find_series(entity_name, signal)
    }

    /// True when this role may write (Writer or Ingest).
    fn can_write(&self) -> bool {
        !matches!(self.role, Role::Reader)
    }

    /// Role gate for every write method (Phase 11). Refusal is AUDITED
    /// too (spec §56: even forbidden attempts leave a trail) and carries
    /// the documented -32003 code in its message so the service layer can
    /// map it onto the JSON-RPC error member.
    fn require_writer(&mut self, method: &str) -> Result<(), AgentApiError> {
        if self.can_write() {
            return Ok(());
        }
        self.audit(method, "REFUSED (role=Reader, write forbidden -32003)");
        Err(AgentApiError {
            error: format!(
                "write forbidden (-32003): agent '{}' holds role Reader; telemetry/topology \
ingestion requires opening the API with Role::Writer or Role::Ingest (spec §55 \
derrogation, Phase 11). The attempt is recorded in the audit trail.",
                self.agent_id
            ),
        })
    }

    /// Log one call (spec §56). Private: audit is filled by the façade.
    fn audit(&mut self, method: &str, params_summary: impl Into<String>) {
        self.audit.push(AuditEntry {
            timestamp: unix_now(),
            agent_id: self.agent_id.clone(),
            method: method.to_string(),
            params_summary: params_summary.into(),
        });
    }

    /// The audit trail so far (spec §56). Itself audited: an agent reading
    /// the trail is also traceable.
    pub fn audit_log(&mut self) -> Vec<AuditEntry> {
        self.audit("audit_log", "");
        self.audit.clone()
    }

    // -----------------------------------------------------------------------
    // schema() — inventory of what the agent can query
    // -----------------------------------------------------------------------

    /// Inventory of the database, derived from the interned slabs:
    /// distinct entity types, relation topology classes, time-series names,
    /// provenance classes. Lets an agent plan queries without blind probing.
    pub fn schema(&mut self) -> Result<Value, AgentApiError> {
        self.audit("schema", "");
        let mut types: Vec<String> = Vec::new();
        let mut topologies: Vec<String> = Vec::new();
        for key in 0..self.gs.entity_count() {
            let t_sid = self.gs.entity_type_sid(&mut self.eng, key)?;
            let t = self.gs.get_str(&mut self.eng, t_sid)?;
            if !types.contains(&t) {
                types.push(t);
            }
        }
        for rel in self.gs.relations.clone() {
            let topo = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(rel.topo_sid))?;
            if !topologies.contains(&topo) {
                topologies.push(topo);
            }
        }
        types.sort();
        topologies.sort();
        let series: Vec<String> = self.ts.series.iter().map(|s| s.name.clone()).collect();
        Ok(json!({
            "entity_types": types,
            "relation_topologies": topologies,
            "series": series,
            "provenance_classes": provenance_class_names(),
        }))
    }

    // -----------------------------------------------------------------------
    // query() — core VidgeQL (MATCH / WHERE / RETURN / LIMIT)
    // -----------------------------------------------------------------------

    /// Execute a core VidgeQL query; rows carry every binding with the
    /// entity's name / type / inline properties resolved.
    ///
    /// Parse failures and storage errors come back as `{"error": "..."}`
    /// (never a panic, per the Phase 7 contract).
    pub fn query(&mut self, vql: &str) -> Result<Value, AgentApiError> {
        self.audit("query", vql);
        let q = match crate::vidgeql::Parser::parse(vql) {
            Ok(q) => q,
            Err(e) => return Ok(err_json(format!("parse error: {}", e))),
        };
        let rows = match executor::execute(&mut self.eng, &mut self.gs, &q) {
            Ok(r) => r,
            Err(e) => return Ok(err_json(format!("execution error: {:?}", e))),
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut obj = serde_json::Map::new();
            for (var, key) in &row.bindings {
                obj.insert(var.clone(), self.entity_summary(*key)?);
            }
            out.push(Value::Object(obj));
        }
        Ok(json!({ "rows": out, "n": out.len() }))
    }

    // -----------------------------------------------------------------------
    // query_temporal() — MEASURE / DURING (spec §23)
    // -----------------------------------------------------------------------

    /// Temporal VidgeQL: `MATCH … MEASURE var.signal DURING last(24h) RETURN
    /// max(signal)…`. `now` anchors relative windows (`last(...)`).
    pub fn query_temporal(&mut self, vql: &str, now: i64) -> Result<Value, AgentApiError> {
        self.audit("query_temporal", vql);
        let q = match crate::temporal::parse_tquery(vql, now) {
            Ok(q) => q,
            Err(e) => return Ok(err_json(format!("parse error: {}", e))),
        };
        let rows = match crate::temporal::execute_t(&mut self.eng, &mut self.gs, &mut self.ts, &q) {
            Ok(r) => r,
            Err(e) => return Ok(err_json(format!("execution error: {:?}", e))),
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut obj = serde_json::Map::new();
            for (var, key) in &row.bindings {
                obj.insert(var.clone(), self.entity_summary(*key)?);
            }
            for (label, agg) in &row.aggs {
                obj.insert(
                    label.clone(),
                    json!({ "kind": format!("{:?}", agg.kind), "value": agg.value }),
                );
            }
            out.push(Value::Object(obj));
        }
        Ok(json!({ "rows": out, "n": out.len() }))
    }

    // -----------------------------------------------------------------------
    // get_entity() — full entity card
    // -----------------------------------------------------------------------

    /// Complete entity record: name, type, properties, relations in/out
    /// (each with type, topology, the other endpoint's name and the
    /// relation's provenance class).
    ///
    /// Lookup is by exact entity key (the stable cell index).
    pub fn get_entity(&mut self, key: u32) -> Result<Value, AgentApiError> {
        self.audit("get_entity", format!("key={}", key));
        let (name, ty) = match self.entity_name_type(key) {
            Ok(v) => v,
            Err(_) => {
                return Ok(err_json(format!("unknown entity key {}", key)));
            }
        };
        let props: serde_json::Map<String, Value> = self
            .gs
            .entity_props(&mut self.eng, key)?
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        let mut relations_out = Vec::new();
        let mut relations_in = Vec::new();
        for (idx, rel) in self.gs.relations.clone().iter().enumerate() {
            if !rel.is_alive() {
                continue;
            }
            let rtype = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(rel.type_sid))?;
            let topo = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(rel.topo_sid))?;
            let prov = provenance_name(rel.provenance);
            if rel.src == key {
                relations_out.push(json!({
                    "relation_idx": idx,
                    "type": rtype,
                    "topology": topo,
                    "to": self.entity_name(rel.dst)?,
                    "provenance": prov,
                    "valid_from": rel.valid_from,
                    "valid_to": rel.valid_to,
                }));
            }
            if rel.dst == key {
                relations_in.push(json!({
                    "relation_idx": idx,
                    "type": rtype,
                    "topology": topo,
                    "from": self.entity_name(rel.src)?,
                    "provenance": prov,
                    "valid_from": rel.valid_from,
                    "valid_to": rel.valid_to,
                }));
            }
        }
        Ok(json!({
            "key": key,
            "name": name,
            "type": ty,
            "properties": props,
            "relations_out": relations_out,
            "relations_in": relations_in,
        }))
    }

    // -----------------------------------------------------------------------
    // get_measurements() — raw telemetry window (spec §28)
    // -----------------------------------------------------------------------

    /// Raw points of the series `<entity_name>.<signal>` in [from, to]
    /// (inclusive unix-seconds bounds), plus count / min / max. `min`/`max`
    /// are `null` when the window holds no points.
    pub fn get_measurements(
        &mut self,
        entity_name: &str,
        signal: &str,
        from: i64,
        to: i64,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "get_measurements",
            format!(
                "entity={} signal={} [{}..{}]",
                entity_name, signal, from, to
            ),
        );
        let sid = self.find_series(entity_name, signal)?;
        let Some(sid) = sid else {
            return Ok(err_json(format!(
                "no series named '{}.{}'",
                entity_name, signal
            )));
        };
        let pts = self.ts.query(&mut self.eng, sid, from, to)?;
        let count = pts.len();
        let min = pts.iter().map(|(_, v)| *v).fold(f64::INFINITY, f64::min);
        let max = pts
            .iter()
            .map(|(_, v)| *v)
            .fold(f64::NEG_INFINITY, f64::max);
        let min = if count == 0 { Value::Null } else { json!(min) };
        let max = if count == 0 { Value::Null } else { json!(max) };
        let points: Vec<Value> = pts
            .iter()
            .map(|(t, v)| json!({ "t": t, "value": v }))
            .collect();
        Ok(json!({
            "points": points,
            "count": count,
            "min": min,
            "max": max,
        }))
    }

    // -----------------------------------------------------------------------
    // check() — spec §25 deviation engine
    // -----------------------------------------------------------------------

    /// CHECK one entity+signal over a window (spec §25): expected comes from
    /// the `spec.<signal>.max` property, observed = worst-case max of the
    /// window. Statuses OK / VIOLATION / NO_DATA / NO_SPEC. The spec §27
    /// JSON example shape is a superset of this payload.
    pub fn check(
        &mut self,
        entity_name: &str,
        signal: &str,
        from: i64,
        to: i64,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "check",
            format!(
                "entity={} signal={} [{}..{}]",
                entity_name, signal, from, to
            ),
        );
        let key = match self.entity_key_by_name(entity_name)? {
            Some(k) => k,
            None => {
                return Ok(err_json(format!("unknown entity '{}'", entity_name)));
            }
        };
        let window = Window { from, to };
        let res = check_entity(
            &mut self.eng,
            &mut self.gs,
            &mut self.ts,
            key,
            signal,
            window,
        )?;
        Ok(check_result_json(&res))
    }

    // -----------------------------------------------------------------------
    // trace() — path between two entities by name
    // -----------------------------------------------------------------------

    /// Find a path `from_name -> to_name` over the relation adjacency,
    /// following OUT-edges only, across ALL topology classes (the topology
    /// of each hop is reported in the steps), bounded by `max_hops` hops of
    /// traversal (so up to `max_hops + 1` entities appear in the path).
    ///
    /// BFS keeps the first (shortest) path found and, in keeping with
    /// read-only vigilance, never invents edges: a step lists only relations
    /// that exist in the slab.
    pub fn trace(
        &mut self,
        from_name: &str,
        to_name: &str,
        max_hops: usize,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "trace",
            format!("from={} to={} max_hops={}", from_name, to_name, max_hops),
        );
        let from = match self.entity_key_by_name(from_name)? {
            Some(k) => k,
            None => {
                return Ok(err_json(format!("unknown entity '{}'", from_name)));
            }
        };
        let to = match self.entity_key_by_name(to_name)? {
            Some(k) => k,
            None => {
                return Ok(err_json(format!("unknown entity '{}'", to_name)));
            }
        };
        if from == to {
            let node = self.entity_summary(from)?;
            return Ok(json!({ "found": true, "steps": [], "path": [node] }));
        }
        // BFS over out-edges of any topology, tracking predecessor + the
        // relation index used for each discovered node.
        let n = self.gs.entity_count();
        let mut prev: Vec<Option<(u32, u32)>> = vec![None; n as usize]; // (pred_key, rel_idx)
        let mut seen = vec![false; n as usize];
        seen[from as usize] = true;
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(from);
        let mut found = false;
        while let Some(cur) = queue.pop_front() {
            let hops = self.hops_from_root(&prev, from, cur);
            if hops as usize >= max_hops {
                continue;
            }
            // Collect edges (neighbor, relation idx) then sort by relation
            // index so discovery order — and thus the returned path — is
            // deterministic across runs.
            let mut edges: Vec<(u32, u32)> = self
                .gs
                .adjacency
                .out
                .get(&cur)
                .map(|v| {
                    v.iter()
                        .map(|&(_, other, i)| (other, i))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            edges.sort_by_key(|&(_, i)| i);
            for (other, idx) in edges {
                if seen[other as usize] {
                    continue;
                }
                seen[other as usize] = true;
                prev[other as usize] = Some((cur, idx));
                if other == to {
                    found = true;
                    break;
                }
                queue.push_back(other);
            }
            if found {
                break;
            }
        }
        if !found {
            return Ok(json!({ "found": false, "steps": [], "path": [] }));
        }
        // Reconstruct the relation-idx chain from `to` back to `from`.
        let mut rel_path: Vec<u32> = Vec::new();
        let mut cur = to;
        while let Some((pred, idx)) = prev[cur as usize] {
            rel_path.push(idx);
            cur = pred;
        }
        rel_path.reverse();
        // Walk the path, emitting one step per relation with resolved names.
        let mut steps = Vec::new();
        let mut path_nodes = vec![self.entity_summary(from)?];
        cur = from;
        for idx in rel_path {
            let r = self.gs.relations[idx as usize];
            let rtype = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(r.type_sid))?;
            let topo = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(r.topo_sid))?;
            let dst_name = self.entity_name(r.dst)?;
            steps.push(json!({
                "from": self.entity_name(cur)?,
                "relation_type": rtype,
                "topology": topo,
                "to": dst_name,
                "provenance": provenance_name(r.provenance),
                "relation_idx": idx,
            }));
            path_nodes.push(self.entity_summary(r.dst)?);
            cur = r.dst;
        }
        Ok(json!({
            "found": true,
            "steps": steps,
            "path": path_nodes,
            "n_hops": steps.len(),
        }))
    }

    /// Chain length (edges) from the BFS root to `key`, via `prev`.
    fn hops_from_root(&self, prev: &[Option<(u32, u32)>], root: u32, key: u32) -> u32 {
        let mut hops = 0u32;
        let mut cur = key;
        while cur != root {
            match prev[cur as usize] {
                Some((p, _)) => {
                    hops += 1;
                    cur = p;
                }
                None => return u32::MAX, // not reached yet / disconnected
            }
        }
        hops
    }

    // -----------------------------------------------------------------------
    // provenance() — provenance surfacing + the write refusal (spec §14/§29)
    // -----------------------------------------------------------------------

    /// Provenance explainer plus the provenance bytes of every live relation
    /// (spec §56: the agent must be able to see where each fact came from).
    ///
    /// ## The provenance invariant (spec §29, Principle 3)
    ///
    /// VidgeDB's central invariant: a `Hypothesis` SHALL NEVER silently
    /// become a Fact/Observation. Since the Phase 11 derrogation a
    /// Writer/Ingest agent CAN write — but only telemetry and topology, and
    /// every ingested datum enters as `Fact` (source "plc") or
    /// `Observation` ("agent"/"sensor", spec §29). There is still NO
    /// hypothesis-promotion path: [`AgentApi::set_hypothesis`] refuses for
    /// EVERY role (see below), and the engine never promotes. The read-only
    /// note below is kept for the Reader default; Writer/Ingest get the
    /// Phase 11 wording.
    pub fn provenance(&mut self) -> Result<Value, AgentApiError> {
        self.audit("provenance", "");
        let note = if self.can_write() {
            "Provenance classes: Fact=0 (structural ground truth), \
Observation=1 (sensor/PLC reading), Specification=2 (design intent), \
Inference=3 (derived), Hypothesis=4 (diagnostic guess), Event=5, Command=6, \
Configuration=7. The engine never promotes Hypothesis to Fact/Observation. \
This agent holds a WRITING role (Phase 11 derrogation to spec §55): it may \
ingest telemetry/topology, and every ingested datum enters as Fact (source \
plc) or Observation (source agent/sensor) — never as Hypothesis; \
set_hypothesis stays refused for every role."
        } else {
            "Provenance classes: Fact=0 (structural ground truth), \
Observation=1 (sensor/PLC reading), Specification=2 (design intent), \
Inference=3 (derived), Hypothesis=4 (diagnostic guess), Event=5, Command=6, \
Configuration=7. The engine never promotes Hypothesis to Fact/Observation; \
this agent holds the Reader role (spec §55): every write is refused and \
audited, so it cannot create or promote anything — every value below keeps \
the provenance recorded at ingestion."
        };
        let mut relations = Vec::new();
        for (idx, rel) in self.gs.relations.clone().iter().enumerate() {
            if !rel.is_alive() {
                continue;
            }
            let rtype = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(rel.type_sid))?;
            let topo = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(rel.topo_sid))?;
            relations.push(json!({
                "relation_idx": idx,
                "src": self.entity_name(rel.src)?,
                "dst": self.entity_name(rel.dst)?,
                "type": rtype,
                "topology": topo,
                "provenance_byte": rel.provenance,
                "provenance": provenance_name(rel.provenance),
                "valid_from": rel.valid_from,
                "valid_to": rel.valid_to,
            }));
        }
        Ok(json!({
            "note": note,
            "provenance_classes": provenance_class_names(),
            "relations": relations,
        }))
    }

    /// Explicitly FORBIDDEN for EVERY role (spec §29): an agent must never
    /// be able to create or promote a hypothesis — even a Writer/Ingest
    /// (Phase 11 derrogation covers TELEMETRY and TOPOLOGY only; provenance
    /// classes are never agent-writable). The method exists so the refusal
    /// is testable and the error machine-readable — it always fails with
    /// `{"error": ...}` and mutates nothing (audited for every role).
    pub fn set_hypothesis(&mut self, _entity: &str, _text: &str) -> Result<Value, AgentApiError> {
        self.audit(
            "set_hypothesis",
            "REFUSED (no hypothesis write in any role, spec §29)",
        );
        Ok(err_json(
            "refused: no agent role may write a hypothesis in any phase (spec §29/§55); \
a hypothesis must never be promoted to fact by the engine or by an agent — \
use an explicit, human-audited external write outside this API",
        ))
    }

    // -----------------------------------------------------------------------
    // state & events — Phase 4 read views (spec §10/§15)
    // -----------------------------------------------------------------------

    /// The state of `state_key` on one entity, valid at `at` (`None` = now),
    /// with its `valid_from` (spec §10). Unknown entity/key -> `null`.
    pub fn get_state(
        &mut self,
        entity_name: &str,
        state_key: &str,
        at: Option<i64>,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "get_state",
            format!(
                "entity={} key={} at={}",
                entity_name,
                state_key,
                at.map_or("now".to_string(), |t| t.to_string())
            ),
        );
        let st = self
            .se
            .get_state(&mut self.eng, entity_name, state_key, at)?;
        Ok(match st {
            Some((value, from)) => json!({ "value": value, "valid_from": from }),
            None => Value::Null,
        })
    }

    /// The full state history of one (entity, key): every pair
    /// `[valid_from, valid_to)` with `valid_to == -1` = open (spec §10:
    /// state history SHALL be queryable). Unknown entity/key -> empty.
    pub fn state_history(
        &mut self,
        entity_name: &str,
        state_key: &str,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "state_history",
            format!("entity={} key={}", entity_name, state_key),
        );
        let hist = self
            .se
            .state_history(&mut self.eng, entity_name, state_key)?;
        let entries: Vec<Value> = hist
            .into_iter()
            .map(|(value, from, to)| json!({ "value": value, "valid_from": from, "valid_to": to }))
            .collect();
        Ok(json!({ "history": entries, "n": entries.len() }))
    }

    /// Events in the inclusive window `[from, to]`, filtered by entity
    /// (`None` = all), with provenance name + byte (spec §15/§56: the agent
    /// sees where each event came from).
    pub fn get_events(
        &mut self,
        entity_name: Option<&str>,
        from: i64,
        to: i64,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "get_events",
            format!("entity={} [{}..{}]", entity_name.unwrap_or("*"), from, to),
        );
        let evs = self.se.get_events(&mut self.eng, entity_name, from, to)?;
        let out: Vec<Value> = evs
            .into_iter()
            .map(|e| {
                json!({
                    "name": e.name,
                    "entity": e.entity,
                    "timestamp": e.timestamp,
                    "provenance": provenance_name(e.provenance),
                    "provenance_byte": e.provenance,
                    "details": e.details,
                })
            })
            .collect();
        Ok(json!({ "events": out, "n": out.len() }))
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Series id of `<entity_name>.<signal>`, matching the naming convention
    /// shared by temporal.rs / check.rs.
    fn find_series(&self, entity_name: &str, signal: &str) -> Result<Option<u32>, AgentApiError> {
        let name = format!("{}.{}", entity_name, signal);
        Ok(self
            .ts
            .series
            .iter()
            .position(|s| s.name == name)
            .map(|i| i as u32))
    }

    /// (name, type) of an entity key.
    fn entity_name_type(&mut self, key: u32) -> Result<(String, String), AgentApiError> {
        let n_sid = self.gs.entity_name_sid(&mut self.eng, key)?;
        let t_sid = self.gs.entity_type_sid(&mut self.eng, key)?;
        let name = self.gs.get_str(&mut self.eng, n_sid)?;
        let ty = self.gs.get_str(&mut self.eng, t_sid)?;
        Ok((name, ty))
    }

    /// Entity name by key.
    fn entity_name(&mut self, key: u32) -> Result<String, AgentApiError> {
        let n_sid = self.gs.entity_name_sid(&mut self.eng, key)?;
        self.gs.get_str(&mut self.eng, n_sid).map_err(Into::into)
    }

    /// Entity key by exact name (interned-string lookup).
    fn entity_key_by_name(&mut self, name: &str) -> Result<Option<u32>, AgentApiError> {
        // The string arena dedups, so the name's sid, if present, is exactly
        // the sid the entity cell points at.
        if let Some(crate::stores::StrId(sid)) = self.gs.str_lookup(name) {
            for key in 0..self.gs.entity_count() {
                let n_sid = self.gs.entity_name_sid(&mut self.eng, key)?;
                if n_sid.0 == sid {
                    return Ok(Some(key));
                }
            }
        }
        Ok(None)
    }

    /// Compact binding summary used inside query rows:
    /// {key, name, type, properties}.
    fn entity_summary(&mut self, key: u32) -> Result<Value, AgentApiError> {
        let (name, ty) = self.entity_name_type(key)?;
        let props: serde_json::Map<String, Value> = self
            .gs
            .entity_props(&mut self.eng, key)?
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        Ok(json!({
            "key": key,
            "name": name,
            "type": ty,
            "properties": props,
        }))
    }

    // -----------------------------------------------------------------------
    // Plateforme : inventaire du graphe et diagnostic en un appel JSON-RPC
    // -----------------------------------------------------------------------

    /// Enumere les entites SANS que l'appelant les connaisse d'avance.
    ///
    /// POURQUOI : `MATCH (x:Type) RETURN x` exige deja un type, et un
    /// `MATCH (x) RETURN x` sans type ne renvoie aucune ligne. L'inventaire
    /// etait donc impossible en un appel, et l'editeur de graphe ne pouvait
    /// pas peupler sa liste de noeuds autrement qu'en devinant les noms.
    ///
    /// `type_filter` restreint a un type (la plateforme liste par famille).
    /// `limit == 0` = pas de plafond. La reponse porte toujours `total` (le
    /// nombre de correspondances) et `truncated`, pour qu'une page tronquee
    /// ne soit jamais confondue avec l'ensemble complet.
    pub fn list_entities(
        &mut self,
        type_filter: Option<&str>,
        limit: usize,
    ) -> Result<Value, AgentApiError> {
        self.audit(
            "list_entities",
            format!(
                "type={} limit={}",
                type_filter.unwrap_or("(any)"),
                if limit == 0 {
                    "none".to_string()
                } else {
                    limit.to_string()
                }
            ),
        );
        let entity_count = self.gs.entity_count();
        let mut matched = Vec::new();
        for key in 0..entity_count {
            let (name, ty) = self.entity_name_type(key)?;
            if let Some(want) = type_filter {
                if ty != want {
                    continue;
                }
            }
            matched.push(json!({ "key": key, "name": name, "type": ty }));
        }
        let total = matched.len();
        let truncated = limit != 0 && total > limit;
        if truncated {
            matched.truncate(limit);
        }
        Ok(json!({
            "entities": matched,
            "total": total,
            "entities_in_db": entity_count,
            "truncated": truncated,
        }))
    }

    /// Rend TOUT le graphe en un seul aller-retour : c'est exactement ce que
    /// dessine l'editeur de relations.
    ///
    /// POURQUOI : sans cela, dessiner N noeuds coute une requete par noeud
    /// (`get_entity`) plus une par relation, soit O(N+E) appels JSON-RPC.
    ///
    /// Les extremites sont rendues par NOM et non par cle interne : un client
    /// ne doit pas avoir a resoudre les cles une a une pour tracer l'arete.
    /// Seules les relations VIVANTES sortent (les enregistrements supprimes
    /// sont des tombstones, jamais retires physiquement).
    pub fn graph(&mut self, limit: usize) -> Result<Value, AgentApiError> {
        self.audit(
            "graph",
            format!("limit={}", if limit == 0 { 0 } else { limit }),
        );
        let entity_count = self.gs.entity_count();
        let mut names: std::collections::HashMap<u32, String> = std::collections::HashMap::new();
        let mut entities = Vec::new();
        for key in 0..entity_count {
            let (name, ty) = self.entity_name_type(key)?;
            names.insert(key, name.clone());
            entities.push(json!({ "key": key, "name": name, "type": ty }));
        }
        // Copie de la table : `get_str` a besoin de `&self` ET de `&mut eng`
        // en meme temps, ce qui est impossible en iterant l'emprunt direct.
        let records = self.gs.relations.clone();
        let live_total = records.iter().filter(|r| r.is_alive()).count();
        let mut relations = Vec::new();
        for r in records.iter().filter(|r| r.is_alive()) {
            if limit != 0 && relations.len() >= limit {
                break;
            }
            let from_name = names
                .get(&r.src)
                .cloned()
                .unwrap_or_else(|| format!("<key:{}>", r.src));
            let to_name = names
                .get(&r.dst)
                .cloned()
                .unwrap_or_else(|| format!("<key:{}>", r.dst));
            let rtype = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(r.type_sid))?;
            let topo = self
                .gs
                .get_str(&mut self.eng, crate::stores::StrId(r.topo_sid))?;
            let relation_type = if topo.is_empty() {
                rtype
            } else {
                format!("{}:{}", topo, rtype)
            };
            relations.push(json!({
                "from": from_name,
                "to": to_name,
                "relation_type": relation_type,
                "valid_from": r.valid_from,
                "valid_to": r.valid_to,
                "provenance": crate::diagnose::prov_byte_name(r.provenance),
            }));
        }
        Ok(json!({
            "entities": entities,
            "relations": relations,
            "relations_total": live_total,
            "truncated": limit != 0 && live_total > limit,
        }))
    }

    /// Le pipeline 8 etapes de la spec §26, expose en JSON-RPC.
    ///
    /// POURQUOI : il n'existait qu'en bibliotheque Rust (`src/diagnose.rs`),
    /// donc la plateforme aurait du etre ecrite en Rust pour l'appeler.
    ///
    /// Strictement en lecture seule : aucun store n'est mute, et les
    /// hypotheses de l'etape 8 ne sont JAMAIS persistees (spec §29).
    /// Une entite inconnue rend un rapport explicite (`component: null`)
    /// plutot qu'une erreur de transport : l'appelant affiche « composant
    /// introuvable » sans casser sa page.
    pub fn diagnose(&mut self, entity: &str, from: i64, to: i64) -> Result<Value, AgentApiError> {
        self.audit(
            "diagnose",
            format!("entity={} window=[{},{}]", entity, from, to),
        );
        let report =
            crate::diagnose::diagnose(&mut self.eng, &mut self.gs, &mut self.ts, entity, from, to)?;
        Ok(serde_json::to_value(&report).unwrap_or(Value::Null))
    }
}

// ---------------------------------------------------------------------------
// Free helpers (serde-friendly serialization of core result types)
// ---------------------------------------------------------------------------

/// Serialize a `CheckResult` (spec §25, with the spec §27 example fields).
pub fn check_result_json(res: &crate::check::CheckResult) -> Value {
    json!({
        "entity": res.entity_name,
        "signal": res.signal,
        "status": res.status.as_str(),
        "expected_max": res.expected_max,
        "observed": res.observed,
        "deviation": res.deviation,
        "unit": res.unit,
        "expected_provenance": res.expected_prov.map(|p| provenance_name(p as u8)),
        "observed_provenance": res.observed_prov.map(|p| provenance_name(p as u8)),
        "points_checked": res.points_checked,
        "window": { "from": res.window.from, "to": res.window.to },
    })
}

/// Names of the 8 provenance classes (spec §14), in byte order.
pub fn provenance_class_names() -> Vec<&'static str> {
    [
        Provenance::Fact,
        Provenance::Observation,
        Provenance::Specification,
        Provenance::Inference,
        Provenance::Hypothesis,
        Provenance::Event,
        Provenance::Command,
        Provenance::Configuration,
    ]
    .iter()
    .map(|p| provenance_name(*p as u8))
    .collect()
}

/// Name for a raw provenance byte (0..7); unknown bytes surface as
/// `"Unknown"` rather than being silently remapped.
pub fn provenance_name(p: u8) -> &'static str {
    match p {
        0 => "Fact",
        1 => "Observation",
        2 => "Specification",
        3 => "Inference",
        4 => "Hypothesis",
        5 => "Event",
        6 => "Command",
        7 => "Configuration",
        _ => "Unknown",
    }
}

/// Unix seconds now (audit timestamps).
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Phase 10 additions — bridge methods the service mode needs.
// log_event is an append-write reserved to WRITING roles (Phase 11 routing):
// events are append-only per spec §15; retain is the ONLY destructive op
// and works at chunk granularity (never 1 point — see timeseries.rs).
// Reader keeps READ access to state/events (get_state/get_events are on the
// read side and stay ungated); only the writes are role-gated.
// Both stay audited (spec §56).
// ---------------------------------------------------------------------------

impl AgentApi {
    /// Append one event (spec §15). Phase 10 bridge, Phase 11-gated:
    /// requires [`Role::Writer`] / [`Role::Ingest`]; a Reader gets the
    /// -32003 refusal (and the attempt its audit entry).
    /// The agent cannot borrow the Engine (AgentApi owns it) — this
    /// wrapper drives the internal engine for the whole operation.
    pub fn log_event(
        &mut self,
        event_name: &str,
        entity: &str,
        timestamp: i64,
        provenance: u8,
        details: &str,
    ) -> Result<Value, AgentApiError> {
        self.require_writer("log_event")?;
        self.audit(
            "log_event",
            format!(
                "{} on {} at {} (prov={})",
                event_name, entity, timestamp, provenance
            ),
        );
        let mut tx = self.eng.begin()?;
        self.se.log_event(
            &mut self.eng,
            &mut tx,
            event_name,
            entity,
            timestamp,
            provenance,
            details,
        )?;
        self.eng.commit(tx)?;
        self.se.persist(&mut self.eng)?;
        Ok(json!({ "logged": true, "event": event_name, "entity": entity }))
    }

    /// Retention enforcement (spec §37). Phase 10 bridge over
    /// [`TimeSeriesStore::retain`]: everything older than `before`
    /// (unix seconds) that forms WHOLE chunks is dropped — see the
    /// doc comment there for the chunk-granularity rule.
    ///
    /// Phase 11 gate: destructive ⇒ Writer/Ingest only (a Reader that
    /// triggered retention would be a silent write path).
    pub fn retain(&mut self, before: i64) -> Result<Value, AgentApiError> {
        self.require_writer("retain")?;
        self.audit("retain", format!("before={}", before));
        let (points, chunks) = self.ts.retain(&mut self.eng, before)?;
        Ok(json!({
            "points_removed": points,
            "chunks_removed": chunks,
        }))
    }

    // -----------------------------------------------------------------------
    // Phase 11 — the ingestion path (the documented §55 derrogation)
    // -----------------------------------------------------------------------

    /// Ingest a batch of telemetry points into the series
    /// `<entity_name>.<signal>` (the naming convention shared with
    /// temporal.rs / check.rs). Writer/Ingest only (-32003 for a Reader).
    ///
    /// - **Batched, one big tx**: the whole call is ONE transaction — the
    ///   series record creation (if needed) + the appends + one final
    ///   flush of every leftover buffered point. The internal buffer
    ///   auto-flushes a chunk every [`crate::timeseries::BATCH`] (256)
    ///   points INSIDE the tx too, so a 10 K-point batch produces ~40
    ///   chunks but still a single commit (one fsync).
    /// - **Auto-create_series**: an absent series is created on the fly
    ///   with `Observation` provenance semantics (a sensor feed); the
    ///   CREATE is audited.
    /// - Provenance lives on the series' owning entity's relations, not
    ///   the points: time-series points carry no per-point provenance
    ///   byte in the Phase 3 wire format — the SERIES declares the source.
    ///
    /// Returns `{accepted, chunks_flushed, series_created}`.
    pub fn ingest_points(
        &mut self,
        entity_name: &str,
        signal: &str,
        points: &[(i64, f64)],
    ) -> Result<Value, AgentApiError> {
        self.require_writer("ingest_points")?;
        self.audit(
            "ingest_points",
            format!(
                "entity={} signal={} n={}",
                entity_name,
                signal,
                points.len()
            ),
        );
        let sid = match self.find_series(entity_name, signal)? {
            Some(sid) => sid,
            None => {
                // Auto-create (spec §29: a sensor feed = Observation — a
                // series carries no provenance byte of its own; its NAME
                // convention and this audit line are the record).
                let series_name = format!("{}.{}", entity_name, signal);
                let bytes = series_name.as_bytes();
                if bytes.len() > crate::timeseries::NAME_MAX {
                    return Ok(err_json(format!(
                        "series name '{}.{}' too long ({} > {} bytes)",
                        entity_name,
                        signal,
                        bytes.len(),
                        crate::timeseries::NAME_MAX
                    )));
                }
                let mut tx = self.eng.begin()?;
                let sid = self
                    .ts
                    .create_series(&mut self.eng, &mut tx, &series_name)?;
                self.eng.commit(tx)?;
                self.ts.persist(&mut self.eng)?;
                sid
            }
        };
        // ONE tx for the whole batch (perf contract: one commit protocol
        // run per ingest call, not per point).
        let chunks_before = self.ts.committed_chunks(sid);
        let mut tx = self.eng.begin()?;
        let mut accepted: usize = 0;
        for &(t, v) in points {
            self.ts.append(&mut self.eng, &mut tx, sid, t, v)?;
            accepted += 1;
        }
        self.ts.flush_series(&mut self.eng, &mut tx, sid)?;
        self.eng.commit(tx)?;
        // Persist the series-slab layout counts (create_series path) —
        // same ordering rule as the fixture code: commit data FIRST,
        // then persist the layout.
        self.ts.persist(&mut self.eng)?;
        // Chunks persisted BY THIS CALL (delta, not the series total).
        let chunks_flushed = self.ts.committed_chunks(sid) - chunks_before;
        Ok(json!({
            "accepted": accepted,
            "chunks_flushed": chunks_flushed,
            "series_id": sid,
        }))
    }

    /// Create OR update an entity (Phase 11). Merge semantics on props:
    /// an existing prop is ONLY overwritten by a non-null new value (a
    /// `null` in `props` is skipped, never erasing what the machine
    /// already declared). Relations are APPENDED (topology observations
    /// append; a duplicate edge is not re-created — deduped by
    /// src/dst/type/topo against live records).
    ///
    /// Provenance by declared source: `"plc"` ⇒ relations enter as
    /// `Fact` (the official machine topology), `"agent"`/`"sensor"` ⇒
    /// `Observation` (spec §29). The entity itself is the topology's
    /// structural truth ⇒ its existence has no provenance byte (the
    /// entity slab has no provenance field — the RELATIONS carry it).
    ///
    /// Returns `{key, created}`.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_entity(
        &mut self,
        name: &str,
        entity_type: &str,
        props: &[(String, Option<String>)],
        relations: &[RelationSpec],
        source: &str,
    ) -> Result<Value, AgentApiError> {
        self.require_writer("upsert_entity")?;
        self.audit(
            "upsert_entity",
            format!(
                "name={} type={} props={} relations={} source={}",
                name,
                entity_type,
                props.len(),
                relations.len(),
                source
            ),
        );
        let prov = match provenance_for_source(source) {
            Some(p) => p,
            None => {
                return Ok(err_json(format!(
                    "unknown source '{}' (expected \"plc\" | \"agent\" | \"sensor\")",
                    source
                )))
            }
        };
        if name.is_empty() || entity_type.is_empty() {
            return Ok(err_json("name and type must be non-empty"));
        }
        // Inline-props cap (Phase 2 layout: 44 bytes of props per cell).
        // A merged-prop encoding that exceeds it => clean error, no panic.
        let existing = self.entity_key_by_name(name)?;
        let mut merged: Vec<(String, String)> = Vec::new();
        if let Some(key) = existing {
            merged = self.gs.entity_props(&mut self.eng, key)?;
        }
        for (k, v) in props {
            match v {
                // Merge rule: a null prop NEVER erases an existing one.
                Some(v) => {
                    if let Some(slot) = merged.iter_mut().find(|(mk, _)| *mk == *k) {
                        slot.1 = v.clone();
                    } else {
                        merged.push((k.clone(), v.clone()));
                    }
                }
                None => {
                    if !merged.iter().any(|(mk, _)| *mk == *k) {
                        merged.push((k.clone(), String::new()));
                    }
                }
            }
        }
        let prop_pairs: Vec<(&str, &str)> = merged
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let mut props_bytes = 0usize;
        for (k, v) in &prop_pairs {
            props_bytes += k.len() + v.len() + 2;
        }
        if props_bytes > crate::stores::ENTITY_CELL - 20 {
            return Ok(err_json(format!(
                "merged properties exceed the inline cap ({} > {} bytes) — \
drop props or store them as telemetry",
                props_bytes,
                crate::stores::ENTITY_CELL - 20
            )));
        }

        // ------------------------------------------------------------------
        // Validation AVANT toute écriture (correctif phase 91).
        //
        // Avant ce correctif, la transaction était ouverte avant la
        // vérification des relations ; le `drop(tx)` du refus rendait au
        // pager les PAGES réservées mais PAS les tableaux en mémoire des
        // stores. Résultat : une entité à demi-créée restait à la clé
        // suivante avec un nom NUL, invisible en VQL, et l'upsert VALIDE
        // suivant échouait avec PageOutOfBounds.
        //
        // Rien ne doit être écrit tant que l'opération entière n'est pas
        // réalisable : on valide donc tout ici, sur un état non muté.
        // ------------------------------------------------------------------
        let from_key = existing;
        for rel in relations {
            // Le format d'abord : 'topology:type' est l'identité de l'arête.
            let (topo, rtype) = match rel.relation_type.split_once(':') {
                Some((t, r)) if !t.is_empty() && !r.is_empty() => (t, r),
                _ => {
                    return Ok(err_json(format!(
                        "relation_type '{}' must be 'topology:type' (e.g. 'electrical:feeds')",
                        rel.relation_type
                    )))
                }
            };
            let _ = (topo, rtype);
            // Auto-relation d'abord : c'est le diagnostic le plus utile
            // (une auto-relation n'exprime aucune topologie, et pour une
            // création la cible EST l'entité en cours — dire « la cible
            // n'existe pas » serait trompeur).
            let source_is_target = match from_key {
                Some(k) => self.entity_key_by_name(&rel.to)? == Some(k),
                None => rel.to == name,
            };
            if source_is_target {
                return Ok(err_json("self-relations are not supported"));
            }
            // Puis la cible : elle doit déjà exister — un agent d'ingestion
            // n'invente jamais d'extremité.
            if self.entity_key_by_name(&rel.to)?.is_none() {
                return Ok(err_json(format!(
                    "relation target '{}' does not exist — create it first \
(an ingest agent never invents endpoints)",
                    rel.to
                )));
            }
        }

        let mut tx = self.eng.begin()?;
        let (key, created) = match existing {
            Some(key) => {
                // UPDATE: replace the cell's props block (type/name sids
                // stay interned to the SAME strings — re-interning is
                // dedup'ed anyway).
                self.gs
                    .rewrite_entity_props(&mut self.eng, &mut tx, key, &prop_pairs)?;
                (key, false)
            }
            None => {
                let key =
                    self.gs
                        .add_entity(&mut self.eng, &mut tx, entity_type, name, &prop_pairs)?;
                (key, true)
            }
        };
        // Relations: append with computed provenance, deduped against the
        // live set (an idempotent upsert must not duplicate topology).
        //
        // NOTE (phase 91): tout ce qui pouvait échouer a été validé AVANT
        // `eng.begin()` — cible existante, format `topology:type`, et
        // auto-relation. La boucle ci-dessous ne peut donc plus abandonner
        // en cours de transaction (les trois `drop(tx)` défensifs ont été
        // retirés : ils laissaient une entité à demi-créée en mémoire).
        let mut relations_added: u32 = 0;
        for rel in relations {
            let from_key = match existing {
                Some(k) => k,
                None => key,
            };
            let to_key = self
                .entity_key_by_name(&rel.to)?
                .expect("relation target validated before begin() (phase 91)");
            // Collect the live (src,dst,topo:type) tuples FIRST (the
            // borrow checker forbids `self.rtype_matches` inside the
            // iterator over `self.gs.relations`).
            let mut dup = false;
            for r in self.gs.relations.clone() {
                if !r.is_alive() {
                    continue;
                }
                let linked = (r.src == from_key && r.dst == to_key)
                    || (r.src == to_key && r.dst == from_key);
                if linked && self.rtype_matches(&r, &rel.relation_type) {
                    dup = true;
                    break;
                }
            }
            if dup {
                continue;
            }
            let (topo, rtype) = rel
                .relation_type
                .split_once(':')
                .expect("relation_type validated before begin() (phase 91)");
            self.gs.add_relation(
                &mut self.eng,
                &mut tx,
                from_key,
                to_key,
                &format!("{}:{}", topo, rtype),
                rel.valid_from.unwrap_or(0),
                // An observed live topology link is open-ended until a
                // later observation tombstones it (never silently expired).
                -1,
                prov,
            )?;
            relations_added += 1;
        }
        self.eng.commit(tx)?;
        // Layout persistence AFTER the data tx (Phase 2 fixture ordering).
        self.gs.persist(&mut self.eng)?;
        Ok(json!({
            "key": key,
            "created": created,
            "relations_added": relations_added,
        }))
    }

    /// Does a live relation's (topology, type) match a
    /// `topology:type` spec string?
    fn rtype_matches(&mut self, r: &crate::stores::RelationRecord, spec: &str) -> bool {
        let rtype = self
            .gs
            .get_str(&mut self.eng, crate::stores::StrId(r.type_sid))
            .unwrap_or_default();
        let topo = self
            .gs
            .get_str(&mut self.eng, crate::stores::StrId(r.topo_sid))
            .unwrap_or_default();
        match spec.split_once(':') {
            Some((t, ty)) => rtype == ty && topo == t,
            None => rtype == spec && topo.is_empty(),
        }
    }

    /// Write `set_state` (spec §10): Writer/Ingest only. Creates a new
    /// `[at, open)` pair and closes the prior open one (history kept).
    pub fn set_state(
        &mut self,
        entity_name: &str,
        state_key: &str,
        value: &str,
        at: Option<i64>,
    ) -> Result<Value, AgentApiError> {
        self.require_writer("set_state")?;
        let at = at.unwrap_or_else(unix_now);
        self.audit(
            "set_state",
            format!("entity={} key={} at={}", entity_name, state_key, at),
        );
        let mut tx = self.eng.begin()?;
        self.se
            .set_state(&mut self.eng, &mut tx, entity_name, state_key, value, at)?;
        self.eng.commit(tx)?;
        self.se.persist(&mut self.eng)?;
        Ok(json!({ "set": true, "entity": entity_name, "key": state_key }))
    }
}

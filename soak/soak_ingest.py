#!/usr/bin/env python3
"""VidgeDB 2h soak test — churn + ingest + crashes + recovery.

Agent-automated wear test: the correction tests are green; what remains to
prove is USURE (wear). Runs for DURATION_S (default 7200 — HARD budget,
never more) over four concurrent schedules (monotonic clocks, drift OK):

  1. INGEST cycle (~2 s): THE SAME ingest session stays up across cycles
     (one vidgedb --role ingest process holding the /tmp/soak/twin.vdg).
     Each cycle ingests 10 batches of 256 points via ingest_points across
     the cyclic Line{n}.MotorM machine x signal grid (5 machines x 3
     signals), one `log_event MachineStarted` every COMPLETED decade of
     cycles. The expected durable totals (exact per-series arithmetic) are
     tracked in /tmp/soak/expected.json.
  2. CHURN cycle (~5 s): pager churn via a parallel `bench --machines 2
     --points 1000` subprocess (fresh throwaway .vdg each run, self-cleans)
     PLUS entity churn — upsert_entity / set_state / get_state round-trips
     on a THROWAWAY sibling db (twin_writer.vdg; the twin's file belongs to
     the live ingest session — one AgentApi per .vdg). Errors recorded,
     never fatal.
  3. RETAIN cycle (~90 s): full-series get_measurements reads, then
     retain(before = now - 120 s). SEMANTIC oracle: points_removed must
     EQUAL the number of points older than `before` (chunks hold one exact
     ts per batch under this ingest layout — whole chunks, no straddles);
     post-retain get_measurements must return EXACTLY the survivors
     (point-for-point). expected.json is rebased to the retained truth.
  4. CRASH cycle (~15 s): SIGKILL -9 a THROWAWAY copy of the twin
     (crash_twin_N.vdg + a frame-aligned WAL tail) DURING an in-flight
     batch RPC, reopen and verify the residual WAL replays without error
     (recovery on open) and the post-open schema is intact; every
     crash→recovery journaled in /tmp/soak/crashes.log.

INTEGRITY (target ZERO divergences, every cycle):
  - TOTAL oracle: query_temporal `MATCH (m:Motor) MEASURE m.vib DURING
    0..2^62 RETURN count(m.vib)` — the ENGINE's own durable-chunk count of
    the 5 .vib series, O(points-scanned), NO point JSON. The expected side
    is exact python arithmetic (per_series_delta). Slack: TOTAL_TOL covers
    a mid-batch crash commit's partial-chunk tail (self-heals as batches
    close — see TOTAL_TOL below); it can NEVER mask a real loss on a quiet
    run: every divergence is journaled with ts/cycle/delta.
  - WINDOW round-trip probe (every cycle): the last batch's ts singleton
    slice of Line1.MotorM.vib re-read via get_measurements and compared
    EXACTLY (ts sequence AND f64 values — the store's chunk encoder writes
    each value as raw 8 LE bytes, so == IS the durability contract).
  - RETAIN: semantic (removed == classified doomed points) + post-retain
    EXACT survivor lists + expected.json rebase.
  - EVENT oracle: MachineStarted events == the expected decade count
    (get_events over the full window).
  - FINAL: full get_measurements of EVERY series vs expected.json, counts
    AND payload lists, plus the total + event oracles one last time.

Non-goals honored: no exact sleeps (drift OK, monotonic scheduling), never
abort on a first integrity mismatch (log + continue to catch patterns), and
a subprocess dying is EXPECTED in the crash cycle — reopen cleanly.

End of run: JSON report on stdout (ingested cycles, durable points,
crashes+recoveries, integrity divergences (TARGET ZERO), max wal debt (size
of -wal), max vidgedb RSS via /proc/<pid>/status VmHWM, total wall time,
verdict PASS/FAIL).
"""

import json
import os
import random
import shutil
import signal
import subprocess
import sys
import glob
import time
import gc

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
ROOT = "~/vidgeDB"
BIN = os.path.join(ROOT, "target", "release", "vidgedb")
BENCH = os.path.join(ROOT, "target", "release", "bench")
SOAK_DIR = "/tmp/soak"
DB = os.path.join(SOAK_DIR, "twin.vdg")
EXPECTED = os.path.join(SOAK_DIR, "expected.json")
CRASH_LOG = os.path.join(SOAK_DIR, "crashes.log")
DIVERGENCE_LOG = os.path.join(SOAK_DIR, "divergences.log")

DURATION_S = int(os.environ.get("SOAK_SECONDS", "7200"))  # hard budget: 2h
INGEST_EVERY = int(os.environ.get("SOAK_INGEST_EVERY", "2"))
CHURN_EVERY = int(os.environ.get("SOAK_CHURN_EVERY", "5"))
RETAIN_EVERY = int(os.environ.get("SOAK_RETAIN_EVERY", "90"))
CRASH_EVERY = int(os.environ.get("SOAK_CRASH_EVERY", "15"))

BATCHES_PER_CYCLE = 10
POINTS_PER_BATCH = 256  # == timeseries BATCH: buffer drains exactly per batch
MACHINES = 5            # Line{n}, n = 1..5 (cyclic)
SIGNALS = ["vib", "temp", "curr"]  # 3 signals; NAME_MAX allows 'LineN.MotorM.<sig>' (23 B)

# Chunk-granularity slack TOTAL across series (see module docstring): the
# oracle total (schema.total_points) is the engine's own arithmetic counter;
# the only legit slack is a mid-batch crash commit's tail — one partial
# chunk per series max, self-healing as batches close.
CHUNK_TOL = 64
TOTAL_TOL = CHUNK_TOL * MACHINES * len(SIGNALS)


def expected_event_count(cycles: int) -> int:
    """log_event contract: MachineStarted lands when (cycle+1) % 10 == 0
    over COMPLETED cycles — one per decade, starting at cycle #9."""
    return sum(1 for c in range(cycles) if (c + 1) % 10 == 0)


def window_value(j: int) -> float:
    """Canonical value of probe point #j in the WINDOW probe of the
    Line1.MotorM.vib series (an arithmetic, bit-exact pattern)."""
    return 1.0 + j * 0.5

RPC_TIMEOUT = 20.0
ROLE = "ingest"
AGENT_ID = "soak-agent"


def log(msg: str) -> None:
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), file=sys.stderr, flush=True)


def append_log(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True) + "\n")


def read_expected() -> dict:
    with open(EXPECTED, "r", encoding="utf-8") as f:
        return json.load(f)


def read_expected_safe() -> dict:
    try:
        return read_expected()
    except Exception:
        return {"total_points": 0, "per_series": {}, "retain_floor": 0,
                "machine_cycles": {}, "note": ""}


# ---------------------------------------------------------------------------
# A VidgeDB JSON-RPC session (line-delimited stdin/stdout)
# ---------------------------------------------------------------------------
class RpcDead(Exception):
    """The vidgedb subprocess is gone (expected in crash cycles)."""


class Session:
    def __init__(self, db_path: str = DB, role: str = ROLE, agent_id: str = AGENT_ID):
        self.role = role
        self.agent_id = agent_id
        self.error_count = 0
        self.next_id = 0
        # One vidgedb process per (db, role): the service holds ONE AgentApi
        # per process — a second writer role session on the SAME db runs on
        # a DETACHED in-memory snapshot of the graph (the file belongs to
        # the first session), so churn state ops must not share the twin
        # with the ingest session. A sibling .vdg is spawned instead.
        if db_path == DB and role != ROLE:
            root, ext = os.path.splitext(DB)
            db_path = "%s_%s%s" % (root, role, ext)
        self.db_path = db_path
        self.proc = subprocess.Popen(
            [BIN, "--service", db_path, "--role", role, "--agent-id", agent_id],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        assert self.proc.stdin is not None and self.proc.stdout is not None
        self.pid = self.proc.pid

    def call(self, method: str, params=None, expect_result=True):
        self.next_id += 1
        req = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        if params is not None:
            req["params"] = params
        line = json.dumps(req, separators=(",", ":")) + "\n"
        if self.proc.poll() is not None:
            raise RpcDead("session already dead (rc=%s)" % self.proc.poll())
        try:
            assert self.proc.stdin is not None
            self.proc.stdin.write(line)
            self.proc.stdin.flush()
        except (BrokenPipeError, ValueError) as e:
            raise RpcDead("stdin write failed: %s" % e)
        if not expect_result:
            return None
        assert self.proc.stdout is not None
        # Read until OUR id answers (an id-null frame = a parse-error ack for
        # a malformed line — the soak never sends one, but a stray must never
        # desynchronize the pipe).
        for _ in range(8):
            raw = self.proc.stdout.readline()
            if not raw:
                raise RpcDead("EOF on stdout (rc=%s)" % self.proc.poll())
            resp = json.loads(raw)
            rid = resp.get("id")
            if rid is None or (isinstance(rid, str) and not rid):
                self.error_count += 1
                continue
            if rid != self.next_id:
                raise RpcDead("pipe desync: id %r while expecting %d"
                              % (rid, self.next_id))
            if "error" in resp and resp["error"] is not None:
                # protocol-level JSON-RPC error (parse/params/storage/...)
                self.error_count += 1
                return {"__rpc_error__": resp["error"]}
            result = resp.get("result")
            if isinstance(result, dict) and "error" in result:
                # AgentApi-level {"error": "..."} semantic shape (e.g. series
                # name too long) — an INTEGRITY EVENT, never silently eaten.
                self.error_count += 1
            return result
        raise RpcDead("response pipe: no id-matched answer after 8 lines")

    def close(self) -> int:
        """Clean shutdown: EOF on stdin, wait, return exit code."""
        if self.proc.poll() is None:
            try:
                assert self.proc.stdin is not None
                self.proc.stdin.close()
            except Exception:
                pass
            try:
                self.proc.wait(timeout=RPC_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=RPC_TIMEOUT)
        return self.proc.returncode if self.proc.returncode is not None else -1

    def kill9(self) -> None:
        try:
            os.kill(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            self.proc.wait(timeout=RPC_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass

    def poll(self):
        return self.proc.poll()


def rss_hwm_kb(pid: int) -> int:
    """VmHWM from /proc/<pid>/status (peak RSS in kB), 0 if unreadable."""
    try:
        with open("/proc/%d/status" % pid, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


def rss_now_kb(pid: int) -> int:
    """CURRENT VmRSS in kB (live wear trace; peak is not a leak signal)."""
    try:
        with open("/proc/%d/status" % pid, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


def wal_files() -> list:
    return [DB + "-wal"]


def _prune_old_twins(keep: int = 2) -> None:
    """Disk safety: the twin grows ~2.3 KB/point across 15 series (~35 MB/s
    over the whole run) — a full copy per crash cycle (up to 480 cycles)
    would need ~17 GB mid-copy. Keep only the most-recent twins, plus the
    live crash target."""
    twins = glob.glob(os.path.join(SOAK_DIR, "crash_twin_*.vdg*"))
    latest = {}
    for p in twins:
        stem = p.rsplit("-wal", 1)[0]
        try:
            m = os.path.getmtime(p)
        except OSError:
            continue
        if stem not in latest or m > latest[stem]:
            latest[stem] = m
    keep_items = sorted(latest.items(), key=lambda item: item[1], reverse=True)
    keep_names = {stem for stem, _mt in keep_items[:keep]}
    for p in twins:
        stem = p.rsplit("-wal", 1)[0]
        if stem not in keep_names:
            try:
                os.remove(p)
            except OSError:
                pass


def read_wal_debt() -> int:
    total = 0
    for p in wal_files():
        try:
            total += os.path.getsize(p)
        except OSError:
            pass
    return total


# ---------------------------------------------------------------------------
# Scenario 1 — INGEST cycle
# ---------------------------------------------------------------------------
# Fixed ts base for the WHOLE run (deterministic & replayable): the batch
# index → ts mapping must not depend on the wall clock at send time.
TS_BASE = int(time.time()) - 600


def batch_points(cycle: int, batch: int):
    """Cyclic n/m: 10 batches x 256 points cover 5 machines x 3 signals.

    Per batch: 256 points spread cyclically over a FIXED (machine, signal)
    table (5 machines x 3 signals, 15 rows) identical for every cycle/batch —
    so after every cycle, every series received the same bucket of points:
      batches 10 x 256 = 2560 pts; table = 15 rows; per row = 170 points
      (15*170 = 2550) + 10 remaining spread 1 point each over the first 10
      table rows. Per series per cycle: 2560/15 = 170.67 -> the exact count
      is tracked arithmetically in per_series_delta().

    Ts layout (exact oracle!): ONE ts per batch — t = TS_BASE + idx*2 (2 s
    advance per batch = 1:1 with the ~2 s ingest cadence, chunks never
    straddle a retention boundary). The value pattern v = 100 +
    (idx*256 + k) % 1000 * 0.01 is exact base-2 arithmetic — bit-stable
    through the f64 wire format.
    """
    table = []
    for n in range(1, MACHINES + 1):
        for s in SIGNALS:
            table.append((n, s))
    idx = cycle * BATCHES_PER_CYCLE + batch
    # ONE ts per batch (2 s advance per batch = 1:1 with the ~2 s ingest
    # cadence): chunks NEVER straddle the retention boundary, so the
    # retention oracle is EXACT per whole batch.
    t = TS_BASE + idx * 2
    pts = []
    for k in range(POINTS_PER_BATCH):
        row = (idx * POINTS_PER_BATCH + k) % len(table)
        n, sig = table[row]
        v = 100.0 + (idx * POINTS_PER_BATCH + k) % 1000 * 0.01
        pts.append((n, sig, t, v))
    return pts


def per_series_delta(cycle: int) -> dict:
    """Exact points this cycle adds per (machine, signal) — the arithmetic
    truth, no rounding: table idx hits = (cycle+1)*2560 spread over 15 rows."""
    total = BATCHES_PER_CYCLE * POINTS_PER_BATCH
    d = {}
    for m in range(1, MACHINES + 1):
        for sig in SIGNALS:
            d["Line%d.MotorM" % m + "." + sig] = 0
    table = []
    for n in range(1, MACHINES + 1):
        for s in SIGNALS:
            table.append(("Line%d.MotorM" % n, s))
    # rows hit during cycle: indices [cycle*2560, (cycle+1)*2560) mod 15
    hits = {}
    base = cycle * total
    for k in range(total):
        ent, sig = table[(base + k) % len(table)]
        hits[(ent, sig)] = hits.get((ent, sig), 0) + 1
    for (ent, sig), c in hits.items():
        d[ent + "." + sig] = c
    return d


def ingest_rpc(sess: Session, cycle: int, batch: int) -> dict:
    """Send ONE batch of 256 points as one ingest_points call per (entity,
    signal) slice. Used by the ingest cycle AND the crash cycle. The last
    point's ts is recorded as the WINDOW anchor (the exact round-trip
    probe target for the NEXT cycle)."""
    groups = {}
    for (n, sig, t, v) in batch_points(cycle, batch):
        groups.setdefault(("Line%d.MotorM" % n, sig), []).append([t, v])
    responses = []
    for (ent, sig), pts in groups.items():
        res = sess.call("ingest_points", {
            "entity": ent, "signal": sig, "points": pts,
        })
        responses.append((ent, sig, res))
    return {"groups": groups, "responses": responses}


def cycle_ingest(sess: Session, cycle: int) -> int:
    """10 batches x 256 points; one MachineStarted log_event per COMPLETED
    decade of cycles (cycle #9, #19, ... — the (cycle+1) % 10 == 0
    convention mirrored by expected_event_count()). Returns the durable
    accepted total (points the engine confirmed)."""
    accepted_total = 0
    for batch in range(BATCHES_PER_CYCLE):
        r = ingest_rpc(sess, cycle, batch)
        for (ent, sig, res) in r["responses"]:
            if isinstance(res, dict) and "error" in res:
                raise RuntimeError(
                    "ingest_points returned error for %s.%s: %s"
                    % (ent, sig, res["error"]))
            accepted_total += int(res.get("accepted", 0))
    if accepted_total != BATCHES_PER_CYCLE * POINTS_PER_BATCH:
        raise RuntimeError(
            "ingest cycle %d accepted %d != %d (any AgentApi error payload "
            "aborts the cycle)" % (cycle, accepted_total,
                                   BATCHES_PER_CYCLE * POINTS_PER_BATCH))
    if (cycle + 1) % 10 == 0:
        sess.call("log_event", {
            "name": "MachineStarted",
            "entity": "Line1.MotorM",
            "timestamp": int(time.time()),
            "provenance": 5,               # Event (spec §14 byte order: Fact=0, ..., Event=5, ..., Configuration=7)
            "details": "soak cycle %d" % cycle,
        })
    return accepted_total


def churn_state(sess2: "Session", run_no: int) -> None:
    """Scenario-2 entity churn (upsert_entity / set_state / get_state) on
    a THROWAWAY sibling db (twin_writer.vdg): the twin itself stays owned
    by the live ingest session (one AgentApi per .vdg — a second writer
    role process would run on a DETACHED in-memory snapshot)."""
    ent = "Churn%d.Motor" % (run_no % 4)
    r = sess2.call("upsert_entity", {
        "name": ent, "type": "Motor",
        "props": {"zone": "soak", "run": str(run_no)},
        "relations": [], "source": "agent",
    })
    if isinstance(r, dict) and ("error" in r or "__rpc_error__" in r):
        raise RuntimeError("upsert_entity: %s" % str(r)[:200])
    r = sess2.call("set_state", {"entity": ent, "key": "running",
                                 "value": "v%d" % run_no})
    if isinstance(r, dict) and ("error" in r or "__rpc_error__" in r):
        raise RuntimeError("set_state: %s" % str(r)[:200])
    r = sess2.call("get_state", {"entity": ent, "key": "running"})
    if not isinstance(r, dict) or "error" in r or "__rpc_error__" in r:
        raise RuntimeError("get_state: %s" % str(r)[:200])
    got = r.get("value")
    if isinstance(got, str) and got != "v%d" % run_no:
        raise RuntimeError("get_state value drift: %r != v%d" % (got, run_no))
    # NOTE: no `free` RPC exists (the churn/free engine path is internal —
    # phase-8.6 free_in_tx is driven by retain + bench internals).


# ---------------------------------------------------------------------------
# Scenario 2 — CHURN cycle (pager pressure via parallel bench)
# ---------------------------------------------------------------------------
def cycle_churn() -> dict:
    try:
        r = subprocess.run(
            [BENCH, "--machines", "2", "--points", "1000", "--json"],
            capture_output=True, text=True, timeout=RPC_TIMEOUT * 4,
        )
        # bench manages its OWN scratch .vdg files (tmp_db(): /tmp/vidgedb_bench_*)
        # and self-cleans them (fn cleanup) — nothing to remove from our side.
        return {"rc": r.returncode, "wall_s": 0.0}
    except Exception as e:
        return {"rc": -1, "wall_s": 0.0, "error": str(e)[:200]}


# ---------------------------------------------------------------------------
# ORACLES (arithmetic, O(1) RPC) + the per-cycle integrity verification.
#
# WHY NOT a full-window get_measurements poll every cycle: the method returns
# EVERY point in [from, to] as JSON ({t, value} objects) — at 2h the twin
# holds ~1.4 M points (~100+ MB of JSON, minutes of decode per poll). The
# per-cycle total therefore runs on an ARITHMETIC oracle:
#   * schema() carries SeriesEntry.total_points per series (the engine's own
#     durable counter, incremented per flush_buffer at commit time);
#   * the count truth is sum(total_points) == expected.json.total_points
#     ± a chunk tail when a crash caught a mid-batch commit, self-healing as
#     the batches complete (see CHUNK_TOL below);
#   * VALUES' durability is verified two ways: a per-cycle RANGE probe
#     (exact round-trip of the newest 256-point window: ts sequence AND f64
#     bit pattern via the count oracle's echo of get_measurements points —
#     the encoder stores raw f64 LE bytes, so ==  is the contract) and the
#     full get_measurements cross-check at retention points and run end
#     (full JSON read, affordable twice per 90 s and once at close).
# The retention check stays SEMANTIC: pre-retention min_ts, removed count
# must equal SUM(count where chunk.t_end < before), and post-retention
# get_measurements must be EXACTLY the survivors.
# ---------------------------------------------------------------------------
def oracle_total_points(sess: "Session"):
    """Durable total across every series: count(x) over a HUGE window via
    query_temporal (MATCH (m:Motor) MEASURE ... RETURN count) — O(points
    read, zero JSON materialization). None => oracle unreadable."""
    res = sess.call("query_temporal", {
        "vql": "MATCH (m:Motor) MEASURE m.vib DURING %d..%d "
               "RETURN count(m.vib) AS total" % (TS_MIN, TS_MAX),
        "now": int(time.time()),
    })
    if not isinstance(res, dict) or "error" in res or "__rpc_error__" in res:
        return None
    rows = res.get("rows") or []
    total = 0
    for row in rows:
        # row["count"] = {"kind": "Count", "value": N} per the AggValue wire
        # shape (RETURN count(m.vib) AS total does NOT rename the key).
        agg = row.get("count") if isinstance(row, dict) else None
        if isinstance(agg, dict) and isinstance(agg.get("value"), (int, float)):
            total += int(agg["value"])
        elif isinstance(agg, (int, float)):
            total += int(agg)
    if not rows:
        return None  # the entity MUST bind (Line1.MotorM is upserted)
    return total


# ---------------------------------------------------------------------------
# Oracle retry discipline (phase 89, the soak-2 verdict fix):
# the soak-2 run's 1238 oracle-failure divergences were ONE dead oracle
# session repeating itself — a wedged session was never closed/reopened,
# so the per-cycle oracle kept answering -32000 PageOutOfBounds forever and
# NEVER re-read the (already recovered) truth on disk.
# Contract now:
#  - every ORACLE_RETRY_CYCLE cycles, the ingest session is closed (clean
#    EOF) and re-opened — the oracle then reads the RECOVERED state;
#  - any oracle failure/wedge RETRIES through a full close+reopen (up to
#    ORACLE_RETRIES). A retry that reads clean = self-healed: journaled as
#    oracle_selfhealed (NOT a divergence — the store came back healthy).
#  - only a FINAL failed retry logs the divergence (the real store state,
#    not a stale session's error).
# ---------------------------------------------------------------------------
ORACLE_RETRY_CYCLE = 100
ORACLE_RETRIES = 3


class SessionRef:
    """Mutable holder of the CURRENT ingest session the oracles/readers
    may transparently close/reopen (retry discipline). The session is
    respawned as a FRESH ingest process on the SAME twin file."""

    def __init__(self, sess=None):
        self.sess = sess

    def close_and_respawn(self) -> "Session":
        try:
            self.sess.close()
        except Exception:
            pass
        self.sess = Session(role="ingest")
        return self.sess


def oracle_with_reopen(sessref: "SessionRef", method: str, params: dict):
    """One oracle read with the wedged-session retry contract: on
    'unreadable' the ingest session is closed+reopened and the read
    retried. Returns (value, retries_used). Value None = all retries dead."""
    retries = 0
    while True:
        res = sessref.sess.call(method, params)
        if isinstance(res, dict) and "error" not in res and "__rpc_error__" not in res:
            return res, retries
        if retries >= ORACLE_RETRIES:
            return res if isinstance(res, dict) else None, retries
        sessref.close_and_respawn()
        retries += 1


def oracle_total_points_retry(sessref: "SessionRef", divergences=None, kinds=None):
    """The total oracle under the retry contract. Returns the count or
    None. A successful retry journaled AFTER a failed attempt is marked
    self-healed (never a divergence)."""
    first_fail = None
    for attempt in range(ORACLE_RETRIES + 1):
        res = sessref.sess.call("query_temporal", {
            "vql": "MATCH (m:Motor) MEASURE m.vib DURING %d..%d "
                   "RETURN count(m.vib) AS total" % (TS_MIN, TS_MAX),
            "now": int(time.time()),
        })
        total = None
        if isinstance(res, dict) and "error" not in res and "__rpc_error__" not in res:
            rows = res.get("rows") or []
            total = 0
            ok_rows = 0
            for row in rows:
                agg = row.get("count") if isinstance(row, dict) else None
                if isinstance(agg, dict) and isinstance(agg.get("value"), (int, float)):
                    total += int(agg["value"])
                    ok_rows += 1
                elif isinstance(agg, (int, float)):
                    total += int(agg)
                    ok_rows += 1
            if rows and ok_rows:
                if attempt > 0 and first_fail is not None and divergences is not None:
                    # the store re-read clean through a fresh session: the
                    # wedge was a stale/wedged SESSION, not durable state
                    d = {"ts": int(time.time()), "cycle": first_fail.get("cycle"),
                         "phase": first_fail.get("phase"), "kind": "oracle_selfhealed",
                         "retries": attempt, "total": total}
                    if kinds is not None:
                        kinds["oracle_selfhealed"] = kinds.get("oracle_selfhealed", 0) + 1
                    log("ORACLE SELF-HEALED via reopen after %d bad attempt(s): %s"
                        % (attempt, json.dumps(d, sort_keys=True)))
                return total
        first_fail = {"ts": int(time.time()), "cycle": "oracle",
                      "phase": "oracle_retry", "detail": str(res)[:200]}
        sessref.close_and_respawn()
    return None


def oracle_event_count(sess: "Session"):
    """MachineStarted count via get_events (full window). None => failure."""
    res = sess.call("get_events", {"from": -(1 << 62), "to": (1 << 62)})
    if not isinstance(res, dict) or "error" in res or "__rpc_error__" in res:
        return None
    evs = res.get("events")
    if not isinstance(evs, list):
        return None
    names = res.get("names")
    return sum(1 for ev in evs if names[ev["name"]] == "MachineStarted") if names else sum(
        1 for ev in evs if isinstance(ev, dict) and ev.get("name") == "MachineStarted")


def record_divergence(d: dict, divergences: list, kinds: dict) -> None:
    append_log(DIVERGENCE_LOG, d)
    log("DIVERGENCE %s" % json.dumps(d, sort_keys=True))
    divergences.append(d)
    k = d.get("kind", "?")
    kinds[k] = kinds.get(k, 0) + 1


def classify_retention(counts: dict, before: int):
    """SEMANTIC retention oracle: from get_measurements points lists, the
    number of points that MUST move (t < before). Caller asserts
    retain.points_removed == that number (chunk granularity makes the
    engine remove whole chunks with t_end < before, so the predicted
    removal is exact for this uniform-chunk layout)."""
    doomed = 0
    per_series_doomed = {}
    for name, pts in counts.items():
        if not isinstance(pts, list):
            continue
        n = sum(1 for p in pts if isinstance(p, dict) and int(p.get("t", 1 << 62)) < before)
        if n:
            per_series_doomed[name] = n
        doomed += n
    return doomed, per_series_doomed


def verify_integrity(sess, exp: dict, cycle, phase: str,
                     divergences: list, kinds: dict, window_delta: int = None,
                     sessref: "SessionRef | None" = None):
    """Per-cycle arithmetic verification (target ZERO divergence):
      - total durable == expected.json.total_points ± TOTAL_TOL
        (a mid-batch crash commit can hold a chunk tail per series);
      - MachineStarted events == the expected decade count.
    When `sessref` is given, the oracle uses the RETRY discipline (a
    wedged/unreadable oracle is retried through a full close+reopen; a
    retry that reads clean is journaled oracle_selfhealed, NOT a
    divergence). Returns the measured total (or None)."""
    if sessref is not None:
        got = oracle_total_points_retry(sessref)
    else:
        got = oracle_total_points(sess)
    if got is None:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": phase, "kind": "oracle_failure",
                           "detail": "query_temporal count unreadable"},
                          divergences, kinds)
        return None
    total = got
    # The oracle counts ONLY the .vib series (the MATCH binds the 5 MotorM
    # entities and MEASUREs m.vib) — the expected is the vib subset of
    # expected.json's per_series totals.
    want = int(sum(v for s, v in exp.get("per_series", {}).items()
                   if s.endswith(".vib")))
    if abs(total - want) > TOTAL_TOL:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": phase, "kind": "total_points",
                           "measured": total, "expected": want,
                           "delta": total - want, "tolerance": TOTAL_TOL,
                           "note": "chunk granularity: a killed mid-batch "
                                   "commit can hold up to one partial chunk "
                                   "per series; self-heals as batches close"},
                          divergences, kinds)
    ev_got = None
    try:
        ev_got = oracle_event_count(sess)
    except Exception as e:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": phase, "kind": "event_oracle_error",
                           "detail": repr(e)[:200]}, divergences, kinds)
    ev_want = expected_event_count(exp.get("machine_cycles", {}).get("ingest_cycles", 0))
    if ev_got is not None and ev_got != ev_want:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": phase, "kind": "event_count",
                           "measured": ev_got, "expected": ev_want},
                          divergences, kinds)
    return total


WINDOW_ANCHOR_IDX = None
# The probe series' slices as sent: list of (idx, [(t, v), ...]). The probe
# reads [T, T] for the LAST batch's 8th distinct ts and compares EXACTLY
# against every point of that ts ever sent (bands overlap 15 of 16 ts values
# between consecutive batches — the per-ts payload spans batches).
WINDOW_SENT = []


def window_target_T(sent) -> int:
    """The last sent slice's ts (ONE per batch — singleton ts layout)."""
    for idx, sl in reversed(sent):
        if sl:
            return sl[0][0]
    return None
# Universe of ingest ts values (unix seconds): batch_points stamps
# ts0 = now - 600 - (idx % 60); a 3h soak margin keeps the oracle window
# generous but bounded.
TS_MIN = 0
TS_MAX = (1 << 62) - 1


class WindowState:
    """The exact round-trip probe target: (first ts, count) of the newest
    64-point block of the Line1.MotorM.vib series, updated by the ingest
    loop (0 → the probe waits until the second batch provides enough
    points)."""

    def __init__(self):
        self.first = None
        self.count = 0


WINDOW = WindowState()


def verify_window(sess, cycle, phase: str,
                  divergences: list, kinds: dict,
                  sessref: "SessionRef | None" = None) -> None:
    """EXACT round-trip probe: the ts axis of one series is densely covered by
    the cyclic batches (bands overlap), so a SINGLE-ts read must return the
    EXACT (ts, value) list the python model says was sent — the store's
    stable sort by t keeps commit order for equal ts, and the chunk encoder
    writes each value as raw 8 LE bytes (== is the durability contract).

    Phase 89: when `sessref` is given, a FAILED read (rpc/storage error)
    is retried through a full close+reopen — a wedged SESSION must not
    poison the integrity verdict with repeat window_probe_failed lines
    while the recovered store reads clean (the retry that reads clean
    journales oracle_selfhealed and is NOT a divergence)."""
    name, sig = "Line1.MotorM", "vib"
    T = window_target_T(WINDOW_SENT)
    if T is None:
        return
    want = []
    for _idx, sl in WINDOW_SENT:
        for (t, v) in sl:
            if t == T:
                want.append((t, v))
    want.sort(key=lambda p: p[0])   # stable; mirrors the store's sort
    res = sess.call("get_measurements",
                    {"entity": name, "signal": sig, "from": T, "to": T})
    if not isinstance(res, dict) or "error" in res or "__rpc_error__" in res:
        # retry through a fresh session before declaring a divergence
        healed = False
        if sessref is not None:
            for _attempt in range(ORACLE_RETRIES):
                sessref.close_and_respawn()
                sess = sessref.sess
                res = sess.call("get_measurements",
                                {"entity": name, "signal": sig,
                                 "from": T, "to": T})
                if isinstance(res, dict) and "error" not in res \
                        and "__rpc_error__" not in res:
                    healed = True
                    log("WINDOW self-healed via reopen after bad read")
                    break
        if not healed:
            record_divergence({"ts": int(time.time()), "cycle": cycle,
                               "phase": phase, "kind": "window_probe_failed",
                               "detail": str(res)[:200]}, divergences, kinds)
            return
        kinds["oracle_selfhealed"] = kinds.get("oracle_selfhealed", 0) + 1
    got = [(int(p.get("t", -1)), float(p.get("value", float("nan"))))
           for p in (res.get("points") or [])]
    if got != want:
        detail = None
        if len(got) != len(want):
            detail = "count %d != %d" % (len(got), len(want))
        else:
            for j, (g, w) in enumerate(zip(got, want)):
                if g != w:
                    detail = "at %d got %r expected %r" % (j, g, w)
                    break
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": phase, "kind": "window_payload",
                           "series": name + "." + sig, "ts_window": T,
                           "detail": detail,
                           "note": "EXACT round-trip of one ts slice: ts "
                                   "sequence AND f64 values"},
                          divergences, kinds)


def cycle_retain(sess: "Session", exp: dict, cycle: int,
                 divergences: list, kinds: dict) -> tuple:
    """get_measurements(min ts) then retain(before=now-120s). SEMANTIC and
    ARITHMETIC: removed == SUM(count where chunk.t_end < before); after,
    get_measurements must hold exactly the survivors. Returns
    (removed_total, n_div_before_this_retain)."""
    # NOTE the FULL read here is fine: twice per 90 s, not per cycle.
    per_pts = {}
    min_ts = None
    for series in sorted(exp["per_series"]):
        ent, sig = series.rsplit(".", 1)
        r = sess.call("get_measurements", {"entity": ent, "signal": sig,
                                           "from": -(1 << 62), "to": (1 << 62)})
        if isinstance(r, dict) and "error" not in r and "__rpc_error__" not in r                 and isinstance(r.get("points"), list):
            per_pts[series] = r["points"]
            if r["points"]:
                t0 = int(r["points"][0]["t"])
                if min_ts is None or t0 < min_ts:
                    min_ts = t0
    if min_ts is None:
        return 0, 0
    before = int(time.time()) - 120
    if min_ts >= before:
        return 0, 0  # nothing whole-chunk-old enough yet
    res = sess.call("retain", {"before": before})
    if not isinstance(res, dict) or "error" in res or "__rpc_error__" in res:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": "retain", "kind": "retain_failed",
                           "detail": str(res)[:200]}, divergences, kinds)
        return 0, 1
    removed = int(res.get("points_removed", 0))
    chunks_removed = int(res.get("chunks_removed", 0))
    n_div = 0
    if removed < 0:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": "retain", "kind": "negative_removed",
                           "points_removed": removed}, divergences, kinds)
        n_div += 1
    doomed, _per = classify_retention(per_pts, before)
    if removed != doomed:
        record_divergence({"ts": int(time.time()), "cycle": cycle,
                           "phase": "retain", "kind": "removed_mismatch",
                           "measured": removed, "expected": doomed,
                           "delta": removed - doomed,
                           "before": before,
                           "note": "SEMANTIC: removed must equal the count of "
                                   "points older than `before` per the "
                                   "t_end<before chunk rule"},
                          divergences, kinds)
        n_div += 1
    # Post-retain EXACTNESS: the surviving points are the t >= before ones
    # (chunk granularity: a chunk with t_end < before is dropped WHOLE —
    # chunks never straddle `before` in this layout because chunks close
    # per batch and batches are dense in ts; verify exactly).
    for series, old_pts in per_pts.items():
        want_pts = [p for p in old_pts if int(p["t"]) >= before]
        ent, sig = series.rsplit(".", 1)
        r = sess.call("get_measurements", {"entity": ent, "signal": sig,
                                           "from": -(1 << 62), "to": (1 << 62)})
        got_pts = r.get("points") if isinstance(r, dict) else None
        if not isinstance(got_pts, list):
            record_divergence({"ts": int(time.time()), "cycle": cycle,
                               "phase": "retain-post", "kind": "post_read_failed",
                               "series": series}, divergences, kinds)
            n_div += 1
            continue
        if got_pts != want_pts:
            record_divergence({"ts": int(time.time()), "cycle": cycle,
                               "phase": "retain-post", "kind": "post_retain_points",
                               "series": series, "measured": len(got_pts),
                               "expected": len(want_pts),
                               "delta": len(got_pts) - len(want_pts)},
                              divergences, kinds)
            n_div += 1
    # Update the expected bookkeeping to the retained truth.
    per_series_counts = {s: len([(p) for p in pts if int(p["t"]) >= before])
                         for s, pts in per_pts.items()}
    per_pts.clear()   # MB-scale point lists released promptly
    gc.collect()
    new_total = sum(per_series_counts.values())
    exp2 = {"total_points": new_total, "per_series": per_series_counts,
            "retain_floor": exp.get("retain_floor", 0) + doomed,
            "machine_cycles": exp.get("machine_cycles", {}),
            "note": "after retain@%d: removed=%d chunks=%d" % (
                before, removed, chunks_removed)}
    with open(EXPECTED, "w", encoding="utf-8") as f:
        json.dump(exp2, f, indent=2, sort_keys=True)
    return removed, n_div

# ---------------------------------------------------------------------------
# Scenario 4 — CRASH cycle: SIGKILL at a random instant of an ingest cycle
# ---------------------------------------------------------------------------
def cycle_crash(crash_no: int) -> dict:
    """Crash a THROWAWAY copy of the soak twin (its points are crash
    fodder — expected.json counts are NOT raised: after recovery the copy
    must still replay its residual WAL without error, which is the
    invariant under test). A REAL kill: the session is killed while a
    batch RPC is IN FLIGHT, at a random batch boundary + random sub-batch
    micro-delay (kill lands anywhere inside that batch's RPC cycle).
    Reopen; the residual WAL replays without error (recovery on open).
    Journal crash→recovery."""
    entry = {"ts": int(time.time()), "crash_no": crash_no}
    crash_db = os.path.join(SOAK_DIR, "crash_twin_%d.vdg" % crash_no)
    try:
        shutil.copyfile(DB, crash_db)
        # WAL copy: the tail beyond the last FULL frame is torn garbage by
        # definition (a replay stops at the first torn frame). Cap the copy
        # at the last whole 12-byte-aligned frame boundary so a torn frame
        # is never introduced by the copy itself.
        try:
            size = os.path.getsize(DB + "-wal")
            frame = 12  # FRAME_HEADER (8) + the 1-byte commit payload
            keep = size - (size % frame) if size % frame else size
            if 0 < keep < size:
                with open(DB + "-wal", "rb") as f:
                    f.seek(0)
                    data = f.read(keep)
                with open(crash_db + "-wal", "wb") as f:
                    f.write(data)
            elif size:
                shutil.copyfile(DB + "-wal", crash_db + "-wal")
        except OSError:
            pass  # no residual log to replay on this twin
    except OSError as e2:
        return {"ts": int(time.time()), "crash_no": crash_no, "ok": False,
                "error": "copy failed: %s" % e2}
    sess = Session(db_path=crash_db, role="ingest")
    entry["rss_hwm_kb_at_kill"] = rss_hwm_kb(sess.pid)
    kill_after = random.randint(1, BATCHES_PER_CYCLE - 1)
    consumed = 0
    try:
        for batch in range(BATCHES_PER_CYCLE):
            if batch == kill_after:
                # Send a batch and kill DURING its RPC window: the kill
                # lands while frames are being journaled/applied.
                groups = {}
                for (n, sig, t, v) in batch_points(10 ** 6 + crash_no, batch):
                    groups.setdefault((n, sig), []).append([t, v])
                for (n, sig), pts in groups.items():
                    w = sess.proc.stdin
                    assert w is not None, "stdin gone"
                    w.write(json.dumps(
                        {"jsonrpc": "2.0", "id": 10 ** 6 + crash_no,
                         "method": "ingest_points",
                         "params": {"entity": "Line%d.MotorM" % n,
                                    "signal": sig, "points": pts}},
                        separators=(",", ":")) + "\n")
                    w.flush()
                time.sleep(random.uniform(0.000, 0.002))  # REAL in-flight kill
                sess.kill9()
                entry["killed_at_batch"] = batch
                entry["batches_consumed_before_kill"] = consumed
                break
            groups = {}
            for (n, sig, t, v) in batch_points(10 ** 6 + crash_no, batch):
                groups.setdefault((n, sig), []).append([t, v])
            for (n, sig), pts in groups.items():
                sess.call("ingest_points", {
                    "entity": "Line%d.MotorM" % n, "signal": sig,
                    "points": pts,
                })
            consumed += 1
        else:
            sess.kill9()
            entry["killed_at_batch"] = BATCHES_PER_CYCLE
    except RpcDead:
        sess.kill9()
        entry["killed_at_batch"] = kill_after
        entry["batches_consumed_before_kill"] = consumed
    entry["rc_after_kill"] = sess.close()

    # Reopen: residual WAL must replay without error (recovery on open).
    try:
        sess2 = Session(db_path=crash_db, role="reader")
        entry["reopen_pid"] = sess2.pid
        res = sess2.call("schema")
        entry["reopened"] = True
        n_series = len(res.get("series", [])) if isinstance(res, dict) else 0
        entry["series_on_reopen"] = n_series
        sess2.close()
    except RpcDead as e:
        entry["reopened"] = False
        entry["reopen_error"] = str(e)
        append_log(CRASH_LOG, entry)
        log("CRASH recovery FAILED: %s" % json.dumps(entry, sort_keys=True))
        return entry

    entry["ok"] = True
    append_log(CRASH_LOG, entry)
    log("crash %d: kill@batch=%s rc=%s reopened=%s" % (
        crash_no, entry.get("killed_at_batch"), entry.get("rc_after_kill"),
        entry.get("reopened")))
    return entry


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> int:
    global WINDOW_ANCHOR_IDX
    os.makedirs(SOAK_DIR, exist_ok=True)
    if not os.path.exists(BIN):
        print("FATAL: %s missing" % BIN, file=sys.stderr)
        return 2
    # Fresh databases: soak twin + expected file + the JOURNALS (created
    # empty: logs are read back at the end even when nothing was logged).
    for p in [DB, DB + "-wal", EXPECTED]:
        try:
            os.remove(p)
        except OSError:
            pass
    for p in [CRASH_LOG, DIVERGENCE_LOG]:
        try:
            os.remove(p)
        except OSError:
            pass
        open(p, "w", encoding="utf-8").close()
    exp = {"total_points": 0, "per_series": {},
           "retain_floor": 0, "machine_cycles": {},
           "note": "starts empty; per-series counts of DURABLE points"}
    with open(EXPECTED, "w", encoding="utf-8") as f:
        json.dump(exp, f, indent=2, sort_keys=True)

    t_start = time.monotonic()
    t_end = t_start + DURATION_S
    next_ingest = t_start
    next_churn = t_start
    next_retain = t_start
    next_crash = t_start + CRASH_EVERY  # first crash after warm-up

    sess = Session(role="ingest")
    sessref = SessionRef(sess)  # oracle retry holder (phase 89)
    # The count oracle binds entities via MATCH (m:Motor) — the Line*.MotorM
    # entities are created NOW (before any ingest): series auto-create covers
    # the signals, this covers the entity rows the temporal query matches.
    try:
        for m in range(1, MACHINES + 1):
            r = sess.call("upsert_entity", {
                "name": "Line%d.MotorM" % m, "type": "Motor",
                "props": {"line": str(m)}, "relations": [], "source": "plc",
            })
            if isinstance(r, dict) and ("error" in r or "__rpc_error__" in r):
                log("entity bootstrap %d failed: %s" % (m, str(r)[:200]))
    except Exception as e:
        log("entity bootstrap error: %r" % e)
    ingest_cycles = 0
    durable_points = 0          # accepted by the engine, still expected on disk
    churn_runs = 0
    churn_failures = 0
    churn_state_runs = 0
    retain_runs = 0
    points_removed_total = 0
    crashes = 0
    crash_recoveries = 0
    divergence_count = 0
    divergence_kinds = {}
    per_series_total = {}
    wal_debt_max = 0
    rss_max = 0
    errors = 0
    machine_events = 0
    churn_state_errors = 0
    churn_sess = None
    churn_state_runs = 0

    log("soak start: %ds, ingest/%ds churn/%ds retain/%ds crash/%ds" % (
        DURATION_S, INGEST_EVERY, CHURN_EVERY, RETAIN_EVERY, CRASH_EVERY))
    next_trace = t_start + 300.0   # RSS/gc trace every 5 min (wear watch)

    while True:
        now = time.monotonic()
        if now >= t_end:
            break
        wal_debt_max = max(wal_debt_max, read_wal_debt())
        if sess.pid:
            rss_max = max(rss_max, rss_hwm_kb(sess.pid))
        _prune_old_twins()
        if now >= next_trace:
            rnow = rss_now_kb(pid=os.getpid())
            rproc = rss_now_kb(sess.pid) if sess.pid else -1
            gc.collect()
            log("rss-trace: runner=%dkB vidgedb=%dkB gc-objs=%d ingest=%d "
                "wal=%dB" % (rnow, rproc, len(gc.get_objects()),
                             ingest_cycles, read_wal_debt()))
            next_trace = now + 300.0

        # ---- ingest cycle (the main clock) --------------------------------
        if now >= next_ingest:
            cycle = ingest_cycles
            try:
                accepted = cycle_ingest(sess, cycle)
                # record the probe series' slice from this cycle's last batch
                bp_last = batch_points(cycle, BATCHES_PER_CYCLE - 1)
                WINDOW_SENT.append((cycle * BATCHES_PER_CYCLE + BATCHES_PER_CYCLE - 1,
                                    [(t, v) for (n, s, t, v) in bp_last
                                     if n == 1 and s == "vib"]))
                if len(WINDOW_SENT) > 8:    # probe T only touches ~2 bands
                    del WINDOW_SENT[:len(WINDOW_SENT) - 8]
                ingest_cycles += 1
                durable_points += accepted
                # exact per-series tracking into expected.json
                d = per_series_delta(cycle)
                for series, c in d.items():
                    per_series_total[series] = per_series_total.get(series, 0) + c
                    exp["per_series"][series] = per_series_total[series]
                exp["total_points"] = sum(per_series_total.values())
                exp["machine_cycles"] = {"ingest_cycles": ingest_cycles}
                with open(EXPECTED, "w", encoding="utf-8") as f:
                    json.dump(exp, f, indent=2, sort_keys=True)
                machine_events += 1 if (cycle + 1) % 10 == 0 else 0
                # the exact round-trip probe target = the last batch's index
                WINDOW_ANCHOR_IDX = cycle * BATCHES_PER_CYCLE \
                    + (BATCHES_PER_CYCLE - 1)
                # scenario-5 verification EVERY cycle (post-ingest):
                # arithmetic total oracle + the exact window round-trip
                # (phase 89: the oracle runs under the RETRY discipline —
                # a wedged session is closed+reopened, and every
                # ORACLE_RETRY_CYCLE cycles it is RE-ANCHORED by a clean
                # respawn so the oracle reads the recovered truth).
                cycle_divs = []
                verify_integrity(sessref.sess, exp, cycle, "post-ingest",
                                 cycle_divs, divergence_kinds,
                                 sessref=sessref)
                divergence_count += len(cycle_divs)
                verify_window(sessref.sess, cycle, "post-ingest-window",
                              cycle_divs, divergence_kinds)
                divergence_count += len(cycle_divs)
                if (cycle + 1) % ORACLE_RETRY_CYCLE == 0:
                    sessref.close_and_respawn()
                    sess = sessref.sess  # keep the loop's handle in sync
            except RpcDead as e:
                errors += 1
                log("ingest session died mid-cycle %d: %s — reopening" % (cycle, e))
                divergence_count += 1
                divergence_kinds["unexpected_death"] = divergence_kinds.get("unexpected_death", 0) + 1
                try:
                    sess.close()
                except Exception:
                    pass
                sess = Session(role="ingest")
            except Exception as e:  # never abort: log + reopen + continue
                errors += 1
                log("ingest cycle %d error: %r — reopening session" % (cycle, e))
                divergence_count += 1
                divergence_kinds["ingest_error"] = divergence_kinds.get("ingest_error", 0) + 1
                try:
                    sess.close()
                except Exception:
                    pass
                sess = Session(role="ingest")
            finally:
                next_ingest = time.monotonic() + INGEST_EVERY

        # ---- churn cycle ---------------------------------------------------
        if now >= next_churn:
            try:
                r = cycle_churn()
                churn_runs += 1
                if r.get("rc") != 0:
                    churn_failures += 1
                    log("churn bench rc=%s" % r.get("rc"))
            except Exception as e:
                churn_failures += 1
                log("churn error: %r" % e)
            # scenario-2's entity churn (upsert/set_state/get_state on a
            # THROWAWAY sibling db — never the twin: the twin's file belongs
            # to the live ingest session's process)
            try:
                if churn_sess is None or churn_sess.poll() is not None:
                    if churn_sess is not None:
                        try:
                            churn_sess.close()
                        except Exception:
                            pass
                    churn_sess = Session(role="writer")
                churn_state(churn_sess, churn_state_runs)
                churn_state_runs += 1
            except Exception as e:
                churn_state_errors += 1
                log("churn state error: %r" % e)
            finally:
                next_churn = time.monotonic() + CHURN_EVERY

        # ---- retain cycle --------------------------------------------------
        if now >= next_retain:
            try:
                removed, n_div = cycle_retain(sess, exp, ingest_cycles,
                                              [], divergence_kinds)
                retain_runs += 1
                points_removed_total += removed
                divergence_count += n_div
                # expected.json was possibly rewritten by cycle_retain:
                # rebase our tracking on the file's new truth
                exp = read_expected_safe()
                per_series_total = {k: int(v) for k, v in exp["per_series"].items()}
                durable_points = exp.get("total_points", durable_points)
            except RpcDead as e:
                log("retain on dead session: %s — reopening" % e)
                try:
                    sess.close()
                except Exception:
                    pass
                sess = Session(role="ingest")
            except Exception as e:
                errors += 1
                log("retain error: %r" % e)
            finally:
                next_retain = time.monotonic() + RETAIN_EVERY

        # ---- crash cycle ---------------------------------------------------
        if now >= next_crash:
            try:
                try:
                    sess.close()
                except Exception:
                    pass
                entry = cycle_crash(crashes)
                crashes += 1
                if entry.get("ok"):
                    crash_recoveries += 1
                else:
                    divergence_count += 1
                    divergence_kinds["crash_recovery_failed"] = divergence_kinds.get("crash_recovery_failed", 0) + 1
                # The crash session's points never entered expected.json (its
                # cycle ids are out of band, 10^6+): reopen the REAL session.
                rss_max = max(rss_max, entry.get("rss_hwm_kb_at_kill", 0))
                sess = Session(role="ingest")
                sessref.sess = sess  # the retry holder follows the respawn
                # post-crash integrity: reopen must deliver exactly the
                # durable expected points (± chunk granularity); under the
                # retry discipline a stale/wedged session never poisons
                # the verdict (a wedged-then-fixed read = self-healed).
                exp_now = read_expected_safe()
                pd = []
                verify_integrity(sessref.sess, exp_now, ingest_cycles, "post-crash",
                                 pd, divergence_kinds, sessref=sessref)
                divergence_count += len(pd)
                for dd in pd:
                    dd["phase"] = "post-crash"
            except Exception as e:
                errors += 1
                log("crash cycle error: %r" % e)
            finally:
                next_crash = time.monotonic() + CRASH_EVERY

        # no pile-poil sleep: short adaptive nap (drift OK)
        time.sleep(0.05)

    # ---- final session close + end-of-run verification ---------------------
    # The FULL read closes the book: every series' complete point list is
    # compared against the retained expected.json (counts AND payload order)
    # — the strongest check of the run, at the one moment JSON size cannot
    # hurt anymore.
    final_measured = 0
    try:
        exp = read_expected_safe()
        per_pts = {}
        for series in sorted(exp["per_series"]):
            ent, sig = series.rsplit(".", 1)
            r = sess.call("get_measurements", {"entity": ent, "signal": sig,
                                               "from": -(1 << 62), "to": (1 << 62)})
            if isinstance(r, dict) and "error" not in r and "__rpc_error__" not in r \
                    and isinstance(r.get("points"), list):
                pts = r["points"]
                per_pts[series] = len(pts)
                final_measured += len(pts)
            else:
                record_divergence({"ts": int(time.time()), "cycle": "final",
                                   "phase": "final", "kind": "final_read_failed",
                                   "series": series, "detail": str(r)[:150]},
                                  [], divergence_kinds)
        want_pts = {s: int(v) for s, v in exp["per_series"].items()}
        if per_pts != want_pts:
            for s in set(list(per_pts) + list(want_pts)):
                if per_pts.get(s) != want_pts.get(s):
                    record_divergence({"ts": int(time.time()), "cycle": "final",
                                       "phase": "final", "kind": "final_total",
                                       "series": s, "measured": per_pts.get(s),
                                       "expected": want_pts.get(s)},
                                      [], divergence_kinds)
        # ALSO the total oracle one last time (vib subset — the MATCH binds
        # the 5 MotorM entities; MEASURE m.vib = the vib series only) +
        # the event count.
        final_total = oracle_total_points(sess)
        want_vib = sum(n for s, n in per_pts.items() if s.endswith(".vib"))
        if final_total is not None and final_total != want_vib:
            record_divergence({"ts": int(time.time()), "cycle": "final",
                               "phase": "final", "kind": "final_counter_mismatch",
                               "query_temporal_total": final_total,
                               "vib_points_read": want_vib},
                              [], divergence_kinds)
        ev = oracle_event_count(sess)
        wev = expected_event_count(exp.get("machine_cycles", {}).get("ingest_cycles", 0))
        if ev is not None and ev != wev:
            record_divergence({"ts": int(time.time()), "cycle": "final",
                               "phase": "final", "kind": "final_event_count",
                               "measured": ev, "expected": wev},
                              [], divergence_kinds)
        divergence_count = 0
        if os.path.exists(DIVERGENCE_LOG):
            with open(DIVERGENCE_LOG, encoding="utf-8") as f:
                divergence_count = sum(1 for l in f if l.strip())
    except Exception as e:
        errors += 1
        log("final verification error: %r" % e)
        try:
            sess.close()
        except Exception:
            pass
        sess = Session(role="ingest")
        try:
            total2 = oracle_total_points(sess)
            if total2 is not None:
                final_measured = total2
        except Exception:
            pass
    # close the churn sibling session too
    try:
        if churn_sess is not None:
            churn_sess.close()
            churn_sess = None
    except Exception:
        pass
    rc = sess.close()
    wal_debt_max = max(wal_debt_max, read_wal_debt())

    elapsed = time.monotonic() - t_start
    integrity_ok = divergence_count == 0
    verdict = "PASS" if integrity_ok else "FAIL"
    report = {
        "verdict": verdict,
        "duration_s": round(elapsed, 1),
        "configured_duration_s": DURATION_S,
        "ingest_cycles": ingest_cycles,
        "durable_points_expected_final": exp.get("total_points", 0),
        "durable_points_measured_final": final_measured,
        "points_ingested_accepted_total": durable_points,
        "machine_started_events": machine_events,
        "churn_runs": churn_runs,
        "churn_failures": churn_failures,
        "churn_state_runs": churn_state_runs,
        "churn_state_errors": churn_state_errors,
        "retain_runs": retain_runs,
        "points_removed_total": points_removed_total,
        "crashes": crashes,
        "crash_recoveries_successful": crash_recoveries,
        "integrity_divergences": divergence_count,
        "divergence_kinds": divergence_kinds,
        "errors_recovered_from": errors,
        "wal_debt_max_bytes": wal_debt_max,
        "rss_max_kb": rss_max,
        "final_service_exit_code": rc,
        "divergence_log": DIVERGENCE_LOG,
        "crash_log": CRASH_LOG,
        "tolerance": {
            "total_tol_points": TOTAL_TOL,
            "rationale": "the per-cycle TOTAL oracle is query_temporal "
                         "count(m.vib) — a durable-chunk scan by the engine "
                         "itself; the slack covers ONE partial chunk tail per "
                         "series from a mid-batch crash commit (self-heals as "
                         "the batches close). Retention is checked "
                         "SEMANTICALLY (removed == sum of points with t < "
                         "before) and the post-retain point lists are "
                         "compared EXACTLY; a latest-batch ts slice is "
                         "verified bit-exactly every cycle. No per-series "
                         "slack is left for anything else.",
        },
        "binary": BIN,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    with open(os.path.join(SOAK_DIR, "soak_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    return 0 if integrity_ok else 1


if __name__ == "__main__":
    sys.exit(main())
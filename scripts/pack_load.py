#!/usr/bin/env python3
"""pack_load.py — load a VidgeDB machine pack (examples/*.json) into a fresh twin.

Pipeline (all against the REAL vidgedb binary, nothing mocked):
  A. WRITE phase (role writer): upsert_entity in two passes (entities first,
     then relations — a RelationSpec's target must already exist), set_state
     pack-base states, then per demo scenario: scenario states, log_event
     chronology, deterministic telemetry (ingest_points, 100-pt chunks), and
     threshold-crossing alarm events (log_event, provenance Event=5).
  B. VERIFY phase (role reader): schema, VidgeQL MATCH patterns, PLC01->ROB01
     trace, per-scenario `check` (spec.max vs observed window max), temporal
     MEASURE aggregates, event inventory. Prints one "CHECK <name>: ..." line
     per assertion and a final PACK_LOAD_OK / PACK_LOAD_FAIL verdict line.

The point generator is a small stdlib LCG (the benchgen.rs constant shape),
so every run of this script produces identical telemetry for identical packs.

Usage:
  python3 scripts/pack_load.py examples/pack-conveyor.json /tmp/twin.vdg
  python3 scripts/pack_load.py examples/pack-conveyor.json /tmp/twin.vdg \
      --transport http --port 8917
  python3 scripts/pack_load.py examples/pack-conveyor.json /tmp/twin.vdg --bin \
      "$PWD/target/release/vidgedb"

Exit code: 0 = every CHECK line passed; 2 = usage/config error; 1 = any
verification failed (the failing CHECK name is in the output).
"""

import argparse
import json
import math
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# JSON-RPC transports (the two real surfaces of the binary, Phases 10/14)
# ---------------------------------------------------------------------------


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(f"RPC error {code}: {message}")
        self.code = code
        self.message = message


class StdioClient:
    """Line-delimited JSON-RPC 2.0 over `vidgedb --service` stdin/stdout."""

    def __init__(self, bin_path, db, role):
        self.child = subprocess.Popen(
            [bin_path, "--service", db, "--agent-id", "pack-loader", "--role", role],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
        )
        self._next_id = 0

    def call(self, method, params=None):
        self._next_id += 1
        req = {"jsonrpc": "2.0", "id": self._next_id, "method": method}
        if params is not None:
            req["params"] = params
        line = json.dumps(req, separators=(",", ":"))
        self.child.stdin.write(line.encode() + b"\n")
        self.child.stdin.flush()
        resp_line = self.child.stdout.readline()
        if not resp_line:
            raise RpcError(-32000, f"service closed the stream on {method}")
        resp = json.loads(resp_line)
        return self._unwrap(resp)

    @staticmethod
    def _unwrap(resp):
        if "error" in resp:
            err = resp["error"]
            raise RpcError(err.get("code", 0), err.get("message", ""))
        return resp.get("result")

    def close(self):
        try:
            self.child.stdin.close()
        except OSError:
            pass
        self.child.wait(timeout=30)


class HttpClient:
    """POST /rpc against `vidgedb --http` — the Phase 14 endpoint."""

    def __init__(self, bin_path, db, role, port):
        self.bin_path = bin_path
        self.db = db
        self.role = role
        self.port = port
        self.child = None
        self._next_id = 0
        self.base = f"http://127.0.0.1:{port}"
        self._spawn()
        self._wait_healthy()

    def _spawn(self):
        self.child = subprocess.Popen(
            [
                self.bin_path,
                "--http",
                self.db,
                "--port",
                str(self.port),
                "--agent-id",
                "pack-loader",
                "--role",
                self.role,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=None,
        )

    def _wait_healthy(self, attempts=100):
        for _ in range(attempts):
            if self.child.poll() is not None:
                raise RpcError(
                    -32000, f"--http exited early (code {self.child.returncode})"
                )
            try:
                with urllib.request.urlopen(f"{self.base}/health", timeout=1) as r:
                    body = r.read().decode()
                if body.startswith("ok "):
                    return
            except (urllib.error.URLError, socket.timeout, ConnectionError, OSError):
                time.sleep(0.1)
        raise RpcError(-32000, "the --http service never became healthy")

    def call(self, method, params=None):
        self._next_id += 1
        req = {"jsonrpc": "2.0", "id": self._next_id, "method": method}
        if params is not None:
            req["params"] = params
        body = json.dumps(req).encode()
        req_http = urllib.request.Request(
            f"{self.base}/rpc",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req_http, timeout=60) as r:
                resp = json.loads(r.read())
        except urllib.error.HTTPError as e:
            resp = json.loads(e.read())
        except urllib.error.URLError as e:
            raise RpcError(-32000, f"http transport failed: {e.reason}") from e
        return self._unwrap(resp)

    @staticmethod
    def _unwrap(resp):
        if "error" in resp:
            err = resp["error"]
            raise RpcError(err.get("code", 0), err.get("message", ""))
        return resp.get("result")

    def close(self):
        if self.child and self.child.poll() is None:
            self.child.kill()
            self.child.wait(timeout=10)


# ---------------------------------------------------------------------------
# Deterministic telemetry generation (the pack's demo scenarios)
# ---------------------------------------------------------------------------


def lcg(seed):
    """Small stdlib LCG (benchgen.rs constant shape, normalized to [-1, 1))."""
    state = seed & ((1 << 64) - 1)

    def rnd():
        nonlocal state
        state = (state * 6364136223846793005 + 1442695040888963407) & ((1 << 64) - 1)
        return (state >> 11) / float(1 << 53) - 1.0

    return rnd


def _waypoint_interp(knots, fr):
    """Piecewise-linear interpolation over (frac, value) knots."""
    if fr <= knots[0][0]:
        return knots[0][1]
    for k in range(1, len(knots)):
        f1, v1 = knots[k]
        f0, v0 = knots[k - 1]
        if fr <= f1:
            if f1 == f0:
                return v1
            return v0 + (v1 - v0) * (fr - f0) / (f1 - f0)
    return knots[-1][1]


def gen_points(spec, ts_start, dt_s, n, seed):
    """Generate one deterministic telemetry series.

    shape=wave: base + amp*sin + noise (clamped) for healthy regimes.
    shape=waypoints: piecewise-linear story through (frac, value) knots —
        frac in [0,1] of the hour grid — plus clamped noise.
    shape=pulse: piece-cadence square wave with proportional edge jitter.
    """
    rnd = lcg(seed)
    shape = spec["shape"]
    t0 = float(ts_start)
    pts = []
    if shape == "wave":
        base, amp = spec["base"], spec["amp"]
        noise, period = spec["noise"], spec["period_s"]
        lo, hi = spec.get("min", -1e9), spec.get("max", 1e9)
        dec = spec.get("dec", 2)
        for i in range(n):
            t = t0 + i * dt_s
            v = base + amp * math.sin(2.0 * math.pi * (t % period) / period) + noise * rnd()
            v = min(max(v, lo), hi)
            pts.append([int(t), round(v, dec)])
    elif shape == "waypoints":
        knots = spec["points"]
        noise = spec.get("noise", 0.0)
        lo, hi = spec.get("min", -1e9), spec.get("max", 1e9)
        dec = spec.get("dec", 2)
        for i in range(n):
            fr = i / (n - 1)
            v = _waypoint_interp(knots, fr) + noise * rnd()
            v = min(max(v, lo), hi)
            pts.append([int(t0 + i * dt_s), round(v, dec)])
    elif shape == "pulse":
        period, duty = spec["period_s"], spec["duty"]
        jitter = spec.get("jitter", 0.0)
        for i in range(n):
            t = t0 + i * dt_s
            phase = (t % period) / period
            edge = 1.0 - duty + jitter * rnd() / 2.5
            in_win = edge <= phase < edge + duty if edge + duty <= 1.0 else False
            v = 1.0 if in_win else 0.0
            pts.append([int(t), v])
    else:
        raise ValueError(f"unknown shape {shape}")
    return pts


def crossing_events(pts, rule):
    """Threshold crossings of one alarm rule over the generated points →
    event descriptors (a rise, and a clear when it comes back under)."""
    out = []
    prev_over = False
    for t, v in pts[1:]:
        over = v > rule["threshold"]
        if over != prev_over:
            tmpl = rule["detail"] if over else rule["detail_clear"]
            out.append(
                {
                    "ts": t,
                    "name": rule["name"] if over else rule["name"] + "_clear",
                    "entity": rule["entity"],
                    "details": tmpl.format(obs=v, thr=rule["threshold"]),
                }
            )
        prev_over = over
    return out


# ---------------------------------------------------------------------------
# The loader
# ---------------------------------------------------------------------------


class Loader:
    def __init__(self, pack, client, require_create=True):
        self.pack = pack
        self.api = client
        self.require_create = require_create
        self.failures = []
        self.counts = {}

    def say(self, msg):
        print(msg, flush=True)

    # ---- write phase ------------------------------------------------------

    def load(self):
        pack = self.pack
        self.api.call("schema", {})

        # Pass 1: entities (no relations — a RelationSpec's target must exist).
        for e in pack["entities"]:
            res = self.api.call(
                "upsert_entity",
                {
                    "name": e["name"],
                    "type": e["type"],
                    "props": e.get("props", {}),
                    "relations": [],
                    "source": "plc",
                },
            )
            if self.require_create and not res.get("created"):
                raise RuntimeError(f"entity {e['name']} not created: {res}")
        n_ent = len(pack["entities"])
        self.say(f"LOAD entities: {n_ent} created (pass 1, source=plc => Fact)")

        # Pass 2: relations appended onto the now-existing endpoints.
        by_name = {e["name"]: e for e in pack["entities"]}
        n_rel = 0
        for r in pack["relations"]:
            src = by_name[r["from"]]
            res = self.api.call(
                "upsert_entity",
                {
                    "name": r["from"],
                    "type": src["type"],
                    "props": {},
                    "relations": [
                        {
                            "to": r["to"],
                            "relation_type": r["type"],
                            "valid_from": pack["ts_base"],
                        }
                    ],
                    "source": "plc",
                },
            )
            if self.require_create and res.get("relations_added") != 1:
                raise RuntimeError(f"relation {r} not added: {res}")
            n_rel += 1
        self.say(f"LOAD relations: {n_rel} added (pass 2, Fact)")

        # Pack-base states (the line at rest before the scenarios start).
        t0 = pack["ts_base"]
        n_states = 0
        for s in pack.get("states_packbase", []):
            res = self.api.call(
                "set_state",
                {
                    "entity": s["entity"],
                    "key": s["key"],
                    "value": s["value"],
                    "at": t0 + s["offset_s"],
                },
            )
            if not res.get("set"):
                raise RuntimeError(f"set_state {s} refused: {res}")
            n_states += 1

        n_events = 0
        n_points = 0
        for si, sc in enumerate(pack["demo_scenarios"]):
            t1 = sc["ts_start"]
            for s in sc.get("states", []):
                self.api.call(
                    "set_state",
                    {
                        "entity": s["entity"],
                        "key": s["key"],
                        "value": s["value"],
                        "at": t1 + s["offset_s"],
                    },
                )
                n_states += 1
            for ev in sc.get("events", []):
                self.api.call(
                    "log_event",
                    {
                        "name": ev["name"],
                        "entity": ev["entity"],
                        "timestamp": t1 + ev["offset_s"],
                        "provenance": 5,  # Provenance::Event (spec §14)
                        "details": ev["details"],
                    },
                )
                n_events += 1
            n_series = len(sc["signals"])
            for series_i, (series, sig_spec) in enumerate(sorted(sc["signals"].items())):
                entity, signal = series.split(".", 1)
                pts = gen_points(
                    sig_spec,
                    sc["ts_start"],
                    sc["dt_s"],
                    sc["points_per_series"],
                    seed=si * 100 + series_i,
                )
                for c in range(0, len(pts), 100):
                    chunk = pts[c : c + 100]
                    res = self.api.call(
                        "ingest_points",
                        {"entity": entity, "signal": signal, "points": chunk},
                    )
                    if res.get("accepted") != len(chunk):
                        raise RuntimeError(f"{series}: partial ingest {res}")
                n_points += len(pts)
            # Alarm rules: evaluate the REAL threshold crossings of the
            # generated points and log each one as an event (provenance 5).
            seed_for = {}
            for j, name in enumerate(sorted(sc["signals"].keys())):
                seed_for[name] = si * 100 + j
            for rule in pack.get("alarm_rules", []):
                series = rule["series"]
                if series not in sc["signals"]:
                    continue
                pts = gen_points(
                    sc["signals"][series],
                    sc["ts_start"],
                    sc["dt_s"],
                    sc["points_per_series"],
                    seed=seed_for[series],
                )
                for ev in crossing_events(pts, rule):
                    self.api.call(
                        "log_event",
                        {
                            "name": ev["name"],
                            "entity": ev["entity"],
                            "timestamp": ev["ts"],
                            "provenance": 5,
                            "details": ev["details"],
                        },
                    )
                    n_events += 1
            self.say(
                f"LOAD scenario '{sc['name']}': {n_series} series x "
                f"{sc['points_per_series']} pts @ dt={sc['dt_s']}s "
                f"[{t1}..{t1 + 3493}]"
            )
        self.counts = {
            "entities": n_ent,
            "relations": n_rel,
            "states": n_states,
            "events": n_events,
            "points": n_points,
        }
        self.say(
            "LOAD totals: {entities} entities, {relations} relations, "
            "{states} states, {events} events, {points} telemetry points".format(
                **self.counts
            )
        )

    # ---- verify phase -----------------------------------------------------

    def verify(self):
        pack = self.pack

        schema = self.api.call("schema", {})
        types = set(schema["entity_types"])
        topos = set(schema["relation_topologies"])
        all_series = set(schema["series"])
        need_types = {e["type"] for e in pack["entities"]}
        need_topos = {r["type"].split(":")[0] for r in pack["relations"]}
        need_series = set()
        for sc in pack["demo_scenarios"]:
            need_series |= set(sc["signals"])
        self.check(
            "schema entity_types",
            need_types <= types,
            f"need {sorted(need_types)} — schema has {sorted(types)}",
        )
        self.check(
            "schema topologies",
            need_topos <= topos,
            f"need {sorted(need_topos)} — schema has {sorted(topos)}",
        )
        self.check(
            "schema series",
            need_series <= all_series,
            f"need {len(need_series)} — schema has {len(all_series)}",
        )

        rows = self.api.call(
            "query",
            {"vql": "MATCH (p:PLC) -[:network]-> (r:Robot) RETURN p, r"},
        )["rows"]
        self.check(
            "MATCH PLC-network->Robot",
            len(rows) == 1,
            f"n={len(rows)} (PLC01->ROB01 over profinet)",
        )
        rows = self.api.call(
            "query",
            {
                "vql": "MATCH (d:Drive) -[:electrical]-> (m:Motor) -[:mechanical]-> (c:Conveyor) RETURN d, m, c"
            },
        )["rows"]
        self.check(
            "MATCH Drive-electrical->Motor-mechanical->Conveyor",
            len(rows) == 1,
            f"n={len(rows)} (DRIV01->EMOT01->CONV01)",
        )
        rows = self.api.call(
            "query",
            {"vql": "MATCH (s:Sensor) -[:instrumentation]-> (m:Motor) RETURN s, m"},
        )["rows"]
        self.check(
            "MATCH Sensor-instrumentation->Motor",
            len(rows) == 2,
            f"n={len(rows)} (SEN03, SEN04 -> EMOT01)",
        )

        tr = self.api.call("trace", {"from": "PLC01", "to": "ROB01", "max_hops": 6})
        ok_trace = tr.get("found") is True and tr.get("n_hops") == 1
        step_topo = tr.get("steps", [{}])[0].get("topology", "?") if tr.get("found") else "-"
        self.check(
            "trace PLC01->ROB01",
            ok_trace,
            f"found={tr.get('found')} n_hops={tr.get('n_hops')} topology={step_topo}",
        )

        for sc in pack["demo_scenarios"]:
            t1, t2 = sc["ts_start"], sc["ts_start"] + 3600
            for signal, expected_status in sorted(EXPECTED_VERDICTS[sc["name"]].items()):
                entity = CHECK_ENTITY[signal]
                res = self.api.call(
                    "check", {"entity": entity, "signal": signal, "from": t1, "to": t2}
                )
                self.check_scenario(sc, signal, entity, expected_status, res)
            rows = self.api.call(
                "query_temporal",
                {
                    "vql": (
                        'MATCH (m:Motor) WHERE m.name = "EMOT01" '
                        f"MEASURE m.temperature DURING {t1}..{t2} "
                        "RETURN max(m.temperature), min(m.temperature), "
                        "avg(m.temperature), count(m.temperature)"
                    )
                },
            )["rows"]
            aggs = rows[0] if rows else {}
            cnt = int((aggs.get("count") or {}).get("value") or 0)
            tmax = (aggs.get("max") or {}).get("value")
            tavg = (aggs.get("avg") or {}).get("value")
            span = TEMP_SPANS[sc["name"]]
            self.check(
                f"MEASURE {sc['name']}: count",
                cnt == sc["points_per_series"],
                f"count={cnt} (expected {sc['points_per_series']})",
            )
            self.check(
                f"MEASURE {sc['name']}: temperature max/avg in planned band",
                tmax is not None and span[0] <= (tavg or 0) and tmax <= span[1],
                f"max={tmax} avg={tavg} (planned band {span})",
            )
            evs = self.api.call(
                "get_events", {"entity": "EMOT01", "from": t1, "to": t2}
            )["events"]
            n_expect = EXPECTED_EVENTS[sc["name"]]
            provs = {e["provenance"] for e in evs}
            self.check(
                f"events {sc['name']} on EMOT01",
                len(evs) == n_expect and (provs <= {"Event"} if evs else not provs),
                f"n={len(evs)} (planned {n_expect}) provenance={sorted(provs) or '{}'}",
            )

        rows = self.api.call(
            "query",
            {"vql": 'MATCH (m:Motor) WHERE m.name = "EMOT01" RETURN m'},
        )["rows"]
        props = rows[0]["m"]["properties"]
        self.check(
            "spec props on EMOT01",
            props.get("spec.current.max") == "3"
            and props.get("spec.temperature.max") == "60",
            f"spec.current.max={props.get('spec.current.max')} "
            f"spec.temperature.max={props.get('spec.temperature.max')}",
        )
        rows = self.api.call(
            "query",
            {"vql": 'MATCH (s:Sensor) WHERE s.name = "SEN03" RETURN s'},
        )["rows"]
        props = rows[0]["s"]["properties"]
        self.check(
            "spec prop on SEN03",
            props.get("spec.vibration.max") == "2.5",
            f"spec.vibration.max={props.get('spec.vibration.max')}",
        )

    def check_scenario(self, sc, signal, entity, expected_status, res):
        status = res.get("status")
        dev = res.get("deviation")
        obs = res.get("observed")
        exp = res.get("expected_max")
        ok = status == expected_status and exp is not None and obs is not None
        if ok and expected_status == "OK":
            ok = (dev or 0) <= 0
        if ok and expected_status == "VIOLATION":
            ok = dev is not None and dev > 0
        self.check(
            f"CHECK {sc['name']}: {entity}.{signal}",
            ok,
            f"status={status} observed={obs} expected_max={exp} deviation={dev} "
            f"(expected verdict {expected_status})",
        )

    def check(self, name, ok, detail):
        tag = "PASS" if ok else "FAIL"
        self.say(f"CHECK {name}: {tag} — {detail}")
        if not ok:
            self.failures.append(name)


# The demo contract: per scenario, which entity+signal to CHECK and the
# verdict the telemetry must produce.
EXPECTED_VERDICTS = {
    "normal": {"current": "OK", "temperature": "OK", "vibration": "OK"},
    "overload": {"current": "VIOLATION", "temperature": "OK", "vibration": "VIOLATION"},
    "overheating": {"current": "OK", "temperature": "VIOLATION", "vibration": "OK"},
}
CHECK_ENTITY = {"current": "EMOT01", "temperature": "EMOT01", "vibration": "SEN03"}
TEMP_SPANS = {
    "normal": [40.0, 55.2],
    "overload": [44.5, 58.8],
    "overheating": [51.5, 80.2],
}
# EMOT01 events per scenario window: chronology (story) events + alarm
# crossings on EMOT01-owned series, evaluated on the generated points.
# overload: alarm_current_max rise/clear x3 + alarm_vibration_max x1 = 7;
# overheating: cooling_fault + temp rise + trip + temp clear = 4.
EXPECTED_EVENTS = {"normal": 0, "overload": 7, "overheating": 4}


# ---------------------------------------------------------------------------


def fresh_db(path):
    for suffix in ["", "-wal"]:
        p = path + suffix
        if os.path.exists(p):
            os.remove(p)


def default_bin():
    env = os.environ.get("VIDGEDB_BIN")
    if env and os.path.exists(env):
        return env
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cand = os.path.join(here, "target", "release", "vidgedb")
    if os.path.exists(cand):
        return cand
    raise SystemExit(
        "vidgedb binary not found: build with `cargo build --release --bin vidgedb`"
    )


def client_for(bin_path, db, transport, role, port):
    if transport == "http":
        return HttpClient(bin_path, db, role, port)
    return StdioClient(bin_path, db, role)


def main(argv):
    ap = argparse.ArgumentParser(
        description="Load a VidgeDB machine pack JSON into a fresh twin."
    )
    ap.add_argument("pack", help="pack definition JSON (examples/pack-conveyor.json)")
    ap.add_argument(
        "db", nargs="?", default=None, help="target .vdg path (a FRESH file is created)"
    )
    ap.add_argument(
        "--bin",
        default=None,
        help="vidgedb binary (default $VIDGEDB_BIN or target/release/vidgedb)",
    )
    ap.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    ap.add_argument("--port", type=int, default=8917, help="HTTP port (--transport http)")
    ap.add_argument(
        "--fresh",
        action="store_true",
        default=True,
        help="delete the target twin first (default True — the loader targets a fresh twin)",
    )
    ap.add_argument(
        "--append",
        action="store_true",
        help="keep the existing twin and skip created=True assertions (idempotent reload path)",
    )
    ap.add_argument(
        "--no-verify", action="store_true", help="load only, skip the read-back verification"
    )
    args = ap.parse_args(argv)

    with open(args.pack, "r", encoding="utf-8") as f:
        pack = json.load(f)

    db = args.db or f"/tmp/vidgedb-pack-{pack.get('pack', 'twin')}-{os.getpid()}.vdg"
    if args.fresh and not args.append:
        fresh_db(db)
    require_create = not args.append
    bin_path = args.bin or default_bin()
    print(
        f"PACK {pack.get('pack')} v{pack.get('pack_version')}: target {db} "
        f"via {args.transport} ({bin_path})",
        flush=True,
    )

    writer = client_for(bin_path, db, args.transport, "writer", args.port)
    loader = Loader(pack, writer, require_create=require_create)
    try:
        loader.load()
    finally:
        writer.close()

    if not args.no_verify:
        # A SECOND service process reopens the twin freshly — the verify
        # stage reads what is on disk, not the writer's in-RAM state.
        reader = client_for(bin_path, db, args.transport, "reader", args.port + 1)
        try:
            loader.api = reader
            loader.verify()
        finally:
            reader.close()

    if loader.failures:
        print(
            f"PACK_LOAD_FAIL: {len(loader.failures)} failed CHECK(s): {loader.failures}",
            flush=True,
        )
        return 1
    if not args.no_verify:
        print(f"PACK_LOAD_OK: {json.dumps(loader.counts, sort_keys=True)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
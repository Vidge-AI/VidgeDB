#!/usr/bin/env python3
"""Deterministic JSON-RPC fixture generator for the VidgeDB SDK test suites.

Builds the SDK "twin" database entirely through the binary's PUBLIC
JSON-RPC service (``upsert_entity`` / ``ingest_points``) — no Rust build step,
no dependence on the engine sources. Stdlib only.

The generated twin reproduces the deterministic one-machine benchgen
fixture that the Python and JS SDK suites assert against:

    PLC_000001 -network:profinet-> Drive_000001 -electrical:feeds->
    Motor_000001 -mechanical:drives-> Pump_000001

    Motor_000001 carries ``spec.current.max = "10.0"`` and
    ``spec.current.unit = "A"``, plus a 300-point ``Motor_000001.current``
    series sampled on the 60 s grid from ``T0 = 1_700_000_000`` whose max
    overshoots the 10.0 spec (=> CHECK status VIOLATION).

Determinism: every "random" value is the pure function
``point_value(seed, kind, i, n)`` of the same seeded LCG the engine's
``benchgen`` uses (spec §42 reproducibility) — the same output every run.

CLI:
    VIDGEDB_BIN=/path/to/vidgedb python3 fixture.py <out.vdg>

The module is also importable: ``generate_fixture(db_path, bin=...)``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# benchgen constants (mirrored, not imported — the SDK must not need the crate)
# ---------------------------------------------------------------------------

DEFAULT_SEED = 0x56_49_44_47_45_44_42_38  # "VIDGEDB8"
K1 = 0x9E37_79B9_7F4A_7C15
K2 = 0xBF58_476D_1CE4_E5B9
T0 = 1_700_000_000  # first timestamp of every generated series
POINTS_PER_SIGNAL = 300
LCG_A = 6364136223846793005
LCG_C = 1442695040888963407
_MASK64 = (1 << 64) - 1


def series_seed(machine: int, signal_idx: int, base: int = DEFAULT_SEED) -> int:
    """Per-series seed: base mixed with machine and signal indices."""
    return (
        base
        ^ ((machine * K1) & _MASK64)
        ^ (((signal_idx + 1) * K2) & _MASK64)
    ) & _MASK64


def _lcg_new(seed: int) -> int:
    """LCG state after the 4 warm-up draws (mirrors benchgen::Lcg::new)."""
    s = seed & _MASK64
    for _ in range(4):
        s = (s * LCG_A + LCG_C) & _MASK64
    return s


def _lcg_next(state: int) -> int:
    return (state * LCG_A + LCG_C) & _MASK64


def point_value(seed: int, kind: int, i: int, n: int) -> float:
    """Value of point ``i`` of a series — pure function of (seed, kind, i, n).

    Mirrors ``benchgen::point_value``: one LCG draw per point (after ``i``
    skipped draws), a linear ramp, plus deterministic uniform noise.
    """
    state = _lcg_new(seed)
    for _ in range(i):
        state = _lcg_next(state)
    state = _lcg_next(state)  # the single noise draw for this point
    u = ((state >> 11) & ((1 << 53) - 1)) / float(1 << 53)  # [0, 1)
    noise = (u - 0.5) * 2.0 * 0.4
    frac = 0.0 if n <= 1 else i / (n - 1.0)
    if kind % 3 == 0:
        return 4.0 + 6.0 * frac + noise  # current: ramp 4..10 A
    raise ValueError("SDK fixture only generates the `current` signal (kind 0)")


def _current_points(machine: int = 0) -> list:
    """The 300 ``[t, value]`` points of ``Motor_<machine>.current``."""
    seed = series_seed(machine, 0)
    return [
        [T0 + i * 60, point_value(seed, 0, i, POINTS_PER_SIGNAL)]
        for i in range(POINTS_PER_SIGNAL)
    ]


# ---------------------------------------------------------------------------
# Minimal JSON-RPC client over `vidgedb --service` (stdlib only)
# ---------------------------------------------------------------------------


class _Service:
    """Thin line-delimited JSON-RPC 2.0 client over a `--service` subprocess."""

    def __init__(self, db_path: str, bin_path: str, role: str = "ingest", agent_id: str = "fixturegen"):
        self._next_id = 1
        self._proc = subprocess.Popen(
            [bin_path, "--service", db_path, "--agent-id", agent_id, "--role", role],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )

    def call(self, method: str, **params):
        req = {"jsonrpc": "2.0", "id": self._next_id, "method": method}
        self._next_id += 1
        if params:
            req["params"] = params
        assert self._proc.stdin is not None and self._proc.stdout is not None
        self._proc.stdin.write((json.dumps(req, separators=(",", ":")) + "\n").encode())
        self._proc.stdin.flush()
        line = self._proc.stdout.readline()
        if not line:
            raise RuntimeError(f"vidgedb closed stdout during {method!r}")
        resp = json.loads(line.decode("utf-8"))
        if "error" in resp:
            err = resp["error"]
            raise RuntimeError(f"{method}: [{err.get('code')}] {err.get('message')}")
        result = resp.get("result")
        # AgentApi v0 semantic-error shape: {"error": "..."} inside the result.
        if isinstance(result, dict) and set(result.keys()) == {"error"}:
            raise RuntimeError(f"{method}: {result['error']}")
        return result

    def close(self) -> None:
        try:
            if self._proc.stdin and not self._proc.stdin.closed:
                self._proc.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=10)


# ---------------------------------------------------------------------------
# Binary resolution
# ---------------------------------------------------------------------------


def resolve_bin(bin_path: Optional[str] = None) -> str:
    """Resolve the vidgedb binary: explicit ``bin=``, else ``$VIDGEDB_BIN``."""
    candidate = bin_path or os.environ.get("VIDGEDB_BIN")
    if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
        return candidate
    raise RuntimeError(
        "vidgedb binary not found — set VIDGEDB_BIN to the compiled engine binary"
    )


# ---------------------------------------------------------------------------
# Fixture generation
# ---------------------------------------------------------------------------


def generate_fixture(db_path, bin_path: Optional[str] = None) -> Path:
    """Build the deterministic SDK twin at ``db_path`` via JSON-RPC.

    Returns the path to the freshly written ``.vdg`` file. Any pre-existing
    file (and its sidecars) is removed first so generation is repeatable.
    """
    db_path = Path(db_path)
    for side in ("", "-wal", ".lock"):
        Path(str(db_path) + side).unlink(missing_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    svc = _Service(str(db_path), resolve_bin(bin_path))
    try:
        # 1. Entities, created in key order 0..3 (PLC, Drive, Motor, Pump).
        svc.call("upsert_entity", name="PLC_000001", type="PLC", props={}, source="plc")
        svc.call("upsert_entity", name="Drive_000001", type="Drive", props={}, source="plc")
        svc.call(
            "upsert_entity",
            name="Motor_000001",
            type="Motor",
            props={"spec.current.max": "10.0", "spec.current.unit": "A"},
            source="plc",
        )
        svc.call("upsert_entity", name="Pump_000001", type="Pump", props={}, source="plc")
        # 2. Forward relations PLC -> Drive -> Motor -> Pump. `upsert_entity`
        #    appends edges SRC -> target, so each edge is emitted from its
        #    source entity (dedup makes the repeated upsert a no-op merge).
        #    First two are the machine's official topology (source "plc" =>
        #    Fact); the mechanical link is an observed one (=> Observation).
        svc.call(
            "upsert_entity",
            name="PLC_000001",
            type="PLC",
            props={},
            relations=[{"to": "Drive_000001", "relation_type": "network:profinet"}],
            source="plc",
        )
        svc.call(
            "upsert_entity",
            name="Drive_000001",
            type="Drive",
            props={},
            relations=[{"to": "Motor_000001", "relation_type": "electrical:feeds"}],
            source="plc",
        )
        svc.call(
            "upsert_entity",
            name="Motor_000001",
            type="Motor",
            props={},
            relations=[{"to": "Pump_000001", "relation_type": "mechanical:drives"}],
            source="sensor",
        )
        # 2. The 300-point deterministic Motor_000001.current series
        #    (auto-created by the first ingest_points call).
        svc.call(
            "ingest_points",
            entity="Motor_000001",
            signal="current",
            points=_current_points(0),
        )
    finally:
        svc.close()
    return db_path


def main(argv) -> int:
    if len(argv) != 2:
        print("usage: fixture.py <out.vdg>", file=sys.stderr)
        return 2
    try:
        out = generate_fixture(argv[1])
    except RuntimeError as exc:
        print(f"fixture: {exc}", file=sys.stderr)
        return 1
    print(f"fixture: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

"""Phase 11 — VidgeDB Python SDK test suite.

Spawns the REAL `vidgedb --service` binary (located via $VIDGEDB_BIN) against
a fixture database built entirely through the binary's PUBLIC JSON-RPC
service (`upsert_entity` / `ingest_points`) by `tests/fixture.py` — no Rust
build step, no dependence on the engine sources. Exercises the full client
surface: schema, valid/invalid VidgeQL, typed measurements, the deviation
check, JSON-RPC error → Python exception mapping, dynamic (future-method)
dispatch, threading, and context-manager teardown.

Note on the fixture: the twin is a deterministic one-machine production line
(PLC→Drive→Motor→Pump + a 300-point `Motor_000001.current` series) generated
ONCE per pytest session by `fixture.generate_fixture()` and copied per-test
(cheap). The values are the same pure function of the seeded LCG that the
engine's `benchgen` uses, so the assertions are byte-stable.

Stdlib + pytest only, matching the SDK's zero-dependency design.
"""

import json
import os
import shutil
import subprocess
import sys
import threading
from pathlib import Path

import pytest

import fixture  # noqa: E402 — tests/ is on sys.path; the JSON-RPC fixture generator

SDK_ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(SDK_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from vidgedb import (  # noqa: E402
    CheckResult,
    CheckStatus,
    Measurement,
    MeasurementSeries,
    VidgeDB,
    VidgeDBError,
    __version__,
)

NOW = 1_760_000_000  # fixed reference clock (matches Rust fixtures)
MOTOR = "Motor_000001"
PLC = "PLC_000001"
PUMP = "Pump_000001"

# ---------------------------------------------------------------------------
# Binary + fixture plumbing (session-scoped)
# ---------------------------------------------------------------------------


def find_vidgedb_bin() -> Path:
    """Resolve the vidgedb binary from $VIDGEDB_BIN (the single source of
    truth for an autonomous SDK repo — no engine build tree is consulted)."""
    env = os.environ.get("VIDGEDB_BIN")
    if env and Path(env).is_file() and os.access(env, os.X_OK):
        return Path(env)
    pytest.fail(
        "vidgedb binary not found — set VIDGEDB_BIN to the compiled "
        "`vidgedb` binary (e.g. VIDGEDB_BIN=/path/to/vidgedb)"
    )


VIDGEDB_BIN = find_vidgedb_bin()

_fixture_src: Path | None = None


def ensure_fixture_db() -> Path:
    """Build (once per session) the deterministic 1-machine twin through the
    binary's PUBLIC JSON-RPC service, via `tests/fixture.py` (stdlib only)."""
    global _fixture_src
    if _fixture_src is not None and _fixture_src.is_file():
        return _fixture_src
    out = Path("/tmp") / f"vidgedb_sdk_fixture_{os.getpid()}.vdg"
    try:
        fixture.generate_fixture(out, bin_path=str(VIDGEDB_BIN))
    except RuntimeError as exc:
        pytest.fail(f"cannot generate SDK fixture via JSON-RPC: {exc}")
    _fixture_src = out
    return out


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "sdk_test.vdg"


@pytest.fixture()
def fresh_db(db_path: Path) -> Path:
    """An empty-but-valid .vdg: the service creates storage on first open."""
    db_path.unlink(missing_ok=True)
    return db_path


@pytest.fixture()
def populated_db(tmp_path: Path) -> Path:
    """A copy of the session fixture: PLC→Drive→Motor→Pump + 300-pt
    Motor.current series whose max breaches spec.current.max=10 (VIOLATION)."""
    env_db = os.environ.get("VIDGEDB_TEST_DB", "")
    src = Path(env_db) if env_db else ensure_fixture_db()
    dst = tmp_path / "populated.vdg"
    shutil.copy(src, dst)
    for side in ("-wal",):
        if Path(str(src) + side).is_file():
            shutil.copy(str(src) + side, str(dst) + side)
    return dst


@pytest.fixture()
def db(populated_db: Path):
    with VidgeDB(str(populated_db), bin=str(VIDGEDB_BIN), agent_id="sdk-test", role="writer") as d:
        yield d


# ---------------------------------------------------------------------------
# Construction / lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_spawns_and_closes_cleanly(self, fresh_db):
        d = VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN), agent_id="t-lifecycle")
        assert d.is_running()
        d.close()
        assert not d.is_running()
        # clean EOF exit, not a kill: returncode 0
        assert d._proc.returncode == 0

    def test_context_manager_closes_process(self, fresh_db):
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            assert d.pid > 0
            d.schema()  # at least one round-trip inside the context
        assert not d.is_running()
        assert d._proc.returncode == 0

    def test_close_is_idempotent(self, fresh_db):
        d = VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN))
        d.close()
        d.close()  # second close must not raise
        assert not d.is_running()

    def test_call_after_close_raises_client_closed(self, fresh_db):
        d = VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN))
        d.close()
        with pytest.raises(VidgeDBError, match="client is closed"):
            d.schema()

    def test_version_dunder(self):
        assert __version__ == "0.1.0"

    def test_repr_and_pid(self, fresh_db):
        d = VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN))
        try:
            assert str(d.pid) in repr(d)
            assert "open" in repr(d)
        finally:
            d.close()


# ---------------------------------------------------------------------------
# schema()
# ---------------------------------------------------------------------------


class TestSchema:
    def test_schema_returns_provenance_classes(self, fresh_db):
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            s = d.schema()
        assert s["provenance_classes"][0] == "Fact"
        assert "Observation" in s["provenance_classes"]
        assert "Specification" in s["provenance_classes"]
        assert isinstance(s["entity_types"], list)

    def test_schema_lists_the_fixture_types(self, db):
        s = db.schema()
        assert set(s["entity_types"]) == {"PLC", "Drive", "Motor", "Pump"}
        assert "Motor_000001.current" in s["series"]
        assert "electrical" in s["relation_topologies"]

    def test_dynamic_dispatch_matches_mirror(self, fresh_db):
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            assert d.call("schema") == d.schema()


# ---------------------------------------------------------------------------
# query() + error paths
# ---------------------------------------------------------------------------


class TestQuery:
    def test_valid_query_returns_rows_shape(self, db):
        r = db.query("MATCH (m:Motor) RETURN m")
        assert r["n"] >= 1
        assert isinstance(r["rows"], list)
        row = r["rows"][0]["m"]
        assert row["type"] == "Motor"
        assert row["name"] == MOTOR

    def test_where_filter_narrows(self, db):
        r = db.query(f'MATCH (m:Motor) WHERE m.name = "{MOTOR}" RETURN m')
        assert r["n"] == 1

    def test_invalid_vql_raises_semantic_error_no_panic(self, fresh_db):
        """A parse-error result arrives as the AgentApi v0 `{"error": …}`
        shape → the SDK raises VidgeDBError(code=0), and the zero-panic
        contract means the service still answers afterwards."""
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            with pytest.raises(VidgeDBError) as ei:
                d.query("TOTAL GARBAGE NOT VQL")
            assert d.is_running()
            assert d.schema()["provenance_classes"]
        assert ei.value.code == 0
        assert "parse error" in ei.value.message

    def test_unknown_method_raises_method_not_found(self, fresh_db):
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            d.schema()  # warm session
            with pytest.raises(VidgeDBError) as ei:
                d.call("definitely_not_a_method")
        assert ei.value.code == -32601

    def test_invalid_params_raises_invalid_params(self, fresh_db):
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            with pytest.raises(VidgeDBError) as ei:
                d.call("get_entity", key="not-a-number")
        assert ei.value.code == -32602

    def test_missing_required_param_raises(self, fresh_db):
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            with pytest.raises(VidgeDBError) as ei:
                d.call("get_entity")  # missing required `key`
        assert ei.value.code == -32602

    def test_semantic_error_in_result_raises_code_zero(self, db):
        """AgentApi v0's `{"error": "..."}`-inside-result shape (an unknown
        entity passed to get_measurements) is a SEMANTIC miss, not a
        transport failure — the SDK surfaces it as VidgeDBError(code=0)."""
        with pytest.raises(VidgeDBError) as ei:
            db.get_measurements("Ghost", "x", 0, 100)
        assert ei.value.code == 0
        assert "no series named" in ei.value.message
        assert ei.value.data["error"] == ei.value.message
        # zero panic: still serving
        assert db.schema()["provenance_classes"]

    def test_garbage_responses_keep_client_consistent(self, db):
        # after an error, the next request must still round-trip correctly
        with pytest.raises(VidgeDBError):
            db.call("nope")
        assert db.schema()["provenance_classes"]


# ---------------------------------------------------------------------------
# Typed data paths (measurements / check / events / audit)
# ---------------------------------------------------------------------------


class TestTypedData:
    def test_get_measurements_returns_typed_series(self, db):
        series = db.get_measurements(MOTOR, "current", 0, 9_900_000_000)
        assert isinstance(series, MeasurementSeries)
        assert series.count == len(series.points) == 300
        first = series.points[0]
        assert isinstance(first, Measurement)
        assert first.t == 1_700_000_000
        assert first.t < series.points[1].t
        assert series.min is not None and series.max is not None
        assert series.min <= series.max

    def test_measurement_window_from_to_respected(self, db):
        full = db.get_measurements(MOTOR, "current", 0, 9_900_000_000)
        narrow = db.get_measurements(MOTOR, "current", 1_700_000_000, 1_700_000_060)
        assert full.count == 300
        assert narrow.count == 2
        assert [p.t for p in narrow.points] == [1_700_000_000, 1_700_000_060]

    def test_check_returns_typed_check_result(self, db):
        r = db.check(MOTOR, "current", 0, 9_900_000_000)
        assert isinstance(r, CheckResult)
        assert r.status in {
            CheckStatus.OK,
            CheckStatus.VIOLATION,
            CheckStatus.NO_DATA,
            CheckStatus.NO_SPEC,
        }
        assert r.window_to == 9_900_000_000
        assert r.points_checked == 300
        # the deterministic benchgen fixture overshoots spec 10.0 → VIOLATION
        assert r.status == CheckStatus.VIOLATION
        assert r.is_violation is True
        assert r.expected_max == 10.0
        assert r.observed > r.expected_max
        assert r.deviation == pytest.approx(r.observed - r.expected_max)
        assert r.expected_provenance == "Specification"
        assert r.observed_provenance == "Observation"

    def test_check_unknown_entity_semantic_error(self, populated_db):
        with VidgeDB(str(populated_db), bin=str(VIDGEDB_BIN)) as d:
            with pytest.raises(VidgeDBError) as ei:
                d.check("Ghost-Machine", "current", 0, 100)
        assert ei.value.code == 0
        assert "unknown entity" in ei.value.message

    def test_no_spec_status_when_spec_missing(self, db):
        r = db.check(PUMP, "pressure", 0, 9_900_000_000)
        assert r.status == CheckStatus.NO_SPEC

    def test_query_temporal_aggregates(self, db):
        r = db.query_temporal(
            "MATCH (m:Motor) MEASURE m.current DURING last(1h) "
            "RETURN count(current), max(current)",
            now=1_700_002_000,
        )
        assert r["n"] >= 1
        row = r["rows"][0]
        assert row["count"]["kind"] == "Count"
        assert row["count"]["value"] >= 1.0

    def test_log_event_then_get_events_roundtrip(self, db):
        out = db.log_event("sdk_test_event", MOTOR, 1_700_000_500, 1, "from pytest")
        assert out["logged"] is True
        ev = db.call("get_events", entity=MOTOR)
        names = [e["name"] for e in ev["events"]]
        assert "sdk_test_event" in names

    def test_audit_trail_lists_agent_and_methods(self, db):
        db.schema()
        db.check(MOTOR, "current", 0, 9_900_000_000)
        a = db.audit()
        methods = [e["method"] for e in a["entries"]]
        assert "schema" in methods and "check" in methods
        assert all(e["agent_id"] == "sdk-test" for e in a["entries"])

    def test_get_entity_card(self, db):
        card = db.get_entity(2)
        assert card["name"] == MOTOR
        assert card["type"] == "Motor"

    def test_trace_finds_path_across_the_line(self, db):
        t = db.trace(PLC, PUMP, 4)
        assert t["found"] is True
        names = [n["name"] for n in t["path"]]
        assert names == [PLC, "Drive_000001", MOTOR, PUMP]
        assert t["n_hops"] == 3

    def test_trace_unknown_target_semantic_error(self, db):
        with pytest.raises(VidgeDBError) as ei:
            db.trace(PLC, "Nope", 3)
        assert ei.value.code == 0
        assert "unknown entity" in ei.value.message

    def test_dynamic_dispatch_for_methods_without_mirrors(self, db):
        """`provenance` is real but unmirrored: dynamic dispatch reaches it."""
        prov = db.provenance()
        assert "provenance_classes" in prov

    def test_state_tools(self, db):
        db.log_event("statechange", MOTOR, 1_700_000_900, 5, "running")
        ev = db.call("get_events", entity=MOTOR)
        assert any(e["name"] == "boot" or e["name"] == "statechange" for e in ev["events"])


# ---------------------------------------------------------------------------
# Thread-safety
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_threaded_requests_never_interleave(self, db):
        errors = []

        def worker() -> None:
            try:
                for _ in range(10):
                    s = db.schema()
                    assert "provenance_classes" in s
                    m = db.get_measurements(MOTOR, "current", 0, 9_900_000_000)
                    assert len(m.points) == m.count
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not errors, f"threaded round-trips failed: {errors}"


# ---------------------------------------------------------------------------
# Raw-wire protocol conformance (drives the binary directly)
# ---------------------------------------------------------------------------


def _raw_pipe_session(fresh_db, lines: bytes):
    """Spawn the service, push lines, collect all responses, close cleanly.
    Returns exit code + parsed responses. All fds deterministically closed
    (context managers — no ResourceWarning under pytest -W error)."""
    with subprocess.Popen(
        [str(VIDGEDB_BIN), "--service", str(fresh_db)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ) as p:
        p.stdin.write(lines)
        p.stdin.flush()
        p.stdin.close()
        raw = p.stdout.read()
        p.stdout.close()
        p.wait(timeout=10)
    return p.returncode, [json.loads(l) for l in raw.decode().splitlines()]


class TestProtocol:
    def test_binary_speaks_line_delimited_jsonrpc(self, fresh_db):
        rc, resps = _raw_pipe_session(
            fresh_db, b'{"jsonrpc":"2.0","id":7,"method":"schema"}\n'
        )
        assert rc == 0  # clean EOF exit
        assert len(resps) == 1
        assert resps[0]["id"] == 7
        assert resps[0]["jsonrpc"] == "2.0"

    def test_malformed_json_gets_parse_error(self, fresh_db):
        rc, resps = _raw_pipe_session(fresh_db, b"this is not json\n")
        assert rc == 0  # zero panic — clean exit after the parse error
        assert resps[0]["error"]["code"] == -32700

    def test_service_survives_invalid_request_then_answers(self, fresh_db):
        """Zero-panic contract, seen from the SDK level."""
        with VidgeDB(str(fresh_db), bin=str(VIDGEDB_BIN)) as d:
            with pytest.raises(VidgeDBError):
                d.call("get_entity")  # missing required key param
            assert d.is_running()
            assert d.schema()  # alive and serving

    def test_retain_reports_counts(self, db):
        r = db.retain(NOW)
        assert set(r.keys()) == {"points_removed", "chunks_removed"}
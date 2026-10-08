"""VidgeDB service client — spawn `vidgedb --service` and speak JSON-RPC 2.0.

Wire protocol (VidgeDB Phase 10, verified e2e): line-delimited JSON-RPC 2.0
over the subprocess's stdin/stdout — one request per line, one response per
line. Transport-level errors use the standard codes (-32700 parse, -32600
invalid request, -32601 method not found, -32602 invalid params, -32000
storage); method-level semantic problems come back inside `result` as the
AgentApi v0 `{"error": "..."}` shape and are surfaced here as
`VidgeDBError(code=0)`.

The client is deliberately thin and dynamic: methods are NOT hardcoded into a
dispatch table beyond the convenience mirrors — `call(method, **params)` sends
ANY method name, so methods added by newer vidgedb binaries (e.g. an ingest
surface) work immediately without an SDK release.
"""

from __future__ import annotations

import atexit
import json
import os
import threading
from typing import Any, Dict, Optional

from .types import CheckResult, MeasurementSeries

__all__ = ["VidgeDB", "VidgeDBError"]

# Protocol-level JSON-RPC error codes (service.rs).
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
STORAGE_ERROR = -32000

_DEFAULT_TIMEOUT = 30.0
_CLOSE_TIMEOUT = 5.0


class VidgeDBError(Exception):
    """A JSON-RPC error (transport or AgentApi v0 semantic shape).

    Attributes:
        code: JSON-RPC error code (e.g. -32601), or 0 for the AgentApi v0
            ``{"error": "..."}``-in-result shape (semantic, not transport).
        message: human-readable error text.
        data: raw error payload (dict) when available.
    """

    def __init__(self, code: int, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"[{code}] {message}" if code else message)


class VidgeDB:
    """A running `vidgedb --service <db.vdg>` subprocess, JSON-RPC client.

    Spawns the binary, speaks line-delimited JSON-RPC 2.0 on its stdin,
    reads one response line per request. Thread-safe: a lock serializes
    request+readline so concurrent threads never interleave a read.

    Usable as a context manager (`with VidgeDB(path) as db:` — close()
    closes stdin, waits up to _CLOSE_TIMEOUT, then SIGKILLs) or standalone;
    with a plain constructor the subprocess is registered with atexit and
    killed at interpreter shutdown if still open.

    The binary path resolves in order: explicit `bin=` argument, the
    ``VIDGEDB_BIN`` environment variable, else "vidgedb" on PATH. If the
    binary is not found, raises FileNotFoundError at construction (the
    service process never starts).
    """

    def __init__(
        self,
        db_path: str,
        bin: Optional[str] = None,
        agent_id: Optional[str] = None,
        retention_days: Optional[int] = None,
        timeout: float = _DEFAULT_TIMEOUT,
        role: Optional[str] = None,
    ):
        self.db_path = os.fspath(db_path)
        self.bin = bin or os.environ.get("VIDGEDB_BIN") or "vidgedb"
        self.agent_id = agent_id
        self.timeout = timeout
        self._lock = threading.Lock()
        self._closed = False
        self._next_id = 1

        args = [self.bin, "--service", self.db_path]
        if agent_id is not None:
            args += ["--agent-id", agent_id]
        if retention_days is not None:
            args += ["--retention-days", str(retention_days)]
        if role is not None:
            if role not in ("reader", "writer", "ingest"):
                raise VidgeDBError(0, f"unknown role {role!r} (reader|writer|ingest)")
            args += ["--role", role]

        self._proc = __import__("subprocess").Popen(
            args,
            stdin=__import__("subprocess").PIPE,
            stdout=__import__("subprocess").PIPE,
            # stderr passes through (service logs go there, not to stdout).
        )
        # Only register for interpreter-exit cleanup when the caller did not
        # opt into the context-manager discipline; harmless if called twice.
        atexit.register(self.close)

    # ------------------------------------------------------------------
    # Protocol plumbing
    # ------------------------------------------------------------------

    def call(self, method: str, **params: Any) -> Any:
        """Send one JSON-RPC request, return the `result` member.

        Generic dispatcher: any method the binary understands works here,
        including methods this SDK version does not mirror yet (dynamic
        dispatch — new vidgedb methods need no SDK update). A `result`
        carrying the AgentApi v0 `{"error": ...}` shape raises VidgeDBError
        with code 0; a JSON-RPC `error` member raises with its real code.
        """
        with self._lock:
            if self._closed:
                raise VidgeDBError(0, "client is closed")
            req_id = self._next_id
            self._next_id += 1
            req: Dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
            }
            if params:
                req["params"] = params
            assert self._proc.stdin is not None
            assert self._proc.stdout is not None
            try:
                self._proc.stdin.write(
                    (json.dumps(req, separators=(",", ":")) + "\n").encode()
                )
                self._proc.stdin.flush()
            except (BrokenPipeError, ValueError) as exc:  # dead child
                raise VidgeDBError(0, f"vidgedb process is not running: {exc}") from exc

            line = self._proc.stdout.readline()
            if not line:
                raise VidgeDBError(
                    0, f"vidgedb closed stdout unexpectedly (exit code pending: use .close())"
                )
            resp = json.loads(line.decode("utf-8"))
            if resp.get("id") != req_id:
                raise VidgeDBError(
                    PARSE_ERROR, f"response id mismatch: expected {req_id}, got {resp.get('id')!r}"
                )
            if "error" in resp:
                err = resp["error"]
                raise VidgeDBError(err.get("code", 0), err.get("message", ""), err)
            result = resp.get("result")
            # AgentApi v0 semantic-error shape: `{"error": "..."}` INSIDE the
            # result (method-level problems, not transport) → code 0.
            if isinstance(result, dict) and set(result.keys()) == {"error"} and isinstance(
                result["error"], str
            ):
                raise VidgeDBError(0, result["error"], result)
            return result

    def __getattr__(self, name: str):
        """Dynamic dispatch: any `db.any_method(**params)` forwards as-is.

        Lets the SDK tolerate (and immediately use) methods added by newer
        vidgedb binaries — e.g. an ingest surface — without a release.
        """
        if name.startswith("_"):
            raise AttributeError(name)

        def _dynamic(**params: Any) -> Any:
            return self.call(name, **params)

        _dynamic.__name__ = name
        _dynamic.__doc__ = f"Dynamic JSON-RPC dispatch for method '{name}'."
        return _dynamic

    # ------------------------------------------------------------------
    # Method mirrors (typed conveniencies over a subset of the documented methods;
    # the generic `call()` above is the contract — it reaches every method)
    # ------------------------------------------------------------------

    def schema(self) -> Dict[str, Any]:
        """Entity types, relation topologies, series list, provenance classes."""
        return self.call("schema")

    def query(self, vql: str) -> Dict[str, Any]:
        """Core VidgeQL (MATCH/WHERE/RETURN/LIMIT, AT time-travel)."""
        return self.call("query", vql=vql)

    def query_temporal(self, vql: str, now: Optional[int] = None) -> Dict[str, Any]:
        """Temporal VidgeQL — MEASURE … DURING last(n)/t1..t2, RETURN aggregates."""
        params: Dict[str, Any] = {"vql": vql}
        if now is not None:
            params["now"] = now
        return self.call("query_temporal", **params)

    def get_entity(self, key: int) -> Dict[str, Any]:
        """Full entity card by stable key (name, type, props, relations)."""
        return self.call("get_entity", key=key)

    def get_measurements(
        self,
        entity: str,
        signal: str,
        from_: int = 0,
        to: int = (1 << 62),
    ) -> MeasurementSeries:
        """Raw points of `<entity>.<signal>` in [from, to]; typed series."""
        return MeasurementSeries.from_rpc(
            self.call("get_measurements", entity=entity, signal=signal, **{"from": from_}, to=to)
        )

    def check(
        self,
        entity: str,
        signal: str,
        from_: int = 0,
        to: int = (1 << 62),
    ) -> CheckResult:
        """Spec §25 deviation check; typed CheckResult (OK/VIOLATION/…)."""
        return CheckResult.from_rpc(
            self.call("check", entity=entity, signal=signal, **{"from": from_}, to=to)
        )

    def trace(self, from_name: str, to_name: str, max_hops: int = 6) -> Dict[str, Any]:
        """BFS path between two entities by name, out-edges only."""
        return self.call("trace", **{"from": from_name}, to=to_name, max_hops=max_hops)

    def log_event(
        self,
        name: str,
        entity: str,
        timestamp: Optional[int] = None,
        provenance: int = 1,
        details: str = "",
    ) -> Dict[str, Any]:
        """Append one event (the only append-write the service exposes)."""
        import time

        params: Dict[str, Any] = {
            "name": name,
            "entity": entity,
            "timestamp": timestamp if timestamp is not None else int(time.time()),
            "provenance": provenance,
            "details": details,
        }
        return self.call("log_event", **params)

    def get_state(self, entity: str, key: str, at: Optional[int] = None) -> Any:
        """Current (or at-time) value of a state key on an entity."""
        params: Dict[str, Any] = {"entity": entity, "key": key}
        if at is not None:
            params["at"] = at
        return self.call("get_state", **params)

    def state_history(self, entity: str, key: str) -> Dict[str, Any]:
        """Full closed-window history of a state key."""
        return self.call("state_history", entity=entity, key=key)

    def audit(self) -> Dict[str, Any]:
        """Audit trail (spec §56) accumulated in this service session."""
        return self.call("audit")

    def retain(self, before: Optional[int] = None) -> Dict[str, Any]:
        """Drop WHOLE time-series chunks older than `before` (unix secs)."""
        params: Dict[str, Any] = {}
        if before is not None:
            params["before"] = before
        return self.call("retain", **params)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self, timeout: float = _CLOSE_TIMEOUT) -> None:
        """Close stdin, wait for a clean EOF exit, SIGKILL past the timeout."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            proc = self._proc
            try:
                if proc.stdin and not proc.stdin.closed:
                    proc.stdin.close()
            except (OSError, ValueError):
                pass
            try:
                proc.wait(timeout=timeout)
            except __import__("subprocess").TimeoutExpired:
                proc.kill()
                proc.wait(timeout=timeout)
            atexit.unregister(self)

    @property
    def pid(self) -> int:
        """OS pid of the spawned vidgedb subprocess."""
        return self._proc.pid

    def is_running(self) -> bool:
        """True while the subprocess has not exited."""
        return self._proc.poll() is None

    def __enter__(self) -> "VidgeDB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "open" if not self._closed else "closed"
        return f"VidgeDB({self.db_path!r}, bin={self.bin!r}, {state}, pid={self._proc.pid})"
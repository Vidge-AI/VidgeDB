# vidgedb (Python) — API reference

*Every signature below is the real one, copied from the shipped source. Return values
shown in examples are real engine output.*

---

## `class VidgeDB`

A running `vidgedb --service <db.vdg>` subprocess, spoken to over line-delimited
JSON-RPC 2.0.

### Constructor

```python
VidgeDB(
    db_path: str,
    bin: Optional[str] = None,
    agent_id: Optional[str] = None,
    retention_days: Optional[int] = None,
    timeout: float = ...,          # read timeout, seconds
    role: Optional[str] = None,    # "reader" | "writer" | "ingest"
)
```

| Parameter | Meaning |
|---|---|
| `db_path` | path to the `.vdg` file; created on first open |
| `bin` | engine binary; falls back to `$VIDGEDB_BIN`, then `vidgedb` on `PATH` |
| `agent_id` | the name that appears in the audit trail for every call |
| `retention_days` | run a retention pass at startup + hourly; **requires** `role="writer"` or `"ingest"` |
| `timeout` | read timeout in seconds |
| `role` | `reader` (default) refuses every write with `-32003`, audited; `writer`/`ingest` may write |

Raises `VidgeDBError` immediately for an unknown `role` (before spawning), and
`FileNotFoundError` if the binary cannot be found.

The subprocess is registered with `atexit`, so a forgotten `close()` still gets cleaned
up at interpreter exit. Using the context manager is preferred:

```python
with VidgeDB("twin.vdg", agent_id="x") as db:
    ...
```

### `call(method, **params) -> Any`

The generic dispatcher — **the contract**. Reaches any method the engine understands,
including ones this package version does not mirror.

```python
db.call("schema")
db.call("get_entity", key=0)
db.call("ingest_points", entity="Motor42", signal="current", points=[[1760000000, 8.2]])
```

Returns the `result` member as a plain `dict`/`list`/value. Raises `VidgeDBError` with
the real numeric code for a JSON-RPC `error` member, and with **code 0** when `result`
carries the semantic `{"error": "..."}` shape. See [errors.md](errors.md).

#### Methods added by newer binaries

The dispatcher is what makes version skew painless: an engine newer than this SDK is
reachable with no package update. The phase-93 inventory/diagnosis methods land this
way:

```python
db.call("list_entities")                       # every entity, no prior knowledge needed
db.call("list_entities", type="Motor")        # one family
db.call("graph")                              # whole graph in one round-trip (for a graph editor)
db.call("diagnose", entity="Motor42", **{"from": t0, "to": t1})   # 8-step report
```

Note the `**{"from": …}` form: `from` is a Python keyword, so it cannot be passed as
`from=t0`. This is exactly why the generic dispatcher — and not a typed helper — is the
official contract; a mirrored `graph(from=…)` signature is impossible in Python.

`list_entities` returns `{entities, total, entities_in_db, truncated}` — always check
`truncated` before treating the reply as the complete inventory. `graph` returns
`{entities, relations, relations_total, truncated}`, with relation endpoints given by
**name**. `diagnose` returns the 8-step report; an unknown entity yields `component:
null` plus empty lists rather than an error, so a UI shows "component not found"
instead of breaking.

### Typed helpers

| Method | Returns | Notes |
|---|---|---|
| `schema()` | `dict` | `entity_types`, `relation_topologies`, `series`, `provenance_classes` |
| `query(vql)` | `dict` | `{n, rows}` — graph queries |
| `query_temporal(vql, now=None)` | `dict` | `{n, rows}` — `MEASURE … DURING …` |
| `get_entity(key)` | `dict` | `name`, `type`, `properties`, `relations_in`, `relations_out` |
| `get_measurements(entity, signal, from_=0, to=…)` | `MeasurementSeries` | typed series |
| `check(entity, signal, from_=0, to=…)` | `CheckResult` | the deviation verdict |
| `trace(from_name, to_name, max_hops=6)` | `dict` | `{found, n_hops, path, steps}` |
| `log_event(name, entity, timestamp=None, provenance=1, details="")` | `dict` | appends one event |
| `get_state(entity, key, at=None)` | `Any` | `{value, valid_from}` or `None` |
| `state_history(entity, key)` | `dict` | every closed validity window |
| `audit()` | `dict` | `{entries, n}` for this service session |
| `retain(before=None)` | `dict` | drops whole chunks older than `before` |

Note the naming: `from_` carries a trailing underscore because `from` is a Python
keyword; the wire parameter is `from`. Same idea for `get_measurements` and `check`.

The engine also serves `provenance` and `get_events`; they have no typed helper in this
version — call them through `call()`:

```python
db.call("provenance")
db.call("get_events", entity="Motor42", **{"from": 1760000000, "to": 1760003600})
```

### Lifecycle

| Member | Behaviour |
|---|---|
| `close(timeout=…)` | closes stdin, waits for the clean EOF exit, `SIGKILL`s past the timeout; idempotent |
| `pid` | OS pid of the engine subprocess |
| `is_running()` | `True` while the subprocess has not exited |
| `__enter__` / `__exit__` | context manager; `__exit__` calls `close()` |
| `__getattr__` | unknown attribute names are forwarded to `call()` — `db.schema` works like `db.schema()`'s method lookup; useful for engine methods with no helper |

### Thread-safety

A single lock serialises *request + readline*, so several threads may share one
`VidgeDB` instance without interleaving a read with someone else's response. If you
need throughput rather than convenience, open one client per thread/process instead —
the engine itself is the bottleneck, not the lock.

---

## Typed results

### `CheckResult`

```python
@dataclass
class CheckResult:
    entity: str
    signal: str
    status: str                       # OK / VIOLATION / NO_DATA / NO_SPEC
    expected_max: Optional[float]
    observed: Optional[float]
    deviation: Optional[float]
    unit: Optional[str]
    expected_provenance: Optional[str]
    observed_provenance: Optional[str]
    points_checked: int
    window_from: int
    window_to: int
    raw: Dict[str, Any]
```

Real output: `VIOLATION 10.0 11.7 1.6999999999999993` for `status, expected_max,
observed, deviation`. Status semantics are in [protocol](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/protocol.md) §4.

The two provenance fields are not decoration: they let a report state that the expected
value is a `Specification` and the measurement an `Observation`.

### `MeasurementSeries` and `Measurement`

```python
@dataclass
class Measurement:
    t: int
    value: float

@dataclass
class MeasurementSeries:
    entity: str
    signal: str
    points: List[Measurement]
    count: int
    min: Optional[float]
    max: Optional[float]
    raw: Dict[str, Any]
```

`min`/`max` come from the engine's chunk metadata — they are exact, and they remain
exact after retention drops whole chunks.

### `CheckStatus`

The status strings as constants, to avoid stringly-typed comparisons in caller code.

### `DiagnosticReport`

Present in the package as the shape a diagnostic report takes (component, anomalies,
hypotheses, `source`). **Not produced by this client**: the engine does not expose the
diagnosis pipeline over JSON-RPC at `v0.1.0` — it exists only in the engine's Rust
library. Anything claiming otherwise would be a fiction.

---

## What this client does *not* do

- no storage, no caching, no schema mirroring — every call is a round-trip;
- no reconnection logic: if the engine dies, calls raise; reopen a client;
- no validation of your VQL before sending it — the engine answers with a parse error
  inside `result`, which `call()` surfaces;
- no hypotheses: there is no helper to write one, by design (see
  [architecture](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/architecture.md) §4).

# vidgedb (Python) — errors

*How failures surface, and what to do with each. Two error channels exist and confusing
them is the most common client-side mistake.*

---

## 1. One exception type, and the code carries the meaning

```python
class VidgeDBError(Exception):
    code: int        # the JSON-RPC error code, or 0 for a semantic error
    message: str
    data: Any        # whatever the response carried
```

Everything the client raises is a `VidgeDBError` — except `FileNotFoundError` from the
constructor when the engine binary cannot be located (which happens before any engine
exists to answer).

```python
from vidgedb import VidgeDB, VidgeDBError

try:
    with VidgeDB("twin.vdg", role="reader") as db:
        db.call("ingest_points", entity="Motor42", signal="current", points=[[1, 2.0]])
except VidgeDBError as e:
    print(e.code, e.message)
    # -32003 write forbidden (-32003): agent 'x' holds role Reader; ...
```

## 2. The two channels

| Origin | Shape on the wire | `VidgeDBError.code` |
|---|---|---|
| transport failure | `{"error": {"code": N, "message": "..."}}` | **N** (negative, real) |
| semantic failure | `{"result": {"error": "unknown entity 'X'"}}` | **0** |

Code `0` therefore does **not** mean "no error": it means *the call succeeded but the
operation could not be performed as asked*. Always inspect `code` and `message`
together.

## 3. Transport codes you will actually meet

| Code | Meaning | Typical trigger |
|---|---|---|
| `-32700` | parse error | malformed JSON on the wire (rare from a client — a bug in your payload) |
| `-32600` | invalid request | the request body was not an object |
| `-32601` | method not found | a typo, or a method this engine build does not have (e.g. `set_hypothesis`) |
| `-32602` | invalid params | missing `entity`; a **float** timestamp; an empty `points` array |
| `-32000` | storage error | engine-level failure surfaced by the method |
| `-32003` | **write forbidden** | a `reader` client called a write method |

`-32003` is the one to understand: it is not a defect, it is the role doing its job, and
**the refusal is recorded in the audit trail** (`db.audit()` shows it with
`params_summary: "REFUSED (role=Reader, write forbidden -32003)"`).

## 4. Semantic errors (code 0)

| Message | Means | Fix |
|---|---|---|
| `unknown entity 'X'` | the series/state/event target does not exist | create the entity first |
| `unknown entity key 0` | `get_entity(key)` with a key that is not live | list types, then query |
| `relation target 'X' does not exist — create it first` | a relation was sent before its target | two passes: entities, then relations |
| `self-relations are not supported` | `from == to` | model it differently |
| `relation_type 'X' must be 'topology:type'` | malformed relation type | use `electrical:feeds`, `network:profinet`, … |
| `merged properties exceed the inline cap (N > 44 bytes)` | the entity's props do not fit the cell | shorten names/values; move detail to telemetry |
| `parse error: …` | invalid VQL | fix the query; the message names the token |
| `unknown source 'X' (expected "plc" \| "agent" \| "sensor")` | bad `source` on upsert | use one of the three |
| `name and type must be non-empty` | empty `name`/`type` | — |

## 5. Lifecycle errors

| Message | When |
|---|---|
| `vidgedb process is not running: …` | you wrote to a dead child (broken pipe) |
| `client is closed` | a call after `close()` |
| `vidgedb process exited unexpectedly (rc=N)` | the engine died mid-call; check stderr and `is_running()` |
| `timeout after Ns waiting for "method"` | the engine did not answer in time |

## 6. What the client deliberately does *not* do

- **no retry.** A refused write is refused; retrying hides the real problem. Retry policy
  belongs to your program, where it can be reasoned about.
- **no exception swallowing.** A semantic error is raised like any other: a query that
  returns "unknown entity" is a bug in your model, not an empty result.
- **no reconnect.** If the engine died, the client stays dead — reopen it. A silent
  reconnect would hide an engine restart in the middle of a measurement window, which is
  exactly the kind of gap a twin must not paper over.

## 7. Debugging checklist

1. `e.code == -32003` → role, not a bug: reopen with `role="writer"`/`"ingest"`.
2. `e.code == -32601` → spelling, or a method this build does not serve.
3. `e.code == 0` → semantic: the message names the entity/property that is wrong. Read it.
4. `e.code == -32602` → params: most often a **float** timestamp instead of an integer.
5. The engine is not answering at all → check the binary first:
   `"$VIDGEDB_BIN" --version` and `"$VIDGEDB_BIN" smoke`.

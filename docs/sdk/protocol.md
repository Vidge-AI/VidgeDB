# Protocol — the JSON-RPC 2.0 contract shared by every client

*Everything here was observed on the real binary. Where behaviour is version-specific
it is flagged. This is the frozen contract: clients are forward-compatible because of
it.*

---

## 1. Framing (stdio)

The engine is started as a child process and speaks **one JSON object per line**:

```text
vidgedb --service <db.vdg> [--agent-id <id>] [--role reader|writer|ingest]
vidgedb --http    <db.vdg> [--port N] [--bind 127.0.0.1|0.0.0.0] [--http-token X]
```

- **in** — one request object per line, `\n`-terminated.
- **out** — one response object per line, `\n`-terminated.
- **stderr** — human-readable logs only (`vidgedb: served N requests`). Never parse it.
- A **notification** (a request with **no `id`**) gets **no response**, per JSON-RPC.
- A malformed line gets `-32700` **and the stream keeps listening** — one bad line does
  not kill the session.

Over HTTP the same methods are served at `POST /rpc`, one request object per body, one
response per reply. `--http-token X` requires `Authorization: Bearer X` on **every**
request (401 otherwise).

## 2. Reading and writing

```jsonc
// request
{"jsonrpc": "2.0", "id": 1, "method": "check",
 "params": {"entity": "Motor42", "signal": "current", "from": 1760000000, "to": 1760003600}}

// response
{"jsonrpc": "2.0", "id": 1, "result": {"status": "VIOLATION", ...}}
```

**Two error channels, and this is the part that trips people up:**

| Where | Shape | Meaning |
|---|---|---|
| transport `error` member | `{"error": {"code": -32601, "message": "..."}}` | the *call* failed: malformed JSON, unknown method, bad params, forbidden write, storage failure |
| semantic `result.error` | `{"result": {"error": "unknown entity 'X'"}}` | the call **succeeded** but the operation could not be done as asked |

A client must check **both**. The shipped Python and JS clients map transport errors to
a typed error carrying the numeric code, and semantic errors to the same typed error
with **code 0**. See the per-package `errors.md`.

## 3. Method set (v0.1.0)

Read — available to every role:

| Method | Params | Returns |
|---|---|---|
| `schema` | — | `entity_types`, `relation_topologies`, `series`, `provenance_classes` |
| `query` | `vql` | `{n, rows}` — VQL `MATCH … WHERE … RETURN …` |
| `query_temporal` | `vql`, `now?` | `{n, rows}` — VQL with `MEASURE … DURING …` |
| `get_entity` | `key` (number or integer string) | entity card with `relations_in` / `relations_out` |
| `get_measurements` | `entity`, `signal`, `from?`, `to?` | `{count, points[], min, max}` |
| `check` | `entity`, `signal`, `from?`, `to?` | deviation verdict (see §4) |
| `trace` | `from`, `to`, `max_hops?` | `{found, n_hops, path[], steps[]}` |
| `provenance` | — | the 8 provenance classes and per-relation provenance |
| `get_state` | `entity`, `key`, `at?` | `{value, valid_from}` or `null` |
| `state_history` | `entity`, `key` | every `[valid_from, valid_to)` window |
| `get_events` | `entity?`, `from?`, `to?` | `{events[], n}` |
| `audit` | — | `{entries[], n}` — every call, including refused writes |

Write — `writer` / `ingest` only, `reader` gets `-32003` **and the refusal is audited**:

| Method | Params |
|---|---|
| `ingest_points` | `entity`, `signal`, `points` — `[[ts, value], …]`, integer ts, numeric value |
| `upsert_entity` | `name`, `type`, `props?`, `relations?`, `source?` |
| `set_state` | `entity`, `key`, `value`, `at?` |
| `log_event` | `name`, `entity`, `timestamp?`, `provenance?`, `details?` |
| `retain` | `before?` — drops **whole** chunks older than the cutoff |

## 4. `check` — the verdict, and the semantics to know

```json
{"deviation": 1.6999999999999993, "entity": "Motor42", "expected_max": 10.0,
 "expected_provenance": "Specification", "observed": 11.7,
 "observed_provenance": "Observation", "points_checked": 6, "signal": "current",
 "status": "VIOLATION", "unit": null, "window": {"from": 1760000000, "to": 1760003600}}
```

| Status | Meaning |
|---|---|
| `OK` | observed max is within the spec |
| `VIOLATION` | observed max **exceeds** `spec.<signal>.max` — **strict**, so a transient overshoot cannot hide |
| `NO_DATA` | a spec exists but the window holds no points |
| `NO_SPEC` | no `spec.<signal>.max` property on the series owner |

The spec is read from the **entity properties of the series owner** — that is, for
series `Motor42.current`, the property `spec.current.max` on entity `Motor42`. A typo
in the property name silently yields `NO_SPEC`, not an error. Both provenances are
returned on purpose: a report can then cite where each number came from.

## 5. Provenance — 8 classes, engine-enforced

| Byte | Class | Enters the database how |
|---|---|---|
| 0 | `Fact` | declared `source: "plc"` |
| 1 | `Observation` | declared `source: "agent"` / `"sensor"`, or ingested telemetry |
| 2 | `Specification` | a spec property written explicitly by a human decision |
| 3 | `Inference` | derived by an explicit computation (not produced by the service in v0) |
| 4 | `Hypothesis` | **never writable by any role** — output-only |
| 5 | `Event` | `log_event` with the matching provenance |
| 6 | `Command` | a human/system action record |
| 7 | `Configuration` | a settings change |

There is no `source` value that produces `Hypothesis`, and no code path re-labels a
stored record. Clients therefore do not expose a "write a hypothesis" helper; see
[architecture.md](architecture.md) §4 for the honest state of the refusal path.

## 6. Traps worth knowing before you debug for an hour

1. **`MATCH (x) RETURN x` without a type returns `n: 0`** — silently, not as an error.
   To enumerate, iterate the types from `schema.entity_types`. (This is the single most
   confusing behaviour of the query surface.)
2. **Timestamps are integer unix seconds.** A float `ts` is refused with `-32602`.
3. **`get_measurements` and `check` are different questions.** The first returns points;
   the second compares them to a spec. Neither implies the other.
4. **Entity properties are capped at ~44 bytes inline.** The exact error names the
   numbers. Units do not fit next to two spec values — keep them in your own mapping.
5. **A relation target must exist first.** `upsert_entity` with a relation to an unknown
   name returns `"relation target 'X' does not exist — create it first"` inside
   `result`. Create entities first, then relations.
6. **One writer per `.vdg` file.** A second writer is refused with the pid of the holder;
   readers are always allowed. See [troubleshooting.md](troubleshooting.md) §3.
7. **A refused write is not a lost connection.** `-32003` means the role forbids it; the
   stream is healthy.

## 7. VQL in one screen

```sql
MATCH (m:Motor) WHERE m.vendor = "ABB" RETURN m
MATCH (p:Pump) -[:MECHANICAL]-> (m:Motor) -[:ELECTRICAL]-> (d:Drive) RETURN p, m, d
MATCH (x:Motor) AT 1760000000 RETURN x                      -- topology as it was then
MATCH (m:Motor) MEASURE m.current DURING last(24h) RETURN max(m.current), avg(m.current)
```

- the edge filter is the **topology class** (`mechanical`, `electrical`, `network`,
  `instrumentation`, `process`), which is part of the edge's identity — `electrical` is
  a different edge from `mechanical`;
- `WHERE` supports `= != < > <= >=`, `AND`, `OR` (AND binds tighter);
- aggregates are `max`, `min`, `avg`, `sum`, `count`;
- `DURING last(nH)`, `DURING last(nD)`, or an explicit `t1..t2`;
- parse errors come back as `{"error": "parse error: …"}` inside `result`, never a crash.

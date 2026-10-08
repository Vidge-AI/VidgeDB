# vidgedb (JavaScript / TypeScript) — API reference

*Signatures copied from the shipped source. Return values shown are real engine output.*

---

## `class VidgeDB`

A running `vidgedb --service <db.vdg>` subprocess speaking line-delimited JSON-RPC 2.0,
with a Promise queue for serialized round-trips.

### `VidgeDBOptions`

```ts
interface VidgeDBOptions {
  bin?: string;            // engine path; default $VIDGEDB_BIN, else "vidgedb" on PATH
  agentId?: string;        // identity recorded in the audit trail
  role?: VidgeRole;        // "reader" (default) | "writer" | "ingest"
  retentionDays?: number;  // startup + hourly retention pass (needs writer/ingest)
  timeoutMs?: number;      // per-call timeout, default 30000
}
```

### Opening and closing

| Member | Behaviour |
|---|---|
| `static open(dbPath, opts = {})` | spawns the engine, returns the client. **Throws** on an unknown `role`, and throws a `VidgeDBError` if the binary does not exist (fail fast, same intent as the Python `FileNotFoundError`) |
| `static async with(dbPath, opts, fn)` | opens, runs `fn(db)`, closes in a `finally` — the recommended shape |
| `await db.close(timeoutMs?)` | closes stdin, waits for the clean EOF exit, kills past the timeout |
| `db.isRunning()` | `true` while the subprocess has not exited |
| `db.pid` | OS pid (0 after exit) |
| `await db.dispose()` | close + release handlers |

```js
await VidgeDB.with("twin.vdg", { agentId: "x" }, async (db) => {
  console.log(await db.schema());
});
```

Properties exposed on the instance: `dbPath`, `bin`, `agentId`, `role`, `timeoutMs`.

### `call(method, params = {}) => Promise<any>`

The generic dispatcher — **the contract**. Reaches any method the engine understands,
including methods added after this package was published.

```js
await db.call("schema");
await db.call("get_entity", { key: 0 });
await db.call("ingest_points", { entity: "Motor42", signal: "current", points: [[1760000000, 8.2]] });
```

Transport errors reject with a `VidgeDBError` carrying the real numeric code; a
semantic `{error: ...}` inside `result` rejects with code **0**. See [errors.md](errors.md).

### Typed helpers

| Method | Returns | Notes |
|---|---|---|
| `schema()` | `Promise<Schema>` | `entityTypes`, `relationTopologies`, `series`, `provenanceClasses` |
| `query(vql)` | `Promise<QueryResult>` | `{n, rows}` |
| `queryTemporal(vql, now?)` | `Promise<QueryResult>` | `MEASURE … DURING …` |
| `getEntity(key)` | `Promise<EntityCard>` | accepts a number or an integer string |
| `getMeasurements(entity, signal, from = 0, to = …)` | `Promise<MeasurementSeries>` | raw points + `min`/`max` from chunk metadata |
| `check(entity, signal, from = 0, to = …)` | `Promise<CheckResult & {isViolation: boolean}>` | adds `isViolation` for convenience |
| `trace(fromName, toName, maxHops = 6)` | `Promise<TraceResult>` | out-edges only |
| `provenance()` | `Promise<any>` | the 8 classes and per-relation provenance (the Python package reaches this via `call`) |
| `getState(entity, key, at?)` | `Promise<any>` | `{value, valid_from}` or `null` |
| `stateHistory(entity, key)` | `Promise<any>` | `{history, n}` |
| `ingestPoints(entity, signal, points)` | `Promise<IngestResult>` | accepts `[ts, value]`, `{ts, value}` or `{t, value}` and normalizes |
| `upsertEntity(name, type, props = {}, relations = [], source = "plc")` | `Promise<UpsertResult>` | `{key, created, relations_added}` |
| `setState(entity, key, value, at?)` | `Promise<SetStateResult>` | closes the previous validity window |
| `logEvent(name, entity, timestamp?, provenance = 1, details = "")` | `Promise<LogEventResult>` | timestamp defaults to now (seconds) |
| `getEvents(entity?, from?, to?)` | `Promise<{events, n}>` | inclusive window |
| `audit()` | `Promise<{entries, n}>` | every call of the session, refusals included |
| `retain(before?)` | `Promise<RetainResult>` | whole chunks older than `before` |

Note the naming difference from Python: JavaScript has no `from` keyword problem, so the
parameters are literally `from`/`to` — and `ingestPoints` is more forgiving about the
point shape than the Python helper.

---

## Typed results (`src/types.ts`)

`Measurement`, `MeasurementSeries`, `QueryResult`, `AggCell`, `CheckResult`,
`TraceStep`, `TraceResult`, `EntityCard`, `Schema`, `IngestResult`, `UpsertResult`,
`SetStateResult`, `StateEntry`, `LogEventResult`, `EventEntry`, `AuditEntry`,
`RetainResult`, `RelationSpec`, `DiagnosticReport`, and the union
`CheckStatus = "OK" | "VIOLATION" | "NO_DATA" | "NO_SPEC"`.

`CheckResult` mirrors the wire response and adds `isViolation`. The two provenance
fields (`expectedProvenance`, `observedProvenance`) are what let a report state where
each number came from — a `Specification` compared against an `Observation`.

`DiagnosticReport` describes the shape a diagnostic report takes (component, anomalies,
hypotheses). **This client does not produce one**: the engine does not expose the
diagnosis pipeline over JSON-RPC at `v0.1.0`.

---

## Concurrency, in one rule

Calls are queued: **await everything**. A floating promise (no `await`) still executes —
in order — but its rejection has nobody to land on. If you need real parallelism, open
one client per machine rather than sharing one across parallel tasks: the engine is one
process, and the queue is there to keep request/response pairing correct, not to make it
a thread pool.

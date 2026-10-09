# vidgedb (JavaScript / TypeScript) — errors

---

## 1. One error class, with the code

```ts
class VidgeDBError extends Error {
  code: number;      // the JSON-RPC error code, or 0 for a semantic error
  data: any;
}
```

`VidgeDB.open()` also throws this class for a bad `role` or a missing binary — so a
`try/catch` around the open call is worth having.

```js
import { VidgeDB, VidgeDBError } from "@vidge-ai/vidgedb";

try {
  const db = VidgeDB.open("twin.vdg", { role: "reader" });
  await db.call("ingest_points", { entity: "M", signal: "s", points: [[1, 2.0]] });
} catch (e) {
  if (e instanceof VidgeDBError) console.error(e.code, e.message);
  else throw e;
}
```

## 2. The two channels — the trap

| Origin | Shape on the wire | `e.code` |
|---|---|---|
| transport failure | `{ error: { code: N, message } }` | **N** (negative, real) |
| semantic failure | `{ result: { error: "unknown entity 'X'" } }` | **0** |

Code `0` means *the call succeeded, the operation did not*. Never treat `0` as success.

## 3. Codes you will meet

| Code | Meaning | Trigger |
|---|---|---|
| `-32700` | parse error | malformed JSON (a bug in your payload) |
| `-32600` | invalid request | not an object |
| `-32601` | method not found | typo, or a method this engine build does not serve |
| `-32602` | invalid params | missing `entity`, a **float** timestamp, empty `points` |
| `-32000` | storage error | engine-level failure |
| `-32003` | write forbidden | a `reader` client called a write method — **audited** |
| `0` | semantic / client-side | `unknown entity`, parse error in VQL, unknown role, binary not found |

## 4. Timeouts are client-side, and they do not kill the engine

```text
timeout after 30000ms waiting for "check"
```

The pending promise is rejected with code `0`. The engine keeps running and its
response, if it ever arrives, is discarded. Raise `timeoutMs` if a window query over a
large series legitimately takes longer; do not retry blind (see §6).

## 5. Semantic messages (code 0) and their fixes

| Message | Fix |
|---|---|
| `unknown entity 'X'` | create the entity first |
| `relation target 'X' does not exist — create it first` | two passes: entities, then relations |
| `self-relations are not supported` | `from === to` |
| `relation_type 'X' must be 'topology:type'` | use `electrical:feeds`, `network:profinet`, … |
| `merged properties exceed the inline cap (N > 44 bytes)` | shorten props; move detail to telemetry |
| `parse error: …` | invalid VQL; the message names the token |
| `unknown source 'X'` | `plc` \| `agent` \| `sensor` |
| `vidgedb binary not found: …` | set `VIDGEDB_BIN` or pass `bin` |

## 6. What this client deliberately does not do

- **no automatic retry** — a refused write is refused; retry policy belongs to your code;
- **no reconnection** — if the engine dies, the client stays dead; open a new one. A
  silent reconnect would hide an engine restart inside a measurement window;
- **no error swallowing** — a semantic error rejects the promise like any other.

## 7. Debugging checklist

1. `e.code === -32003` → reopen with `role: "writer"` or `"ingest"`.
2. `e.code === -32601` → spelling, or the method does not exist in this build.
3. `e.code === 0` → read the message: it names the entity or property at fault.
4. `e.code === -32602` → params; most often `Math.floor(Date.now()/1000)` was forgotten.
5. Nothing answers at all → check the engine: `"$VIDGEDB_BIN" --version`,
   `"$VIDGEDB_BIN" smoke`.

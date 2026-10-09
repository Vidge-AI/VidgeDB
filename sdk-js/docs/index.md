# vidgedb (JavaScript / TypeScript) — index

The Node client for **VidgeDB**: spawn the `vidgedb` engine as a subprocess and drive it
with JSON-RPC 2.0. **Zero runtime dependencies** — `node:child_process` and
`node:readline` only. Node ≥ 18, shipped as ESM **and** CJS with TypeScript types.

```js
import { VidgeDB } from "@vidge-ai/vidgedb"; // const { VidgeDB } = require("@vidge-ai/vidgedb");

const w = VidgeDB.open("twin.vdg", { agentId: "setup", role: "ingest" });
console.log(await w.upsertEntity("Motor42", "Motor",
  { "spec.current.max": "10" }, [], "plc"));
console.log(await w.ingestPoints("Motor42", "current",
  [[1760000000, 8.2], [1760001200, 11.7]]));
await w.close();

const db = VidgeDB.open("twin.vdg", { agentId: "js-agent" });   // reader by default
const rep = await db.check("Motor42", "current", 1760000000, 1760003600);
console.log(rep.status, rep.isViolation);      // VIOLATION true
await db.close();
```

## Install

```bash
npm install @vidge-ai/vidgedb
```

To build from source instead of npm: `cd sdk-js && npm install && npm run build`.
See [installation](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/installation.md) §2.

The client also needs the **engine binary**, resolved as: `opts.bin` → `$VIDGEDB_BIN` →
`vidgedb` on `PATH`. Unlike Python (which raises at spawn), `VidgeDB.open` checks the
path up-front and throws a `VidgeDBError` with a readable message if it is missing.

## Documentation

| File | Contents |
|---|---|
| [api.md](api.md) | classes, options, every method, the typed results |
| [errors.md](errors.md) | `VidgeDBError`, code semantics, the promise queue and timeouts |
| [examples.md](examples.md) | runnable snippets: setup, streaming ingest, watchdog, Express bridge |

Shared documentation: [architecture](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/architecture.md) ·
[protocol](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/protocol.md) · [recipes](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/recipes.md) ·
[troubleshooting](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/troubleshooting.md).

## Design in one paragraph

`VidgeDB.open()` spawns the engine and returns a client whose calls are **serialised
through a promise queue** — interleaved `await`s never interleave a request with
somebody else's response. Three layers: the **transport**, a **generic dispatcher**
(`db.call(method, params)`) that reaches any engine method including ones added after
this package was published, and **typed helpers**. The dispatcher is the contract; the
helpers are convenience. Nothing here stores data: the truth is in the `.vdg` file.

## Tests

```bash
export VIDGEDB_BIN=/path/to/vidgedb
cd js && npm install && npm test        # 32 passed (builds first)
```

No mock: the suite drives the real binary.

# vidgedb — JavaScript / TypeScript client

Node client for **VidgeDB**, the embedded temporal graph database for digital twins of
industrial machines. Zero runtime dependencies, ESM + CJS, TypeScript types. Node ≥ 18.

```js
import { VidgeDB } from "@vidge-ai/vidgedb"; // const { VidgeDB } = require("@vidge-ai/vidgedb");

const w = VidgeDB.open("twin.vdg", { agentId: "setup", role: "ingest" });
await w.upsertEntity("Motor42", "Motor", { "spec.current.max": "10" }, [], "plc");
await w.ingestPoints("Motor42", "current", [[1760000000, 8.2], [1760001200, 11.7]]);
await w.close();

const db = VidgeDB.open("twin.vdg", { agentId: "js-agent" });   // reader by default
const rep = await db.check("Motor42", "current", 1760000000, 1760003600);
console.log(rep.status, rep.isViolation);      // VIOLATION true
await db.close();
```

## Requirements

This package **drives an engine** — it does not store anything. You also need the
`vidgedb` binary, resolved as: `opts.bin` → `$VIDGEDB_BIN` → `vidgedb` on `PATH`.
`VidgeDB.open()` fails fast with a readable error if it cannot find it.

```bash
export VIDGEDB_BIN=/path/to/vidgedb
"$VIDGEDB_BIN" --version      # vidgedb 0.1.0
```

Getting the engine: <https://github.com/Vidge-AI/VidgeDB>.

## Documentation

- [docs/index.md](docs/index.md) — overview and install
- [docs/api.md](docs/api.md) — options, methods, typed results
- [docs/errors.md](docs/errors.md) — `VidgeDBError`, codes, timeouts
- [docs/examples.md](docs/examples.md) — snippets, including an Express bridge

Shared (cross-language) documentation lives in [`../../docs/sdk/`](../../docs/sdk/) —
architecture, wire protocol, installation, recipes, troubleshooting.

## Tests

```bash
export VIDGEDB_BIN=/path/to/vidgedb
npm install && npm test        # 32 passed (builds first)
```

No mock: the suite drives the real binary.

## Licence

Apache-2.0.

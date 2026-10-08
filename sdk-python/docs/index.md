# vidgedb (Python) — index

The Python client for **VidgeDB**: spawn the `vidgedb` engine as a subprocess and drive
it with JSON-RPC 2.0. **Zero runtime dependencies** — `subprocess`, `json`, `dataclasses`
from the standard library only. Python ≥ 3.9, including Raspberry Pi.

```python
from vidgedb import VidgeDB

with VidgeDB("twin.vdg", agent_id="setup", role="ingest") as db:
    db.upsert_entity(name="Motor42", type="Motor",
                     props={"spec.current.max": "10"}, source="plc")
    db.call("ingest_points", entity="Motor42", signal="current",
            points=[[1760000000, 8.2], [1760001200, 11.7]])

with VidgeDB("twin.vdg", agent_id="diag") as db:          # reader by default
    rep = db.check("Motor42", "current", from_=1760000000, to=1760003600)
    print(rep.status, rep.expected_max, rep.observed, rep.deviation)
    # VIOLATION 10.0 11.7 1.6999999999999993
```

## Install

```bash
pip install vidgedb
```

Not on PyPI yet at `v0.1.0` — from the repository: `cd python && pip install -e .`
See [../../docs/installation.md](../../docs/installation.md) §2.

The client also needs the **engine binary**, resolved as: explicit `bin=` →
`$VIDGEDB_BIN` → `vidgedb` on `PATH`. Without it, construction raises
`FileNotFoundError`. See [../../docs/installation.md](../../docs/installation.md) §1-3.

## Documentation

| File | Contents |
|---|---|
| [api.md](api.md) | every class, method and parameter, with real return values |
| [errors.md](errors.md) | `VidgeDBError`, the code semantics, the two error channels |
| [examples.md](examples.md) | runnable snippets: setup, ingest loop, watchdog, retention |

Shared documentation: [architecture](../../docs/architecture.md) ·
[protocol](../../docs/protocol.md) · [recipes](../../docs/recipes.md) ·
[troubleshooting](../../docs/troubleshooting.md).

## Design in one paragraph

`VidgeDB` starts the engine, serialises every request under a lock (so the class is
thread-safe: two threads never interleave a read), and correlates responses by `id`.
Three layers: the **transport**, a **generic dispatcher** (`db.call(method, **params)`)
that reaches *any* engine method — including ones added after this package was
published — and **typed helpers** for the methods a human calls. The dispatcher is the
contract; the helpers are convenience. Nothing here stores data: the truth is in the
`.vdg` file, held by the engine.

## Tests

```bash
export VIDGEDB_BIN=/path/to/vidgedb
cd python && python -m pytest -q      # 35 passed
```

No mock: the suite drives the real binary.

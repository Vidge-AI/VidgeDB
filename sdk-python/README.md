# vidgedb — Python client

Python client for **VidgeDB**, the embedded temporal graph database for digital twins of
industrial machines. Zero runtime dependencies (standard library only), Python ≥ 3.9.

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

## Requirements

This package **drives an engine** — it does not store anything. You also need the
`vidgedb` binary, resolved as: explicit `bin=` → `$VIDGEDB_BIN` → `vidgedb` on `PATH`.

```bash
export VIDGEDB_BIN=/path/to/vidgedb
"$VIDGEDB_BIN" --version      # vidgedb 0.1.0
```

Getting the engine: <https://github.com/Vidge-AI/VidgeDB> (build, container, or a
release artifact).

## Documentation

Full documentation lives in the repository, next to the code:

- [docs/index.md](docs/index.md) — overview and install
- [docs/api.md](docs/api.md) — every class, method and parameter
- [docs/errors.md](docs/errors.md) — `VidgeDBError`, codes, the two error channels
- [docs/examples.md](docs/examples.md) — runnable snippets

Shared (cross-language) documentation lives in [`../../docs/sdk/`](../../docs/sdk/) —
architecture, wire protocol, installation, recipes, troubleshooting.

## Tests

```bash
export VIDGEDB_BIN=/path/to/vidgedb
python -m pytest -q        # 35 passed
```

No mock: the suite drives the real binary.

## Licence

Apache-2.0.

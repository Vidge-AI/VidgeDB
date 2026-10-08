# Recipes — end-to-end tasks

*Each recipe is a complete, runnable script. They use the Python client; the JS
equivalent is given for the two most common ones. Every output shown was produced by
the real engine.*

---

## Recipe 1 — Set up a twin (write once)

```python
from vidgedb import VidgeDB

with VidgeDB("twin.vdg", agent_id="setup", role="ingest") as db:
    db.upsert_entity(name="PLC01", type="PLC",
                     props={"vendor": "Siemens"}, source="plc")
    db.upsert_entity(name="Drive12", type="Drive", props={"vendor": "ABB"},
                     relations=[{"to": "PLC01", "relation_type": "network:profinet"}],
                     source="plc")
    db.upsert_entity(name="Motor42", type="Motor",
                     props={"vendor": "ABB", "spec.current.max": "10"},
                     relations=[{"to": "Drive12", "relation_type": "electrical:feeds"}],
                     source="plc")
    db.set_state(entity="Motor42", key="running", value="yes")
```

Order matters: **entities before relations**. A relation whose target does not exist is
refused (and, in the shipped engine, leaves the session in a bad state — see
[troubleshooting.md](troubleshooting.md) §4).

## Recipe 2 — Ingest a telemetry batch

```python
import time
with VidgeDB("twin.vdg", agent_id="plc-bridge", role="ingest") as db:
    pts = [[int(time.time()), 8.2], [int(time.time()) + 60, 9.1]]
    print(db.call("ingest_points", entity="Motor42", signal="current", points=pts))
    # {'accepted': 2, 'chunks_flushed': 1, 'series_id': 0}
```

One call = **one transaction** = one fsync, whatever the batch size; a chunk is flushed
every 256 points, and the series is created on the fly if it does not exist.

*JS equivalent*

```js
const w = VidgeDB.open("twin.vdg", { agentId: "plc-bridge", role: "ingest" });
console.log(await w.ingestPoints("Motor42", "current", [[ts, 8.2], [ts + 60, 9.1]]));
await w.close();
```

## Recipe 3 — Alert on a deviation (the core use case)

```python
from vidgedb import VidgeDB

with VidgeDB("twin.vdg", agent_id="watchdog") as db:        # reader is enough
    rep = db.check("Motor42", "current", from_=1760000000, to=1760003600)
    print(rep.status, rep.observed, rep.expected_max, rep.deviation)
    if rep.status == "VIOLATION":
        print(f"ALARM: {rep.observed} > {rep.expected_max} "
              f"(observed={rep.observed_provenance}, expected={rep.expected_provenance})")
```

Real output: `VIOLATION 11.7 10.0 1.6999999999999993`.

The four statuses and what to do about each:

| Status | Action |
|---|---|
| `OK` | nothing |
| `VIOLATION` | alert; the deviation is signed (positive = over the spec) |
| `NO_DATA` | the spec exists, the window is empty → check your ingestion, not the machine |
| `NO_SPEC` | the property `spec.<signal>.max` is missing on the series owner → a modelling gap |

## Recipe 4 — Loop over a fleet, one process, many machines

```python
from vidgedb import VidgeDB

MACHINES = ["press01.vdg", "conv02.vdg", "robot03.vdg"]
with VidgeDB("press01.vdg", agent_id="fleet") as _:      # only to show the pattern
    pass

for path in MACHINES:
    with VidgeDB(path, agent_id="fleet") as db:
        schema = db.schema()
        for etype in schema["entity_types"]:            # never `MATCH (x) RETURN x`
            rows = db.query(f'MATCH (x:{etype}) RETURN x')["rows"]
            ...
```

Two traps this recipe exists to encode: **`MATCH (x) RETURN x` without a type returns
0 rows**, and **one `.vdg` file has one writer** — so a fleet loop opens each machine
in turn, or runs one ingest process per machine and a reader elsewhere.

## Recipe 5 — Trace a symptom upstream

```python
with VidgeDB("conv02.vdg", agent_id="diag") as db:
    t = db.trace("EMOT01", "PLC01", max_hops=6)
    print(t["found"], t["n_hops"])
    for step in t["steps"]:
        print(f'  {step["from"]} -[{step["topology"]}:{step["relation_type"]}]- '
              f'{step["to"]}  ({step["provenance"]})')
```

The path is bounded by `max_hops` and each step carries its topology class and
provenance, so a report can state *how* two things are connected, not just that they are.

## Recipe 6 — History of a value, and of a state

```python
with VidgeDB("twin.vdg", agent_id="hist") as db:
    s = db.get_measurements("Motor42", "current", from_=1760000000, to_=1760003600)
    print(s.count, s.min, s.max)                 # 6 8.2 11.7

    print(db.state_history("Motor42", "running"))  # every [valid_from, valid_to) window
    print(db.get_state("Motor42", "running"))      # the one valid now
```

Every `set_state` closes the previous window; nothing is overwritten. The same is true
of relations (a deleted edge is a validity window that was closed, not a row that
vanished).

## Recipe 7 — What happened, and who did it

```python
with VidgeDB("twin.vdg", agent_id="post-mortem") as db:
    for ev in db.get_events(from_=1760000000, to_=1760003600)["events"]:
        print(ev["timestamp"], ev["name"], ev["details"], ev["provenance"])

    for entry in db.audit()["entries"]:
        print(entry["timestamp"], entry["agent_id"], entry["method"], entry["params_summary"])
```

The audit trail records **every** call of the service session, including refused writes
— that is how you prove an agent stayed within its mandate.

## Recipe 8 — Retention (keep a Pi alive)

```python
with VidgeDB("twin.vdg", agent_id="housekeeper", role="writer") as db:
    print(db.retain(before=1760000000))     # drops WHOLE chunks older than this
```

Retention is chunk-granular by design: statistics that live in chunk metadata stay
exact, and nothing partial is ever deleted. The CLI equivalent runs it hourly:
`vidgedb --service twin.vdg --role ingest --retention-days 30`.

## Recipe 9 — Raw dispatch, for a method the client does not know

```python
with VidgeDB("twin.vdg", agent_id="explorer") as db:
    print(db.call("schema"))
    print(db.call("get_entity", key=0))
    print(db.call("provenance"))
```

`call()` reaches **any** method the engine serves. This is the escape hatch that makes
these clients forward-compatible: a new engine method is usable the day it ships,
without waiting for a client release.

# vidgedb (Python) — examples

*Runnable snippets, in the order you would use them. Shared background:
[recipes](../../docs/recipes.md) (cross-language), [protocol](../../docs/protocol.md).*

---

## 1. Configure the engine path once

```python
import os
os.environ.setdefault("VIDGEDB_BIN", "/opt/vidgeDB/target/release/vidgedb")

from vidgedb import VidgeDB
```

Or pass it explicitly, which is clearer in a service:

```python
db = VidgeDB("twin.vdg", bin="/opt/vidgeDB/target/release/vidgedb", agent_id="bridge")
```

## 2. Build a twin (entities first, then relations)

```python
with VidgeDB("twin.vdg", agent_id="setup", role="ingest") as db:
    db.upsert_entity(name="PLC01",   type="PLC",   props={"vendor": "Siemens"}, source="plc")
    db.upsert_entity(name="Drive12", type="Drive", props={"vendor": "ABB"},
                     relations=[{"to": "PLC01", "relation_type": "network:profinet"}],
                     source="plc")
    db.upsert_entity(name="Motor42", type="Motor",
                     props={"vendor": "ABB", "spec.current.max": "10"},
                     relations=[{"to": "Drive12", "relation_type": "electrical:feeds"}],
                     source="plc")
    db.set_state(entity="Motor42", key="running", value="yes",
                 at=1760000000)
```

`upsert_entity` is idempotent: running this twice does not duplicate anything, and
re-sending a relation that already exists is a no-op (`relations_added: 0`).

## 3. A polling loop that only writes measurements

```python
import time
from vidgedb import VidgeDB

with VidgeDB("twin.vdg", agent_id="bridge", role="ingest") as db:
    while True:
        ts = int(time.time())                     # integer seconds, always
        value = read_from_the_machine()           # your Modbus/OPC-UA/S7 read
        db.call("ingest_points", entity="Motor42", signal="current",
                points=[[ts, value]])
        time.sleep(5)
```

One call = one transaction = one fsync. Batching is cheaper: accumulate and send 100
points in a single call rather than 100 calls.

## 4. A watchdog that only reads

```python
from vidgedb import VidgeDB

with VidgeDB("twin.vdg", agent_id="watchdog") as db:          # reader: enough
    rep = db.check("Motor42", "current", from_=1760000000, to=1760003600)
    if rep.status == "VIOLATION":
        print(f"{rep.observed} > {rep.expected_max} "
              f"(+{rep.deviation:.2f}) [{rep.observed_provenance} vs {rep.expected_provenance}]")
    elif rep.status == "NO_SPEC":
        print("modelling gap: spec.current.max is missing on Motor42")
    elif rep.status == "NO_DATA":
        print("no points in the window — check ingestion, not the machine")
```

## 5. Read a series and compute something of your own

```python
with VidgeDB("twin.vdg", agent_id="analytics") as db:
    s = db.get_measurements("Motor42", "current", from_=1760000000, to=1760003600)
    print(s.count, s.min, s.max)                    # 6 8.2 11.7
    over = [p for p in s.points if p.value > 10.0]
    print(len(over), "points above spec")
```

`min`/`max` come from chunk metadata: exact, and unaffected by retention.

## 6. Aggregates over a window, in the engine rather than in Python

```python
with VidgeDB("twin.vdg", agent_id="analytics") as db:
    rows = db.query_temporal(
        'MATCH (m:Motor) WHERE m.name = "Motor42" '
        'MEASURE m.current DURING last(24h) '
        'RETURN max(m.current), avg(m.current), count(m.current)'
    )["rows"][0]
    print(rows["max"]["value"], rows["avg"]["value"], rows["count"]["value"])
```

## 7. Enumerate the twin — the trap and the right way

```python
with VidgeDB("twin.vdg", agent_id="inventory") as db:
    schema = db.schema()
    for etype in schema["entity_types"]:            # NOT `MATCH (x) RETURN x`
        for row in db.query(f'MATCH (x:{etype}) RETURN x')["rows"]:
            e = row["x"]
            print(e["key"], e["name"], e["type"], e["properties"])
```

`MATCH (x) RETURN x` without a type returns **0 rows silently**. Iterate the types.

## 8. Who did what

```python
with VidgeDB("twin.vdg", agent_id="post-mortem") as db:
    for entry in db.audit()["entries"]:
        print(entry["timestamp"], entry["agent_id"], entry["method"], entry["params_summary"])
```

Refused writes appear with `params_summary: "REFUSED (role=Reader, write forbidden -32003)"`
— that is how you prove an agent stayed within its mandate.

## 9. Retention on a long-lived box

```python
with VidgeDB("twin.vdg", agent_id="housekeeper", role="writer") as db:
    print(db.retain(before=int(time.time()) - 30 * 86400))   # keep 30 days
```

Whole chunks only — statistics in chunk metadata stay exact, nothing partial is deleted.
For a Pi, prefer the CLI flag so it runs hourly without your program:
`vidgedb --service twin.vdg --role ingest --retention-days 30`.

## 10. Two clients, one machine (reader while a writer runs)

```python
writer = VidgeDB("twin.vdg", agent_id="bridge", role="ingest")
reader = VidgeDB("twin.vdg", agent_id="dashboard")        # reader is allowed
writer.call("ingest_points", entity="Motor42", signal="current", points=[[1760000700, 9.4]])
print(reader.check("Motor42", "current", from_=1760000000, to=1760003600).status)
reader.close(); writer.close()
```

Two *writers* on the same file are refused at open — one writer per `.vdg`, by design.

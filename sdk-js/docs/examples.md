# vidgedb (JavaScript / TypeScript) — examples

*Runnable snippets, from setup to an HTTP bridge. Shared background:
[recipes](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/recipes.md).*

---

## 0. Configure the engine path

```bash
export VIDGEDB_BIN=/opt/vidgeDB/target/release/vidgedb
```

```js
import { VidgeDB } from "vidgedb";
const db = VidgeDB.open("twin.vdg", { bin: process.env.VIDGEDB_BIN, agentId: "bridge" });
```

## 1. Build a twin, then read it

```js
import { VidgeDB } from "vidgedb";

await VidgeDB.with("twin.vdg", { agentId: "setup", role: "ingest" }, async (db) => {
  await db.upsertEntity("PLC01", "PLC", { vendor: "Siemens" }, [], "plc");
  await db.upsertEntity("Drive12", "Drive", { vendor: "ABB" },
    [{ to: "PLC01", relation_type: "network:profinet" }], "plc");
  await db.upsertEntity("Motor42", "Motor",
    { vendor: "ABB", "spec.current.max": "10" },
    [{ to: "Drive12", relation_type: "electrical:feeds" }], "plc");
  await db.setState("Motor42", "running", "yes");
});

await VidgeDB.with("twin.vdg", { agentId: "reader" }, async (db) => {
  console.log(await db.schema());
  console.log(await db.query('MATCH (m:Motor) RETURN m'));
});
```

## 2. Streaming ingest from anything that produces a number

```js
const db = VidgeDB.open("twin.vdg", { agentId: "bridge", role: "ingest" });

// MQTT, serial port, fetch() — anything:
mqttClient.on("message", async (topic, payload) => {
  const ts = Math.floor(Date.now() / 1000);          // integer seconds
  const value = Number(payload.toString());
  try {
    await db.ingestPoints("Motor42", "current", [[ts, value]]);
  } catch (e) {
    console.error("ingest failed", e.code, e.message);
  }
});
```

One call = one transaction. Batch when you can: 100 points in one call beats 100 calls.

## 3. A watchdog with a clean shutdown

```js
const db = VidgeDB.open("twin.vdg", { agentId: "watchdog" });   // reader

setInterval(async () => {
  const now = Math.floor(Date.now() / 1000);
  const rep = await db.check("Motor42", "current", now - 3600, now);
  if (rep.isViolation) {
    console.log(`ALARM ${rep.observed} > ${rep.expectedMax} (+${rep.deviation.toFixed(2)})`);
    console.log(`  observed=${rep.observedProvenance} expected=${rep.expectedProvenance}`);
  }
}, 10_000);

process.on("SIGINT", async () => { await db.close(); process.exit(0); });
```

`rep.status` is one of `OK` / `VIOLATION` / `NO_DATA` / `NO_SPEC`; `isViolation` is the
shortcut for the case you alert on.

## 4. Enumerate a twin (and the trap to avoid)

```js
const schema = await db.schema();
for (const t of schema.entityTypes) {                  // NOT `MATCH (x) RETURN x`
  const { rows } = await db.query(`MATCH (x:${t}) RETURN x`);
  for (const row of rows) console.log(row.x.key, row.x.name, row.x.properties);
}
```

A pattern with no type label returns `n: 0` **silently**. Iterate the types.

## 5. Aggregate in the engine, not in JS

```js
const { vql } = { vql:
  'MATCH (m:Motor) WHERE m.name = "Motor42" ' +
  'MEASURE m.current DURING last(24h) RETURN max(m.current), avg(m.current)' };
const rows = (await db.queryTemporal(vql)).rows;
console.log(rows[0].max.value, rows[0].avg.value);
```

## 6. Bridge to the web (Express), without exposing the engine

```js
import express from "express";
import { VidgeDB } from "vidgedb";

const app = express();
const db = VidgeDB.open("/data/twin.vdg", { agentId: "web" });   // ONE engine instance

app.get("/api/status", async (_req, res) => {
  const now = Math.floor(Date.now() / 1000);
  res.json(await db.check("Motor42", "current", now - 3600, now));
});
app.get("/api/entities", async (_req, res) => {
  const schema = await db.schema();
  const out = [];
  for (const t of schema.entityTypes) {
    const { rows } = await db.query(`MATCH (x:${t}) RETURN x`);
    out.push(...rows.map((r) => r.x));
  }
  res.json(out);
});
app.listen(3000);
```

The browser never talks to the engine: the Node process owns the subprocess and the
promise queue, and the HTTP layer is yours to authenticate. Do not proxy raw JSON-RPC
to the internet — the engine has no network authentication of its own at `v0.1.0`.

## 7. Trace a fault

```js
const t = await db.trace("EMOT01", "PLC01", 6);
if (t.found) {
  for (const s of t.steps) {
    console.log(`${s.from} -[${s.topology}:${s.relation_type}]- ${s.to} (${s.provenance})`);
  }
}
```

## 8. Raw dispatch for anything not wrapped

```js
console.log(await db.call("provenance"));
console.log(await db.call("get_events", { entity: "Motor42", from: 1760000000, to: 1760003600 }));
console.log(await db.call("get_entity", { key: "0" }));     // integer string accepted
```

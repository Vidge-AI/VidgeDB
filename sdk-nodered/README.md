# @vidge-ai/node-red-contrib-vidgedb

Node-RED nodes for **VidgeDB** — the embedded temporal graph database for digital twins
of industrial machines. Wire a machine's telemetry into a twin, query it, and alert on
deviations, without writing a program.

> **This is the only document for this package.** The Python and JavaScript clients ship
> a multi-file `docs/` directory; the Node-RED package is a visual component, so one
> file — this one — covers it. Shared background lives in
> [architecture](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/architecture.md) and
> [protocol](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/protocol.md).

---

## 1. Install

```bash
cd ~/.node-red
npm install @vidge-ai/node-red-contrib-vidgedb
```

Install it from npm, then restart Node-RED. The package declares `node-red.nodes` in its
`package.json`, which is what makes the palette entry appear.

The nodes need the **engine binary**: the config node's `binaryPath` field, or
`$VIDGEDB_BIN`, or `vidgedb` on `PATH`. See
[installation](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/installation.md) §1.

## 2. The config node — `vidgedb-service`

This node owns **one engine subprocess** and every other node points at it. That is the
important design decision: five telemetry streams do not spawn five engines.

| Field | Default | Meaning |
|---|---|---|
| `binaryPath` | `/usr/local/bin/vidgedb` | engine binary (**required**) |
| `dbPath` | — | the `.vdg` file (**required**); created on first open |
| `role` | `reader` | `reader` \| `writer` \| `ingest` |
| `agentId` | `node-red` | the name recorded in the audit trail |
| `retentionDays` | `0` (off) | startup + hourly retention pass; **needs `writer`/`ingest`** |
| `timeoutMs` | `30000` | per-call timeout |
| `autoRespawn` | on | restart the engine automatically after a crash |

It shows a **status dot**: green when the engine is up, a red ring while it is down. It
respawns 2.5 s after an unexpected exit. Note that a `reader` service refuses every write
— the visual symptom is a red "write forbidden" status on the ingest and log-event nodes,
which is why the flow below uses `ingest` on the config node and keeps the *reading* nodes
read-only by nature.

## 3. The five nodes

| Node | Input | Does | Output |
|---|---|---|---|
| `vidgedb-query` | `msg.payload.vql` (or `msg.vql`) | runs a VQL query | `msg.payload` = the result; `msg.payload.output` selects what is forwarded (`rows` by default) |
| `vidgedb-ingest` | `msg.payload` = `{entity, signal, points}` **or** a bare `[ts, value]` pair | writes telemetry | the ingest result; `passthrough` forwards the original message |
| `vidgedb-check` | `msg.payload` = `{entity, signal, from, to}` | runs the deviation check | the check result; **sets `msg.violations = [result]` only on `VIOLATION`** |
| `vidgedb-log-event` | `msg.payload` = `{name, entity, timestamp?, provenance?, details?}` | appends an event | the event result |
| `vidgedb-service` | — | owns the engine | (config only) |

The `vidgedb-check` behaviour is the one that makes a flow simple: wire it to a `switch`
on `msg.violations[0]` and you have an alarm path without a function node.

## 4. The documented flow

[`examples/telemetry-ingest-flow.json`](examples/telemetry-ingest-flow.json) —
import it with **Menu → Import**:

```text
[mqtt in: plant/motor77/current]
        │
        ▼
[function: msg.payload = [[Math.floor(Date.now()/1000), Number(msg.payload.value ?? msg.payload)]]]
        │
        ▼
[vidgedb-ingest]  (entity Motor77 / signal current)
        │
        ▼
     (status)

[inject] ─▶ [vidgedb-query] ─▶ [debug]
[vidgedb-check] ─▶ [switch: msg.violations[0]] ─▶ [debug: ALERT]
```

The `function` node exists for one reason: VidgeDB takes **integer unix seconds**, and
MQTT payloads arrive as strings. That shaping is the only JavaScript this flow needs.

## 5. From OPC-UA or Modbus instead of MQTT

Keep the same shape — only the first node changes. Any node that produces a number can
feed `vidgedb-ingest`:

```text
[node-red-contrib-opcua, read/subscribe mode] ─▶ [shaper] ─▶ [vidgedb-ingest]
[node-red-contrib-modbus]                     ─▶ [shaper] ─▶ [vidgedb-ingest]
```

The shaper stays identical: `[[int seconds, number]]`. This is the pattern to teach: the
protocol node is replaceable, the twin is not.

## 6. A worked alarm path

1. `mqtt in` on the current of the motor;
2. `function` shaper (timestamp + number);
3. `vidgedb-ingest` → series `Motor77.current`;
4. every 30 s: `inject` → `vidgedb-check` `{entity: "Motor77", signal: "current", from: ts-3600, to: ts}`;
5. `switch` on `msg.violations[0]` → `debug`/e-mail/SMS.

Prerequisite for step 4 to answer anything other than `NO_SPEC`: the entity must carry
`spec.current.max` — that is what `check` compares against (see
[protocol](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/protocol.md) §4).

## 7. Traps that will bite a Node-RED user

| Symptom | Cause | Fix |
|---|---|---|
| `write forbidden` in the node status | the config node's role is `reader` | set `writer` or `ingest` |
| `NO_SPEC` on every check | `spec.<signal>.max` missing on the entity | write it with a `vidgedb-query`/`upsert` call or the loader |
| `unknown entity 'X'` | telemetry sent before the entity exists | create entities first (see [recipes](https://github.com/Vidge-AI/VidgeDB/blob/main/docs/sdk/recipes.md), Recipe 1) |
| A check never fires the ALERT | the `switch` reads `msg.violations`, which is set **only** on `VIOLATION` | verify the status by wiring a debug on the check node itself |
| The engine stops and the flow goes quiet | it exited; the config node respawns after 2.5 s | look at the config node's status dot |
| Two flows writing at once | one engine per config node — pointing two flows at *different* config nodes on the **same** `.vdg` gives you a refusal (one writer per file) | share one config node |

## 8. Tests

```bash
cd node-red
npm test                        # 26 cases (node:test), drives the real engine
npm run test:flow               # the example flow end-to-end (installs a throwaway
                                # Node-RED in /tmp, deploys the flow, checks registration)
```

`npm test` runs the unit suite only, and it is the one to keep green on every change.
`test:flow` is slower and heavier (it downloads Node-RED); it is the check that the nodes
still appear in a real palette.

No mock: the tests drive the real engine.

> Note: run `npm test` on Node 18+ with an explicit file path (as the script does).
> `node --test tests/` was the historical form and breaks on Node 22, which resolves the
> directory as a module.

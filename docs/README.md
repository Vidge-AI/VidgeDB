# VidgeDB — User Documentation

**VidgeDB** — base de données embarquée de jumeaux numériques temporels, pilotée par JSON-RPC/VidgeQL depuis Python, Node.js, Node-RED ou tout agent IA. Ce document est la référence utilisateur.

**VidgeDB is an embedded temporal graph database for AI-native digital twins of industrial machines.** One Rust binary, one `.vdg` file, no server, no dependencies. An AI agent (or any program) opens a VidgeDB *service*, speaks line-delimited **JSON-RPC 2.0** on the subprocess's stdin/stdout, and gets a machine-readable twin of a physical machine: typed topology, timestamped telemetry, expected-vs-observed deviation checks, and traceable diagnostic paths — with **tamper-evident provenance on every fact**.

> **Résumé (français)** — VidgeDB est une base embarquée qui maintient le jumeau numérique d'une machine industrielle dans **un seul fichier `.vdg`** (Rust, pages 4 KiB, WAL + recovery crash-safe, binaire statique aarch64 de ~10 Mo strippé → déployable sur Raspberry Pi comme sur cloud). Un agent IA lance `vidgedb --service plant.vdg --role reader` et pilote la base via **JSON-RPC 2.0 ligne-par-ligne** (16 méthodes de lecture + 5 d'écriture) ou le langage **VidgeQL** (`MATCH … WHERE … RETURN`, `MEASURE … DURING last(24h) RETURN max(...)`, `CHECK … FOR … DURING … RETURN status, deviation`). Toutes les données portent une **provenance** parmi 8 classes (Fact, Observation, Specification, Inference, Hypothesis, Event, Command, Configuration) ; **le moteur refuse structurellement d'écrire ou de promouvoir une hypothèse** — les diagnostics produits par un agent restent des sorties de rapport, jamais des faits. Écriture (télémetrie, topologie, état, événements) réservée aux rôles `writer`/`ingest`, chaque appel audité. SDK Python (`pip install vidgedb`), SDK JS/TS (`npm install @vidge-ai/vidgedb`), nœuds Node-RED (`@vidge-ai/node-red-contrib-vidgedb`) — tous zéro dépendance.

**Status**: v0.1.0, Apache-2.0 licence (see [LICENSE](../LICENSE)), maintained by VIDGE AI. Engine: 227 Rust tests green, zero warnings, SIGKILL crash harness 7/7 (zero torn commits). The docs you are reading were generated against the real binary — every JSON-RPC example below is copied from an actual session.

**Feature flags**: the OPC-UA server (Phase 16) is an **optional Cargo feature `opcua`, ON by default** — existing builds are unchanged. Build the small core binary with `--no-default-features` (**1.31 MiB**, measured on x86_64 at phase 93) instead of the full one (**14.06 MiB**, OPC-UA + vendored OpenSSL). A core binary refuses `--opcua` with an explicit message and does not advertise the mode in `--help`.

---

## Table of contents

1. [What is VidgeDB](#1-what-is-vidgedb)
2. [Quickstart](#2-quickstart)
3. [Concepts](#3-concepts)
4. [VidgeQL](#4-vidgeql)
5. [JSON-RPC service](#5-json-rpc-service)
6. [CLI](#6-cli)
7. [SDKs](#7-sdks-python--javascript--node-red)
8. [AI safety model (provenance, roles, audit)](#8-ai-safety-model)
9. [Deployment](#9-deployment)
10. [Benchmarks](#10-benchmarks)
11. [Known limits](#11-known-limits)
12. [FAQ](#12-faq)

See also: [jsonrpc-reference.md](jsonrpc-reference.md) (full protocol reference), [vidgeql-reference.md](vidgeql-reference.md) (language reference), [deployment.md](deployment.md) (step-by-step deployment guide).

---

## 1. What is VidgeDB

VidgeDB answers one question: *what is the state of this machine, what does the data say, and can an AI agent reason about it without being able to lie in the record?*

| Property | Value |
|---|---|
| Positioning | **Machine twin for AI agents** — diagnostic & maintenance reasoning over industrial equipment (PLC → drive → motor → pump…) |
| Storage | **Single `.vdg` file** (page store, 4 KiB pages, `VDG1` superblock, CRC32-framed WAL, crash-safe — SIGKILL at 7 delay points leaves zero torn commits) |
| Interface | JSON-RPC 2.0 **line-delimited over stdin/stdout** of the `vidgedb --service` subprocess (the interface AI agents already speak; MCP-compatible pattern) + a human REPL |
| Query language | **VidgeQL** — graph patterns (`MATCH -["topology"]->`), temporal windows (`DURING last(24h)` / `t1..t2`), aggregates, and a `CHECK … FOR …` deviation statement |
| The differentiator | **8-class provenance on every stored fact + the invariant that a `Hypothesis` can never be written or promoted by any agent role** (the engine refuses). Diagnostics are outputs, not data. |
| Footprint | Static musl binary for aarch64: **12.3 MiB unstripped / 10.1 MiB stripped** (OPC-UA vendored OpenSSL since P16 — was 596 KiB before); x86_64 release binary: 14.7 MiB; Windows `.exe`: 18.0 MiB; 10-machine/200K-point twin: **11.75 MB process RSS measured**, data file 242 KiB |
| SDKs | Python (`vidgedb` on PyPI, stdlib-only), JS/TS (`@vidge-ai/vidgedb` on npm, stdlib-only), Node-RED (`@vidge-ai/node-red-contrib-vidgedb`) |
| Deployment hosts | Raspberry Pi 4/5/Zero 2 (static aarch64 build), any Linux x86_64 box, cloud |
| NOT in v1 | network transport (no TCP listener — a child process per client), multi-writer concurrency, cryptographic auth, mmap, page checksums, compaction — see [Known limits](#11-known-limits) |

**What it is NOT:** not a historian (no HTTP API, no SQL), not a multi-user server, not a replacement for your plant historian — it is the *reasoning substrate* an agent holds a conversation with.

---

## 2. Quickstart

Build (any Linux with Rust):

```bash
cd ~/vidgeDB && cargo build --release --bin vidgedb
export VIDGEDB_BIN=$PWD/target/release/vidgedb
```

### 2.1 Ten-line tour (bash, raw JSON-RPC)

Build a mini machine — one writer session creates entities, telemetry, state and events:

```bash
B=target/release/vidgedb
$B --service twin.vdg --agent-id setup --role writer <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"upsert_entity","params":{"name":"PLC01","type":"PLC","props":{"vendor":"Siemens"},"source":"plc"}}
{"jsonrpc":"2.0","id":2,"method":"upsert_entity","params":{"name":"Motor42","type":"Motor","props":{"spec.current.max":"10"},"relations":[{"to":"PLC01","relation_type":"network:profinet"}],"source":"plc"}}
{"jsonrpc":"2.0","id":3,"method":"ingest_points","params":{"entity":"Motor42","signal":"current","points":[[1760000000,8.2],[1760001200,11.7]]}}
{"jsonrpc":"2.0","id":4,"method":"set_state","params":{"entity":"Motor42","key":"running","value":"yes"}}
{"jsonrpc":"2.0","id":5,"method":"log_event","params":{"name":"OverheatAlarm","entity":"Motor42","timestamp":1760001200,"provenance":1,"details":"relay tripped"}}
EOF
```

Real responses (captured from the binary):

```json
{"id":1,"jsonrpc":"2.0","result":{"created":true,"key":0,"relations_added":0}}
{"id":2,"jsonrpc":"2.0","result":{"created":true,"key":1,"relations_added":1}}
{"id":3,"jsonrpc":"2.0","result":{"accepted":3,"chunks_flushed":1,"series_id":0}}
{"id":4,"jsonrpc":"2.0","result":{"entity":"Motor42","key":"running","set":true}}
{"id":5,"jsonrpc":"2.0","result":{"entity":"Motor42","event":"OverheatAlarm","logged":true}}
```

Then the agent loop — **query → check → trace** with a plain Reader (the default role):

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"check","params":{"entity":"Motor42","signal":"current","from":1760000000,"to":1760003600}}' \
  | $B --service twin.vdg --agent-id diag
# {"id":1,"jsonrpc":"2.0","result":{"deviation":1.6999999999999993,"entity":"Motor42",
#  "expected_max":10.0,"expected_provenance":"Specification","observed":11.7,
#  "observed_provenance":"Observation","points_checked":3,"signal":"current",
#  "status":"VIOLATION","unit":null,"window":{"from":1760000000,"to":1760003600}}}
echo '{"jsonrpc":"2.0","id":2,"method":"trace","params":{"from":"Pump17","to":"Drive12","max_hops":4}}' | $B --service twin.vdg
```

### 2.1b Same quickstart via the Python SDK (`pip install vidgedb`)

```python
from vidgedb import VidgeDB

with VidgeDB("twin.vdg", agent_id="setup", role="ingest", bin="target/release/vidgedb") as db:
    print(db.upsert_entity(name="PLC01", type="PLC", props={"vendor": "Siemens"}, source="plc"))
    print(db.upsert_entity(name="Motor42", type="Motor",
                           props={"vendor": "ABB", "spec.current.max": "10"},
                           relations=[{"to": "PLC01", "relation_type": "network:profinet"}],
                           source="plc"))
    print(db.call("ingest_points", entity="Motor42", signal="current",
                  points=[[1760000000, 8.2], [1760001200, 11.7], [1760002400, 10.4]]))
    db.set_state(entity="Motor42", key="running", value="yes")
    db.call("log_event", name="OverheatAlarm", entity="Motor42",
            timestamp=1760001200, provenance=1, details="relay tripped")

with VidgeDB("twin.vdg", agent_id="diagnostician") as db:      # reader by default
    print(db.schema())
    print(db.query('MATCH (m:Motor) RETURN m'))
    print(db.get_measurements("Motor42", "current", from_=1760000000, to=1760003600).max)
    rep = db.check("Motor42", "current", from_=1760000000, to=1760003600)
    print(rep.status, rep.expected_max, rep.observed, rep.deviation)   # VIOLATION 10.0 11.7 1.7…
    print(db.trace("Motor42", "PLC01", max_hops=3)["found"])           # True
# exiting the `with` closes stdin; the service exits on EOF.
```

Verified output of this exact script (SDK 35/35 pytest green; example reproduced 2026-09-29):

```
{'created': True, 'key': 0, 'relations_added': 0}
{'created': True, 'key': 1, 'relations_added': 1}
{'accepted': 3, 'chunks_flushed': 1, 'series_id': 0}
schema → {'entity_types': ['Motor', 'PLC'], ...}
VIOLATION 10.0 11.7 1.6999999999999993
```

### 2.1c Same via the JS/TS SDK (`npm install @vidge-ai/vidgedb`, ESM or CJS)

```js
import { VidgeDB } from "vidgedb";           // const { VidgeDB } = require("vidgedb");

const w = VidgeDB.open("twin.vdg", { agentId: "setup", role: "ingest",
                                     bin: "target/release/vidgedb" });
console.log(await w.upsertEntity("Motor42", "Motor",
  { vendor: "ABB", "spec.current.max": "10" },
  [{ to: "PLC01", relation_type: "network:profinet" }], "plc"));
console.log(await w.ingestPoints("Motor42", "current",
  [[1760000000, 8.2], [1760001200, 11.7]]));
await w.close();

const db = VidgeDB.open("twin.vdg", { agentId: "js-agent" });
const rep = await db.check("Motor42", "current", 1760000000, 1760003600);
console.log(rep.status, rep.isViolation);    // VIOLATION true
await db.close();
```

(Executed for this doc: both SDK suites pass — Python `pytest` 35/35, JS `node --test` 32/32, Node-RED nodes 26/26, engine `cargo test --release` 227/227 — run it as `VIDGEDB_BIN=target/release/vidgedb cargo test --release`, see docs/deployment.md.)

---

## 3. Concepts

A `.vdg` file holds **four stores** over one transactional page engine (4 KiB pages, WAL-first commit, fsync durability, deterministic recovery on open):

```
┌─────────────────────────────────────────────────────────┐
│ .vdg single file                                        │
│  ┌────────────┐ ┌──────────────┐ ┌───────┐ ┌─────────┐  │
│  │ graph      │ │ time-series  │ │ state │ │ events  │  │
│  │ entities + │ │ chunks       │ │ [from,to) │ append- │
│  │ relations  │ │ (256 pts)    │ │ history │ │ only    │  │
│  └────────────┘ └──────────────┘ └───────┘ └─────────┘  │
│  every store: WAL-first tx, crash-safe, provenance      │
└─────────────────────────────────────────────────────────┘
```

### 3.1 Typed graph with provenance (entities + relations)

- **Entity**: `(key: u32 stable, name, type, inline properties)` — 64 B fixed cell, ~44 B of inline properties per entity. Properties are strings (`"spec.current.max": "10"`); the CHECK engine parses the numeric value.
- **Relation**: 48 B record `(src, dst, relation_type, provenance, [valid_from, valid_to))`, where `relation_type` is **`"topology:type"`** — the topology prefix (`electrical`, `mechanical`, `network`, …) is part of the edge's identity: traversal on `electrical` is a different edge than traversal on `mechanical`. Tombstoned relations stay physical (append-only), and each carries a provenance byte.
- **Provenance — 8 classes** (1 byte per record, surfaced by `provenance()` and `get_entity`):

| byte | class | meaning | example in this repo |
|---|---|---|---|
| 0 | `Fact` | structural ground truth from the official source | `upsert_entity(..., source="plc")` relations |
| 1 | `Observation` | a sensor/PLC/agent reading | ingested telemetry, `log_event(provenance=1)` |
| 2 | `Specification` | design intent (datasheet) | property `spec.current.max = "10"` on an entity |
| 3 | `Inference` | derived by an explicit computation | (not produced by the service in v0) |
| 4 | `Hypothesis` | a diagnostic guess | **output-only**: `trace`/agent reports; never writable |
| 5 | `Event` | a logged occurrence | `log_event(provenance=5)` |
| 6 | `Command` | a human/system action record | `log_event(provenance=6, name="MaintenanceDone")` |
| 7 | `Configuration` | a settings change | `log_event(provenance=7)` |

Real output (`provenance` on the demo twin):

```json
{"id":28,"jsonrpc":"2.0","result":{
 "provenance_classes":["Fact","Observation","Specification","Inference","Hypothesis","Event","Command","Configuration"],
 "relations":[{"dst":"Drive12","provenance":"Fact","provenance_byte":0,"relation_idx":1,
               "src":"Motor42","topology":"electrical","type":"fed_by","valid_from":0,"valid_to":-1},
              {"dst":"Motor42","provenance":"Fact","provenance_byte":0,"relation_idx":2,
               "src":"Pump17","topology":"mechanical","type":"coupled_to","valid_from":0,"valid_to":-1}]}}
```

**The invariant: a hypothesis is never promoted.** `Provenance` classes are engine-enforced (spec §29): ingestion maps declared `source` → `Fact` (`"plc"`) or `Observation` (`"agent"`, `"sensor"`) — there is no `hypothesis` source; the `set_hypothesis` method **refuses for every role** (tested: `{"error": "refused: no agent role may write a hypothesis in any phase (spec §29/§55)…"}`); and no engine code path re-labels a stored record. A diagnostic conclusion lives in the agent's context (and its own audit sink), never inside the twin. This is deliberate: telemetry drifts, specs change, machines get rewired — but an agent's guess about *why* must never masquerade as ground truth for the next agent.

### 3.2 Time-series store (telemetry)

- Series names follow the convention **`<entity_name>.<signal>`** (e.g. `Motor42.current`) — this is the join key between graph, MEASURE queries and CHECK.
- Points are buffered (256 points) then frozen into **chunks** — the unit of storage, indexing, *and deletion*: timestamps delta-encoded with zigzag-varint (`ENC_TS_VARINT`, or `ENC_TS_CONST` when all deltas are equal), values raw f64 or `ENC_VAL_CONST` when constant; each chunk carries min/max metadata.
- Ingest is **one transaction per call** — a 10 000-point batch is a single commit (one fsync), auto-flushing a chunk every 256 points, auto-creating the series on the fly.
- **Retention** (`retain` method / `--retention-days`) drops **whole chunks** older than a cutoff — never single points — so metadata-only statistics stay exact and freed pages return to the freelist. `--retention-days N` runs one pass at startup and then hourly.
- Timestamps are **integer unix seconds** (a float `ts` is refused, `-32602`).

### 3.3 State store

Key/value pairs **per entity**, each write creating a half-open validity window `[valid_from, valid_to)` — `valid_to = -1` means open-ended. Writing a new value *closes* the previous open window; the full history stays queryable (verified):

```json
// set_state Motor77/running = "no" @1760009500, then "yes", then "no" at the same instant:
// state_history →
{"id":4,"jsonrpc":"2.0","result":{"history":[
  {"valid_from":1760009500,"valid_to":1760009500,"value":"no"},
  {"valid_from":1760009500,"valid_to":1760009500,"value":"yes"},
  {"valid_from":1760009500,"valid_to":-1,"value":"no"}],"n":3}}
```

`get_state(entity, key, at?)` resolves the pair valid at `at` (or now) and returns `{"value": "...", "valid_from": t}` or `null`.

### 3.4 Events store

Append-only discrete occurrences `(name, entity, timestamp, provenance, details≤20 chars inline)`. Queried with `get_events(entity?, from, to)` — inclusive window; every event surfaces its provenance class and byte. Verified:

```json
{"id":27,"jsonrpc":"2.0","result":{"events":[
  {"details":"threshold relay trip","entity":"Motor42","name":"OverheatAlarm",
   "provenance":"Observation","provenance_byte":1,"timestamp":1760001200},
  {"details":"bearing swap","entity":"Motor42","name":"MaintenanceDone",
   "provenance":"Command","provenance_byte":6,"timestamp":1760002000}],"n":2}}
```

---

## 4. VidgeQL

One declarative language, three clause families. Full grammar: [vidgeql-reference.md](vidgeql-reference.md).

| Clause | Purpose | Example |
|---|---|---|
| `MATCH` | graph pattern (1..N hops, each edge topology-filtered, outbound) | `MATCH (plc:PLC) -[:NETWORK]-> (m:Motor)` |
| `WHERE` | filters on `name`/`type`/any inline prop (`= != < > <= >=`, `AND`, `OR`) | `WHERE m.name = "Motor42" AND m.poles > 2` |
| `AT <unix>` | time-travel: bind hops through the topology valid at that instant | `MATCH … AT 1760000000` |
| `MEASURE var.signal DURING win` | join bound entities with their time series | `MEASURE m.current DURING last(24h)` |
| `RETURN` | variables and/or aggregates `max/min/avg/sum/count` | `RETURN max(m.current), avg(m.current)` |
| `LIMIT n` | cap rows | `LIMIT 10` |
| `CHECK var.signal FOR (pattern) [WHERE …] DURING win RETURN fields` | deviation engine statement | see below |

Topologies are case-insensitive in patterns (`-[:MECHANICAL]->` matches `mechanical` edges — lowercased at parse).

### Ten worked examples (every one executed against the binary; DB = PLC01→Drive12→Motor42→Pump17, telemetry `Motor42.current` 6 pts, peak 11.7 A, spec max 10 A)

**1 — Simple MATCH** (all Motors):
```json
{"method":"query","params":{"vql":"MATCH (m:Motor) RETURN m"}}
→ {"n":1,"rows":[{"m":{"key":2,"name":"Motor42","properties":{"spec.current.max":"10","vendor":"ABB"},"type":"Motor"}}]}
```

**2 — WHERE on a property**:
```json
{"vql":"MATCH (m:Motor) WHERE m.name = \"Motor42\" RETURN m"}     → n:1 (as above)
{"vql":"MATCH (m:Motor) WHERE m.vendor = \"Siemens\" RETURN m"}   → {"n":0,"rows":[]}
```

**3 — Two-hop traversal with topology filter**:
```json
{"vql":"MATCH (p:Pump) -[:MECHANICAL]-> (m:Motor) -[:ELECTRICAL]-> (d:Drive) RETURN p, m, d"}
→ {"n":1,"rows":[{"d":{"key":1,"name":"Drive12",…},"m":{"key":2,"name":"Motor42",…},"p":{"key":3,"name":"Pump17",…}}]}
```

**4 — AT timestamp (topology as it was at t)**. With an edge created `valid_from: 1000`:
```json
{"vql":"MATCH (p:Pump) -[:MECHANICAL]-> (m:Motor) AT 1500 RETURN p"}  → binds (n:1)
{"vql":"MATCH (p:Pump) -[:MECHANICAL]-> (m:Motor) AT 500  RETURN p"}  → {"n":0,"rows":[]} (edge not yet valid)
```

**5 — WHERE with AND across two variables**:
```json
{"vql":"MATCH (m:Motor) -[:ELECTRICAL]-> (d:Drive) WHERE m.vendor = \"ABB\" AND d.vendor = \"ABB\" RETURN m, d"}
→ n:1 (Motor42, Drive12)
```

**6 — WHERE with OR (AND binds tighter than OR)**:
```json
{"vql":"MATCH (m:Motor) WHERE m.vendor = \"Siemens\" OR m.spec.current.max > 5 RETURN m"}
→ n:1 (Motor42 — the AND-equivalent leaf matched; a Siemens motor would also bind)
```

**7 — MEASURE / DURING with RETURNS aggregates** (temporal path, `query_temporal`):
```json
{"method":"query_temporal","params":{"vql":"MATCH (m:Motor) WHERE m.name = \"Motor42\" MEASURE m.current DURING 1759999000..1760003600 RETURN max(m.current), min(m.current), avg(m.current), count(m.current)","now":1760003600}}
→ {"n":1,"rows":[{"avg":{"kind":"Avg","value":9.683333333333332},
                 "count":{"kind":"Count","value":6.0},
                 "m":{…},"max":{"kind":"Max","value":11.7},"min":{"kind":"Min","value":8.2}}]}
```

**8 — DURING `last(nH)` relative window**:
```json
{"vql":"… MEASURE m.current DURING last(2h) RETURN max(m.current), min(m.current)","now":1760003600}
→ max 11.7, min 8.2
```

**9 — CHECK statement** (parsed by the same engine; exposed over JSON-RPC through the `check` method and the `vidgedb-check` Node-RED node):
```json
{"method":"check","params":{"entity":"Motor42","signal":"current","from":1760000000,"to":1760003600}}
→ {"deviation":1.6999999999999993,"entity":"Motor42","expected_max":10.0,
   "expected_provenance":"Specification","observed":11.7,
   "observed_provenance":"Observation","points_checked":6,"signal":"current",
   "status":"VIOLATION","unit":null,"window":{"from":1760000000,"to":1760003600}}
```
Statuses: `OK` / `VIOLATION` (observed max > spec max, strict — worst-case semantics, a transient overshoot must not hide) / `NO_DATA` (spec exists, window empty) / `NO_SPEC` (no `spec.<signal>.max` property).

**10 — Parse/semantic errors** (never a panic; surface as `{"error": "…"}` inside `result` for VQL, or `-32602` for malformed params):
```json
{"vql":"MATCH (m:Motor) RETURN m LIMIT"}      → {"error":"parse error: unexpected token: expected number"}
{"vql":"MATCH (m:Motor -[:ELECTRICAL]-> …"}   → parse error (unbalanced)
{"vql":"MATCH (x:Robot) RETURN x"}            → {"n":0,"rows":[]}   (unknown type matches nothing)
{"method":"query_temporal","params":{"vql":"… MEASURE m.current DURING bogus RETURN max(m.current)"}}
                                              → {"error":"parse error: DURING expects last(n) or a..b"}
```

---

## 5. JSON-RPC service

`vidgedb --service <db.vdg> [--agent-id X] [--role reader|writer|ingest] [--retention-days N]` speaks **JSON-RPC 2.0, one request per line on stdin, one response per line on stdout**. Logs go to stderr. A notification (no `id`) gets no response. Malformed JSON → standard `-32700` and the stream **keeps listening**. Zero panic on any input (fuzzed by the SDK test suites).

The complete per-method reference with real request/response pairs lives in [jsonrpc-reference.md](jsonrpc-reference.md).

```text
READ (every role)                              WRITE (writer|ingest; reader → -32003)
  schema            {}                            ingest_points {entity, signal, points}
  list_entities     {type?, limit?}   ← NEW       upsert_entity {name, type, props?, relations?, source?}
  graph             {limit?}          ← NEW       set_state     {entity, key, value, at?}
  query             {vql}                         log_event     {name, entity, timestamp?, provenance?, details?}
  query_temporal    {vql, now?}                   retain        {before?}
  get_entity        {key}
  get_measurements  {entity, signal, from?, to?}
  check             {entity, signal, from?, to?}
  trace             {from, to, max_hops?}
  provenance        {}
  get_state         {entity, key, at?}
  state_history     {entity, key}
  get_events        {entity?, from?, to?}
  audit             {}
  diagnose          {entity, from?, to?}  ← NEW   (read-only: 8-step report, §26)
  set_hypothesis    {entity, text}                (REFUSED in every role, §29 — the refusal is the contract)
```

The three methods marked **NEW** were added in phase 93, after probing the shipped
binary showed what the platform could not do: `list_entities` (alias `entities`) is the
inventory call — before it, `MATCH (x) RETURN x` without a type silently returned 0 rows
and enumerating meant one call per known type; `graph` returns entities **and** relations
in one round-trip with endpoints by **name** (what a graph editor draws); `diagnose`
exposes the 8-step pipeline, which previously existed only as a Rust library function, so
a platform in Go or Python had no way to reach it. `set_hypothesis` is the odd one out:
it is listed because it is **reachable and refuses**, which is the contract — it used to
answer `-32601 unknown method`, i.e. "this does not exist", instead of "this is forbidden,
and here is why".

Error codes (transport-level, in the JSON-RPC `error` member):

| Code | Meaning | Example trigger |
|---|---|---|
| `-32700` | parse error (malformed JSON) | a non-JSON line on stdin |
| `-32600` | invalid request object | request body not an object |
| `-32601` | method not found | `"method":"nope"` |
| `-32602` | invalid params | missing `entity`; non-integer `ts`; empty `points` |
| `-32000` | storage error | engine-level failure surfaced by the method |
| `-32003` | **write forbidden** (server-defined) | a Reader calls a write method |

Method-level *semantic* problems ("unknown entity 'X'", "relation target does not exist") come back **inside `result`** as `{"error": "…"}` — the AgentApi v0 contract — while the JSON-RPC `error` member is reserved for the transport layer.

**Roles**: `reader` (default; every write refused with -32003 **and the refusal is audited**), `writer`, `ingest` (same capabilities as writer in v1; the distinction is declared intent for the audit trail). `--retention-days` additionally requires `--role writer|ingest` (the retention pass writes; a reader service must not — checked up front, exit 2).

**Audit trail**: every call — including refused writes — is recorded in-memory (`timestamp`, `agent_id`, `method`, `params_summary`) and served by `audit`. Real output after one refused ingest attempt by a reader:

```json
{"entries":[{"agent_id":"must-stay-reader","method":"ingest_points",
             "params_summary":"REFUSED (role=Reader, write forbidden -32003)","timestamp":…},
            {"agent_id":"must-stay-reader","method":"upsert_entity",
             "params_summary":"REFUSED (role=Reader, write forbidden -32003)",…},
            {"agent_id":"must-stay-reader","method":"audit_log","params_summary":"",…}],"n":3}
```

---

## 6. CLI

Three subcommands, one binary (full options in `--help`):

| Command | Options | Semantics |
|---|---|---|
| `vidgedb --service <db.vdg>` | `--agent-id <id>` (default `agent-cli`) · `--role reader\|writer\|ingest` (default **reader**: every write refused `-32003`, audited) · `--retention-days <days>` (startup pass + hourly thread; **requires** `--role writer\|ingest`, else exit 2) | JSON-RPC 2.0 over stdin/stdout for AI agents. Zero panic on invalid input; storage/log notes on stderr. |
| `vidgedb open <db.vdg>` | `--agent-id` (default `human`) · `--role` | the *same* service loop on a TTY: an interactive REPL for human inspection (prompt on stderr). `exit`/`quit` leaves. |
| `vidgedb smoke` | — | the spec-§58 example machine (PLC→Drive→Motor→Pump, two out-of-spec readings) — prints the 2 detected violations, exit 0. |
| `vidgedb --version` / `--help` | | `vidgedb 0.1.0`. |

Exit codes: 0 clean EOF; 2 wrong arguments (unknown `--role`, missing path, retention+reader mismatch); 3 startup retention failure; 4 stdio error.

---

## 7. SDKs (Python, JavaScript, Node-RED)

Three thin clients over the same subprocess protocol — **all zero-dependency** (Python: `subprocess`/`json`; Node: `child_process`+`readline`):

| SDK | Package | Style | Tests |
|---|---|---|---|
| Python ≥ 3.9 | `pip install vidgedb` (0.1.0) | `VidgeDB(path, agent_id=, role=, bin=)`; context manager; typed `CheckResult`, `MeasurementSeries` dataclasses; `db.call(method, **params)` = dynamic dispatch (new binary methods need no SDK update); `VidgeDBError(code, message)` | 35 pytest |
| Node ≥ 18 (ESM+CJS) | `npm install @vidge-ai/vidgedb` (0.1.0) | `VidgeDB.open(path, {agentId, role, bin})`; Promise queue serializes interleaved calls; typed mirrors `query/check/trace/ingestPoints/…`; `VidgeDBError.code` | 32 node:test |
| Node-RED | `@vidge-ai/node-red-contrib-vidgedb` (0.1.0) | config node `vidgedb-service` (owns the subprocess, auto-respawn 2.5 s, status dot green/ring red) + `vidgedb-query` / `vidgedb-ingest` / `vidgedb-check` / `vidgedb-log-event` | 27 node:test |

Binary resolution in every SDK: explicit `bin` → `$VIDGEDB_BIN` → `vidgedb` on `PATH`.

Error mapping (both SDKs): JSON-RPC `error` member → `VidgeDBError` with the **real code** (`-32700…-32003`); `{"error": …}` inside `result` → `VidgeDBError` with **code 0**.

### Node-RED ingestion (the visual path from a real machine)

```
[mqtt in: plant/motor77/current] → [function: msg.payload=[[ts, value]]] → [vidgedb-ingest] → (status)
[inject] → [vidgedb-query] → [debug]
[vidgedb-check] → [switch: msg.violations[0]] → [debug: ALERT]
```

The example flow `sdk-nodered/examples/telemetry-ingest-flow.json` wires exactly this: an `mqtt in` (topic `plant/motor77/current`), one function node shaping `[[Math.floor(Date.now()/1000), Number(msg.payload.value ?? msg.payload)]]`, two `vidgedb-ingest` nodes writing series `Motor77.current`, an inject → `vidgedb-query` → debug manual path, and a `vidgedb-check` node wired to a `switch` on `msg.violations[0]` that fans out to an ALERT debug. Import: **Menu → Import**. Node behaviors: `vidgedb-ingest` accepts a `{entity, signal, points}` batch or a bare `[ts, value]` pair; `vidgedb-check` sets `msg.violations = [result]` only on status `VIOLATION`.

---

## 8. AI safety model

This is the differentiator, and it is *engine behavior*, not policy.

**Why provenance?** A diagnosis is only as good as the distinction between *what the PLC said* (Fact), *what a sensor reported* (Observation), *what the datasheet promises* (Specification) and *what the agent guessed* (Hypothesis). VidgeDB stores that distinction **on every relation, event and check output**, surfaces it on every read, and gives it structural force.

**What the engine refuses — verified behaviors (spec §29):**

1. `set_hypothesis` is refused for **every** role — Reader, Writer, Ingest. There is no API surface that writes provenance class 4 (`{"error": "refused: no agent role may write a hypothesis in any phase (spec §29/§55); … use an explicit, human-audited external write outside this API"}`).
2. No promotion path exists in the engine: nothing re-labels a stored `Hypothesis`/`Observation`/`Fact`. Ingested data enters as `Fact` (declared source `plc`) or `Observation` (`agent`/`sensor`) — never `Hypothesis`.
3. A **Reader** service (the default) refuses every write method with `-32003`, and the refusal lands in the audit trail — nothing is written silently (verified against the binary; sample above).
4. `check`/`trace` results carry the provenance sides explicitly (`expected_provenance: "Specification"`, `observed_provenance: "Observation"`), so an agent's final report can cite where each number came from.
5. Diagnostics (`diagnose`-style reports; hypothesis strings an agent derives from `check`+`trace`) are **outputs of the report only** — the API performs no write whatsoever to represent them.

**Role gating (the §55 rule and its one documented derrogation):** agents read, never write — that is the default (`--role reader`). The single negotiated exception: a Writer/Ingest may append telemetry (`ingest_points`), topology (`upsert_entity`), state (`set_state`), events (`log_event`) and run retention (`retain`). Even there, ingested values keep Fact/Observation provenance; class bytes are never agent-settable (e.g. a spec property is only ever written as a plain entity property by an explicit decision, and reads on it report `Specification`).

**Audit (spec §56):** every API call is recorded `{timestamp, agent_id, method, params_summary}` in-memory for the service session and served by `audit` — including *refused* attempts, so an agent probing beyond its mandate leaves a trace.

**v1 limits (documented, not hidden):** no cryptographic authentication — a role is a trust declaration by the local user (`--role` flag, single-user deployment); a TCP/TLS wrapper (future work) is the planned enforcement point for real authn/z. One writer per `.vdg` file. See [Known limits](#11-known-limits).

---

## 9. Deployment

Full step-by-step (build, scp to a Pi, systemd, Node-RED): [deployment.md](deployment.md).

### Cross-compile for the edge

```bash
cd ~/vidgeDB
cargo build --release --target aarch64-unknown-linux-musl --bin vidgedb \
    RUSTFLAGS=-C\ linker=rust-lld
# → target/aarch64-unknown-linux-musl/release/vidgedb — ELF 64-bit ARM aarch64,
#   statically linked, 12.3 MiB unstripped (12 946 840 B, verified 2026-10-06).
#   Strip it for deployment:  aarch64-linux-gnu-strip → 10.1 MiB (10 596 936 B).
#   NOTE: the old "596 KiB" figure predates the Phase-16 OPC-UA server, which
#   links a vendored OpenSSL — expect ~10-12 MiB from now on.
#   Verified under qemu-user (ARM emulation): `--version`, `smoke` (2 violations
#   detected), `--http` (JSON-RPC write accepted) and `--opcua` (browse endpoint
#   up on 127.0.0.1, ns=2) all run on the aarch64 binary.
```

### Memory plan (measured: `tests/phase83b_ram.rs` prints RSS; chunk index = one 56-B record per 256-pt chunk; ~5 B compressed per point)

| Machines (10 signals each) | Points | Chunk-index RAM | Total process RSS |
|---|---|---|---|
| 10 | 200 K (20K × 10) | ~4 MB | **~12 MB** (measured 11 752 kB stable after retention) |
| 50 | 1 M (20K × 50) | ~20 MB | ~55 MB |
| 100 | 30 days ≈ 5 M | ~100 MB | ~230 MB — **enable `--retention-days`** |

Without retention the chunk index grows linearly with time; a Pi running 100 machines without retention saturates RAM within weeks. Retention bounds it (chunk granularity, ~5 bytes/point on disk).

### Fleet sizing (Pi Zero 2 W = 512 MB, Pi 4 = 1–8 GB, Pi 5 = 2–16 GB)

| Fleet | Recommendation |
|---|---|
| ≤ 10 machines, 20 K pts each | Pi Zero 2 W is fine (12 MB for data + ~1 MB binary). No retention needed. |
| ≤ 50 machines | Pi 4, 1 GB+. `--retention-days 30` if always-on. |
| 100+ machines, long horizons | Pi 5 (4 GB+) **with retention enabled**, or move to an x86_64 cloud host; remember ts_aggregate is the weakest scenario (see benchmarks) — prefer `CHECK` + range queries over full-window aggregation at this size. |

### systemd unit

```ini
[Unit]
Description=VidgeDB digital-twin service (plant.vdg)
After=network.target

[Service]
ExecStart=/usr/local/bin/vidgedb --service /var/lib/vidgedb/plant.vdg --agent-id plc-bridge --role ingest --retention-days 30
Restart=always
RestartSec=3
User=vidgedb
# stdout is the JSON-RPC pipe: a TCP/HTTP wrapper (or socat, or your agent) reads it.
StandardInput=socket          # or leave the process speaking on stdio for a local agent
[Install]
WantedBy=multi-user.target
```

*(For a pure stdin/stdout consumer, drop `StandardInput=socket` and let the parent process — agent, SDK, Node-RED config node — own the child; that is exactly what the SDKs do.)*

### JSON-RPC transport — stdin/stdout now, TCP later (documented limit)

The service speaks JSON-RPC **on stdio only**: the natural interface for a child process (any Python/Node script spawns it), explicitly planned as *not* networkable in v1. To reach it over a socket today, wrap it yourself (`socat TCP-LISTEN:7678,reuseaddr,fork EXEC:"vidgedb --service /data/twin.vdg --role reader",pty` or a small inetd-style shim) — and note the **documented limit**: v1 has **no authentication**; do not expose the TCP wrapper beyond a trusted interface until real authn/z lands in the planned wrapper.

---

## 10. Benchmarks

Measured with the repo's own benchmark binary (`src/bin/bench.rs` → `bench`), re-run on the release build of the current main (`--machines 50 --points 100000`). p50/p95 are per-op percentiles; ts scenarios report the batch total (µs/point ≈ p50/n_ops).

| Scenario (50 machines/100 K pts, release, 2026-09-29 re-run) | total | throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| entity_insertion (1 000 entities, 10 tx) | 84.4 ms | 11 849 ops/s | 8.48 ms/tx | 9.91 | 9.91 |
| relation_insertion (3 000 relations) | 113.7 ms | 26 376 ops/s | 11.1 ms/tx | 13.2 | 13.2 |
| lookup (name → key, 1 000) | 69.6 ms | 14 366 ops/s | 69 µs | 131 µs | 143 µs |
| ts_sequential (100 K pts, 1 series) | 86.3 ms | **1.16 M pts/s** | 86.3 ms/batch | — | — |
| ts_batched (100 K pts / 10 series) | 84.2 ms | **1.19 M pts/s** | 84.2 ms/batch | — | — |
| ts_range_query (100 windows/100 K pts) | 1.5 ms | 68 175 ops/s | 12 µs | 25 µs | 43 µs |
| ts_aggregate (50 full-window aggs) | 110.7 ms | 452 ops/s | 2.17 ms | 2.38 | 3.24 |
| recovery (cold reopen) | 0.13 ms | — | 0.132 ms | — | — |

Smaller fleet (10 machines / 20 K pts, same binary run): ts ingest **1.35–1.37 M pts/s**, range query 141 K ops/s, cold reopen 0.073 ms, synthetic-twin file **241 664 B** (≈242 KiB); the 50-machine fixture twin is **1 093 632 B ≈ 1.09 MB**. Storage ≈ **5 B/point compressed** post-chunking.

Phase-9 audit trail (median of 4 runs, archived): entity_insertion −31%, relation_insertion **711→111 ms (6.5×)** after fixing the per-append adjacency rebuild, WAL CRC32 table-based 32→20 µs/frame; disk format unchanged end to end (byte-identical sizes before/after); crash harness re-verified 7/7 zero torn commits after optimization.

---

## 11. Known limits

Stated plainly, all of them (nothing here is hidden):

| Limit | Detail |
|---|---|
| **One writer** | no cross-process lock or MVCC: one `Engine` per `.vdg` by convention; a second writer process may corrupt. Reader-style services in other processes must open after writes are done, or use separate files. |
| **No page checksums** | integrity rides on CRC32 WAL frames; the `.vdg` data pages themselves are not checksummed (planned hardening). |
| **No mmap** | explicit `File` read/write I/O (spec §22) — simpler crash discipline, slower large scans. |
| **No compaction** | slabs are append-only with physical tombstones; a delete-happy workload never shrinks the file yet. |
| **Strings ≤ 4 KiB** | a string must fit one arena page (`PAGE_HDR + cell ≤ 4096`); no cross-page strings. |
| **Entity props ≤ 44 bytes inline** | merged property payload >44 B is refused cleanly (measured: `{"spec.current.max":"10","vendor":"ABB"}` = 31 B fits; adding `spec.current.unit`=A overflows → error `merged properties exceed the inline cap (51 > 44 bytes)`). The 44-B cap is why spec+unit in demo setups stayed at 2 props. |
| **Series name ≤ 23 bytes** | `<entity>.<signal>` must fit in a 23-B series-name cell. |
| **Event `details` ≤ 20 bytes** | longer details truncated at a UTF-8 boundary (statestore.rs `DETAILS_INLINE_MAX`). |
| **No auth** | roles are trust declarations; TCP wrapper (future) is where authn/z lives. |
| **`list_entities` is O(entities)** | the entity store has no name index: each entry decodes its name from the interned string arena. Fine for a machine twin (hundreds of entities); a fleet-level inventory at 100k+ would want an index or a cached snapshot. `graph` is likewise O(entities + relations). |
| **`list_entities` has no `offset`** | pagination is by `limit` only. An offset would age badly on an append-only store (an insert shifts the next page), so the natural cursor is the `key` (`after: N`) when it is needed. |
| **Timestamps are unix seconds (i64)** | ingest refuses float timestamps (`1760000000.5` → `-32602`); ns-resolution is a spec-level decision not implemented. |
| **ts_aggregate is the slow path** | ~452–2 195 ops/s (decode ~95 % of the cost); prefer `check`/range queries over full-window aggregates on big fleets. |
| **`AT` only affects hops** | a single-node `MATCH (:Motor) RETURN m` binds regardless of `AT` (per Phase-6.5 semantics, `MATCH (…)` alone has no hop to time-travel). |
| **No per-point provenance on series** | a series declares its source by *name convention* + the audit log; points themselves carry no provenance byte. |
| **Retention is chunk-granular** | points are never deleted individually; a window older than `before` that ends mid-chunk keeps the whole chunk. |

---

## 12. FAQ

**Why not just TimescaleDB / InfluxDB / SQLite?**
They are better historians. VidgeDB couples four structures (typed graph, chunked series, valid-interval state, provenance-tagged events) under **one crash-safe single file** with **agent-native access** (JSON-RPC in any language, no SQL injection surface, no server to harden) and **engine-enforced provenance** (no role can write a hypothesis; spec provenance is carried through checks into the report). If you need a long-horizon fleet historian, put InfluxDB beside it and let VidgeDB hold the *reasoning* twin.

**Why can't an agent write its hypotheses ("bearing wear suspected") into the database?**
Because the value of a hypothesis depends on being able to trust that nobody — including a future process — turned it into a fact. `set_hypothesis` is refused in every role, no engine path promotes classes, and diagnostics stay outputs of `diagnose`/agent reports. Your agent should keep its hypotheses in its own memory/notes, cite the `check`+`trace` evidence (with provenance), and a human decides what becomes Fact. See §8.

**How do I connect my OPC-UA / MQTT / Modbus data?**
Any tool that can spawn a process and write a line of JSON works. Industrial answer today: Node-RED (`@vidge-ai/node-red-contrib-vidgedb`, example flow MQTT→ingest included in `sdk-nodered/examples/`) or a small Python bridge with the SDK (`role="ingest"`); see [deployment.md](deployment.md) for both walk-throughs.

**Can several agents read the same twin at once?**
Yes — each spawns its own `vidgedb --service <db.vdg>` reader process; readers are concurrent and don't block each other. Only writers serialize (one writer process per file at a time).

**How do I delete old telemetry?**
`retain` (JSON-RPC, `writer|ingest` role) or run the service with `--retention-days 30` (automated). It drops **whole chunks** older than the cutoff — points inside a partially old chunk keep the chunk; `state`/`events`/graph are never touched by retention.

**Is the file format stable / can I move a `.vdg` between x86 and ARM?**
Yes — little-endian, versioned superblock `VDG1`, plain page array, no mmap and no host-dependent layout; the same file opens on a Pi (musl static build) and a cloud box. The tests reopen fixtures across processes as their contract.

**Does the service accept HTTPS / run as a daemon listening on a port?**
Not in v1. It is a stdio child process. The planned TCP/TLS wrapper is the documented future home of authn/z. Until then, keep it local to the machine (agent on same host, or Node-RED/socat bridging).

---

*License Apache-2.0 · Author JMK · Docs examples executed against `vidgedb 0.1.0` (release build) on 2026-09-29.*
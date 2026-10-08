# VidgeDB

**Embedded temporal graph database for industrial digital twins.** *Vigilance on the edge.*

One machine's twin in one crash-safe `.vdg` file: the typed equipment graph (PLC, drive,
motor, conveyor…), chunked telemetry, valid-interval state, and **provenance on every
value**. A Rust engine; clients for Python, JavaScript and Node-RED.

Apache-2.0 · [vidge-ai.com](https://vidge-ai.com) · `git@github.com:Vidge-AI/VidgeDB.git`

```bash
cargo build --release                          # full build (OPC-UA included)  — 14.06 MiB
cargo build --release --no-default-features   # core only                     —  1.31 MiB
VIDGEDB_BIN=$PWD/target/release/vidgedb cargo test --release   # 227 tests, 0 warnings
./target/release/vidgedb smoke                 # spec §58 machine → exactly 2 violations
```

> **The env var is not optional.** Several tests drive the real binary as a subprocess —
> the only way to observe session poisoning, signal handling, or the JSON-RPC contract.
> Without `VIDGEDB_BIN` they fail to spawn and the total count **silently drops**.

## What it is for

An AI agent — or any program — opens a VidgeDB *service*, speaks line-delimited JSON-RPC
2.0 on its stdin/stdout, and gets a machine-readable twin: query the topology, read
measurements, compare observations against specifications, find deviations, and build
traceable diagnostic hypotheses.

Two invariants shape the whole API, and both are enforced by the engine rather than by
convention:

- **A hypothesis is never persisted and never promoted.** `set_hypothesis` is *reachable
  and refuses* in every role, with a machine-readable audited reason — an agent's guess
  can never masquerade as ground truth for the next agent.
- **Provenance is carried through.** A `check` violation reports which side is the
  **Specification** and which is the **Observation**; ingested data enters as `Fact`
  (declared `source: "plc"`) or `Observation` (`"agent"`/`"sensor"`), never as a hypothesis.

## Quickstart

```bash
# 1) one-line call from any shell
echo '{"jsonrpc":"2.0","id":1,"method":"schema"}' | vidgedb --service twin.vdg
# → {"id":1,"jsonrpc":"2.0","result":{"entity_types":[…],"provenance_classes":[…]}}

# 2) ingest a machine and read it back (Python, no SDK required)
python3 - <<'PY'
import json, subprocess, time
p = subprocess.Popen(['vidgedb','--service','twin.vdg','--role','ingest'],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
def rpc(i, m, **kw):
    p.stdin.write(json.dumps({'jsonrpc':'2.0','id':i,'method':m,'params':kw})+'\n'); p.stdin.flush()
    return json.loads(p.stdout.readline())['result']

rpc(1,'upsert_entity', name='PLC01', type='PLC', props={'vendor':'Siemens'}, source='plc')
rpc(2,'upsert_entity', name='M77', type='Motor', props={'spec.current.max':'10'},
    relations=[{'to':'PLC01','relation_type':'network:profinet'}], source='plc')
t = int(time.time())
rpc(3,'ingest_points', entity='M77', signal='current',
    points=[[t-60,9.0],[t,12.5]])          # 2 points, ONE transaction, ONE fsync
print(rpc(4,'check', entity='M77', signal='current'))   # → status VIOLATION, deviation 2.5
print(rpc(5,'diagnose', entity='M77', **{'from':t-3600,'to':t}))  # 8-step report
p.stdin.close()
PY
```

**Batch your points.** One point per call costs a round-trip (~250 pts/s); 1 000 points in
one call measures **244 000 pts/s**, 20 000 points **531 000 pts/s**. A batch is a single
transaction and a single fsync.

## API surface

Full per-method reference with real request/response pairs:
**[docs/jsonrpc-reference.md](docs/jsonrpc-reference.md)**.

```text
READ (every role)                          WRITE (--role writer|ingest; a reader gets -32003)
  schema          {}                        ingest_points   {entity, signal, points:[[ts,value]…]}
  list_entities   {type?, limit?}           upsert_entity   {name, type, props?, relations?, source?}
  graph           {limit?}                  set_state       {entity, key, value, at?}
  query           {vql}                     log_event       {name, entity, timestamp?, provenance?, details?}
  query_temporal  {vql, now?}               retain          {before?}
  get_entity      {key}
  get_measurements{entity, signal, from?, to?}   set_hypothesis  {entity, text}
  check           {entity, signal, from?, to?}     ↳ REFUSED in every role (§29) — the refusal is the contract
  trace           {from, to, max_hops?}
  diagnose        {entity, from?, to?}     8-step report; hypotheses never persisted
  provenance      {}                       audit  {}   get_state  {}   state_history  {}   get_events  {}
```

- `list_entities` (alias `entities`) is the inventory call — no prior knowledge needed;
  always check `truncated` before treating a reply as the complete set.
- `graph` returns entities **and** relations in one round-trip, endpoints by **name**:
  what a graph editor draws.
- `diagnose` exposes the 8-step pipeline of spec §26. An unknown entity is **not** an
  error: `component` is `null` and the lists are empty, so a UI shows "component not
  found" instead of breaking.

Transport errors: `-32700` malformed JSON · `-32600` invalid request · `-32601` unknown
method · `-32602` invalid params · `-32000` storage · `-32003` write forbidden.
Method-level *semantic* failures ("unknown entity 'X'") arrive **inside `result`** as
`{"error": "…"}` — the JSON-RPC `error` member is reserved for the transport layer.

## Clients

Under `sdk-python/`, `sdk-js/`, `sdk-nodered/` — each autonomous and dependency-free at
runtime. Their tests build the test twin over JSON-RPC from a stdlib fixture, so **no Rust
toolchain is needed** to run them.

```python
db.call("list_entities", type="Motor")        # the generic dispatcher reaches ANY method
db.call("diagnose", entity="Motor42", **{"from": t0, "to": t1})
```

The dispatcher, not a mirrored helper, is the official contract: `diagnose`'s window
parameters are `from`/`to`, and `from` is a Python keyword — a typed
`def diagnose(from=…)` signature is impossible in Python.

## Deployment

One binary, one file, no server.

```bash
docker run -e VIDGEDB_TOKEN="$(openssl rand -hex 24)" -p 127.0.0.1:8888:8888 \
    ghcr.io/vidge-ai/vidgedb          # refuses to start without a token
vidgedb --http twin.vdg --port 8888 --role ingest    # or run it directly
```

Cross-compile for a Raspberry Pi (static musl): see
[docs/deployment.md](docs/deployment.md). Measured footprints: aarch64 12.3 MiB
unstripped / 10.1 MiB stripped; a 10-machine twin at 200 000 points ≈ **11.75 MB RSS**,
data file 242 KiB (~2.2 bytes per point compressed).

## Known limits

Stated plainly — the full list is in [docs/README.md §11](docs/README.md).

- **One writer.** No cross-process lock; one `Engine` per `.vdg` by convention.
- **Entity props ≤ 44 bytes inline**; series names ≤ 23 bytes; entity count has no name
  index, so `list_entities` is O(entities).
- **No auth.** Roles are trust declarations; a TCP/TLS wrapper is where authn/z belongs.
- **Retention is chunk-granular** — points are never deleted individually.
- **No macOS build verified** (no Apple machine available); Windows and Pi binaries are
  cross-builds whose real hardware run is also unverified.

## Contributing

**[CONTRIBUTING.md](CONTRIBUTING.md)** — how to build, test, and what a good change looks
like. **[DEVELOPING.md](DEVELOPING.md)** — architecture, the invariants not to break,
the test procedure, and the build history.
Reference docs live in [`docs/`](docs/).

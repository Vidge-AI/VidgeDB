# Deploying VidgeDB — Step by Step

From zero to a running machine-twin on x86_64 or a Raspberry Pi 4/5/Zero 2, with systemd, a Node-RED MQTT ingestion flow, and a complete Python AI diagnostician agent. All commands verified against `vidgedb 0.1.0` (repo main, `e1a625b`).

See also: [README.md](README.md) (concepts), [jsonrpc-reference.md](jsonrpc-reference.md) (protocol), [vidgeql-reference.md](vidgeql-reference.md).

---

## 1. Build the binary

### 1.1 Native (x86_64 Linux)

```bash
git clone git@github.com:Vidge-AI/VidgeDB.git ~/vidgeDB   # or your fork
cd ~/vidgeDB
cargo build --release --bin vidgedb
./target/release/vidgedb --version        # vidgedb 0.1.0
./target/release/vidgedb smoke            # spec-§58 check: 2 violations found, exit 0
```

Binary size: **1.31 MiB** (1 375 528 B, glibc dynamic, `--no-default-features`). Run the test battery once to trust your toolchain: `VIDGEDB_BIN=$PWD/target/release/vidgedb cargo test --release` → **227 passed** as of v0.1.0.

The env var is not optional: a few tests drive the real binary as a subprocess (the only way to observe session poisoning, signal handling, or the JSON-RPC contract). Without it they fail to spawn and the aggregate count silently drops.

### 1.2 Cross-compile for Raspberry Pi / aarch64 (static musl)

```bash
cd ~/vidgeDB
cargo build --release --target aarch64-unknown-linux-musl --bin vidgedb \
    RUSTFLAGS=-C\ linker=rust-lld
# → target/aarch64-unknown-linux-musl/release/vidgedb
file   target/aarch64-unknown-linux-musl/release/vidgedb
# ELF 64-bit LSB executable, ARM aarch64, version 1 (SYSV), statically linked
ls -la target/aarch64-unknown-linux-musl/release/vidgedb
# -rwxr-xr-x  608424 bytes  (596 KiB)
```

Requires the two target std pieces on first use (`rustup target add aarch64-unknown-linux-musl`). `rust-lld` ships with Rust — no cross-linker package needed. The result is fully static: copy & run, no libc on the target.

### 1.3 Ship it to the Pi

```bash
# on the Pi: sudo useradd -r -s /usr/sbin/nologin vidgedb
#            sudo mkdir -p /var/lib/vidgedb && sudo chown vidgedb /var/lib/vidgedb
scp target/aarch64-unknown-linux-musl/release/vidgedb pi@plant-pi:/tmp/
ssh pi 'sudo install -m 755 /tmp/vidgedb /usr/local/bin/vidgedb && vidgedb --version'
# vidgedb 0.1.0
```

---

## 2. Run the service (dev mode, foreground)

```bash
# writer session — build the twin once (see §3 for the full machine)
vidgedb --service /var/lib/vidgedb/plant.vdg --agent-id setup --role writer
# paste JSON-RPC lines, Ctrl-D to finish

# always-on service with retention (30 days):
vidgedb --service /var/lib/vidgedb/plant.vdg --role writer --retention-days 30
# stderr note at startup: vidgedb: startup retention: 0 points / 0 chunks removed (cutoff=…)

# read-only consumer (e.g. a dashboard agent) in another process — fine concurrently:
vidgedb --service /var/lib/vidgedb/plant.vdg --agent-id dashboard
```

Sanity check round-trip:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"schema"}' | vidgedb --service /var/lib/vidgedb/plant.vdg
```

---

## 3. systemd unit (full example)

`/etc/systemd/system/vidgedb.service`:

```ini
[Unit]
Description=VidgeDB machine twin (plant.vdg)
After=network.target

[Service]
# JSON-RPC on stdio. For a local AI agent, systemd owns the process on a
# socket-activated stdin; for the Node-RED/SDK pattern (§4) REMOVE this file
# and let the parent own the child instead (the SDK/Node-RED spawn it).
ExecStart=/usr/local/bin/vidgedb --service /var/lib/vidgedb/plant.vdg --agent-id plc-bridge --role ingest --retention-days 30
Restart=always
RestartSec=3
User=vidgedb
Group=vidgedb
# Hardening (the binary needs only file access; stdio protocol)
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/vidgedb
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vidgedb
journalctl -u vidgedb -f          # stderr notes land here (retention passes, errors)
```

**Retention semantics**: `--retention-days 30` performs a pass at startup and then hourly in a background thread, dropping **whole time-series chunks** older than the cutoff (never partial chunks; graph/state/events untouched). It requires `--role writer|ingest` — a reader+retention combination is refused up front (`exit 2`).

**Choosing a role** (the one §55 derrogation): `reader` (default — writes refused `-32003`, audited) for agents/dashboards; `ingest` for telemetry plumbing (Node-RED bridge, OPC-UA gateway); `writer` when you also want `upsert_entity`/`set_state`/`retain` from that process. No network listener exists in v1 — do *not* `socat`/`ncat` the stdio onto a LAN port without understanding [the v1 no-auth limit](README.md#11-known-limits).

---

## 4. Node-RED flow: MQTT → VidgeDB ( passo a passo )

**Install** (on the Pi or the box that has the binary):

```bash
# binary first:
cd ~/vidgeDB && cargo build --release
sudo install -m 755 target/release/vidgedb /usr/local/bin/vidgedb

# the palette:
cd ~/.node-red
npm install ~/vidgeDB/sdk-nodered
# restart Node-RED → five nodes appear: vidgedb-service + 4 function nodes
```

**Import the example flow**: Menu → Import → select `sdk-nodered/examples/telemetry-ingest-flow.json` (label: *"VidgeDB — telemetry ingest (MQTT in -> vidgedb-ingest)"*). It creates:

| Node (JSON `id`) | Type | Purpose |
|---|---|---|
| `vs_plant` | `vidgedb-service` (config) | spawns `vidgedb --service /var/lib/vidgedb/plant.vdg --role ingest --agent-id node-red-mqtt-bridge`; autoRespawn true; timeoutMs 30000 |
| `mqtt_in` | `mqtt in` | topic `plant/motor77/current`, JSON datatype |
| `ing1` | `vidgedb-ingest` | entity Motor77, signal current |
| `st1` | `function` | `msg.payload = [[Math.floor(Date.now()/1000), Number(msg.payload.value ?? msg.payload)]]; return msg;` |
| `ing2` | `vidgedb-ingest` | writes the batch (entity/signal wired from node config) |
| `inj_q` → `q1` | `inject` → `vidgedb-query` | manual query path (payload = a VQL string) |
| `dbg1` | `debug` | rows out |
| `chk1` | `vidgedb-check` | `spec check current` — payload `{entity:"Motor77", signal:"current"}` |
| `sw1` | `switch` | `msg.violations[0]` non-empty → ALERT path |
| `dbg2` | `debug` | ALERT |

**Step-by-step:**

1. Configure the `vidgedb-service` config node: **Binary** `/usr/local/bin/vidgedb`, **Database** `/var/lib/vidgedb/plant.vdg`, **Role** `ingest`, **Agent ID** `node-red-mqtt-bridge`, **Auto-respawn** on. Deploy — status shows a green dot once the child is up (red ring while respawning/binary missing).
2. Point `mqtt in` at your broker and topic (`plant/motor77/current`, QoS 1; payload either a bare number or JSON containing `value`).
3. Keep the tiny `function` node between them (timestamps must be **integer unix seconds**). It converts one MQTT message → one `[ts, value]` pair.
4. `vidgedb-ingest` accepts a bare pair (uses its configured entity/signal) or a full `{entity, signal, points:[[ts,v],…]}` override. The series (`Motor77.current`) is created on the first write; points enter with `Observation` provenance semantics.
5. Wire the check branch: `vidgedb-check` reads `{entity, signal, from?, to?}` from `msg.payload`, emits `msg.violations = [result]` **only** on status `VIOLATION` (OK/NO_DATA/NO_SPEC → empty; check errors surface in `msg.checkError` without throwing). Your `switch` on `msg.violations[0]` gates the ALERT.
6. Query path: put a VQL string in `msg.payload` (e.g. `MATCH (m:Motor) RETURN m`) → `vidgedb-query` → `msg.payload` = rows array, `msg.n` = row count; parse problems appear in `msg.queryError` (flows keep running).
7. Deploy. Verify from a shell:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"get_measurements","params":{"entity":"Motor77","signal":"current","from":0,"to":99999999999}}' \
  | vidgedb --service /var/lib/vidgedb/plant.vdg
```

**From OPC-UA instead of MQTT:** keep the same shape — any node that produces `msg.payload = <number>` (e.g. `node-red-contrib-opcua` in read mode) can replace `mqtt in`; the shaper function and `vidgedb-ingest` stay identical.

---

## 5. The complete AI diagnostician (Python, < 70 lines)

This is the entire agent loop the database was designed for: **check → trace → report hypotheses with provenance** — read-only, zero dependencies beyond the PyPI SDK.

```python
# diagnose.py — one machine, one window, one honest report. (63 lines, runnable)
import sys
from vidgedb import VidgeDB

MACHINE = sys.argv[1] if len(sys.argv) > 1 else "Motor42"
DB_FILE = sys.argv[2] if len(sys.argv) > 2 else "/var/lib/vidgedb/plant.vdg"
FRM, TO = 1760000000, 1760003600     # or int(time.time())-3600, int(time.time())

db = VidgeDB(DB_FILE, agent_id="diagnostician")   # Reader role by default
schema = db.schema()
print("twin inventory: types={} topologies={}".format(
    schema["entity_types"], schema["relation_topologies"]))

def key_of(name):                                # name -> stable key
    for t in schema["entity_types"]:
        vql = 'MATCH (e:' + t + ') WHERE e.name = "' + name + '" RETURN e'
        for row in db.query(vql)["rows"]:
            return row["e"]["key"]

key = key_of(MACHINE)
if key is None:
    print("unknown entity " + MACHINE); sys.exit(1)
card = db.call("get_entity", key=key)
print("component: {} type={} props={}".format(card["name"], card["type"],
                                              card["properties"]))

findings = []
for series in schema["series"]:                  # check every signal of the machine
    if not series.startswith(MACHINE + "."):
        continue
    sig = series.split(".", 1)[1]
    rep = db.check(MACHINE, sig, from_=FRM, to=TO)
    print("check {}{} {} expected={} observed={} dev={} pts={} [exp:{}/obs:{}]".format(
        sig, "!" if rep.is_violation else ".", rep.status, rep.expected_max,
        rep.observed, rep.deviation, rep.points_checked, rep.expected_provenance,
        rep.observed_provenance))
    if rep.is_violation:
        findings.append(rep)

ups = [r["from"] for r in card["relations_in"]]  # upstream dependencies
for up in ups:
    tr = db.trace(up, MACHINE, max_hops=3)
    hops = [s["topology"] + ":" + s["relation_type"] for s in tr["steps"]]
    print("trace {}->{}: found={} hops={}".format(up, MACHINE, tr["found"], hops))

if findings:                                     # OUTPUT ONLY — never persisted (§29)
    print("== hypotheses (OUTPUT ONLY - never persisted, provenance=Hypothesis, spec s29) ==")
    for rep in findings:
        print("  hyp: {}.{} observed {} vs spec {}{} (dev {}) -> plausible upstream "
              "cause in {} (confidence 0.5, source vidgedb_diagnose_v0)".format(
                  rep.entity, rep.signal, rep.observed, rep.expected_max,
                  rep.unit or "", round(rep.deviation, 2), ups))
else:
    print("no violations in window; nothing to hypothesize")
audit = db.audit()                               # read-only fingerprint
print("audit trail: n={} agent={!r}".format(audit["n"],
                                            audit["entries"][0]["agent_id"]))
db.close()
```

*(This exact agent — 63 lines — was executed end-to-end against the demo twin while writing these docs.)*

Real output (demo twin: Motor42 with spec max 10 A, telemetry peaking 11.7 A):

```text
twin inventory: types=['Drive', 'Motor', 'PLC', 'Pump'] topologies=['electrical', 'mechanical', 'network']
component: Motor42 type=Motor props={'spec.current.max': '10', 'vendor': 'ABB'}
check current! VIOLATION expected=10.0 observed=11.7 dev=1.6999999999999993 pts=6 [exp:Specification/obs:Observation]
trace Pump17->Motor42: found=True hops=['mechanical:coupled_to']
== hypotheses (OUTPUT ONLY - never persisted, provenance=Hypothesis, spec s29) ==
  hyp: Motor42.current observed 11.7 vs spec 10.0 (dev 1.7) -> plausible upstream cause in ['Pump17'] (confidence 0.5, source vidgedb_diagnose_v0)
audit trail: n=7 agent='diagnostician'
```

The printed hypothesis cites **both provenance sides** (`Specification` expected vs `Observation` observed) — that's what makes the agent's final answer inspectable. The database stores **none** of these hypotheses; your agent's memory/CMMS ticket is where they belong, with the `audit()` fingerprint proving what the agent queried.

---

## 6. Verification checklist after deployment

```bash
vidgedb smoke                                   # engine self-check: 2 violations
echo 'not-json' | vidgedb --service /var/lib/vidgedb/plant.vdg
# →  {"id":null,"jsonrpc":"2.0","error":{"code":-32700,…}}  (and the stream keeps serving)
vidgedb --service /var/lib/vidgedb/plant.vdg   # REPL: schema / MATCH … / exit
```

- File grew? `ls -la /var/lib/vidgedb/plant.vdg*` (`.vdg` + `-wal` sidecar appears while the writer is alive; it is clean after a graceful EOF shutdown).
- RAM: expect ≈12 MB for a 10-machine/200 K-point twin (measured 11 752 kB; see [README benchmarks](README.md#10-benchmarks)); `--retention-days 30` keeps it bounded on always-on edge boxes.
# OPC-UA Reference — VidgeDB Twin Server (Phase 16)

The native OPC-UA server exposes your `.vdg` twin to standard SCADA/industrial
clients: **UaExpert, Siemens TIA Portal, KEPServerEX, AVEVA, node-red-contrib-opcua**.
Wire-tested against the `opcua` 0.12 Rust crate (client + server).

## Launch

```bash
vidgedb --opcua plant.vdg --opcua-port 4840 --opcua-poll-ms 1000
# combinable with HTTP in one process (one twin, two protocols):
vidgedb --opcua plant.vdg --http plant.vdg --opcua-port 4840 --port 8888
```

- Endpoint: `opc.tcp://<bind>:<opcua-port>/` (bind default `127.0.0.1`)
- Poll cycle: `--opcua-poll-ms` (default 1000) — each tick opens a fresh
  AgentApi Reader and pushes the LAST point of every series into its Variable
  (monitored-item clients see live data at ≤ poll interval + subscription lag)

## Browse tree (namespace 2, `urn:vidgedb:<file-stem>:twin`)

```text
Root (ns=0;i=85)
└── VidgeDB (ns=2;i=1)                # folder
    ├── PLC (folder)                  # one folder per entity type
    │   └── PLC01 (Object, ns=2;i=1000+key)
    │       └── DRIV01 (Object)       # topology → HasComponent children
    │           └── EMOT01 (Object)
    │               ├── current      (Variable, Double,  ns=2;i=2001)
    │               └── temperature  (Variable, Double,  ns=2;i=2002)
    ├── Motor (folder)
    ├── Robot (folder)  → ROB01…
    └── Sensor (folder) → SEN01…
```

- **Node ids are deterministic** (`1000+entity_key`, `2000+series_index`) — a
  client can pin addresses across restarts.
- **Relations become the hierarchy** (`HasComponent`): browsing `PLC01` finds
  `DRIV01` and `ROB01` underneath (parent = topology source, child = destination).
- **Series ownership rule** (`<owner>.<signal>`): drive readbacks are published
  on the MOTOR's object (the variable is about the motor), sensor-local series
  stay on the sensor.
- **EngineeringUnit**: an entity prop `spec.<signal>.unit` adds an EUInformation
  property to the Variable. Absent unit ⇒ NO unit served (never a wrong one).

## Connecting from UaExpert (typical industrial workflow)

1. Server → **Add** (Endpoints discovery), Custom Discovery URL
   `opc.tcp://127.0.0.1:4840`
2. Security policy **None** → Connect (anonymous)
3. Browse address space → namespace **`urn:vidgedb:…:twin`** (ns=2)
4. Drag a Variable into the data access view → live values
5. Optional: right-click → **Subscribe (monitored item)** → values update
   every ≤ 1 s (or your `--opcua-poll-ms`)

TIA Portal / KEPServerEX: same discovery + anonymous/None policy; the twin
appears as a standard machine tree next to your S7 devices.

## Minimal Rust client (30 lines)

```rust
use opcua::client::{Client, SessionState};
use opcua::types::{EndpointDescription, ApplicationType};

fn main() {
    let mut client = Client::new(
        opcua::client::ClientConfig::new(ApplicationType::Client, "vidge-client", "vidge"),
    );
    let endpoint = EndpointDescription::new(
        "opc.tcp://127.0.0.1:4840".into(),
        String::new(),                      // user token = anonymous
        "http://opcfoundation.org/UA/SecurityPolicy#None".into(),
        opcua::types::MessageSecurityMode::None,
        opcua::types::UserTokenPolicy::anonymous(),
    );
    client.add_endpoint(endpoint.clone(), None, None);
    // Connect, browse ns=2, read / subscribe variables…
    // See tests/phase16_opcua.rs for a complete working example
    // (browse + read + monitored-item live update).
}
```

A complete working client — connect, browse, read, subscribe, receive live
datachanges — lives in `tests/phase16_opcua.rs` (kept green in CI).

## Reads vs writes

The OPC-UA surface is **read-only by design** (the same provenance/trust rules
as the HTTP/stdio API). Ingestion goes through:

```bash
# stdio (machine-local gateway):
echo '{"jsonrpc":"2.0","id":1,"method":"ingest_points","params":{…}}' \
  | vidgedb --service plant.vdg --role ingest --agent-id gateway
# HTTP (network):
curl -X POST http://<host>:8888/rpc -d '{"jsonrpc":"2.0","id":1,"method":"ingest_points",…}'
```

Readers (OPC-UA) never contend with writers: the write lock (`*.vdg-wlock`)
only covers write-role sessions.

## Limits (v1, documented per docs/README.md §11)

- **SecurityPolicy None only, anonymous only** — designed for an ISOLATED
  machine LAN (the classic industrial deployment). TLS + user auth arrive
  with the HTTPS/TCP wrapper roadmap item; do NOT expose `--bind 0.0.0.0`
  outside an isolated plant network.
- **Read-only** — no OPC-UA write methods are exposed in v1.
- **Fresh-reader poller** — the poller reads the twin with a fresh Reader each
  tick (the Phase-15 staleness law: a long-lived reader session never sees
  other processes' commits). Result: OPC-UA clients see other writers'
  ingest within one poll tick.
- **Points must be ingested in the past/present** — the poller window is
  `[now − 3600 s, now + 1 s]`; future-dated points are invisible to OPC-UA
  until their timestamp arrives (matches the temporal engine's event-time
  semantics, spec §24).
- Static musl build verified (`cargo check --target aarch64-unknown-linux-musl`
  with the 2 GB swap; `vendored-openssl` compiles OpenSSL statically).
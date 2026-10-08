# Acquisition gateway — many sources, many protocols

*How to connect a real machine to VidgeDB without weighing down the engine: acquisition
drivers live **outside** the binary and speak JSON-RPC. A working PoC is described in §2.*

---

## 1. The distinction that matters: two roles, not one

Before choosing "in the database or beside it", separate **the two jobs the word "OPC-UA"
covers**:

| Role | Who does it | Where it belongs |
|---|---|---|
| **Client** — read a machine (OPC UA, Modbus, S7, MQTT…) | us | **outside the engine**: a gateway |
| **Server** — *expose* the twin so a third party can read it (TIA, UaExpert) | us | inside the binary (it is a way of publishing the data) |

The gateway under discussion (reading a PLC, an IO-Link master, a drive) is the **client
role** — and there the reasoning holds: **it has no business being inside the database
binary.** A database should not know about protocols; it should receive measurements and
answer questions.

The native OPC-UA server is the other role: it reads nothing at all, it **serves** the
twin. It is legitimate for an S7-1500 or a supervisory system that speaks OPC UA — but it
is not a driver, and it does not replace a gateway.

## 2. Working PoC: Modbus TCP → Python gateway → VidgeDB

**This is tested, not suggested.** Three parts, two packages:

```text
[simulated PLC, pymodbus]  --Modbus TCP-->  [Python gateway]  --JSON-RPC (SDK)-->  [vidgedb]
   register 0 = 28 (2.8 A)                   script loop          upsert + ingest
   register 1 = 30 (3.0 A)                                        then check()
```

Files: `examples/gateway/poc_simulated_plc.py` (the "PLC") and
`examples/gateway/poc_modbus_to_vidgedb.py` (the gateway). Real output:

```text
MODBUS READ: register0=28 register1=30
SDK: {'created': True, 'key': 0, 'relations_added': 0}
SDK: {'accepted': 1, 'chunks_flushed': 1, 'series_id': 0}
CHECK: status=OK observed=2.8 expected_max=3.0 dev=-0.20000000000000018
PROVENANCE: observed=Observation expected=Specification
GATEWAY_OK
```

What this demonstrates is the point: **the separation of responsibilities holds.**

- `pymodbus` handles the **protocol** (the bytes on the wire);
- the `vidgedb` SDK handles the **database** (JSON-RPC to the binary);
- the **gateway** is a register → series mapping loop, and that is where the domain
  knowledge lives (scale factors, addresses, periods).

The engine itself learned nothing about Modbus: it received a measurement and weighed the
`Specification` (3.0 A, written from the registers) against the `Observation` (2.8 A, read
off the wire). That is exactly the intended behaviour.

## 3. What each protocol actually requires (verified, not assumed)

| Protocol | **Client** side (read) | **Server/device** side (be a machine) |
|---|---|---|
| **Modbus TCP** | ✅ `pymodbus` — **done in the PoC** (3.15.0, zero native dependency) | ✅ doable: the PoC runs a Modbus server in Python. Enough for bench work. |
| **Modbus RTU** (RS-485) | ✅ `pymodbus` + `pyserial` | ⚠️ feasible but **tight timing** (frame timeout); for a real slave role, prefer a hardware converter |
| **OPC UA** | ✅ `asyncua` (Python client) | ✅ (this is what the binary does, and what an S7-1500 does) |
| **S7 (Siemens)** | ✅ `python-snap7` — **pure Python since 3.0**, no native library, Windows/Linux/macOS/ARM | ❌ makes no sense |
| **MQTT** | ✅ `paho-mqtt` | — |
| **PROFINET** | ⚠️ **no** — see below | ❌ **not in Python** |
| **IO-Link** | ✅ indirectly: read the **IO-Link master** through its uplink (PROFINET/Modbus/OPC UA) | ❌ the master role requires a hardware component |

**PROFINET, the hard point — no wishful thinking.** Real-time PROFINET (RT/IRT) **does not
go over TCP/IP**: the frame leaves directly from layer 2 (EtherType `0x8892`) to the
application, precisely to avoid the latency and jitter of the IP stack. PI (PROFIBUS &
PROFINET International) documents three levels — TCP/IP for configuration, RT for process
data, IRT for motion — and CC-A/B/C certification requires **dedicated stacks** (ASIC or
FPGA mandatory at CC-C/IRT). In other words: **you do not build a PROFINET device in
Python**, and you do not build one "by going through a service" either — it is a card, a
certified stack, a GSDML file, and a trip through a test lab.

The practical consequence, and this is the good news: **we do not need one.** On a PROFINET
line the PLC is the master; we **read the PLC** over OPC UA or S7, or we read an IO-Link
master through its uplink. The real-time layer stays where it belongs — in the PLC.

## 4. The architecture chosen: a driver-based gateway

```text
vidge-gateway (Python, a separate deliverable with its own dependencies)
├── drivers/
│   ├── modbus.py     (TCP/RTU client — pymodbus)     ← proven
│   ├── opcua.py      (client         — asyncua)
│   ├── s7.py         (client         — python-snap7)
│   └── mqtt.py       (client         — paho-mqtt)
├── mapping/          one file per machine:
│                     register / tag / node → VidgeDB series (+ scale, unit, period)
├── buffer/           a local queue (the network drops ≠ we lose the measurement)
└── cli               `vidge-gateway run machine.yaml`
```

Three rules, in the spirit of the "small binary / SDK around it" decision:

1. **The engine knows no acquisition protocol.** It receives measurements. The only
   protocol it carries is the one it *serves* (OPC UA, HTTP, stdio) — a different job.
2. **The gateway is a separate deliverable**, not a dependency of the SDK. The SDK keeps its
   "zero dependency" promise; the gateway, for its part, owns its drivers
   (`pip install vidge-gateway` bundles pymodbus/asyncua/etc.).
3. **The mapping is data, not code.** One file per machine: that is what lets an integrator
   add a line without touching the engine.

## 5. What becomes of the native OPC-UA server

It stays, but its place changes:

- it is already behind the `opcua` feature (optional, **12.8 MiB on its own**) — so it costs
  the 1.31 MiB core **nothing**;
- it keeps a real use case: the **S7-1500** exposes a native OPC-UA server, and the binary
  can then publish the twin to TIA/UaExpert with no gateway;
- but it must **not** be the default acquisition path: to read a machine, the Python gateway
  is more flexible (several protocols, several sources, per-file mapping).

In other words: the instinct about **acquisition** was right, and the `opcua` feature covers
the one case where the native server is justified. The two do not exclude each other.

## 6. The concrete next step

1. **`vidge-gateway`**: extract the PoC into a package (drivers + YAML mapping + buffer +
   supervision).
2. **A second proven driver**: OPC UA (`asyncua`) or S7 (`python-snap7`) — same method, a
   simulated PLC on the other side.
3. **The mapping as a format**: it is what makes the gateway usable by an integrator.

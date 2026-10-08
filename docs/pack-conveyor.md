# Machine Pack #1 — CONVEYOR LINE (PLC / Drive / Motor / Belt / 4 Sensors / Palletizing Robot)

**The first complete machine-twin for Vidge Plant Management** — a minimalist AND
realistic conveyoring line, loadable into a VidgeDB `.vdg` twin through one
declarative JSON file and one stdlib-only Python loader. It serves three purposes:

1. **Product demo** — a self-contained plant line with topology, telemetry,
   specs, alarms and three ready-made stories (healthy / overloaded / overheated).
2. **Course fixture** — teaches digital-twin modeling on an example every
   technician knows, including the engine's honest constraints (the 44-byte
   inline-props cap is *part of the lesson*, see §7).
3. **Dashboard base** — every panel query of a conveyor-line dashboard is
   written out in §8, all executed against the real binary.

Lineage: spec.md §58 (PLC01 → Drive12 → Motor42 → Pump17) generalized to
machine-agnostic entity types — nothing in the engine knows "pump" or
"conveyor"; the pack carries the machine's identity.

- Pack JSON: [`examples/pack-conveyor.json`](../examples/pack-conveyor.json)
- Loader: [`scripts/pack_load.py`](../scripts/pack_load.py) (Python stdlib only)
- Rust e2e test: [`tests/phase15_pack.rs`](../tests/phase15_pack.rs)
- Format version: `pack: "conveyor", pack_version: 1`

---

## 1. The machine

```text
        profinet (network:profinet)              packing zone
   ┌───────────────────────────────────┐              ▲ pieces out
   │                                   │              │
┌──┴───┐   electrical:feeds   ┌───────┴──┐    mechanical:drives   ┌─────────┐
│PLC01 │─────────────────────▶│ DRIV01   │───electrical──────────▶│ EMOT01  │
│S7-120│                     │ SINAMICS │                       │ 1.5 kW  │
└──┬───┘                      │  G120    │                       │ 1500rpm │
   │ network:profinet         └──────────┘                       │  2.9 A  │
   ▼                                           ┌──────────────└────┬────┘
┌──────┐                                       │                   │ mechanical
│ROB01 │◀── takes pieces off the belt ─────────│── CONV01 ◀────────┘ :drives
│6-axis│   (process:palletizes)                │   3.5 m, PU belt
│ 12 kg│                                       │   0.2–0.5 m/s
└──────┘                                       └──────▲──────▲──────
                                                      │      │ instrumentation
                                               (SEN01 pres, SEN02 metal to CONV01;
                                                SEN03 vib, SEN04 temp to EMOT01)
```

Cleaner edge-by-edge (the 9 relations the pack writes):

```text
 PLC01 ──network:profinet────▶ DRIV01      (drive telegram over profinet)
 PLC01 ──network:profinet────▶ ROB01       (robot control over profinet)
 DRIV01 ─electrical:feeds────▶ EMOT01      (3-phase power)
 EMOT01 ─mechanical:drives───▶ CONV01      (shaft → belt)
 SEN01 ─instrumentation:measures▶ CONV01   (photoelectric piece presence, 24 V)
 SEN02 ─instrumentation:measures▶ CONV01   (inductive metal detect, 24 V)
 SEN03 ─instrumentation:measures▶ EMOT01   (case vibration, 4–20 mA → mm/s)
 SEN04 ─instrumentation:measures▶ EMOT01   (PT100 body temperature, via drive)
 ROB01 ─process:palletizes────▶ CONV01     (line-end palletizing)
```

## 2. Entity inventory (9 entities, all `source=“plc”` ⇒ relations enter as **Fact**)

| Entity | Type | Key properties | Role |
|---|---|---|---|
| PLC01 | `PLC` | vendor=Siemens, model=S7-1200, fw=V4.4 | line controller; owns both profinet links |
| DRIV01 | `Drive` | vendor=Siemens, model=SINAMICS-G120 | VFD feeding EMOT01; current/speed readbacks |
| EMOT01 | `Motor` | `spec.current.max`=3, `spec.temperature.max`=60 | 1.5 kW 3-phase, 1500 rpm, 2.9 A nominal |
| CONV01 | `Conveyor` | belt=PU, len_m=3.5, v_min=0.2, v_max=0.5 | belt section 1, M/M construction |
| SEN01 | `Sensor` | kind=photoelectric, supply=24V | piece presence at belt start |
| SEN02 | `Sensor` | kind=inductive, detect=metal | metal detect mid-belt |
| SEN03 | `Sensor` | kind=accel, `spec.vibration.max`=2.5 | motor-case vibration (ISO 10816-3 A/B = 2.5 mm/s) |
| SEN04 | `Sensor` | kind=PT100, loop=4-20mA | body temperature into the drive analog input |
| ROB01 | `Robot` | vendor=ABB, axes=6, payload_kg=12 | end-of-line palletizer, 12 kg |

Entity types are free strings in VidgeDB — `PLC`, `Drive`, `Motor`, `Conveyor`,
`Sensor`, `Robot` are *this pack's vocabulary*, not the engine's.

## 3. Specifications (the `specs` table → entity props the CHECK engine reads)

| Entity | Signal | Max | Unit | Rationale |
|---|---|---|---|---|
| EMOT01 | current | **3.0 A** | A | nominal 2.9 A × 1.05 service margin (IEC 60034 S1) |
| EMOT01 | temperature | **60 °C** | C | PT100 body limit, class F winding / B rise budget @ 40 °C ambient |
| SEN03 | vibration | **2.5 mm/s** | mm/s | ISO 10816-3 / 20816-3 zone A→B for small machines |

`check` reads `spec.<signal>.max` from the *entity props* of the series
owner (EMOT01 for current/temperature, SEN03 for vibration). Units are kept in
the pack + doc table only — see §7 (the props cap).

## 4. Sensors & signals (6 series)

Series naming is the VidgeDB convention `<entity>.<signal>`; the pack states
*who* measures *what* and where the value lands:

| Series | Producer | Measured entity | Unit | Notes |
|---|---|---|---|---|
| `EMOT01.current` | DRIV01 readback | EMOT01 | A | phase current RMS over profinet (the drive reports the motor's draw) |
| `EMOT01.temperature` | SEN04 → DRIV01 | EMOT01 | C | PT100 on the G120 analog input; value lands on the motor series |
| `SEN03.vibration` | SEN03 | EMOT01 (case) | mm/s | scaled at the sensor; **series stays sensor-local** |
| `CONV01.speed` | DRIV01 (encoder) | CONV01 | m/s | belt speed 0.2–0.5 m/s |
| `SEN01.presence` | SEN01 | CONV01 | 0/1 | one pulse per piece |
| `SEN02.metal` | SEN02 | CONV01 | 0/1 | metal flag per piece |

Ownership rule (deliberate + worth teaching): *drive readbacks are published on
the motor, sensor-local measurements keep their own series.* This is what makes
`CHECK EMOT01.current …` and `diagnose(EMOT01)` work as the spec §26 pipeline
expects, while vibration stays attributable to its own instrumentation chain.

## 5. Alarm rules (computed, not hand-written)

The loader evaluates each rule over the generated points and logs **real
threshold-crossing events** (`rise` and, when the value comes back under, `clear`)
with `provenance = Event`:

| Rule | Series | Threshold | Event detail template |
|---|---|---|---|
| alarm_current_max | EMOT01.current | > 3.0 A | `I={obs}A>{thr}A` |
| alarm_temperature_max | EMOT01.temperature | > 60 °C | `T={obs}C>{thr}C` |
| alarm_vibration_max | SEN03.vibration | > 2.5 mm/s | `V={obs}>{thr}` |

The pack JSON carries only the rule; the events are computed in
`scripts/pack_load.py::crossing_events` — the same discipline the engine
follows: nothing enters the log that did not happen on the data.

## 6. Demo scenarios (3 × 500 pts × 6 series, deterministic)

All three sample at `dt = 7 s` ([ts_start .. ts_start+3493]) with per-series
seeds derived from `(scenario_index, series_index)` — two loads of the same
pack produce byte-identical twins (the Rust test reproduces the same verdicts
from pure Rust, see §9.3). Absolute windows in this section are the pack
defaults (`ts_base = 1 760 000 000`):

| Scenario | Window (unix) | Story | Signal bands |
|---|---|---|---|
| **normal** | 1 760 000 000 .. +3600 | healthy hour, ~1 piece/30 s | 2.5–2.9 A · < 2 mm/s · 40–55 °C · speed 0.40–0.47 m/s |
| **overload** | 1 760 007 200 .. +3600 | stick-slip jam of the belt rollers: current spikes into the 3–4 A zone between partial recoveries; belt sags to ~0.35 m/s | 2.75–4.1 A (**spec 3.0 → VIOLATION**) · vibration 2.3–3.5 mm/s (**spec 2.5 → VIOLATION**) · temp to 58 °C (OK) |
| **overheating** | 1 760 014 400 .. +3600 | cabinet fan fault at +60 s (`cooling_fault`), body crosses 60 °C at ~+590 s and keeps climbing; drive trips thermally at ~+3250 s (`thermal_trip`, state = TRIPPED_THERMAL, belt stops), body falls back below 60 °C | 65–80 °C (**spec 60 → VIOLATION**) · current 2.6–2.9 A then ~0 after the trip (OK) · vibration < 2 mm/s (OK) |

Captured verdicts (from the validated load — §9.1; these are the numbers a
dashboard panel would show):

```text
          CHECK EMOT01.current  CHECK EMOT01.temperature  CHECK SEN03.vibration
normal:   OK    2.82 A (−0.18)   OK    52.9 °C (−7.1)      OK    1.59 mm/s (−0.91)
overload: VIOL  4.07 A (+1.07)   OK    58.0 °C (−2.0)      VIOL  3.49 mm/s (+0.99)
overheat: OK    2.85 A (−0.15)   VIOL  80.0 °C (+20.0)     OK    1.79 mm/s (−0.71)
```

Event chronology (provenance `Event`): normal — `run_start`, `pallet_full`,
`run_end` (on CONV01/ROB01); overload — `run_degraded` + computed
`alarm_current_max` rise/clear ×3 + `alarm_vibration_max` rise ×1;
overheating — `cooling_fault`, temp-rise alarm, `thermal_trip`, temp-clear
alarm (the full fault arc). Pack totals per load: **9 entities, 9 relations,
11 states, 20 events, 9 000 points**.

## 7. The 44-byte props cap (why the specs look like this)

The Phase 2 layout inlines entity properties (`k=v\0` list, `ENTITY_CELL−20` =
44 bytes). The pack *embraces* the constraint instead of hiding it:

- `EMOT01` props: `spec.current.max=3` (19 B) + `spec.temperature.max=60`
  (24 B) = **43 B ≤ 44 B** — exactly at the cap; `spec.current.max=3.0` would
  be 45 B and the loader would (correctly) refuse it.
- Unit props (`spec.<signal>.unit`) don't fit next to the two spec values, so
  units live in the pack + §3 table.
- The vibration spec rides on **SEN03** (its series owner, whose props block
  is at 34 B).

This is a real lesson for pack authors: *at the storage
edge, every byte has a landlord.*

## 8. Pack file format (v1)

```jsonc
{
  "pack": "conveyor", "pack_version": 1,
  "ts_base": 1760000000,
  "entities":    [ { "name": "PLC01", "type": "PLC", "props": {…}, "doc": "…" } ],
  "relations":   [ { "from": "PLC01", "to": "DRIV01", "type": "network:profinet" } ],
  "specs":       [ { "entity": "EMOT01", "signal": "current", "comparison": "max", "value": 3.0, "unit": "A" } ],
  "sensors_signals": [ { "sensor": "SEN04", "measures_entity": "EMOT01",
                         "signal": "temperature", "series": "EMOT01.temperature",
                         "unit": "C", "kind": "PT100 …" } ],
  "alarm_rules": [ { "name": "alarm_current_max", "series": "EMOT01.current",
                     "threshold": 3.0, "detail": "I={obs}A>{thr}A",
                     "detail_clear": "…", "entity": "EMOT01", "signal": "current" } ],
  "states_packbase": [ { "entity": "CONV01", "key": "state", "value": "STOPPED",
                         "offset_s": -60 } ],
  "demo_scenarios": [ { "name": "normal", "ts_start": 1760000000,
                        "points_per_series": 500, "dt_s": 7,
                        "signals": { "<entity>.<signal>": {shape…} },
                        "states": [...], "events": [...],
                        "description": "…" } ],
  "notes": { … }   // design commentary, provenance rules, determinism
}
```

Signal shapes (the loader's deterministic generator, stdlib only):

- `wave` — `base + amp·sin + noise`, clamped `[min,max]` (healthy regimes);
- `waypoints` — piecewise-linear through `(frac, value)` knots + noise (jam /
  overheat / fault arcs: the story bends when the physics changes);
- `pulse` — piece-cadence square wave with proportional edge jitter.

## 9. Quickstart (10 lines — load, query, dashboard)

The 10-line quickstart (4 load / 6 read is the split; the read block below is
exactly the captured session of §10.2):

```bash
cd ~/vidgeDB && cargo build --release --bin vidgedb
python3 scripts/pack_load.py examples/pack-conveyor.json /tmp/conveyor.vdg   # load + verify (PACK_LOAD_OK)
python3 scripts/pack_load.py examples/pack-conveyor.json /tmp/conveyor.vdg --transport http --port 8917
```

```bash
T="/tmp/conveyor.vdg"; B=target/release/vidgedb
printf '%s\n' \
 '{"jsonrpc":"2.0","id":1,"method":"query","params":{"vql":"MATCH (p:PLC) -[:network]-> (r:Robot) RETURN p, r"}}' \
 '{"jsonrpc":"2.0","id":2,"method":"check","params":{"entity":"EMOT01","signal":"temperature","from":1760014400,"to":1760018000}}' \
 '{"jsonrpc":"2.0","id":3,"method":"query_temporal","params":{"vql":"MATCH (m:Motor) WHERE m.name = \"EMOT01\" MEASURE m.temperature DURING last(1h) RETURN max(m.temperature), min(m.temperature), avg(m.temperature)","now":1760018000}}' \
 | $B --service $T --role reader
```

Dashboard queries (paste into the same pipe):

```sql
-- Line health panel: the three spec verdicts of the LAST hour
CHECK m.current FOR (m:Motor) WHERE m.name = "EMOT01" DURING last(1h) RETURN status, deviation     -- (library-level statement, tests/phase64)
-- Dashboard panels via the service (absolute windows):
-- (edge MATCH binds the TOPOLOGY class — network, electrical…, not the relation type)
MATCH (d:Drive) -[:electrical]-> (m:Motor) -[:mechanical]-> (c:Conveyor) RETURN d, m, c            -- energy path panel
MATCH (s:Sensor) -[:instrumentation]-> (m:Motor) RETURN s, m                                        -- instrumentation panel
MATCH (p:PLC) -[:network]-> (r:Robot) RETURN p, r                                                   -- automation panel
MATCH (m:Motor) WHERE m.name = "EMOT01" MEASURE m.temperature DURING last(1h) RETURN max, avg       -- temperature panel
CHECK (service) entity=SEN03 signal=vibration from=t1 to=t2                                         -- vibration gauge → OK/VIOLATION
```

Notes: `--transport http` starts the Phase 14 HTTP endpoint
(`POST /rpc`, `GET /health`) — the two transports share ONE method dispatch.
A verify stage opens the twin twice (writer → reader) by design: the engine's
read-after-commit reads must go through a fresh open (known staleness class,
documented in the codebase skill).

## 10. Validation — what was executed (captured, not invented)

1. **Load** — `PACK_LOAD_OK: {"entities": 9, "events": 20, "points": 9000,
   "relations": 9, "states": 11}` on a fresh twin via the release binary,
   both stdio writer→reader and HTTP.
2. **VQL verification** — MATCH patterns (3), `trace(PLC01→ROB01)` = found,
   1 hop, `topology=network`; per-scenario `check` ×9 verdicts (table §6);
   `MEASURE EMOT01.temperature DURING <window> RETURN max/avg/count` per
   scenario (count = exactly 500 each; e.g. overheat: max 80.0, avg 68.63).
3. **DIAGNOSE** — the §26 8-step pipeline on EMOT01 in the overheat window
   (via the same public entry point `diagnose::diagnose` that
   tests/phase71_diagnose.rs exercises): component (2 spec props), 3 upstream
   (DRIV01 feeds, SEN03/SEN04 measures — relation provenance Fact), 1
   downstream (CONV01), 3 topology classes, measurements 500 pts each,
   checks = current OK / temperature **VIOLATION (dev +20.0)**, exactly 1
   anomaly, 1 causal hypothesis (provenance Hypothesis, source
   `vidgedb_diagnose_v0`, upstream = DRIV01/SEN03/SEN04) — output only,
   never written (spec §29).
4. **tests/phase15_pack.rs** — 3 tests green: the contract e2e over the real
   spawned binary, the Rust replay of the scenario verdicts (seeds from the
   pack are reproducible in Rust), and the pack-JSON consistency/props-cap
   assertions.
5. Full suite: `cargo test` — `VIDGEDB_BIN=$PWD/target/release/vidgedb cargo test --release` — **227 passed / 0 failed** (counts move as phases land; see docs/deployment.md for the authoritative procedure);
   the "202" in the task context matches the Phase-14 commit message; the
   fine-grid probe suite has been consolidated to one test since), zero
   build warnings.
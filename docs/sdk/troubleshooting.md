# Troubleshooting

*Ordered by how often each thing actually bites. Every symptom below was observed on the
real engine, not imagined.*

---

## 1. "It says the binary is not found"

```
FileNotFoundError: [Errno 2] No such file or directory: 'vidgedb'
```

The client resolved the engine to the bare name and there is none on `PATH`. Resolution
order is **explicit argument → `$VIDGEDB_BIN` → `PATH`**. Fix it with an absolute path:

```bash
export VIDGEDB_BIN=/opt/vidgeDB/target/release/vidgedb
```

A long-lived service has no useful `PATH`: always set `VIDGEDB_BIN` in the unit file or
the container environment.

## 2. Reading errors: two channels, not one

| What you see | Where it is | What to do |
|---|---|---|
| `{"error": {"code": -32601, "message": "unknown method \"foo\""}}` | transport | the method does not exist (check spelling — the method set is in [protocol.md](protocol.md) §3) |
| `{"error": {"code": -32003, ...}}` | transport | the role forbids the write; the stream is healthy, see §3 |
| `{"result": {"error": "unknown entity 'X'"}}` | semantic | the call worked; the *operation* could not be done |
| `{"result": {"error": "parse error: ..."}}` | semantic | VQL syntax |

In the shipped clients, transport errors become a typed error with the numeric code;
semantic errors become the same typed error with **code 0**. Per-package details:
`sdk-python/docs/errors.md`, `sdk-js/docs/errors.md`.

## 3. "-32003 write forbidden" — the role, not a bug

```
write forbidden (-32003): agent 'x' holds role Reader; telemetry/topology ingestion
requires opening the API with Role::Writer or Role::Ingest
```

`reader` is the **default** role, deliberately. Pass `role="writer"` or `role="ingest"`
on the client that must write. The refusal is recorded in the audit trail — proving an
agent stayed read-only is a feature, not an inconvenience.

## 4. "relation target does not exist — create it first"

```
{"result": {"error": "relation target 'CONV01' does not exist — create it first
             (an ingest agent never invents endpoints)"}}
```

You sent a relation before creating its target. **Create entities first, then
relations** (Recipe 1 shows the correct order).

⚠️ **Known defect in the shipped engine (v0.1.0), verified:** after that refusal, in the
*same* process, the half-created entity remains as key 0 with a NUL name — invisible to
VQL, `check` answers `NO_DATA` instead of `unknown entity`, and **the next valid
`upsert_entity` fails with `PageOutOfBounds`**. The file on disk is fine (reopening
recovers); the corruption is per-session, in memory. Workarounds: create the entities
in a first pass and relations in a second (which is what the reference loader does), and
restart the client process if you ever hit it. The fix belongs in the engine
(validate relation targets before opening the transaction).

## 5. "database is write-locked by process N"

```
cannot open /data/twin.vdg: database '/data/twin.vdg' is write-locked by process 7
(single-writer: ... open as a reader role or wait for the owner to exit)
```

**One writer per `.vdg` file.** Readers are always allowed. Two situations:

- **a real second writer** — stop one of them; this is the guard doing its job;
- **a stale lock after `docker stop`** — the engine releases the lock on clean exit,
  but it installs no `SIGTERM` handler (verified), so `docker stop` leaves the lock
  behind. A new container is then refused for **75 s** (the age at which the lock is
  declared abandoned and stolen, with an explicit message), after which it recovers.
  Workaround today: `docker restart` instead of `stop` + `run`, or wait. The fix is an
  orderly-signal handler in the engine.

## 6. "MATCH (x) RETURN x" returns nothing

Not an error — `{"n": 0, "rows": []}`. **A pattern with no type label matches nothing.**
Enumerate `schema.entity_types` and issue one query per type. This is the trap most
likely to make you think an empty database is a broken client.

## 7. "merged properties exceed the inline cap"

```
merged properties exceed the inline cap (45 > 44 bytes) — drop props or store them as telemetry
```

Entity properties are inlined in a 64-byte cell (~44 bytes usable). Two spec values plus
their units do not fit; keep units in your own mapping, not in the twin. The error names
the real numbers, so it is self-explanatory once you know the cap exists.

## 8. `NO_SPEC` when a spec clearly exists

`check` reads `spec.<signal>.max` from the properties of the **series owner**. For
series `Motor42.current` that is entity `Motor42`. A mismatch anywhere in the name
(`spec.current_max`, `spec.Current.max`) silently yields `NO_SPEC` — there is no
namespace to validate against. Print `get_entity(key)["properties"]` to see what is
really stored.

## 9. A float timestamp

```
-32602 invalid params: points: ts must be an integer, ...
```

Timestamps are integer unix **seconds**. `time.time()` returns a float: cast with
`int(...)`.

## 10. The stream died mid-session

The engine exits on **EOF of stdin** (that is the documented shutdown) and panics on
nothing else. If your process closes the pipe, or the child is killed, the client marks
itself as failed and later calls raise rather than hang. Check `db.is_running()` (Python)
/ `db.isRunning()` (JS) before blaming the engine. In Node-RED, the config node
auto-respawns after 2.5 s and shows a red ring while down.

## 11. Still stuck — the three commands that answer most questions

```bash
"$VIDGEDB_BIN" --version      # which build is it?
"$VIDGEDB_BIN" smoke          # is the engine itself healthy? (2 violations expected)
echo '{"jsonrpc":"2.0","id":1,"method":"schema","params":{}}' | \
  "$VIDGEDB_BIN" --service twin.vdg --role reader   # what is actually in this file?
```

If `smoke` is clean and `schema` looks right, the problem is in the client code or the
role — not in the database.

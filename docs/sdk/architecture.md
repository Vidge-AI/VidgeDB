# Architecture — what a client is, and where the truth lives

*This document describes the shape shared by all three packages. Read it once; the
per-package docs then only cover their own surface.*

---

## 1. One engine, three clients

VidgeDB is **one Rust binary**. Everything else is a client:

```text
┌────────────────────────────────────────────────────────────────────┐
│  vidgedb  (the engine — in the vidgeDB repository, NOT here)       │
│                                                                    │
│   .vdg file: graph (entities+relations) · time-series · state ·     │
│              events     — one transactional page store, WAL-first   │
│                                                                    │
│   surfaces:  --service  JSON-RPC 2.0 on stdin/stdout                │
│              --http     JSON-RPC 2.0 on POST /rpc                   │
│              --opcua    native OPC-UA server (optional build)      │
│              open       interactive REPL                            │
└────────────────────────────────────────────────────────────────────┘
        ▲                    ▲                     ▲
        │ stdio JSON-RPC     │ HTTP JSON-RPC       │ stdio (managed child)
   ┌────┴─────┐        ┌─────┴──────┐       ┌──────┴────────┐
   │ vidgedb  │        │ vidgedb    │       │ node-red-     │
   │ (Python) │        │ (npm)      │       │ contrib-vidgedb│
   └──────────┘        └────────────┘       └───────────────┘
```

The Python and JavaScript clients manage a **child process** and speak to it over
pipes. The Node-RED nodes do the same, with the process owned by a shared config node
so several nodes reuse one engine instance.

> **Transports, measured:** the engine can serve **one transport per process** for
> stdio vs HTTP (`--service` is *not* combinable with `--http`), but `--http` and
> `--opcua` **are** combinable in the same process. The Python and JS clients use
> stdio; a browser dashboard would use HTTP.

## 2. Where the truth lives

There is exactly one answer, and it is never the client:

| Thing | Lives in | Never in |
|---|---|---|
| Entities, relations, typed topology | the `.vdg` file | the client's memory |
| Telemetry points | chunks in the `.vdg` file | a Python list |
| Expected values (`spec.<signal>.max`) | entity properties in the file | your script's constants |
| Diagnostics / hypotheses | **the output of a report** | the database (by design) |

That last line is the product's central invariant: an agent may read, ingest and
reason, but the engine **refuses to store or promote a hypothesis**. A diagnostic
conclusion is a report, not a fact. A client cannot change that — there is no API for
it — which is precisely why a client is safe to hand to an LLM.

## 3. What each client does, concretely

Every client, whatever the language, provides the same three layers:

1. **Transport** — spawn (`subprocess.Popen` / `child_process.spawn`) or connect,
   write one JSON object per line, read one JSON object per line, correlate responses
   by `id`. Errors of transport become a typed error carrying the JSON-RPC code.
2. **Dispatch** — a generic `call(method, **params)` covering *all* methods, including
   ones added to the engine after the client was published. This is why a new engine
   method never requires a client release.
3. **Typed sugar** — named methods and dataclasses for the methods a human actually
   calls: `schema`, `query`, `get_measurements`, `check`, `trace`, `ingest_points`,
   `upsert_entity`, `log_event`, `get_events`, `set_state`, `state_history`,
   `retain`, `audit`, `provenance`.

The distinction matters: **layer 2 is the contract, layer 3 is convenience.** If a
typed method is missing, `call()` still reaches the engine.

## 4. Roles and safety

The engine takes a role at open time; the client only passes it through.

| Role | Can do | Cannot do |
|---|---|---|
| `reader` (**default**) | every read method | any write — refused with `-32003`, **and the refusal is audited** |
| `writer` | + ingest, topology, state, events, retention | write a hypothesis |
| `ingest` | same as `writer` (the difference is a declared intent in the audit trail) | write a hypothesis |

Two consequences for client design, both deliberate:

- **A client that only reads should never pass `role="ingest"`.** The default is
  reader for that reason. If a dashboard shows nothing when it should write, check the
  role before anything else.
- **`set_hypothesis` does not exist as a callable method in the shipped engine.** The
  refusal is implemented in the library layer but not wired into the service dispatch,
  so it currently answers `-32601 unknown method`. Treat "the engine refuses" as the
  *design* and do not build a feature that depends on the refusal message.

## 5. Versioning

| | Version | Compatibility rule |
|---|---|---|
| Engine | `0.1.0` | the wire contract is the JSON-RPC method set |
| Clients | `0.1.0` | a client of version X works with any engine that speaks the same methods |

Because dispatch is generic, **clients are forward-compatible by construction**: an
engine that adds methods keeps working with an old client (the new methods are reachable
via `call()`), and a client never has to ship to "support" a new engine feature.

The reverse is not true: an engine that *removes* or *renames* a method breaks typed
helpers. That is why the method set is treated as the frozen contract in
[protocol.md](protocol.md).

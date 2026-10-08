# VidgeDB — Contributor / agent guide

An embedded digital-twin database: one `.vdg` file, a Rust core, built so AI agents can
query a machine's topology, compare observations against specifications, and construct
traceable diagnostic hypotheses.

The repository holds the code, the tests and this documentation. The `spec §NN` notation
found in the code and comments refers to the project's **design specification**, an internal
document not versioned here: those numbers are stable anchors, not links. Where the two
disagree, **the tested code wins** — the suite in `tests/` is the runnable reference, and
`docs/` describes the observable behaviour.
**Repo: `git@github.com:Vidge-AI/VidgeDB.git`** — branch `main`.

## Navigating the codebase (graphify)

A knowledge map of the code is generated locally (tree-sitter AST, no LLM, no API):

```bash
~/.local/bin/graphify . --code-only --no-viz   # build (code only)
~/.local/bin/graphify cluster-only .           # clustering + GRAPH_REPORT.md
graphify update .                              # after changing the code
graphify query "how does the WAL commit protocol work?"
graphify path Engine Wal                       # path between two symbols
```

Outputs land in `graphify-out/` (**not versioned** — it is in `.gitignore`, because it is
regenerable and large; each contributor produces their own):
- `graph.html` — interactive graph (open in a browser)
- `GRAPH_REPORT.md` — god nodes, communities, surprising connections
- `graph.json` — the full graph, queryable

**Rule for agents:** read `GRAPH_REPORT.md` before exploring files; use `graphify query`
rather than grep. The graph is dated to the commit it was built from — if
`git rev-parse HEAD` differs, re-run `graphify update .`.

Current god nodes (the core of the design): `Engine` (61 edges), `EngineError` (59),
`GraphStore` (45), `TimeSeriesStore` (32), `Tx` (29), `Pager` (27).

## Architecture (layers, spec §17)

```text
Application
     ↓
engine.rs      — transactions (WAL-first, fsync, recovery on open)
     ↓
wal.rs         — CRC32 frames, commit marker, deterministic replay, checkpoint
     ↓
pager.rs       — 4 KiB pages, superblock VDG1, chained freelist
```

Above that:
- `model.rs` — the Phase 0 data model (Entity, Relation, provenance, validity), with the
  invariants encoded in the type system
- `stores.rs` — GraphStore: string arena, entity store (64 B/cell), relation store
  (48 B/record, tombstones, provenance, validity), layout page, adjacency index (rebuilt
  on open, multi-hop BFS)
- `timeseries.rs` — TimeSeriesStore: buffer → batch (256 pts) → chunks (zigzag-varint
  deltas, const/raw values, min/max), per-series stream, temporal index = chunk index,
  query/aggregate
- `vidgeql.rs` — VidgeQL parser (MATCH/WHERE/RETURN/LIMIT)
- `executor.rs` — pattern binding over the adjacency index
- `temporal.rs` — MEASURE/DURING + aggregates (graph ↔ time-series join)

## Invariants that must not break (lessons from Phases 1-3)

1. **Page 0 is the superblock `VDG1`.** Slab heads are `NULL_PAGE` (u32::MAX), never 0.
2. **A page staged inside a Tx is not on disk** — any re-read during the tx goes through
   the `staged` cache.
3. **Wire writes go through `encode_into(data, at)`** — no `slice.try_into()` (the
   copy-by-value trap).
4. **The GraphStore Meta has 11 fields**, including `ts_meta_page` and `se_meta_page` — one
   source of truth, never a mirror field outside Meta.
5. **`load_strings` resets `next_sid` to 0** — sids are rebuilt in chain order.
6. **`add_entity` interns the real name supplied** (no placeholder).
7. **Durability**: WAL frames → superblock frame → commit marker → fsync → pager apply →
   flush. A crash at any point is healed on the next open (replay).
8. **Provenance**: a hypothesis never silently becomes a fact (spec §29) — that is encoded
   in the type system.
9. **Rollback = restoration, never a write** (Phase 9): a Tx dropped without commit
   publishes its reservations (taken/freed) to the shared rollback log; the next Engine
   call (begin/commit/alloc_in_tx/free_in_tx) drains it and returns each id — a recycled id
   goes back onto the freelist mirror (durable state identical), an append id goes to the
   pager's scratch pool (NEVER onto the chain: writing a head beyond page_count would
   corrupt recovery). No durable byte moves on rollback.
10. **A reserved id can never be handed to two txs**: the freelist mirror and the scratch
    pool are consumed by `alloc()`, and the `pending_alloc` guard remains the inter-tx
    uniqueness source.
11. **Fail-closed recovery on an unknown opcode** (Phase 8.5): any committed
    `WalOp::Custom` whose tag ≠ `COMMIT_TAG` (= `u32::MAX`, the commit marker) is an opcode
    unknown to this build → `Engine::open` REFUSES with `WalError::UnknownOpcode { tag }`.
    The old silent arm (`Custom => {}`) would have reopened a file from a later version in
    a mutilated state. The on-disk format does NOT change (the tag is already in the Custom
    encoding).
12. **MAX_FRAME_SIZE** (Phase 8.5, defence in depth): `scan()` rejects with
    `WalError::CorruptFrame` any header announcing a payload > 64 KiB (`wal::MAX_FRAME_SIZE`,
    a 16× margin over a 4 KiB SetPage). A torn write leaves a REAL (small) length — this
    bound only fires on corruption or a foreign format.
13. **Settle log / end-of-tx discipline** (Phase 8.7, BUG-1 fix): each tx publishes EXACTLY
    one end event `(tx_id, committed)` — `(id, true)` from a successful commit, `(id, false)`
    from a rollback (`Tx::drop`, `Engine::rollback`, or the `free_in_tx` net-out — any end
    that never became durable). A store that mutates its in-memory state inside a tx
    (TimeSeriesStore) arms a snapshot on first touch and REVERTS it when a `(id, false)`
    event is drained. The TS entry points drain (settle) before/after each call — settling
    is LAZY by design: between a tx drop and the next TS call, the ghost state stays visible
    in memory. Every new TS store entry point must call `settle`/`arm`/`snap` like the
    existing ones (`create_series`/`append`/`flush_series`/`retain`), and
14. **TS snapshot-revert = TOTAL revert**: the snapshot covers the WHOLE mutable part of the
    store (buffers, chunk_index, slab heads/counts, staged, tails, series table). A series
    created inside the aborted tx is captured on the `revive` list (its sid stays usable —
    its next mutating touch re-renders it onto the slab through the ordinary
    `create_series_in` path); a revival tx that itself rolls back puts it back on hold
    (idempotent). Do NOT exclude fields from the revert "because it would be DDL" — a partial
    revert would leave series_pos pointing at pages returned to the scratch pool (wedge).

## Test & validation

```bash
VIDGEDB_BIN=$PWD/target/release/vidgedb cargo test --release   # 227 tests (unit + per-phase e2e)
cargo fmt                                                       # before every commit
./crash_test.sh                                                 # SIGKILL matrix × 7 delays
```

Per-phase integration tests live in `tests/`: `phase2_graph.rs`, `phase3_timeseries.rs`,
`phase6_vidgeql.rs`, `phase62_temporal.rs`. Each new phase adds its file. The binary-driving
tests need `VIDGEDB_BIN` — see [CONTRIBUTING.md](CONTRIBUTING.md) for why the count silently
drops without it.

## Hardening: done vs remaining (state at Phase 9)

**Closed:**
- **BUG-1 (Phase 8.7) — a rollback after a TS auto-flush wedges the store**: the TS layer
  mutated its in-memory state (chunk_index, series counters, stream_head, staged map
  pointing at pages reserved by the tx) BEFORE the commit; a rollback did not revert them →
  stream_head anchored on a page that was never committed, phantom counters
  (total_points=256 for points that were never durable), and the first write after the
  rollback panicked (`Err(PageOutOfBounds)`, repro `tests/phase87_ts_audit.rs::probe_p1`).
  FIX: the settle-log discipline + snapshot-revert + deferred revival (invariants 13/14
  above). Regression probes: probe_p1 (green), plus fix_87_p1b (a double flush, rolled back),
  fix_87_p1c (a pre-existing series reverted and a new series revived in the same tx),
  fix_87_p1d (the revival tx rolls itself back — still revivable), fix_87_p1e (the READ path
  settles too: query/aggregate after a rollback serve no phantom).
- **BUG-2 (Phase 8.7) — the 2-commit window in `retain` is CLOSED**: `retain` no longer
  commits twice (page frees + series patch, THEN chunk-index slab compaction in a separate
  tx). Both steps now live in ONE transaction (`compact_chunk_slab_in` stages onto the
  caller's tx; the layout is re-staged in the same tx), committed atomically — a crash can
  no longer leave a stale index (records pointing at freed pages, or retained data
  resurrected) under already-patched counters. The audit probe `probe_p2` documented the
  window as not simply fixable: the atomic fix EXISTED (merging the two txs, zero on-disk
  format change). phase83/83b stay green (chunk-granular retention is unchanged).
- Transactional rollback + id restoration (Phase 8.1 + 9): an aborted Tx no longer mutates
  anything durable AND its page is IMMEDIATELY recyclable (freelist mirror for durable pages,
  the pager's scratch pool for append reservations). Zero leak measured at the 10th cycle
  (`tests/phase81_tx_recovery.rs::rollback_recycles_10_cycles`).
- Journaled free: the freelist chain node is an ordinary SetPage journaled in the commit
  (ChatGPT probes 2/2b converted into tests).
- Fail-closed recovery: grouped replay + freelist rebuild through the cache (not the raw
  disk); any inconsistency refuses the open.
- **Fail-closed on an unknown opcode** (Phase 8.5, ChatGPT probe #2 converted): a committed
  `Custom` with an unknown tag refuses the open instead of being ignored —
  `tests/phase85_wal_hardening.rs`.
- **MAX_FRAME_SIZE** (Phase 8.5): `scan()` rejects any frame > 64 KiB (old behaviour: a
  forged `len` of 0xFFFFFFFF passed silently in ~9 µs as long as the file had no giant tail).
- Cargo warnings: zero (build + tests).

**Documented v1 limits (WAL recovery):**
- **Torn tail vs mid-WAL corruption: indistinguishable in v1.** The frame format has neither
  an LSN nor a checksum chain: after a frame that fails, `scan()` keeps the verified prefix
  and stops. (a) Torn tail (crash mid-write): exact replay of the txs committed before it.
  (b) Corruption IN THE MIDDLE of the log (TX1 durable, TX2 durable then TX2's bytes
  corrupted): the SAME mechanism keeps TX1 and silently abandons the rest — `Engine::open`
  succeeds WITHOUT a warning. A torn write produces exactly what corruption produces (a bad
  CRC), so v1 replays the conservative prefix rather than refusing a possibly-sound
  database. Future mechanism (roadmap, decision §21: NOT implemented in v1): a per-frame LSN
  / checksum chain — each frame names its predecessor, which distinguishes "end of log" from
  "hole in the middle". Do NOT rewrite the protocol around this limit: it is the documented
  contract.

**Remaining (documented limits, out of scope for Phase 9):**
- **No mmap**: explicit `File` read/write I/O (spec §22).
- **No per-page checksum**: integrity rides on the CRC32 of WAL frames; the `.vdg` data pages
  themselves are not checksummed.
- **1 writer**: no multi-process lock; one Engine per file by convention (the crash harness
  kills the writer, it does not coexist).
- **Strings 4 KiB/page**: a string must fit one arena page
  (`PAGE_HDR + cell_len <= PAGE_SIZE`); cross-page is forbidden.
- **Props inline 44 bytes** (entity cell 64 − 20).
- No compaction: append-only slabs, physical tombstones.

## VidgeQL queries (examples that run)

```text
MATCH (plc:PLC) -[:ELECTRICAL]-> (w:Wire) -[:ELECTRICAL]-> (m:Motor) RETURN plc, w, m
MATCH (m:Motor) WHERE m.name = "Motor42" RETURN m LIMIT 10
MATCH (m:Motor) MEASURE m.current DURING last(24h) RETURN max(current), avg(current)
```

## Roadmap (spec §69)

Done: P0 (model), P1 (pager+WAL+engine+crash harness), P2 (graph stores + adjacency),
P3 (chunked time-series), P6 (VidgeQL), P6.2 (temporal).
Remaining: OR / `AT "timestamp"` (§24), `CHECK/FOR` deviation (§25), agent API (§28, P7),
compaction + mmap + checksummed pages (hardening).

## Engine status and phase history

v0.1.0 — **227 tests, 0 failed, 0 warnings**. The phases below are the build history; the
current API surface is documented in [`docs/jsonrpc-reference.md`](docs/jsonrpc-reference.md).

> **Counting the tests.** Some tests drive the real binary as a subprocess (the only way to
> observe session poisoning, a signal, or the JSON-RPC contract):
> `VIDGEDB_BIN=$PWD/target/release/vidgedb cargo test --release`. Without that variable those
> tests fail to spawn and the total **silently drops**.

- [x] Core data model (`src/model.rs`): Entity, Port, Relation (topology-tagged),
      SpecBound, Measurement, Violation, Provenance (7 classes), Validity intervals
- [x] Invariants encoded in types: hypothesis ≠ fact; spec provenance forced;
      measurements restricted to Observation provenance
- [x] Expected-vs-observed comparison (`check_constraints`, spec §25)
- [x] Smoke test: spec §58 example machine → exactly 2 violations detected
- [x] Phase 1: page store (superblock `VDG1`, freelist in pages, 4096-byte pages)
- [x] Phase 1: WAL (CRC32 frames, commit marker, torn/corrupt detection,
      deterministic replay, checkpoint) — spec §18
- [x] Phase 1: transactional engine (WAL-first commit protocol, fsync
      durability boundary, recovery on open) — spec §17/§18
- [x] Phase 1: crash harness — SIGKILL at arbitrary points, checker verifies
      zero torn commits (crash_test.sh, 7 delay points, spec §38.5)
- [x] Phase 2: string arena + entity store + relation store (tombstones,
      provenance, validity) + layout page; adjacency index rebuilt on open
      (topology-filtered one-hop + multi-hop BFS, spec §20); e2e tests:
      spec §58 machine persisted/reopened/traversed, 599-relation bulk chain
- [x] Phase 3: time-series chunked store (spec §19) — buffer → batch (256
      points) → chunk (zigzag-varint timestamp deltas, const/raw value
      encoding, min/max metadata) → per-series page streams; chunk index
      doubles as the temporal index; range queries + aggregation +
      metadata-only stats; e2e: 600 points persisted/reopened/queried
- [x] Phase 6: VidgeQL parser (tokenizer + recursive descent, spec §23 subset)
      + executor — MATCH `(var:Type) -[:TOPO]-> (next:Type)` chains, WHERE on
      name/type/inline-props (=/!=/</>/<=/>=, AND), RETURN, LIMIT; e2e:
      two-hop traversal, WHERE+LIMIT, unknown-topology-matches-nothing
- [x] Phase 6.2: temporal queries (spec §23 hybrid example) — MEASURE
      var.signal DURING last(24h)|t1..t2, RETURN aggregates
      (max/min/avg/sum/count over the window), executor joins graph
      bindings with the time-series store (series = "<entity>.<signal>");
      e2e: ramp telemetry 600 pts, absolute-window slice, entity without
      telemetry still binds (aggs empty)
- [x] Phase 6.3: OR logic — `BoolExpr{And,Or,Leaf}` tree, AND>OR precedence
      (a OR b AND c → Or[a, And[b,c]]), executor short-circuit eval,
      `where_` kept flat for compat; e2e: OR across two motors
- [x] Phase 6.4: CHECK/FOR deviation engine (spec §25) — `check_entity`
      reads spec from entity prop `spec.<signal>.max` (+ `.unit`), observed
      = max over window (worst case, documented), deviation = observed −
      expected_max; statuses OK / VIOLATION / NO_DATA / NO_SPEC; VidgeQL
      `CHECK <entity> FOR <signal> DURING ...` statement; provenance kept
      (expected=Specification, observed=Observation, no hypothesis→fact)
- [x] Phase 6.5: AT timestamp (spec §24) — `Adjacency::build_at(records, t)`
      (tombstone + validity `[from, to)` with `VALID_TO_OPEN = -1` sentinel),
      `neighbors_at`/`multi_hop_at`, VidgeQL `AT <unix>` clause, executor
      binds hops through the time-scoped adjacency; e2e: expired relation
      invisible at past/future instants, visible without AT
- [x] Phase 7: Agent Tool API (spec §28/§27) — `AgentApi` JSON façade:
      schema / query / query_temporal / get_entity / get_measurements /
      check / trace / provenance; read-only by design (spec §55);
      audit trail per call {timestamp, agent_id, method, params} (spec §56);
      hypothesis→fact promotion refused and documented (spec §29)
- [x] Phase 11: INGESTION PATH (documented spec §55 derrogation) —
      explicit roles `AgentApi::open_with_role(path, id, Role::{Reader,
      Writer, Ingest})`; Reader (default) = strict read-only, any write
      refused with **-32003** and audited; Writer/Ingest = telemetry +
      topology ingestion (`ingest_points` batched one-tx auto-flush-256 +
      auto-create_series, `upsert_entity` prop-MERGE null-safe + deduped
      provenance-tagged relations, `set_state`, `log_event`, `retain`);
      provenance by declared source (`plc`⇒Fact, `agent`/`sensor`⇒Observation,
      spec §29); no hypothesis write in ANY role; v1 has NO crypto authn
      (single-user deployment, TCP wrapper later); JSON-RPC surface +
      `--role` flag; tests/phase84_ingest.rs (8 tests incl. spawned-binary
      e2e with a 1000-pt batch and the -32003 wire refusal)
- [x] Phase 7.1: DIAGNOSE (spec §26) — 8-step report (component,
      dependencies up/downstream with resolved names+provenance, topology
      grouping, explicit events limitation note, measurements per series,
      spec comparisons via check_entity, anomalies, candidate causal
      hypotheses as OUTPUT ONLY with confidence+provenance, never written);
- [ ] Phase 8: benchmarks (synthetic generator, baselines);
      ISO-8601 AT strings; Phase 4 state/events store
- [x] Phase 8.1 hardening: transactional alloc/free (sb candidates applied
      only at commit, freelist node journaled as an ordinary SetPage,
      coexisting-tx collision guard), fail-closed recovery (a corrupt group
      or chain refuses the open instead of being swallowed), rollback = zero
      durable effect with immediate id recycle (Phase 9: the old
      "rolled-back Tx leaks a page" limit is CLOSED — 10-cycle churn test)
- [ ] Phase 3 hardening: page compaction, mmap I/O, checksummed pages

## Phase 0 decisions (recorded per spec §21)

| Choice | Decision | Rationale |
|---|---|---|
| Timestamp | `i64` UTC ns since epoch | fits spec §24 event time; ingestion time added at WAL layer |
| Validity | half-open `[from, to)` | closed-open composes without overlap on edits |
| Topology | prefix of relation_type, split at `:` | edge identity includes topology class (Principle 6) |
| Signal/spec join | exact signal-name match | test spec58 caught name-mismatch bug; keep strict |
| IDs | `u32` entity handles (internal), string ids external | stable internal handles, portable file text |

## Phase 1 decisions (recorded per spec §21)

| Choice | Decision | Rationale |
|---|---|---|
| Page size | 4096 B, `PageId = u32` index | sector-friendly; file = plain page array (spec §22) |
| Superblock | page 0, magic `VDG1`, page_count/freelist_head | written LAST in every flush = allocation commit point |
| Freelist | chained INSIDE data pages, LIFO recycle | survives restart, reconstructible (spec §17); double-free rejected |
| WAL frame | `[len u32][crc32 u32][payload]` | torn frame = scan stops there (spec §18 incomplete-tx detection) |
| Commit | ops + superblock frame + `[0xFF]` marker + fsync | ordered durability; replay = pure fold (deterministic) |
| Recovery | replay committed frames → pager flush → WAL truncate | open() always heals; uncommitted tail silently dropped |
| Phase 9 fix | rolled-back Tx hands its reserved id back (freelist mirror or scratch pool) — zero page loss, recycled by the next tx; no WAL, no durable mutation | the page was never referenced by any committed sb, so in-memory hand-back restores it exactly (page81: `rollback_recycles_10_cycles`) |

## Phase 2 decisions (recorded per spec §21)

| Choice | Decision | Rationale |
|---|---|---|
| Strings | slab arena, cell `[len u32][bytes]`, dedup in-memory | intern once, stable StrId; leak on re-write (compaction later) |
| Entities | 64-byte fixed cells, key = cell index | dense packing, stable keys; props inline capped |
| Relations | fixed 48-byte records, tombstone flag | append-only, physical delete = compaction |
| Heads | `NULL_PAGE` (u32::MAX) = empty chain | page 0 is the superblock — 0 is never a valid head |
| Adjacency | derived, in-memory, rebuilt on open | source of truth = relation slab (spec §17) |
| Page staging | tx-scoped write cache (`staged`) | a page staged in a tx isn't on disk; re-reads must see staged bytes |
| Wire writes | `encode_into(data, at)` | slice `.try_into()` copies-by-value pitfalls; direct buffer writes |

**Verified crash matrix** (crash_test.sh, SIGKILL × 7 delay points 2–500 ms):
after every kill, checker finds only fully-committed pages — no torn data
ever visible (spec §38.5: crash during write/commit/WAL flush, partial tx,
restart).

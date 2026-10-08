# VidgeQL Language Reference

VidgeQL is VidgeDB's query language: a small, Cypher-inspired pattern language with temporal windows and a deviation-checking statement. This page documents **exactly what the shipped parser accepts** (source of truth: `src/vidgeql.rs`, `src/temporal.rs`, `src/check.rs` — not the concept spec, which describes a superset). Every example was executed against the release binary; errors are copied verbatim.

See also: [README.md](README.md), [jsonrpc-reference.md](jsonrpc-reference.md).

---

## 1. Grammar (as implemented)

Documented in `src/vidgeql.rs` (EBNF-ish, recursive descent) and extended by `src/temporal.rs` / `src/check.rs`:

```text
query        := MATCH pattern (WHERE cond_logic)? (AT num)? RETURN items LIMIT num?
pattern      := node (edge node)*
node         := '(' [ident ':'] Type ')'
edge         := '-[' ':' Topology ']' '->'
cond_logic   := cond ((AND | OR) cond)*          # AND binds tighter than OR
cond         := var '.' prop ('=' | '==' | '!=' | '<' | '>' | '<=' | '>=') value
value        := string | number
items        := item (',' item)*
item         := ident                            # core path
              | agg '(' [var '.'] signal ')'     # temporal path: min/max/avg/sum/count

temporal query (query_temporal path):
tquery       := MATCH pattern (WHERE …)? (MEASURE var.signal DURING window)? RETURN items LIMIT num?
window       := last '(' duration ')' | num '..' num
duration     := n('s'|'m'|'h'|'d')               # e.g. 24h, 90m, 500s, 7d

check statement (parsed by check::parse_check; executed by the check()/Node-RED paths):
check        := CHECK var.signal FOR ( pattern ) (WHERE cond_logic)? DURING window RETURN field (',' field)*
field        := status | expected | observed | deviation | entity | name | signal | unit | points
```

Where clauses also accept **time travel**: `AT <unix-seconds>` may appear after MATCH/WHERE, before RETURN (order-insensitive with WHERE in practice as long as it precedes RETURN). `AT` reconstructs relation validity at that instant for every hop of the pattern.

Keywords are case-insensitive where tokenized (`match` ≡ `MATCH`? — no: the tokenizer uppercases **known keywords only**; `MATCH`, `WHERE`, `RETURN`, `LIMIT`, `AND`, `OR`, `AT` are matched case-insensitively through the `.to_uppercase()` check, but topology names in `-[:TOPO]->` are lowercased, and type names like `Motor` are matched exactly against stored types — **type names are case-sensitive**).

Values: strings in `"double"` or `'single'` quotes (no escapes); numbers are `f64` literals. Identifiers: alphanumerics + `_` + `.` (dotted property refs stay one token).

### What is NOT in the language (v0)

- Inbound edges (`<-[:TOPO]-`), undirected edges, variable-length paths (`*1..3`).
- `WHERE` on measurements (filter on telemetry) — use `MEASURE` + aggregate + post-filter in your client, or `CHECK`.
- Labels on edges (`-[r:TOPO]->`), multiple RETURN patterns per item, aliases beyond `var AS alias` (unsupported: no AS).
- Aggregates in WHERE; HAVING; ORDER BY.
- `DURING` on `get_measurements`-style windows in *core* `query` — windows only exist on the temporal path (`query_temporal`, `MEASURE`) and `CHECK`.
- Boolean NOT; NULL literals.

---

## 2. Pattern forms & types

| Pattern | Binds |
|---|---|
| `MATCH (m:Motor)` | every entity of type `Motor` |
| `MATCH (:Motor) RETURN m` | invalid (no var to return) — anonymous nodes are parsed `( [:]Type )` but unbound vars can't be returned |
| `MATCH (m:Motor) -[:ELECTRICAL]-> (d:Drive)` | Motors feeding (out-edge, topology `electrical`) a Drive |
| `MATCH (p:Pump) -[:MECHANICAL]-> (m:Motor) -[:ELECTRICAL]-> (d:Drive)` | 3-node chains |
| `MATCH (m:Motor) -[:mechanical]-> …` | topology is **lowercased** at parse — case-insensitive vs stored `mechanical` |

- Node vars bind per row; `RETURN m.name` is **not** supported — you return whole entity cards (`{key, name, type, properties}` per binding).
- An unknown topology or type matches **nothing** (`n: 0`), by design — not an error.
- `AT <unix>`: hops bind only through relations whose `[valid_from, valid_to)` contains the instant (`AT 1500` binds, `AT 500` doesn't — verified outputs in [README](README.md#4-vidgeql)). Without `AT`, current "now" topology (all live records) is used.

---

## 3. WHERE operators & values

| Op | String comparison | Numeric comparison |
|---|---|---|
| `=` or `==` | exact (lexicographic) | numeric equality on parsed f64 |
| `!=` | | |
| `<`, `>`, `<=`, `>=` | lexicographic | numeric |

- Left side: `var.prop` where `prop` is `name`, `type`, or **any inline property key** (missing props compare as `""` — no error, just a miss). A bare `ident` without `.prop` → `parse error: unknown property: <ident>`.
- Comparing a string prop to a number (`m.spec.current.max > 5`) parses the prop value to f64; **non-parseable values make the condition false**, not an error.
- `AND` binds tighter than `OR`: `a OR b AND c` ≡ `a OR (b AND c)` (unit-asserted in `vidgeql.rs::where_clause_precedence_and_over_or`).
- Empty `AND` (no WHERE) = vacuously true; empty `OR` = false.

---

## 4. Windows

| Form | Meaning | Notes |
|---|---|---|
| `DURING last(24h)` | `[now−24h, now]` | `now` supplied by the caller (JSON-RPC `now` param or wall clock); units `s m h d`; `12x`, `abc` fail cleanly |
| `DURING 1759999000..1760003600` | absolute inclusive `[t1, t2]` | two unix-seconds numbers separated by `..` |
| `AT 1760000000` | topology snapshot (graph clauses) | single number, unix seconds |

Both window forms are accepted by `MEASURE` … `DURING` and `CHECK … DURING`. The window is **inclusive on both ends** (a `Window{from, to}` in the engine; `check` docs call its bounds inclusive).

---

## 5. Aggregates & CHECK fields

Aggregate functions (temporal path only, over the series bound by `MEASURE`): `max(x)`, `min(x)`, `avg(x)`, `sum(x)`, `count(x)`. The inner argument may be `var.signal` or a bare `signal`. On a window with no points: `max/min/avg` return `value: null`, `count` returns `0.0` (verified). `avg` over an empty window is `null` (JSON), not NaN.

`CHECK <var>.<signal> FOR (pattern) [WHERE …] DURING <window> RETURN status, expected, observed, deviation, entity, name, signal, unit, points` — fields any subset; unknown field → `unknown CHECK field: x`. Semantics identical to the `check` method (observed = max of window, strict `>` = VIOLATION; deviation = observed − expected; statuses OK / VIOLATION / NO_DATA / NO_SPEC). Note: the JSON-RPC *service* exposes CHECK through the `check` **method** (entity+signal form); the full `CHECK … FOR (pattern)…` statement shape is parsed and executed by the Rust engine (`check::parse_check`/`execute_check`, e2e-tested in `tests/phase64_check.rs`) — a JSON-RPC `query` of a CHECK statement is **not** routed through the temporal parser in v0, so use the `check` method over the wire.

---

## 6. Common parse errors (verbatim strings from the binary)

| You wrote | You get |
|---|---|
| `MATCH (m:Motor) RETURN m LIMIT` | `{"error":"parse error: unexpected token: expected number"}` |
| `MATCH (m:Motor AT 1760000000) RETURN m` | `{"error":"parse error: unexpected token: expected RParen"}` — `AT` is a top-level clause, not inside a node |
| `MATCH (m:Motor -[:ELECTRICAL]-> (d:Drive)) RETURN m, d` | parse error (nodes/edges unbalanced) |
| `MATCH (x:Robot) RETURN x` | no error — `{"n":0,"rows":[]}` (unknown type matches nothing) |
| `MEASURE m.current DURING bogus RETURN max(m.current)` | `{"error":"parse error: DURING expects last(n) or a..b"}` |
| `MEASURE m.current DURING last(12x) RETURN max(m.current)` | `{"error":"parse error: bad last() duration"}` |
| `CHECK m.current FOR (m:Motor) DURING last(1h) RETURN bogus` | `unknown CHECK field: bogus` |
| Bare condition without a prop (`WHERE m = "x"`) | `parse error: unknown property: m` |
| Unterminated string | `unexpected end of query` (UnexpectedEof) |
| Non-JSON-RPC garbage in the *query* — no crash ever; the service keeps serving the next line either way | |

### Gotchas worth knowing

- **`RETURN` is required** in core `query` MATCH clauses (the temporal layer synthesizes a placeholder when `MEASURE` carries the RETURN — so `MATCH (m:Motor) MEASURE m.current DURING last(1h) RETURN max(m.current)` is the canonical form there).
- Variables **not** in the pattern cannot be RETURNed; a pattern variable bound via an anonymous node (`(:Motor)`) cannot be returned.
- Aggregates in `query` (non-temporal) are parsed as ordinary identifiers — use `query_temporal` to aggregate, or `get_measurements`/`check` methods.
- `LIMIT` with no number, `AT` without a number, and `!` alone → `UnexpectedToken` family of errors (messages above).
- The `CHECK` statement's FOR pattern reuses core MATCH syntax, including `AT`-less snapshots only — combining `AT` with CHECK is not wired in v0.
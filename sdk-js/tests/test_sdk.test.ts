/**
 * Phase 12b — VidgeDB JavaScript/TypeScript SDK test suite.
 *
 * Spawns the REAL `vidgedb --service` binary (located via $VIDGEDB_BIN) and
 * exercises the full client surface: schema, valid/invalid VidgeQL, ingest
 * roundtrip (Ingest role), typed measurements, JSON-RPC error → JS exception
 * mapping, concurrent-call sequencing through the promise queue, close()/
 * dispose teardown, and a runtime shape-coherence check of the hand-written
 * interfaces. The test fixture twin is built through the binary's PUBLIC
 * JSON-RPC service (see ../sdk-python/tests/fixture.py) — no Rust build step.
 *
 * Zero test dependencies: node:test + node:assert (stdlib), like the SDK
 * itself. Run with `npm test`.
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as path from "node:path";
import * as fs from "node:fs";
import * as os from "node:os";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { VidgeDB, VidgeDBError } from "../dist/esm/client.js";
import { VERSION } from "../dist/esm/index.js";
import type { Schema, CheckResult, MeasurementSeries } from "../dist/esm/types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SDK_ROOT = path.resolve(__dirname, "..");
// The autonomous SDK fixture generator (stdlib-only, JSON-RPC through the
// binary) — lives in the sibling `sdk-python/tests/` directory of this repo.
// Resolved by walking up from here so the test works from a clone, whatever
// the layout: monorepo root, or the package installed on its own.
function findFixturePy(): string {
  const candidates = [
    path.resolve(__dirname, "..", "..", "sdk-python", "tests", "fixture.py"),
    path.resolve(__dirname, "..", "..", "python", "tests", "fixture.py"),
  ];
  for (const c of candidates) if (fs.existsSync(c)) return c;
  throw new Error(
    "SDK fixture generator (fixture.py) not found — it ships with the Python " +
      "client in `sdk-python/tests/`. Looked in:\n  " +
      candidates.join("\n  "),
  );
}
const FIXTURE_PY = findFixturePy();

// ---------------------------------------------------------------------------
// Binary + fixture plumbing (module-scoped, mirrors the Python fixtures)
// ---------------------------------------------------------------------------

function findVidgedbBin(): string {
  const env = process.env.VIDGEDB_BIN;
  if (env && fs.existsSync(env)) return env;
  throw new Error(
    "vidgedb binary not found — set VIDGEDB_BIN to the compiled `vidgedb` binary",
  );
}

const VIDGEDB_BIN = findVidgedbBin();
const NOW = 1_760_000_000; // fixed reference clock (matches Rust fixtures)
const MOTOR = "Motor_000001";
const PLC = "PLC_000001";
const PUMP = "Pump_000001";

let fixtureSrc: string | null = null;

/**
 * Build (once) the deterministic 1-machine twin through the binary's PUBLIC
 * JSON-RPC service, by driving `sdk-python/tests/fixture.py` (stdlib only) — the
 * JS suite never invokes a Rust build nor touches the engine sources.
 */
function ensureFixtureDb(): string {
  if (fixtureSrc && fs.existsSync(fixtureSrc)) return fixtureSrc;
  if (!fs.existsSync(FIXTURE_PY)) {
    throw new Error(`SDK fixture generator not found at ${FIXTURE_PY}`);
  }
  const out = path.join(os.tmpdir(), `vidgedb_sdk_js_fixture_${process.pid}.vdg`);
  const res = spawnSync(process.env.PYTHON ?? "python3", [FIXTURE_PY, out], {
    env: { ...process.env, VIDGEDB_BIN },
    encoding: "utf8",
  });
  if (res.status !== 0) {
    throw new Error(
      `fixture generation failed (exit ${res.status}): ${res.stderr || res.stdout}`,
    );
  }
  fixtureSrc = out;
  return out;
}

function makeTmpDb(t: any): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "vidgedb-js-test-"));
  t.after(() => {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  });
  return path.join(dir, "test.vdg");
}

function freshDb(t: any): string {
  return makeTmpDb(t); // service creates storage on first open
}

function populatedDb(t: any): string {
  const src = ensureFixtureDb();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "vidgedb-js-test-"));
  t.after(() => {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  });
  const dst = path.join(dir, "populated.vdg");
  fs.copyFileSync(src, dst);
  if (fs.existsSync(src + "-wal")) fs.copyFileSync(src + "-wal", dst + "-wal");
  return dst;
}

const BIN = { bin: VIDGEDB_BIN };

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

test("lifecycle: spawns and closes cleanly (exit 0 on EOF)", async (t) => {
  const dbPath = freshDb(t);
  const db = VidgeDB.open(dbPath, { ...BIN, agentId: "t-lifecycle" });
  assert.ok(db.isRunning());
  assert.ok(db.pid > 0);
  await db.close();
  assert.equal(db.isRunning(), false);
  const code = await db.waitExit();
  assert.equal(code, 0); // clean EOF exit, not a kill
});

test("lifecycle: close() is idempotent", async (t) => {
  const db = VidgeDB.open(freshDb(t), BIN);
  await db.close();
  await db.close(); // second close must not throw
  assert.equal(db.isRunning(), false);
});

test("lifecycle: call after close raises client-closed error", async (t) => {
  const db = VidgeDB.open(freshDb(t), BIN);
  await db.close();
  await assert.rejects(
    () => db.schema(),
    (err: VidgeDBError) => err instanceof VidgeDBError && /client is closed/.test(err.message),
  );
});

test("lifecycle: async dispose closes the process", async (t) => {
  const dbPath = freshDb(t);
  {
    const db = VidgeDB.open(dbPath, BIN);
    await db.schema(); // one round-trip inside the block
    await db.dispose();
    assert.equal(db.isRunning(), false);
    assert.equal(await db.waitExit(), 0);
  }
});

// ---------------------------------------------------------------------------
// schema() — the runtime type-coherence check (hand-written interfaces)
// ---------------------------------------------------------------------------

test("schema: runtime shape matches the hand-written Schema interface", async (t) => {
  const db = VidgeDB.open(freshDb(t), BIN);
  try {
    const s: Schema = await db.schema();
    // Assert the runtime shape against the interface's declared fields.
    assert.ok(Array.isArray(s.entity_types), "entity_types must be an array");
    assert.ok(Array.isArray(s.provenance_classes), "provenance_classes must be an array");
    assert.ok(Array.isArray(s.relation_topologies), "relation_topologies must be an array");
    assert.ok(Array.isArray(s.series), "series must be an array");
    assert.equal(s.provenance_classes[0], "Fact");
    assert.ok(s.provenance_classes.includes("Observation"));
    assert.ok(s.provenance_classes.includes("Specification"));
    assert.ok(s.provenance_classes.length >= 8);
  } finally {
    await db.close();
  }
});

test("schema: fixture inventory lists types, topologies and series", async (t) => {
  await VidgeDB.with(populatedDb(t), { ...BIN, agentId: "sdk-js-test", role: "writer" }, async (db) => {
    const s = await db.schema();
    assert.deepEqual(new Set(s.entity_types), new Set(["PLC", "Drive", "Motor", "Pump"]));
    assert.ok(s.series.includes(`${MOTOR}.current`));
    assert.ok(s.relation_topologies.includes("electrical"));
  });
});

test("schema: call('schema') matches mirror schema()", async (t) => {
  await VidgeDB.with(freshDb(t), BIN, async (db) => {
    assert.deepEqual(await db.call("schema"), await db.schema());
  });
});

// ---------------------------------------------------------------------------
// query() + error paths
// ---------------------------------------------------------------------------

async function withWriterDb(t: any, fn: (db: VidgeDB) => Promise<void>): Promise<void> {
  const db = VidgeDB.open(populatedDb(t), { ...BIN, agentId: "sdk-js-test", role: "writer" });
  try {
    await fn(db);
  } finally {
    await db.close();
  }
}

test("query: valid VidgeQL returns Row rows", async (t) => {
  await withWriterDb(t, async (db) => {
    const r = await db.query("MATCH (m:Motor) RETURN m");
    assert.ok(r.n >= 1);
    assert.ok(Array.isArray(r.rows));
    const row = r.rows[0].m;
    assert.equal(row.type, "Motor");
    assert.equal(row.name, MOTOR);
  });
});

test("query: WHERE filter narrows to one row", async (t) => {
  await withWriterDb(t, async (db) => {
    const r = await db.query(`MATCH (m:Motor) WHERE m.name = "${MOTOR}" RETURN m`);
    assert.equal(r.n, 1);
  });
});

test("query: invalid VidgeQL raises VidgeDBError(code=0) — semantic, zero-panic", async (t) => {
  const db = VidgeDB.open(freshDb(t), BIN);
  try {
    await assert.rejects(
      () => db.query("TOTAL GARBAGE NOT VQL"),
      (err: VidgeDBError) => {
        assert.equal(err.code, 0);
        assert.match(err.message, /parse error/);
        return true;
      },
    );
    assert.ok(db.isRunning()); // the zero-panic contract: still serving
    assert.ok((await db.schema()).provenance_classes.length > 0);
  } finally {
    await db.close();
  }
});

test("unknown method raises VidgeDBError(-32601)", async (t) => {
  const db = VidgeDB.open(freshDb(t), BIN);
  try {
    await db.schema(); // warm session
    await assert.rejects(
      () => db.call("definitely_not_a_method"),
      (err: VidgeDBError) => err.code === -32601,
    );
  } finally {
    await db.close();
  }
});

test("invalid params raises VidgeDBError(-32602)", async (t) => {
  const db = VidgeDB.open(freshDb(t), BIN);
  try {
    await assert.rejects(
      () => db.call("get_entity", { key: "not-a-number" }),
      (err: VidgeDBError) => err.code === -32602,
    );
    await assert.rejects(
      () => db.call("get_entity"), // missing required key
      (err: VidgeDBError) => err.code === -32602,
    );
  } finally {
    await db.close();
  }
});

test("semantic error in result raises code 0 (AgentApi v0 shape)", async (t) => {
  await withWriterDb(t, async (db) => {
    await assert.rejects(
      () => db.getMeasurements("Ghost", "x", 0, 100),
      (err: VidgeDBError) => {
        assert.equal(err.code, 0);
        assert.match(err.message, /no series named/);
        assert.equal(err.data.error, err.message);
        return true;
      },
    );
    assert.ok((await db.schema()).provenance_classes.length > 0); // alive
  });
});

test("after an error the next round-trip stays consistent", async (t) => {
  await withWriterDb(t, async (db) => {
    await assert.rejects(() => db.call("nope"), VidgeDBError);
    assert.ok((await db.schema()).provenance_classes.length > 0);
  });
});

// ---------------------------------------------------------------------------
// Ingest roundtrip (Ingest role) + typed measurements
// ---------------------------------------------------------------------------

test("ingestPoints roundtrip: ingest then get_measurements returns the data", async (t) => {
  // A FRESH database can take a bare name — series auto-create proves the write path.
  const db = VidgeDB.open(freshDb(t), { ...BIN, agentId: "sdk-js-ingest", role: "ingest" });
  try {
    // upsert the entity first (so the series naming convention has an owner)
    await db.upsertEntity("TestMotor", "Motor", { "spec.current.max": "5.0", unit: "amp" });
    const res = await db.ingestPoints("TestMotor", "current", [
      [NOW, 3.5],
      [NOW + 60, 4.25],
      [NOW + 120, 7.5],
    ]);
    assert.equal(res.accepted, 3);
    assert.ok(typeof res.chunks_flushed === "number");
    assert.ok(typeof res.series_id === "number");

    const series: MeasurementSeries = await db.getMeasurements("TestMotor", "current", 0, 9_900_000_000);
    assert.equal(series.count, 3);
    assert.equal(series.points.length, 3);
    assert.deepEqual(
      series.points.map((p) => [p.t, p.value]),
      [
        [NOW, 3.5],
        [NOW + 60, 4.25],
        [NOW + 120, 7.5],
      ],
    );
    assert.equal(series.min, 3.5);
    assert.equal(series.max, 7.5);
  } finally {
    await db.close();
  }
});

test("ingestPoints with role=ingest works and get_measurements roundtrips via a second client", async (t) => {
  const dbPath = freshDb(t);
  {
    const db = VidgeDB.open(dbPath, { ...BIN, agentId: "ingestor", role: "ingest" });
    try {
      await db.upsertEntity("Sensor42", "Motor", {});
      await db.ingestPoints("Sensor42", "vibration", [
        [NOW, 1.0],
        [NOW + 10, 2.0],
      ]);
    } finally {
      await db.close();
    }
  }
  {
    // A fresh reader client reads what the ingest-role client wrote.
    const reader = VidgeDB.open(dbPath, { ...BIN, agentId: "reader", role: "reader" });
    try {
      const series = await reader.getMeasurements("Sensor42", "vibration", NOW, NOW + 20);
      assert.equal(series.count, 2);
      assert.equal(series.points[0].value, 1.0);
    } finally {
      await reader.close();
    }
  }
});

test("ingestPoints with empty array raises -32602", async (t) => {
  const db = VidgeDB.open(freshDb(t), { ...BIN, role: "ingest" });
  try {
    await assert.rejects(
      () => db.ingestPoints("X", "y", []),
      (err: VidgeDBError) => err.code === -32602,
    );
  } finally {
    await db.close();
  }
});

test("reader role cannot ingest (-32003 write-forbidden)", async (t) => {
  const db = VidgeDB.open(freshDb(t), { ...BIN, role: "reader" });
  try {
    await assert.rejects(
      () => db.ingestPoints("X", "y", [[NOW, 1.0]]),
      (err: VidgeDBError) => {
        assert.equal(err.code, -32003);
        assert.match(err.message, /-32003/);
        return true;
      },
    );
  } finally {
    await db.close();
  }
});

test("upsertEntity + getEntity roundtrip", async (t) => {
  const db = VidgeDB.open(freshDb(t), { ...BIN, role: "writer" });
  try {
    // Relation targets must exist BEFORE the edge is appended (the service
    // "never invents endpoints") — create Panel_B2 first.
    await db.upsertEntity("Panel_B2", "PLC", {});
    const up = await db.upsertEntity("Motor_A1", "Motor", { "spec.current.max": "10.0" }, [
      { to: "Panel_B2", relation_type: "electrical:feeds" },
    ]);
    assert.ok(typeof up.key === "number");
    assert.equal(up.created, true);
    assert.ok(up.relations_added >= 1);

    const card = await db.getEntity(up.key);
    assert.equal(card.name, "Motor_A1");
    assert.equal(card.type, "Motor");
    assert.equal(card.properties["spec.current.max"], "10.0");

    // upsert again — merge, not create
    const up2 = await db.upsertEntity("Motor_A1", "Motor", { unit: "A" });
    assert.equal(up2.created, false);
    assert.equal(up2.key, up.key);

    // Appending the same edge again is deduped (0 new relations).
    const up3 = await db.upsertEntity("Motor_A1", "Motor", {}, [
      { to: "Panel_B2", relation_type: "electrical:feeds" },
    ]);
    assert.equal(up3.relations_added, 0);

    // A relation to a NON-existent target is a clean semantic error (code 0,
    // "never invents endpoints") — and the service stays alive (zero panic).
    await assert.rejects(
      () => db.upsertEntity("Motor_B2", "Motor", {}, [{ to: "Ghost_Z9", relation_type: "electrical:feeds" }]),
      (err: VidgeDBError) => {
        assert.equal(err.code, 0);
        assert.match(err.message, /does not exist/);
        return true;
      },
    );
  } finally {
    await db.close();
  }
});

test("setState + getState + stateHistory roundtrip", async (t) => {
  const db = VidgeDB.open(freshDb(t), { ...BIN, role: "writer" });
  try {
    await db.upsertEntity("Machine_C1", "Motor", {});
    const at = NOW;
    const set = await db.setState("Machine_C1", "run_mode", "running", at);
    assert.equal(set.set, true);

    const cur = await db.getState("Machine_C1", "run_mode");
    assert.equal(cur.value, "running");
    assert.equal(cur.valid_from, at);

    const atPast = await db.getState("Machine_C1", "run_mode", at + 1);
    assert.equal(atPast.value, "running");

    const hist = await db.stateHistory("Machine_C1", "run_mode");
    assert.equal(hist.n, 1);
    assert.equal(hist.history[0].value, "running");
    assert.equal(hist.history[0].valid_to, -1); // open window
  } finally {
    await db.close();
  }
});

// ---------------------------------------------------------------------------
// Typed data paths on the fixture (check / trace / events / audit / retain)
// ---------------------------------------------------------------------------

test("check returns a typed CheckResult (VIOLATION on the fixture)", async (t) => {
  await withWriterDb(t, async (db) => {
    const r: CheckResult = await db.check(MOTOR, "current", 0, 9_900_000_000);
    assert.ok(["OK", "VIOLATION", "NO_DATA", "NO_SPEC"].includes(r.status));
    assert.equal(r.status, "VIOLATION");
    assert.equal(r.isViolation, true);
    assert.equal(r.window.to, 9_900_000_000);
    assert.equal(r.points_checked, 300);
    assert.equal(r.expected_max, 10.0);
    assert.ok((r.observed ?? 0) > (r.expected_max ?? 0));
    assert.ok(Math.abs((r.deviation ?? 0) - ((r.observed ?? 0) - (r.expected_max ?? 0))) < 1e-9);
    assert.equal(r.expected_provenance, "Specification");
    assert.equal(r.observed_provenance, "Observation");
  });
});

test("check NO_SPEC when the spec property is missing", async (t) => {
  await withWriterDb(t, async (db) => {
    const r = await db.check(PUMP, "pressure", 0, 9_900_000_000);
    assert.equal(r.status, "NO_SPEC");
  });
});

test("check unknown entity → semantic VidgeDBError(code=0)", async (t) => {
  await withWriterDb(t, async (db) => {
    await assert.rejects(
      () => db.check("Ghost-Machine", "current", 0, 100),
      (err: VidgeDBError) => {
        assert.equal(err.code, 0);
        assert.match(err.message, /unknown entity/);
        return true;
      },
    );
  });
});

test("getMeasurements returns the deterministic fixture series", async (t) => {
  await withWriterDb(t, async (db) => {
    const series = await db.getMeasurements(MOTOR, "current", 0, 9_900_000_000);
    assert.equal(series.count, 300);
    assert.equal(series.points.length, 300);
    assert.equal(series.points[0].t, 1_700_000_000);
    assert.ok(series.points[0].t < series.points[1].t);
    assert.ok(series.min !== null && series.max !== null);
    assert.ok((series.min ?? 0) <= (series.max ?? 0));

    const narrow = await db.getMeasurements(MOTOR, "current", 1_700_000_000, 1_700_000_060);
    assert.equal(narrow.count, 2);
    assert.deepEqual(
      narrow.points.map((p) => p.t),
      [1_700_000_000, 1_700_000_060],
    );
  });
});

test("queryTemporal returns aggregate rows", async (t) => {
  await withWriterDb(t, async (db) => {
    const r = await db.queryTemporal(
      "MATCH (m:Motor) MEASURE m.current DURING last(1h) RETURN count(current), max(current)",
      1_700_002_000,
    );
    assert.ok(r.n >= 1);
    const row = r.rows[0];
    assert.equal(row.count.kind, "Count");
    assert.ok(row.count.value >= 1.0);
  });
});

test("logEvent → getEvents roundtrip", async (t) => {
  await withWriterDb(t, async (db) => {
    const out = await db.logEvent("sdk_js_event", MOTOR, 1_700_000_500, 1, "from node:test");
    assert.equal(out.logged, true);
    assert.equal(out.event, "sdk_js_event");

    const ev = await db.getEvents(MOTOR);
    const names = ev.events.map((e: any) => e.name);
    assert.ok(names.includes("sdk_js_event"));
    const entry = ev.events.find((e: any) => e.name === "sdk_js_event");
    assert.equal(entry.provenance, "Observation"); // provenance byte 1
    assert.equal(entry.details, "from node:test");
  });
});

test("audit trail lists agent and methods", async (t) => {
  await withWriterDb(t, async (db) => {
    await db.schema();
    await db.check(MOTOR, "current", 0, 9_900_000_000);
    const a = await db.audit();
    const methods = a.entries.map((e) => e.method);
    assert.ok(methods.includes("schema"));
    assert.ok(methods.includes("check"));
    assert.ok(a.entries.every((e) => e.agent_id === "sdk-js-test"));
  });
});

test("getEntity card and trace across the line", async (t) => {
  await withWriterDb(t, async (db) => {
    const card = await db.getEntity(2);
    assert.equal(card.name, MOTOR);
    assert.equal(card.type, "Motor");

    const tr = await db.trace(PLC, PUMP, 4);
    assert.equal(tr.found, true);
    const names = tr.path.map((n) => n.name);
    assert.deepEqual(names, [PLC, "Drive_000001", MOTOR, PUMP]);
    assert.equal(tr.n_hops, 3);

    const missing = await db.trace(PLC, PUMP, 1); // out of reach in 1 hop
    assert.equal(missing.found, false);
    assert.deepEqual(missing.path, []);
  });
});

test("retain reports counts", async (t) => {
  await withWriterDb(t, async (db) => {
    const r = await db.retain(NOW);
    assert.deepEqual(Object.keys(r).sort(), ["chunks_removed", "points_removed"]);
    assert.ok(typeof r.points_removed === "number");
    assert.ok(typeof r.chunks_removed === "number");
  });
});

test("dynamic call('provenance') reaches unmirrored methods", async (t) => {
  await withWriterDb(t, async (db) => {
    const prov = await db.provenance();
    assert.ok("provenance_classes" in prov);
  });
});

// ---------------------------------------------------------------------------
// Concurrency — the Promise queue must serialize concurrent round-trips
// ---------------------------------------------------------------------------

test("concurrent calls are sequenced and all resolve correctly", async (t) => {
  await withWriterDb(t, async (db) => {
    const jobs: Promise<any>[] = [];
    for (let i = 0; i < 20; i++) {
      jobs.push(i % 2 === 0 ? db.schema() : db.getMeasurements(MOTOR, "current", 0, 9_900_000_000));
    }
    const results = await Promise.all(jobs);
    for (let i = 0; i < 20; i++) {
      if (i % 2 === 0) assert.ok(results[i].provenance_classes.length > 0);
      else assert.equal(results[i].count, 300);
    }
  });
});

// ---------------------------------------------------------------------------
// version parity
// ---------------------------------------------------------------------------

test("VERSION matches package.json", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(SDK_ROOT, "package.json"), "utf8"));
  assert.equal(VERSION, pkg.version);
  assert.equal(VERSION, "0.1.0");
});
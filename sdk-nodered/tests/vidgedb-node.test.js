//! Unit tests for the VidgeDB Node-RED node module (Phase 13).
//!
//! Two layers, no Node-RED runtime needed:
//!   1. pure `parseResponse` — the JSON-RPC 2.0 response parser the
//!      nodes/vidgedb.js stdout line-handler uses (every documented shape:
//!      ok, every transport error code, the AgentApi v0 {"error":…} shape);
//!   2. real `VidgeClient` subprocess management against the actual Rust
//!      binary — spawn, schema round-trip, VQL round-trip, ingest round-trip
//!      (writer role), reader-refused write (-32003), malformed-line
//!      tolerance, clean close (stdin EOF), and respawn-after-kill.
//!
//! Run: node --test tests/vidgedb-node.test.js
//! Skips itself if the vidgedb binary is missing (pure-parse tests still run).

"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const mod = require("../nodes/vidgedb.js");
const { parseResponse, VidgeClient, CONSTANTS } = mod;

const {
    PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS,
    STORAGE_ERROR, WRITE_FORBIDDEN,
} = CONSTANTS;

// The engine binary: $VIDGEDB_BIN first (the documented contract), then the
// bare name for PATH resolution. No hardcoded path into another checkout —
// this repository must be testable on its own.
const BIN = process.env.VIDGEDB_BIN || "vidgedb";
const HAVE_BIN = binAvailable();

function binAvailable() {
    if (BIN.includes("/")) return fs.existsSync(BIN);
    // Bare name: look for it on PATH (a POSIX-ish heuristic is fine here).
    return (process.env.PATH || "").split(path.delimiter).some((dir) => {
        try { return fs.existsSync(path.join(dir, BIN)); } catch (err) { return false; }
    });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Temp workspace for a scratch .vdg file (never pollutes the repo). */
function tmpDb() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "vidgedb-nodered-"));
    return { dir, file: path.join(dir, "twin.vdg") };
}

// ===========================================================================
// 1. parseResponse — the JSON-RPC response parser (pure)
// ===========================================================================

test("parseResponse: valid ok response routes by id", () => {
    const r = parseResponse({ jsonrpc: "2.0", id: 1, result: { rows: [], n: 0 } });
    assert.ok(r, "should parse");
    assert.equal(r.id, 1);
    assert.deepEqual(r.result, { rows: [], n: 0 });
    assert.equal(r.error, undefined);
});

test("parseResponse: null id (notification-shaped reply) is valid", () => {
    const r = parseResponse({ jsonrpc: "2.0", id: null, result: {} });
    assert.ok(r);
    assert.equal(r.id, "null");
});

test("parseResponse: string id round-trips", () => {
    const r = parseResponse({ jsonrpc: "2.0", id: "abc", result: null });
    assert.ok(r);
    assert.equal(r.id, "abc");
});

test("parseResponse: numeric id normalizes to number", () => {
    const r = parseResponse({ jsonrpc: "2.0", id: 42, result: null });
    assert.ok(r);
    assert.strictEqual(r.id, 42);
});

// Every documented transport error code surfaces with code + message intact.
for (const [code, label] of [
    [-32700, "parse error"],
    [-32600, "invalid request"],
    [-32601, "method not found"],
    [-32602, "invalid params"],
    [-32000, "storage error"],
    [-32003, "write forbidden"],
]) {
    test(`parseResponse: error response ${code} (${label}) passes through`, () => {
        const r = parseResponse({
            jsonrpc: "2.0", id: 9,
            error: { code, message: label },
        });
        assert.ok(r, "should parse");
        assert.equal(r.id, 9);
        assert.ok(r.error, "error member present");
        assert.equal(r.error.code, code);
        assert.equal(r.error.message, label);
        assert.equal(r.result, undefined, "no result member on error");
    });
}

test("parseResponse: not-an-object -> null (stdout garbage)", () => {
    assert.equal(parseResponse(null), null);
    assert.equal(parseResponse(42), null);
    assert.equal(parseResponse("hello"), null);
    assert.equal(parseResponse([1, 2, 3]), null);
});

test("parseResponse: missing jsonrpc version -> null", () => {
    assert.equal(parseResponse({ id: 1, result: {} }), null);
    assert.equal(parseResponse({ jsonrpc: "1.0", id: 1, result: {} }), null);
});

test("parseResponse: missing id -> null (a stray notification line)", () => {
    assert.equal(parseResponse({ jsonrpc: "2.0", result: {} }), null);
});

test("parseResponse: neither result nor error -> null", () => {
    assert.equal(parseResponse({ jsonrpc: "2.0", id: 1 }), null);
});

test("parseResponse: BOTH result and error -> null (never routes that)", () => {
    assert.equal(
        parseResponse({ jsonrpc: "2.0", id: 1, result: {}, error: { code: 1, message: "x" } }),
        null
    );
});

test("parseResponse: malformed error object (missing code / message) -> null", () => {
    assert.equal(parseResponse({ jsonrpc: "2.0", id: 1, error: { message: "no code" } }), null);
    assert.equal(parseResponse({ jsonrpc: "2.0", id: 1, error: { code: -1 } }), null);
    assert.equal(parseResponse({ jsonrpc: "2.0", id: 1, error: "flat string is invalid" }), null);
});

test("parseResponse: undefined result normalizes to null (JSON-RPC 2.0 allows it)", () => {
    const r = parseResponse({ jsonrpc: "2.0", id: 5, result: undefined });
    assert.ok(r);
    assert.strictEqual(r.result, null);
});

// ===========================================================================
// 2. VidgeClient — the actual subprocess (skips cleanly without the binary)
// ===========================================================================

test("client: schema round-trip over the real binary (spawn + line protocol)", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const states = [];
    const client = new VidgeClient({
        dbPath: file,
        bin: BIN,
        role: "writer",
        agentId: "nodered-unit",
        timeoutMs: 15000,
        respawn: false,
        onChange: (s) => states.push(s),
    });
    try {
        client.start();
        // Wait for the spawn to settle (state event runs synchronously on
        // 'spawn' via the process handle, but give the child a beat).
        for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
        assert.ok(client.isRunning(), "subprocess should be running");
        assert.ok(states.some((s) => s.running === true), "onChange running=true fired");

        const resp = await client.request("schema", {});
        assert.ok(resp.result, "schema ok");
        assert.ok(Array.isArray(resp.result.provenance_classes));
        assert.equal(resp.result.provenance_classes.length, 8);
        assert.ok(resp.result.provenance_classes.includes("Fact"));

        // VQL after a schema — proves multiplexing works over sequential ids.
        await client.request("upsert_entity", { name: "Motor77", type: "Motor", source: "plc" });
        const q = await client.request("query", { vql: "MATCH (m:Motor) RETURN name" });
        assert.ok(Array.isArray(q.result.rows));
        assert.equal(q.result.n, 1);
        // Row shape: one key per bound variable — {m: {key, name, type, props}}.
        assert.equal(q.result.rows[0].m.name, "Motor77");
    } finally {
        client.close();
    }
    // stdin EOF is the service's polite exit — verify the child actually left.
    await sleep(120);
    assert.equal(
        states.filter((s) => s.running === false).length > 0,
        true,
        "onChange running=false fired on close"
    );
});

test("client: ingest_points round-trip (writer role, mixed point forms)", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const client = new VidgeClient({
        dbPath: file, bin: BIN, role: "writer", agentId: "nodered-unit",
        timeoutMs: 15000, respawn: false, onChange: () => { },
    });
    try {
        client.start();
        for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
        const now = Math.floor(Date.now() / 1000);
        const resp = await client.request("ingest_points", {
            entity: "PLC1",
            signal: "temp",
            points: [[now - 10, 55.5], [now - 5, 60.0], { ts: now, value: 61.25 }],
        });
        assert.equal(resp.error, undefined, "no JSON-RPC error");
        assert.ok(resp.result.accepted >= 3, "accepted the 3 points");
        const m = await client.request("get_measurements", { entity: "PLC1", signal: "temp" });
        // get_measurements shape: {count, min, max, points:[{t, value}…]}.
        assert.ok(Array.isArray(m.result.points));
        assert.equal(m.result.count, 3);
        assert.equal(m.result.points.length, 3);
        assert.equal(m.result.max, 61.25);
        assert.equal(m.result.min, 55.5);
    } finally {
        client.close();
    }
});

test("client: reader role surfaces -32003 write forbidden", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const client = new VidgeClient({
        dbPath: file, bin: BIN, role: "reader", agentId: "nodered-unit",
        timeoutMs: 15000, respawn: false, onChange: () => { },
    });
    try {
        client.start();
        for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
        const resp = await client.request("ingest_points", {
            entity: "X", signal: "y", points: [[1, 1]],
        });
        assert.ok(resp.error, "reader write refused");
        assert.equal(resp.error.code, WRITE_FORBIDDEN);
    } finally {
        client.close();
    }
});

test("client: JSON-RPC errors surface as {error} (unknown method / bad params)", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const client = new VidgeClient({
        dbPath: file, bin: BIN, role: "reader", agentId: "nodered-unit",
        timeoutMs: 15000, respawn: false, onChange: () => { },
    });
    try {
        client.start();
        for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
        const nf = await client.request("definitely_not_a_method", {});
        assert.equal(nf.error.code, METHOD_NOT_FOUND);

        const bad = await client.request("get_entity", { key: "not-a-number!!" });
        assert.equal(bad.error.code, INVALID_PARAMS);
    } finally {
        client.close();
    }
});

test("client: clean close — stdin EOF makes the child exit", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const client = new VidgeClient({
        dbPath: file, bin: BIN, role: "reader", agentId: "nodered-unit",
        timeoutMs: 15000, respawn: false, onChange: () => { },
    });
    client.start();
    for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
    const child = client.proc;
    assert.ok(child && child.pid > 0);
    client.close();
    // The child must DIE within CLOSE_WAIT (2 s), not linger as an orphan.
    for (let i = 0; i < 100 && child.exitCode === null && child.signalCode === null; i++) {
        await sleep(50);

    }
    const exited = child.exitCode !== null || child.signalCode !== null;
    assert.ok(exited, "child exited after stdin.close()");
});

test("client: stdout garbage does not corrupt the id-mux", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const client = new VidgeClient({
        dbPath: file, bin: BIN, role: "reader", agentId: "nodered-unit",
        timeoutMs: 15000, respawn: false, onChange: () => { },
    });
    try {
        client.start();
        for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
        // Inject a garbage line between legit requests.
        client.onLine("this is not json");
        client.onLine("42");
        const resp = await client.request("schema", {});
        assert.ok(resp.result, "garbage lines don't poison the mux");
        // Feeding a response for an UNKNOWN id must be silently dropped.
        client.onLine(JSON.stringify({ jsonrpc: "2.0", id: 99999, result: {} }));
        const resp2 = await client.request("provenance", {});
        assert.ok(resp2.result, "id-mux unharmed after stray response");
    } finally {
        client.close();
    }
});

test("client: respawn when the child is killed (auto-respawn on)", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const { file } = tmpDb();
    const states = [];
    const client = new VidgeClient({
        dbPath: file, bin: BIN, role: "reader", agentId: "nodered-unit",
        timeoutMs: 15000, respawn: true, onChange: (s) => states.push(s),
    });
    try {
        client.start();
        for (let i = 0; i < 50 && !client.isRunning(); i++) await sleep(20);
        assert.ok(client.isRunning(), "first spawn live");
        const firstPid = client.proc.pid;

        // Commit the "crash": SIGKILL the binary while a request is in flight.
        const pendingPromise = client.request("schema", {}).catch((e) => e);
        client.proc.kill("SIGKILL");

        // The in-flight request rejects (child died), auto-respawn after 2.5 s.
        const err = await pendingPromise;
        assert.ok(/exited|not running|closed/.test(String(err && err.message)), "in-flight rejected on child death: " + (err && err.message));

        const deadline = Date.now() + 6000;
        while (Date.now() < deadline && !(client.proc && client.isRunning() && client.proc.pid !== firstPid)) {
            await sleep(100);
        }
        assert.ok(client.proc, "respawned");
        assert.notEqual(client.proc.pid, firstPid, "a NEW pid appeared");
        assert.ok(states.some((s) => s.running === false), "down state was reported");
        // Respawned child answers again.
        const resp = await client.request("schema", {});
        assert.ok(resp.result);
    } finally {
        client.close();
    }
});

test("client: timeout on a hung request rejects and clears", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    // A stand-in "service" that reads stdin forever but NEVER answers:
    // `sleep` ignores stdin entirely (equivalently cat, minus stdout noise).
    const client = new VidgeClient({
        dbPath: "/whatever.vdg", bin: "/bin/sleep",
        role: "reader", timeoutMs: 400, respawn: false, onChange: () => { },
    });
    // Sleep takes an arg; give it 30 s of patience it will never use.
    client.argv = () => ["/bin/sleep", "30"];
    try {
        client.start();
        await assert.rejects(
            () => client.request("schema", {}),
            /timed out after 400 ms/,
            "request timed out cleanly"
        );
    } finally {
        client.close();
    }
});

test("client: close() rejects in-flight requests", { skip: !HAVE_BIN && "vidgedb binary not built" }, async () => {
    const client = new VidgeClient({
        dbPath: "/whatever.vdg", bin: "/bin/sleep",
        role: "reader", timeoutMs: 9000, respawn: false, onChange: () => { },
    });
    client.argv = () => ["/bin/sleep", "30"];
    client.start();
    const p = client.request("schema", {}).catch((e) => e);
    client.close();
    const err = await p;
    assert.ok(
        /closed|not running|exited/.test(String(err && err.message)),
        "pending rejected once close() ran: " + (err && err.message)
    );
});
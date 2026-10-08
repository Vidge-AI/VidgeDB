//! VidgeDB Node-RED nodes — Phase 13 (the visual connector for real machines).
//!
//! One config node (`vidgedb-service`) owns the spawned
//! `vidgedb --service <db.vdg>` subprocess (line-delimited JSON-RPC 2.0 on
//! its stdin/stdout — the same wire the Python SDK speaks); four function
//! nodes talk to it through the shared client:
//!
//!   - vidgedb-query     : msg.payload (a VQL string, or {vql, now}) -> rows
//!   - vidgedb-ingest    : msg.payload = {entity, signal, points} -> ingest_points
//!   - vidgedb-check     : msg.payload = {entity, signal}[..] -> msg.violations on VIOLATION
//!   - vidgedb-log-event : msg.payload = {name, entity[, timestamp, provenance, details]}
//!
//! Every function node reports the subprocess state on its status strip:
//! a GREEN dot while running, a RED ring while down (dead/respawning/not
//! found). The client node spawns at deploy, respawns the binary if it
//! dies, and closes stdin cleanly at close/re-deploy (the service exits on
//! EOF — the polite shutdown the Python SDK uses too).
//!
//! Protocol (src/service.rs): one request line -> one response line,
//! `{jsonrpc:"2.0",id,result}` or `{jsonrpc:"2.0",id,error:{code,message}}`.
//! Error codes: -32700/-32600/-32601/-32602/-32000, -32003 write-forbidden
//! (Reader role on a write method). Method-level semantic problems come
//! back INSIDE `result` as the AgentApi v0 `{"error": "..."}` shape.

"use strict";

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");

// JSON-RPC error codes mirrored from src/service.rs.
const PARSE_ERROR = -32700;
const INVALID_REQUEST = -32600;
const METHOD_NOT_FOUND = -32601;
const INVALID_PARAMS = -32602;
const STORAGE_ERROR = -32000;
const WRITE_FORBIDDEN = -32003;

const DEFAULT_TIMEOUT_MS = 30000;
const RESPAWN_DELAY_MS = 2500;
const CLOSE_WAIT_MS = 2000;

// ---------------------------------------------------------------------------
// Client: one vidgedb --service subprocess + JSON-RPC request/response mux
// ---------------------------------------------------------------------------

/**
 * A managed `vidgedb --service <path>` subprocess.
 *
 * - spawn() on deploy (first use), respawn with backoff if the process dies,
 *   clean close (stdin.end -> the child exits on EOF) on node close.
 * - requests are serialized per client (the binary is a single-threaded
 *   service; a request gets exactly one response line, in order).
 * - pending requests are rejected when the child dies (the id-mux keeps
 *   incrementing, so no stale response survives a respawn).
 */
class VidgeClient {
    /**
     * @param {object} opts  {dbPath, bin, role, agentId, retentionDays,
     *   timeoutMs, respawn, onChange(state)} — `onChange` reports
     *   `{running: boolean, detail: string}` on spawn/exit/close, which the
     *   config node forwards so every function node can paint its status.
     */
    constructor(opts) {
        this.dbPath = opts.dbPath;
        this.bin = opts.bin || process.env.VIDGEDB_BIN || "vidgedb";
        this.role = opts.role || "reader";
        this.agentId = opts.agentId || "";
        this.retentionDays = opts.retentionDays || 0;
        this.timeoutMs = opts.timeoutMs || DEFAULT_TIMEOUT_MS;
        this.respawn = opts.respawn !== false;
        this.onChange = opts.onChange || (() => { });

        this.proc = null;
        this.closed = false;
        this.nextId = 1;
        this.pending = new Map(); // id -> {resolve,reject,timer}
        this.buf = "";            // stdout line reassembly
        this.respawnTimer = null;
        this.startedAt = 0;
    }

    /** Full argv for the subprocess (mirrors the Python SDK's args). */
    argv() {
        const args = [this.bin, "--service", this.dbPath];
        if (this.agentId) args.push("--agent-id", this.agentId);
        if (this.retentionDays) args.push("--retention-days", String(this.retentionDays));
        if (this.role && this.role !== "reader") args.push("--role", this.role);
        return args;
    }

    /** Spawn the subprocess (once; later calls are no-ops while healthy). */
    start() {
        this.closed = false;
        const args = this.argv();
        let proc;
        try {
            proc = spawn(args[0], args.slice(1), {
                stdio: ["pipe", "pipe", "pipe"],
            });
        } catch (err) {
            this.onChange({ running: false, detail: err.message });
            this.scheduleRespawn();
            return;
        }
        this.proc = proc;
        this.startedAt = Date.now();
        this.onChange({ running: true, detail: `pid ${proc.pid}` });

        let stdoutBuf = "";
        proc.stdout.on("data", (chunk) => {
            stdoutBuf += chunk.toString("utf8");
            let nl;
            while ((nl = stdoutBuf.indexOf("\n")) >= 0) {
                const line = stdoutBuf.slice(0, nl).trim();
                stdoutBuf = stdoutBuf.slice(nl + 1);
                if (line) this.onLine(line);
            }
        });

        proc.stderr.on("data", () => {
            // Service logs go to stderr by design — not parsed, not status.
        });

        proc.on("error", (err) => {
            // spawn() failure (ENOENT etc.): resolve every pending request.
            const pending = [...this.pending.values()];
            this.pending.clear();
            pending.forEach((p) =>
                p.reject(new Error(`vidgedb spawn failed: ${err.message}`))
            );
            this.proc = null;
            this.onChange({ running: false, detail: err.message });
            this.scheduleRespawn();
        });

        proc.on("exit", (code, signal) => {
            const pending = [...this.pending.values()];
            this.pending.clear();
            pending.forEach((p) =>
                p.reject(new Error(`vidgedb process exited (code=${code}, signal=${signal})`))
            );
            this.proc = null;
            this.buf = "";
            this.onChange({ running: false, detail: `exited (code=${code} ${signal || ""})`.trim() });
            if (!this.closed) this.scheduleRespawn();
        });
    }

    /** One stdout line: parse the JSON-RPC response and route by id. */
    onLine(line) {
        let resp;
        try {
            resp = JSON.parse(line);
        } catch (err) {
            // Malformed line from stdout — surfaces as PARSE_ERROR-ish; no
            // id to route to, so log-and-continue (never crash the flow).
            this.onChange({ running: true, detail: `unparseable line: ${line.slice(0, 40)}` });
            return;
        }
        resp = parseResponse(resp); // tolerant normalization (see below)
        if (resp === null) {
            this.onChange({ running: true, detail: "not a JSON-RPC response" });
            return;
        }
        const entry = this.pending.get(resp.id);
        if (!entry) return; // stray/late response (e.g. after a timeout) — ignore
        this.pending.delete(resp.id);
        clearTimeout(entry.timer);
        entry.resolve(resp);
    }

    /**
     * Send one JSON-RPC request; resolves to the FULL response object
     * ({result}|{error}) — method-level `{error:"..."}`-in-result shapes
     * are the caller's business (node-specific surfacing).
     */
    request(method, params) {
        return new Promise((resolve, reject) => {
            if (this.closed) {
                reject(new Error("vidgedb client is closed"));
                return;
            }
            if (!this.proc || !this.proc.stdin.writable) {
                reject(new Error("vidgedb process is not running"));
                return;
            }
            const id = this.nextId++;
            const req = { jsonrpc: "2.0", id, method };
            if (params !== undefined) req.params = params;
            const line = JSON.stringify(req) + "\n";
            const timer = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error(`vidgedb request timed out after ${this.timeoutMs} ms (${method})`));
            }, this.timeoutMs);
            this.pending.set(id, { resolve, reject, timer });
            try {
                this.proc.stdin.write(line);
                this.proc.stdin.flush && this.proc.stdin.flush();
            } catch (err) {
                this.pending.delete(id);
                clearTimeout(timer);
                reject(new Error(`vidgedb write failed: ${err.message}`));
            }
        });
    }

    /** Dead child: schedule a respawn (skipped once the client is closed). */
    scheduleRespawn() {
        if (this.closed || !this.respawn) return;
        if (this.respawnTimer) return; // already scheduled
        this.respawnTimer = setTimeout(() => {
            this.respawnTimer = null;
            if (this.closed) return;
            this.start();
        }, RESPAWN_DELAY_MS);
    }

    /** Clean close: stdin EOF is the service's exit signal, then SIGKILL. */
    close() {
        this.closed = true;
        if (this.respawnTimer) {
            clearTimeout(this.respawnTimer);
            this.respawnTimer = null;
        }
        const pending = [...this.pending.values()];
        this.pending.clear();
        pending.forEach((p) => {
            clearTimeout(p.timer);
            p.reject(new Error("vidgedb client is closed"));
        });
        const proc = this.proc;
        this.proc = null;
        if (!proc) return;
        try {
            proc.stdin.end(); // EOF; the service exits cleanly, no kill needed
        } catch (err) {
            /* already dead */
        }
        const killTimer = setTimeout(() => {
            try {
                proc.kill("SIGKILL");
            } catch (err) {
                /* already gone */
            }
        }, CLOSE_WAIT_MS);
        killTimer.unref && killTimer.unref();
        this.onChange({ running: false, detail: "closed" });
    }

    /** Is the subprocess currently alive and writable? */
    isRunning() {
        return !!(this.proc && this.proc.stdin && this.proc.stdin.writable && this.proc.exitCode === null);
    }
}

// ---------------------------------------------------------------------------
// Response parsing (the unit-tested core: pure, no Node-RED involved)
// ---------------------------------------------------------------------------

/**
 * Parse/normalize ONE parsed JSON-RPC response line.
 * Returns the normalized {id,result?|error?} object, or null when the line
 * is not a valid response. Id comparison is tolerant: numeric ids stringify
 * both sides (the binary echoes the numeric id verbatim).
 */
function parseResponse(resp) {
    if (!resp || typeof resp !== "object" || Array.isArray(resp)) return null;
    if (resp.jsonrpc !== "2.0" && resp.jsonrpc !== 2) return null;
    if (!("id" in resp)) return null;
    const id = typeof resp.id === "number" ? resp.id : String(resp.id);
    const hasResult = "result" in resp;
    const hasError = resp.error && typeof resp.error === "object";
    if (!hasResult && !hasError) return null;
    if (hasResult && hasError) return null;
    if (hasError) {
        const err = resp.error;
        if (typeof err.code !== "number" || typeof err.message !== "string") return null;
        return { id, jsonrpc: "2.0", error: err };
    }
    return { id, jsonrpc: "2.0", result: resp.result === undefined ? null : resp.result };
}

// ---------------------------------------------------------------------------
// Node-RED plugin: one config node + four function nodes
// ---------------------------------------------------------------------------

let RED; // bound in module.exports — the Node-RED runtime

function validateDbPath(dbPath) {
    return (
        typeof dbPath === "string" &&
        dbPath.trim().length > 0 &&
        /\.(vdg|vidge|db)$/i.test(dbPath.trim())
    );
}

module.exports = function (RED) {
    RED = RED;

    // ---- config node: vidgedb-service -------------------------------------
    function VidgeServiceNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        /**
         * User-facing fields (validated):
         *  - binaryPath : abs path to the vidgedb binary (or on PATH)
         *  - dbPath     : the .vdg file
         *  - role       : reader | writer | ingest (default reader)
         *  - agentId    : audit-trail identifier for this connector
         *  - retentionDays, timeoutMs, autoRespawn
         */
        node.binaryPath = (config.binaryPath || process.env.VIDGEDB_BIN || "vidgedb").trim();
        node.dbPath = (config.dbPath || "").trim();
        node.role = (config.role || "reader").trim();
        node.agentId = (config.agentId || "").trim();
        node.retentionDays = parseInt(config.retentionDays, 10) || 0;
        node.timeoutMs = parseInt(config.timeoutMs, 10) || DEFAULT_TIMEOUT_MS;
        node.autoRespawn = config.autoRespawn !== false;

        node.client = null;

        /** Lazily spawn; reuses a healthy client across redeploys. */
        node.ensureClient = function () {
            if (node.client && node.client.isRunning()) return node.client;
            if (node.client) node.client.close();
            const client = new VidgeClient({
                dbPath: node.dbPath,
                bin: node.binaryPath,
                role: node.role,
                agentId: node.agentId,
                retentionDays: node.retentionDays,
                timeoutMs: node.timeoutMs,
                respawn: node.autoRespawn,
                onChange: (state) => {
                    node.subprocessState = state;
                    node.emit("vidgedb:state", state);
                },
            });
            node.client = client;
            client.start();
            return client;
        };

        node.getClient = function () {
            return node.client;
        };

        node.on("close", function (removed, done) {
            // Redeploy of ANY node sharing this config calls close() — the
            // service must exit cleanly (stdin EOF), not leave orphans.
            if (node.client) {
                const c = node.client;
                node.client = null;
                c.close();
            }
            if (done) done();
        });
    }
    RED.nodes.registerType("vidgedb-service", VidgeServiceNode);

    // ---- shared helpers for the function nodes ----------------------------

    /** Red-ring / green-dot status per function node. */
    function statusRunning(n) {
        n.status({ fill: "green", shape: "dot", text: "running" });
    }
    function statusDown(n, why) {
        n.status({ fill: "red", shape: "ring", text: why || "down" });
    }
    function statusError(n, txt) {
        n.status({ fill: "red", shape: "ring", text: (txt || "error").slice(0, 30) });
    }

    /**
     * Subscribe one function node to the config node's "vidgedb:state"
     * events: GREEN dot while the subprocess runs, RED ring while down.
     * Auto-unsubscribes when the function node closes.
     */
    function bindState(client, configNode, n) {
        const handler = (state) => {
            if (state.running) statusRunning(n);
            else statusDown(n, (state.detail || "down").slice(0, 26));
        };
        configNode.on("vidgedb:state", handler);
        n.on("close", function () {
            try { configNode.removeListener("vidgedb:state", handler); } catch (err) { /* noop */ }
        });
        // Paint immediately (spawn is async; a state may already exist).
        const cur = configNode.subprocessState;
        if (client && client.proc && (!cur || cur.running !== false)) statusRunning(n);
        else statusDown(n, (cur && (cur.detail || "down")) || "starting");
    }

    /** Normalize a JSON-RPC error object into a Node-RED-friendly error. */
    function rpcErrorMessage(err) {
        if (!err) return "unknown error";
        if (err.error && typeof err.error === "object") {
            return `vidgedb error ${err.error.code || ""}: ${err.error.message || ""}`.trim();
        }
        return err.message || String(err);
    }

    function isResultError(result) {
        // AgentApi v0 semantic shape: `{"error": "..."}` inside result.
        return (
            result &&
            typeof result === "object" &&
            !Array.isArray(result) &&
            typeof result.error === "string"
        );
    }

    function nowSecs() {
        return Math.floor(Date.now() / 1000);
    }

    // ---- vidgedb-query -----------------------------------------------------
    function VidgeQueryNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.serviceConfig = RED.nodes.getNode(config.service);

        node.on("input", async function (msg, send, done) {
            const cfg = node.serviceConfig;
            if (!cfg) {
                statusError(node, "no config");
                done && done(new Error("vidgedb-query: no vidgedb-service config node selected"));
                return;
            }
            const client = cfg.ensureClient();
            bindState(client, cfg, node);

            const payload = msg.payload;
            let vql, now, temporal = false;
            if (typeof payload === "string") {
                vql = payload.trim();
            } else if (payload && typeof payload === "object") {
                vql = typeof payload.vql === "string" ? payload.vql.trim() : "";
                if (payload.now !== undefined) now = payload.now;
                if (payload.temporal === true) temporal = true;
                if (payload.now !== undefined || payload.temporal === true) temporal = true;
            }
            if (!vql) {
                statusError(node, "no vql");
                done && done(new Error("vidgedb-query: msg.payload must be a VQL string or {vql} object"));
                return;
            }

            try {
                const method = temporal ? "query_temporal" : "query";
                const params = temporal ? { vql, now: now !== undefined ? now : nowSecs() } : { vql };
                const resp = await client.request(method, params);
                if (resp.error) {
                    statusError(node, `rpc ${resp.error.code || ""}`);
                    msg.error = { code: resp.error.code || 0, message: resp.error.message || "" };
                    send(msg);
                    if (done) done(new Error(rpcErrorMessage(resp)));
                    return;
                }
                const result = resp.result;
                const rows = result && Array.isArray(result.rows) ? result.rows : [];
                msg.payload = rows;
                msg.rows = rows;
                msg.n = rows.length;
                if (isResultError(result)) {
                    // Query parse/execution problem — surface inside result
                    // (never a panic), status still shows the issue.
                    statusError(node, result.error.slice(0, 24));
                    msg.queryError = result.error;
                } else {
                    statusRunning(node);
                }
                send(msg);
                if (done) done();
            } catch (err) {
                statusError(node, err.message.slice(0, 24));
                (node.error(err, msg), done && done(err));
            }
        });
    }
    RED.nodes.registerType("vidgedb-query", VidgeQueryNode);

    // ---- vidgedb-ingest ----------------------------------------------------
    function VidgeIngestNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.serviceConfig = RED.nodes.getNode(config.service);
        node.defaultEntity = (config.entity || "").trim();
        node.defaultSignal = (config.signal || "").trim();

        node.on("input", async function (msg, send, done) {
            const cfg = node.serviceConfig;
            if (!cfg) {
                statusError(node, "no config");
                done && done(new Error("vidgedb-ingest: no vidgedb-service config node selected"));
                return;
            }
            const client = cfg.ensureClient();
            bindState(client, cfg, node);

            const p = msg.payload;
            const entity = (p && p.entity) || node.defaultEntity;
            const signal = (p && p.signal) || node.defaultSignal;
            let points = p && Array.isArray(p.points) ? p.points : null;

            // Accept a bare [ts,value] pair (MQTT pattern: one message ==
            // one reading) by wrapping it into a single-point batch.
            if (!points && Array.isArray(p) && p.length === 2
                && typeof p[0] === "number" && typeof p[1] === "number") {
                points = [p];
            }

            if (!entity || !signal || !Array.isArray(points) || points.length === 0) {
                statusError(node, "bad payload");
                done && done(new Error(
                    "vidgedb-ingest: msg.payload must be {entity, signal, points:[[ts,value],…]} (non-empty points, integer ts + number value)"
                ));
                return;
            }

            // Normalize object-form points {ts,value}/{timestamp,value} into
            // pair form; validate types (the binary refuses gracefully, but
            // we can catch most mistakes before a round-trip).
            const normPoints = points.map((pt) => {
                if (Array.isArray(pt) && pt.length === 2
                    && Number.isInteger(pt[0]) && typeof pt[1] === "number") {
                    return [pt[0], pt[1]];
                }
                if (pt && typeof pt === "object" && !Array.isArray(pt)) {
                    const t = pt.ts !== undefined ? pt.ts : pt.timestamp;
                    const v = pt.value;
                    if (Number.isInteger(t) && typeof v === "number") return [t, v];
                }
                return null;
            });
            if (normPoints.some((x) => x === null)) {
                statusError(node, "bad point");
                done && done(new Error(
                    "vidgedb-ingest: every point must be [integerTs, numberValue] (or {ts, value})"
                ));
                return;
            }

            if (cfg.role === "reader") {
                statusError(node, "role=reader");
                done && done(new Error(
                    "vidgedb-ingest: the selected vidgedb-service role is 'reader' — ingest needs --role writer|ingest"
                ));
                return;
            }

            try {
                const resp = await client.request("ingest_points", {
                    entity, signal, points: normPoints,
                });
                if (resp.error) {
                    // -32003 write-forbidden when the configured role lost a race
                    statusError(node, resp.error.code === WRITE_FORBIDDEN ? "write forbidden" : `rpc ${resp.error.code}`);
                    msg.error = { code: resp.error.code || 0, message: resp.error.message || "" };
                    send(msg);
                    if (done) done(new Error(rpcErrorMessage(resp)));
                    return;
                }
                const result = resp.result;
                if (isResultError(result)) {
                    statusError(node, result.error.slice(0, 24));
                    msg.ingestError = result.error;
                    msg.payload = { ok: false, error: result.error };
                    send(msg);
                    if (done) done(new Error(result.error));
                    return;
                }
                statusRunning(node);
                msg.payload = Object.assign({ ok: true }, result);
                msg.ingested = result && result.accepted !== undefined ? result.accepted : msg.payload.ingested;
                send(msg);
                if (done) done();
            } catch (err) {
                statusError(node, err.message.slice(0, 24));
                done && done(err);
            }
        });
    }
    RED.nodes.registerType("vidgedb-ingest", VidgeIngestNode);

    // ---- vidgedb-check ------------------------------------------------------
    function VidgeCheckNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.serviceConfig = RED.nodes.getNode(config.service);

        node.on("input", async function (msg, send, done) {
            const cfg = node.serviceConfig;
            if (!cfg) {
                statusError(node, "no config");
                done && done(new Error("vidgedb-check: no vidgedb-service config node selected"));
                return;
            }
            const client = cfg.ensureClient();
            bindState(client, cfg, node);

            const p = msg.payload || {};
            const entity = p.entity;
            const signal = p.signal;
            if (!entity || !signal) {
                statusError(node, "no entity/signal");
                done && done(new Error("vidgedb-check: msg.payload must be {entity, signal, from?, to?}"));
                return;
            }
            const params = { entity, signal };
            if (p.from !== undefined) params.from = p.from;
            if (p.to !== undefined) params.to = p.to;

            try {
                const resp = await client.request("check", params);
                if (resp.error) {
                    statusError(node, `rpc ${resp.error.code || ""}`);
                    msg.error = { code: resp.error.code || 0, message: resp.error.message || "" };
                    send(msg);
                    if (done) done(new Error(rpcErrorMessage(resp)));
                    return;
                }
                const result = resp.result;
                if (isResultError(result)) {
                    statusError(node, result.error.slice(0, 24));
                    msg.checkError = result.error;
                    send(msg);
                    if (done) done(new Error(result.error));
                    return;
                }
                const status = result && result.status ? result.status : "";
                msg.payload = result;
                if (status === "VIOLATION") {
                    msg.violations = [result];
                    statusError(node, "VIOLATION"); // a violation IS a red event
                } else {
                    msg.violations = [];
                    statusRunning(node);
                }
                send(msg);
                if (done) done();
            } catch (err) {
                statusError(node, err.message.slice(0, 24));
                done && done(err);
            }
        });
    }
    RED.nodes.registerType("vidgedb-check", VidgeCheckNode);

    // ---- vidgedb-log-event --------------------------------------------------
    function VidgeLogEventNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.serviceConfig = RED.nodes.getNode(config.service);

        node.on("input", async function (msg, send, done) {
            const cfg = node.serviceConfig;
            if (!cfg) {
                statusError(node, "no config");
                done && done(new Error("vidgedb-log-event: no vidgedb-service config node selected"));
                return;
            }
            if (cfg.role === "reader") {
                statusError(node, "role=reader");
                done && done(new Error(
                    "vidgedb-log-event: the selected vidgedb-service role is 'reader' — log_event needs --role writer"
                ));
                return;
            }
            const client = cfg.ensureClient();
            bindState(client, cfg, node);

            const p = msg.payload || {};
            const name = p.name || p.event_name || (p.event && p.event.name);
            const entity = p.entity || p.entity_name;
            if (!name || !entity) {
                statusError(node, "no name/entity");
                done && done(new Error(
                    "vidgedb-log-event: msg.payload must be {name, entity, timestamp?, provenance?, details?}"
                ));
                return;
            }
            const params = {
                name,
                entity,
                timestamp: p.timestamp !== undefined ? p.timestamp : nowSecs(),
                provenance: p.provenance !== undefined ? p.provenance : 1,
                details: typeof p.details === "string" ? p.details : (p.details ? JSON.stringify(p.details) : ""),
            };

            try {
                const resp = await client.request("log_event", params);
                if (resp.error) {
                    statusError(node, resp.error.code === WRITE_FORBIDDEN ? "write forbidden" : `rpc ${resp.error.code || ""}`);
                    msg.error = { code: resp.error.code || 0, message: resp.error.message || "" };
                    send(msg);
                    if (done) done(new Error(rpcErrorMessage(resp)));
                    return;
                }
                const result = resp.result;
                if (isResultError(result)) {
                    statusError(node, result.error.slice(0, 24));
                    msg.logEventError = result.error;
                    msg.payload = { ok: false, error: result.error };
                    send(msg);
                    if (done) done(new Error(result.error));
                    return;
                }
                statusRunning(node);
                msg.payload = Object.assign({ ok: true }, result);
                send(msg);
                if (done) done();
            } catch (err) {
                statusError(node, err.message.slice(0, 24));
                done && done(err);
            }
        });
    }
    RED.nodes.registerType("vidgedb-log-event", VidgeLogEventNode);
};

// ---------------------------------------------------------------------------
// Test hooks (node:test imports this module's helpers without Node-RED)
// ---------------------------------------------------------------------------

/** Pure helpers exported for unit tests — no Node-RED runtime needed. */
module.exports.VidgeClient = VidgeClient;
module.exports.parseResponse = parseResponse;
module.exports.validateDbPath = validateDbPath;
module.exports.CONSTANTS = {
    PARSE_ERROR, INVALID_REQUEST, METHOD_NOT_FOUND, INVALID_PARAMS,
    STORAGE_ERROR, WRITE_FORBIDDEN, DEFAULT_TIMEOUT_MS, RESPAWN_DELAY_MS,
};
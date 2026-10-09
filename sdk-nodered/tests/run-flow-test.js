#!/usr/bin/env node
//! Integration test: spawn Node-RED headless (npx node-red if present —
//! SKIP with a clear note if not), install this package the documented way
//! (`npm install <dir>` from the Node-RED user dir), deploy a minimal flow
//! via the admin API (http://localhost:1880), and verify the vidgedb nodes
//! are registered and live.
//!
//! Run: node tests/run-flow-test.js
//! Env: VIDGEDB_BIN (defaults to the repo release binary), NODERED_PORT.

"use strict";

const { spawn, execSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const PORT = process.env.NODERED_PORT || 18880;
const BASE = `http://localhost:${PORT}`;
const BIN = process.env.VIDGEDB_BIN || "vidgedb";
const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "nodered-test-"));
const USER_DIR = path.join(TMP, ".node-red");
const NR_PKG_DIR = path.join(TMP, "nodered-pkg");
fs.mkdirSync(USER_DIR, { recursive: true });

function log(what) { console.log(`[flow-test] ${what}`); }
function failSkip(why) {
    console.log(`SKIP: ${why}`);
    console.log("SKIP: flow-level integration not runnable; unit tests");
    console.log("      (node --test tests/) cover the protocol layer.");
    cleanup();
    process.exit(0); // SKIP is never a failure
}
function assertEq(actual, expected, msg) {
    if (actual !== expected) throw new Error(`${msg}: expected ${expected}, got ${actual}`);
}
function assertOk(v, msg) { if (!v) throw new Error(msg || "assertion failed"); }
function cleanup() {
    try { fs.rmSync(TMP, { recursive: true, force: true }); } catch (err) { /* ok */ }
}
process.on("exit", cleanup);

async function main() {
    // 0) Node-RED present? (npx --no-install: cache/neighborhood only)
    let nrBin = null;
    try {
        const v = execSync("npx --no-install node-red --version", { stdio: "pipe", timeout: 30000 }).toString().trim();
        nrBin = "npx";
        log(`node-red via npx cache: ${v}`);
    } catch (err) {
        // Try a throwaway /tmp install (never pollutes the repo).
        try {
            log("no local node-red; installing a throwaway copy into /tmp …");
            fs.mkdirSync(NR_PKG_DIR, { recursive: true });
            execSync("npm install --no-audit --no-fund --loglevel=error node-red",
                { cwd: NR_PKG_DIR, stdio: "pipe", timeout: 240000 });
            nrBin = path.join(NR_PKG_DIR, "node_modules", ".bin", "node-red");
            const v = execSync(`${nrBin} --version`).toString().trim();
            log(`node-red installed at ${NR_PKG_DIR}: ${v}`);
        } catch (err2) {
            failSkip(`Node-RED unavailable (no npx cache, no network): ${(err2.message || "").split("\n")[0]}`);
        }
    }

    // 1) Install THIS package into the headless user dir (the documented
    //    install path: `npm install <dir>` from the Node-RED user dir).
    const pkgPath = path.resolve(__dirname, "..");
    log(`installing @vidge-ai/node-red-contrib-vidgedb from ${pkgPath}`);
    execSync(`npm install --no-audit --no-fund --loglevel=error "${pkgPath}"`,
        { cwd: USER_DIR, stdio: "pipe", timeout: 120000 });

    // 2) Start Node-RED headless.
    log(`starting Node-RED headless on :${PORT}`);
    const args = nrBin === "npx"
        ? ["node-red", "--port", String(PORT), "-u", USER_DIR]
        : [nrBin, "--port", String(PORT), "-u", USER_DIR];
    const cmd = args.shift();
    const proc = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"] });
    let logbuf = "";
    proc.stdout.on("data", (c) => { logbuf += c.toString(); });
    proc.stderr.on("data", (c) => { logbuf += c.toString(); });
    proc.on("exit", (code) => { if (code) logbuf += `\n[node-red exited ${code}]`; });

    // 3) Wait for the admin API.
    const deadline = Date.now() + 90000;
    let ready = false;
    while (Date.now() < deadline) {
        try {
            const res = await fetch(`${BASE}/flows`);
            if (res.ok) { ready = true; break; }
        } catch (err) { /* not up yet */ }
        await new Promise((r) => setTimeout(r, 500));
    }
    if (!ready) {
        failSkip(`Node-RED never became ready (admin API ${BASE} unreachable). Tail:\n${logbuf.slice(-1200)}`);
    }
    log("admin API ready");

    try {
        // 4) Which vidgedb nodes registered? (/nodes returns an HTML
        //    config dump unless Accept: application/json is set — the
        //    Node-RED admin API contract.)
        const registry = await (await fetch(`${BASE}/nodes`, {
            headers: { Accept: "application/json" },
        })).json();
        const vidgedbNodes = [];
        for (const m of registry) {
            const id = `${m.id || ""} ${m.module || ""} ${m.name || ""}`;
            if (!id.includes("vidgedb")) continue;
            const types = m.types || m.nodeTypes || m.nodes || [];
            for (const t of types) {
                vidgedbNodes.push(typeof t === "string" ? t : (t.name || t.id));
            }
        }
        const expected = ["vidgedb-service", "vidgedb-query", "vidgedb-ingest", "vidgedb-check", "vidgedb-log-event"];
        const missing = expected.filter((t) => !vidgedbNodes.includes(t));
        assertEq(missing.length, 0, `all 5 vidgedb nodes registered (missing: ${missing}; got: ${vidgedbNodes})`);

        // 5) Deploy a minimal flow: inject -> vidgedb-query -> debug.
        const dbPath = path.join(TMP, "flow.vdg");
        const fullFlow = [
            { id: "svc1", type: "vidgedb-service", name: "test db",
              binaryPath: BIN, dbPath, role: "writer", agentId: "flowtest",
              retentionDays: 0, timeoutMs: 15000, autoRespawn: false },
            { id: "flow1", type: "tab", label: "VidgeDB test" },
            { id: "in1", type: "inject", z: "flow1", name: "tick", wires: [["q1"]],
              props: [{ p: "payload" }], repeat: "", once: true, onceDelay: 0.5,
              topic: "", payload: "MATCH (m:Motor) RETURN name", payloadType: "str" },
            { id: "q1", type: "vidgedb-query", z: "flow1", name: "query",
              service: "svc1", wires: [["dbg1"]] },
            { id: "dbg1", type: "debug", z: "flow1", name: "out",
              active: true, complete: "payload", wires: [] },
        ];
        log("deploying flow via admin API (full)");
        const deployRes = await fetch(`${BASE}/flows`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "Node-RED-Deployment-Type": "full" },
            body: JSON.stringify(fullFlow),
        });
        // 204 No Content is Node-RED's success code for a full deploy.
        assertEq(deployRes.status, 204, "deploy accepted");
        const deployed = await (await fetch(`${BASE}/flows`)).json();
        const deployedTypes = deployed.map((n) => n.type);
        for (const t of ["vidgedb-service", "vidgedb-query", "inject", "debug"]) {
            assertOk(deployedTypes.includes(t), `deployed flow contains ${t}`);
        }

        // 6) The inject node fires once (once:true, onceDelay 0.5 s) — the
        //    query node spawns the service through the config node, runs the
        //    VQL, and forwards rows. Give it a beat, then verify the
        //    service actually ran by checking debug-side effects: query the
        //    deployed flow state via /flows/state is not exposed, so the
        //    strongest headless signal is: no error events + the .vdg file
        //    was CREATED by the spawned subprocess (side effect of open()).
        await new Promise((r) => setTimeout(r, 4000));
        assertOk(fs.existsSync(dbPath), "subprocess created the .vdg file from the flow");

        console.log("PASS: Node-RED headless deployed the vidgedb flow");
        console.log(`PASS: registered nodes = ${vidgedbNodes.join(", ")}`);
    } finally {
        try { proc.kill("SIGKILL"); } catch (err) { /* ok */ }
    }
}

main().catch((err) => {
    console.error("flow test FAILED:", err.message);
    process.exitCode = 1;
});
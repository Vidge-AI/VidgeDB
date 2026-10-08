/**
 * VidgeDB JSON-RPC service client — Phase 12b JS/TS SDK.
 *
 * Spawns `vidgedb --service <db.vdg>` and speaks line-delimited JSON-RPC 2.0
 * over the child's stdin/stdout (one request per line, one response per line —
 * the wire protocol implemented in src/service.rs).
 *
 * Concurrency: Node is single-threaded but ASYNC calls interleave; a Promise
 * queue serializes each write-line + read-response pair so two concurrent
 * calls can never interleave their requests/responses on the shared pipe.
 *
 * Error mapping (verified e2e against src/service.rs):
 *  - JSON-RPC `error` member → VidgeDBError with the real code
 *    (-32700 parse, -32600 invalid request, -32601 method not found,
 *     -32602 invalid params, -32000 storage, -32003 write-forbidden).
 *  - AgentApi v0 `{"error": "..."}` shape INSIDE `result` (method-level
 *    semantic problems, zero-panic contract) → VidgeDBError(code=0).
 */

import { spawn, ChildProcessByStdio } from "node:child_process";
import { createInterface, Interface as ReadlineInterface } from "node:readline";
import { EventEmitter } from "node:events";
import type { Writable, Readable } from "node:stream";
import * as path from "node:path";
import * as fs from "node:fs";
import { fileURLToPath } from "node:url";
import type { Schema } from "./types.js";
import type {
  QueryResult,
  EntityCard,
  MeasurementSeries,
  CheckResult,
  TraceResult,
  IngestResult,
  RelationSpec,
  UpsertResult,
  SetStateResult,
  LogEventResult,
  AuditEntry,
  RetainResult,
} from "./types.js";

/** Protocol-level JSON-RPC error codes (service.rs). */
export const PARSE_ERROR = -32700;
export const INVALID_REQUEST = -32600;
export const METHOD_NOT_FOUND = -32601;
export const INVALID_PARAMS = -32602;
export const STORAGE_ERROR = -32000;
/** Server-defined (Phase 11): a WRITE method on a Reader-role service. */
export const WRITE_FORBIDDEN = -32003;

const DEFAULT_TIMEOUT_MS = 30_000;
const CLOSE_TIMEOUT_MS = 5_000;

export type VidgeRole = "reader" | "writer" | "ingest";

/** A JSON-RPC error (transport) or AgentApi v0 `{"error": …}` semantic miss. */
export class VidgeDBError extends Error {
  /** JSON-RPC error code (e.g. -32601), or 0 for the AgentApi `{"error": …}`-in-result shape. */
  readonly code: number;
  /** Raw error payload (`data.error` for the semantic shape). */
  readonly data: any;

  constructor(code: number, message: string, data: any = null) {
    super(code ? `[${code}] ${message}` : message);
    this.name = "VidgeDBError";
    this.code = code;
    this.data = data;
  }
}

export interface VidgeDBOptions {
  /** Explicit path to the vidgedb binary. Defaults to $VIDGEDB_BIN, else "vidgedb" on PATH. */
  bin?: string;
  /** Agent identity recorded in the audit trail (spec §56). */
  agentId?: string;
  /** Role gate: "reader" (reads only) | "writer" (all) | "ingest" (writes incl. ingest_points). */
  role?: VidgeRole;
  /** Retention horizon in days. */
  retentionDays?: number;
  /** Per-call timeout in milliseconds (default 30000). */
  timeoutMs?: number;
}

type PendingEntry = {
  resolve: (value: any) => void;
  reject: (err: VidgeDBError) => void;
  timer: NodeJS.Timeout | null;
};

/**
 * A running `vidgedb --service <db.vdg>` subprocess speaking line-delimited
 * JSON-RPC 2.0, with a Promise queue for serialized round-trips.
 */
export class VidgeDB {
  readonly dbPath: string;
  readonly bin: string;
  readonly agentId?: string;
  readonly role?: VidgeRole;
  readonly timeoutMs: number;

  private readonly proc: ChildProcessByStdio<Writable, Readable, Readable | null>;
  private readonly readline: ReadlineInterface;
  private readonly pending = new Map<number, PendingEntry>();
  private readonly queue: (() => void)[] = [];
  private readonly emitter = new EventEmitter();
  private nextId = 1;
  private closed = false;
  private queueRunning = false;

  private constructor(
    dbPath: string,
    bin: string,
    agentId: string | undefined,
    role: VidgeRole | undefined,
    retentionDays: number | undefined,
    timeoutMs: number,
  ) {
    this.dbPath = dbPath;
    this.bin = bin;
    this.agentId = agentId;
    this.role = role;
    this.timeoutMs = timeoutMs;

    const args: string[] = ["--service", dbPath];
    if (agentId !== undefined) args.push("--agent-id", agentId);
    if (retentionDays !== undefined) args.push("--retention-days", String(retentionDays));
    if (role !== undefined) args.push("--role", role);

    this.proc = spawn(bin, args, {
      stdio: ["pipe", "pipe", "inherit"], // service logs go to stderr, not stdout
    }) as ChildProcessByStdio<Writable, Readable, Readable | null>;

    this.readline = createInterface({ input: this.proc.stdout });
    this.readline.on("line", (line: string) => this.handleLine(line));

    this.proc.on("error", (err) => this.failAll(new VidgeDBError(0, `vidgedb spawn failed: ${err.message}`)));
    this.proc.on("exit", (code, signal) => {
      this.failAll(
        new VidgeDBError(0, `vidgedb process exited (code=${code ?? "null"}, signal=${signal ?? "null"})`),
      );
      this.emitter.emit("exit", code, signal);
    });
  }

  /**
   * Spawn the service and return a connected client.
   *
   * Binary resolution order (exact parity with the Python SDK):
   * explicit `opts.bin` → `$VIDGEDB_BIN` → `"vidgedb"` on PATH.
   */
  static open(
    dbPath: string,
    opts: VidgeDBOptions = {},
  ): VidgeDB {
    const bin = opts.bin ?? process.env.VIDGEDB_BIN ?? "vidgedb";
    if (opts.role !== undefined && !["reader", "writer", "ingest"].includes(opts.role)) {
      throw new VidgeDBError(0, `unknown role ${JSON.stringify(opts.role)} (reader|writer|ingest)`);
    }
    // Fail fast, like the Python SDK's FileNotFoundError at construction.
    if (!fs.existsSync(bin)) {
      throw new VidgeDBError(0, `vidgedb binary not found: ${bin} (set VIDGEDB_BIN or pass bin=)`);
    }
    return new VidgeDB(
      path.resolve(dbPath),
      bin,
      opts.agentId,
      opts.role,
      opts.retentionDays,
      opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    );
  }

  // -------------------------------------------------------------------------
  // Concurrency queue — serialize full request+response round-trips
  // -------------------------------------------------------------------------

  /**
   * Node is single-threaded, but async calls interleave; without sequencing,
   * two concurrent `call()`s would write both requests then read both
   * responses — fine per protocol but fragile against timeouts and close.
   * A tiny promise queue gives each call an exclusive pipe session.
   */
  private enqueue<T>(job: () => Promise<T>): Promise<T> {
    if (this.closed) {
      return Promise.reject(new VidgeDBError(0, "client is closed"));
    }
    return new Promise<T>((resolve, reject) => {
      this.queue.push(() => {
        job().then(resolve, reject).finally(() => {
          const next = this.queue.shift();
          if (next) next();
          else this.queueRunning = false;
        });
      });
      if (!this.queueRunning) {
        this.queueRunning = true;
        const first = this.queue.shift();
        if (first) first();
      }
    });
  }

  // -------------------------------------------------------------------------
  // Core protocol plumbing
  // -------------------------------------------------------------------------

  private handleLine(line: string): void {
    const trimmed = line.trim();
    if (!trimmed) return;
    let resp: any;
    try {
      resp = JSON.parse(trimmed);
    } catch {
      // A malformed line must not wedge the client; pending calls will
      // hit their timeout. Never throw from the read path.
      return;
    }
    const id = typeof resp?.id === "number" ? resp.id : null;
    const entry = id !== null ? this.pending.get(id) : undefined;
    if (!entry) return; // bye / notification note / unknown id — ignore
    this.pending.delete(id!);
    if (entry.timer) clearTimeout(entry.timer);
    if (resp.error && typeof resp.error === "object") {
      entry.reject(new VidgeDBError(resp.error.code ?? 0, resp.error.message ?? "", resp.error));
      return;
    }
    const result = resp.result;
    // AgentApi v0 semantic-error shape: `{"error": "..."}` INSIDE result.
    if (
      result !== null &&
      typeof result === "object" &&
      !Array.isArray(result) &&
      Object.keys(result).length === 1 &&
      typeof (result as any).error === "string"
    ) {
      entry.reject(new VidgeDBError(0, (result as any).error, result));
      return;
    }
    entry.resolve(result);
  }

  private failAll(err: VidgeDBError): void {
    for (const entry of [...this.pending.values()]) {
      if (entry.timer) clearTimeout(entry.timer);
      entry.reject(err);
    }
    this.pending.clear();
  }

  /**
   * Send one JSON-RPC request, await its response, return the `result`.
   *
   * Generic dispatcher: ANY method the binary understands works here —
   * including methods added by newer vidgedb binaries that this SDK does not
   * mirror yet (dynamic dispatch — no SDK release needed for new methods).
   */
  async call(method: string, params: Record<string, any> = {}): Promise<any> {
    return this.enqueue(async () => this.roundTrip(method, params));
  }

  private roundTrip(method: string, params: Record<string, any>): Promise<any> {
    if (this.closed) {
      return Promise.reject(new VidgeDBError(0, "client is closed"));
    }
    const id = this.nextId++;
    const req: Record<string, any> = { jsonrpc: "2.0", id, method };
    if (params && Object.keys(params).length > 0) req.params = params;

    return new Promise<any>((resolve, reject) => {
      const timer =
        this.timeoutMs > 0
          ? setTimeout(() => {
              this.pending.delete(id);
              reject(new VidgeDBError(0, `timeout after ${this.timeoutMs}ms waiting for "${method}"`));
            }, this.timeoutMs)
          : null;
      if (timer) timer.unref?.();

      this.pending.set(id, { resolve, reject, timer });

      try {
        const line = JSON.stringify(req) + "\n";
        this.proc.stdin.write(line);
        // stdin is a Socket; flush() is a legacy alias for the write callback path.
        if (typeof (this.proc.stdin as any).flush === "function") {
          (this.proc.stdin as any).flush();
        }
      } catch (err: any) {
        this.pending.delete(id);
        if (timer) clearTimeout(timer);
        const e = new VidgeDBError(0, `vidgedb process is not running: ${err?.message ?? String(err)}`);
        try {
          this.proc.stdin.destroy();
        } catch {
          /* already dead */
        }
        reject(e);
      }
    });
  }

  // -------------------------------------------------------------------------
  // Method mirrors (typed conveniences over the documented AgentApi surface)
  // -------------------------------------------------------------------------

  /** Entity types, relation topologies, series list, provenance classes. */
  async schema(): Promise<Schema> {
    return this.call("schema");
  }

  /** Core VidgeQL (MATCH/WHERE/RETURN/LIMIT, AT time-travel). */
  async query(vql: string): Promise<QueryResult> {
    return this.call("query", { vql });
  }

  /** Temporal VidgeQL — MEASURE … DURING last(n)/t1..t2, RETURN aggregates. */
  async queryTemporal(vql: string, now?: number): Promise<QueryResult> {
    const params: Record<string, any> = { vql };
    if (now !== undefined) params.now = now;
    return this.call("query_temporal", params);
  }

  /** Full entity card by stable key (name, type, props, relations). */
  async getEntity(key: number | string): Promise<EntityCard> {
    return this.call("get_entity", { key });
  }

  /** Raw points of `<entity>.<signal>` in [from, to]; typed series. */
  async getMeasurements(
    entity: string,
    signal: string,
    from: number = 0,
    to: number = 2315841784746323908, // i64::MAX / 2, the service default
  ): Promise<MeasurementSeries> {
    return this.call("get_measurements", { entity, signal, from, to });
  }

  /** Spec §25 deviation check; typed CheckResult (OK/VIOLATION/NO_DATA/NO_SPEC). */
  async check(
    entity: string,
    signal: string,
    from: number = 0,
    to: number = 2315841784746323908,
  ): Promise<CheckResult> {
    const raw = await this.call("check", { entity, signal, from, to });
    return { ...raw, isViolation: raw.status === "VIOLATION" };
  }

  /** BFS path between two entities by name, out-edges only. */
  async trace(fromName: string, toName: string, maxHops: number = 6): Promise<TraceResult> {
    return this.call("trace", { from: fromName, to: toName, max_hops: maxHops });
  }

  /** The provenance inventory (unmirrored by the Python SDK; here for parity). */
  async provenance(): Promise<any> {
    return this.call("provenance");
  }

  /** Current (or at-time) value of a state key on an entity: `{value, valid_from}` or null. */
  async getState(entity: string, key: string, at?: number): Promise<any> {
    const params: Record<string, any> = { entity, key };
    if (at !== undefined) params.at = at;
    return this.call("get_state", params);
  }

  /** Full closed-window history of a state key: `{history: StateEntry[], n}`. */
  async stateHistory(entity: string, key: string): Promise<any> {
    return this.call("state_history", { entity, key });
  }

  /** Ingest a batch of `[timestamp, value]` telemetry points (Writer/Ingest role). */
  async ingestPoints(
    entity: string,
    signal: string,
    points: Array<[number, number] | { ts: number; value: number } | { t: number; value: number }>,
  ): Promise<IngestResult> {
    const normalized = points.map((p) =>
      Array.isArray(p) ? p : [(p as any).ts ?? (p as any).t, (p as any).value],
    );
    return this.call("ingest_points", { entity, signal, points: normalized });
  }

  /** Create-or-update an entity (merge props, append relations). Returns `{key, created, relations_added}`. */
  async upsertEntity(
    name: string,
    type: string,
    props: Record<string, string | null> = {},
    relations: RelationSpec[] = [],
    source: string = "plc",
  ): Promise<UpsertResult> {
    return this.call("upsert_entity", { name, type, props, relations, source });
  }

  /** Write one state key (spec §10); history is kept (closes prior open window). */
  async setState(entity: string, key: string, value: string, at?: number): Promise<SetStateResult> {
    const params: Record<string, any> = { entity, key, value };
    if (at !== undefined) params.at = at;
    return this.call("set_state", params);
  }

  /** Append one event (the only append-write beside ingest/upsert/set_state). */
  async logEvent(
    name: string,
    entity: string,
    timestamp?: number,
    provenance: number = 1,
    details: string = "",
  ): Promise<LogEventResult> {
    const params: Record<string, any> = {
      name,
      entity,
      timestamp: timestamp ?? Math.floor(Date.now() / 1000),
      provenance,
      details,
    };
    return this.call("log_event", params);
  }

  /** Events in the inclusive `[from, to]` window, optionally per entity: `{events, n}`. */
  async getEvents(
    entity?: string,
    from: number = -4611686018427387904, // i64::MIN / 2, the service default
    to: number = 4611686018427387903, // i64::MAX / 2
  ): Promise<any> {
    const params: Record<string, any> = { from, to };
    if (entity !== undefined) params.entity = entity;
    return this.call("get_events", params);
  }

  /** Audit trail (spec §56) accumulated in this service session: `{entries, n}`. */
  async audit(): Promise<{ entries: AuditEntry[]; n: number }> {
    return this.call("audit");
  }

  /** Drop WHOLE time-series chunks older than `before` (unix secs; Writer/Ingest only). */
  async retain(before?: number): Promise<RetainResult> {
    const params: Record<string, any> = {};
    if (before !== undefined) params.before = before;
    return this.call("retain", params);
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /** OS pid of the spawned vidgedb subprocess (0 after exit). */
  get pid(): number {
    return this.proc.pid ?? 0;
  }

  /** True while the subprocess has not exited and close() has not run. */
  isRunning(): boolean {
    return !this.closed && this.proc.exitCode === null && this.proc.signalCode === null;
  }

  /** Awaitable when the child exits (used by tests / dispose). */
  waitExit(ms = CLOSE_TIMEOUT_MS): Promise<number | null> {
    if (this.proc.exitCode !== null || this.proc.signalCode !== null) {
      return Promise.resolve(this.proc.exitCode);
    }
    return new Promise((resolve) => {
      const t = setTimeout(() => resolve(null), ms);
      t.unref?.();
      this.emitter.once("exit", (code: number | null) => {
        clearTimeout(t);
        resolve(code);
      });
    });
  }

  /**
   * Close stdin, wait up to `timeoutMs` for the clean EOF exit, SIGKILL past it.
   * Idempotent (safe to call twice); the zero-panic service exits 0 on EOF.
   */
  async close(timeoutMs: number = CLOSE_TIMEOUT_MS): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.failAll(new VidgeDBError(0, "client is closed"));
    try {
      this.proc.stdin.end();
    } catch {
      /* already dead */
    }
    const exited = await this.waitExit(timeoutMs);
    if (exited === null && this.proc.exitCode === null && this.proc.signalCode === null) {
      try {
        this.proc.kill("SIGKILL");
      } catch {
        /* race — already gone */
      }
    }
    this.readline.close();
  }

  /** `using`/`Symbol.asyncDispose` support (Node 20+ with --harmony, or await using). */
  async dispose(): Promise<void> {
    await this.close();
  }

  /** Async context-manager sugar: `await using db = await VidgeDB.openAsync(...)` style. */
  static async with<T>(dbPath: string, opts: VidgeDBOptions, fn: (db: VidgeDB) => Promise<T>): Promise<T> {
    const db = VidgeDB.open(dbPath, opts);
    try {
      return await fn(db);
    } finally {
      await db.close();
    }
  }
}

// ---------------------------------------------------------------------------
// Re-exports (index parity with the Python SDK)
// ---------------------------------------------------------------------------

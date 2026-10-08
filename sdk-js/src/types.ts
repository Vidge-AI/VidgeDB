/**
 * VidgeDB SDK — typed result models for the AgentApi v0 payloads.
 *
 * Hand-written interfaces mapped onto the REAL JSON the service returns
 * (src/tools.rs `AgentApi`, verified e2e). Deliberately tolerant: optional
 * fields stay optional so a newer vidgedb binary adding fields never breaks
 * the SDK. Run `npm test` — the schema/runtime coherence test asserts the
 * shapes live against the real binary.
 */

/** One time-series point: integer unix-seconds `t`, float `value`. */
export interface Measurement {
  t: number;
  value: number;
}

/** `get_measurements` payload: points + aggregate min/max/count. */
export interface MeasurementSeries {
  points: Measurement[];
  count: number;
  min: number | null;
  max: number | null;
}

/** `query` / `query_temporal` payload: VidgeQL rows. */
export interface QueryResult {
  rows: Record<string, any>[];
  n: number;
}

/** Aggregated cell inside a `query_temporal` row. */
export interface AggCell {
  kind: string;
  value: number;
}

/** Statuses of the spec §25 deviation engine (check.rs). */
export type CheckStatus = "OK" | "VIOLATION" | "NO_DATA" | "NO_SPEC";

/** `check` payload (spec §25, with the §27 example fields). */
export interface CheckResult {
  entity: string;
  signal: string;
  status: CheckStatus;
  expected_max: number | null;
  observed: number | null;
  deviation: number | null;
  unit: string | null;
  expected_provenance: string | null;
  observed_provenance: string | null;
  points_checked: number;
  window: { from: number; to: number };
}

/** One hop of a `trace` BFS path. */
export interface TraceStep {
  from: string;
  relation_type: string;
  topology: string;
  to: string;
  provenance: string;
  relation_idx: number;
}

/** `trace` payload: out-edge-only BFS path between two entity names. */
export interface TraceResult {
  found: boolean;
  steps: TraceStep[];
  path: { name: string; type: string }[];
  n_hops?: number;
}

/** `get_entity` payload: the full entity card. */
export interface EntityCard {
  key: number;
  name: string;
  type: string;
  properties: Record<string, string>;
  relations_out?: any[];
  relations_in?: any[];
}

/** `schema` payload: the service inventory. */
export interface Schema {
  entity_types: string[];
  provenance_classes: string[];
  relation_topologies: string[];
  series: string[];
}

/** `ingest_points` payload. */
export interface IngestResult {
  accepted: number;
  chunks_flushed: number;
  series_id: number;
}

/** `upsert_entity` payload: `{key, created, relations_added}`. */
export interface UpsertResult {
  key: number;
  created: boolean;
  relations_added: number;
}

/** `set_state` payload. */
export interface SetStateResult {
  set: boolean;
  entity: string;
  key: string;
}

/** One closed-window entry of a `state_history` payload (`valid_to === -1` = open). */
export interface StateEntry {
  value: string;
  valid_from: number;
  valid_to: number;
}

/** `log_event` payload. */
export interface LogEventResult {
  logged: boolean;
  event: string;
  entity: string;
}

/** One event of a `get_events` payload. */
export interface EventEntry {
  name: string;
  entity: string;
  timestamp: number;
  provenance: string;
  provenance_byte: number;
  details: string;
}

/** One audit-trail entry of an `audit` payload. */
export interface AuditEntry {
  timestamp: number;
  agent_id: string | null;
  method: string;
  params_summary: string;
}

/** `retain` payload. */
export interface RetainResult {
  points_removed: number;
  chunks_removed: number;
}

/** Tolerant view over a DIAGNOSE-shaped report (spec §26; via query_temporal). */
export interface DiagnosticReport {
  source?: string;
  anomalies?: Record<string, any>[];
  hypotheses?: Record<string, any>[];
  [k: string]: any;
}

/** Relation to create with an entity (upsertEntity). `relation_type` MUST be 'topology:type'. */
export interface RelationSpec {
  to: string;
  relation_type: string;
  valid_from?: number | null;
}
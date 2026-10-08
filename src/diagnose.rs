//! DIAGNOSE — high-level diagnostic query (Phase 7.1, spec §26).
//!
//! `DIAGNOSE "Motor42" DURING last(30m)` (spec §26) is executed as a
//! READ-ONLY 8-step pipeline over the existing stores:
//!
//! 1. **identify component** — resolve `entity_name` to (key, type, props)
//!    by scanning the entity slab for a matching name (Phase 2 has no name
//!    index; entity counts are small, correctness first). Unknown names
//!    yield `component: None` with every other section empty — a structured
//!    "not found" answer, not an engine error (AI-native design, spec §27).
//! 2. **identify dependencies** — direct (1-hop) neighbors from the derived
//!    adjacency index: `in_` edges = upstream (amont: what feeds the
//!    component), `out` edges = downstream (aval: what the component
//!    drives). Names/types are resolved from the string arena; each entry
//!    keeps the provenance byte of its relation record.
//! 3. **inspect topology** — the distinct topology classes of the relations
//!    touching the component (electrical, mechanical, network, ...), with
//!    the relation types seen on each.
//! 4. **inspect events** — v0 LIMITATION: there is NO event store yet
//!    (discrete occurrences, spec §14 `Provenance::Event`, are planned for
//!    a later phase). Step 4 returns an empty list plus an explicit note;
//!    nothing is invented to fill the gap.
//! 5. **inspect measurements** — every series named `<entity_name>.<...>`
//!    (same naming convention as `temporal.rs`/`check.rs`): signal, point
//!    count on the window, min/max via `TimeSeriesStore::query`.
//! 6. **compare specifications** — for each measured signal that has a
//!    `spec.<signal>.max` property, delegate to `check.rs::check_entity`
//!    (statuses OK / VIOLATION / NO_DATA / NO_SPEC, deviation = observed -
//!    expected, semantics unchanged). Measured signals WITHOUT a spec prop
//!    produce no check row (the NO_SPEC case is check.rs's business, not a
//!    diagnostic anomaly).
//! 7. **identify anomalies** — the checks whose status is VIOLATION.
//! 8. **construct candidate causal paths** — v0: each anomaly yields ONE
//!    structured textual HYPOTHESIS (`confidence: 0.5`,
//!    `source: vidgedb_diagnose_v0`, upstream dependency names attached).
//!    These hypotheses are OUTPUTS OF THE REPORT ONLY: `diagnose` performs
//!    no write whatsoever (no entity/relation/series creation, no property
//!    update), so a hypothesis is never persisted and never promoted to
//!    Fact or Observation — the spec §29 no-silent-promotion rule holds by
//!    construction (the only code that may store a Hypothesis is an
//!    explicit external write, outside this module).
//!
//! Provenance (spec §26: the result SHALL preserve provenance): every
//! section carries its provenance class (`observed`, `specification`,
//! `hypothesis`); dependency entries additionally carry the per-record
//! provenance of the underlying relation (Fact/Observation/...), and check
//! summaries keep the expected=Specification / observed=Observation classes
//! attached by `check.rs`.

use crate::check::{check_entity, spec_prop_key, CheckResult, CheckStatus};
use crate::engine::{Engine, EngineError};
use crate::model::Provenance;
use crate::stores::GraphStore;
use crate::temporal::Window;
use crate::timeseries::TimeSeriesStore;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Report model (provenance-annotated, spec §26)
// ---------------------------------------------------------------------------

/// Section-level provenance class (spec §26: "SHALL preserve provenance").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProvClass {
    /// Read from the stores as-is (graph topology, telemetry).
    Observed,
    /// Design intent (spec properties, datasheet values).
    Specification,
    /// Diagnostic guess produced by this module (never persisted, spec §29).
    Hypothesis,
}

/// One inline entity property (`k = v`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Prop {
    pub name: String,
    pub value: String,
}

/// Step 1 — the diagnosed component (provenance: observed).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ComponentInfo {
    pub key: u32,
    pub name: String,
    pub type_name: String,
    pub props: Vec<Prop>,
    pub provenance: ProvClass,
}

/// One direct dependency, resolved by name (provenance: observed graph +
/// the relation record's own provenance byte).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Dependency {
    pub key: u32,
    pub name: String,
    pub type_name: String,
    /// Relation type (e.g. `feeds`), from the record's type_sid.
    pub relation_type: String,
    /// Topology class of the relation (e.g. `electrical`).
    pub topology: String,
    /// Provenance of the RELATION record (Fact/Observation/...).
    pub relation_provenance: String,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// Step 2 — direct neighbors, split by direction. Upstream = relations_in
/// (what feeds the component), downstream = relations_out.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct Dependencies {
    pub upstream: Vec<Dependency>,
    pub downstream: Vec<Dependency>,
}

/// Step 3 — one topology class touched by the component's relations.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TopologyInfo {
    pub topology: String,
    pub relation_types: Vec<String>,
    pub edge_count: usize,
    pub provenance: ProvClass,
}

/// Step 5 — measurement summary for one series over the window
/// (provenance: observed).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MeasurementSummary {
    /// Full series name (`<entity_name>.<signal>`).
    pub series: String,
    /// Everything after the entity name's dot.
    pub signal: String,
    pub points: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub provenance: ProvClass,
}

/// Step 6 — one spec comparison, flattened from `check::CheckResult` so the
/// whole report is `Serialize` (provenance kept per side).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CheckSummary {
    pub signal: String,
    /// `OK` / `VIOLATION` / `NO_DATA` / `NO_SPEC` (spec §25 statuses).
    pub status: String,
    pub expected_max: Option<f64>,
    /// Always `Specification` when present (spec prop).
    pub expected_prov: Option<String>,
    pub unit: Option<String>,
    /// Worst-case (max) observation over the window.
    pub observed: Option<f64>,
    /// Always `Observation` when present (telemetry).
    pub observed_prov: Option<String>,
    /// `observed - expected_max`; positive = overshoot.
    pub deviation: Option<f64>,
    pub points_checked: usize,
}

/// Step 7 — one VIOLATION (provenance: observed, i.e. an observation
/// compared against a specification).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Anomaly {
    pub signal: String,
    pub observed: Option<f64>,
    pub expected_max: Option<f64>,
    pub deviation: Option<f64>,
    pub points_checked: usize,
    pub provenance: ProvClass,
}

/// Step 8 — candidate causal path, v0: a structured textual HYPOTHESIS.
/// Spec §29: this stays a report output — never written to the database,
/// never promoted to Fact/Observation by the engine.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CausalHypothesis {
    pub signal: String,
    pub hypothesis: String,
    pub confidence: f64,
    /// Always `Hypothesis` (spec §29 provenance class).
    pub provenance: String,
    /// Always `vidgedb_diagnose_v0` in this version.
    pub source: String,
    /// Names of the upstream (amont) dependencies of the component.
    pub upstream: Vec<String>,
}

/// Absolute window, restated for traceability in the report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WindowSpan {
    pub from: i64,
    pub to: i64,
}

/// The full DIAGNOSE report (spec §26, 8 steps, provenance preserved).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DiagnosticReport {
    /// Name that was requested (echoed for traceability).
    pub entity: String,
    pub window: WindowSpan,
    /// Step 1 — None when the name resolves to no entity.
    pub component: Option<ComponentInfo>,
    /// Step 2.
    pub dependencies: Dependencies,
    /// Step 3.
    pub topology: Vec<TopologyInfo>,
    /// Step 4 — always empty in v0 (no event store yet).
    pub events: Vec<serde_json::Value>,
    /// Step 4 note documenting the limitation.
    pub events_note: String,
    /// Step 5.
    pub measurements: Vec<MeasurementSummary>,
    /// Step 6.
    pub checks: Vec<CheckSummary>,
    /// Step 7.
    pub anomalies: Vec<Anomaly>,
    /// Step 8 — hypotheses ONLY (never persisted, spec §29).
    pub causal_paths: Vec<CausalHypothesis>,
    /// Provenance policy of this report, spelled out for the consumer.
    pub provenance_note: String,
}

// ---------------------------------------------------------------------------
// Provenance helpers
// ---------------------------------------------------------------------------

/// Name of a relation-record provenance byte (discriminants of
/// `model::Provenance`: Fact=0 ... Configuration=7).
pub fn prov_byte_name(p: u8) -> &'static str {
    match p {
        0 => "Fact",
        1 => "Observation",
        2 => "Specification",
        3 => "Inference",
        4 => "Hypothesis",
        5 => "Event",
        6 => "Command",
        7 => "Configuration",
        _ => "Unknown",
    }
}

/// Name of a `model::Provenance` enum value.
fn prov_enum_name(p: Provenance) -> &'static str {
    match p {
        Provenance::Fact => "Fact",
        Provenance::Observation => "Observation",
        Provenance::Specification => "Specification",
        Provenance::Inference => "Inference",
        Provenance::Hypothesis => "Hypothesis",
        Provenance::Event => "Event",
        Provenance::Command => "Command",
        Provenance::Configuration => "Configuration",
    }
}

// ---------------------------------------------------------------------------
// Core pipeline
// ---------------------------------------------------------------------------

/// Run the 8 diagnostic steps of spec §26 for one entity over
/// `[window_from, window_to]`. Strictly read-only: no store is mutated.
pub fn diagnose(
    eng: &mut Engine,
    gs: &mut GraphStore,
    ts: &mut TimeSeriesStore,
    entity_name: &str,
    window_from: i64,
    window_to: i64,
) -> Result<DiagnosticReport, EngineError> {
    // -- Step 1: identify component --------------------------------------
    let mut component = None;
    for key in 0..gs.entity_count() {
        let name_sid = gs.entity_name_sid(eng, key)?;
        let name = gs.get_str(eng, name_sid)?;
        if name == entity_name {
            let type_sid = gs.entity_type_sid(eng, key)?;
            let type_name = gs.get_str(eng, type_sid)?;
            let props = gs
                .entity_props(eng, key)?
                .into_iter()
                .map(|(name, value)| Prop { name, value })
                .collect();
            component = Some(ComponentInfo {
                key,
                name,
                type_name,
                props,
                provenance: ProvClass::Observed,
            });
            break;
        }
    }

    let mut deps = Dependencies::default();
    let mut topology = Vec::new();
    let mut measurements = Vec::new();
    let mut checks = Vec::new();
    let mut anomalies = Vec::new();
    let mut causal_paths = Vec::new();

    if let Some(ref comp) = component {
        let key = comp.key;

        // -- Step 2: identify dependencies (amont = in, aval = out) ------
        let in_edges = gs.adjacency.in_.get(&key).cloned().unwrap_or_default();
        let out_edges = gs.adjacency.out.get(&key).cloned().unwrap_or_default();
        deps.upstream = resolve_dependencies(eng, gs, &in_edges)?;
        deps.downstream = resolve_dependencies(eng, gs, &out_edges)?;

        // -- Step 3: inspect topology ------------------------------------
        topology = inspect_topology(eng, &deps)?;

        // -- Step 4: inspect events (v0: no event store) ------------------
        // (empty list + note; filled in the report below)

        // -- Step 5: inspect measurements --------------------------------
        let prefix = format!("{}.", comp.name);
        // Names cloned first (phase 8.7: ts.query takes &mut self; the
        // series table itself is not needed for the loop).
        let names: Vec<String> = ts.series.iter().map(|s| s.name.clone()).collect();
        for (idx, name) in names.iter().enumerate() {
            if !name.starts_with(&prefix) {
                continue;
            }
            let signal = name[prefix.len()..].to_string();
            let pts = ts.query(eng, idx as u32, window_from, window_to)?;
            let min = pts
                .iter()
                .map(|&(_, v)| v)
                .fold(None, |acc: Option<f64>, v| {
                    Some(acc.map_or(v, |m| m.min(v)))
                });
            let max = pts
                .iter()
                .map(|&(_, v)| v)
                .fold(None, |acc: Option<f64>, v| {
                    Some(acc.map_or(v, |m| m.max(v)))
                });
            measurements.push(MeasurementSummary {
                series: name.clone(),
                signal,
                points: pts.len(),
                min,
                max,
                provenance: ProvClass::Observed,
            });
        }

        // -- Step 6: compare specifications ------------------------------
        for m in &measurements {
            let has_spec = comp
                .props
                .iter()
                .any(|p| p.name == spec_prop_key(&m.signal));
            if !has_spec {
                continue;
            }
            let res = check_entity(
                eng,
                gs,
                ts,
                key,
                &m.signal,
                Window {
                    from: window_from,
                    to: window_to,
                },
            )?;
            checks.push(CheckSummary::from_result(&res));

            // -- Step 7: identify anomalies ------------------------------
            if res.status == CheckStatus::Violation {
                anomalies.push(Anomaly {
                    signal: res.signal.clone(),
                    observed: res.observed,
                    expected_max: res.expected_max,
                    deviation: res.deviation,
                    points_checked: res.points_checked,
                    provenance: ProvClass::Observed,
                });

                // -- Step 8: construct candidate causal paths ------------
                // v0: one textual hypothesis per anomaly. OUTPUT ONLY —
                // nothing is written to any store here (spec §29).
                causal_paths.push(CausalHypothesis {
                    signal: res.signal.clone(),
                    hypothesis: format!(
                        "signal {} exceeded expected max over window [{}, {}]",
                        res.signal, window_from, window_to
                    ),
                    confidence: 0.5,
                    provenance: "Hypothesis".to_string(),
                    source: "vidgedb_diagnose_v0".to_string(),
                    upstream: deps.upstream.iter().map(|d| d.name.clone()).collect(),
                });
            }
        }
    }

    Ok(DiagnosticReport {
        entity: entity_name.to_string(),
        window: WindowSpan {
            from: window_from,
            to: window_to,
        },
        component,
        dependencies: deps,
        topology,
        events: Vec::new(),
        events_note: NO_EVENT_STORE_NOTE.to_string(),
        measurements,
        checks,
        anomalies,
        causal_paths,
        provenance_note: PROVENANCE_NOTE.to_string(),
    })
}

/// Step 4 note: documents the v0 limitation instead of inventing events.
pub const NO_EVENT_STORE_NOTE: &str =
    "v0 limitation: no event store exists yet (discrete occurrences, spec §14 \
     Provenance::Event, are planned for a later phase); step 4 returns an \
     empty list rather than inventing data";

/// Report-level provenance policy (spec §26/§29).
pub const PROVENANCE_NOTE: &str =
    "provenance preserved: component/dependencies/topology/measurements are \
     observed (relation records keep their own Fact/Observation/... class), \
     expected values are specification, comparisons are observed-vs-\
     specification; causal paths are hypotheses only — produced by \
     vidgedb_diagnose_v0, never written to the database and never promoted \
     to fact (spec §29)";

/// Resolve adjacency edges (topo_sid, other, rel_idx) into named
/// dependencies. Upstream callers pass `in_` edges, downstream `out` edges.
fn resolve_dependencies(
    eng: &mut Engine,
    gs: &mut GraphStore,
    edges: &[(u32, u32, u32)],
) -> Result<Vec<Dependency>, EngineError> {
    let mut out = Vec::new();
    for &(topo_sid, other, rel_idx) in edges {
        let name_sid = gs.entity_name_sid(eng, other)?;
        let name = gs.get_str(eng, name_sid)?;
        let type_sid = gs.entity_type_sid(eng, other)?;
        let type_name = gs.get_str(eng, type_sid)?;
        let topology = gs.get_str(eng, crate::stores::StrId(topo_sid))?;
        let rec = gs.relations.get(rel_idx as usize).ok_or(EngineError::Page(
            crate::pager::PageError::PageOutOfBounds(rel_idx),
        ))?;
        let relation_type = gs.get_str(eng, crate::stores::StrId(rec.type_sid))?;
        out.push(Dependency {
            key: other,
            name,
            type_name,
            relation_type,
            topology,
            relation_provenance: prov_byte_name(rec.provenance).to_string(),
            valid_from: rec.valid_from,
            valid_to: rec.valid_to,
        });
    }
    Ok(out)
}

/// Group the resolved dependencies by topology class (step 3), preserving
/// first-seen order.
fn inspect_topology(
    eng: &mut Engine,
    deps: &Dependencies,
) -> Result<Vec<TopologyInfo>, EngineError> {
    let _ = eng; // reserved for future per-topology lookups
    let mut order: Vec<String> = Vec::new();
    let mut types: Vec<Vec<String>> = Vec::new();
    let mut counts: Vec<usize> = Vec::new();
    for d in deps.upstream.iter().chain(deps.downstream.iter()) {
        if let Some(i) = order.iter().position(|t| *t == d.topology) {
            counts[i] += 1;
            if !types[i].contains(&d.relation_type) {
                types[i].push(d.relation_type.clone());
            }
        } else {
            order.push(d.topology.clone());
            types.push(vec![d.relation_type.clone()]);
            counts.push(1);
        }
    }
    Ok(order
        .into_iter()
        .zip(types.into_iter().zip(counts.into_iter()))
        .map(|(topology, (relation_types, edge_count))| TopologyInfo {
            topology,
            relation_types,
            edge_count,
            provenance: ProvClass::Observed,
        })
        .collect())
}

impl CheckSummary {
    /// Flatten a `check::CheckResult` (status/deviation + provenance kept).
    fn from_result(r: &CheckResult) -> CheckSummary {
        CheckSummary {
            signal: r.signal.clone(),
            status: r.status.as_str().to_string(),
            expected_max: r.expected_max,
            expected_prov: r.expected_prov.map(prov_enum_name).map(String::from),
            unit: r.unit.clone(),
            observed: r.observed,
            observed_prov: r.observed_prov.map(prov_enum_name).map(String::from),
            deviation: r.deviation,
            points_checked: r.points_checked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prov_byte_names_cover_all_classes() {
        assert_eq!(prov_byte_name(0), "Fact");
        assert_eq!(prov_byte_name(1), "Observation");
        assert_eq!(prov_byte_name(2), "Specification");
        assert_eq!(prov_byte_name(3), "Inference");
        assert_eq!(prov_byte_name(4), "Hypothesis");
        assert_eq!(prov_byte_name(5), "Event");
        assert_eq!(prov_byte_name(6), "Command");
        assert_eq!(prov_byte_name(7), "Configuration");
        assert_eq!(prov_byte_name(200), "Unknown");
    }

    #[test]
    fn prov_class_serializes_lowercase() {
        let v = serde_json::to_value(ProvClass::Observed).unwrap();
        assert_eq!(v, "observed");
        let v = serde_json::to_value(ProvClass::Specification).unwrap();
        assert_eq!(v, "specification");
        let v = serde_json::to_value(ProvClass::Hypothesis).unwrap();
        assert_eq!(v, "hypothesis");
    }

    #[test]
    fn check_summary_keeps_provenance_sides() {
        let r = CheckResult {
            entity_key: 1,
            entity_name: "Motor42".to_string(),
            signal: "current".to_string(),
            window: Window { from: 0, to: 10 },
            status: CheckStatus::Violation,
            expected_max: Some(10.0),
            expected_prov: Some(Provenance::Specification),
            unit: Some("A".to_string()),
            observed: Some(12.5),
            observed_prov: Some(Provenance::Observation),
            deviation: Some(2.5),
            points_checked: 2,
        };
        let s = CheckSummary::from_result(&r);
        assert_eq!(s.status, "VIOLATION");
        assert_eq!(s.deviation, Some(2.5));
        assert_eq!(s.expected_prov.as_deref(), Some("Specification"));
        assert_eq!(s.observed_prov.as_deref(), Some("Observation"));
    }
}

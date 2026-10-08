//! CHECK/FOR — Expected vs Observed deviation engine (Phase 6.4, spec §25).
//!
//! The central twin operation is the EXPECTED -> OBSERVED -> COMPARE ->
//! DEVIATION pipeline. Expected values are stored as entity properties
//! (`spec.<signal>.max`, e.g. `spec.current.max = "10"`, optionally
//! `spec.<signal>.unit`); observations live in the Phase 3 time-series store
//! under the same naming convention as `temporal.rs`:
//! `"<entity_name>.<signal>"`.
//!
//! Design choices (v0):
//! - **observed = max of the window** (worst case). A violation must be
//!   detected even if it happened once; averaging would hide transient
//!   overshoots. `points_checked` lets callers see how much evidence backs
//!   the verdict.
//! - **deviation = observed - expected_max**: positive = overshoot,
//!   negative = within spec (spec §25 example: 21.8 - 24 = -2.2).
//! - **No tolerance band in v0**: `observed > expected_max` => VIOLATION,
//!   strict inequality, so `observed == expected_max` is still OK.
//! - **Provenance is preserved, never mutated**: expected values keep
//!   `Provenance::Specification` provenance (datasheet/design intent) and
//!   observed values keep `Provenance::Observation` provenance (sensor
//!   readings). The CHECK output carries `expected` / `observed` / `status`
//!   (spec §25) with their provenance classes attached; nothing here promotes
//!   a Hypothesis or rewrites any stored record.
//!
//! Statuses: `OK` (observed <= expected_max), `VIOLATION` (observed >
//! expected_max), `NO_DATA` (spec present, no points in window — including a
//! missing series), `NO_SPEC` (no `spec.<signal>.max` property on the
//! entity).
//!
//! VidgeQL statement (bonus integration, §25 example shape):
//! ```text
//! CHECK m.current FOR (m:Motor) WHERE m.name = "Motor42" DURING last(1h) RETURN status, deviation
//! ```

use crate::engine::EngineError;
use crate::model::Provenance;
use crate::stores::GraphStore;
use crate::temporal::{parse_last, Window};
use crate::timeseries::TimeSeriesStore;

// ---------------------------------------------------------------------------
// Result model
// ---------------------------------------------------------------------------

/// Outcome of a CHECK comparison (spec §25 statuses).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    /// Observed max <= expected max.
    Ok,
    /// Observed max > expected max (strict, no tolerance band in v0).
    Violation,
    /// Spec present but no observation points in the window (or no series).
    NoData,
    /// No `spec.<signal>.max` property on the entity.
    NoSpec,
}

impl CheckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckStatus::Ok => "OK",
            CheckStatus::Violation => "VIOLATION",
            CheckStatus::NoData => "NO_DATA",
            CheckStatus::NoSpec => "NO_SPEC",
        }
    }
}

/// One CHECK result: expected/observed/status with provenance preserved.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckResult {
    pub entity_key: u32,
    pub entity_name: String,
    pub signal: String,
    pub window: Window,
    pub status: CheckStatus,
    /// Expected max from the `spec.<signal>.max` property (Specification).
    pub expected_max: Option<f64>,
    pub expected_prov: Option<Provenance>,
    /// Optional `spec.<signal>.unit` property.
    pub unit: Option<String>,
    /// Worst-case (max) observation over the window (Observation).
    pub observed: Option<f64>,
    pub observed_prov: Option<Provenance>,
    /// `observed - expected_max`; positive = overshoot. None unless both
    /// expected and observed exist.
    pub deviation: Option<f64>,
    /// Number of points aggregated into `observed`.
    pub points_checked: usize,
}

// ---------------------------------------------------------------------------
// Core engine
// ---------------------------------------------------------------------------

/// Property key holding the expected max for a signal: `spec.<signal>.max`.
pub fn spec_prop_key(signal: &str) -> String {
    format!("spec.{}.max", signal)
}

/// Property key holding the unit for a signal: `spec.<signal>.unit`.
fn spec_unit_key(signal: &str) -> String {
    format!("spec.{}.unit", signal)
}

/// CHECK one entity + signal over a window.
///
/// Reads the expected value from the entity's inline properties
/// (`gs.entity_props`) and the observations from the time-series series
/// named `<entity_name>.<signal>` (same convention as temporal.rs).
pub fn check_entity(
    eng: &mut crate::engine::Engine,
    gs: &mut GraphStore,
    ts: &mut TimeSeriesStore,
    entity_key: u32,
    signal: &str,
    window: Window,
) -> Result<CheckResult, EngineError> {
    let name_sid = gs.entity_name_sid(eng, entity_key)?;
    let entity_name = gs.get_str(eng, name_sid)?;

    // EXPECTED: entity property `spec.<signal>.max` (Specification provenance).
    let props = gs.entity_props(eng, entity_key)?;
    let key = spec_prop_key(signal);
    let expected_max = props
        .iter()
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse::<f64>().ok());
    let unit = props
        .iter()
        .find(|(k, _)| *k == spec_unit_key(signal))
        .map(|(_, v)| v.clone());

    // OBSERVED: series `<entity_name>.<signal>`, max over the window.
    let series_name = format!("{}.{}", entity_name, signal);
    let observed = match ts.series.iter().position(|s| s.name == series_name) {
        Some(i) => {
            let pts = ts.query(eng, i as u32, window.from, window.to)?;
            points_max(&pts)
        }
        None => None,
    };
    let points_checked = match ts.series.iter().position(|s| s.name == series_name) {
        Some(i) => ts.query(eng, i as u32, window.from, window.to)?.len(),
        None => 0,
    };

    // COMPARE -> DEVIATION / status.
    let status = match (expected_max, observed) {
        (None, _) => CheckStatus::NoSpec,
        (Some(_), None) => CheckStatus::NoData,
        (Some(exp), Some(obs)) => {
            if obs > exp {
                CheckStatus::Violation
            } else {
                CheckStatus::Ok
            }
        }
    };
    let deviation = match (expected_max, observed) {
        (Some(exp), Some(obs)) => Some(obs - exp),
        _ => None,
    };

    Ok(CheckResult {
        entity_key,
        entity_name,
        signal: signal.to_string(),
        window,
        status,
        expected_max,
        expected_prov: expected_max.map(|_| Provenance::Specification),
        unit,
        observed,
        observed_prov: observed.map(|_| Provenance::Observation),
        deviation,
        points_checked,
    })
}

/// Max value of a point list; None when empty (NO_DATA).
fn points_max(pts: &[(i64, f64)]) -> Option<f64> {
    pts.iter()
        .fold(None::<f64>, |acc, (_, v)| Some(acc.unwrap_or(*v).max(*v)))
}

// ---------------------------------------------------------------------------
// VidgeQL CHECK statement (spec §25 example shape)
// ---------------------------------------------------------------------------

/// Parsed `CHECK var.signal FOR (pattern) [WHERE ..] DURING win RETURN fields`.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckQuery {
    pub var: String,
    pub signal: String,
    pub pattern: crate::vidgeql::Pattern,
    /// Boolean WHERE tree (Phase 6.3), evaluated by the core executor.
    pub where_tree: crate::vidgeql::BoolExpr,
    pub window: Window,
    /// Requested RETURN fields (status, deviation, expected, observed, ...).
    pub ret_fields: Vec<String>,
}

/// Fields a CHECK RETURN may request, with their rendering.
const CHECK_FIELDS: &[&str] = &[
    "status",
    "expected",
    "observed",
    "deviation",
    "entity",
    "name",
    "signal",
    "unit",
    "points",
];

/// Parse a CHECK statement. The FOR/WHERE core is delegated to the Phase 6
/// parser (same glue trick as temporal.rs: wrap the pattern in a MATCH with a
/// placeholder RETURN).
pub fn parse_check(input: &str, now: i64) -> Result<CheckQuery, String> {
    let up = input.to_uppercase();
    let rest = up.strip_prefix("CHECK").ok_or("expected CHECK")?;
    let _ = rest;
    let after_check = &input["CHECK".len()..];

    // Split the tail at the top-level keywords, in order of appearance.
    let tail_up = up["CHECK".len()..].to_string();
    let idx_of = |kw: &str| tail_up.find(kw);
    let for_i = idx_of("FOR").ok_or("CHECK requires FOR")?;
    let during_i = idx_of("DURING").ok_or("CHECK requires DURING")?;
    let ret_i = idx_of("RETURN").ok_or("CHECK requires RETURN")?;

    let signal_ref = after_check[..for_i].trim();
    let (var, signal) = signal_ref
        .split_once('.')
        .ok_or("CHECK expects var.signal")?;

    let pattern_txt = after_check[for_i + "FOR".len()..during_i].trim();
    // WHERE lives inside pattern_txt; hand the whole FOR..DURING span to the
    // base parser as a MATCH clause.
    let core = format!("MATCH {} RETURN _tmp", pattern_txt);
    let base = crate::vidgeql::Parser::parse(&core).map_err(|e| e.to_string())?;

    // Window: last(n) or t1..t2, cut at RETURN/LIMIT.
    let win_txt = after_check[during_i + "DURING".len()..ret_i].trim();
    let win_txt = match win_txt.to_uppercase().find("LIMIT") {
        Some(i) => win_txt[..i].trim(),
        None => win_txt,
    };
    let window = if win_txt.to_uppercase().starts_with("LAST") {
        let open = win_txt.find('(').ok_or("last() requires (")?;
        let close = win_txt.rfind(')').ok_or("last() requires )")?;
        parse_last(win_txt[open + 1..close].trim(), now)
            .ok_or_else(|| "bad last() duration".to_string())?
    } else {
        let (a, b) = win_txt
            .split_once("..")
            .ok_or("DURING expects last(n) or a..b")?;
        Window {
            from: a.trim().parse().map_err(|_| "bad from".to_string())?,
            to: b.trim().parse().map_err(|_| "bad to".to_string())?,
        }
    };

    let ret_txt = after_check[ret_i + "RETURN".len()..].trim();
    let ret_txt = match ret_txt.to_uppercase().find("LIMIT") {
        Some(i) => ret_txt[..i].trim(),
        None => ret_txt,
    };
    let mut ret_fields = Vec::new();
    for f in ret_txt.split(',') {
        let f = f.trim().to_lowercase();
        if f.is_empty() {
            continue;
        }
        if !CHECK_FIELDS.contains(&f.as_str()) {
            return Err(format!("unknown CHECK field: {}", f));
        }
        ret_fields.push(f);
    }
    if ret_fields.is_empty() {
        return Err("CHECK RETURN requires at least one field".to_string());
    }

    Ok(CheckQuery {
        var: var.trim().to_string(),
        signal: signal.trim().to_string(),
        pattern: base.pattern,
        where_tree: base.where_tree,
        window,
        ret_fields,
    })
}

/// One CHECK statement result row: the full CheckResult plus the requested
/// RETURN fields rendered as (field, value) pairs ("-" when absent).
#[derive(Debug, Clone, PartialEq)]
pub struct CheckRow {
    pub result: CheckResult,
    pub fields: Vec<(String, String)>,
}

fn fmt_opt(v: Option<f64>) -> String {
    match v {
        Some(x) => {
            if x == x.trunc() && x.abs() < 1e15 {
                format!("{}", x as i64)
            } else {
                format!("{}", x)
            }
        }
        None => "-".to_string(),
    }
}

fn render_field(res: &CheckResult, field: &str) -> String {
    match field {
        "status" => res.status.as_str().to_string(),
        "expected" => fmt_opt(res.expected_max),
        "observed" => fmt_opt(res.observed),
        "deviation" => fmt_opt(res.deviation),
        "entity" | "name" => res.entity_name.clone(),
        "signal" => res.signal.clone(),
        "unit" => res.unit.clone().unwrap_or_else(|| "-".to_string()),
        "points" => res.points_checked.to_string(),
        _ => "-".to_string(),
    }
}

/// Execute a CHECK statement: bind the FOR pattern via the base executor,
/// then run check_entity per binding.
pub fn execute_check(
    eng: &mut crate::engine::Engine,
    gs: &mut GraphStore,
    ts: &mut TimeSeriesStore,
    q: &CheckQuery,
) -> Result<Vec<CheckRow>, EngineError> {
    let core = crate::vidgeql::Query {
        pattern: q.pattern.clone(),
        where_: Vec::new(),
        where_tree: q.where_tree.clone(),
        at: None,
        ret_vars: Vec::new(),
        limit: None,
    };
    let mut rows = Vec::new();
    for row in crate::executor::execute(eng, gs, &core)? {
        let key = match row.get(&q.var) {
            Some(k) => k,
            None => continue,
        };
        let res = check_entity(eng, gs, ts, key, &q.signal, q.window)?;
        let fields = q
            .ret_fields
            .iter()
            .map(|f| (f.clone(), render_field(&res, f)))
            .collect();
        rows.push(CheckRow {
            result: res,
            fields,
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::stores::GraphStore;
    use crate::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_check_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn parse_check_statement() {
        let now = 1_760_000_000i64;
        let q = parse_check(
            "CHECK m.current FOR (m:Motor) WHERE m.name = \"Motor42\" DURING last(1h) RETURN status, deviation",
            now,
        )
        .unwrap();
        assert_eq!(q.var, "m");
        assert_eq!(q.signal, "current");
        assert_eq!(q.window, parse_last("1h", now).unwrap());
        assert_eq!(q.ret_fields, vec!["status", "deviation"]);
        assert_eq!(
            q.where_tree,
            crate::vidgeql::BoolExpr::Leaf(crate::vidgeql::Cond {
                var: "m".to_string(),
                prop: "name".to_string(),
                op: crate::vidgeql::CmpOp::Eq,
                value: crate::vidgeql::Value::Str("Motor42".to_string()),
            })
        );
        assert_eq!(q.pattern.nodes.len(), 1);
        // Absolute window form.
        let q2 = parse_check(
            "CHECK p.pressure FOR (p:Pump) DURING 100..200 RETURN expected",
            now,
        )
        .unwrap();
        assert_eq!(q2.window, Window { from: 100, to: 200 });
        assert_eq!(q2.ret_fields, vec!["expected"]);
        // Bad field rejected.
        assert!(parse_check("CHECK m.x FOR (m:Motor) DURING last(1h) RETURN bogus", now).is_err());
    }

    #[test]
    fn status_strings() {
        assert_eq!(CheckStatus::Ok.as_str(), "OK");
        assert_eq!(CheckStatus::Violation.as_str(), "VIOLATION");
        assert_eq!(CheckStatus::NoData.as_str(), "NO_DATA");
        assert_eq!(CheckStatus::NoSpec.as_str(), "NO_SPEC");
    }

    #[test]
    fn fmt_opt_renders() {
        assert_eq!(fmt_opt(Some(2.5)), "2.5");
        assert_eq!(fmt_opt(Some(10.0)), "10");
        assert_eq!(fmt_opt(Some(-2.2)), "-2.2");
        assert_eq!(fmt_opt(None), "-");
    }

    #[test]
    fn points_max_semantics() {
        assert_eq!(points_max(&[(0, 1.0), (1, 3.5), (2, 2.0)]), Some(3.5));
        assert_eq!(points_max(&[]), None);
    }

    #[test]
    fn spec_prop_key_format() {
        assert_eq!(spec_prop_key("current"), "spec.current.max");
    }

    #[test]
    fn e2e_ok_violation_nodata_nospec() {
        let path = tmp_path("core");
        cleanup(&path);
        let now = 1_760_000_000i64;
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let mut tx = eng.begin().unwrap();
        // Conformant motor: spec 10, readings <= 10.
        let m1 = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "MotorOK",
                &[("spec.current.max", "10"), ("spec.current.unit", "A")],
            )
            .unwrap();
        // Violating motor: spec 10, peak 12.5.
        let m2 = gs
            .add_entity(
                &mut eng,
                &mut tx,
                "Motor",
                "MotorBad",
                &[("spec.current.max", "10")],
            )
            .unwrap();
        // Motor with series but no spec.
        let m3 = gs
            .add_entity(&mut eng, &mut tx, "Motor", "MotorNoSpec", &[])
            .unwrap();
        let s1 = ts
            .create_series(&mut eng, &mut tx, "MotorOK.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s1, now - 600, 9.0).unwrap();
        ts.append(&mut eng, &mut tx, s1, now - 300, 10.0).unwrap();
        let s2 = ts
            .create_series(&mut eng, &mut tx, "MotorBad.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s2, now - 600, 8.0).unwrap();
        ts.append(&mut eng, &mut tx, s2, now - 300, 12.5).unwrap();
        let s3 = ts
            .create_series(&mut eng, &mut tx, "MotorNoSpec.current")
            .unwrap();
        ts.append(&mut eng, &mut tx, s3, now - 300, 5.0).unwrap();
        for s in [s1, s2, s3] {
            ts.flush_series(&mut eng, &mut tx, s).unwrap();
        }
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        ts.persist(&mut eng).unwrap();

        let win = parse_last("1h", now).unwrap();
        let r1 = check_entity(&mut eng, &mut gs, &mut ts, m1, "current", win).unwrap();
        assert_eq!(r1.status, CheckStatus::Ok);
        assert_eq!(r1.expected_max, Some(10.0));
        assert_eq!(r1.observed, Some(10.0));
        assert_eq!(r1.deviation, Some(0.0));
        assert_eq!(r1.expected_prov, Some(Provenance::Specification));
        assert_eq!(r1.observed_prov, Some(Provenance::Observation));
        assert_eq!(r1.unit, Some("A".to_string()));
        // Strict inequality: observed == expected_max is OK (no tolerance v0).

        let r2 = check_entity(&mut eng, &mut gs, &mut ts, m2, "current", win).unwrap();
        assert_eq!(r2.status, CheckStatus::Violation);
        assert_eq!(r2.observed, Some(12.5));
        assert_eq!(r2.deviation, Some(2.5));

        // NO_DATA: spec present, series missing entirely.
        let mut tx2 = eng.begin().unwrap();
        let m4 = gs
            .add_entity(
                &mut eng,
                &mut tx2,
                "Motor",
                "MotorNoSeries",
                &[("spec.current.max", "10")],
            )
            .unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx2).unwrap();
        ts.persist(&mut eng).unwrap();
        let r3 = check_entity(&mut eng, &mut gs, &mut ts, m4, "current", win).unwrap();
        assert_eq!(r3.status, CheckStatus::NoData);
        assert_eq!(r3.expected_max, Some(10.0));
        assert_eq!(r3.observed, None);
        assert_eq!(r3.deviation, None);

        // NO_SPEC: series present, no spec prop.
        let r4 = check_entity(&mut eng, &mut gs, &mut ts, m3, "current", win).unwrap();
        assert_eq!(r4.status, CheckStatus::NoSpec);
        assert_eq!(r4.observed, Some(5.0));
        assert_eq!(r4.expected_max, None);
        cleanup(&path);
    }
}

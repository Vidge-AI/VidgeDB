//! VidgeQL temporal extension — Phase 6.2 (spec §23 examples, §24).
//!
//! Adds to the parser/executor:
//! - `MEASURE <series> DURING <range>`: pulls points from the Phase 3
//!   time-series store for the entities bound by MATCH;
//! - `DURING last(<dur>)` / `DURING t1..t2`: temporal window (relative
//!   durations resolve against a caller-provided `now`);
//! - RETURN aggregates: `max(x), avg(x), min(x), sum(x), count(x)` over
//!   the measured points.
//!
//! Grammar additions:
//!   query   := MATCH pattern (WHERE ...)? (MEASURE var.series DURING win)?
//!              RETURN items LIMIT n?
//!   win     := last '(' unit ')' | num '..' num
//!   unit    := <n><s|m|h|d>
//!   agg     := ('max'|'min'|'avg'|'sum'|'count') '(' var '.' signal ')'
//!
//! The executor joins graph bindings with time-series reads; aggregate
//! results land in the row's `aggs` map.

use crate::engine::{Engine, EngineError};
use crate::stores::GraphStore;
use crate::timeseries::TimeSeriesStore;
use crate::vidgeql::{BoolExpr, Pattern};

// ---------------------------------------------------------------------------
// Temporal window
// ---------------------------------------------------------------------------

/// Absolute [from, to] window (inclusive bounds; unix seconds).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Window {
    pub from: i64,
    pub to: i64,
}

/// Parse `last(24h)` style durations against a reference `now`.
pub fn parse_last(dur: &str, now: i64) -> Option<Window> {
    let s = dur.trim();
    let (num_str, unit) = s.split_at(s.len() - 1);
    let n: i64 = num_str.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => return None,
    };
    Some(Window {
        from: now - secs,
        to: now,
    })
}

// ---------------------------------------------------------------------------
// Aggregate AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    Max,
    Min,
    Avg,
    Sum,
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggExpr {
    pub kind: AggKind,
    pub var: String,
    /// Signal name == series name (by convention: "<entity>.<signal>").
    pub signal: String,
}

/// RETURN item: either a variable or an aggregate expression.
#[derive(Debug, Clone, PartialEq)]
pub enum RetItem {
    Var(String),
    Agg(AggExpr),
}

// ---------------------------------------------------------------------------
// Temporal query AST (extends the base Query)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct TQuery {
    pub pattern: Pattern,
    /// Boolean WHERE tree (Phase 6.3): the temporal layer evaluates the
    /// same tree semantics as the core executor via `core_view`.
    pub where_tree: BoolExpr,
    /// Temporal snapshot (Phase 6.5, spec §24): `AT <unix-seconds>` instant;
    /// `None` = current topology (no time travel).
    pub at: Option<i64>,
    pub ret: Vec<RetItem>,
    pub limit: Option<usize>,
    /// (var, signal, window) — single MEASURE clause in v0.
    pub measure: Option<Measure>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Measure {
    pub var: String,
    pub signal: String,
    pub window: Window,
}

/// Aggregate result value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AggValue {
    pub kind: AggKind,
    pub value: f64,
}

/// A result row: bindings + per-aggregate results.
#[derive(Debug, Clone, PartialEq)]
pub struct TRow {
    pub bindings: Vec<(String, u32)>,
    pub aggs: Vec<(String, AggValue)>,
}

// ---------------------------------------------------------------------------
// Temporal parser (superset grammar)
// ---------------------------------------------------------------------------

/// Parse the extended grammar. The MATCH/WHERE/RETURN core is delegated to
/// the Phase 6 parser; MEASURE/DURING and aggregate RETURN items are parsed
/// here on the raw text segments.
pub fn parse_tquery(input: &str, now: i64) -> Result<TQuery, String> {
    let up = input.to_uppercase();
    let (core, measure_part) = match up.find("MEASURE") {
        Some(i) => (&input[..i], Some(&input[i + "MEASURE".len()..])),
        None => (input, None),
    };
    // Measure clause: <var>.<signal> DURING last(<dur>) | t1..t2
    let measure = measure_part
        .map(|m| -> Result<Measure, String> {
            let m = m.trim();
            let (sig_part, win_part) = m.split_once("DURING").ok_or("MEASURE requires DURING")?;
            let signal_full = sig_part.trim();
            // The measure clause may be followed by RETURN/LIMIT; cut there.
            let win_str = win_part.trim();
            let win_str = match win_str.to_uppercase().find("RETURN") {
                Some(i) => win_str[..i].trim(),
                None => win_str,
            };
            let win_str = match win_str.to_uppercase().find("LIMIT") {
                Some(i) => win_str[..i].trim(),
                None => win_str,
            };
            let win = if win_str.to_uppercase().starts_with("LAST") {
                let open = win_str.find('(').ok_or("last() requires (")?;
                let close = win_str.rfind(')').ok_or("last() requires )")?;
                let inner = win_str[open + 1..close].trim();
                parse_last(inner, now).ok_or_else(|| "bad last() duration".to_string())?
            } else {
                let (a, b) = win_str
                    .split_once("..")
                    .ok_or("DURING expects last(n) or a..b")?;
                Window {
                    from: a.trim().parse().map_err(|_| "bad from".to_string())?,
                    to: b.trim().parse().map_err(|_| "bad to".to_string())?,
                }
            };
            let (var, signal) = signal_full
                .split_once('.')
                .ok_or("MEASURE expects var.signal")?;
            Ok(Measure {
                var: var.trim().to_string(),
                signal: signal.trim().to_string(),
                window: win,
            })
        })
        .transpose()?;
    // The core may end after MATCH/WHERE when MEASURE carries the RETURN;
    // the base parser requires a RETURN, so append a placeholder one.
    let core_for_base = if measure_part.is_some() {
        format!("{} RETURN _tmp", core.trim_end())
    } else {
        core.to_string()
    };
    let base = crate::vidgeql::Parser::parse(&core_for_base).map_err(|e| e.to_string())?;
    // RETURN items: re-scan the RETURN list from the raw text (the base
    // parser folds aggregates and variables into plain idents).
    // When MEASURE carries the RETURN, scan the full input instead of core.
    let mut ret = Vec::new();
    let ret_src: String = if measure_part.is_some() {
        input.to_string()
    } else {
        core.to_string()
    };
    let ret_start = ret_src
        .to_uppercase()
        .find("RETURN")
        .map(|i| i + "RETURN".len());
    if let Some(rs) = ret_start {
        let ret_text = ret_src[rs..].trim();
        let ret_text = match ret_text.to_uppercase().find("LIMIT") {
            Some(i) => ret_text[..i].trim(),
            None => ret_text,
        };
        for item in ret_text.split(',') {
            let item = item.trim();
            let lower = item.to_lowercase();
            // Aggregates look like max(var.signal); the inner item may be a
            // bare signal (spec §23 example: RETURN max(current)).
            let kind = [
                ("max(", AggKind::Max),
                ("min(", AggKind::Min),
                ("avg(", AggKind::Avg),
                ("sum(", AggKind::Sum),
                ("count(", AggKind::Count),
            ]
            .iter()
            .find(|(k, _)| lower.starts_with(k))
            .map(|(_, k)| *k);
            match kind {
                Some(kind) => {
                    let open = item.find('(').ok_or("aggregate requires (")?;
                    let close = item.rfind(')').ok_or("aggregate requires )")?;
                    let inner = item[open + 1..close].trim();
                    // var.signal or bare signal (var from MEASURE).
                    let (var, sig) = match inner.split_once('.') {
                        Some((v, s)) => (v.trim().to_string(), s.trim().to_string()),
                        None => (
                            measure.as_ref().map(|m| m.var.clone()).unwrap_or_default(),
                            inner.to_string(),
                        ),
                    };
                    ret.push(RetItem::Agg(AggExpr {
                        kind,
                        var,
                        signal: sig,
                    }));
                }
                None => ret.push(RetItem::Var(item.to_string())),
            }
        }
    }
    Ok(TQuery {
        pattern: base.pattern,
        where_tree: base.where_tree,
        at: base.at,
        ret,
        limit: base.limit,
        measure,
    })
}

// ---------------------------------------------------------------------------
// Executor extension
// ---------------------------------------------------------------------------

pub fn aggregate_points(kind: AggKind, pts: &[(i64, f64)]) -> f64 {
    match kind {
        AggKind::Count => pts.len() as f64,
        AggKind::Sum => pts.iter().map(|(_, v)| v).sum(),
        AggKind::Avg => {
            if pts.is_empty() {
                f64::NAN
            } else {
                pts.iter().map(|(_, v)| v).sum::<f64>() / pts.len() as f64
            }
        }
        AggKind::Max => pts
            .iter()
            .fold(f64::NEG_INFINITY, |acc, (_, v)| acc.max(*v)),
        AggKind::Min => pts.iter().fold(f64::INFINITY, |acc, (_, v)| acc.min(*v)),
    }
}

/// Execute a temporal query: MATCH/WHERE via the base executor, then MEASURE
/// per binding: the series name convention is "<entity_name>.<signal>".
pub fn execute_t(
    eng: &mut Engine,
    gs: &mut GraphStore,
    ts: &mut TimeSeriesStore,
    q: &TQuery,
) -> Result<Vec<TRow>, EngineError> {
    let mut rows = Vec::new();
    for row in crate::executor::execute(eng, gs, &core_view(q))? {
        let mut aggs = Vec::new();
        if let Some(m) = &q.measure {
            // Resolve the bound entity's name -> series name.
            let key = row.get(&m.var).ok_or(EngineError::Page(
                crate::pager::PageError::PageOutOfBounds(u32::MAX),
            ))?;
            let name_sid = gs.entity_name_sid(eng, key)?;
            let ename = gs.get_str(eng, name_sid)?;
            let series_name = format!("{}.{}", ename, m.signal);
            if let Some(sid) = find_series(ts, &series_name) {
                let pts = ts.query(eng, sid, m.window.from, m.window.to)?;
                for (kind, label) in ret_aggs(q) {
                    let v = aggregate_points(kind, &pts);
                    aggs.push((label, AggValue { kind, value: v }));
                }
            }
        }
        rows.push(TRow {
            bindings: row.bindings,
            aggs,
        });
    }
    if let Some(n) = q.limit {
        rows.truncate(n);
    }
    Ok(rows)
}

fn ret_aggs(q: &TQuery) -> Vec<(AggKind, String)> {
    let mut out = Vec::new();
    if let Some(m) = &q.measure {
        for item in &q.ret {
            if let RetItem::Agg(a) = item {
                if a.signal == m.signal {
                    out.push((a.kind, format!("{:?}", a.kind).to_lowercase()));
                }
            }
        }
    }
    out
}

fn find_series(ts: &TimeSeriesStore, name: &str) -> Option<u32> {
    ts.series
        .iter()
        .position(|s| s.name == name)
        .map(|i| i as u32)
}

/// View the TQuery as the base Query the core executor consumes.
fn core_view(q: &TQuery) -> crate::vidgeql::Query {
    crate::vidgeql::Query {
        pattern: q.pattern.clone(),
        where_: Vec::new(),
        where_tree: q.where_tree.clone(),
        // Propagate the AT snapshot: the core executor binds hops through
        // the adjacency valid at that instant (Phase 6.5, spec §24).
        at: q.at,
        ret_vars: Vec::new(),
        limit: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_last_durations() {
        let now = 1_700_000_000;
        let w = parse_last("24h", now).unwrap();
        assert_eq!(w.to, now);
        assert_eq!(w.from, now - 24 * 3600);
        let w2 = parse_last("30m", now).unwrap();
        assert_eq!(w2.from, now - 1800);
        assert!(parse_last("12x", now).is_none());
        assert!(parse_last("abc", now).is_none());
    }

    #[test]
    fn aggregate_kinds() {
        let pts = vec![(0, 1.0), (1, 2.0), (2, 3.0)];
        assert_eq!(aggregate_points(AggKind::Count, &pts), 3.0);
        assert_eq!(aggregate_points(AggKind::Sum, &pts), 6.0);
        assert_eq!(aggregate_points(AggKind::Avg, &pts), 2.0);
        assert_eq!(aggregate_points(AggKind::Max, &pts), 3.0);
        assert_eq!(aggregate_points(AggKind::Min, &pts), 1.0);
        assert!(aggregate_points(AggKind::Avg, &[]).is_nan());
        assert_eq!(aggregate_points(AggKind::Count, &[]), 0.0);
    }
}

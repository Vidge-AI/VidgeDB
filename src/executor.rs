//! VidgeQL executor — Phase 6 (spec §23/§25).
//!
//! Binds a parsed Query against the Phase 2 graph store:
//! - pattern binding walks the adjacency index (topology-filtered hops);
//! - WHERE filters on entity properties (name / type / any inline prop);
//! - RETURN collects the bound rows; LIMIT truncates.
//!
//! Binding order: start from every entity matching node[0].type, then for
//! each hop follow out-edges of the topology class to candidates of the
//! next node's type. A binding is a `Vec<u32>` of entity keys, parallel to
//! `pattern.nodes`.
//!
//! Property model (Phase 6 v0): `name`/`type` come from the interned
//! strings referenced by the entity cell; other props decode the inline
//! `k=v\0` payload. Comparisons: strings lexicographic, numbers numeric.

use crate::engine::{Engine, EngineError};
use crate::stores::GraphStore;
use crate::vidgeql::{BoolExpr, CmpOp, Cond, Query, Value};

/// One bound result row: var name -> entity key.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub bindings: Vec<(String, u32)>,
}

impl Row {
    pub fn get(&self, var: &str) -> Option<u32> {
        self.bindings
            .iter()
            .find(|(v, _)| v == var)
            .map(|(_, e)| *e)
    }
}

fn entity_fields(
    eng: &mut Engine,
    gs: &mut GraphStore,
    key: u32,
) -> Result<(String, String), EngineError> {
    let name_sid = gs.entity_name_sid(eng, key)?;
    let type_sid = gs.entity_type_sid(eng, key)?;
    let name = gs.get_str(eng, name_sid)?;
    let type_name = gs.get_str(eng, type_sid)?;
    Ok((name, type_name))
}

fn compare_str(a: &str, op: CmpOp, b: &str) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Gt => a > b,
        CmpOp::Le => a <= b,
        CmpOp::Ge => a >= b,
    }
}

fn eval_cond(
    eng: &mut Engine,
    gs: &mut GraphStore,
    cond: &Cond,
    row: &Row,
) -> Result<bool, EngineError> {
    let key = row
        .get(&cond.var)
        .ok_or_else(|| EngineError::Page(crate::pager::PageError::PageOutOfBounds(u32::MAX)))?;
    let (name, type_name) = entity_fields(eng, gs, key)?;
    let lhs: String = match cond.prop.as_str() {
        "name" => name,
        "type" => type_name,
        p => gs
            .entity_props(eng, key)?
            .into_iter()
            .find(|(k, _)| k == p)
            .map(|(_, v)| v)
            .unwrap_or_default(),
    };
    Ok(match &cond.value {
        Value::Str(s) => compare_str(&lhs, cond.op, s),
        Value::Num(n) => lhs.parse::<f64>().is_ok_and(|x| match cond.op {
            CmpOp::Eq => x == *n,
            CmpOp::Ne => x != *n,
            CmpOp::Lt => x < *n,
            CmpOp::Gt => x > *n,
            CmpOp::Le => x <= *n,
            CmpOp::Ge => x >= *n,
        }),
    })
}

/// Evaluate the boolean WHERE tree (Phase 6.3, spec §23): And = all
/// children hold, Or = at least one child holds, Leaf = one comparison.
/// An empty And is vacuously true; an empty Or is false (SQL semantics).
fn eval_bool(
    eng: &mut Engine,
    gs: &mut GraphStore,
    expr: &BoolExpr,
    row: &Row,
) -> Result<bool, EngineError> {
    match expr {
        BoolExpr::And(children) => {
            for c in children {
                if !eval_bool(eng, gs, c, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        BoolExpr::Or(children) => {
            for c in children {
                if eval_bool(eng, gs, c, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        BoolExpr::Leaf(cond) => eval_cond(eng, gs, cond, row),
    }
}

/// Execute a query against the graph store.
pub fn execute(eng: &mut Engine, gs: &mut GraphStore, q: &Query) -> Result<Vec<Row>, EngineError> {
    // Start set: every entity whose type matches node[0].
    let t0 = &q.pattern.nodes[0];
    let mut bindings: Vec<Vec<u32>> = Vec::new();
    for key in 0..gs.entity_count() {
        let (_, tn) = entity_fields(eng, gs, key)?;
        if tn == t0.type_name {
            bindings.push(vec![key]);
        }
    }
    // Walk the hops through the adjacency index. With `AT <t>` (Phase 6.5,
    // spec §24) hops bind through the topology valid at that instant:
    // the index is rebuilt from the in-memory relation records, filtered to
    // records alive at t (valid_from <= t < valid_to, -1 = open-ended).
    for (hop, edge) in q.pattern.edges.iter().enumerate() {
        let topo_sid = gs
            .str_lookup(&edge.topology)
            .map(|s| s.0)
            .unwrap_or(u32::MAX); // unknown topology matches nothing
        let mut next_bindings: Vec<Vec<u32>> = Vec::new();
        for b in &bindings {
            let last = *b.last().unwrap();
            let edges = if let Some(t) = q.at {
                // Temporal snapshot: rebuild the adjacency at instant t from
                // the in-memory records and use it for this hop.
                let snap = gs.adjacency_at(t);
                snap.out.get(&last).cloned().unwrap_or_default()
            } else {
                gs.adjacency.out.get(&last).cloned().unwrap_or_default()
            };
            for (topo, other, _) in edges {
                if topo != topo_sid {
                    continue;
                }
                let (_, tn) = entity_fields(eng, gs, other)?;
                if tn == q.pattern.nodes[hop + 1].type_name {
                    let mut nb = b.clone();
                    nb.push(other);
                    next_bindings.push(nb);
                }
            }
        }
        bindings = next_bindings;
    }
    // WHERE + RETURN projection.
    let mut rows: Vec<Row> = Vec::new();
    for b in bindings {
        let row = Row {
            bindings: q
                .pattern
                .nodes
                .iter()
                .zip(&b)
                .filter_map(|(n, e)| n.var.clone().map(|v| (v, *e)))
                .collect(),
        };
        let ok = eval_bool(eng, gs, &q.where_tree, &row)?;
        if ok {
            rows.push(row);
        }
    }
    if let Some(n) = q.limit {
        rows.truncate(n);
    }
    Ok(rows)
}

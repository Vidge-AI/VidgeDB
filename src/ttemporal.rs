//! Temporal adjacency — Phase 6.5 (spec §9 Temporal Relations, §24 Temporal
//! Queries).
//!
//! Design choice (recorded per spec §21): this is a NEW module (`ttemporal`)
//! instead of an extension of `stores.rs` because it layers time-aware
//! semantics on top of the Phase 2 graph store without touching the on-disk
//! formats. Inherent `impl` blocks on `Adjacency` / `GraphStore` are legal
//! anywhere in the crate, so the store types keep their wire/IO code in
//! `stores.rs` while the temporal read-path lives here.
//!
//! Validity convention (PRESERVED, do not change): `RelationRecord.valid_to`
//! is an `i64` where `-1` means *open-ended* (the relation never expires).
//! This sentinel is baked into existing data and tests; it is NOT `Option`
//! and must stay a plain `i64`. Use `VALID_TO_OPEN` instead of a literal.
//!
//! Semantics: a relation is *valid at instant* `t` iff
//!
//! ```text
//! r.is_alive() && r.valid_from <= t && (r.valid_to == -1 || t < valid_to)
//! ```
//!
//! i.e. the half-open interval `[valid_from, valid_to)`, `-1 = ∞`.
//! `Adjacency::build()` (stores.rs) intentionally does NOT apply this filter:
//! it remains the Phase 2 "current" index (tombstone-only). Time-aware
//! traversal goes through `Adjacency::build_at` / `GraphStore::neighbors_at`
//! / `GraphStore::multi_hop_at`, which rebuild the index from
//! `GraphStore.relations` (already fully in memory — no page reads).
//!
//! VidgeQL: `AT <unix_seconds>` is parsed into `Query.at` / `TQuery.at` and
//! the executor binds hops through the adjacency rebuilt at that instant.

use crate::stores::{Adjacency, Direction, GraphStore, RelationRecord};

/// Sentinel for an open-ended validity interval (`valid_to = -1`).
pub const VALID_TO_OPEN: i64 = -1;

/// Is relation record `r` valid at instant `t`?
///
/// Tombstoned records are never valid. The validity interval is the
/// half-open range `[valid_from, valid_to)`; `valid_to == VALID_TO_OPEN`
/// (-1) means the interval never ends. Boundary behavior: `t == valid_from`
/// is visible, `t == valid_to` is not.
pub fn relation_alive_at(r: &RelationRecord, t: i64) -> bool {
    r.is_alive() && r.valid_from <= t && (r.valid_to == VALID_TO_OPEN || t < r.valid_to)
}

impl Adjacency {
    /// Build the adjacency index as of instant `t` (spec §9/§24).
    ///
    /// Same edge layout as `Adjacency::build` (topo_sid, other, record idx),
    /// but only records that are alive AND whose validity interval covers
    /// `t` contribute edges. Index entries keep the ORIGINAL slab position
    /// (`i`), so the result stays consistent with `GraphStore.relations`.
    pub fn build_at(records: &[RelationRecord], t: i64) -> Adjacency {
        let mut adj = Adjacency::default();
        for (i, r) in records.iter().enumerate() {
            if !relation_alive_at(r, t) {
                continue;
            }
            adj.out
                .entry(r.src)
                .or_default()
                .push((r.topo_sid, r.dst, i as u32));
            adj.in_
                .entry(r.dst)
                .or_default()
                .push((r.topo_sid, r.src, i as u32));
        }
        adj
    }
}

impl GraphStore {
    /// Adjacency as of instant `t`, rebuilt from the in-memory relation
    /// records (spec §24: reconstruct the topology at this point in time).
    /// Rebuilding per call is fine at Phase 2 scale: records are already
    /// resident, no page reads involved.
    pub fn adjacency_at(&self, t: i64) -> Adjacency {
        Adjacency::build_at(&self.relations, t)
    }

    /// One hop from `entity` through relations valid at `t`.
    pub fn neighbors_at(
        &self,
        entity: u32,
        topology: Option<u32>,
        dir: Direction,
        t: i64,
    ) -> Vec<u32> {
        self.adjacency_at(t).neighbors(entity, topology, dir)
    }

    /// Multi-hop BFS through relations valid at `t` (spec §20 + §24).
    pub fn multi_hop_at(
        &self,
        start: u32,
        topology: Option<u32>,
        dir: Direction,
        max_hops: usize,
        t: i64,
    ) -> Vec<u32> {
        self.adjacency_at(t)
            .multi_hop(start, topology, dir, max_hops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(src: u32, dst: u32, topo: u32, vf: i64, vt: i64, flags: u8) -> RelationRecord {
        RelationRecord {
            src,
            dst,
            type_sid: 10,
            topo_sid: topo,
            valid_from: vf,
            valid_to: vt,
            provenance: 0,
            flags,
        }
    }

    /// Half-open window: [1000, 2000) — visible at both bounds' edges.
    #[test]
    fn bounded_window_boundaries() {
        let r = rec(1, 2, 5, 1000, 2000, 0);
        assert!(!relation_alive_at(&r, 999));
        assert!(relation_alive_at(&r, 1000)); // valid_from inclusive
        assert!(relation_alive_at(&r, 1500));
        assert!(relation_alive_at(&r, 1999));
        assert!(!relation_alive_at(&r, 2000)); // valid_to exclusive
        let adj = Adjacency::build_at(&[r], 1500);
        assert_eq!(adj.neighbors(1, None, Direction::Out), vec![2]);
        assert!(Adjacency::build_at(&[rec(1, 2, 5, 1000, 2000, 0)], 2500)
            .neighbors(1, None, Direction::Out)
            .is_empty());
    }

    /// Open-ended (`valid_to = -1`) is valid at every instant.
    #[test]
    fn open_ended_is_everywhere() {
        let r = rec(1, 2, 5, 0, VALID_TO_OPEN, 0);
        for t in [0i64, 1000, 1_700_000_000, i64::MAX] {
            assert!(relation_alive_at(&r, t), "t = {t}");
        }
        // valid_from still gates the start even when open-ended.
        let r2 = rec(1, 2, 5, 1000, VALID_TO_OPEN, 0);
        assert!(!relation_alive_at(&r2, 500));
        assert!(relation_alive_at(&r2, 1500));
    }

    /// Tombstones win over validity: a dead record is never traversed.
    #[test]
    fn tombstone_beats_validity() {
        let r = rec(1, 2, 5, 0, VALID_TO_OPEN, crate::stores::REL_FLAG_DEAD);
        assert!(!relation_alive_at(&r, 1500));
        assert!(Adjacency::build_at(&[r], 1500)
            .neighbors(1, None, Direction::Out)
            .is_empty());
    }

    /// Slab positions are preserved so the index matches the record vec.
    #[test]
    fn record_indices_preserved() {
        let recs = vec![
            rec(1, 2, 5, 0, VALID_TO_OPEN, 0), // idx 0
            rec(1, 9, 5, 1000, 2000, 0),       // idx 1 (expired at 2500)
            rec(2, 3, 5, 0, VALID_TO_OPEN, 0), // idx 2
        ];
        let adj = Adjacency::build_at(&recs, 1500);
        assert_eq!(adj.out.get(&1).unwrap().len(), 2);
        let adj_late = Adjacency::build_at(&recs, 2500);
        // Only idx 0 and 2 remain; no stale idx references.
        assert_eq!(
            adj_late
                .out
                .get(&1)
                .unwrap()
                .iter()
                .map(|(_, dst, i)| (*dst, *i))
                .collect::<Vec<_>>(),
            vec![(2, 0)]
        );
    }
}

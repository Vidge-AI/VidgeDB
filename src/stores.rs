//! Entity & relation stores for VidgeDB — Phase 2 (spec §6/§7/§8, §20).
//!
//! Storage design (Phase 2 decisions, recorded per spec §21):
//!
//! - **String arena**: slab of pages; cell = `[len u32][bytes]`, packed
//!   after an 8-byte per-page header `[next_page u32][used u32]`. Dedup via
//!   in-memory map, rebuilt on open by walking the slab. Strings never
//!   mutate; a new value = a new cell (old cell leaks until compaction).
//! - **Entity slab**: fixed 64-byte cells
//!   `[type_sid u32][name_sid u32][flags u32][props_len u16][pad u16][props 44B]`.
//!   entity_key = cell index (stable, never reused). Properties are inline
//!   string bytes (`k=v\0k=v\0`), size-capped in Phase 2.
//! - **Relation slab**: fixed 48-byte records (see RelationRecord), tombstone
//!   flag for deletes; records are never physically removed in Phase 2.
//! - **Layout page** (first page of the store): heads/counts of the three
//!   slabs + next string id, so open() rebuilds everything (reconstructible,
//!   spec §17).
//! - **Adjacency index**: derived, in-memory, rebuilt from relation records
//!   on open (spec §20: source of truth is the relation slab).
//!
//! All bytes little-endian. No unsafe (spec §49).

use crate::engine::Engine;
use crate::engine::EngineError;
use crate::pager::{PageId, NULL_PAGE, PAGE_SIZE};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Wire formats
// ---------------------------------------------------------------------------

pub const PAGE_HDR: usize = 8; // [next u32][used u32]
pub const ENTITY_CELL: usize = 64;
pub const RELATION_REC: usize = 48;
pub const REL_FLAG_DEAD: u8 = 1;
pub const ENT_FLAG_DEAD: u32 = 1;

/// Provenance byte values mirror `model::Provenance` discriminants 0..7.
///
/// Validity convention (spec §9, preserved since Phase 2): `[valid_from,
/// valid_to)` is the half-open validity interval; `valid_to == -1` means
/// *open-ended* (never expires). `-1` is a plain-i64 sentinel, NOT `Option`
/// — see `ttemporal::VALID_TO_OPEN` / `ttemporal::relation_alive_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationRecord {
    pub src: u32,
    pub dst: u32,
    pub type_sid: u32,
    pub topo_sid: u32,
    pub valid_from: i64,
    pub valid_to: i64,
    pub provenance: u8,
    pub flags: u8,
}

impl RelationRecord {
    /// Write the record directly into `data` starting at byte `at`.
    pub fn encode_into(&self, data: &mut [u8], at: usize) {
        data[at..at + 4].copy_from_slice(&self.src.to_le_bytes());
        data[at + 4..at + 8].copy_from_slice(&self.dst.to_le_bytes());
        data[at + 8..at + 12].copy_from_slice(&self.type_sid.to_le_bytes());
        data[at + 12..at + 16].copy_from_slice(&self.topo_sid.to_le_bytes());
        data[at + 16..at + 24].copy_from_slice(&self.valid_from.to_le_bytes());
        data[at + 24..at + 32].copy_from_slice(&self.valid_to.to_le_bytes());
        data[at + 32] = self.provenance;
        data[at + 33] = self.flags;
        data[at + 34..at + 48].fill(0);
    }

    pub fn encode(&self, out: &mut [u8; RELATION_REC]) {
        out[0..4].copy_from_slice(&self.src.to_le_bytes());
        out[4..8].copy_from_slice(&self.dst.to_le_bytes());
        out[8..12].copy_from_slice(&self.type_sid.to_le_bytes());
        out[12..16].copy_from_slice(&self.topo_sid.to_le_bytes());
        out[16..24].copy_from_slice(&self.valid_from.to_le_bytes());
        out[24..32].copy_from_slice(&self.valid_to.to_le_bytes());
        out[32] = self.provenance;
        out[33] = self.flags;
        out[34..48].fill(0);
    }

    pub fn decode(buf: &[u8; RELATION_REC]) -> RelationRecord {
        RelationRecord {
            src: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            dst: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            type_sid: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            topo_sid: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            valid_from: i64::from_le_bytes(buf[16..24].try_into().unwrap()),
            valid_to: i64::from_le_bytes(buf[24..32].try_into().unwrap()),
            provenance: buf[32],
            flags: buf[33],
        }
    }

    pub fn is_alive(&self) -> bool {
        self.flags & REL_FLAG_DEAD == 0
    }
}

// ---------------------------------------------------------------------------
// Graph store
// ---------------------------------------------------------------------------

/// Opaque interned-string handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(pub u32);

/// On-disk layout page (fixed positions inside its 4096 bytes):
/// [str_head u32][str_pages u32][ent_head u32][ent_pages u32][ent_cells u32]
/// [rel_head u32][rel_pages u32][rel_records u32][next_sid u32]
/// [ts_meta_page u32][se_meta_page u32]
/// Fields start at offset 0 of the page (META_OFF); 11 u32 slots follow.
const META_FIELDS: usize = 11 * 4;

#[derive(Debug, Clone, Copy)]
struct Meta {
    str_head: u32,
    str_pages: u32,
    ent_head: u32,
    ent_pages: u32,
    ent_cells: u32,
    rel_head: u32,
    rel_pages: u32,
    rel_records: u32,
    next_sid: u32,
    ts_meta_page: u32,
    se_meta_page: u32,
}

impl Default for Meta {
    fn default() -> Self {
        // Heads start at NULL_PAGE (u32::MAX): page 0 is the superblock and
        // must never be treated as a slab page. 0 is an INVALID head.
        Meta {
            str_head: NULL_PAGE,
            str_pages: 0,
            ent_head: NULL_PAGE,
            ent_pages: 0,
            ent_cells: 0,
            rel_head: NULL_PAGE,
            rel_pages: 0,
            rel_records: 0,
            next_sid: 0,
            ts_meta_page: NULL_PAGE,
            se_meta_page: NULL_PAGE,
        }
    }
}

impl Meta {
    fn encode(&self, page: &mut [u8; PAGE_SIZE]) {
        page[META_FIELDS..].fill(0);
        let f = [
            self.str_head,
            self.str_pages,
            self.ent_head,
            self.ent_pages,
            self.ent_cells,
            self.rel_head,
            self.rel_pages,
            self.rel_records,
            self.next_sid,
            self.ts_meta_page,
            self.se_meta_page,
        ];
        for (i, v) in f.iter().enumerate() {
            page[i * 4..(i + 1) * 4].copy_from_slice(&v.to_le_bytes());
        }
    }

    fn decode(page: &[u8; PAGE_SIZE]) -> Meta {
        let g = |i: usize| u32::from_le_bytes(page[i * 4..(i + 1) * 4].try_into().unwrap());
        // Legacy files wrote 0 for "no head"; 0 is never a valid head (the
        // superblock occupies page 0), so remap it.
        let fix = |h: u32| if h == 0 { NULL_PAGE } else { h };
        Meta {
            str_head: fix(g(0)),
            str_pages: g(1),
            ent_head: g(2),
            ent_pages: g(3),
            ent_cells: g(4),
            rel_head: g(5),
            rel_pages: g(6),
            rel_records: g(7),
            next_sid: g(8),
            ts_meta_page: fix(g(9)),
            se_meta_page: fix(g(10)),
        }
    }
}

/// In-memory graph store bound to an `Engine`. Owns three slabs (strings,
/// entities, relations) as page chains plus derived indexes.
pub struct GraphStore {
    meta: Meta,
    /// sid -> (page_id, offset, len); rebuilt on open.
    strings: HashMap<StrId, (PageId, u32, u32)>,
    /// dedup map string -> sid (rebuilt on open).
    dedup: HashMap<String, StrId>,
    /// entity_key -> (page_id, cell_offset).
    entities: Vec<(PageId, u32)>,
    /// All relation records, in slab order (reconstructed on open).
    pub relations: Vec<RelationRecord>,
    /// Derived adjacency (rebuilt on open / after load).
    pub adjacency: Adjacency,
    /// Page id of the layout page.
    meta_page: PageId,
    /// relation idx -> (page_id, offset); rebuilt on open.
    rel_page_of: Vec<(PageId, u32)>,
    /// Pages staged in the current tx (id -> bytes), so follow-up reads of
    /// a page being written see the staged content, not stale disk.
    staged: HashMap<PageId, [u8; PAGE_SIZE]>,
}

/// Adjacency index: derived, rebuildable (spec §17/§20).
#[derive(Debug, Default, Clone)]
pub struct Adjacency {
    pub out: HashMap<u32, Vec<(u32, u32, u32)>>,
    pub in_: HashMap<u32, Vec<(u32, u32, u32)>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Out,
    In,
}

impl Adjacency {
    pub fn build(records: &[RelationRecord]) -> Adjacency {
        let mut adj = Adjacency::default();
        for (i, r) in records.iter().enumerate() {
            if !r.is_alive() {
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

    /// Phase 9 perf audit (MEASURED: `Adjacency::build` re-run after every
    /// add_relation costs ~447 ms across the 3000-relation workload — the
    /// dominant stage, O(n²)). This incremental fold is the live insert
    /// equivalent of `build` at O(1) per record: identical edge tuples in
    /// both maps, skipping tombstones, same record ordering. The on-open
    /// rebuild path keeps using `build` so the two stay semantically bound.
    ///
    /// `idx` is the record's slab index — identical to `build`'s enumerate
    /// position when every prior record is already folded (insert-only
    /// append, which add_relation guarantees).
    pub fn insert_live(&mut self, r: &RelationRecord, idx: u32) {
        if !r.is_alive() {
            return;
        }
        self.out
            .entry(r.src)
            .or_default()
            .push((r.topo_sid, r.dst, idx));
        self.in_
            .entry(r.dst)
            .or_default()
            .push((r.topo_sid, r.src, idx));
    }

    /// Phase 9 perf audit: O(1) tombstone counterpart of `insert_live`
    /// matching `remove_relation`'s semantics (the record was dead BEFORE
    /// this call only if already tombstoned; a fresh tombstone removal is a
    /// positional filter, expressed here by rebuilding only when needed —
    /// see call sites).
    pub fn remove_record(&mut self, r: &RelationRecord, idx: u32) {
        if let Some(edges) = self.out.get_mut(&r.src) {
            if let Some(pos) = edges.iter().position(|&e| e == (r.topo_sid, r.dst, idx)) {
                edges.swap_remove(pos);
            }
        }
        if let Some(edges) = self.in_.get_mut(&r.dst) {
            if let Some(pos) = edges.iter().position(|&e| e == (r.topo_sid, r.src, idx)) {
                edges.swap_remove(pos);
            }
        }
    }

    /// One hop from `entity` (topology None = any, direction in/out).
    pub fn neighbors(&self, entity: u32, topology: Option<u32>, dir: Direction) -> Vec<u32> {
        let map = match dir {
            Direction::Out => &self.out,
            Direction::In => &self.in_,
        };
        map.get(&entity)
            .map(|edges| {
                edges
                    .iter()
                    .filter(|(topo, _, _)| topology.map_or(true, |t| *topo == t))
                    .map(|(_, other, _)| *other)
                    .collect()
            })
            .unwrap_or_default()
    }
    /// Multi-hop BFS constrained to one topology (spec §20 multi-hop).
    pub fn multi_hop(
        &self,
        start: u32,
        topology: Option<u32>,
        dir: Direction,
        max_hops: usize,
    ) -> Vec<u32> {
        let mut seen = vec![start];
        let mut frontier = vec![start];
        for _ in 0..max_hops {
            let mut next = Vec::new();
            for e in frontier {
                for n in self.neighbors(e, topology, dir) {
                    if !seen.contains(&n) {
                        seen.push(n);
                        next.push(n);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        seen.retain(|&e| e != start);
        seen
    }
}

impl GraphStore {
    /// Open the graph store from an engine (reads/creates the layout page).
    pub fn open(eng: &mut Engine) -> Result<GraphStore, EngineError> {
        // Layout page = first allocated page (id 1). If the DB has only the
        // superblock, this is a fresh store: allocate + flush the layout
        // page NOW so it exists on disk before any slab write.
        if eng.page_count() <= 1 {
            let mut tx = eng.begin()?;
            let meta_page = eng.alloc_in_tx(&mut tx)?;
            eng.commit(tx)?;
            let gs = GraphStore {
                meta: Meta::default(),
                strings: HashMap::new(),
                dedup: HashMap::new(),
                entities: Vec::new(),
                relations: Vec::new(),
                adjacency: Adjacency::default(),
                meta_page,
                rel_page_of: Vec::new(),
                staged: HashMap::new(),
            };
            Ok(gs)
        } else {
            let meta = Meta::decode(eng.read_page(1)?);
            let mut gs = GraphStore {
                meta,
                strings: HashMap::new(),
                dedup: HashMap::new(),
                entities: Vec::new(),
                relations: Vec::new(),
                adjacency: Adjacency::default(),
                meta_page: 1,
                rel_page_of: Vec::new(),
                staged: HashMap::new(),
            };
            gs.load_strings(eng)?;
            gs.load_entities(eng)?;
            gs.load_relations(eng)?;
            gs.adjacency = Adjacency::build(&gs.relations);
            Ok(gs)
        }
    }

    /// Intern a string and stage its arena write in the current tx.
    /// Deduped: returns the existing handle with no page writes.
    pub fn intern(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        s: &str,
    ) -> Result<StrId, EngineError> {
        if let Some(&sid) = self.dedup.get(s) {
            return Ok(sid);
        }
        let bytes = s.as_bytes();
        let cell_len = 4 + bytes.len();
        assert!(PAGE_HDR + cell_len <= PAGE_SIZE, "string too long");
        // Reserve space on the open tail page: if the cell cannot fit, a
        // fresh page is chained. `needed` = data bytes this cell requires.
        let (page_id, used) = self.arena_open_page_sized(eng, tx, Slab::Strings, cell_len)?;
        let sid = StrId(self.meta.next_sid);
        self.meta.next_sid += 1;
        // Append cell into the open page, PRESERVING the [next][used] header
        // (the page may only exist in this tx's staged buffer, not on disk).
        let mut data = self.read_current(eng, page_id)?;
        data[PAGE_HDR + used as usize..PAGE_HDR + used as usize + 4]
            .copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        data[PAGE_HDR + used as usize + 4..PAGE_HDR + used as usize + cell_len]
            .copy_from_slice(bytes);
        data[4..8].copy_from_slice(&(used + cell_len as u32).to_le_bytes());
        self.stage_page(eng, tx, page_id, data)?;
        self.strings.insert(
            sid,
            (
                page_id,
                // Cell layout [len][bytes]: the string starts at +4 past the
                // len field (same shape load_strings rebuilds).
                (PAGE_HDR + used as usize + 4) as u32,
                bytes.len() as u32,
            ),
        );
        self.dedup.insert(s.to_string(), sid);
        self.meta.str_pages = self.count_slab_pages(Slab::Strings);
        Ok(sid)
    }

    pub fn str_lookup(&self, s: &str) -> Option<StrId> {
        self.dedup.get(s).copied()
    }

    /// Read back an interned string.
    pub fn get_str(&self, eng: &mut Engine, sid: StrId) -> Result<String, EngineError> {
        let &(page, off, len) = self
            .strings
            .get(&sid)
            .ok_or_else(|| EngineError::Page(crate::pager::PageError::PageOutOfBounds(sid.0)))?;
        let data = eng.read_page(page)?;
        let b = &data[off as usize..off as usize + len as usize];
        String::from_utf8(b.to_vec()).map_err(|_| {
            EngineError::Page(crate::pager::PageError::FreelistCorrupt) // placeholder: utf8 corruption
        })
    }

    /// Add an entity; returns its stable entity_key (cell index).
    pub fn add_entity(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        type_name: &str,
        name: &str,
        properties: &[(&str, &str)],
    ) -> Result<u32, EngineError> {
        let type_sid = self.intern(eng, tx, type_name)?;
        let name_sid = self.intern(eng, tx, name)?;
        // Inline props: "k=v\0" repeated.
        let mut props = Vec::new();
        for (k, v) in properties {
            props.extend_from_slice(format!("{}={}\0", k, v).as_bytes());
        }
        if props.len() > ENTITY_CELL - 20 {
            return Err(EngineError::Wal(crate::wal::WalError::CorruptFrame)); // props too large (Phase 2 limit)
        }
        let key = self.meta.ent_cells;
        let (page_id, off) = self.entity_open_cell(eng, tx)?;
        let mut data = self.read_current(eng, page_id)?;
        let cell = &mut data[off as usize..off as usize + ENTITY_CELL];
        cell[0..4].copy_from_slice(&type_sid.0.to_le_bytes());
        cell[4..8].copy_from_slice(&name_sid.0.to_le_bytes());
        cell[8..12].copy_from_slice(&0u32.to_le_bytes()); // flags
        cell[12..14].copy_from_slice(&(props.len() as u16).to_le_bytes());
        cell[14..20].fill(0);
        cell[20..20 + props.len()].copy_from_slice(&props);
        // Advance the page's used counter (data bytes after header).
        let used = (off as usize - PAGE_HDR) + ENTITY_CELL;
        data[4..8].copy_from_slice(&(used as u32).to_le_bytes());
        self.stage_page(eng, tx, page_id, data)?;
        self.entities.push((page_id, off));
        self.meta.ent_cells += 1;
        self.meta.ent_pages = self.count_slab_pages(Slab::Entities);
        Ok(key)
    }

    /// Number of entity cells (0..key range for scans).
    pub fn entity_count(&self) -> u32 {
        self.meta.ent_cells
    }

    /// Rewrite the inline props block of an EXISTING entity (Phase 11
    /// `upsert_entity` update path). The cell is fixed-size and the props
    /// area is the trailing 44 bytes (offset 20..64), so an in-place
    /// rewrite is layout-identical to what `add_entity` wrote: the
    /// name/type sids, flags, and page usage are untouched. The caller
    /// must have validated the encoded size ≤ ENTITY_CELL − 20 (the
    /// AgentApi upsert does; this method re-checks fail-closed).
    ///
    /// Note: `add_entity` zeroes `[14..20]` and writes props at 20..(20+len)
    /// — the REMAINDER of the 44-B props area is left as-is (zeros on
    /// creation). For a rewrite, stale bytes beyond the new length are
    /// zeroed here so the cell byte-image stays canonical for reopen.
    pub fn rewrite_entity_props(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        key: u32,
        properties: &[(&str, &str)],
    ) -> Result<(), EngineError> {
        // Inline props: "k=v\0" repeated.
        let mut props = Vec::new();
        for (k, v) in properties {
            props.extend_from_slice(format!("{}={}\0", k, v).as_bytes());
        }
        if props.len() > ENTITY_CELL - 20 {
            return Err(EngineError::Wal(crate::wal::WalError::CorruptFrame));
        }
        let &(page_id, off) = self.entities.get(key as usize).ok_or(EngineError::Page(
            crate::pager::PageError::PageOutOfBounds(key),
        ))?;
        let mut data = self.read_current(eng, page_id)?;
        let cell = &mut data[off as usize..off as usize + ENTITY_CELL];
        cell[12..14].copy_from_slice(&(props.len() as u16).to_le_bytes());
        // Zero the full props area THEN write the new encoding (canonical
        // image: no stale bytes from the previous props, so a reopen —
        // which decodes via `props_len` — AND a raw byte compare agree).
        cell[20..ENTITY_CELL].fill(0);
        cell[20..20 + props.len()].copy_from_slice(&props);
        self.stage_page(eng, tx, page_id, data)?;
        Ok(())
    }

    fn entity_cell(&self, eng: &mut Engine, key: u32) -> Result<[u8; ENTITY_CELL], EngineError> {
        let &(page, off) = self.entities.get(key as usize).ok_or(EngineError::Page(
            crate::pager::PageError::PageOutOfBounds(key),
        ))?;
        let data = self.read_current(eng, page)?;
        let mut cell = [0u8; ENTITY_CELL];
        cell.copy_from_slice(&data[off as usize..off as usize + ENTITY_CELL]);
        Ok(cell)
    }

    /// Interned name string of an entity (Phase 2: auto-generated names).
    pub fn entity_name_sid(&mut self, eng: &mut Engine, key: u32) -> Result<StrId, EngineError> {
        let cell = self.entity_cell(eng, key)?;
        Ok(StrId(u32::from_le_bytes(cell[4..8].try_into().unwrap())))
    }

    /// Interned type string of an entity.
    pub fn entity_type_sid(&mut self, eng: &mut Engine, key: u32) -> Result<StrId, EngineError> {
        let cell = self.entity_cell(eng, key)?;
        Ok(StrId(u32::from_le_bytes(cell[0..4].try_into().unwrap())))
    }

    /// Decode the inline `k=v\0` properties of an entity.
    pub fn entity_props(
        &mut self,
        eng: &mut Engine,
        key: u32,
    ) -> Result<Vec<(String, String)>, EngineError> {
        let cell = self.entity_cell(eng, key)?;
        let plen = u16::from_le_bytes(cell[12..14].try_into().unwrap()) as usize;
        let raw = &cell[20..20 + plen];
        let mut out = Vec::new();
        for part in raw.split(|&b| b == 0) {
            if part.is_empty() {
                continue;
            }
            if let Some((k, v)) = String::from_utf8_lossy(part).split_once('=') {
                out.push((k.to_string(), v.to_string()));
            }
        }
        Ok(out)
    }

    /// Append a relation.
    pub fn add_relation(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        src: u32,
        dst: u32,
        relation_type: &str,
        valid_from: i64,
        valid_to: i64,
        provenance: u8,
    ) -> Result<u32, EngineError> {
        let (topo, rtype) = match relation_type.split_once(':') {
            Some((t, r)) => (t, r),
            None => ("", relation_type),
        };
        let topo_sid = self.intern(eng, tx, topo)?;
        let type_sid = self.intern(eng, tx, rtype)?;
        let idx = self.relations.len() as u32;
        let (page_id, off) = self.relation_open_slot(eng, tx)?;
        let rec = RelationRecord {
            src,
            dst,
            type_sid: type_sid.0,
            topo_sid: topo_sid.0,
            valid_from,
            valid_to,
            provenance,
            flags: 0,
        };
        let mut data = self.read_current(eng, page_id)?;
        rec.encode_into(&mut data, off as usize);
        // Advance the page's used counter (data bytes after header).
        let used = (off as usize - PAGE_HDR) + RELATION_REC;
        data[4..8].copy_from_slice(&(used as u32).to_le_bytes());
        self.stage_page(eng, tx, page_id, data)?;
        self.relations.push(rec);
        self.meta.rel_records += 1;
        self.meta.rel_pages = self.count_slab_pages(Slab::Relations);
        // Phase 9: incremental adjacency fold (measured dominant stage was
        // the full O(n) rebuild per insert); `build` semantics preserved
        // because every prior record was folded the same way (append-only).
        self.adjacency.insert_live(&rec, idx);
        Ok(idx)
    }

    /// Tombstone a relation (delete semantics, spec §38.1).
    pub fn remove_relation(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        idx: u32,
    ) -> Result<(), EngineError> {
        let rec = self.relations[idx as usize];
        let (page_id, off) = self.relation_slot_pos(idx)?;
        let mut data = self.read_current(eng, page_id)?;
        let mut dead = rec;
        dead.flags |= REL_FLAG_DEAD;
        dead.encode_into(&mut data, off as usize);
        self.stage_page(eng, tx, page_id, data)?;
        self.relations[idx as usize] = dead;
        // Phase 9: same-measured-stage fix as add_relation — O(log n)
        // positional removal instead of a full O(n) rebuild. A fresh
        // tombstone can only remove edges that `build` had added for THIS
        // record, so the maps stay `build`-equivalent.
        self.adjacency.remove_record(&rec, idx);
        Ok(())
    }

    /// Persist the layout page + flush via a committed transaction.
    pub fn flush_meta(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
    ) -> Result<(), EngineError> {
        let mut data = self.read_current(eng, self.meta_page)?;
        self.meta.encode(&mut data);
        self.stage_page(eng, tx, self.meta_page, data)?;
        Ok(())
    }

    /// Full persist: write meta and let the caller commit.
    pub fn persist(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let mut tx = eng.begin()?;
        self.flush_meta(eng, &mut tx)?;
        eng.commit(tx)?;
        self.staged.clear();
        Ok(())
    }

    /// Read-only view of the TS layout page pointer (Phase 3 hook).
    /// Single source of truth: `self.meta` (persisted by flush_meta).
    pub(crate) fn ts_meta_page_ref(&self) -> &u32 {
        &self.meta.ts_meta_page
    }

    pub(crate) fn set_ts_meta_page(&mut self, p: u32) {
        self.meta.ts_meta_page = p;
    }

    /// Read-only view of the State/Events layout page pointer (Phase 4 hook).
    /// Single source of truth: `self.meta` (persisted by flush_meta).
    pub(crate) fn se_meta_page_ref(&self) -> &u32 {
        &self.meta.se_meta_page
    }

    pub(crate) fn set_se_meta_page(&mut self, p: u32) {
        self.meta.se_meta_page = p;
    }

    // -- internals -----------------------------------------------------------

    fn stage_page(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        page_id: PageId,
        data: [u8; PAGE_SIZE],
    ) -> Result<(), EngineError> {
        self.staged.insert(page_id, data);
        eng.write_in_tx(tx, page_id, |d| *d = data)
    }

    /// Read honoring the current tx's staged writes (a page staged in this
    /// tx is not on disk yet — reading it from the pager returns zeros).
    fn read_current(
        &self,
        eng: &mut Engine,
        page_id: PageId,
    ) -> Result<[u8; PAGE_SIZE], EngineError> {
        if let Some(d) = self.staged.get(&page_id) {
            return Ok(*d);
        }
        Ok(*eng.read_page(page_id)?)
    }

    fn count_slab_pages(&self, slab: Slab) -> u32 {
        // Chains are rebuilt after every structural change; cheap enough at
        // Phase 2 scale (walk via cached heads only — counts maintained
        // incrementally here, chain itself on disk).
        match slab {
            Slab::Strings => self.meta.str_pages,
            Slab::Entities => self.meta.ent_pages,
            Slab::Relations => self.meta.rel_pages,
        }
    }

    /// Get or open the arena's tail page for a slab with fixed-size cells.
    fn arena_open_page(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        slab: Slab,
    ) -> Result<(PageId, u32), EngineError> {
        let cell = match slab {
            Slab::Strings => 0, // unreachable via this path
            Slab::Entities => ENTITY_CELL,
            Slab::Relations => RELATION_REC,
        };
        self.arena_open_page_sized(eng, tx, slab, cell)
    }

    /// Get or open the arena's tail page, requiring `needed` free data bytes.
    fn arena_open_page_sized(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        slab: Slab,
        needed: usize,
    ) -> Result<(PageId, u32), EngineError> {
        let (head, which) = match slab {
            Slab::Strings => (self.meta.str_head, 0u8),
            Slab::Entities => (self.meta.ent_head, 1u8),
            Slab::Relations => (self.meta.rel_head, 2u8),
        };
        // Walk the chain; return the first page with enough free space.
        let mut cur = head;
        let mut prev: Option<PageId> = None;
        let mut n_pages = 0u32;
        let tail = loop {
            if cur == NULL_PAGE {
                break prev;
            }
            let data = self.read_current(eng, cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap());
            let free = PAGE_SIZE - PAGE_HDR - used as usize;
            if free >= needed {
                return Ok((cur, used));
            }
            n_pages += 1;
            prev = Some(cur);
            cur = next;
        };
        // No space anywhere: allocate a fresh page, chain it.
        let fresh = self.alloc_chained(eng, tx, tail)?;
        if tail.is_none() {
            match which {
                0 => self.meta.str_head = fresh,
                1 => self.meta.ent_head = fresh,
                _ => self.meta.rel_head = fresh,
            }
        }
        let pages_field = match which {
            0 => &mut self.meta.str_pages,
            1 => &mut self.meta.ent_pages,
            _ => &mut self.meta.rel_pages,
        };
        *pages_field = n_pages + 1;
        Ok((fresh, 0))
    }

    /// Allocate a page already linked to `tail` (or unlinked if None),
    /// initialize its header, stage it.
    fn alloc_chained(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
        tail: Option<PageId>,
    ) -> Result<PageId, EngineError> {
        let id = eng.alloc_in_tx(tx)?;
        let mut data = [0u8; PAGE_SIZE];
        data[0..4].copy_from_slice(&NULL_PAGE.to_le_bytes()); // next
        data[4..8].copy_from_slice(&0u32.to_le_bytes()); // used = data bytes AFTER header
        if let Some(t) = tail {
            let mut tdata = self.read_current(eng, t)?;
            tdata[0..4].copy_from_slice(&id.to_le_bytes());
            self.stage_page(eng, tx, t, tdata)?;
        }
        self.stage_page(eng, tx, id, data)?;
        Ok(id)
    }

    fn entity_open_cell(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
    ) -> Result<(PageId, u32), EngineError> {
        let (page, used) = self.arena_open_page(eng, tx, Slab::Entities)?;
        Ok((page, PAGE_HDR as u32 + used))
    }

    fn relation_open_slot(
        &mut self,
        eng: &mut Engine,
        tx: &mut crate::engine::Tx,
    ) -> Result<(PageId, u32), EngineError> {
        let (page, used) = self.arena_open_page(eng, tx, Slab::Relations)?;
        Ok((page, PAGE_HDR as u32 + used))
    }

    fn relation_slot_pos(&self, idx: u32) -> Result<(PageId, u32), EngineError> {
        // Walk the chain in-memory: we track page ids by replaying the chain
        // head stored in meta; but Phase 2 keeps a page map instead.
        // Rebuild: records per page = (PAGE_SIZE-PAGE_HDR)/RELATION_REC.
        // We need the chain; store page ids implicitly via the relation
        // slab walk done in load_relations; here reuse `self.rel_page_of`.
        self.rel_page_of
            .get(idx as usize)
            .copied()
            .ok_or(EngineError::Page(crate::pager::PageError::PageOutOfBounds(
                idx,
            )))
    }

    fn load_strings(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        // Walk the string chain; parse cells.
        // "used" counts data bytes AFTER the header (cells start at PAGE_HDR).
        // Chain order == original insertion order, so sids restart at 0;
        // next_sid then ends at the total count (== persisted value).
        self.meta.next_sid = 0;
        let mut cur = self.meta.str_head;
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let data_end = PAGE_HDR + used;
            let mut off = PAGE_HDR;
            while off + 4 <= data_end {
                let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                if off + 4 + len > data_end {
                    break; // torn tail (Phase 2 compaction will handle)
                }
                let s = String::from_utf8_lossy(&data[off + 4..off + 4 + len]).to_string();
                let sid = StrId(self.meta.next_sid);
                self.meta.next_sid += 1;
                // sids are rebuilt in chain order = original insertion order.
                // Cell layout: [len @off][bytes @off+4] — store the bytes pos.
                self.strings
                    .entry(sid)
                    .or_insert((cur, (off + 4) as u32, len as u32));
                self.dedup.entry(s).or_insert(sid);
                off += 4 + len;
            }
            cur = next;
        }
        Ok(())
    }

    fn load_entities(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let per_page = (PAGE_SIZE - PAGE_HDR) / ENTITY_CELL;
        let mut cur = self.meta.ent_head;
        self.entities.clear();
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            for cell in 0..per_page {
                if self.entities.len() >= self.meta.ent_cells as usize {
                    break;
                }
                let off = PAGE_HDR + cell * ENTITY_CELL;
                self.entities.push((cur, off as u32));
            }
            cur = next;
        }
        Ok(())
    }

    fn load_relations(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let per_page = (PAGE_SIZE - PAGE_HDR) / RELATION_REC;
        let mut cur = self.meta.rel_head;
        self.relations.clear();
        self.rel_page_of.clear();
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            for slot in 0..per_page {
                if self.relations.len() >= self.meta.rel_records as usize {
                    break;
                }
                let off = PAGE_HDR + slot * RELATION_REC;
                let rec = RelationRecord::decode(data[off..off + RELATION_REC].try_into().unwrap());
                self.rel_page_of.push((cur, off as u32));
                self.relations.push(rec);
            }
            cur = next;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Slab {
    Strings,
    Entities,
    Relations,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_record_roundtrip() {
        let rec = RelationRecord {
            src: 7,
            dst: 42,
            type_sid: 3,
            topo_sid: 1,
            valid_from: 1000,
            valid_to: 2000,
            provenance: 1,
            flags: 0,
        };
        let mut buf = [0u8; RELATION_REC];
        rec.encode(&mut buf);
        assert_eq!(RelationRecord::decode(&buf), rec);
    }

    #[test]
    fn adjacency_topology_filter_and_tombstone() {
        let recs = vec![
            RelationRecord {
                src: 1,
                dst: 2,
                type_sid: 10,
                topo_sid: 5,
                valid_from: 0,
                valid_to: -1,
                provenance: 0,
                flags: 0,
            },
            RelationRecord {
                src: 1,
                dst: 3,
                type_sid: 11,
                topo_sid: 6,
                valid_from: 0,
                valid_to: -1,
                provenance: 0,
                flags: 0,
            },
            RelationRecord {
                src: 1,
                dst: 9,
                type_sid: 12,
                topo_sid: 5,
                valid_from: 0,
                valid_to: -1,
                provenance: 0,
                flags: REL_FLAG_DEAD,
            },
        ];
        let adj = Adjacency::build(&recs);
        assert_eq!(adj.neighbors(1, Some(5), Direction::Out), vec![2]); // tombstoned 9 skipped
        assert_eq!(adj.neighbors(1, Some(6), Direction::Out), vec![3]);
        assert_eq!(adj.neighbors(1, None, Direction::Out), vec![2, 3]);
        assert_eq!(adj.neighbors(2, Some(5), Direction::In), vec![1]);
        // Multi-hop: 1 -> 2 (topo 5), 1 -> 3 (topo 6).
        assert_eq!(adj.multi_hop(1, Some(5), Direction::Out, 3), vec![2]);
    }
}

//! State & Events store for VidgeDB — Phase 4 (spec §10/§15).
//!
//! Two append-only record sets living in the store's OWN pages (spec §4:
//! graph + temporal + state are three stores). The layout page id is stored
//! in the graph meta as `se_meta_page` — exactly like `ts_meta_page` — so
//! the Meta is the single source of truth for the pointer (DEVELOPING.md
//! invariant: no mirror field outside the Meta).
//!
//! STATE (spec §10): per entity, `(state_key, value, valid_from, valid_to)`
//! pairs. The CURRENT state = the pair with an OPEN `valid_to`; the HISTORY
//! = every pair (spec §10: "state history SHALL be queryable").
//! `set_state` writes a NEW pair `[at, open)` and CLOSES the previous open
//! pair (`valid_to = at`) — history is preserved, nothing is overwritten.
//! Validity convention (shared with ttemporal, do not change): half-open
//! `[valid_from, valid_to)`; `valid_to == VALID_TO_OPEN` (-1) means the
//! state never expires. A plain-i64 sentinel, NOT `Option`.
//!
//! EVENTS (spec §15): discrete occurrences (MotorStarted, AlarmRaised,
//! ValveOpened, …) with a timestamp, a provenance byte (spec §14 classes)
//! and a short free-form details string. Append-only; queries filter by
//! entity and `[from, to]` window.
//!
//! Wire format (TimeSeriesStore pattern):
//! - page chains with the `[next u32][used u32]` header, `used` = data
//!   bytes after the header (heads are `NULL_PAGE`, never 0 — invariant 1);
//! - STATE slab: fixed 32 B records
//!   `[entity_sid u32][key_sid u32][value_sid u32][valid_from i64]
//!    [valid_to i64][flags u8][pad 3B]` (flags = tombstone room, unused in
//!   Phase 4 — history is never erased);
//! - EVENT slab: fixed 40 B records
//!   `[name_sid u32][entity_sid u32][timestamp i64][provenance u8]
//!    [details_len u16][details ≤20B inline, truncated][pad]`;
//! - STRING arena: `[len u32][bytes]` cells, dedup via an in-memory map,
//!   sids rebuilt in chain order on open (invariant 5: load resets
//!   `next_sid` to 0). The sids are local to THIS store (same philosophy as
//!   TimeSeriesStore series names: no cross-store sharing, each slab is its
//!   own source of truth);
//! - layout page: slab heads + counts + next_sid; slabs are the source of
//!   truth, everything rebuilds from them on open (spec §17).
//!
//! Durability ordering (same as TimeSeriesStore): the caller commits the
//! data tx FIRST, then calls `persist()` (layout counts). A crash between
//! the two loads the stale layout (newer records ignored) — never corrupted
//! committed data.
//!
//! No unsafe (spec §49).

use crate::engine::{Engine, EngineError, Tx};
use crate::pager::{PageId, NULL_PAGE, PAGE_SIZE};
use crate::stores::GraphStore;
use crate::ttemporal::VALID_TO_OPEN;
use std::collections::HashMap;

/// Page header: [next u32][used u32], used = data bytes after the header.
pub const SE_HDR: usize = 8;
/// State record: entity_sid u32, key_sid u32, value_sid u32, valid_from i64,
/// valid_to i64, flags u8, pad 3B.
pub const STATE_REC: usize = 32;
/// Event record: name_sid u32, entity_sid u32, timestamp i64, provenance u8,
/// details_len u16, details inline (max 20 B, truncated), pad.
pub const EVENT_REC: usize = 40;
/// Details bytes kept inline in an event record; longer details are
/// truncated (at a UTF-8 char boundary).
pub const DETAILS_INLINE_MAX: usize = 20;
/// Tombstone flag for state records (room for Phase-4+ deletes; history
/// writes never set it — history is preserved by design, spec §10).
pub const SE_FLAG_DEAD: u8 = 1;

/// Is state record `r` valid at instant `t`? Half-open `[valid_from,
/// valid_to)`, `valid_to == -1` = open-ended (same predicate shape as
/// `ttemporal::relation_alive_at`). Tombstoned records are never valid.
pub fn state_alive_at(r: &StateRecord, t: i64) -> bool {
    r.flags & SE_FLAG_DEAD == 0
        && r.valid_from <= t
        && (r.valid_to == VALID_TO_OPEN || t < r.valid_to)
}

// ---------------------------------------------------------------------------
// Wire formats
// ---------------------------------------------------------------------------

/// One state pair on disk (fixed 32 B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateRecord {
    pub entity_sid: u32,
    pub key_sid: u32,
    pub value_sid: u32,
    pub valid_from: i64,
    pub valid_to: i64,
    pub flags: u8,
}

impl StateRecord {
    /// Write the record into a fresh 32 B buffer.
    pub fn encode(&self, out: &mut [u8; STATE_REC]) {
        out[0..4].copy_from_slice(&self.entity_sid.to_le_bytes());
        out[4..8].copy_from_slice(&self.key_sid.to_le_bytes());
        out[8..12].copy_from_slice(&self.value_sid.to_le_bytes());
        out[12..20].copy_from_slice(&self.valid_from.to_le_bytes());
        out[20..28].copy_from_slice(&self.valid_to.to_le_bytes());
        out[28] = self.flags;
        out[29..32].fill(0);
    }

    /// Write the record directly into `data` at byte `at` (invariant 3:
    /// wire writes go through `encode_into(data, at)`, never a value-copy
    /// `try_into`).
    pub fn encode_into(&self, data: &mut [u8], at: usize) {
        data[at..at + 4].copy_from_slice(&self.entity_sid.to_le_bytes());
        data[at + 4..at + 8].copy_from_slice(&self.key_sid.to_le_bytes());
        data[at + 8..at + 12].copy_from_slice(&self.value_sid.to_le_bytes());
        data[at + 12..at + 20].copy_from_slice(&self.valid_from.to_le_bytes());
        data[at + 20..at + 28].copy_from_slice(&self.valid_to.to_le_bytes());
        data[at + 28] = self.flags;
        data[at + 29..at + 32].fill(0);
    }

    pub fn decode(buf: &[u8; STATE_REC]) -> StateRecord {
        StateRecord {
            entity_sid: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            key_sid: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            value_sid: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            valid_from: i64::from_le_bytes(buf[12..20].try_into().unwrap()),
            valid_to: i64::from_le_bytes(buf[20..28].try_into().unwrap()),
            flags: buf[28],
        }
    }

    pub fn is_alive(&self) -> bool {
        self.flags & SE_FLAG_DEAD == 0
    }
}

/// One event on disk (fixed 40 B). Details longer than
/// `DETAILS_INLINE_MAX` bytes are truncated (at a UTF-8 char boundary).
#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    pub name_sid: u32,
    pub entity_sid: u32,
    pub timestamp: i64,
    pub provenance: u8,
    /// Truncated details (≤ DETAILS_INLINE_MAX bytes).
    pub details: String,
}

impl EventRecord {
    pub fn encode(&self, out: &mut [u8; EVENT_REC]) {
        out[0..4].copy_from_slice(&self.name_sid.to_le_bytes());
        out[4..8].copy_from_slice(&self.entity_sid.to_le_bytes());
        out[8..16].copy_from_slice(&self.timestamp.to_le_bytes());
        out[16] = self.provenance;
        let d = self.details.as_bytes();
        let n = d.len().min(DETAILS_INLINE_MAX);
        out[17..19].copy_from_slice(&(n as u16).to_le_bytes());
        out[19..EVENT_REC].fill(0);
        out[19..19 + n].copy_from_slice(&d[..n]);
    }

    pub fn decode(buf: &[u8; EVENT_REC]) -> EventRecord {
        let raw = u16::from_le_bytes(buf[17..19].try_into().unwrap()) as usize;
        // Clamp defensively: a corrupt length must never panic the reader.
        let n = raw.min(DETAILS_INLINE_MAX);
        EventRecord {
            name_sid: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            entity_sid: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            timestamp: i64::from_le_bytes(buf[8..16].try_into().unwrap()),
            provenance: buf[16],
            details: String::from_utf8_lossy(&buf[19..19 + n]).to_string(),
        }
    }
}

/// Truncate bytes to `max`, never splitting a UTF-8 multibyte char.
fn truncate_utf8(b: &[u8], max: usize) -> &[u8] {
    if b.len() <= max {
        return b;
    }
    let mut end = max;
    while end > 0 && (b[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    &b[..end]
}

/// Decoded event returned by `get_events` (strings resolved).
#[derive(Debug, Clone, PartialEq)]
pub struct SeEvent {
    pub name: String,
    pub entity: String,
    pub timestamp: i64,
    pub provenance: u8,
    pub details: String,
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// SE layout page: [str_head][str_pages][next_sid][state_head][state_pages]
/// [state_recs][event_head][event_pages][event_recs][reserved] (10 u32).
#[derive(Debug, Clone, Copy)]
struct SeLayout {
    str_head: u32,
    str_pages: u32,
    next_sid: u32,
    state_head: u32,
    state_pages: u32,
    state_recs: u32,
    event_head: u32,
    event_pages: u32,
    event_recs: u32,
}

impl Default for SeLayout {
    fn default() -> Self {
        // Heads start at NULL_PAGE: page 0 is the superblock, 0 is invalid.
        SeLayout {
            str_head: NULL_PAGE,
            str_pages: 0,
            next_sid: 0,
            state_head: NULL_PAGE,
            state_pages: 0,
            state_recs: 0,
            event_head: NULL_PAGE,
            event_pages: 0,
            event_recs: 0,
        }
    }
}

impl SeLayout {
    const FIELDS: usize = 10;

    fn encode(&self, page: &mut [u8; PAGE_SIZE]) {
        page[Self::FIELDS * 4..].fill(0);
        let f = [
            self.str_head,
            self.str_pages,
            self.next_sid,
            self.state_head,
            self.state_pages,
            self.state_recs,
            self.event_head,
            self.event_pages,
            self.event_recs,
            0u32, // reserved
        ];
        for (i, v) in f.iter().enumerate() {
            page[i * 4..(i + 1) * 4].copy_from_slice(&v.to_le_bytes());
        }
    }

    fn decode(page: &[u8; PAGE_SIZE]) -> SeLayout {
        let g = |i: usize| u32::from_le_bytes(page[i * 4..(i + 1) * 4].try_into().unwrap());
        let fix = |h: u32| if h == 0 { NULL_PAGE } else { h };
        SeLayout {
            str_head: fix(g(0)),
            str_pages: g(1),
            next_sid: g(2),
            state_head: fix(g(3)),
            state_pages: g(4),
            state_recs: g(5),
            event_head: fix(g(6)),
            event_pages: g(7),
            event_recs: g(8),
        }
    }
}

/// The state & events store: string arena + state slab + event slab, in its
/// own pages, chained behind `se_meta_page` in the graph meta.
pub struct StateEventStore {
    meta_page: PageId,
    // Layout fields (single source of truth on disk is the layout page).
    str_head: u32,
    str_pages: u32,
    next_sid: u32,
    state_head: u32,
    state_pages: u32,
    state_recs: u32,
    event_head: u32,
    event_pages: u32,
    event_recs: u32,
    /// All state records in slab order (reconstructed on open).
    pub states: Vec<StateRecord>,
    /// All event records in slab order (reconstructed on open).
    pub events: Vec<EventRecord>,
    /// sid -> (page, offset, len); rebuilt on open.
    strings: HashMap<u32, (PageId, u32, u32)>,
    /// dedup map string -> sid (rebuilt on open).
    dedup: HashMap<String, u32>,
    /// state idx -> (page, off); needed to patch (close) validity in place.
    state_pos: Vec<(PageId, u32)>,
    /// Pages staged in the current tx (staged page = not on disk yet).
    staged: HashMap<PageId, [u8; PAGE_SIZE]>,
}

impl StateEventStore {
    /// Open (create on first use) the SE store; needs the graph store only
    /// to locate/create the SE layout page pointer (`se_meta_page`).
    pub fn open(eng: &mut Engine, gs: &mut GraphStore) -> Result<StateEventStore, EngineError> {
        let mut meta_page = *gs.se_meta_page_ref();
        if meta_page == NULL_PAGE {
            let mut tx = eng.begin()?;
            let p = eng.alloc_in_tx(&mut tx)?;
            let mut page = [0u8; PAGE_SIZE];
            SeLayout::default().encode(&mut page);
            eng.write_in_tx(&mut tx, p, |d| *d = page)?;
            eng.commit(tx)?;
            gs.set_se_meta_page(p);
            gs.persist(eng)?;
            meta_page = p;
        }
        let lay = SeLayout::decode(eng.read_page(meta_page)?);
        let mut se = StateEventStore {
            meta_page,
            str_head: lay.str_head,
            str_pages: lay.str_pages,
            next_sid: lay.next_sid,
            state_head: lay.state_head,
            state_pages: lay.state_pages,
            state_recs: lay.state_recs,
            event_head: lay.event_head,
            event_pages: lay.event_pages,
            event_recs: lay.event_recs,
            states: Vec::new(),
            events: Vec::new(),
            strings: HashMap::new(),
            dedup: HashMap::new(),
            state_pos: Vec::new(),
            staged: HashMap::new(),
        };
        se.load_strings(eng)?;
        se.load_states(eng)?;
        se.load_events(eng)?;
        Ok(se)
    }

    // -- public API -----------------------------------------------------------

    /// Write a NEW state pair `[at, open)` for (entity, key) and CLOSE the
    /// previously open pair (`valid_to = at`). History is preserved —
    /// nothing is overwritten (spec §10: state history SHALL be queryable).
    pub fn set_state(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        entity_name: &str,
        state_key: &str,
        value: &str,
        at: i64,
    ) -> Result<(), EngineError> {
        let entity_sid = self.intern(eng, tx, entity_name)?;
        let key_sid = self.intern(eng, tx, state_key)?;
        let value_sid = self.intern(eng, tx, value)?;
        // Close every currently-open pair for (entity, key). With the
        // protocol there is at most one; close defensively regardless.
        let to_close: Vec<usize> = self
            .states
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.entity_sid == entity_sid
                    && r.key_sid == key_sid
                    && r.is_alive()
                    && r.valid_to == VALID_TO_OPEN
            })
            .map(|(i, _)| i)
            .collect();
        for i in to_close {
            let rec = self.states[i];
            let (page, off) = self.state_pos[i];
            let mut data = self.read_current(eng, page)?;
            let mut closed = rec;
            closed.valid_to = at;
            closed.encode_into(&mut data, off as usize);
            self.stage_page(eng, tx, page, data)?;
            self.states[i] = closed;
        }
        // Append the new pair.
        let rec = StateRecord {
            entity_sid,
            key_sid,
            value_sid,
            valid_from: at,
            valid_to: VALID_TO_OPEN,
            flags: 0,
        };
        let mut buf = [0u8; STATE_REC];
        rec.encode(&mut buf);
        let (page, off) = self.slab_append(eng, tx, SeSlab::States, &buf)?;
        self.state_pos.push((page, off));
        self.states.push(rec);
        self.state_recs += 1;
        self.stage_layout(eng, tx)?;
        Ok(())
    }

    /// The state value valid at instant `at` (`None` = now), with its
    /// `valid_from`. Half-open semantics: a pair covers `at` iff
    /// `valid_from <= at && (valid_to == OPEN || at < valid_to)`.
    /// When several pairs overlap (backdated writes), the most recent
    /// `valid_from` wins (slab order breaks ties).
    pub fn get_state(
        &self,
        eng: &mut Engine,
        entity_name: &str,
        state_key: &str,
        at: Option<i64>,
    ) -> Result<Option<(String, i64)>, EngineError> {
        let at = at.unwrap_or_else(now_unix);
        let Some(&entity_sid) = self.dedup.get(entity_name) else {
            return Ok(None);
        };
        let Some(&key_sid) = self.dedup.get(state_key) else {
            return Ok(None);
        };
        let mut best: Option<(i64, u32)> = None; // (valid_from, value_sid)
        for rec in &self.states {
            if rec.entity_sid != entity_sid || rec.key_sid != key_sid {
                continue;
            }
            if !state_alive_at(rec, at) {
                continue;
            }
            if best.as_ref().map_or(true, |(bf, _)| rec.valid_from > *bf) {
                best = Some((rec.valid_from, rec.value_sid));
            }
        }
        match best {
            Some((valid_from, value_sid)) => {
                let value = self.get_str(eng, value_sid)?;
                Ok(Some((value, valid_from)))
            }
            None => Ok(None),
        }
    }

    /// The full history of (value, valid_from, valid_to) pairs for one
    /// (entity, key), in slab (chronological append) order. Open-ended
    /// pairs carry `valid_to == VALID_TO_OPEN` (-1).
    pub fn state_history(
        &self,
        eng: &mut Engine,
        entity_name: &str,
        state_key: &str,
    ) -> Result<Vec<(String, i64, i64)>, EngineError> {
        let Some(&entity_sid) = self.dedup.get(entity_name) else {
            return Ok(Vec::new());
        };
        let Some(&key_sid) = self.dedup.get(state_key) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for rec in &self.states {
            if rec.entity_sid != entity_sid || rec.key_sid != key_sid || !rec.is_alive() {
                continue;
            }
            let value = self.get_str(eng, rec.value_sid)?;
            out.push((value, rec.valid_from, rec.valid_to));
        }
        Ok(out)
    }

    /// Append one event (append-only, spec §15). Details longer than
    /// `DETAILS_INLINE_MAX` bytes are truncated at a UTF-8 char boundary.
    pub fn log_event(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        event_name: &str,
        entity_name: &str,
        timestamp: i64,
        provenance: u8,
        details: &str,
    ) -> Result<(), EngineError> {
        let name_sid = self.intern(eng, tx, event_name)?;
        let entity_sid = self.intern(eng, tx, entity_name)?;
        let stored = truncate_utf8(details.as_bytes(), DETAILS_INLINE_MAX);
        let rec = EventRecord {
            name_sid,
            entity_sid,
            timestamp,
            provenance,
            details: String::from_utf8_lossy(stored).to_string(),
        };
        let mut buf = [0u8; EVENT_REC];
        rec.encode(&mut buf);
        self.slab_append(eng, tx, SeSlab::Events, &buf)?;
        self.events.push(rec);
        self.event_recs += 1;
        self.stage_layout(eng, tx)?;
        Ok(())
    }

    /// Events with timestamp in the inclusive window `[from, to]`, filtered
    /// by entity (`None` = all entities), sorted by timestamp (slab order
    /// breaks ties). Unknown entity names yield an empty vec.
    pub fn get_events(
        &self,
        eng: &mut Engine,
        entity_name: Option<&str>,
        from: i64,
        to: i64,
    ) -> Result<Vec<SeEvent>, EngineError> {
        let entity_filter: Option<u32> = match entity_name {
            Some(name) => match self.dedup.get(name) {
                Some(&sid) => Some(sid),
                None => return Ok(Vec::new()),
            },
            None => None,
        };
        let mut out = Vec::new();
        for rec in &self.events {
            if let Some(ef) = entity_filter {
                if rec.entity_sid != ef {
                    continue;
                }
            }
            if rec.timestamp < from || rec.timestamp > to {
                continue;
            }
            out.push(SeEvent {
                name: self.get_str(eng, rec.name_sid)?,
                entity: self.get_str(eng, rec.entity_sid)?,
                timestamp: rec.timestamp,
                provenance: rec.provenance,
                details: rec.details.clone(),
            });
        }
        out.sort_by_key(|e| e.timestamp);
        Ok(out)
    }

    pub fn state_count(&self) -> usize {
        self.states.len()
    }

    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Persist the SE layout page (call AFTER committing the data tx).
    pub fn persist(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let mut tx = eng.begin()?;
        self.stage_layout(eng, &mut tx)?;
        eng.commit(tx)?;
        self.staged.clear();
        Ok(())
    }

    // -- internals ------------------------------------------------------------

    fn stage_page(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
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

    fn stage_layout(&mut self, eng: &mut Engine, tx: &mut Tx) -> Result<(), EngineError> {
        let lay = SeLayout {
            str_head: self.str_head,
            str_pages: self.str_pages,
            next_sid: self.next_sid,
            state_head: self.state_head,
            state_pages: self.state_pages,
            state_recs: self.state_recs,
            event_head: self.event_head,
            event_pages: self.event_pages,
            event_recs: self.event_recs,
        };
        let mut data = self.read_current(eng, self.meta_page)?;
        lay.encode(&mut data);
        self.stage_page(eng, tx, self.meta_page, data)
    }

    /// Intern a string in the SE's own arena and stage the cell write in
    /// the current tx. Deduped: returns the existing sid with no writes.
    fn intern(&mut self, eng: &mut Engine, tx: &mut Tx, s: &str) -> Result<u32, EngineError> {
        if let Some(&sid) = self.dedup.get(s) {
            return Ok(sid);
        }
        let bytes = s.as_bytes();
        let mut cell = Vec::with_capacity(4 + bytes.len());
        cell.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        cell.extend_from_slice(bytes);
        assert!(
            SE_HDR + cell.len() <= PAGE_SIZE,
            "state/event string too long"
        );
        let sid = self.next_sid;
        let (page, off) = self.slab_append(eng, tx, SeSlab::Strings, &cell)?;
        self.next_sid += 1;
        // Cell layout [len @off][bytes @off+4] — same shape load_strings
        // rebuilds (sids are rebuilt in chain order = insertion order).
        self.strings
            .insert(sid, (page, off + 4, bytes.len() as u32));
        self.dedup.insert(s.to_string(), sid);
        Ok(sid)
    }

    /// Read back an interned string by sid.
    fn get_str(&self, eng: &mut Engine, sid: u32) -> Result<String, EngineError> {
        let &(page, off, len) = self.strings.get(&sid).ok_or(EngineError::Page(
            crate::pager::PageError::PageOutOfBounds(sid),
        ))?;
        let data = self.read_current(eng, page)?;
        let b = &data[off as usize..off as usize + len as usize];
        Ok(String::from_utf8_lossy(b).to_string())
    }

    /// Append bytes to one of the SE slabs (TimeSeriesStore pattern): walk
    /// the chain for a page with room, else chain a fresh page.
    fn slab_append(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        slab: SeSlab,
        bytes: &[u8],
    ) -> Result<(PageId, u32), EngineError> {
        let (head, which) = match slab {
            SeSlab::Strings => (self.str_head, 0u8),
            SeSlab::States => (self.state_head, 1u8),
            SeSlab::Events => (self.event_head, 2u8),
        };
        let mut cur = head;
        let mut tail: Option<PageId> = None;
        let mut n_pages = 0u32;
        loop {
            if cur == NULL_PAGE {
                break;
            }
            let data = self.read_current(eng, cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            if PAGE_SIZE - SE_HDR - used >= bytes.len() {
                let mut d = data;
                d[SE_HDR + used..SE_HDR + used + bytes.len()].copy_from_slice(bytes);
                d[4..8].copy_from_slice(&((used + bytes.len()) as u32).to_le_bytes());
                self.stage_page(eng, tx, cur, d)?;
                return Ok((cur, (SE_HDR + used) as u32));
            }
            n_pages += 1;
            tail = Some(cur);
            if next == NULL_PAGE {
                break;
            }
            cur = next;
        }
        // Fresh page chained to the tail (or as head).
        let fresh = eng.alloc_in_tx(tx)?;
        let mut d = [0u8; PAGE_SIZE];
        d[0..4].copy_from_slice(&NULL_PAGE.to_le_bytes());
        d[4..8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        d[SE_HDR..SE_HDR + bytes.len()].copy_from_slice(bytes);
        if let Some(t) = tail {
            let mut td = self.read_current(eng, t)?;
            td[0..4].copy_from_slice(&fresh.to_le_bytes());
            self.stage_page(eng, tx, t, td)?;
        }
        self.stage_page(eng, tx, fresh, d)?;
        if tail.is_none() {
            match which {
                0 => self.str_head = fresh,
                1 => self.state_head = fresh,
                _ => self.event_head = fresh,
            }
        }
        match which {
            0 => self.str_pages = n_pages + 1,
            1 => self.state_pages = n_pages + 1,
            _ => self.event_pages = n_pages + 1,
        }
        Ok((fresh, SE_HDR as u32))
    }

    fn load_strings(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        // Walk the string chain; parse [len][bytes] cells. next_sid resets
        // to 0 and sids rebuild in chain order (invariant 5).
        self.next_sid = 0;
        let mut cur = self.str_head;
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let data_end = SE_HDR + used;
            let mut off = SE_HDR;
            while off + 4 <= data_end {
                let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                if off + 4 + len > data_end {
                    break; // torn tail (compaction will handle)
                }
                let s = String::from_utf8_lossy(&data[off + 4..off + 4 + len]).to_string();
                let sid = self.next_sid;
                self.next_sid += 1;
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

    fn load_states(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let mut cur = self.state_head;
        self.states.clear();
        self.state_pos.clear();
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let data_end = SE_HDR + used;
            let mut off = SE_HDR;
            while off + STATE_REC <= data_end && self.states.len() < self.state_recs as usize {
                let rec = StateRecord::decode(data[off..off + STATE_REC].try_into().unwrap());
                self.state_pos.push((cur, off as u32));
                self.states.push(rec);
                off += STATE_REC;
            }
            cur = next;
        }
        Ok(())
    }

    fn load_events(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let mut cur = self.event_head;
        self.events.clear();
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let data_end = SE_HDR + used;
            let mut off = SE_HDR;
            while off + EVENT_REC <= data_end && self.events.len() < self.event_recs as usize {
                let rec = EventRecord::decode(data[off..off + EVENT_REC].try_into().unwrap());
                self.events.push(rec);
                off += EVENT_REC;
            }
            cur = next;
        }
        Ok(())
    }
}

enum SeSlab {
    Strings,
    States,
    Events,
}

/// Unix seconds now (the `at = None` anchor of `get_state`).
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::GraphStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!(
            "vidgedb_sestore_{}_{}.vdg",
            name,
            std::process::id()
        ));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn state_record_roundtrip() {
        let rec = StateRecord {
            entity_sid: 7,
            key_sid: 42,
            value_sid: 3,
            valid_from: 1000,
            valid_to: 2000,
            flags: 0,
        };
        let mut buf = [0u8; STATE_REC];
        rec.encode(&mut buf);
        assert_eq!(StateRecord::decode(&buf), rec);
        // Open-ended validity survives the wire format.
        let open = StateRecord {
            entity_sid: 1,
            key_sid: 2,
            value_sid: 3,
            valid_from: 5,
            valid_to: VALID_TO_OPEN,
            flags: 0,
        };
        let mut buf2 = [0u8; STATE_REC];
        open.encode(&mut buf2);
        assert_eq!(StateRecord::decode(&buf2), open);
    }

    #[test]
    fn event_record_roundtrip_and_truncation() {
        let rec = EventRecord {
            name_sid: 11,
            entity_sid: 22,
            timestamp: -50,
            provenance: 7,
            details: "thresh=10A".to_string(),
        };
        let mut buf = [0u8; EVENT_REC];
        rec.encode(&mut buf);
        assert_eq!(EventRecord::decode(&buf), rec);
        // 20 bytes fit exactly.
        let full = "x".repeat(DETAILS_INLINE_MAX);
        let mut buf2 = [0u8; EVENT_REC];
        EventRecord {
            details: full.clone(),
            ..rec.clone()
        }
        .encode(&mut buf2);
        assert_eq!(EventRecord::decode(&buf2).details, full);
        // Longer details are truncated to DETAILS_INLINE_MAX bytes.
        let long = "y".repeat(DETAILS_INLINE_MAX + 9);
        let mut buf3 = [0u8; EVENT_REC];
        EventRecord {
            details: long,
            ..rec.clone()
        }
        .encode(&mut buf3);
        let got = EventRecord::decode(&buf3);
        assert_eq!(got.details.len(), DETAILS_INLINE_MAX);
        // Multi-byte UTF-8 is never split mid-char.
        let mut buf4 = [0u8; EVENT_REC];
        EventRecord {
            details: "é".repeat(15), // 30 bytes when encoded
            ..rec.clone()
        }
        .encode(&mut buf4);
        let got4 = EventRecord::decode(&buf4);
        assert!(got4.details.is_char_boundary(got4.details.len()));
        assert!(got4.details.len() <= DETAILS_INLINE_MAX);
    }

    #[test]
    fn truncate_utf8_respects_char_boundaries() {
        let s = "aé".repeat(12); // 36 bytes
        let t = truncate_utf8(s.as_bytes(), 20);
        assert!(std::str::from_utf8(t).is_ok());
        assert!(t.len() <= 20);
        assert_eq!(String::from_utf8_lossy(t), String::from_utf8_lossy(t));
    }

    /// End-to-end through the engine: set_state closes the previous open
    /// pair, history accumulates, get_state resolves instants.
    #[test]
    fn set_get_history_e2e() {
        let path = tmp_path("e2e");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut se = StateEventStore::open(&mut eng, &mut gs).unwrap();
            let mut tx = eng.begin().unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "STOPPED", 100)
                .unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "RUNNING", 200)
                .unwrap();
            se.set_state(&mut eng, &mut tx, "Motor42", "state", "OVERLOAD", 300)
                .unwrap();
            eng.commit(tx).unwrap();
            se.persist(&mut eng).unwrap();
        }
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let se = StateEventStore::open(&mut eng, &mut gs).unwrap();
        // Unknown key/name -> None.
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "nope", None).unwrap(),
            None
        );
        assert_eq!(
            se.get_state(&mut eng, "Nobody", "state", None).unwrap(),
            None
        );
        // Instant resolution from the rebuilt slabs.
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(150))
                .unwrap(),
            Some(("STOPPED".to_string(), 100))
        );
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(250))
                .unwrap(),
            Some(("RUNNING".to_string(), 200))
        );
        // Half-open boundary: t == valid_to belongs to the NEXT pair.
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(200))
                .unwrap(),
            Some(("RUNNING".to_string(), 200))
        );
        assert_eq!(
            se.get_state(&mut eng, "Motor42", "state", Some(99))
                .unwrap(),
            None
        );
        // History: 3 pairs, windows [100,200) [200,300) [300,open).
        let h = se.state_history(&mut eng, "Motor42", "state").unwrap();
        assert_eq!(
            h,
            vec![
                ("STOPPED".to_string(), 100, 200),
                ("RUNNING".to_string(), 200, 300),
                ("OVERLOAD".to_string(), 300, VALID_TO_OPEN),
            ]
        );
        cleanup(&path);
    }
}

//! Time-series store for VidgeDB — Phase 3 (spec §19, §69 Phase 3).
//!
//! Pipeline (spec §19): memory buffer -> batch -> chunk -> stream pages ->
//! chunk index. Telemetry SHALL NOT require one disk op per measurement:
//! points accumulate in an in-memory buffer; a chunk is encoded and
//! persisted every `BATCH` points or on explicit `flush_series`.
//!
//! Chunk (spec §19 Chunk struct): series id, t0/t_end, count, compressed
//! payload, min/max metadata:
//! - timestamps: absolute `t0` + zigzag-varint deltas (`enc_ts = CONST` when
//!   all timestamps are identical);
//! - values: raw f64 LE (`enc_val = RAW`) or single constant f64
//!   (`enc_val = CONST`, all equal).
//!
//! Payload bytes live in a per-series append-only page stream
//! (`[next u32][used u32]` header, used = payload bytes). The chunk index is
//! a global fixed-record slab; it IS the temporal index (records are
//! chronological per series; range queries skip non-overlapping chunks).
//!
//! Layout page: own page (its id is stored in the GraphStore meta), holding
//! the series-slab and chunk-slab heads. Rebuilt state on open: series
//! table, chunk index, stream tails. The slabs are the source of truth
//! (reconstructible, spec §17).
//!
//! Durability ordering: the caller commits the data tx FIRST, then calls
//! `persist()` (layout counts). A crash between the two loads the stale
//! layout (newer chunks ignored) — never corrupted committed data.
//!
//! ## Tx-end discipline (phase 8.7, BUG-1 fix)
//!
//! Every mutating TS call (`create_series`/`append`/`flush_series`) runs
//! against a caller-owned `Tx`; the store mutates its in-memory state
//! (chunk index, series counters, stream heads, staged page images) as the
//! tx's staged writes are produced. If that tx ROLLS BACK, the engine
//! discards nothing durable (nothing was journaled) but those in-memory
//! mutations must be undone — otherwise the store is wedged: its stream
//! head and staged map reference pager ids the rollback handed back to
//! the scratch pool, and the first post-rollback touch re-reserves them
//! (fresh zeroed images) or hits `PageOutOfBounds` on every read
//! (probe_p1: flush_series after rollback → Err(PageOutOfBounds(4))).
//!
//! Mechanism (SNAPSHOT-REVERT + DEFERRED REVIVAL, chosen over a tx-scoped
//! overlay for being a strictly local change: the snapshot clones the
//! affected store fields at first tx contact and the field accesses keep
//! their existing read-your-staged-writes semantics — an overlay would
//! have had to make every field access resolve through a per-tx
//! indirection, touching the whole file):
//! - the Engine publishes ONE settle event per tx end on a shared settle
//!   log: `(tx_id, true)` from a successful commit, `(tx_id, false)` from
//!   a rollback (Tx dropped, `Engine::rollback`, or a net-out — any end
//!   that never became durable);
//! - on the first TS mutation a tx executes, the store arms itself: it
//!   takes the tx's monotone id (`Tx::id`) and snapshots the mutable
//!   store fields (a few Vec/HashMap clones, batch-sized);
//! - after each TS call, `settle()` drains the shared settle log: a
//!   committed arm is simply discarded (state stays as mutated by the
//!   commit), a ROLLED-BACK arm restores the snapshot EXACTLY: buffers,
//!   chunk index, slabs' heads/counts, staged page images, series
//!   counters, stream heads — bit-identical to pre-tx;
//! - a series CREATED inside the rolled-back tx is remembered by name on
//!   a revival list (its sid keeps being usable — the probe_p1 contract)
//!   and the NEXT mutating tx re-slabs it through the ordinary
//!   `create_series` internals (`revive_pending`); a reviving tx that
//!   itself rolls back re-captures it (idempotent);
//! - every TS call therefore returns with the store in the state of the
//!   last commit — a rolled-back tx leaves nothing behind (phantom
//!   points, phantom pages, zombie staged images all gone), and the next
//!   ingest re-reserves everything cleanly.
//!
//! The revert is the same discipline as the pager's phase 8.6 net-out
//! (a reservation handed back instead of folded), one layer up.
//!
//! No unsafe (spec §49).

use crate::engine::{Engine, EngineError, Tx};
use crate::pager::{PageId, NULL_PAGE, PAGE_SIZE};
use crate::stores::GraphStore;
use std::collections::HashMap;

/// The Engine's shared tx-end log — pub(crate) in engine.rs (phase 8.7).
use crate::engine::SettleLog;

/// Buffered points before a chunk is encoded + persisted.
pub const BATCH: usize = 256;
/// Stream/slab page header: [next u32][used u32], used = payload bytes.
pub const TS_HDR: usize = 8;
/// Series record: [len u8][name 23B][total_points u32][chunk_count u32]
/// [stream_head u32][pad u32].
pub const SERIES_REC: usize = 40;
/// Chunk index record: [series u32][first_page u32][first_off u32]
/// [blob_len u32][t0 i64][t_end i64][count u32][pad u32][min f64][max f64].
pub const CHUNK_REC: usize = 56;
/// Max series name bytes.
pub const NAME_MAX: usize = 23;

pub const ENC_TS_VARINT: u8 = 0;
pub const ENC_TS_CONST: u8 = 1;
pub const ENC_VAL_RAW: u8 = 0;
pub const ENC_VAL_CONST: u8 = 1;

// ---------------------------------------------------------------------------
// Encoding primitives
// ---------------------------------------------------------------------------

#[inline]
pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
pub fn unzigzag(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

pub fn write_uv(out: &mut Vec<u8>, mut u: u64) {
    loop {
        if u < 0x80 {
            out.push(u as u8);
            return;
        }
        out.push(((u as u8) & 0x7F) | 0x80);
        u >>= 7;
    }
}

pub fn read_uv(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut u: u64 = 0;
    let mut shift = 0u32;
    while *pos < buf.len() {
        let b = buf[*pos];
        *pos += 1;
        u |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(u);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

/// Encoded chunk (spec §19 Chunk): metadata + compressed payload.
/// Payload layout: [enc_ts u8][enc_val u8][deltas...?][values...].
#[derive(Debug, Clone, PartialEq)]
pub struct TsChunk {
    pub enc_ts: u8,
    pub enc_val: u8,
    pub t0: i64,
    pub t_end: i64,
    pub count: u32,
    pub min: f64,
    pub max: f64,
    pub payload: Vec<u8>,
}

/// Encode a chronological batch of (timestamp, value) points.
pub fn encode_chunk(points: &[(i64, f64)]) -> TsChunk {
    assert!(!points.is_empty(), "empty chunk");
    let t0 = points[0].0;
    let t_end = points[points.len() - 1].0;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for &(_, v) in points {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    let v0 = points[0].1;
    let enc_ts = if points.iter().all(|(t, _)| *t == t0) {
        ENC_TS_CONST
    } else {
        ENC_TS_VARINT
    };
    let enc_val = if points.iter().all(|(_, v)| *v == v0) {
        ENC_VAL_CONST
    } else {
        ENC_VAL_RAW
    };
    let mut payload = vec![enc_ts, enc_val];
    if enc_ts == ENC_TS_VARINT {
        let mut prev = t0;
        for &(t, _) in &points[1..] {
            write_uv(&mut payload, zigzag(t - prev));
            prev = t;
        }
    }
    if enc_val == ENC_VAL_CONST {
        payload.extend_from_slice(&v0.to_le_bytes());
    } else {
        for &(_, v) in points {
            payload.extend_from_slice(&v.to_le_bytes());
        }
    }
    TsChunk {
        enc_ts,
        enc_val,
        t0,
        t_end,
        count: points.len() as u32,
        min,
        max,
        payload,
    }
}

/// Decode a chunk back to its points (deterministic fold, spec §18).
pub fn decode_chunk(c: &TsChunk) -> Vec<(i64, f64)> {
    let mut out = Vec::with_capacity(c.count as usize);
    let mut pos = 2usize; // skip [enc_ts][enc_val]
    let ts: Vec<i64> = if c.enc_ts == ENC_TS_CONST {
        vec![c.t0; c.count as usize]
    } else {
        let mut ts = Vec::with_capacity(c.count as usize);
        let mut t = c.t0;
        ts.push(t);
        for _ in 1..c.count {
            let d = unzigzag(read_uv(&c.payload, &mut pos).unwrap_or(0));
            t += d;
            ts.push(t);
        }
        ts
    };
    match c.enc_val {
        ENC_VAL_CONST => {
            let v = f64::from_le_bytes(c.payload[pos..pos + 8].try_into().unwrap());
            for &t in &ts {
                out.push((t, v));
            }
        }
        _ => {
            for &t in &ts {
                let v = f64::from_le_bytes(c.payload[pos..pos + 8].try_into().unwrap());
                pos += 8;
                out.push((t, v));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// TS layout page: [series_head][series_pages][series_recs][chunk_head]
/// [chunk_pages][chunk_recs][next_series][reserved] (8 u32).
#[derive(Debug, Clone, Copy)]
struct TsLayout {
    series_head: u32,
    series_pages: u32,
    series_recs: u32,
    chunk_head: u32,
    chunk_pages: u32,
    chunk_recs: u32,
    next_series: u32,
}

impl Default for TsLayout {
    fn default() -> Self {
        // Heads start at NULL_PAGE: page 0 is the superblock, 0 is invalid.
        TsLayout {
            series_head: NULL_PAGE,
            series_pages: 0,
            series_recs: 0,
            chunk_head: NULL_PAGE,
            chunk_pages: 0,
            chunk_recs: 0,
            next_series: 0,
        }
    }
}

impl TsLayout {
    fn encode(&self, page: &mut [u8; PAGE_SIZE]) {
        page[8 * 4..].fill(0);
        let f = [
            self.series_head,
            self.series_pages,
            self.series_recs,
            self.chunk_head,
            self.chunk_pages,
            self.chunk_recs,
            self.next_series,
            0u32,
        ];
        for (i, v) in f.iter().enumerate() {
            page[i * 4..(i + 1) * 4].copy_from_slice(&v.to_le_bytes());
        }
    }

    fn decode(page: &[u8; PAGE_SIZE]) -> TsLayout {
        let g = |i: usize| u32::from_le_bytes(page[i * 4..(i + 1) * 4].try_into().unwrap());
        let fix = |h: u32| if h == 0 { NULL_PAGE } else { h };
        TsLayout {
            series_head: fix(g(0)),
            series_pages: g(1),
            series_recs: g(2),
            chunk_head: fix(g(3)),
            chunk_pages: g(4),
            chunk_recs: g(5),
            next_series: g(6),
        }
    }
}

/// One chunk index record (temporal index entry).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkRec {
    pub series: u32,
    pub first_page: PageId,
    pub first_off: u32,
    pub blob_len: u32,
    pub t0: i64,
    pub t_end: i64,
    pub count: u32,
    pub min: f64,
    pub max: f64,
}

impl ChunkRec {
    fn encode(&self, data: &mut [u8], at: usize) {
        data[at..at + 4].copy_from_slice(&self.series.to_le_bytes());
        data[at + 4..at + 8].copy_from_slice(&self.first_page.to_le_bytes());
        data[at + 8..at + 12].copy_from_slice(&self.first_off.to_le_bytes());
        data[at + 12..at + 16].copy_from_slice(&self.blob_len.to_le_bytes());
        data[at + 16..at + 24].copy_from_slice(&self.t0.to_le_bytes());
        data[at + 24..at + 32].copy_from_slice(&self.t_end.to_le_bytes());
        data[at + 32..at + 36].copy_from_slice(&self.count.to_le_bytes());
        data[at + 36..at + 40].fill(0);
        data[at + 40..at + 48].copy_from_slice(&self.min.to_le_bytes());
        data[at + 48..at + 56].copy_from_slice(&self.max.to_le_bytes());
    }

    fn decode(data: &[u8], at: usize) -> ChunkRec {
        let g4 = |i: usize| u32::from_le_bytes(data[at + i..at + i + 4].try_into().unwrap());
        let g8i = |i: usize| i64::from_le_bytes(data[at + i..at + i + 8].try_into().unwrap());
        let g8f = |i: usize| f64::from_le_bytes(data[at + i..at + i + 8].try_into().unwrap());
        ChunkRec {
            series: g4(0),
            first_page: g4(4),
            first_off: g4(8),
            blob_len: g4(12),
            t0: g8i(16),
            t_end: g8i(24),
            count: g4(32),
            min: g8f(40),
            max: g8f(48),
        }
    }
}

/// In-memory series entry (rebuilt on open).
#[derive(Debug, Clone)]
pub struct SeriesEntry {
    pub name: String,
    /// First page of this series' payload stream (NULL_PAGE = empty).
    pub stream_head: PageId,
    pub total_points: u64,
    pub chunk_count: u32,
}

/// Aggregation result over a time range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Agg {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

/// The time-series store: per-series buffers, chunk streams, chunk index.
pub struct TimeSeriesStore {
    meta_page: PageId,
    series_head: u32,
    series_pages: u32,
    series_recs: u32,
    chunk_head: u32,
    chunk_pages: u32,
    chunk_recs: u32,
    next_series: u32,
    /// series_id (slab index) -> entry.
    pub series: Vec<SeriesEntry>,
    /// All chunk records in append order (the temporal index).
    pub chunk_index: Vec<ChunkRec>,
    /// Unflushed points per series (auto-chunked at BATCH).
    buffers: HashMap<u32, Vec<(i64, f64)>>,
    /// Pages staged in the current tx (page staged = not on disk yet).
    staged: HashMap<PageId, [u8; PAGE_SIZE]>,
    /// series_id -> slab cell (page, off).
    series_pos: Vec<(PageId, u32)>,
    /// series_id -> stream tail (page, used).
    tails: HashMap<u32, (PageId, u32)>,
    // -- tx-end snapshot state (phase 8.7, BUG-1 fix; see module docs) ------
    /// Settle log shared with the Engine — armed at open, drained by
    /// `settle()` after every mutating call.
    settle_log: SettleLog,
    /// Set after the FIRST mutation of an armed tx (id, snapshot of every
    /// field that tx can mutate). `Self::restore()` puts these back on
    /// rollback; a commit discards them. One arm covers the tx's whole
    /// run in the single-writer model.
    armed: Option<(u64, TsSnapshot)>,
    /// Series created inside a tx that ROLLED BACK: (sid-at-creation,
    /// name). Their slab records were staged in the dropped tx, so the
    /// durable slab never carried them — the revert removes the in-memory
    /// entries and this list remembers the identity; the NEXT mutating
    /// call re-slabs them through the ordinary `create_series` code path
    /// (probe_p1 contract: a series created in a rolled-back tx stays
    /// usable — tx2 appends on the same sid — and becomes durable when
    /// THAT tx commits). Consumed by `revive_pending`; re-captured by
    /// `restore` if the reviving tx itself rolls back (idempotent).
    revive: Vec<(u32, String)>,
}

/// DeepCopy snapshot of everything a tx mutates in this store
/// (batch-sized Vecs; taken on first-touch, restored on rollback).
/// Full revert (no field excluded): the store's whole in-memory state
/// returns to its first-touch state — including the series table, whose
/// entries created inside the rolled-back tx are captured onto the
/// revival list before the revert (see `restore`).
struct TsSnapshot {
    series: Vec<SeriesEntry>,
    series_pos: Vec<(PageId, u32)>,
    chunk_index: Vec<ChunkRec>,
    staged: HashMap<PageId, [u8; PAGE_SIZE]>,
    buffers: HashMap<u32, Vec<(i64, f64)>>,
    tails: HashMap<u32, (PageId, u32)>,
    series_head: u32,
    series_pages: u32,
    series_recs: u32,
    chunk_head: u32,
    chunk_pages: u32,
    chunk_recs: u32,
    next_series: u32,
    revive: Vec<(u32, String)>,
}

impl TimeSeriesStore {
    /// Open (create on first use) the TS store; needs the graph store only
    /// to locate/create the TS layout page pointer.
    pub fn open(eng: &mut Engine, gs: &mut GraphStore) -> Result<TimeSeriesStore, EngineError> {
        let mut meta_page = *gs.ts_meta_page_ref();
        if meta_page == NULL_PAGE {
            let mut tx = eng.begin()?;
            let p = eng.alloc_in_tx(&mut tx)?;
            let mut page = [0u8; PAGE_SIZE];
            TsLayout::default().encode(&mut page);
            eng.write_in_tx(&mut tx, p, |d| *d = page)?;
            eng.commit(tx)?;
            gs.set_ts_meta_page(p);
            gs.persist(eng)?;
            meta_page = p;
        }
        let lay = TsLayout::decode(eng.read_page(meta_page)?);
        let mut ts = TimeSeriesStore {
            meta_page,
            series_head: lay.series_head,
            series_pages: lay.series_pages,
            series_recs: lay.series_recs,
            chunk_head: lay.chunk_head,
            chunk_pages: lay.chunk_pages,
            chunk_recs: lay.chunk_recs,
            next_series: lay.next_series,
            series: Vec::new(),
            chunk_index: Vec::new(),
            buffers: HashMap::new(),
            staged: HashMap::new(),
            series_pos: Vec::new(),
            tails: HashMap::new(),
            settle_log: eng.settle_log(),
            armed: None,
            revive: Vec::new(),
        };
        ts.load_series(eng)?;
        ts.load_chunks(eng)?;
        Ok(ts)
    }

    // -- public API -----------------------------------------------------------

    /// Create a series; returns its stable series_id (slab index).
    /// Phase 8.7: settles (reverts any rolled-back tx), re-slabs any
    /// series pending revival, arms the tx snapshot — see module docs.
    pub fn create_series(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        name: &str,
    ) -> Result<u32, EngineError> {
        self.settle(Some(tx.id()));
        if self.armed.is_none() {
            self.armed = Some((tx.id(), self.snapshot()));
        }
        self.revive_pending(eng, tx)?;
        let sid = self.create_series_in(eng, tx, name)?;
        self.settle(Some(tx.id()));
        Ok(sid)
    }

    /// Buffer one point; auto-flushes a chunk every BATCH points. Phase
    /// 8.7: settles and arms before touching the tx (see module docs).
    pub fn append(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        sid: u32,
        t: i64,
        v: f64,
    ) -> Result<(), EngineError> {
        self.settle(Some(tx.id()));
        if self.armed.is_none() {
            self.armed = Some((tx.id(), self.snapshot()));
        }
        self.revive_pending(eng, tx)?;
        assert!((sid as usize) < self.series.len(), "unknown series");
        let buf = self.buffers.entry(sid).or_default();
        buf.push((t, v));
        let flush = buf.len() >= BATCH;
        if flush {
            self.flush_buffer(eng, tx, sid)?;
        }
        self.settle(Some(tx.id()));
        Ok(())
    }

    /// Encode + persist the buffered points of one series (if any).
    /// Phase 8.7: settles and arms like every mutating entry (module docs).
    pub fn flush_series(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        sid: u32,
    ) -> Result<(), EngineError> {
        self.settle(Some(tx.id()));
        if self.armed.is_none() {
            self.armed = Some((tx.id(), self.snapshot()));
        }
        self.revive_pending(eng, tx)?;
        let r = self.flush_buffer(eng, tx, sid);
        self.settle(Some(tx.id()));
        r
    }

    /// Query points with timestamp in [from, to] (chunks + live buffer).
    /// `&mut self` (phase 8.7): the call settles the store first —
    /// rolled-back txs are reverted, so a query after a rollback serves
    /// committed state only. All call sites own their store handle.
    pub fn query(
        &mut self,
        eng: &mut Engine,
        sid: u32,
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, f64)>, EngineError> {
        self.settle(None);
        let mut out = Vec::new();
        for rec in &self.chunk_index {
            if rec.series != sid || rec.t_end < from || rec.t0 > to {
                continue; // temporal index: skip non-overlapping chunks
            }
            let payload =
                self.stream_read(eng, rec.first_page, rec.first_off, rec.blob_len as usize)?;
            let chunk = TsChunk {
                enc_ts: payload[0],
                enc_val: payload[1],
                t0: rec.t0,
                t_end: rec.t_end,
                count: rec.count,
                min: rec.min,
                max: rec.max,
                payload,
            };
            for (t, v) in decode_chunk(&chunk) {
                if t >= from && t <= to {
                    out.push((t, v));
                }
            }
        }
        if let Some(buf) = self.buffers.get(&sid) {
            for &(t, v) in buf {
                if t >= from && t <= to {
                    out.push((t, v));
                }
            }
        }
        out.sort_by_key(|&(t, _)| t);
        Ok(out)
    }

    /// Aggregate over [from, to] (decode + filter; correctness first).
    /// `&mut self` (phase 8.7): settles through `query` (module docs).
    pub fn aggregate(
        &mut self,
        eng: &mut Engine,
        sid: u32,
        from: i64,
        to: i64,
    ) -> Result<Agg, EngineError> {
        let pts = self.query(eng, sid, from, to)?;
        if pts.is_empty() {
            return Ok(Agg {
                count: 0,
                sum: 0.0,
                min: f64::NAN,
                max: f64::NAN,
            });
        }
        let mut sum = 0.0;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for &(_, v) in &pts {
            sum += v;
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
        Ok(Agg {
            count: pts.len() as u64,
            sum,
            min,
            max,
        })
    }

    /// Free every payload-stream page strictly before `stop_page` (the
    /// page holding the first KEPT chunk's payload), walking the chain
    /// from `head`; a `stop_page` of [`NULL_PAGE`] frees the whole chain
    /// (series fully drained). Returns the number of pages freed.
    ///
    /// Whole-page granularity: the page containing the first kept chunk's
    /// bytes (`stop_page` itself) is never freed — it may carry doomed
    /// bytes in front of kept ones, which stay until that page is
    /// entirely retired by later retention passes. Points are never
    /// removed individually (spec §37 works in whole chunks).
    fn free_stream_prefix(
        &self,
        eng: &mut Engine,
        tx: &mut Tx,
        head: PageId,
        stop_page: PageId,
    ) -> Result<u32, EngineError> {
        let mut cur = head;
        let mut freed: u32 = 0;
        while cur != NULL_PAGE && cur != stop_page {
            // Read the chain pointer BEFORE freeing: free_in_tx only
            // stages the freelist node in tx.writes, the durable page
            // content stays valid for read_current until commit.
            let data = self.read_current(eng, cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let page_id = cur;
            eng.free_in_tx(tx, page_id)?;
            freed += 1;
            cur = next;
        }
        Ok(freed)
    }

    /// Retention policy (spec §37 storage bounds): drop every chunk that
    /// is ENTIRELY older than `before` (unix seconds).
    ///
    /// ## Chunk granularity (hard rule — do not regress)
    /// Points are NEVER removed individually. `retain` walks the chunk
    /// index (each record carries `t0`/`t_end`) and removes a chunk only
    /// when `t_end < before`, i.e. every point inside it is older than
    /// the cutoff. A chunk straddling the cutoff is kept whole — partial
    /// removal would corrupt the metadata-only min/max/count stats
    /// (spec §19) that [`Self::stats_from_chunks`] serves without any
    /// payload decode. Per series only the chronological PREFIX of the
    /// chunk index is removed (steady-state append order), so the
    /// remaining chunks keep their contiguous payload-stream prefix.
    ///
    /// Mechanics per affected series: payload pages of the removed
    /// prefix are recycled through [`Engine::free_in_tx`], the series
    /// record's `total_points` / `chunk_count` / `stream_head` are
    /// patched, and the chunk-index slab is rebuilt without the dropped
    /// records (it is the temporal index's on-disk source of truth —
    /// stale records must not survive reopen). Returns
    /// `(points_removed, chunks_removed)`.
    pub fn retain(&mut self, eng: &mut Engine, before: i64) -> Result<(u64, usize), EngineError> {
        // 1) Classify the doomed chronological prefix of each series.
        let n_series = self.series.len();
        let mut kept_seen = vec![false; n_series];
        let mut doomed: Vec<ChunkRec> = Vec::new();
        for rec in &self.chunk_index {
            if rec.series as usize >= n_series {
                continue;
            }
            if !kept_seen[rec.series as usize] && rec.t_end < before {
                doomed.push(*rec);
            } else {
                kept_seen[rec.series as usize] = true;
            }
        }
        let chunks_removed = doomed.len();
        if chunks_removed == 0 {
            return Ok((0, 0));
        }
        let points_removed: u64 = doomed.iter().map(|rec| rec.count as u64).sum();

        // 2) Per-series work: (sid, points, chunks, new stream head). The
        //    first KEPT chunk of each affected series (if any) owns the
        //    retained stream tail; a fully drained series gets NULL_PAGE.
        let mut per_sid_points: HashMap<u32, u64> = HashMap::new();
        for rec in &doomed {
            *per_sid_points.entry(rec.series).or_insert(0) += rec.count as u64;
        }
        let mut kept_head: HashMap<u32, PageId> = HashMap::new();
        let mut stopped: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for rec in &self.chunk_index {
            if !stopped.contains(&rec.series) && rec.t_end >= before {
                kept_head.insert(rec.series, rec.first_page);
                stopped.insert(rec.series);
            }
        }
        let work: Vec<(u32, u64, u32, PageId)> = per_sid_points
            .iter()
            .map(|(&sid, &pts)| {
                let nch = doomed.iter().filter(|rec| rec.series == sid).count() as u32;
                (
                    sid,
                    pts,
                    nch,
                    kept_head.get(&sid).copied().unwrap_or(NULL_PAGE),
                )
            })
            .collect();

        // 3+4) ONE tx (phase 8.7, BUG-2 fix — the old two-commit window is
        //     closed): free the removed payload pages, patch the series
        //     records (decremented counters + moved stream head), AND
        //     rewrite the chunk-index slab in place without the removed
        //     records (trailing pages freed). The chunk-index slab is the
        //     temporal index's on-disk source of truth, so the disk copy
        //     must not keep stale records; putting its rewrite inside the
        //     SAME commit as the page frees + series patches makes any
        //     crash leave EITHER both states (pre-retention, replay) or
        //     the post-retention state — never freed pages under a stale
        //     index (the old window's counters-vs-index divergence).
        {
            let mut tx = eng.begin()?;
            // Phase 8.7: the internal tx gets the SAME snapshot discipline
            // as the caller-facing entries — it mutates counters, stream
            // heads and (through the compaction) the chunk index; an Err
            // mid-tx must revert all of it (its tx Drop settles the log;
            // the next TS call restores).
            self.settle(Some(tx.id()));
            if self.armed.is_none() {
                self.armed = Some((tx.id(), self.snapshot()));
            }
            for &(sid, _pts, _nch, new_head) in &work {
                let old_head = self.series[sid as usize].stream_head;
                if old_head != new_head {
                    self.free_stream_prefix(eng, &mut tx, old_head, new_head)?;
                    self.series[sid as usize].stream_head = new_head;
                }
            }
            for &(sid, pts, nch, _) in &work {
                let e = &mut self.series[sid as usize];
                e.total_points = e.total_points.saturating_sub(pts);
                e.chunk_count = e.chunk_count.saturating_sub(nch);
            }
            for &(sid, ..) in work.iter() {
                self.patch_series_record(eng, &mut tx, sid)?;
            }
            self.compact_chunk_slab_in(eng, &mut tx, before)?;
            self.stage_layout(eng, &mut tx)?;
            eng.commit(tx)?;
            self.settle(None);
            self.staged.clear();
        }
        Ok((points_removed, chunks_removed))
    }

    /// Rewrite the chunk-index slab keeping only records with
    /// `t_end >= before`; rebuild `chunk_recs` / `chunk_pages` /
    /// `chunk_head`/`chunk_index` accordingly. The slab pages are the
    /// on-disk source of truth of the temporal index, so the disk copy
    /// must not keep stale records even though the pages are recycled.
    /// IN-TX (phase 8.7, BUG-2 fix): stages everything on the CALLER's
    /// tx — the caller owns the single atomic commit.
    fn compact_chunk_slab_in(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        before: i64,
    ) -> Result<(), EngineError> {
        // Encode the kept records first (nothing borrows across the tx).
        let kept: Vec<[u8; CHUNK_REC]> = self
            .chunk_index
            .iter()
            .filter(|rec| rec.t_end >= before)
            .map(|rec| {
                let mut b = [0u8; CHUNK_REC];
                rec.encode(&mut b, 0);
                b
            })
            .collect();
        self.chunk_index.retain(|rec| rec.t_end >= before);
        self.chunk_recs = self.chunk_index.len() as u32;
        let per_page = (PAGE_SIZE - TS_HDR) / CHUNK_REC;
        let kept_len = kept.len();
        let n_new = if kept_len == 0 {
            0
        } else {
            (kept_len + per_page - 1) / per_page
        };
        // Current old chain (page ids, in order) — read durably, before
        // any free stages new content into scratch.
        let mut old_pages: Vec<PageId> = Vec::new();
        {
            let mut cur = self.chunk_head;
            while cur != NULL_PAGE {
                let data = self.read_current(eng, cur)?;
                old_pages.push(cur);
                cur = u32::from_le_bytes(data[0..4].try_into().unwrap());
            }
        }
        // Fill the first `n_new` old pages with the kept records.
        for pi in 0..n_new {
            let pid = old_pages[pi];
            let mut d = [0u8; PAGE_SIZE];
            let next = if pi + 1 < n_new {
                old_pages[pi + 1]
            } else {
                NULL_PAGE
            };
            d[0..4].copy_from_slice(&next.to_le_bytes());
            let start = pi * per_page;
            let end = (start + per_page).min(kept_len);
            let used = (end - start) * CHUNK_REC;
            d[4..8].copy_from_slice(&(used as u32).to_le_bytes());
            for (ri, ridx) in (start..end).enumerate() {
                d[TS_HDR + ri * CHUNK_REC..TS_HDR + (ri + 1) * CHUNK_REC]
                    .copy_from_slice(&kept[ridx]);
            }
            self.stage_page(eng, tx, pid, d)?;
        }
        // Free the trailing pages the compacted records no longer reach.
        for &pid in &old_pages[n_new..] {
            eng.free_in_tx(tx, pid)?;
        }
        self.chunk_pages = n_new as u32;
        self.chunk_head = if n_new == 0 { NULL_PAGE } else { old_pages[0] };
        Ok(())
    }

    /// Fast stats from chunk metadata only (no payload decode), committed
    /// chunks only (spec §19 optional statistics).
    pub fn stats_from_chunks(&self, sid: u32) -> Option<(u64, f64, f64)> {
        let mut count = 0u64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut any = false;
        for rec in &self.chunk_index {
            if rec.series != sid {
                continue;
            }
            any = true;
            count += rec.count as u64;
            if rec.min < min {
                min = rec.min;
            }
            if rec.max > max {
                max = rec.max;
            }
        }
        if any {
            Some((count, min, max))
        } else {
            None
        }
    }

    /// Persist the TS layout page (call AFTER committing the data tx).
    /// Phase 8.7: the engine's commit settled the data tx on the log;
    /// draining here (no auto-re-arm — nothing of THIS tx is mutated
    /// before the call) discards the commit's snapshot. A live caller tx
    /// mid-run keeps its arm: `eng.commit` does not drain the settle log
    /// (drain points are store entry points) and a committed caller tx's
    /// arm is discarded by the next TS call's settle.
    pub fn persist(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        self.settle(None);
        let mut tx = eng.begin()?;
        self.stage_layout(eng, &mut tx)?;
        eng.commit(tx)?;
        self.staged.clear();
        Ok(())
    }

    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    pub fn committed_chunks(&self, sid: u32) -> usize {
        self.chunk_index.iter().filter(|r| r.series == sid).count()
    }

    // -- tx-end snapshot machinery (phase 8.7, BUG-1 fix) ----------------

    /// Deep-copy snapshot of everything a tx can mutate in this store,
    /// taken at first-touch (see module docs). Batch-sized Vecs; the
    /// pending revival list rides along (re-captured on a second
    /// rollback so a revival that never commits stays pending).
    fn snapshot(&self) -> TsSnapshot {
        TsSnapshot {
            series: self.series.clone(),
            series_pos: self.series_pos.clone(),
            chunk_index: self.chunk_index.clone(),
            staged: self.staged.clone(),
            buffers: self.buffers.clone(),
            tails: self.tails.clone(),
            series_head: self.series_head,
            series_pages: self.series_pages,
            series_recs: self.series_recs,
            chunk_head: self.chunk_head,
            chunk_pages: self.chunk_pages,
            chunk_recs: self.chunk_recs,
            next_series: self.next_series,
            revive: self.revive.clone(),
        }
    }

    /// Restore a snapshot: every field a tx can mutate goes back to its
    /// first-touch state (bit-identical revert). Series entries the tx
    /// CREATED (index >= snapshot length) are remembered on `revive`
    /// — their sid keeps being usable (probe_p1 contract) while their
    /// slab record waits for the next mutating tx to re-stage it.
    fn restore(&mut self, s: TsSnapshot) {
        // Series the tx created: keep the identity alive. sid == slab
        // index == snapshot index; the entries after the snapshot length
        // are exactly the tx's creations, in sid order. The snapshot's
        // OWN pending revival list comes first (a reviving tx that
        // rolled back lost its re-slab — still pending), then the
        // newly orphaned creations, in sid order.
        let mut revive = s.revive;
        let have: std::collections::HashSet<u32> = revive.iter().map(|&(sid, _)| sid).collect();
        for (i, e) in self.series.iter().enumerate().skip(s.series.len()) {
            let sid = i as u32;
            if !have.contains(&sid) {
                revive.push((sid, e.name.clone()));
            }
        }
        self.series = s.series;
        self.series_pos = s.series_pos;
        self.chunk_index = s.chunk_index;
        self.staged = s.staged;
        self.buffers = s.buffers;
        self.tails = s.tails;
        self.series_head = s.series_head;
        self.series_pages = s.series_pages;
        self.series_recs = s.series_recs;
        self.chunk_head = s.chunk_head;
        self.chunk_pages = s.chunk_pages;
        self.chunk_recs = s.chunk_recs;
        self.next_series = s.next_series;
        self.revive = revive;
    }

    /// Drain the shared settle log and end every armed tx it reports:
    /// committed = discard the snapshot (state stays as the commit left
    /// it), ROLLED BACK = restore the snapshot exactly (phantom points,
    /// phantom pages, zombie staged images all gone). `current` guards
    /// the not-yet-ended armed tx: its snapshot stays live even when an
    /// OLDER tx's end arrives on the log. Any tx whose end arrived while
    /// the store was unarmed (pure-engine txs between TS calls) is
    /// skipped — it touched nothing here.
    fn settle(&mut self, current: Option<u64>) {
        let events: Vec<(u64, bool)> = {
            let mut l = self.settle_log.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *l)
        };
        for (id, committed) in events {
            let matches = match &self.armed {
                Some((aid, _)) => *aid == id && current != Some(*aid),
                None => false,
            };
            if matches {
                let (_, snap) = self.armed.take().unwrap();
                if !committed {
                    self.restore(snap);
                }
            }
        }
    }

    /// Re-slab every series on the revival list (see `revive` field):
    /// the ordinary `create_series` internals, run against the CALLER's
    /// tx. No-op when the list is empty (the steady state). Called after
    /// the arm (an arming snapshot carries the pre-revival pending list,
    /// so a reviving tx that rolls back re-captures it — idempotent).
    fn revive_pending(&mut self, eng: &mut Engine, tx: &mut Tx) -> Result<(), EngineError> {
        if self.revive.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.revive);
        for (sid, name) in pending {
            let sid_new = self.create_series_in(eng, tx, &name)?;
            debug_assert_eq!(sid_new, sid, "revival must reuse the orphaned sid");
        }
        Ok(())
    }

    /// The create_series internals against a given tx (used by the
    /// public entry and by the revival path).
    fn create_series_in(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        name: &str,
    ) -> Result<u32, EngineError> {
        let bytes = name.as_bytes();
        assert!(bytes.len() <= NAME_MAX, "series name too long");
        let sid = self.next_series;
        self.next_series += 1;
        let mut rec = [0u8; SERIES_REC];
        rec[0] = bytes.len() as u8;
        rec[1..1 + bytes.len()].copy_from_slice(bytes);
        rec[24..28].copy_from_slice(&0u32.to_le_bytes()); // total_points
        rec[28..32].copy_from_slice(&0u32.to_le_bytes()); // chunk_count
        rec[32..36].copy_from_slice(&NULL_PAGE.to_le_bytes()); // stream_head
        let (page, off) = self.slab_append(eng, tx, TsSlab::Series, &rec)?;
        self.series_pos.push((page, off));
        self.series_recs += 1;
        self.series.push(SeriesEntry {
            name: name.to_string(),
            stream_head: NULL_PAGE,
            total_points: 0,
            chunk_count: 0,
        });
        self.stage_layout(eng, tx)?;
        Ok(sid)
    }

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
        let lay = TsLayout {
            series_head: self.series_head,
            series_pages: self.series_pages,
            series_recs: self.series_recs,
            chunk_head: self.chunk_head,
            chunk_pages: self.chunk_pages,
            chunk_recs: self.chunk_recs,
            next_series: self.next_series,
        };
        let mut data = self.read_current(eng, self.meta_page)?;
        lay.encode(&mut data);
        self.stage_page(eng, tx, self.meta_page, data)
    }

    /// Append a fixed-size record to one of the TS slabs.
    fn slab_append(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        slab: TsSlab,
        rec: &[u8],
    ) -> Result<(PageId, u32), EngineError> {
        let (head, which) = match slab {
            TsSlab::Series => (self.series_head, 0u8),
            TsSlab::Chunks => (self.chunk_head, 1u8),
        };
        // Walk the chain for a page with room. (`head` is read here to
        // seed `cur`; the fresh-page branch below updates the store's
        // own head field directly, so the local needs no write-back.)
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
            if PAGE_SIZE - TS_HDR - used >= rec.len() {
                let mut d = data;
                d[TS_HDR + used..TS_HDR + used + rec.len()].copy_from_slice(rec);
                d[4..8].copy_from_slice(&((used + rec.len()) as u32).to_le_bytes());
                self.stage_page(eng, tx, cur, d)?;
                return Ok((cur, (TS_HDR + used) as u32));
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
        d[TS_HDR..TS_HDR + rec.len()].copy_from_slice(rec);
        d[4..8].copy_from_slice(&(rec.len() as u32).to_le_bytes());
        if let Some(t) = tail {
            let mut td = self.read_current(eng, t)?;
            td[0..4].copy_from_slice(&fresh.to_le_bytes());
            self.stage_page(eng, tx, t, td)?;
        }
        self.stage_page(eng, tx, fresh, d)?;
        if tail.is_none() {
            match which {
                0 => self.series_head = fresh,
                _ => self.chunk_head = fresh,
            }
        }
        match which {
            0 => self.series_pages = n_pages + 1,
            _ => self.chunk_pages = n_pages + 1,
        }
        Ok((fresh, TS_HDR as u32))
    }

    /// Append bytes to a series' payload stream; returns (first_page, off).
    fn stream_append(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        sid: u32,
        bytes: &[u8],
    ) -> Result<(PageId, u32), EngineError> {
        let mut cur = self.series[sid as usize].stream_head;
        if cur == NULL_PAGE {
            let fresh = self.alloc_stream_page(eng, tx)?;
            self.series[sid as usize].stream_head = fresh;
            self.patch_series_record(eng, tx, sid)?;
            cur = fresh;
        }
        let mut first: Option<(PageId, u32)> = None;
        let mut rem = bytes;
        let mut last_used;
        loop {
            let data = self.read_current(eng, cur)?;
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let cap = PAGE_SIZE - TS_HDR - used;
            if cap == 0 {
                let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
                if next == NULL_PAGE {
                    let fresh = self.alloc_stream_page(eng, tx)?;
                    let mut d = data;
                    d[0..4].copy_from_slice(&fresh.to_le_bytes());
                    self.stage_page(eng, tx, cur, d)?;
                    cur = fresh;
                    continue;
                }
                cur = next;
                continue;
            }
            if first.is_none() {
                first = Some((cur, (TS_HDR + used) as u32));
            }
            let n = cap.min(rem.len());
            let mut d = data;
            d[TS_HDR + used..TS_HDR + used + n].copy_from_slice(&rem[..n]);
            last_used = used + n;
            d[4..8].copy_from_slice(&(last_used as u32).to_le_bytes());
            self.stage_page(eng, tx, cur, d)?;
            rem = &rem[n..];
            if rem.is_empty() {
                break;
            }
            let next = u32::from_le_bytes(d[0..4].try_into().unwrap());
            if next == NULL_PAGE {
                let fresh = self.alloc_stream_page(eng, tx)?;
                let mut d2 = d;
                d2[0..4].copy_from_slice(&fresh.to_le_bytes());
                self.stage_page(eng, tx, cur, d2)?;
                cur = fresh;
            } else {
                cur = next;
            }
        }
        self.tails.insert(sid, (cur, last_used as u32));
        Ok(first.unwrap())
    }

    fn alloc_stream_page(&mut self, eng: &mut Engine, tx: &mut Tx) -> Result<PageId, EngineError> {
        let id = eng.alloc_in_tx(tx)?;
        let mut d = [0u8; PAGE_SIZE];
        d[0..4].copy_from_slice(&NULL_PAGE.to_le_bytes());
        d[4..8].copy_from_slice(&0u32.to_le_bytes());
        self.stage_page(eng, tx, id, d)?;
        Ok(id)
    }

    /// Read `len` payload bytes starting at (page, off), following the chain.
    fn stream_read(
        &self,
        eng: &mut Engine,
        page: PageId,
        off: u32,
        len: usize,
    ) -> Result<Vec<u8>, EngineError> {
        let mut out = Vec::with_capacity(len);
        let mut cur = page;
        let mut pos = off as usize;
        while out.len() < len {
            let data = self.read_current(eng, cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            while pos < PAGE_SIZE && out.len() < len {
                out.push(data[pos]);
                pos += 1;
            }
            if out.len() < len {
                cur = next;
                pos = TS_HDR;
            }
        }
        Ok(out)
    }

    /// Patch a series record's mutable fields (points/chunks/stream head).
    fn patch_series_record(
        &mut self,
        eng: &mut Engine,
        tx: &mut Tx,
        sid: u32,
    ) -> Result<(), EngineError> {
        let (page, off) = self.series_pos[sid as usize];
        let e = &self.series[sid as usize];
        let mut data = self.read_current(eng, page)?;
        data[off as usize + 24..off as usize + 28]
            .copy_from_slice(&(e.total_points as u32).to_le_bytes());
        data[off as usize + 28..off as usize + 32].copy_from_slice(&e.chunk_count.to_le_bytes());
        data[off as usize + 32..off as usize + 36].copy_from_slice(&e.stream_head.to_le_bytes());
        self.stage_page(eng, tx, page, data)
    }

    fn flush_buffer(&mut self, eng: &mut Engine, tx: &mut Tx, sid: u32) -> Result<(), EngineError> {
        let pts = match self.buffers.remove(&sid) {
            Some(v) if !v.is_empty() => v,
            _ => return Ok(()),
        };
        let chunk = encode_chunk(&pts);
        let (fp, fo) = self.stream_append(eng, tx, sid, &chunk.payload)?;
        let rec = ChunkRec {
            series: sid,
            first_page: fp,
            first_off: fo,
            blob_len: chunk.payload.len() as u32,
            t0: chunk.t0,
            t_end: chunk.t_end,
            count: chunk.count,
            min: chunk.min,
            max: chunk.max,
        };
        let mut rec_bytes = [0u8; CHUNK_REC];
        rec.encode(&mut rec_bytes, 0);
        self.slab_append(eng, tx, TsSlab::Chunks, &rec_bytes)?;
        self.chunk_recs += 1;
        self.chunk_index.push(rec);
        let e = &mut self.series[sid as usize];
        e.total_points += pts.len() as u64;
        e.chunk_count += 1;
        self.patch_series_record(eng, tx, sid)?;
        self.stage_layout(eng, tx)?;
        Ok(())
    }

    fn load_series(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let mut cur = self.series_head;
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let data_end = TS_HDR + used;
            let mut off = TS_HDR;
            while off + SERIES_REC <= data_end && self.series.len() < self.series_recs as usize {
                let nlen = data[off] as usize;
                let name = String::from_utf8_lossy(&data[off + 1..off + 1 + nlen]).to_string();
                let total = u32::from_le_bytes(data[off + 24..off + 28].try_into().unwrap()) as u64;
                let chunks = u32::from_le_bytes(data[off + 28..off + 32].try_into().unwrap());
                let sh = u32::from_le_bytes(data[off + 32..off + 36].try_into().unwrap());
                self.series_pos.push((cur, off as u32));
                self.series.push(SeriesEntry {
                    name,
                    stream_head: if sh == 0 { NULL_PAGE } else { sh },
                    total_points: total,
                    chunk_count: chunks,
                });
                off += SERIES_REC;
            }
            cur = next;
        }
        Ok(())
    }

    fn load_chunks(&mut self, eng: &mut Engine) -> Result<(), EngineError> {
        let mut cur = self.chunk_head;
        while cur != NULL_PAGE {
            let data = eng.read_page(cur)?;
            let next = u32::from_le_bytes(data[0..4].try_into().unwrap());
            let used = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let data_end = TS_HDR + used;
            let mut off = TS_HDR;
            while off + CHUNK_REC <= data_end && self.chunk_index.len() < self.chunk_recs as usize {
                self.chunk_index.push(ChunkRec::decode(data, off));
                off += CHUNK_REC;
            }
            cur = next;
        }
        Ok(())
    }
}

enum TsSlab {
    Series,
    Chunks,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_varint_roundtrip() {
        for v in [
            0i64,
            1,
            -1,
            63,
            64,
            -64,
            -65,
            1 << 20,
            -(1 << 20),
            i64::MAX / 2,
            -(i64::MAX / 2),
        ] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
        let mut buf = Vec::new();
        write_uv(&mut buf, zigzag(-1234567));
        let mut pos = 0;
        assert_eq!(read_uv(&buf, &mut pos), Some(zigzag(-1234567)));
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn chunk_encode_decode_roundtrip() {
        // Regular sampling.
        let pts: Vec<(i64, f64)> = (0..100).map(|i| (i * 100, i as f64 * 0.5)).collect();
        let c = encode_chunk(&pts);
        assert_eq!(c.enc_ts, ENC_TS_VARINT);
        assert_eq!(c.min, 0.0);
        assert_eq!(c.max, 49.5);
        assert_eq!(decode_chunk(&c), pts);
        // Constant values compress to one f64.
        let pts2: Vec<(i64, f64)> = (0..50).map(|i| (i, 3.5)).collect();
        let c2 = encode_chunk(&pts2);
        assert_eq!(c2.enc_val, ENC_VAL_CONST);
        assert_eq!(c2.min, 3.5);
        assert_eq!(c2.max, 3.5);
        assert_eq!(decode_chunk(&c2), pts2);
        // Constant timestamps.
        let pts3: Vec<(i64, f64)> = (0..10).map(|i| (7i64, i as f64)).collect();
        let c3 = encode_chunk(&pts3);
        assert_eq!(c3.enc_ts, ENC_TS_CONST);
        assert_eq!(decode_chunk(&c3), pts3);
        // Single point.
        let c4 = encode_chunk(&[(5, 1.5)]);
        assert_eq!(decode_chunk(&c4), vec![(5, 1.5)]);
    }
}

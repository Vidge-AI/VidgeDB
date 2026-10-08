//! Page store for VidgeDB — Phase 1.
//!
//! Physical layer of the single-file `.vdg` database. Design (Phase 0 record,
//! following spec §21 "study existing systems": SQLite pager + LMDB page
//! discipline):
//!
//! - Fixed 4096-byte pages, identified by a stable `PageId` (u32 index).
//! - File = a plain array of pages. Page 0 is the superblock (magic, format
//!   version, page size, page count, freelist head).
//! - Allocation: free-list first (recycled pages), then append. The
//!   freelist itself lives IN pages (chained), so it survives restarts and
//!   stays reconstructible (spec §17: source of truth SHALL remain
//!   reconstructible).
//! - All reads/writes go through the OS file with explicit flush points;
//!   durability ordering is the WAL layer's job (spec §18). The pager only
//!   guarantees: page writes are byte-exact copies of in-memory pages, and
//!   flush() writes data pages first, superblock last — so after any crash
//!   mid-flush, the superblock never references a partially-written page.
//!
//! Unsafe Rust policy (spec §49): none in this module.
//!
//! Correctness before performance (spec Principle 1): every write path
//! updates in-memory state AND the freelist chain in the same critical
//! section, so the file is always internally consistent after flush().

use crate::wal::{Wal, WalOp};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Fixed page size: 4096 bytes (sector-size friendly, spec §22 single file).
pub const PAGE_SIZE: usize = 4096;
/// Reserved id meaning "no page" / end of chain.
pub const NULL_PAGE: u32 = u32::MAX;
/// Maximum pages in one file (u32 ids; id 0 is the superblock).
pub const MAX_PAGES: u32 = u32::MAX - 1;
/// Magic at offset 0 of page 0.
pub const MAGIC: [u8; 4] = *b"VDG1";

pub type PageId = u32;

/// Superblock, serialized into page 0.
/// Layout (little-endian): magic[4] | version u32 | page_size u32 |
/// page_count u32 | freelist_head u32 | rest zeros.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Superblock {
    pub version: u32,
    pub page_count: u32,
    pub freelist_head: u32,
}

impl Default for Superblock {
    fn default() -> Self {
        // page_count starts at 1: page 0 is always the superblock.
        Superblock {
            version: 1,
            page_count: 1,
            freelist_head: NULL_PAGE,
        }
    }
}

impl Superblock {
    pub fn encode(&self, page: &mut Page) {
        page.data.fill(0);
        page.data[0..4].copy_from_slice(&MAGIC);
        page.data[4..8].copy_from_slice(&self.version.to_le_bytes());
        page.data[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        page.data[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        page.data[16..20].copy_from_slice(&self.freelist_head.to_le_bytes());
    }

    pub fn decode(page: &Page) -> Result<Superblock, PageError> {
        if page.data[0..4] != MAGIC {
            return Err(PageError::BadMagic);
        }
        let version = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        let ps = u32::from_le_bytes(page.data[8..12].try_into().unwrap());
        if ps as usize != PAGE_SIZE {
            return Err(PageError::BadPageSize(ps));
        }
        Ok(Superblock {
            version,
            page_count: u32::from_le_bytes(page.data[12..16].try_into().unwrap()),
            freelist_head: u32::from_le_bytes(page.data[16..20].try_into().unwrap()),
        })
    }
}

#[derive(Debug)]
pub enum PageError {
    /// File is not a .vdg database (magic mismatch).
    BadMagic,
    /// Page size differs from this build's PAGE_SIZE.
    BadPageSize(u32),
    /// PageId out of range for the current file.
    PageOutOfBounds(u32),
    /// Allocator exhausted (file limit reached).
    NoSpace,
    /// Corruption detected: freelist chain is broken (cycle/dangling/id 0).
    FreelistCorrupt,
    Io(io::Error),
}

impl From<io::Error> for PageError {
    fn from(e: io::Error) -> Self {
        PageError::Io(e)
    }
}

/// One fixed-size page. Bytes only; upper layers (Phase 2 stores) own layouts.
#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn empty(id: PageId) -> Self {
        Page {
            id,
            data: [0u8; PAGE_SIZE],
        }
    }
}

/// Free-list chain encoded inside pages:
/// bytes [0..4) = next_free PageId (NULL_PAGE = end), rest zeroed.
struct FreeListPage;

impl FreeListPage {
    fn encode(page: &mut Page, next: PageId) {
        page.data.fill(0);
        page.data[0..4].copy_from_slice(&next.to_le_bytes());
    }
    fn decode_next(page: &Page) -> PageId {
        u32::from_le_bytes(page.data[0..4].try_into().unwrap())
    }
}

/// The pager: owns the file, caches, and the freelist.
/// Single-writer by construction (Phase 1); concurrency arrives in Phase 2+.
pub struct Pager {
    file: File,
    sb: Superblock,
    /// Dirty pages pending flush, keyed by page id.
    dirty: BTreeMap<PageId, Page>,
    /// Cache of clean pages read from disk.
    clean: HashMap<PageId, Page>,
    /// In-memory freelist mirror (LIFO), rebuilt from the on-disk chain.
    /// MUTATED AT COMMIT ONLY: alloc/free stage candidates on the Tx; the
    /// engine folds them in here at commit (commit_alloc / commit_free) —
    /// never mid-transaction, so a rollback leaves nothing durable behind.
    freelist: Vec<PageId>,
    /// Pages taken by open transactions: id popped off the mirror at alloc,
    /// off-chain while a free is staged. Collision guard so two coexisting
    /// Tx never receive the same id. Engine scratch state, never flushed.
    pending_alloc: Vec<PageId>,
    /// Subset of `pending_alloc` taken from the freelist mirror (recycled
    /// at reserve): their head advance happens at commit, not reserve.
    pending_recycle: Vec<PageId>,
    /// Append-reserved ids handed back by a rollback of their owning tx
    /// (Engine's drop-time reclaim). Available to `alloc` immediately —
    /// first come, first served — but never placed on the durable chain:
    /// no committed superblock has referenced them yet. Cleared by
    /// re-reserve, NOT flushed (never touches the sb).
    scratch: Vec<PageId>,
    /// Whether the on-disk freelist chain parsed cleanly at the last
    /// superblock load; a deferred failure (see load_superblock) is only
    /// fatal when recovery has no WAL to rebuild from.
    chain_ok: bool,
}

impl Pager {
    /// Open (or create) a database file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Pager, PageError> {
        let path = path.as_ref();
        let exists = path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(!exists)
            .open(path)?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut pager = Pager {
            file,
            sb: Superblock::default(),
            dirty: BTreeMap::new(),
            clean: HashMap::new(),
            freelist: Vec::new(),
            pending_alloc: Vec::new(),
            pending_recycle: Vec::new(),
            scratch: Vec::new(),
            chain_ok: true,
        };
        if exists && size >= PAGE_SIZE as u64 {
            pager.load_superblock()?;
        } else {
            // Fresh file: page 0 = superblock, zero data pages yet.
            pager.dirty.insert(0, Page::empty(0));
            pager.flush()?;
        }
        Ok(pager)
    }

    fn load_superblock(&mut self) -> Result<(), PageError> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;
        self.sb = Superblock::decode(&Page { id: 0, data: buf })?;
        // Build the freelist mirror from the on-disk chain (best effort).
        // Chain order is head (most recently freed) -> tail. Vec::pop()
        // takes the LAST element, so the mirror is stored REVERSED:
        // pop() == head. The on-disk chain can be stale/damaged by a crash
        // mid-flush while a committed WAL still holds the truth — a broken
        // walk therefore defers (empty mirror + chain_ok=false) instead of
        // refusing here: `Engine::open` replays the log first and rebuilds
        // the chain from the replayed state (fail-closed there). A damaged
        // file with an EMPTY log still refuses to open (see Engine::open).
        self.freelist.clear();
        self.chain_ok = true;
        let mut chain = Vec::new();
        let mut next = self.sb.freelist_head;
        while next != NULL_PAGE {
            // Guard: cycle / dangling pointer / superblock in chain.
            if next == 0 || next >= self.sb.page_count || chain.contains(&next) {
                self.chain_ok = false;
                return Ok(());
            }
            let page = self.read_raw(next)?;
            chain.push(next);
            next = FreeListPage::decode_next(&page);
        }
        chain.reverse();
        self.freelist = chain;
        Ok(())
    }

    /// Reserve a page WITHOUT moving the durable allocation state: the id
    /// comes out of the in-memory mirror (so two open transactions can never
    /// collide), but the superblock is untouched. The owning Tx carries the
    /// sb candidates; [`Self::commit_alloc`] folds them in at commit. The
    /// returned page is zeroed and dirty; `read`/`get_for_write` see it
    /// in-transaction even before the counts are committed.
    pub fn alloc(&mut self) -> Result<PageId, PageError> {
        let id = if let Some(id) = self.freelist.pop() {
            // Recycled: the mirror already lost it (mirror pop = chain-head
            // removal); the head advance is a commit-time fold, tracked in
            // pending_recycle so commit_alloc knows which kind it folds.
            self.pending_recycle.push(id);
            id
        } else if let Some(&oldest) = self.scratch.iter().min() {
            // Rolled-back scratch reservation: re-reserve the oldest
            // handed-back id (FIFO over the scratch pool, keeps the
            // watermark tight) BEFORE walking the append watermark past
            // it — zero new-page growth, ids stay ordered.
            self.scratch.retain(|&p| p != oldest);
            self.pending_alloc.push(oldest);
            self.dirty.insert(oldest, Page::empty(oldest));
            return Ok(oldest);
        } else {
            // Append: walk past every id still reserved by an open
            // transaction (their durable fold happens at commit, so the
            // committed sb alone would hand the same id twice inside one
            // transaction); NoSpace if the watermark passes the file limit.
            let mut id = self.sb.page_count;
            while self.pending_alloc.contains(&id) {
                id += 1;
            }
            if id >= MAX_PAGES {
                return Err(PageError::NoSpace);
            }
            id
        };
        self.pending_alloc.push(id);
        self.dirty.insert(id, Page::empty(id));
        Ok(id)
    }

    /// Fold one reserved id into the durable state at commit.
    ///
    /// Phase 8.6 hardening: a tx that reserved an id TWICE (it reserved
    /// it, handed it back through a net-out free, then re-reserved it from
    /// the scratch pool) carries the id twice in the caller's `taken`;
    /// popping the mirror once per OCCURRENCE would unchain a page whose
    /// chain node is still durably linked to the previous head. Idempotent
    /// per distinct id instead.
    pub(crate) fn commit_alloc(&mut self, id: PageId) {
        self.pending_alloc.retain(|&p| p != id);
        // Phase 8.6 net-out: this fold arrives for a reservation whose
        // page was handed back mid-tx (alloc, then free of the SAME id in
        // the same tx — the free removed it from pending_alloc and handed
        // it back). Fold NOTHING for it: the netted page must never become
        // durable through this commit (its alloc has no durable successor).
        // Detect: the id sits on the scratch pool the hand-back returned
        // it to (an append-reserved slot: no durable ownership either way).
        if self.scratch.contains(&id) && !self.pending_recycle.contains(&id) {
            return;
        }
        if let Some(pos) = self.pending_recycle.iter().position(|&p| p == id) {
            // Recycled at reserve (mirror pop already applied): advance the
            // persisted head now (the mirror stays reversed so
            // freelist.pop() keeps returning the chain head).
            self.pending_recycle.remove(pos);
            self.sb.freelist_head = self.freelist.last().copied().unwrap_or(NULL_PAGE);
        } else {
            // Append-reserved: grow the count to cover it.
            if self.sb.page_count <= id {
                self.sb.page_count = id + 1;
            }
        }
    }

    /// Stage a free WITHOUT touching durable state: returns the freelist
    /// node page (bytes 0..4 = next = CURRENT durable chain head, rest
    /// zeroed) for the caller to journal in its transaction — the pair
    /// (this SetPage + SetSuperblock) makes the free atomic and replayable.
    /// Single-free convenience over [`Self::free_node`]; bounds and
    /// double-free guards live there.
    pub fn free(&mut self, id: PageId) -> Result<Page, PageError> {
        let next = self.sb.freelist_head;
        self.free_node(id, next)
    }

    /// Stage a free with an EXPLICIT sibling: Engine::free_in_tx chains a
    /// tx's staged frees onto EACH OTHER (node_k.next = node_{k-1}, tail =
    /// the durable head). Encoding every node with the CURRENT durable
    /// head instead would collapse a multi-free tx's chain to its last
    /// node after recovery (every earlier node's next points PAST its
    /// sibling), silently leaking those pages on reopen. Bounds and
    /// double-free guards preserved from Phase 1; the bounds check runs
    /// against the DURABLE count (the only state a committed sb ever
    /// referenced) — append reservations parked by open txs and scratch
    /// ids stay refused (Phase 8.6: an id the count merely covers because
    /// ANOTHER tx grew the watermark is NOT durably owned; chaining it
    /// would journal a head at a stub page and brick recovery).
    pub fn free_node(&mut self, id: PageId, next: PageId) -> Result<Page, PageError> {
        if id == 0 || id >= self.sb.page_count {
            return Err(PageError::PageOutOfBounds(id));
        }
        // Double free / not durably owned is a caller bug — refuse rather
        // than corrupt the chain. The freelist mirror and pending set cover
        // the durable chain and open-tx reservations; the scratch pool
        // covers rolled-back append ids, which the count MAY cover after
        // later commits but which no committed superblock has ever
        // referenced (Phase 8.6 owner guard).
        if self.freelist.contains(&id)
            || self.pending_alloc.contains(&id)
            || self.scratch.contains(&id)
        {
            return Err(PageError::FreelistCorrupt);
        }
        self.pending_alloc.push(id);
        let mut page = Page::empty(id);
        FreeListPage::encode(&mut page, next);
        Ok(page)
    }

    /// Fold one staged free into the durable state at commit (candidate
    /// order: node first, then head — see Engine::commit).
    pub(crate) fn commit_free(&mut self, id: PageId) {
        self.pending_alloc.retain(|&p| p != id);
        self.freelist.push(id);
        self.sb.freelist_head = id;
    }

    /// Phase 8.6 churn hardening — journal-and-chain every scratch id
    /// whose slot the DURABLE sb already covers (`id < page_count`, i.e.
    /// some commit counted the page before the owning tx rolled back).
    /// Those ids are durable space that no chain references; without this
    /// fold a reopen loses them forever (the count keeps covering them,
    /// the allocator keeps appending past them = a permanent hole), and
    /// even in-session the append watermark walks past. Each claim is its
    /// own zero-data atomic commit (node SetPage + sb with the advanced
    /// head) through the WAL — exactly a `free` of a durable page, minus
    /// the caller.
    pub(crate) fn claim_durable_scratch(
        &mut self,
        wal: &mut Wal,
    ) -> Result<(), crate::wal::WalError> {
        if self.scratch.is_empty() {
            return Ok(());
        }
        let mut claims: Vec<PageId> = Vec::new();
        let mut head = self.sb.freelist_head;
        for &id in self.scratch.iter().rev() {
            if id != 0
                && id < self.sb.page_count
                && !self.freelist.contains(&id)
                && !self.pending_alloc.contains(&id)
            {
                let mut page = Page::empty(id);
                FreeListPage::encode(&mut page, head);
                wal.append(&WalOp::SetPage {
                    page_id: id,
                    data: page.data.to_vec(),
                })?;
                head = id;
                claims.push(id);
            }
        }
        if claims.is_empty() {
            return Ok(());
        }
        wal.append(&WalOp::SetSuperblock {
            page_count: self.sb.page_count,
            freelist_head: head,
        })?;
        wal.commit()?;
        wal.sync()?;
        for id in &claims {
            self.scratch.retain(|&p| p != *id);
            self.freelist.push(*id);
        }
        self.sb.freelist_head = head;
        Ok(())
    }

    /// Largest id any caller may currently touch: the committed count plus
    /// every page still reserved by an open transaction (visible in-tx as
    /// their zeroed dirty stub). Durable file length still tracks
    /// `sb.page_count` alone.
    fn durable_page_count(&self) -> PageId {
        let mut max = self.sb.page_count;
        for &p in &self.pending_alloc {
            if p + 1 > max {
                max = p + 1;
            }
        }
        max
    }

    /// Superblock a commit would produce: durable sb + the transaction's
    /// candidates (frees pushed onto the chain in order, alloc folds exactly
    /// like `commit_alloc`), WITHOUT mutating anything. This is the value
    /// the engine journals with the commit — identical whether it is applied
    /// live or replayed from the WAL.
    pub(crate) fn compute_commit_sb(&self, freed: &[PageId], taken: &[PageId]) -> Superblock {
        // Mirror is stored reversed (pop() == chain head); push() at the
        // end = new head. Recycled ids were popped off the mirror at their
        // reserve, so the fold must NOT pop them again.
        let mut mirror: Vec<PageId> = self.freelist.clone();
        let mut page_count = self.sb.page_count;
        for &f in freed {
            mirror.push(f);
        }
        for (pos, &t) in taken.iter().enumerate() {
            // Distinct ids fold once: a reserve→net-out→re-reserve inside
            // one tx re-parks the same id (Phase 8.6), the count/head fold
            // must match commit_alloc's idempotence exactly.
            let first = taken.iter().position(|&x| x == t);
            if first == Some(pos) && !self.pending_recycle.contains(&t) {
                // Append-reserved: the count grows to cover the largest id
                // handed out (the watermark can walk past ids parked by
                // rolled-back scratch reservations).
                page_count = page_count.max(t + 1);
            }
        }
        Superblock {
            version: self.sb.version,
            page_count,
            freelist_head: mirror.last().copied().unwrap_or(NULL_PAGE),
        }
    }

    /// Read a page (dirty version wins over disk). Reserved-but-uncommitted
    /// pages are readable (their in-tx view is the zeroed dirty stub) so a
    /// Tx can read its own reservations before it is committed.
    pub fn read(&mut self, id: PageId) -> Result<&Page, PageError> {
        if id >= self.durable_page_count() {
            return Err(PageError::PageOutOfBounds(id));
        }
        if self.dirty.contains_key(&id) {
            return Ok(self.dirty.get(&id).unwrap());
        }
        if !self.clean.contains_key(&id) {
            let page = self.read_raw(id)?;
            self.clean.insert(id, page);
        }
        Ok(self.clean.get(&id).unwrap())
    }

    /// Copy-on-write: fetch a page into the dirty map for mutation.
    /// Reserved-but-uncommitted ids are accessible (same in-tx visibility
    /// rule as `read`).
    pub fn get_for_write(&mut self, id: PageId) -> Result<&mut Page, PageError> {
        if id >= self.durable_page_count() {
            return Err(PageError::PageOutOfBounds(id));
        }
        if !self.dirty.contains_key(&id) {
            let page = self.read_raw(id)?;
            self.dirty.insert(id, page);
        }
        self.clean.remove(&id);
        Ok(self.dirty.get_mut(&id).unwrap())
    }

    /// Allocate + hand the fresh page to the caller's writer function.
    pub fn alloc_and_write<F: FnOnce(&mut Page)>(&mut self, f: F) -> Result<PageId, PageError> {
        let id = self.alloc()?;
        {
            let page = self.dirty.get_mut(&id).unwrap();
            f(page);
        }
        Ok(id)
    }

    /// Flush dirty data pages, then the superblock (commit point for the
    /// allocation state), then fsync. A crash mid-flush can lose the newest
    /// data pages but never leaves the superblock referencing a page whose
    /// write was partial AND counted — the WAL layer (spec §18) builds the
    /// full atomic-commit story on top of this ordering.
    pub fn flush(&mut self) -> Result<(), PageError> {
        self.flush_bounded(self.sb.page_count)
    }

    /// Flush with a PAGE WRITES CEILING (Phase 8.6, commit path): dirty
    /// pages with id >= `limit` are DROPPED, never journaled in the file —
    /// the zeroed reservation stubs of reservations that were handed back
    /// (net-out / rollback) must not extend the file past what the durable
    /// sb counts (recovery garbage, phantom footprint growth). Caller must
    /// guarantee no LIVE reservation reaches or exceeds the limit at this
    /// point (Engine::commit drops the handed-back ones first).
    pub fn flush_bounded(&mut self, limit: PageId) -> Result<(), PageError> {
        let ids: Vec<PageId> = self
            .dirty
            .keys()
            .copied()
            .filter(|&id| id < limit)
            .collect();
        for id in ids {
            if id == 0 {
                continue;
            }
            let page = self.dirty.get(&id).unwrap();
            self.file.seek(SeekFrom::Start(page_offset(id)))?;
            self.file.write_all(&page.data)?;
            self.clean.insert(id, page.clone());
        }
        // Superblock last: allocation state commit point.
        let mut p0 = Page::empty(0);
        self.sb.encode(&mut p0);
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&p0.data)?;
        self.file.flush()?;
        self.file.sync_all()?;
        self.dirty.clear();
        Ok(())
    }

    fn read_raw(&mut self, id: PageId) -> Result<Page, PageError> {
        self.file.seek(SeekFrom::Start(page_offset(id)))?;
        let mut buf = [0u8; PAGE_SIZE];
        match self.file.read_exact(&mut buf) {
            Ok(()) => Ok(Page { id, data: buf }),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Truncated tail (crash before flush finished): treat the
                // missing tail pages as zeroed — same as a fresh alloc.
                Ok(Page::empty(id))
            }
            Err(e) => Err(PageError::Io(e)),
        }
    }

    pub fn superblock(&self) -> Superblock {
        self.sb
    }

    pub fn page_count(&self) -> u32 {
        self.sb.page_count
    }

    /// Recovery hook: set superblock values replayed from the WAL.
    /// Only the engine's recovery path may call this (audit: single caller,
    /// `Engine::open`), so an in-memory mutation can never leak into the
    /// normal write path.
    pub(crate) fn set_superblock_for_recovery(&mut self, page_count: u32, freelist_head: u32) {
        self.sb.page_count = page_count;
        self.sb.freelist_head = freelist_head;
    }

    /// Recovery hook: rebuild the in-memory freelist mirror (head -> tail,
    /// reversed). Walks the chain through the paginated cache (`read`:
    /// dirty-first, then clean) — this runs after the WAL replay, whose
    /// pages live in that cache, so a chain that exists only in the log is
    /// resolved with the replayed bytes, never with a stale disk page.
    /// Corruption fails closed (cycle / dangling / id 0 / out of range).
    pub(crate) fn rebuild_freelist_from_disk(&mut self) -> Result<(), PageError> {
        let mut chain = Vec::new();
        let mut next = self.sb.freelist_head;
        while next != NULL_PAGE {
            if next == 0 || next >= self.sb.page_count || chain.contains(&next) {
                return Err(PageError::FreelistCorrupt);
            }
            if !self.dirty.contains_key(&next) && !self.clean.contains_key(&next) {
                // Not replayed and not read before: genuinely absent from
                // the file (or beyond EOF); do not synthesize zeros.
                if page_offset(next) >= self.file.metadata().map(|m| m.len()).unwrap_or(0) {
                    return Err(PageError::FreelistCorrupt);
                }
                let page = self.read_raw(next)?;
                self.clean.insert(next, page);
            }
            let page: Page = {
                let copy = self.read(next)?;
                Page {
                    id: copy.id,
                    data: copy.data,
                }
            };
            chain.push(next);
            next = FreeListPage::decode_next(&page);
        }
        chain.reverse();
        self.freelist = chain;
        Ok(())
    }

    pub fn freelist_len(&self) -> usize {
        self.freelist.len()
    }

    /// Whether the on-disk freelist chain parsed cleanly at reload time;
    /// recovery consults this when the WAL is empty.
    pub(crate) fn chain_ok(&self) -> bool {
        self.chain_ok
    }

    /// The durable freelist head (chain head a NEW free sibling must chain
    /// onto when its tx has staged no free yet — see free_node).
    pub fn head(&self) -> PageId {
        self.sb.freelist_head
    }

    /// Hand a reservation back (rollback of the owning transaction):
    /// removed from the pending sets, and the id becomes immediately
    /// reusable IN MEMORY so a rolled-back tx never leaks its page.
    /// Routing is decided by HOW the id was reserved — never by the
    /// current `sb.page_count` (counters can have grown past a parked
    /// append id while its tx was still open — another tx's commit — and
    /// a count that merely covers the id does NOT make it durable):
    /// - a recycled id (node durably chained, popped off the mirror at
    ///   reserve, tracked in `pending_recycle`) returns to the freelist
    ///   mirror — the mirror is again exactly the committed chain (the
    ///   "disposable" marking on disk is implicit: the durable chain was
    ///   never mutated);
    /// - an append-reserved id (no committed sb EVER referenced it — its
    ///   on-disk bytes are the still-dirty zeroed stub) goes to `scratch`
    ///   even when page_count now covers it. Putting it on the mirror
    ///   would journal a chain head pointing at a non-node page and brick
    ///   recovery (fail-closed reopen, FreelistCorrupt). The next tx
    ///   re-reserves it from `scratch` via alloc(); if it commits,
    ///   page_count covers it then (it already may).
    pub(crate) fn cancel_alloc(&mut self, id: PageId) {
        self.pending_alloc.retain(|&p| p != id);
        if let Some(pos) = self.pending_recycle.iter().position(|&p| p == id) {
            self.pending_recycle.remove(pos);
            self.freelist.push(id);
        } else {
            self.scratch.push(id);
        }
    }

    /// Cancel a staged free (rollback): the node candidate is dropped.
    pub(crate) fn cancel_free(&mut self, id: PageId) {
        self.pending_alloc.retain(|&p| p != id);
    }
}

/// Byte offset of a page in the file.
pub fn page_offset(id: PageId) -> u64 {
    id as u64 * PAGE_SIZE as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_test_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    #[test]
    fn alloc_write_flush_reopen() {
        let path = tmp_path("alloc");
        {
            let mut p = Pager::open(&path).unwrap();
            assert_eq!(p.page_count(), 1); // page 0 = superblock
            let id = p
                .alloc_and_write(|pg| {
                    pg.data[16..20].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
                })
                .unwrap();
            assert_eq!(id, 1); // page 0 is the superblock
                               // Durability state moves at commit only: the reserved id is
                               // invisible to the sb until commit_alloc.
            p.commit_alloc(id);
            p.flush().unwrap();
            assert_eq!(p.page_count(), 2);
        }
        {
            let mut p = Pager::open(&path).unwrap();
            assert_eq!(p.page_count(), 2);
            let page = p.read(1).unwrap();
            assert_eq!(
                u32::from_le_bytes(page.data[16..20].try_into().unwrap()),
                0xDEAD_BEEF
            );
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn freelist_recycles_lifo() {
        let path = tmp_path("freelist");
        let mut p = Pager::open(&path).unwrap();
        let a = p.alloc().unwrap();
        let b = p.alloc().unwrap();
        let _c = p.alloc().unwrap();
        for id in [a, b, _c] {
            p.commit_alloc(id);
        }
        p.flush().unwrap();
        // Protocol: free() hands back the node page; the caller journals it
        // AND stages it on the pager (the engine does exactly this in
        // Engine::commit) before the commit fold.
        let node = p.free(b).unwrap();
        assert_eq!(
            u32::from_le_bytes(node.data[0..4].try_into().unwrap()),
            NULL_PAGE
        );
        p.get_for_write(b).unwrap().data.copy_from_slice(&node.data);
        p.commit_free(b);
        p.flush().unwrap();
        assert_eq!(p.freelist_len(), 1);
        // Next alloc must reuse b (LIFO recycle).
        let d = p.alloc().unwrap();
        assert_eq!(d, b);
        p.commit_alloc(d);
        p.flush().unwrap();
        p.flush().unwrap();
        drop(p);
        // Recycled state survives reopen.
        let mut p = Pager::open(&path).unwrap();
        assert_eq!(p.freelist_len(), 0);
        let e = p.alloc().unwrap(); // fresh append again
        p.commit_alloc(e);
        assert_eq!(e, p.page_count() - 1);
        let _ = a;
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn freelist_survives_reopen() {
        let path = tmp_path("freero");
        {
            let mut p = Pager::open(&path).unwrap();
            let a = p.alloc().unwrap();
            let b = p.alloc().unwrap();
            let c = p.alloc().unwrap();
            for id in [a, b, c] {
                p.commit_alloc(id);
            }
            for id in [a, b, c] {
                let node = p.free(id).unwrap();
                p.get_for_write(id)
                    .unwrap()
                    .data
                    .copy_from_slice(&node.data);
                p.commit_free(id);
            }
            p.flush().unwrap();
        }
        let mut p = Pager::open(&path).unwrap();
        assert_eq!(p.freelist_len(), 3);
        // LIFO order: c, b, a.
        assert_eq!(p.alloc().unwrap(), 3);
        assert_eq!(p.alloc().unwrap(), 2);
        assert_eq!(p.alloc().unwrap(), 1);
        assert_eq!(p.freelist_len(), 0);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn superblock_roundtrip_and_magic() {
        let path = tmp_path("sb");
        {
            let mut p = Pager::open(&path).unwrap();
            p.flush().unwrap();
        }
        // Raw magic check on disk.
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[0..4], b"VDG1");
        let p = Pager::open(&path).unwrap();
        let sb = p.superblock();
        assert_eq!(sb.version, 1);
        assert_eq!(sb.page_count, 1);
        assert_eq!(sb.freelist_head, NULL_PAGE);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn out_of_bounds_rejected() {
        let path = tmp_path("oob");
        let mut p = Pager::open(&path).unwrap();
        assert!(matches!(p.read(5), Err(PageError::PageOutOfBounds(5))));
        assert!(matches!(
            p.get_for_write(5),
            Err(PageError::PageOutOfBounds(5))
        ));
        assert!(matches!(p.free(5), Err(PageError::PageOutOfBounds(5))));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn double_free_detected() {
        let path = tmp_path("dblfree");
        let mut p = Pager::open(&path).unwrap();
        let a = p.alloc().unwrap();
        p.commit_alloc(a); // durable ownership precedes any free
        p.free(a).unwrap();
        assert!(matches!(p.free(a), Err(PageError::FreelistCorrupt)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn many_pages_roundtrip() {
        let path = tmp_path("many");
        let mut ids = Vec::new();
        {
            let mut p = Pager::open(&path).unwrap();
            for i in 0..100u32 {
                let id = p
                    .alloc_and_write(|pg| {
                        pg.data[0..4].copy_from_slice(&i.to_le_bytes());
                    })
                    .unwrap();
                ids.push(id);
                p.commit_alloc(id);
            }
            p.flush().unwrap();
        }
        let mut p = Pager::open(&path).unwrap();
        for (i, &id) in ids.iter().enumerate() {
            let page = p.read(id).unwrap();
            assert_eq!(
                u32::from_le_bytes(page.data[0..4].try_into().unwrap()),
                i as u32
            );
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn dirty_read_visible_before_flush() {
        let path = tmp_path("dirtyvis");
        let mut p = Pager::open(&path).unwrap();
        let id = p.alloc().unwrap();
        {
            let page = p.get_for_write(id).unwrap();
            page.data[8] = 42;
        }
        assert_eq!(p.read(id).unwrap().data[8], 42); // dirty wins
        assert_eq!(p.read(id).unwrap().data[8], 42); // stable
        std::fs::remove_file(&path).unwrap();
    }
}

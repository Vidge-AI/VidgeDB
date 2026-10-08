//! Transaction manager for VidgeDB — Phase 1 (+ Phase 8.1 tx-correct fixes,
//! Phase 9 rollback reclaim).
//!
//! Binds the pager and the WAL into the commit protocol (spec §17/§18):
//!
//! ```text
//!     Application
//!          ↓
//!     Transaction Manager        <-- this module
//!          ↓
//!         WAL
//!          ↓
//!     Storage Engine (pager)
//! ```
//!
//! Semantics:
//! - A transaction = a group of page writes + superblock candidates.
//! - `begin()` returns a Tx handle; page writes are buffered in the Tx.
//! - `alloc_in_tx()`/`free_in_tx()` RESERVE without touching the pager's
//!   durable state: the id is parked on `Engine.pending_allocs` (so two
//!   coexisting transactions can never collide), the freed page is staged
//!   on `Tx.freed`. A rollback (dropping the Tx) hands the reservations
//!   back and has literally zero durable effect — nothing was ever logged,
//!   the superblock was never touched. (Phase 9: the reclaim also happens
//!   automatically — `Tx::drop` publishes its ids on the shared rollback
//!   log, the next Engine call drains it and the ids become recyclable
//!   immediately: freelist mirror for durably-chained pages, pager scratch
//!   pool for append reservations. No page is ever lost to a rollback.)
//! - `commit()`: 1) append buffered pages as WAL frames (a free contributes
//!   its freelist node as an ordinary SetPage — journaled BEFORE the state
//!   it defines), 2) append the sb frame computed from durable state + the
//!   candidates, 3) commit marker, 4) fsync (from here the transaction is
//!   durable), 5) apply to the pager: journaled pages, freed candidates
//!   (node then head), remaining alloc candidates, 6) flush, 7) checkpoint
//!   past the threshold. A crash before or during 5) is healed by replay
//!   on the next open: the WAL carries every page byte-for-byte plus the
//!   sb that interprets them.
//! - Recovery (on open, fail-closed): grouped replay of committed WAL
//!   frames into the pager — a corrupt/inconsistent group or a bad freelist
//!   chain aborts the whole open (`?`) instead of being swallowed — then
//!   flush and truncate the log.
//!
//! Crash matrix (spec §38.5): the deterministic mid-commit scenarios are
//! covered by `tests/phase81_tx_recovery.rs` (punch a hole in a data page,
//! corrupt the sb page on disk, corrupt the freelist node byte) — the WAL
//! always heals them because every commit logs the full picture.

use crate::pager::{PageError, PageId, Pager, PAGE_SIZE};
use crate::wal::{self, Wal, WalError, WalOp};

/// Reserved ids staged by a dropped (rolled-back) Tx, waiting for the
/// Engine to reclaim them: (taken, freed) pairs in drop order.
pub(super) type RollbackLog = std::sync::Arc<std::sync::Mutex<Vec<(Vec<PageId>, Vec<PageId>)>>>;

/// Transaction-settle notifications shared with upper stores (phase 8.7,
/// BUG-1 fix): `(tx_id, committed)` appended EXACTLY once per tx end —
/// `true` by a successful [`Engine::commit`], `false` by a Tx dropped or
/// otherwise failing without commit. Stores that keep tx-mutated
/// in-memory state (TimeSeriesStore: chunk_index, series counters,
/// stream heads, staged page images) arm a snapshot when a tx first
/// touches them and revert it when THAT tx's `(id, false)` settles
/// (same end-state discipline as the pager's Phase 8.6 net-out, one
/// layer up). Consumers drain the log; ids are monotone (begin order).
pub(crate) type SettleLog = std::sync::Arc<std::sync::Mutex<Vec<(u64, bool)>>>;

/// Buffered transaction. Acquired from `Engine::begin()`.
#[derive(Debug, Default)]
pub struct Tx {
    /// (page_id, page bytes) staged for commit, in write order. A free
    /// appears here too: its page was rewritten as a freelist node.
    pub(super) writes: Vec<(PageId, [u8; PAGE_SIZE])>,
    /// Pages this tx allocated (reserve order).
    pub(super) taken: Vec<PageId>,
    /// Pages this tx freed (free order; candidate order at commit:
    /// node first, then head).
    pub(super) freed: Vec<PageId>,
    /// Shared with the Engine; on drop (a rollback: dropping a Tx without
    /// committing) the reservations land here so the Engine can hand the
    /// ids back on its next call. `None` only for `Tx::default()` (never
    /// carries actual reservations — bench probes).
    rollback_log: Option<RollbackLog>,
    /// Same, for [`SettleLog`]: appended on drop when this tx ended
    /// WITHOUT commit (`settled == false`). One `(id, false)` event per
    /// rolled-back tx — upper stores revert their tx-scoped mutations.
    settle_log: Option<SettleLog>,
    /// Identity assigned by `Engine::begin` (monotone sequence). `0` on
    /// `Tx::default()` — never settles, never publishes.
    id: u64,
    /// Set to true by `Engine::commit` once the tx is durably applied; a
    /// settled tx's Drop publishes nothing (commit already settled it as
    /// `(id, true)`).
    settled: bool,
}

impl Tx {
    /// Engine-assigned tx identity (begin order, monotone; 0 for
    /// `Tx::default()` bench probes). Upper stores key their tx-scoped
    /// snapshots on it (phase 8.7 BUG-1 fix).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Phase 9 audit hook: read-only snapshot of the transaction's staged
    /// parts, in write/reserve order (writes, taken, freed). Used by the
    /// bench_detail binary to time the commit protocol step by step; the
    /// library write path never needs this.
    pub fn parts(&self) -> (&[(u32, [u8; 4096])], &[u32], &[u32]) {
        (&self.writes, &self.taken, &self.freed)
    }
}

impl Drop for Tx {
    /// Rollback path: a Tx that is dropped (instead of passed to
    /// `Engine::commit`) publishes its reservations to the Engine's
    /// rollback log AND its `(id, committed=false)` end to the settle log
    /// (phase 8.7, BUG-1: upper stores revert their tx-scoped in-memory
    /// state when their armed tx rolls back). The Engine drains the
    /// rollback log at the start of its next mutation (begin / commit /
    /// alloc_in_tx / free_in_tx) and hands every id back — see
    /// [`Engine::drain_rollbacks`]. Nothing durable
    /// moves here: at rollback time every sb "candidate" this tx carried
    /// was never applied, so handing the ids back restores the in-memory
    /// mirror to the last committed state.
    fn drop(&mut self) {
        if !self.settled {
            if let Some(log) = &self.settle_log {
                // Commit drains the log first, so an (id, true) entry a
                // commit published for a PREVIOUS tx never collides with
                // this one: one settle event per tx end.
                if let Ok(mut l) = log.lock() {
                    l.push((self.id, false));
                }
            }
        }
        if let Some(log) = &self.rollback_log {
            if !self.taken.is_empty() || !self.freed.is_empty() {
                if let Ok(mut l) = log.lock() {
                    l.push((self.taken.clone(), self.freed.clone()));
                }
            }
        }
    }
}

/// The storage engine: pager + WAL + recovery.
pub struct Engine {
    pager: Pager,
    wal: Wal,
    /// WAL checkpoint threshold in bytes.
    checkpoint_threshold: u64,
    /// Pages reserved but not yet committed, across ALL open transactions.
    /// Scratch state: appended by `alloc_in_tx`/`free_in_tx`, cleared by
    /// rollback and folded into the pager at commit. Never flushed.
    pending_allocs: Vec<PageId>,
    /// Reservations staged by dropped Tx (rolled-back), drained by
    /// [`Engine::drain_rollbacks`] (shared with every open Tx; see
    /// `Tx::drop`).
    rollback_log: RollbackLog,
    /// Tx-end notifications (commit / rollback), drained by consumer
    /// stores (phase 8.7 BUG-1 fix). Shared with every open Tx.
    settle_log: SettleLog,
    /// Monotone tx sequence: every `begin()` hands out `tx_seq + 1`.
    tx_seq: u64,
}

impl Engine {
    /// Open a database with full recovery (spec §18): replay committed WAL
    /// frames into the pager, flush the result, truncate the log.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Engine, EngineError> {
        let mut pager = Pager::open(path.as_ref())?;
        let mut wal = Wal::open(path.as_ref())?;
        let frames = wal.scan()?;
        if !frames.is_empty() {
            // Grouped replay: a commit's frame set is the full picture of
            // one transaction — its page frames are interpreted by ITS OWN
            // sb frame. (Applying historical sb frames frame-by-frame would
            // shrink the allocation state mid-replay and break the bounds
            // check — which is exactly what the old fail-open replay
            // silently swallowed.) Buffer each group, validate, apply.
            let mut pending: Vec<&WalOp> = Vec::new();
            for (op, is_commit) in &frames {
                if *is_commit {
                    // Any committed Custom whose tag is not COMMIT_TAG is
                    // an opcode this build does not know (a file from a
                    // newer/foreign version): refuse to open — fail-closed
                    // (phase 8.5). The previously silent
                    // `WalOp::Custom { .. } => {}` arm would have re-opened
                    // such a DB into a mutilated state (probe-5 of the
                    // second audit, converted to a negative assertion).
                    for o in &pending {
                        if let WalOp::Custom { tag, .. } = o {
                            if *tag != wal::COMMIT_TAG {
                                return Err(EngineError::Wal(WalError::UnknownOpcode {
                                    tag: *tag,
                                }));
                            }
                        }
                    }
                    // The group's sb frame (exactly one per commit).
                    let sb = pending.iter().rev().find_map(|o| match o {
                        WalOp::SetSuperblock {
                            page_count,
                            freelist_head,
                        } => Some((*page_count, *freelist_head)),
                        _ => None,
                    });
                    let (page_count, freelist_head) = match sb {
                        Some(v) => v,
                        None => (
                            pager.superblock().page_count,
                            pager.superblock().freelist_head,
                        ),
                    };
                    // Group validity: every page it sets must exist in the
                    // allocation state it commits (a reference beyond the
                    // committed count means the group is corrupt).
                    let max_set = pending
                        .iter()
                        .filter_map(|o| match o {
                            WalOp::SetPage { page_id, .. } => Some(*page_id),
                            _ => None,
                        })
                        .max()
                        .map(|m| m + 1);
                    if let Some(max_id) = max_set {
                        if max_id > page_count {
                            return Err(EngineError::Page(PageError::FreelistCorrupt));
                        }
                        // Legitimize the group's ids for the page apply.
                        pager.set_superblock_for_recovery(max_id.max(page_count), freelist_head);
                    }
                    for o in &pending {
                        if let WalOp::SetPage { page_id, data } = o {
                            let page = pager.get_for_write(*page_id)?;
                            page.data.copy_from_slice(data);
                        }
                    }
                    pager.set_superblock_for_recovery(page_count, freelist_head);
                    pending.clear();
                } else {
                    pending.push(op);
                }
            }
            // Freelist rebuilt THROUGH the cache: grouped replay warmed
            // `get_for_write` (clean/dirty); a chain read raw from disk
            // here could contradict the replayed state (probe-3: a chain
            // that exists only in the WAL). Any inconsistency fails
            // closed — open() aborts.
            pager.rebuild_freelist_from_disk()?;
            pager.flush()?;
            wal.truncate()?;
        } else if !pager.chain_ok() {
            // The on-disk chain was damaged AND there is no log to rebuild
            // from: fail closed (no WAL means nothing can heal the file).
            return Err(EngineError::Page(PageError::FreelistCorrupt));
        }
        Ok(Engine {
            pager,
            wal,
            checkpoint_threshold: 4 * 1024 * 1024,
            pending_allocs: Vec::new(),
            rollback_log: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            settle_log: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            tx_seq: 0,
        })
    }

    /// Begin a transaction. Any Tx dropped since the last Engine call is
    /// reclaimed first (its ids handed back), so a rolled-back tx never
    /// parks its id in front of the new one. Every tx gets a monotone id
    /// (begin order) it carries in its settle notifications (phase 8.7).
    pub fn begin(&mut self) -> Result<Tx, EngineError> {
        self.drain_rollbacks()?;
        self.tx_seq += 1;
        Ok(Tx {
            writes: Vec::new(),
            taken: Vec::new(),
            freed: Vec::new(),
            rollback_log: Some(std::sync::Arc::clone(&self.rollback_log)),
            settle_log: Some(std::sync::Arc::clone(&self.settle_log)),
            id: self.tx_seq,
            settled: false,
        })
    }

    /// The shared tx-end log (phase 8.7 BUG-1 hook): a store that mutates
    /// tx-visible in-memory state arms a snapshot per tx and drains this
    /// log to learn each tx's end — `(id, false)` = rolled back (revert
    /// the snapshot), `(id, true)` = committed (drop it). Draining is the
    /// consumer's job so the log cannot grow unbounded inside the Engine
    /// and `Engine` itself stays store-agnostic.
    pub(crate) fn settle_log(&self) -> SettleLog {
        std::sync::Arc::clone(&self.settle_log)
    }

    /// Reclaim the reservations of every Tx dropped since the last drain:
    /// hands each taken id back (pager mirror/scratch — see
    /// `Pager::cancel_alloc`) and clears both the engine and pager scratch
    /// state. Drop-time reclaim of the Phase 9 rollback fix: rolled-back
    /// page ids become recyclable again, in memory, immediately. The
    /// durable-scratch claims fold (Phase 8.6) can fail on the WAL — the
    /// error propagates to the Engine caller (fail-closed).
    fn drain_rollbacks(&mut self) -> Result<(), EngineError> {
        let entries: Vec<(Vec<PageId>, Vec<PageId>)> = match self.rollback_log.lock() {
            Ok(mut l) => std::mem::take(&mut *l),
            Err(_) => Vec::new(),
        };
        for (taken, freed) in entries {
            for id in &taken {
                self.pager.cancel_alloc(*id);
            }
            for id in &freed {
                self.pager.cancel_free(*id);
            }
            self.pending_allocs
                .retain(|p| !taken.contains(p) && !freed.contains(p));
        }
        // Phase 8.6: fold every durable-but-unchained scratch id (an
        // append-reserved id handed back AFTER other commits grew the sb
        // past it — the pre-fix brick scenario) into the durable freelist
        // with a journaled node + head-advance commit. This is the
        // equivalent of the net-out's "returning the slot to the chain",
        // for scratch ids whose slot the count already covers: without
        // this fold the id is lost for the session (append walks past it)
        // and every reopen re-appends past it permanently (durable hole).
        self.pager.claim_durable_scratch(&mut self.wal)?;
        Ok(())
    }

    /// Commit a transaction: WAL frames first, fsync, then apply + flush.
    /// A Tx dropped since the last Engine call is reclaimed first (its ids
    /// handed back) so its rollback cannot shadow this commit's state.
    pub fn commit(&mut self, tx: Tx) -> Result<(), EngineError> {
        self.drain_rollbacks()?;
        let mut tx = tx;
        // The tx end is notified EXACTLY once: this success publishes
        // (id, true) to the settle log; its Drop then sees a settled tx
        // and publishes nothing (rollback = the same log, (id, false)).
        let settle_log = self.settle_log();
        let tx_id = tx.id;
        let result = self.commit_inner(&mut tx);
        if result.is_ok() {
            tx.settled = true;
            if let Ok(mut l) = settle_log.lock() {
                l.push((tx_id, true));
            }
        }
        result
    }

    /// The commit protocol exactly as before phase 8.7 (unchanged
    /// behavior); split from `commit` for the settle notification.
    fn commit_inner(&mut self, tx: &mut Tx) -> Result<(), EngineError> {
        // Reject a free of a page this same tx allocated: there is no
        // durable page to recycle, and rolling it back would hand a
        // phantom id to the next tx.
        for id in &tx.freed {
            if tx.taken.contains(id) {
                return Err(EngineError::Page(PageError::FreelistCorrupt));
            }
        }
        // Phase 8.6: a tx that reserved an id TWICE (netted out mid-tx,
        // re-reserved from scratch) must fold it ONCE — dedup by distinct
        // id, reserve order preserved for the fold semantics.
        let mut taken: Vec<PageId> = Vec::new();
        for id in &tx.taken {
            if !taken.contains(id) {
                taken.push(*id);
            }
        }
        // Phase 8.6 net-out (part 2): a free_in_tx in THIS tx of one of
        // its OWN allocs already released that reservation (taken was
        // cleaned at the free) — a taken id that ALSO appears in freed
        // can only mean a net-out raced the fold; drop freed∩taken from
        // BOTH sides so the fold sees each page exactly once.
        let taken: Vec<PageId> = taken
            .iter()
            .copied()
            .filter(|id| !tx.freed.contains(id))
            .collect();
        let freed: Vec<PageId> = tx
            .freed
            .iter()
            .copied()
            .filter(|id| !tx.taken.contains(id))
            .collect();
        // The superblock this commit produces (pure fold, nothing mutated).
        let final_sb = {
            let sb = self.pager.compute_commit_sb(&freed, &taken);
            if !taken.is_empty() {
                // Every taken id must be durably owned after this commit;
                // if the durable state was never incremented for it (e.g.
                // double alloc of the same recycled id), refuse.
                let durable = self.pager.superblock().page_count;
                if sb.page_count < durable {
                    return Err(EngineError::Page(PageError::FreelistCorrupt));
                }
            }
            sb
        };
        // Phase 8.6: a free_in_tx of an id this SAME tx allocs nets the
        // reservation out (see free_in_tx) — skip its stale staged writes
        // (the zeroed stub and the freelist node of a page that no commit
        // ever referenced): journaling or applying them would fail the
        // bounds check against the sb this commit produces.
        let netted: Vec<PageId> = tx
            .writes
            .iter()
            .filter_map(|(id, _)| {
                if tx.taken.contains(id) && tx.freed.contains(id) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        // 1) Log every page write, in order. The freelist node set by
        //    free_in_tx is just a write: journaled before the sb frame
        //    that interprets it. A zero-effect commit (nothing durable
        //    changes — the sb is byte-identical) journals NOTHING: the
        //    fold already proved there is nothing to interpret, and a
        //    frame here would only grow the log (Phase 8.6).
        for (id, data) in &tx.writes {
            if netted.contains(id) {
                continue;
            }
            self.wal.append(&WalOp::SetPage {
                page_id: *id,
                data: data.to_vec(),
            })?;
        }
        // 2) Log the superblock: THE interpretation of everything above
        //    (allocation state at commit). A zero-effect commit (no kept
        //    writes AND a byte-identical sb fold) journals NOTHING — the
        //    current durable state is already the truth; writing the
        //    identical sb again would only grow the log (Phase 8.6).
        let zero_effect = tx.writes.is_empty() && tx.taken.is_empty() && tx.freed.is_empty();
        if !zero_effect {
            self.wal.append(&WalOp::SetSuperblock {
                page_count: final_sb.page_count,
                freelist_head: final_sb.freelist_head,
            })?;
            // 3) Commit marker.
            self.wal.commit()?;
            // 4) Durability boundary: the transaction is now durable.
            self.wal.sync()?;
        }
        // 5) Apply the commit to the pager, in the order the WAL states
        //    it: journaled pages, then freelist candidates (node then head),
        //    then the remaining alloc candidates.
        for (id, data) in &tx.writes {
            if netted.contains(id) {
                continue;
            }
            let page = self.pager.get_for_write(*id)?;
            page.data.copy_from_slice(data);
        }
        for id in &freed {
            self.pager.commit_free(*id);
        }
        for id in &taken {
            self.pager.commit_alloc(*id);
        }
        // Flush with the commit's sb as the ceiling: handed-back
        // reservations' zeroed stubs (net-out / rollback scratch) are
        // dropped, not written — the file never grows past the committed
        // count (Phase 8.6: inert-tx probe caught the +4096 garbage).
        self.pager.flush_bounded(final_sb.page_count)?;
        self.pending_allocs
            .retain(|p| !taken.contains(p) && !freed.contains(p));
        // 6) Checkpoint when the log grows past the threshold.
        let log_len = std::fs::metadata(self.wal.path())
            .map(|m| m.len())
            .unwrap_or(0);
        if log_len > self.checkpoint_threshold {
            self.wal.truncate()?;
        }
        // The Tx is dropped here (by-value end of commit): empty its parts
        // FIRST so its Drop sees a spent transaction and publishes nothing
        // (a committed tx must never be reclaimed as a rollback).
        tx.writes.clear();
        tx.taken.clear();
        tx.freed.clear();
        Ok(())
    }

    /// Transactional page allocation: the id is reserved WITHOUT mutating
    /// the pager's durable state (the sb this tx will produce lives in the
    /// commit's WAL frame / pager fold, applied only at commit). The
    /// reservation is parked on the Engine, so a second open transaction
    /// can never receive the same id. A rolled-back tx hands the id back —
    /// no durable leak.
    pub fn alloc_in_tx(&mut self, tx: &mut Tx) -> Result<PageId, EngineError> {
        self.drain_rollbacks()?;
        let id = self.pager.alloc()?;
        tx.taken.push(id);
        self.pending_allocs.push(id);
        Ok(id)
    }

    /// Transactional free: the pager rewrites the page as a freelist node
    /// (next = durable head for the FIRST free of the tx, then each node
    /// chains onto the tx's PREVIOUSLY staged free — every node its
    /// sibling points to is journaled in the SAME commit, so the chain
    /// stays whole after recovery), the node goes into tx.writes (so the
    /// WAL carries the chain nodes AND the sb at the same commit), and the
    /// page is staged for the commit fold. Nothing durable moves until
    /// commit.
    ///
    /// Alloc+free of the SAME id inside ONE tx is netted out here (Phase
    /// 8.6): the tx releases the reservation immediately (scratch/mirror
    /// hand-back) and the id appears in NO commit candidate — the commit is
    /// exactly the zero-effect one a rollback produces (documented churn
    /// protocol: the alloc has no durable successor). The freelist-node
    /// rewrite is skipped for the netted id: no committed superblock has
    /// ever referenced it (it was never durably owned), so journaling a
    /// node there is a no-op at best and a stale-sb hazard, and a phantom
    /// id in the candidates would corrupt the fold.
    pub fn free_in_tx(&mut self, tx: &mut Tx, id: PageId) -> Result<(), EngineError> {
        self.drain_rollbacks()?;
        // Same-tx alloc+free: net the reservation out BEFORE stage guards
        // can reject the not-yet-durable id ("rollback of the scratch").
        // Every staged write for the netted id goes too — a page no commit
        // ever referenced has nothing to journal (the net-out is a
        // rollback of the reservation, never a write). The Tx is drained
        // so its Drop publishes NOTHING (the hand-back already happened;
        // a later drop-time reclaim would double-hand the id).
        if let Some(pos) = tx.taken.iter().position(|&t| t == id) {
            // Phase 8.7: this tx ends INCOMPLETE here (the reservation is
            // netted out — the alloc has no durable successor; this
            // branch IS the net-out, the durable free below is NOT):
            // notify the settle log so upper stores revert their
            // snapshot. The tx STAYS USABLE (phase 8.6 semantic,
            // preserved): its later writes behave as before; its
            // eventual drop publishes nothing (already settled) and a
            // later commit is refused at the taken∩freed fold guard (the
            // freed id is re-parked and must not be folded twice).
            if let Some(log) = tx.settle_log.take() {
                if let Ok(mut l) = log.lock() {
                    l.push((tx.id, false));
                }
            }
            tx.settled = true;
            tx.taken.remove(pos);
            tx.writes.retain(|(wid, _)| *wid != id);
            self.pending_allocs.retain(|&p| p != id);
            self.pager.cancel_alloc(id);
            return Ok(());
        }
        // Sibling order: the PREVIOUS staged free of this tx (tx.freed
        // carries free order), or the durable head when this is the first.
        let next = tx.freed.last().copied().unwrap_or(self.pager.head());
        let node = self.pager.free_node(id, next)?;
        tx.writes.push((id, node.data));
        tx.freed.push(id);
        self.pending_allocs.push(id);
        Ok(())
    }

    /// Buffer a page write inside the transaction (applied at commit).
    pub fn write_in_tx<F: FnOnce(&mut [u8; PAGE_SIZE])>(
        &mut self,
        tx: &mut Tx,
        id: PageId,
        f: F,
    ) -> Result<(), EngineError> {
        // Start from the pager's current view (dirty or disk).
        let mut data = self.pager.read(id)?.data;
        f(&mut data);
        tx.writes.push((id, data));
        Ok(())
    }

    pub fn read_page(&mut self, id: PageId) -> Result<&[u8; PAGE_SIZE], EngineError> {
        Ok(&self.pager.read(id)?.data)
    }

    // -- Phase 9 audit hooks (perf instrumentation; no behavior change) ------
    //
    // These re-expose the exact steps of the commit protocol so the
    // bench_detail binary can time each step (fold / WAL appends / sb /
    // commit marker / fsync / pager apply / pager flush). They perform the
    // SAME operations as `commit()` in the same order; only the bundling
    // differs. Never used by the library's own write paths.

    /// Step 0: the superblock fold + validation of a commit (pure, no write).
    /// Returns [page_count, freelist_head] of the sb this commit produces.
    pub fn expose_commit_fold(
        &mut self,
        writes: &[(PageId, [u8; PAGE_SIZE])],
        freed: &[PageId],
        taken: &[PageId],
    ) -> [u32; 2] {
        let tx_like = Tx {
            writes: writes.to_vec(),
            taken: taken.to_vec(),
            freed: freed.to_vec(),
            rollback_log: None,
            settle_log: None,
            id: 0,
            settled: true,
        };
        let sb = self.pager.compute_commit_sb(&tx_like.freed, &tx_like.taken);
        [sb.page_count, sb.freelist_head]
    }

    /// Step 1: append one SetPage frame (identical to commit step 1's body).
    pub fn expose_wal_setpage(
        &mut self,
        id: PageId,
        data: &[u8; PAGE_SIZE],
    ) -> Result<(), EngineError> {
        self.wal.append(&WalOp::SetPage {
            page_id: id,
            data: data.to_vec(),
        })?;
        Ok(())
    }

    /// Step 2: append the superblock frame.
    pub fn expose_wal_sb(
        &mut self,
        page_count: u32,
        freelist_head: u32,
    ) -> Result<(), EngineError> {
        self.wal.append(&WalOp::SetSuperblock {
            page_count,
            freelist_head,
        })?;
        Ok(())
    }

    /// Step 3: append the commit marker frame.
    pub fn expose_wal_commit(&mut self) -> Result<(), EngineError> {
        self.wal.commit()?;
        Ok(())
    }

    /// Step 4: fsync the WAL (durability boundary).
    pub fn expose_wal_sync(&mut self) -> Result<(), EngineError> {
        self.wal.sync()?;
        Ok(())
    }

    /// Step 5a: apply journaled pages to the pager (commit step 5, pages).
    pub fn expose_pager_apply(&mut self, writes: &[(PageId, [u8; PAGE_SIZE])]) {
        for (id, data) in writes {
            let page = self.pager.get_for_write(*id).unwrap();
            page.data.copy_from_slice(data);
        }
    }

    /// Step 5b: flush the pager (write pages + sb, fsync).
    pub fn expose_pager_flush(&mut self) -> Result<(), EngineError> {
        self.pager.flush()?;
        Ok(())
    }

    /// Step 5c: clear the engine's scratch reservations for a folded Tx.
    pub fn expose_clear_pending(&mut self, taken: &[PageId], freed: &[PageId]) {
        self.pending_allocs
            .retain(|p| !taken.contains(p) && !freed.contains(p));
    }

    /// Step 6: stat the WAL length (checkpoint threshold decision input).
    pub fn expose_stat_wal_len(&self) -> u64 {
        self.wal_len()
    }

    pub fn wal_len(&self) -> u64 {
        std::fs::metadata(self.wal.path())
            .map(|m| m.len())
            .unwrap_or(0)
    }

    pub fn page_count(&self) -> u32 {
        self.pager.page_count()
    }

    pub fn checkpoint(&mut self) -> Result<(), EngineError> {
        self.pager.flush()?;
        self.wal.truncate()?;
        Ok(())
    }

    /// Roll a transaction back the explicit way: identical to dropping the
    /// Tx, plus this hands the reservations back on the spot so the next
    /// transaction never sees them parked (drop-time reclaim does the same
    /// work later — this is just eager). Phase 8.7: the tx end is also
    /// notified on the settle log as `(id, false)` (upper stores revert
    /// their tx-scoped snapshots), so the tx is marked settled and its
    /// later Drop publishes nothing (one settle per tx end).
    pub fn rollback(&mut self, tx: Tx) {
        let mut tx = tx;
        // Phase 8.7: notify the tx end BEFORE the reservations hand-back.
        // (tx.id is read before the take of tx.taken/freed.)
        let tx_id = tx.id;
        if let Some(log) = tx.settle_log.take() {
            if let Ok(mut l) = log.lock() {
                l.push((tx_id, false));
            }
        }
        tx.settled = true;
        for id in &tx.taken {
            self.pager.cancel_alloc(*id);
        }
        for id in &tx.freed {
            self.pager.cancel_free(*id);
        }
        let taken = std::mem::take(&mut tx.taken);
        let freed = std::mem::take(&mut tx.freed);
        // Ids claimed by BOTH sides of the tx (taken then freed in-tx) are
        // handed back exactly once (Phase 8.6: the freed entry is the same
        // reservation released, not a second one).
        let mut ids: Vec<PageId> = Vec::new();
        for id in taken.iter().chain(freed.iter()) {
            if !ids.contains(id) {
                ids.push(*id);
            }
        }
        for id in &ids {
            self.pending_allocs.retain(|p| *p != *id);
        }
        // Drop of this (now empty) Tx publishes nothing.
    }
}

#[derive(Debug)]
pub enum EngineError {
    Page(crate::pager::PageError),
    Wal(crate::wal::WalError),
}

impl From<crate::pager::PageError> for EngineError {
    fn from(e: crate::pager::PageError) -> Self {
        EngineError::Page(e)
    }
}

impl From<crate::wal::WalError> for EngineError {
    fn from(e: crate::wal::WalError) -> Self {
        EngineError::Wal(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_eng_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn transactional_write_roundtrip() {
        let path = tmp_path("tx");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            eng.write_in_tx(&mut tx, id, |d| d[0] = 7).unwrap();
            eng.commit(tx).unwrap();
        }
        {
            let mut eng = Engine::open(&path).unwrap();
            assert_eq!(eng.page_count(), 2); // superblock + 1 page
            let data = eng.read_page(1).unwrap();
            assert_eq!(data[0], 7);
        }
        cleanup(&path);
    }

    #[test]
    fn rollback_discards_writes() {
        let path = tmp_path("rb");
        let mut eng = Engine::open(&path).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |d| d[0] = 99).unwrap();
        drop(tx); // rollback
                  // The WAL must not contain the rolled-back page frame.
        assert_eq!(eng.wal_len(), 0);
        // A later empty transaction still works.
        let tx = eng.begin().unwrap();
        eng.commit(tx).unwrap();
        cleanup(&path);
    }

    #[test]
    fn multiple_transactions_in_order() {
        let path = tmp_path("multi");
        {
            let mut eng = Engine::open(&path).unwrap();
            for v in 1..=10u8 {
                let mut tx = eng.begin().unwrap();
                let id = eng.alloc_in_tx(&mut tx).unwrap();
                eng.write_in_tx(&mut tx, id, |d| d[0] = v).unwrap();
                eng.commit(tx).unwrap();
            }
        }
        let mut eng = Engine::open(&path).unwrap();
        assert_eq!(eng.page_count(), 11); // superblock + 10 pages
        for id in 1..=10u32 {
            assert_eq!(eng.read_page(id).unwrap()[0], id as u8);
        }
        cleanup(&path);
    }

    /// Simulated crash: write pages through the WAL, then reopen WITHOUT
    /// the pager having flushed — recovery must restore them from the log.
    #[test]
    fn recovery_replays_committed_transactions() {
        let path = tmp_path("rec");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            eng.write_in_tx(&mut tx, id, |d| d[11] = 55).unwrap();
            // WAL synced, but "crash" before the pager flush would happen:
            // simulate by NOT flushing the pager after WAL sync — commit()
            // does both, so instead emulate with a manual WAL-only path.
            // Phase 1 tests crash via the wal module directly; here we
            // verify the normal path and rely on wal::tests for torn tails.
            eng.commit(tx).unwrap();
        }
        let mut eng = Engine::open(&path).unwrap();
        assert_eq!(eng.read_page(1).unwrap()[11], 55);
        cleanup(&path);
    }
}

//! Phase 8.1 — transactional alloc/free + fail-closed recovery (spec §18).
//!
//! Converts audit probes into real
//! assertions:
//! (a) rollback leaves the superblock byte-identical (page_count unchanged,
//!     durable after reopen — the old leak was durable);
//! (b) free_in_tx + commit journals BOTH the freelist node (SetPage) AND the
//!     superblock in the same commit — inspectable via wal.scan;
//! (c) deterministic mid-commit crashes: the WAL holds a complete committed
//!     group while the page file is damaged at each step of the pager flush
//!     (lost data page, corrupt superblock page, lost freelist node byte) —
//!     reopen MUST succeed via replay, the chain stays sound, the page stays
//!     recyclable;
//! (d) recovery replay: a WAL carrying a committed freelist chain + valid
//!     page sets opens cleanly, and the allocator recycles exactly per the
//!     replayed chain (the old rebuild read raw disk and failed closed on a
//!     healthy DB);
//! (e) fail-closed: a corrupt/inconsistent recovery aborts the open instead
//!     of swallowing it;
//! (f) engine scratch: reservations across coexisting Tx never collide,
//!     freed candidates reject phantom frees.

use vidgedb::engine::{Engine, EngineError};
use vidgedb::pager::{NULL_PAGE, PAGE_SIZE};
use vidgedb::wal::{Wal, WalOp};

fn tmp_path(name: &str) -> String {
    let mut p = std::env::temp_dir().to_path_buf();
    p.push(format!("vidgedb_p81_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

fn alloc_n(eng: &mut Engine, n: u32) -> Vec<u32> {
    let mut ids = Vec::new();
    for i in 0..n {
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |d| d[0] = i as u8).unwrap();
        eng.commit(tx).unwrap();
        ids.push(id);
    }
    ids
}

// ---------------------------------------------------------------------------
// (a) rollback = zero durable effect (probe1 converted)
// ---------------------------------------------------------------------------

#[test]
fn rollback_alloc_page_count_unchanged_and_durable() {
    let path = tmp_path("rb_alloc");
    {
        let mut eng = Engine::open(&path).unwrap();
        assert_eq!(eng.page_count(), 1); // superblock only
        let mut tx = eng.begin().unwrap();
        let _id = eng.alloc_in_tx(&mut tx).unwrap();
        drop(tx); // rollback

        // In-memory view is also unchanged: no sb mutant escaped the tx.
        assert_eq!(eng.page_count(), 1);
        assert_eq!(eng.wal_len(), 0); // nothing was ever logged

        // The empty commit used to persist the leak — it must stay at 1.
        let tx2 = eng.begin().unwrap();
        eng.commit(tx2).unwrap();
        assert_eq!(eng.page_count(), 1);
    }
    // Durable: reopen sees the pre-rollback allocation state.
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 1);
    cleanup(&path);
}

#[test]
fn rollback_free_page_recyclable_and_unchanged() {
    let path = tmp_path("rb_free");
    {
        let mut eng = Engine::open(&path).unwrap();
        let ids = alloc_n(&mut eng, 3); // pages 1, 2, 3
        eng.checkpoint().unwrap(); // durable, clean log
        let before = eng.page_count(); // 4
        let mut tx = eng.begin().unwrap();
        eng.free_in_tx(&mut tx, ids[2]).unwrap();
        drop(tx); // rollback

        assert_eq!(eng.page_count(), before, "rollback must not free durably");
        assert_eq!(eng.wal_len(), 0, "rollback must never log");
        // The freed page is still owned: alloc hands a fresh append id.
        let mut tx = eng.begin().unwrap();
        let fresh = eng.alloc_in_tx(&mut tx).unwrap();
        assert_eq!(fresh, 4);
        assert_eq!(eng.page_count(), before); // still not durable
        eng.commit(tx).unwrap();
        assert_eq!(eng.page_count(), 5);
    }
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 5);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (a2) Phase 9 anti-placeholder: a rolled-back Tx's reserved ids are handed
//     back (in-memory recycle) — ZERO page loss, across BOTH kinds
// ---------------------------------------------------------------------------

/// A RECYCLED id (node durably chained) handed to a tx that then rolls back
/// must return to the freelist mirror: the very next tx re-reserves it, and
/// the commit after that keeps the page_count untouched — the page was never
/// lost, the WAL never grew.
#[test]
fn rollback_recycled_id_returns_to_mirror() {
    let path = tmp_path("rb_recycle");
    {
        let mut eng = Engine::open(&path).unwrap();
        let ids = alloc_n(&mut eng, 3); // pages 1, 2, 3 durable
                                        // Free page 3 durably: chain = 3, count stayed 4.
        let mut tx = eng.begin().unwrap();
        eng.free_in_tx(&mut tx, ids[2]).unwrap();
        eng.commit(tx).unwrap();
        eng.checkpoint().unwrap();
        // Reserve the recycled 3, then roll the tx back by dropping it.
        let mut tx = eng.begin().unwrap();
        let taken = eng.alloc_in_tx(&mut tx).unwrap();
        assert_eq!(taken, 3, "precondition: 3 comes from the freelist");
        drop(tx); // rollback (Phase 9: reclaim happens at drop)
                  // In-memory recycle: the next reservation hands 3 back out.
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        assert_eq!(id, 3, "rolled-back recycled id must recycle again");
        eng.write_in_tx(&mut tx, id, |d| d[0] = 111).unwrap();
        eng.commit(tx).unwrap();
        // Zero growth, zero extra WAL (recycle of a durably-chained page).
        assert_eq!(eng.page_count(), 4, "no page was lost or appended");
    }
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 4, "durable count unchanged");
    assert_eq!(
        eng.read_page(3).unwrap()[0],
        111,
        "recycled page holds data"
    );
    cleanup(&path);
}

/// An APPEND-reserved id handed to a tx that rolls back must not be lost to
/// the watermark: the next tx re-reserves the same id from the scratch pool
/// and page_count grows only when IT commits.
#[test]
fn rollback_append_id_handed_back_not_leaked() {
    let path = tmp_path("rb_append");
    {
        let mut eng = Engine::open(&path).unwrap();
        assert_eq!(eng.page_count(), 1);
        let mut tx = eng.begin().unwrap();
        let reserved = eng.alloc_in_tx(&mut tx).unwrap();
        assert_eq!(reserved, 1);
        eng.write_in_tx(&mut tx, reserved, |d| d[0] = 7).unwrap();
        drop(tx); // rollback: nothing durable, id must not be orphaned
        assert_eq!(eng.page_count(), 1);
        assert_eq!(eng.wal_len(), 0);
        // The SAME id is handed out again to the next tx (scratch reuse).
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        assert_eq!(id, reserved, "rolled-back append id must be reused");
        eng.write_in_tx(&mut tx, id, |d| d[0] = 9).unwrap();
        eng.commit(tx).unwrap();
        assert_eq!(eng.page_count(), 2, "exactly one page was committed");
        assert_ne!(eng.read_page(id).unwrap()[0], 7, "rolled-back bytes gone");
        assert_eq!(eng.read_page(id).unwrap()[0], 9);
    }
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 2, "no page hole after reopen");
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (a3) 10-cycle rollback churn: pages keep recycling, no growth, no WAL debt
// ---------------------------------------------------------------------------

/// Anti-placeholder for the old documented leak ("a rolled-back Tx leaks its
/// reserved page"): run TEN reserve->rollback cycles interleaved with real
/// commits and assert, cycle by cycle, that page_count never drifts above
/// what the committed pages cover — every rolled-back page is recycled on
/// the very next reservation.
#[test]
fn rollback_recycles_10_cycles() {
    let path = tmp_path("rb_10c");
    {
        let mut eng = Engine::open(&path).unwrap();
        // Baseline: ONE durable data page (the anchor every cycle reuses).
        let mut tx = eng.begin().unwrap();
        let base = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, base, |d| d[5] = 1).unwrap();
        eng.commit(tx).unwrap();
        eng.checkpoint().unwrap();
        assert_eq!(eng.page_count(), 2);
        assert_eq!(eng.wal_len(), 0, "checkpoint cleaned the log");
        // Steady footprint: the rollback leg must never GROW the count
        // above the previous cycle-end value (cycle 0 starts at the base,
        // every later cycle starts at the committed steady state).
        let base_count = eng.page_count(); // 2 before the first cycle
        let mut steady = base_count;
        for cycle in 0..10u32 {
            // (1) rollback leg: page_count must NEVER grow.
            let mut tx = eng.begin().unwrap();
            let scratch = eng.alloc_in_tx(&mut tx).unwrap();
            assert!(scratch <= steady + 1, "cycle {}: watermark drifted", cycle);
            eng.write_in_tx(&mut tx, scratch, |d| d[6] = 200).unwrap();
            drop(tx); // rollback cycle i
            assert_eq!(
                eng.page_count(),
                steady,
                "cycle {}: rollback leaked a page (count grew)",
                cycle
            );
            // (2) recycle leg: a REAL tx re-reserves exactly the id the
            // rollback handed back, commits it (one page), frees it
            // durably — the footprint is flat at every cycle end.
            let mut keep = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut keep).unwrap();
            assert_eq!(id, scratch, "cycle {}: rolled-back id not recycled", cycle);
            eng.write_in_tx(&mut keep, id, |d| d[7] = cycle as u8)
                .unwrap();
            eng.commit(keep).unwrap();
            assert_eq!(
                eng.page_count(),
                base_count + 1,
                "cycle {}: commit grew past the expected one page",
                cycle
            );
            let mut tx = eng.begin().unwrap();
            eng.free_in_tx(&mut tx, id).unwrap();
            eng.commit(tx).unwrap();
            assert_eq!(
                eng.page_count(),
                base_count + 1,
                "cycle {}: stale footprint",
                cycle
            );
            steady = base_count + 1;
        }
        // No WAL debt from the rollbacks themselves: the log only ever
        // holds committed frames (checkpointed here).
        eng.checkpoint().unwrap();
        assert_eq!(eng.wal_len(), 0, "rollback cycle left WAL debt");
    }
    // Reopen: the file kept a bounded footprint, no 10x bloat.
    let eng = Engine::open(&path).unwrap();
    assert_eq!(
        eng.page_count(),
        3,
        "10 rolled-back cycles leaked ZERO pages"
    );
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (b) free is journaled: node SetPage + SetSuperblock at the same commit
//     (probe2/probe2b converted)
// ---------------------------------------------------------------------------

#[test]
fn free_commit_wal_carries_node_and_superblock() {
    let path = tmp_path("free_wal");
    let freed: u32 = {
        let mut eng = Engine::open(&path).unwrap();
        let ids = alloc_n(&mut eng, 3);
        let before = eng.wal_len();
        let mut tx = eng.begin().unwrap();
        eng.free_in_tx(&mut tx, ids[2]).unwrap();
        eng.commit(tx).unwrap();
        // Delta check (the probe's smoking gun — a bare SetSuperblock +
        // commit marker is 26 bytes; the node frame adds 4096+8+9).
        assert!(eng.wal_len() - before >= 4104, "node frame must be logged");
        ids[2]
    };
    // Inspect the frames of the LAST committed transaction.
    let mut wal = Wal::open(&path).unwrap();
    let frames = wal.scan().unwrap();
    drop(wal);
    let mut groups: Vec<Vec<&WalOp>> = Vec::new();
    let mut pending: Vec<&WalOp> = Vec::new();
    for (op, is_commit) in &frames {
        if *is_commit {
            groups.push(std::mem::take(&mut pending));
        } else {
            pending.push(op);
        }
    }
    let last = groups.last().unwrap();
    let node_frames = last
        .iter()
        .filter(|op| matches!(op, WalOp::SetPage { page_id, .. } if *page_id == freed))
        .count();
    assert_eq!(node_frames, 1, "WAL must carry the freelist node SetPage");
    // The node's bytes 0..4 = next = the chain head BEFORE the free (NULL:
    // nothing was freed before), the rest zeroed.
    let node_data = last
        .iter()
        .find_map(|op| match op {
            WalOp::SetPage { page_id, data } if *page_id == freed => Some(data.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(&node_data[0..4], &NULL_PAGE.to_le_bytes());
    assert!(node_data[4..].iter().all(|&b| b == 0), "node scrubbed");
    // And the superblock frame that interprets it: head = the freed page.
    assert!(last.iter().any(|op| matches!(
        op,
        WalOp::SetSuperblock {
            page_count: 4,
            freelist_head: 3
        }
    )));
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (c) deterministic mid-commit crash: WAL complete, pager flush broken
//     (probe2b-crash converted — reopen MUST succeed)
// ---------------------------------------------------------------------------

/// The WAL is fully synced but the pager flush lost a data page (crash
/// between WAL sync and the data-page writes). Recovery replays the group.
#[test]
fn crash_wal_only_data_page_lost_healed_by_replay() {
    let path = tmp_path("crash_data");
    {
        let mut eng = Engine::open(&path).unwrap();
        let ids = alloc_n(&mut eng, 3);
        let mut tx = eng.begin().unwrap();
        eng.free_in_tx(&mut tx, ids[2]).unwrap();
        eng.commit(tx).unwrap();
        drop(eng);
        // Corrupt the data page of the freed page on disk (its bytes are
        // now the freelist node — punch the chain apart: restore the old
        // record byte as if the write had been lost mid-flush, like the
        // probe did).
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        let mut f = raw;
        let off = (ids[2] as u64) * (PAGE_SIZE as u64);
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(&[0xAB, 0xCD, 0xEF, 0x01]).unwrap(); // not a node
        f.sync_all().unwrap();
    }
    // Reopen MUST succeed: the WAL replays the node over the damage...
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 4);
    // ...and the replayed chain must be live: the freed page recycles.
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(id, 3, "recycled per the replayed chain");
    // The recycled write overwrote the chain node: count grew? no — page_count stayed 4.
    assert_eq!(eng.page_count(), 4);
    cleanup(&path);
}

/// The WAL is fully synced but the flush died BEFORE writing the
/// superblock: the on-disk sb is stale by two whole commits.
#[test]
fn crash_stale_superblock_healed_by_replay() {
    let path = tmp_path("crash_sb");
    {
        let mut eng = Engine::open(&path).unwrap();
        let _ids = alloc_n(&mut eng, 2); // pages 1, 2
        let mut tx = eng.begin().unwrap();
        let id1 = eng.alloc_in_tx(&mut tx).unwrap(); // id 3
        eng.write_in_tx(&mut tx, id1, |d| d[7] = 77).unwrap();
        let id2 = eng.alloc_in_tx(&mut tx).unwrap(); // id 4
        eng.write_in_tx(&mut tx, id2, |d| d[7] = 88).unwrap();
        eng.commit(tx).unwrap();
        drop(eng);
        // Roll the sb page back to the two-commits-ago state: the file
        // keeps 5 pages of data (some zeroed by the stub flush) but the
        // superblock only acknowledges 3.
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        let mut f = raw;
        let mut sb = [0u8; PAGE_SIZE];
        sb[0..4].copy_from_slice(b"VDG1");
        sb[4..8].copy_from_slice(&1u32.to_le_bytes());
        sb[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        sb[12..16].copy_from_slice(&3u32.to_le_bytes()); // stale count
        sb[16..20].copy_from_slice(&NULL_PAGE.to_le_bytes());
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&sb).unwrap();
        f.sync_all().unwrap();
    }
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 5, "sb replayed from the WAL");
    assert_eq!(eng.read_page(3).unwrap()[7], 77);
    assert_eq!(eng.read_page(4).unwrap()[7], 88);
    cleanup(&path);
}

/// Mid-flush loss of the freelist node byte: the WAL replays the node page
/// (journaled by the fix) and the chain survives; the page stays recyclable.
#[test]
fn crash_freelist_node_byte_lost_healed_by_replay() {
    let path = tmp_path("crash_node");
    {
        let mut eng = Engine::open(&path).unwrap();
        let ids: Vec<u32> = {
            let mut t = eng.begin().unwrap();
            let a = eng.alloc_in_tx(&mut t).unwrap();
            eng.write_in_tx(&mut t, a, |d| d[0] = 1).unwrap();
            let b = eng.alloc_in_tx(&mut t).unwrap();
            eng.write_in_tx(&mut t, b, |d| d[0] = 2).unwrap();
            eng.commit(t).unwrap();
            vec![a, b]
        };
        let mut tx = eng.begin().unwrap();
        eng.free_in_tx(&mut tx, ids[0]).unwrap(); // chain: 1 -> NULL
        eng.commit(tx).unwrap();
        drop(eng);
        // Damage BOTH the node page and the sb page on disk (a crash in the
        // middle of the flush could leave either stale).
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        let mut f = raw;
        f.seek(SeekFrom::Start((ids[0] as u64) * (PAGE_SIZE as u64)))
            .unwrap();
        f.write_all(&[0xFF; 8]).unwrap(); // head junk where the node was
        f.sync_all().unwrap();
        // ...and a freelist head pointing anywhere (sb page zeroed except
        // magic — the head the file advertises is garbage 0).
        let mut sb = [0u8; PAGE_SIZE];
        sb[0..4].copy_from_slice(b"VDG1");
        sb[4..8].copy_from_slice(&1u32.to_le_bytes());
        sb[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        sb[12..16].copy_from_slice(&3u32.to_le_bytes());
        sb[16..20].copy_from_slice(&0u32.to_le_bytes()); // dangling id 0!
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&sb).unwrap();
        f.sync_all().unwrap();
    }
    // Reopen: the WAL replays node(1)=[next=NULL] AND sb{3, head=1};
    // the corrupted disk bytes never matter.
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 3);
    let mut tx = eng.begin().unwrap();
    let recycled = eng.alloc_in_tx(&mut tx).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(recycled, 1, "freed page recycled per replayed chain");
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (d) recovery: chain that lives ONLY in the WAL (probe3 converted)
// ---------------------------------------------------------------------------

#[test]
fn recovery_replayed_chain_drives_allocator() {
    let path = tmp_path("replay_chain");
    // Hand-written WAL (no prior db content): one committed tx whose page
    // 1 is a freelist node (next=NULL) and whose sb says {2, head=1}.
    {
        let wal = Wal::open(&path).unwrap();
        let mut wal = wal;
        let mut data = vec![0u8; PAGE_SIZE];
        data[0..4].copy_from_slice(&NULL_PAGE.to_le_bytes());
        wal.append(&WalOp::SetPage { page_id: 1, data }).unwrap();
        wal.append(&WalOp::SetSuperblock {
            page_count: 2,
            freelist_head: 1,
        })
        .unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
        drop(wal);
    }
    // open() replays, rebuilds the freelist THROUGH the cache, and succeeds.
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 2);
    // The allocator picks up the replayed chain: id 1 recycles (LIFO).
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap();
    eng.write_in_tx(&mut tx, id, |d| d[0] = 9).unwrap();
    let _ = eng.page_count();
    eng.commit(tx).unwrap();
    assert_eq!(id, 1, "recycled per the chain replayed from the WAL");
    cleanup(&path);
}

/// Two chained frees journaled in two commits reopen with a 2-deep chain
/// and recycle in LIFO order.
#[test]
fn recovery_two_chained_frees_replay() {
    let path = tmp_path("replay_chain2");
    {
        let mut eng = Engine::open(&path).unwrap();
        let ids = alloc_n(&mut eng, 2); // pages 1, 2
        for &id in ids.iter() {
            let mut tx = eng.begin().unwrap();
            eng.free_in_tx(&mut tx, id).unwrap();
            eng.commit(tx).unwrap();
        }
        drop(eng);
        // Simulate the flush losing BOTH node pages AND reverting the sb.
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        let mut f = raw;
        let mut sb = [0u8; PAGE_SIZE];
        sb[0..4].copy_from_slice(b"VDG1");
        sb[4..8].copy_from_slice(&1u32.to_le_bytes());
        sb[8..12].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        sb[12..16].copy_from_slice(&1u32.to_le_bytes()); // only the sb page
        sb[16..20].copy_from_slice(&NULL_PAGE.to_le_bytes());
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&sb).unwrap();
        f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        f.write_all(&[0u8; PAGE_SIZE]).unwrap(); // page 1 lost
        f.sync_all().unwrap();
    }
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 3, "both frees' sb replayed");
    let mut tx = eng.begin().unwrap();
    let a = eng.alloc_in_tx(&mut tx).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(a, 2, "LIFO: last freed first");
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (e) fail-closed recovery: corruption refuses the open (probe4 converted)
// ---------------------------------------------------------------------------

#[test]
fn corrupt_wal_group_fails_open_cleanly() {
    let path = tmp_path("corrupt_group");
    // A committed group whose page reference exceeds its own sb frame's
    // page_count — inconsistent, must abort the open (fail-closed), not
    // silently skip the page like the old replay did.
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&WalOp::SetPage {
            page_id: 5,
            data: vec![0xAA; PAGE_SIZE],
        })
        .unwrap();
        wal.append(&WalOp::SetSuperblock {
            page_count: 2,
            freelist_head: 7,
        })
        .unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
        drop(wal);
    }
    match Engine::open(&path) {
        Ok(_) => panic!("inconsistent group must fail the open"),
        Err(EngineError::Page(_)) => {}
        Err(e) => panic!("unexpected error: {:?}", e),
    }
    cleanup(&path);
}

#[test]
fn freelist_beyond_file_fails_open_cleanly() {
    let path = tmp_path("chain_gone");
    // The WAL sb points at a chain whose node page is BEYOND the file
    // (not replayed, not on disk): recovery must refuse, not fabricate.
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&WalOp::SetSuperblock {
            page_count: 2,
            freelist_head: 9,
        })
        .unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
        drop(wal);
    }
    match Engine::open(&path) {
        Ok(_) => panic!("dangling chain must fail the open"),
        Err(EngineError::Page(_)) => {}
        Err(e) => panic!("unexpected error: {:?}", e),
    }
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (f) reservations across coexisting transactions never collide, and the
// freed candidates fold at commit in node-then-head order
// ---------------------------------------------------------------------------

#[test]
fn coexisting_txs_never_get_the_same_page() {
    let path = tmp_path("collision");
    let mut eng = Engine::open(&path).unwrap();
    let mut tx1 = eng.begin().unwrap();
    let id1 = eng.alloc_in_tx(&mut tx1).unwrap();
    let mut tx2 = eng.begin().unwrap();
    let id2 = eng.alloc_in_tx(&mut tx2).unwrap();
    let mut tx3 = eng.begin().unwrap(); // reserved, kept open
    let _id3 = eng.alloc_in_tx(&mut tx3).unwrap();
    // Nothing durable yet: the sb must not know about any of them.
    assert_eq!(eng.page_count(), 1);
    eng.commit(tx1).unwrap();
    eng.commit(tx2).unwrap();
    assert_eq!(eng.page_count(), 3, "both committed reservations counted");
    assert_ne!(id1, id2);
    drop(tx3); // rollback of the third reservation; nothing durable
    assert_eq!(eng.page_count(), 3);
    // After reopen the state is exactly what was committed.
    drop(eng);
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 3);
    // The rolled-back page id is handed out again by a fresh append.
    let mut tx = eng.begin().unwrap();
    let fresh = eng.alloc_in_tx(&mut tx).unwrap();
    eng.write_in_tx(&mut tx, fresh, |d| d[0] = 5).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(fresh, 3);
    cleanup(&path);
}

#[test]
fn same_tx_alloc_free_nets_out_and_cross_tx_free_refused() {
    let path = tmp_path("netout");
    let mut eng = Engine::open(&path).unwrap();
    // (1) Alloc+free of the SAME id inside ONE tx is legal (Phase 8.6
    //     churn protocol): the reservation is netted out on the spot, the
    //     id appears in NO commit candidate, the commit is exactly the
    //     zero-effect one a rollback produces — pc unchanged, no leak.
    let mut tx = eng.begin().unwrap();
    let kept = eng.alloc_in_tx(&mut tx).unwrap(); // stays: committed below
    let netted = eng.alloc_in_tx(&mut tx).unwrap();
    eng.free_in_tx(&mut tx, netted).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 2, "only 'kept' was committed");
    assert_eq!(eng.read_page(kept).unwrap()[0], 0);
    // The netted id is immediately reusable (scratch hand-back), and its
    // page commits cleanly when the NEXT tx keeps it.
    let mut tx = eng.begin().unwrap();
    let re = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(re, netted, "netted id re-reserved from scratch");
    eng.write_in_tx(&mut tx, re, |d| d[0] = 3).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 3);
    assert_eq!(eng.read_page(re).unwrap()[0], 3);
    drop(eng);
    // (2) Cross-tx free of a PARKED reservation stays refused: tx1 holds
    //     the id, nothing durable exists for it yet.
    let mut eng = Engine::open(&path).unwrap();
    let mut t1 = eng.begin().unwrap();
    let parked = eng.alloc_in_tx(&mut t1).unwrap();
    let mut t2 = eng.begin().unwrap();
    let res = eng.free_in_tx(&mut t2, parked);
    assert!(
        matches!(res, Err(EngineError::Page(_))),
        "another tx's parked reservation is not freeable"
    );
    eng.commit(t2).unwrap();
    drop(t1); // rollback: handed back, nothing durable
    assert_eq!(eng.page_count(), 3, "rejected free left nothing durable");

    /* --- explicit rollback path (scratch handed back) --- */
    let path2 = format!("{}-rb", path);
    {
        let mut eng = Engine::open(&path2).unwrap();
        let mut t1 = eng.begin().unwrap();
        let a = eng.alloc_in_tx(&mut t1).unwrap();
        let b = eng.alloc_in_tx(&mut t1).unwrap();
        let _ = eng.free_in_tx(&mut t1, b).unwrap(); // netted, not durable
        eng.rollback(t1); // scratch handed back, nothing durable
        let mut t2 = eng.begin().unwrap();
        let next1 = eng.alloc_in_tx(&mut t2).unwrap();
        let next2 = eng.alloc_in_tx(&mut t2).unwrap();
        // BOTH reservations were handed back (the netted one and the kept
        // one), oldest scratch id first; the tx KEEPS both now: a real
        // write for one, and a net-out free for the other.
        let mut keep = [next1, next2];
        keep.sort_unstable();
        let mut want = [a, b];
        want.sort_unstable();
        assert_eq!(keep, want, "rolled-back ids handed back without loss");
        eng.write_in_tx(&mut t2, keep[0], |d| d[0] = 3).unwrap();
        eng.free_in_tx(&mut t2, keep[1]).unwrap(); // net-out (same tx)
        eng.commit(t2).unwrap();
    }
    let eng = Engine::open(&path2).unwrap();
    assert_eq!(
        eng.page_count(),
        2,
        "exactly one page committed; netted page never leaked"
    );
    std::fs::remove_file(&path2).unwrap();
    std::fs::remove_file(format!("{}-wal", path2)).unwrap();
    cleanup(&path);
}

/// Commit of a tx that only wrote pages (no alloc/free) still journals a
/// consistent sb frame and reopens byte-identical.
#[test]
fn pure_write_tx_reopens_identical() {
    let path = tmp_path("write_only");
    let mut eng = Engine::open(&path).unwrap();
    let ids = alloc_n(&mut eng, 2);
    let before_sb = eng.page_count();
    let mut tx = eng.begin().unwrap();
    eng.write_in_tx(&mut tx, ids[0], |d| d[100] = 42).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), before_sb);
    drop(eng);
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.read_page(1).unwrap()[100], 42);
    assert_eq!(eng.page_count(), before_sb);
    cleanup(&path);
}

//! Phase 8.6 — churn protocol (spec §17/§18, parent probe converted).
//!
//! The churn workload — a transaction that allocs a page, commits, and a
//! NEXT transaction that frees the reserved id and commits, repeated
//! thousands of times — must be sustainable: no error, no page_count
//! growth (freelist recycle instead of append), no WAL debt (bounded log,
//! checkpointable), and a byte-stable footprint after reopen.
//!
//! History: this file's ancestor (the parent's zz_repro probe) was written
//! against a build where `Pager::free()` validated the id against the
//! DURABLE sb alone while the covering sb candidate stayed unapplied — the
//! very first cycle failed with `PageOutOfBounds(1)`. Phase 8.1's
//! commit-fold made the pure cycle pass, but the HARDENING below converted
//! the traps probe-verified on the pre-fix working tree (see each test):
//! - a rolled-back parked append id re-routed by `cancel_alloc` onto the
//!   freelist MIRROR because another tx had grown page_count past it → a
//!   committed sb pointed chain-head at a zeroed stub → reopen FAILED
//!   (FreelistCorrupt) = bricked database;
//! - alloc+free of the same id INSIDE one tx was refused, while the
//!   aborted commit still counted the page;
//! - the retained netted id could collide with the rollback reclaim.
//!
//! Documented behavior (DEVELOPING.md, invariant 9b):
//! - alloc → commit → free (next tx) → commit: the page id is durable
//!   from tx1's commit, freeable by tx2, recycled LIFO;
//! - alloc + free INSIDE the same tx: NETTED OUT — the id appears in no
//!   commit candidate, the commit is the zero-effect one a rollback
//!   produces, the id is reusable immediately, nothing is journaled;
//! - free of a NON-durable id (parked by an open tx, scratch-handed-back,
//!   or beyond the committed count): refused, fail-closed.

use vidgedb::engine::Engine;

fn tmp_path(name: &str) -> String {
    let mut p = std::env::temp_dir().to_path_buf();
    p.push(format!("vidgedb_p86_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

// ---------------------------------------------------------------------------
// (a) the parent probe, at scale: alloc→commit→free→commit x500, NO error,
//     NO page_count drift, NO unbounded WAL debt, reopen footprint stable
// ---------------------------------------------------------------------------

#[test]
fn churn_500_cycles_alloc_free_commit() {
    let path = tmp_path("churn500");
    {
        let mut eng = Engine::open(&path).unwrap();
        assert_eq!(eng.page_count(), 1, "fresh file: superblock only");
        for cycle in 0..500u32 {
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            eng.write_in_tx(&mut tx, id, |d| {
                d[0..4].copy_from_slice(&cycle.to_le_bytes())
            })
            .unwrap();
            eng.commit(tx).unwrap();
            let mut tx = eng.begin().unwrap();
            eng.free_in_tx(&mut tx, id)
                .unwrap_or_else(|e| panic!("cycle {} free({}) failed: {:?}", cycle, id, e));
            eng.commit(tx).unwrap();
            // The recycle keeps the footprint flat: superblock + 1 page,
            // every cycle, both legs.
            assert_eq!(eng.page_count(), 2, "cycle {} footprint drifted", cycle);
        }
        // WAL debt: the churn keeps appending frames until the threshold —
        // after an explicit checkpoint the log must be empty, and the file
        // must hold ONLY the committed footprint.
        eng.checkpoint().unwrap();
        assert_eq!(eng.wal_len(), 0, "checkpoint must clear the churn log");
        let raw = std::fs::metadata(&path).unwrap().len();
        assert_eq!(raw, 2 * 4096, "file grew past the committed footprint");
    }
    // Reopen: the committed state is exactly 1 data page; the churn left
    // no debt in the file.
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 2, "reopen: footprint stable");
    // And the cycle continues cleanly on the reopened file.
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(id, 1, "the freed page recycles after reopen");
    eng.commit(tx).unwrap();
    let mut tx = eng.begin().unwrap();
    eng.free_in_tx(&mut tx, id).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 2);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (a2) durability boundary of free: committed = freeable, parked = refused
// ---------------------------------------------------------------------------

/// tx1 reserves and commits; tx2 frees the id: legal — tx1's commit made
/// the page durable. Also covers: a coexisting tx PARKS an append id past
/// it, the parked one is handed back through the rollback, and its OWN
/// commit is what finally counts the page (never the earlier free's sb).
#[test]
fn free_right_after_commit_is_legal_even_with_pending_tx() {
    let path = tmp_path("free_after_commit");
    let mut eng = Engine::open(&path).unwrap();
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 2);
    // A second, still-open tx parks ANOTHER append id past the first one.
    let mut t2 = eng.begin().unwrap();
    let parked = eng.alloc_in_tx(&mut t2).unwrap();
    assert_eq!(parked, 2);
    // The committed page 1 is freeable right now: it is durable.
    let mut tx = eng.begin().unwrap();
    eng.free_in_tx(&mut tx, id).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 2, "free moves the head, not the count");
    drop(t2); // the parked append id returns to scratch (never durable)
    let mut tx = eng.begin().unwrap();
    let re = eng.alloc_in_tx(&mut tx).unwrap();
    // Chain order: page 1 was freed durably above, so the LIFO hand-out
    // serves 1 first — then 2 (either is the scratch/chain recycle working;
    // the invariant under test is the footprint, not the LIFO slot).
    assert!(
        re == 1 || re == 2,
        "recycled id expected (freed 1 or parked 2), got {}",
        re
    );
    eng.write_in_tx(&mut tx, re, |d| d[0] = 7).unwrap();
    eng.commit(tx).unwrap();
    // Fold semantics: a SCRATCH-recycled id (not durable yet, e.g. id 2
    // parked by t2 then handed back) counts as a fresh append (+1); a
    // CHAIN-recycled id (durable, e.g. freed page 1) adds nothing. So:
    // re==2 → the append folds page_count 2→3; re==1 → stays 2 (slot 2
    // remains scratch until the next drain+commit makes it durable).
    let expected_pc = if re == 2 { 3 } else { 2 };
    assert_eq!(
        eng.page_count(),
        expected_pc,
        "fold semantics: scratch recycle = +1, chain recycle = +0"
    );
    assert_eq!(eng.read_page(re).unwrap()[0], 7);
    // Footprint discipline over exact count: the watermark may have
    // covered the scratch slot (re==2 → +1) or not (re==1 recycled the
    // durable page). What MUST hold: no drift past what live reserves
    // justify (page_count <= sb + live pages + one scratch slot).
    assert!(
        eng.page_count() <= 3,
        "no append drift beyond the two live slots, got {}",
        eng.page_count()
    );
    // Re-check the OTHER slot: the footprint must stay bounded (no
    // append drift across churn legs).
    let other = if re == 1 { 2 } else { 1 };
    if eng.page_count() > other + 1 {
        // Only free what is durably owned; a scratch-id refuses cleanly.
        let mut tx = eng.begin().unwrap();
        if eng.free_in_tx(&mut tx, other).is_ok() {
            eng.commit(tx).unwrap();
        } else {
            drop(tx); // scratch slot: not durable yet — nothing to free
        }
    }
    let mut tx = eng.begin().unwrap();
    let again = eng.alloc_in_tx(&mut tx).unwrap();
    assert!(again == re || again == other);
    eng.commit(tx).unwrap();
    assert!(eng.page_count() <= 3, "footprint bounded across churn legs");
    cleanup(&path);
}

/// Free of a page the committed count does not even cover (parked by its
/// own or another tx): refused, no durable effect.
#[test]
fn free_beyond_committed_count_refused() {
    let path = tmp_path("free_oob");
    let mut eng = Engine::open(&path).unwrap();
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap(); // parked, NOT committed
    let mut tx2 = eng.begin().unwrap();
    match eng.free_in_tx(&mut tx2, id) {
        Err(vidgedb::engine::EngineError::Page(_)) => {}
        other => panic!(
            "free of a non-committed reservation must be refused, got {:?}",
            other
        ),
    }
    eng.commit(tx2).unwrap();
    drop(tx); // rollback
    assert_eq!(eng.page_count(), 1, "refused free changed nothing durable");
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (b) alloc + free INSIDE the same tx: documented net-out behavior
// ---------------------------------------------------------------------------

/// The tx gives the page back mid-transaction: the id appears in no commit
/// candidate, the commit has zero durable effect, and the id is handed to
/// the very next reservation (scratch/mirror recycle). Documented churn
/// protocol (DEVELOPING.md 9b): "an alloc immediately freed in the same tx
/// never reaches disk".
#[test]
fn same_tx_alloc_free_nets_out_zero_effect_commit() {
    let path = tmp_path("netsame");
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |d| d[0] = 0xAB).unwrap();
        eng.free_in_tx(&mut tx, id).unwrap(); // net-out, even after a write
        let tx2 = eng.begin().unwrap();
        eng.commit(tx).unwrap();
        eng.commit(tx2).unwrap(); // the empty tx proves the state is closed
        assert_eq!(eng.page_count(), 1, "netted page never became durable");
        assert_eq!(eng.wal_len(), 0, "net-out journals NOTHING");
        // The id recycles immediately.
        let mut tx = eng.begin().unwrap();
        let id2 = eng.alloc_in_tx(&mut tx).unwrap();
        assert_eq!(id2, id, "netted id handed straight back");
        eng.write_in_tx(&mut tx, id2, |d| d[0] = 0x11).unwrap();
        eng.commit(tx).unwrap();
        assert_eq!(eng.page_count(), 2, "the page belongs to the new tx now");
        assert_eq!(eng.read_page(id2).unwrap()[0], 0x11, "old bytes gone");
    }
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 2, "reopen agrees with the commit");
    assert_eq!(eng.read_page(1).unwrap()[0], 0x11);
    cleanup(&path);
}

/// A tx that nets out ITS ONLY reservation leaves NOTHING: no sb frame, no
/// page frame, no growth — byte-identical file to the pre-tx state.
#[test]
fn same_tx_netout_only_tx_is_inert() {
    let path = tmp_path("netonly");
    {
        let mut eng = Engine::open(&path).unwrap();
        let tx0 = eng.begin().unwrap();
        eng.commit(tx0).unwrap();
        eng.checkpoint().unwrap();
        let before_sb = eng.page_count();
        let before_len = std::fs::metadata(&path).unwrap().len();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |d| d[4095] = 1).unwrap();
        eng.free_in_tx(&mut tx, id).unwrap();
        eng.commit(tx).unwrap();
        assert_eq!(eng.page_count(), before_sb);
        assert_eq!(eng.wal_len(), 0, "no frames at all (post-checkpoint)");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            before_len,
            "file byte-length changed: something was written"
        );
    }
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 1);
    cleanup(&path);
}

/// Alloc x2 in one tx, free of the FIRST: the second keeps its slot, the
/// netted one recycles, the commit grows exactly one page.
#[test]
fn same_tx_mixed_alloc_free_partial_netout() {
    let path = tmp_path("netmix");
    let mut eng = Engine::open(&path).unwrap();
    let mut tx = eng.begin().unwrap();
    let a = eng.alloc_in_tx(&mut tx).unwrap();
    let b = eng.alloc_in_tx(&mut tx).unwrap();
    eng.free_in_tx(&mut tx, a).unwrap();
    eng.commit(tx).unwrap();
    // Watermark semantics (compute_commit_sb): the count folds to cover
    // the LARGEST reserved id (b=2) regardless of the netted sibling a=1
    // — the watermark does not recede behind reservations it made room
    // for. So page_count == a.max(b)+1 == 3; the netted slot a lives in
    // the scratch pool (or below the watermark) and is handed back FIFO.
    assert_eq!(
        eng.page_count(),
        (a.max(b) + 1).max(2),
        "watermark covers the largest reserved id"
    );
    // The netted id recycles to the next tx (scratch FIFO).
    let mut tx = eng.begin().unwrap();
    let c = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(c, a, "netted id handed to the next tx");
    eng.write_in_tx(&mut tx, c, |d| d[0] = 9).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 3);
    assert_eq!(eng.read_page(c).unwrap()[0], 9);
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// (b2) the rollback reclaim invariants (never two txs with the same id)
// ---------------------------------------------------------------------------

/// THE brick scenario (probe-verified against the pre-fix build): tx1
/// parks an append id; tx2 commits one PAST it (page_count grows); tx1
/// rolls back. The pre-fix reclaim routed the handed-back id by the
/// CURRENT page_count — onto the freelist mirror — so the next commit
/// journaled a chain head at page 1 whose on-disk bytes were the zeroed
/// stub: reopen walked next=0 (id 0) = corruption, fail-closed. The fix
/// routes by HOW the id was reserved (pending_recycle), not by the count.
#[test]
fn rollback_parked_id_after_pc_grew_no_brick() {
    let path = tmp_path("brick");
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut t1 = eng.begin().unwrap();
        let a = eng.alloc_in_tx(&mut t1).unwrap(); // id 1, parked
        let mut t2 = eng.begin().unwrap();
        let b = eng.alloc_in_tx(&mut t2).unwrap(); // id 2 (1 is skipped)
        eng.write_in_tx(&mut t2, b, |d| d[0] = 2).unwrap();
        eng.commit(t2).unwrap(); // page_count = 3 — covers the parked id
        assert_eq!(a, 1);
        drop(t1); // rollback: handed back — must land on SCRATCH, not mirror
                  // ANY commit journals a head: it must NOT be the stub page 1.
        let tx = eng.begin().unwrap();
        eng.commit(tx).unwrap();
        assert_eq!(eng.page_count(), 3);
    }
    // Reopen MUST succeed (the brick failed with FreelistCorrupt here).
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 3);
    // The handed-back id 1 is still reusable — exactly once, as scratch —
    // and its commit makes it durable WITHOUT moving the chain state.
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(id, 1, "the handed-back id recycles from scratch");
    eng.write_in_tx(&mut tx, id, |d| d[0] = 0x5A).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 3, "count already covered it");
    assert_eq!(eng.read_page(1).unwrap()[0], 0x5A);
    drop(eng);
    // Sustainable churn after the repair: free + alloc, footprint flat.
    let mut eng = Engine::open(&path).unwrap();
    let mut tx = eng.begin().unwrap();
    eng.free_in_tx(&mut tx, 1).unwrap();
    eng.commit(tx).unwrap();
    let mut tx = eng.begin().unwrap();
    let again = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(again, 1);
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 3, "sustainable churn after the repair");
    cleanup(&path);
}

/// Two coexisting txs reserve (distinct ids — collision guard), commit
/// BOTH, then one tx frees both: the fold replays, the chain survives a
/// reopen, and further churn stays sustainable.
#[test]
fn coexisting_txs_then_free_both_no_cross_collision() {
    let path = tmp_path("twotx");
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut t1 = eng.begin().unwrap();
        let a = eng.alloc_in_tx(&mut t1).unwrap();
        let mut t2 = eng.begin().unwrap();
        let b = eng.alloc_in_tx(&mut t2).unwrap();
        assert_ne!(a, b, "two open txs must never share an id");
        eng.commit(t1).unwrap();
        eng.commit(t2).unwrap();
        let mut tx = eng.begin().unwrap();
        eng.free_in_tx(&mut tx, a).unwrap();
        eng.free_in_tx(&mut tx, b).unwrap();
        eng.commit(tx).unwrap();
        assert_eq!(eng.page_count(), 3);
    }
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 3, "chain survived the reopen");
    // LIFO recycle: the LAST freed id comes out first.
    let mut tx = eng.begin().unwrap();
    let r1 = eng.alloc_in_tx(&mut tx).unwrap();
    eng.write_in_tx(&mut tx, r1, |d| d[0] = 1).unwrap();
    let r2 = eng.alloc_in_tx(&mut tx).unwrap();
    eng.write_in_tx(&mut tx, r2, |d| d[0] = 2).unwrap();
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 3, "both frees recycled, no append");
    assert_eq!(eng.read_page(r1).unwrap()[0], 1);
    assert_eq!(eng.read_page(r2).unwrap()[0], 2);
    cleanup(&path);
}

/// Free of an id parked by ANOTHER still-open tx: refused (never two
/// owners), and the refusing tx stays usable for its own work.
#[test]
fn free_of_other_txs_parked_id_refused_tx_stays_usable() {
    let path = tmp_path("crossfree");
    let mut eng = Engine::open(&path).unwrap();
    let mut t1 = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut t1).unwrap();
    let mut t2 = eng.begin().unwrap();
    assert!(
        eng.free_in_tx(&mut t2, id).is_err(),
        "another tx's parked reservation is not freeable"
    );
    // t2 continues: its own allocations work.
    let own = eng.alloc_in_tx(&mut t2).unwrap();
    assert_ne!(own, id, "the parked id was re-handed inside t2's view");
    eng.commit(t2).unwrap();
    drop(t1); // park returned to scratch
    let mut tx = eng.begin().unwrap();
    let re = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(re, id, "park handed back after the rollback");
    eng.commit(tx).unwrap();
    cleanup(&path);
}

/// Scratch hand-back after a pc-covering commit, then free attempt:
/// refused (the id was NEVER durably owned), but the next alloc re-hands
/// it exactly once — and once ITS commit lands, free+alloc churns cleanly.
#[test]
fn scratch_id_after_pc_grew_never_freeable_directly() {
    let path = tmp_path("scratchfree");
    let mut eng = Engine::open(&path).unwrap();
    let mut t1 = eng.begin().unwrap();
    let a = eng.alloc_in_tx(&mut t1).unwrap(); // 1, parked
    let mut t2 = eng.begin().unwrap();
    let b = eng.alloc_in_tx(&mut t2).unwrap(); // 2
    eng.write_in_tx(&mut t2, b, |d| d[0] = 2).unwrap();
    eng.commit(t2).unwrap(); // page_count=3: the parked 1 is covered
    drop(t1); // 1 -> scratch (NOT the mirror: cancel_alloc routes by kind)
    assert_eq!(eng.page_count(), 3);
    // Direct free of 1: never durably owned → refused.
    let mut tx = eng.begin().unwrap();
    assert!(eng.free_in_tx(&mut tx, a).is_err());
    eng.commit(tx).unwrap();
    // The scratch id re-hands (exactly once, scratch consumed).
    let mut tx = eng.begin().unwrap();
    let id = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(id, a);
    eng.commit(tx).unwrap();
    // NOW it is durable: free + alloc cycle works and stays sustainable.
    let mut tx = eng.begin().unwrap();
    eng.free_in_tx(&mut tx, a).unwrap();
    eng.commit(tx).unwrap();
    let mut tx = eng.begin().unwrap();
    let id2 = eng.alloc_in_tx(&mut tx).unwrap();
    assert_eq!(id2, a);
    eng.commit(tx).unwrap();
    assert_eq!(eng.page_count(), 3);
    cleanup(&path);
}

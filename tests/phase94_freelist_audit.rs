//! Phase 94 — freelist consistency (regression tests).
//!
//! Two freelist failure modes found by systematic probes of the rollback /
//! recycle paths (the temporary probes are converted into these permanent
//! inverted tests):
//!
//! c1: claim_durable_scratch journaled the freelist node ONLY in the WAL —
//!     never staged it in the pager cache. A later checkpoint() truncated
//!     the log (the only copy of the node): reopen failed closed with
//!     FreelistCorrupt. Fix: the node page is staged in the cache after
//!     the successful sync (a failed journal leaves no cache trace).
//! c2: compute_commit_sb cloned the raw mirror as the head value. After a
//!     rollback of ANOTHER tx's recycled reserve (the mirror carried an id
//!     the committed chain no longer contained) a write-only commit
//!     journaled a head the live sb never took; the WAL replay then chained
//!     a zeroed page -> FreelistCorrupt (no brick when a checkpoint had
//!     already truncated the log — the divergence lives in the journal).
//!     Fix: the fold mirrors commit_alloc's head rule (freed -> its id;
//!     recycled fold -> current chain tail; else unchanged).
//! Both scenarios assert the FIXED behavior: reopen must SUCCEED and the
//! session must keep working (the old failure is the pass condition).
use vidgedb::engine::{Engine, EngineError};
use vidgedb::pager::PageError;

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn cleanup(d: &std::path::Path) {
    let _ = std::fs::remove_dir_all(d);
}
fn reopen_ok(p: &std::path::Path) -> Result<u32, EngineError> {
    match Engine::open(p) {
        Ok(mut e) => {
            // The session keeps working after recovery: write+read roundtrip.
            let mut tx = e.begin().unwrap();
            let id = e.alloc_in_tx(&mut tx).unwrap();
            e.write_in_tx(&mut tx, id, |dg| dg[0] = 1).unwrap();
            e.commit(tx).unwrap();
            assert_eq!(e.read_page(id).unwrap()[0], 1);
            Ok(e.page_count())
        }
        Err(e) => Err(e),
    }
}

/// c1: neighbor commit covers a rolled-back id -> scratch claim -> checkpoint
/// (truncate the only copy of the node) -> reopen. Was: FreelistCorrupt.
#[test]
fn phase94_c1_claimed_scratch_node_survives_checkpoint() {
    let d = dir("p94_c1");
    let p = d.join("twin.vdg");
    {
        let mut eng = Engine::open(&p).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |dg| dg[0] = 7).unwrap();
        eng.commit(tx).unwrap();
    }
    {
        let mut eng = Engine::open(&p).unwrap();
        let mut a = eng.begin().unwrap();
        let _a_id = eng.alloc_in_tx(&mut a).unwrap();
        let mut b = eng.begin().unwrap();
        let b_id = eng.alloc_in_tx(&mut b).unwrap();
        eng.write_in_tx(&mut b, b_id, |dg| dg[0] = 9).unwrap();
        eng.commit(b).unwrap();
        drop(a); // id -> scratch; next engine call claims it durably
        let t = eng.begin().unwrap();
        drop(t);
        eng.checkpoint().unwrap(); // the WAL (only copy of the node) is gone
    }
    let count =
        reopen_ok(&p).expect("reopen must be healthy (probe c1 CONFIRMED the brick pre-fix)");
    assert!(count >= 2);
    cleanup(&d);
}

/// c1 control: WITHOUT a covering neighbor commit the claim never fires —
/// the shape alone must stay healthy (discriminator validity).
#[test]
fn phase94_c1_control_uncovered_rollback_is_unaffected() {
    let d = dir("p94_c1c");
    let p = d.join("twin.vdg");
    {
        let mut eng = Engine::open(&p).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |dg| dg[0] = 7).unwrap();
        eng.commit(tx).unwrap();
    }
    {
        let mut eng = Engine::open(&p).unwrap();
        let mut a = eng.begin().unwrap();
        let _ = eng.alloc_in_tx(&mut a).unwrap();
        drop(a);
        let t = eng.begin().unwrap();
        drop(t);
        eng.checkpoint().unwrap();
    }
    reopen_ok(&p).expect("control must open");
    cleanup(&d);
}

/// c2: A recycles h1; B recycles h2 AND commits; A rolls back; D writes an
/// EXISTING page without allocating (the alloc would pop the diverged head
/// back off the mirror) and commits -> the journaled fold must equal the
/// live fold. Reopen applies D's sb frame — was: replay chained a zero page.
#[test]
fn phase94_c2_rolledback_recycle_neighbor_commit_writeonly_commit_reopens() {
    let d = dir("p94_c2");
    let p = d.join("twin.vdg");
    {
        let mut eng = Engine::open(&p).unwrap();
        for k in 0u32..4 {
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            assert_eq!(id, k + 1);
            eng.write_in_tx(&mut tx, id, |dg| dg[0] = 42).unwrap();
            eng.commit(tx).unwrap();
        }
        for id in [3u32, 2, 1] {
            let mut tx = eng.begin().unwrap();
            eng.free_in_tx(&mut tx, id).unwrap();
            eng.commit(tx).unwrap();
        }
    }
    {
        let mut eng = Engine::open(&p).unwrap();
        let mut a = eng.begin().unwrap();
        let h1 = eng.alloc_in_tx(&mut a).unwrap();
        let mut b = eng.begin().unwrap();
        let h2 = eng.alloc_in_tx(&mut b).unwrap();
        eng.write_in_tx(&mut b, h2, |dg| dg[0] = 77).unwrap();
        eng.commit(b).unwrap();
        assert_eq!(h1, 1, "A must recycle the chain head");
        assert_eq!(h2, 2, "B must recycle the next node");
        drop(a); // mirror now carries id 1 again; durable head = 3
        let mut dd = eng.begin().unwrap();
        eng.write_in_tx(&mut dd, 4, |dg| dg[0] = 55).unwrap();
        eng.commit(dd).unwrap(); // journaled sb == live sb (the c2 invariant)
    }
    let count =
        reopen_ok(&p).expect("reopen must be healthy (probe c2 CONFIRMED FreelistCorrupt pre-fix)");
    assert_eq!(count, 5);
    cleanup(&d);
}

/// c2 fixed-variant: a CHECKPOINT after the divergent commit must also be
/// safe (pre-fix it merely sealed the stale file head=3 while the WAL held
/// the divergent head=1; post-fix the fold and the live sb ARE equal).
#[test]
fn phase94_c2_checkpoint_variant_is_also_consistent() {
    let d = dir("p94_c2c");
    let p = d.join("twin.vdg");
    {
        let mut eng = Engine::open(&p).unwrap();
        for k in 0u32..4 {
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            assert_eq!(id, k + 1);
            eng.write_in_tx(&mut tx, id, |dg| dg[0] = 42).unwrap();
            eng.commit(tx).unwrap();
        }
        for id in [3u32, 2, 1] {
            let mut tx = eng.begin().unwrap();
            eng.free_in_tx(&mut tx, id).unwrap();
            eng.commit(tx).unwrap();
        }
        let mut a = eng.begin().unwrap();
        let h1 = eng.alloc_in_tx(&mut a).unwrap();
        let mut b = eng.begin().unwrap();
        let h2 = eng.alloc_in_tx(&mut b).unwrap();
        eng.write_in_tx(&mut b, h2, |dg| dg[0] = 77).unwrap();
        eng.commit(b).unwrap();
        assert_eq!(h1, 1);
        assert_eq!(h2, 2);
        drop(a);
        let mut dd = eng.begin().unwrap();
        eng.write_in_tx(&mut dd, 4, |dg| dg[0] = 55).unwrap();
        eng.commit(dd).unwrap();
        eng.checkpoint().unwrap();
    }
    reopen_ok(&p).expect("checkpoint variant must be healthy");
    cleanup(&d);
}

/// Phase 94 guard: a data write onto a freelist page (a durable node, or a
/// page this tx freed) is refused AT CALL time — commit frames can no longer
/// destroy a chain node (pre-guard failure class: reopen-time
/// FreelistCorrupt). A write-only tx onto an OWNED page keeps the head.
#[test]
fn phase94_writeonto_freelist_refused_and_head_never_moves() {
    let d = dir("p94_guard");
    let p = d.join("twin.vdg");
    {
        let mut eng = Engine::open(&p).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap(); // id 1
        eng.write_in_tx(&mut tx, id, |dg| dg[0] = 7).unwrap();
        eng.commit(tx).unwrap();
        let mut tx2 = eng.begin().unwrap();
        eng.free_in_tx(&mut tx2, 1).unwrap();
        eng.commit(tx2).unwrap(); // chain = [1], head = 1
                                  // The guard: writing the freelist page is refused in-session.
        let mut tx3 = eng.begin().unwrap();
        let r = eng.write_in_tx(&mut tx3, 1, |dg| dg[0] = 5);
        assert!(
            matches!(r, Err(EngineError::Page(PageError::FreelistCorrupt))),
            "expected the write-onto-freelist guard, got {r:?}"
        );
        drop(tx3);
        // A write-only commit on an OWNED page must not move the head: the
        // tx recycles id 1 (chain empties -> head NULL) — then reopen + a
        // fresh write are healthy.
        let mut txw = eng.begin().unwrap();
        let w = eng.alloc_in_tx(&mut txw).unwrap();
        assert_eq!(w, 1);
        eng.write_in_tx(&mut txw, w, |dg| dg[0] = 8).unwrap();
        eng.commit(txw).unwrap();
        eng.checkpoint().unwrap();
    }
    reopen_ok(&p).expect("guarded session must reopen healthy");
    cleanup(&d);
}

/// A superblock from a NEWER build refuses to open (fail-closed), like the
/// WAL already refuses unknown opcodes.
#[test]
fn phase94_superblock_version_refused() {
    let d = dir("p94_ver");
    let p = d.join("twin.vdg");
    {
        Engine::open(&p).unwrap();
    }
    // Flip the version byte on the disk image.
    let mut raw = std::fs::read(&p).unwrap();
    raw[4] = 2;
    std::fs::write(&p, raw).unwrap();
    let verdict = match Engine::open(&p) {
        Err(EngineError::Page(PageError::UnsupportedVersion(2))) => "REFUSED",
        Err(e) => panic!("wrong error variant: {e:?}"),
        Ok(_) => panic!("expected a refusal, the file opened fine"),
    };
    assert_eq!(verdict, "REFUSED");
    cleanup(&d);
}

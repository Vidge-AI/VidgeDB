//! Phase 8.5 — WAL hardening: recovery fail-closed on unknown opcode,
//! MAX_FRAME_SIZE, commit-marker viability (spec §18, docs/audit second
//! opinion probes converted to assertions).
//!
//! Probes (parent-verified empirically before this suite):
//! (a) a committed `WalOp::Custom { tag != COMMIT_TAG }` used to be
//!     SILENTLY IGNORED by `Engine::open`'s recovery (fail-open): a DB
//!     written by a newer/foreign version reopened into a mutilated state.
//!     Now the open REFUSES with `WalError::UnknownOpcode` (negative
//!     assertion — exactly the probe, inverted);
//! (b) the commit marker itself is encoded as Custom tag `u32::MAX` with
//!     empty data (`COMMIT_TAG`) — the fail-closed check must stay
//!     compatible with it: pages + marker must replay normally;
//! (c) a forged frame header `len = 0xFFFFFFFF` used to be trusted
//!     silently (the old code only checked it against file remaining) —
//!     `scan()` now rejects it outright with `CorruptFrame`
//!     (`MAX_FRAME_SIZE` defense-in-depth bound);
//! (d) regression: every existing recovery path stays green (phase81 tx
//!     recovery, crash harness — run separately, `crash_test.sh` 7/7).

use vidgedb::engine::{Engine, EngineError};
use vidgedb::pager::PAGE_SIZE;
use vidgedb::wal::{Wal, WalError, WalOp};

fn tmp_path(name: &str) -> String {
    let mut p = std::env::temp_dir().to_path_buf();
    p.push(format!("vidgedb_p85_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

/// (a) The probe converted to a NEGATIVE assertion: an unknown Custom tag
/// inside a COMMITTED group must make `Engine::open` FAIL (no panic), with
/// the UnknownOpcode error — previously open() succeeded and dropped the
/// frame silently (fail-open).
#[test]
fn unknown_custom_tag_fails_open() {
    for tag in [9999u32, 1, 7, u32::MAX - 1] {
        let path = tmp_path(&format!("unknown_tag_{}", tag));
        // A healthy committed page, THEN a committed group carrying the
        // unknown opcode (the fail-closed check fires at its commit).
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            eng.write_in_tx(&mut tx, id, |d| d[0] = 1).unwrap();
            eng.commit(tx).unwrap();
            drop(eng);
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&WalOp::Custom {
                tag,
                data: vec![0xDE, 0xAD],
            })
            .unwrap();
            wal.commit().unwrap();
            wal.sync().unwrap();
        }
        match Engine::open(&path) {
            Ok(_) => panic!("unknown Custom tag {} must fail the open", tag),
            Err(EngineError::Wal(WalError::UnknownOpcode { tag: t })) => assert_eq!(t, tag),
            Err(e) => panic!("unexpected error for tag {}: {:?}", tag, e),
        }
        cleanup(&path);
    }
}

/// (b) The simulated commit marker (Custom tag u32::MAX, empty data) must
/// remain VIABLE: committed pages before it replay, open succeeds — the
/// fail-closed check keys on COMMIT_TAG, nothing else.
#[test]
fn simulated_commit_marker_still_replays() {
    let path = tmp_path("marker_sim");
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |d| d[3] = 42).unwrap();
        eng.commit(tx).unwrap();
        drop(eng);
        // Simulate the wal.rs commit-marker encoding path: a Custom frame
        // tag = COMMIT_TAG (u32::MAX), empty data, committed + synced.
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&WalOp::Custom {
            tag: u32::MAX,
            data: vec![],
        })
        .unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
    }
    // open must SUCCEED and the committed page must have replayed.
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 2, "page before the marker replayed");
    assert_eq!(eng.read_page(1).unwrap()[3], 42);
    cleanup(&path);
}

/// (c) A forged frame header (len = 0xFFFFFFFF) must be rejected by
/// `scan()` with CorruptFrame — previously it was trusted silently (the
/// old code measured it only against the file's remaining bytes, so the
/// forged header "passed" in microseconds and replayed the prefix).
#[test]
fn forged_giant_frame_len_rejected_by_scan() {
    let path = tmp_path("giant_len");
    {
        let mut eng = Engine::open(&path).unwrap();
        let mut tx = eng.begin().unwrap();
        let id = eng.alloc_in_tx(&mut tx).unwrap();
        eng.write_in_tx(&mut tx, id, |d| d[0] = 1).unwrap();
        eng.commit(tx).unwrap();
        drop(eng);
        // Forge a frame header right after the committed frames:
        // len = 0xFFFFFFFF, crc = 0 — CRC can never match, the length
        // itself is impossible. (Appending, not corrupting: the verified
        // prefix would replay; the point is that scan() REFUSES.)
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(format!("{}-wal", path))
            .unwrap();
        f.seek(SeekFrom::End(0)).unwrap();
        f.write_all(&0xFFFFFFFFu32.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&[0x41, 0x41, 0x41, 0x41]).unwrap();
        f.sync_all().unwrap();
    }
    let mut wal = Wal::open(&path).unwrap();
    match wal.scan() {
        Ok(_) => panic!("forged giant frame_len must be rejected by scan"),
        Err(WalError::CorruptFrame) => {}
        Err(e) => panic!("unexpected error: {:?}", e),
    }
    // The engine propagates the same refusal (fail-closed open).
    match Engine::open(&path) {
        Ok(_) => panic!("forged giant frame_len must fail the open"),
        Err(EngineError::Wal(WalError::CorruptFrame)) => {}
        Err(e) => panic!("unexpected error: {:?}", e),
    }
    cleanup(&path);
}

/// (d) Regression guard: the boundary of the new bound must not reject
/// legitimate frames. A page op (4 KiB payload) and a marker pass; a
/// payload just over MAX_FRAME_SIZE is refused.
#[test]
fn frame_size_bound_boundary() {
    let path = tmp_path("bound");
    // At/under the bound: accepted (scan sees a well-formed WAL).
    {
        let mut wal = Wal::open(&path).unwrap();
        let data = vec![0u8; PAGE_SIZE]; // real op size, 16x under the cap
        wal.append(&WalOp::SetPage { page_id: 1, data }).unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
        assert_eq!(wal.scan().unwrap().len(), 2);
    }
    cleanup(&path);
    {
        // Just over the cap: rejected outright.
        let mut wal = Wal::open(&path).unwrap();
        let data = vec![0u8; vidgedb::wal::MAX_FRAME_SIZE + 1];
        wal.append(&WalOp::Custom { tag: 5, data }).unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
        match wal.scan() {
            Ok(_) => panic!("over-cap frame must be rejected"),
            Err(WalError::CorruptFrame) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }
    cleanup(&path);
}

/// (e) Full regression: a normal Engine lifecycle (alloc, write, commit,
/// rollback, reopen, checkpoint) is untouched by the hardening — the
/// existing 152 tests stay green, this one exercises the happy path
/// end-to-end in the same suite.
#[test]
fn normal_engine_flow_unaffected() {
    let path = tmp_path("happy");
    {
        let mut eng = Engine::open(&path).unwrap();
        for v in 0..5u8 {
            let mut tx = eng.begin().unwrap();
            let id = eng.alloc_in_tx(&mut tx).unwrap();
            eng.write_in_tx(&mut tx, id, |d| d[0] = v).unwrap();
            eng.commit(tx).unwrap();
        }
        // A rollback interleave.
        let mut tx = eng.begin().unwrap();
        let _ = eng.alloc_in_tx(&mut tx).unwrap();
        drop(tx);
        eng.checkpoint().unwrap();
    }
    let mut eng = Engine::open(&path).unwrap();
    assert_eq!(eng.page_count(), 6); // sb + 5 pages
    for id in 1..=5u32 {
        assert_eq!(eng.read_page(id).unwrap()[0], (id - 1) as u8);
    }
    cleanup(&path);
}

//! Write-Ahead Log for VidgeDB — Phase 1 (+ phase 8.5 hardening).
//!
//! Spec §18: the WAL SHALL provide crash recovery with atomic commit,
//! ordered durability, recovery after process crash / machine restart,
//! detection of incomplete transactions, deterministic replay.
//! Spec §38.5: crash during write / commit / WAL flush, partial transaction.
//!
//! Design (spec §21 research record — SQLite WAL + Redis AOF discipline):
//!
//! - The WAL is a separate append-only file (`<db>.vdg-wal`) of frames.
//! - One frame = fixed-size record: [len u32][crc32 u32][payload bytes].
//!   A frame whose header is present but whose payload/crc is incomplete or
//!   bad marks the transaction INCOMPLETE — replay stops there (detection of
//!   torn writes, spec §18).
//! - Transactions: a `commit` record closes a group of preceding operation
//!   frames. Replay applies operations only up to the last valid commit.
//! - Determinism: replay is a pure fold over the WAL bytes — same WAL always
//!   produces the same state.
//! - The commit marker surfaces in memory as
//!   `WalOp::Custom { tag: COMMIT_TAG, data: [] }` (`is_commit = true`):
//!   recovery keys on that tag. Every OTHER Custom tag in a committed
//!   group is an opcode this build does not know and refuses the DB
//!   (fail-closed, see `WalError::UnknownOpcode` / `MAX_FRAME_SIZE`).
//! - Checkpoint: after a successful WAL replay + flush of the page store,
//!   the log is truncated (checkpoint) so the log cannot grow unbounded.
//!
//! crc32: computed with a small software implementation (IEEE polynomial) —
//! no external dependency, deterministic across platforms.
//!
//! ## Torn tail vs corruption mid-WAL (documented v1 limit)
//!
//! Frame format v1 has NO per-frame position/LSN and NO checksum chain.
//! After a frame that fails to verify, `scan()` returns the verified
//! prefix and stops — it cannot know how much of the log follows. Two
//! cases are therefore indistinguishable in v1, and this is the DOCUMENTED
//! contract (do NOT rewrite the protocol around it):
//!
//! - Torn tail (crash mid-write): the unfinished last frame is the damage;
//!   every committed transaction before it replays. Recovery is exact.
//! - Corruption mid-WAL (TX1 durable, TX2 durable, then TX2's bytes get
//!   corrupted): the same mechanism keeps TX1 and silently forfeits the
//!   rest — `Engine::open` succeeds WITHOUT any warning. A v1 torn write
//!   and a v1 mid-log corruption look identical (a bad CRC is exactly
//!   what a torn write produces), so v1 chooses the conservative prefix
//!   replay rather than refusing a database that may be perfectly intact.
//!   The future mechanism that separates the two is a per-frame LSN /
//!   checksum chain (each frame names its predecessor): recorded in the
//!   roadmap (spec §21 decision record), NOT implemented in v1.
//!
//! What v1 DOES refuse (fail-closed since phase 8.5):
//! - an unknown opcode: any committed `WalOp::Custom` whose tag is not
//!   [`COMMIT_TAG`] aborts the open with `WalError::UnknownOpcode`
//!   (previously the frame was silently skipped: a file written by a
//!   future version would have reopened into a mutilated state);
//! - an impossible frame size: a header announcing more than
//!   [`MAX_FRAME_SIZE`] is rejected as `WalError::CorruptFrame` in
//!   `scan()` itself (a torn write always leaves a REAL, small length —
//!   only corruption or a foreign writer can produce a giant one).
//!
//! Unsafe Rust policy (spec §49): none in this module.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Operation record carried by a frame payload.
/// Phase 1 defines the three primitive writes the page store needs;
/// higher layers serialize their payloads inside `data`.
#[derive(Debug, Clone, PartialEq)]
pub enum WalOp {
    /// Set a data page's bytes (page_id, bytes[PAGE_SIZE]).
    SetPage { page_id: u32, data: Vec<u8> },
    /// Update superblock fields atomically with the commit.
    SetSuperblock { page_count: u32, freelist_head: u32 },
    /// User-defined record (Phase 2+ stores will use this for entities etc).
    Custom { tag: u32, data: Vec<u8> },
}

impl WalOp {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            WalOp::SetPage { page_id, data } => {
                out.push(1);
                out.extend_from_slice(&page_id.to_le_bytes());
                debug_assert_eq!(data.len(), crate::pager::PAGE_SIZE);
                out.extend_from_slice(data);
            }
            WalOp::SetSuperblock {
                page_count,
                freelist_head,
            } => {
                out.push(2);
                out.extend_from_slice(&page_count.to_le_bytes());
                out.extend_from_slice(&freelist_head.to_le_bytes());
            }
            WalOp::Custom { tag, data } => {
                out.push(3);
                out.extend_from_slice(&tag.to_le_bytes());
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<WalOp, WalError> {
        match buf.first() {
            Some(&1) => {
                if buf.len() < 5 {
                    return Err(WalError::CorruptFrame);
                }
                let page_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let data = buf[5..].to_vec();
                if data.len() != crate::pager::PAGE_SIZE {
                    return Err(WalError::CorruptFrame);
                }
                Ok(WalOp::SetPage { page_id, data })
            }
            Some(&2) => {
                if buf.len() != 9 {
                    return Err(WalError::CorruptFrame);
                }
                Ok(WalOp::SetSuperblock {
                    page_count: u32::from_le_bytes(buf[1..5].try_into().unwrap()),
                    freelist_head: u32::from_le_bytes(buf[5..9].try_into().unwrap()),
                })
            }
            Some(&3) => {
                if buf.len() < 9 {
                    return Err(WalError::CorruptFrame);
                }
                let tag = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let len = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;
                if buf.len() != 9 + len {
                    return Err(WalError::CorruptFrame);
                }
                Ok(WalOp::Custom {
                    tag,
                    data: buf[9..].to_vec(),
                })
            }
            _ => Err(WalError::CorruptFrame),
        }
    }
}

#[derive(Debug)]
pub enum WalError {
    /// Frame header present but payload incomplete/corrupt (torn write).
    CorruptFrame,
    /// CRC mismatch: frame written partially or corrupted.
    CrcMismatch,
    /// A committed `WalOp::Custom` carries a tag this build does not know
    /// (neither a known opcode nor [`COMMIT_TAG`]). Phase 8.5 fail-closed
    /// policy: instead of silently skipping the frame and reopening a
    /// file written by a newer/foreign version into a mutilated state,
    /// the DB refuses to open. `tag` is the unknown opcode value.
    UnknownOpcode {
        tag: u32,
    },
    Io(io::Error),
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

/// One WAL frame on disk: [len u32][crc32 u32][payload].
pub const FRAME_HEADER: usize = 8;

/// Hardening cap on one frame's payload (phase 8.5, defense in depth):
/// a header announcing more than this is rejected by `scan()` as
/// `WalError::CorruptFrame` instead of being trusted on a huge log. 64 KiB
/// leaves a 16x margin over the largest current op (SetPage = 4 KiB) and
/// headroom for big future Custom payloads. A genuine torn write leaves a
/// REAL (small) length, so this never fires on a crash-damaged log — only
/// on corruption or a foreign file format (disk layout unchanged: the
/// const is a reader-side bound, not a new field).
pub const MAX_FRAME_SIZE: usize = 64 * 1024;

/// Reserved Custom tag that IS the commit marker: `scan()` decodes the
/// 1-byte `[0xFF]` commit payload as `WalOp::Custom { tag: COMMIT_TAG,
/// data: [] }` with `is_commit = true`. Recovery treats any committed
/// Custom whose tag differs from [`COMMIT_TAG`] as an unknown opcode and
/// fails closed (`WalError::UnknownOpcode`) — see the module doc.
pub(crate) const COMMIT_TAG: u32 = u32::MAX;

pub fn crc32(data: &[u8]) -> u32 {
    // Software CRC-32 (IEEE 802.3), table-based implementation.
    // The 256-entry table is baked at compile time (const fn) — no runtime
    // init, no allocation, deterministic across platforms. Phase 9 perf
    // audit: the former bitwise loop measured 23.2 µs per 4 KiB frame vs
    // 11.0 µs here (2.1x) and WAL appends were 13% of the relation_insertion
    // benchmark. Disk format unchanged: identical CRC values (verified
    // bitwise-vs-table in bench_detail).
    const fn make_table() -> [u32; 256] {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    }
    static TABLE: [u32; 256] = make_table();
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc = (crc >> 8) ^ TABLE[((crc ^ (b as u32)) & 0xFF) as usize];
    }
    !crc
}

/// Commit marker payload: a 1-byte frame `COMMIT_PAYLOAD = [0xFF]`.
/// (An empty-length frame with a dedicated marker byte would be ambiguous
/// with a zero-len op.) `scan()` surfaces it in memory as
/// `WalOp::Custom { tag: COMMIT_TAG, data: [] }` — see the module doc for
/// the fail-closed treatment of any other committed Custom tag.
pub const COMMIT_PAYLOAD: [u8; 1] = [0xFF];

/// The write-ahead log.
pub struct Wal {
    path: PathBuf,
    file: File,
}

impl Wal {
    /// Open (or create) the WAL for a database at `<path>.vdg`.
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Wal, WalError> {
        let mut p = db_path.as_ref().to_path_buf().into_os_string();
        p.push("-wal");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&p)?;
        Ok(Wal {
            path: p.into(),
            file,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one frame (operation) to the log. Does NOT commit.
    pub fn append(&mut self, op: &WalOp) -> Result<(), WalError> {
        self.append_frame(op.encode())
    }

    /// Append the commit marker. Only after this returns (and sync_wal) is
    /// the transaction durable (ordered durability, spec §18).
    pub fn commit(&mut self) -> Result<(), WalError> {
        self.append_frame(COMMIT_PAYLOAD.to_vec())
    }

    fn append_frame(&mut self, payload: Vec<u8>) -> Result<(), WalError> {
        let len = payload.len() as u32;
        let crc = crc32(&payload);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;
        Ok(())
    }

    /// fsync the WAL file (commit durability boundary).
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Read every valid committed frame; stop at the first torn/corrupt
    /// frame or at end of log.
    pub fn scan(&mut self) -> Result<Vec<(WalOp, bool)>, WalError> {
        let len = self.file.seek(SeekFrom::End(0))?;
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&self.file);
        let mut frames = Vec::new();
        let mut pos: u64 = 0;
        while pos + FRAME_HEADER as u64 <= len {
            let mut hdr = [0u8; FRAME_HEADER];
            if reader.read_exact(&mut hdr).is_err() {
                break; // torn header
            }
            let frame_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
            let frame_crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
            // Impossible size: reject outright (phase 8.5). Before this
            // bound the old code only checked `frame_len > remaining`, so
            // it would have waited for a giant tail to exist somewhere in
            // the file — i.e. never on a small/compacted WAL — and trusted
            // the corrupted length silently.
            if frame_len > MAX_FRAME_SIZE {
                return Err(WalError::CorruptFrame);
            }
            // Payload beyond EOF: torn write (the damage is the TAIL; the
            // verified prefix is exactly what replays).
            if frame_len > (len - pos - FRAME_HEADER as u64) as usize {
                break;
            }
            let mut payload = vec![0u8; frame_len];
            if reader.read_exact(&mut payload).is_err() {
                break; // torn payload
            }
            if crc32(&payload) != frame_crc {
                break; // corrupted tail: incomplete transaction
            }
            pos += (FRAME_HEADER + frame_len) as u64;
            if payload == COMMIT_PAYLOAD {
                // The commit marker's in-memory form (phase 8.5: named
                // constant instead of a bare u32::MAX literal).
                frames.push((
                    WalOp::Custom {
                        tag: COMMIT_TAG,
                        data: vec![],
                    },
                    true,
                ));
            } else {
                frames.push((WalOp::decode(&payload)?, false));
            }
        }
        Ok(frames)
    }

    /// Truncate the log (checkpoint done). The file stays allocated.
    pub fn truncate(&mut self) -> Result<(), WalError> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Remove the WAL file entirely (used after checkpoint for tests).
    pub fn destroy(self) -> Result<(), WalError> {
        drop(self.file);
        std::fs::remove_file(&self.path).map_err(WalError::Io)?;
        Ok(())
    }
}

/// Replay: apply committed operations to a page store, in order.
/// Deterministic: pure fold over frames (spec §18).
/// Returns the number of committed transactions replayed.
pub fn replay(frames: &[(WalOp, bool)], apply: &mut dyn FnMut(&WalOp)) -> usize {
    let mut committed_tx = 0;
    let mut pending: Vec<&WalOp> = Vec::new();
    for (op, is_commit) in frames {
        if *is_commit {
            for o in &pending {
                apply(o);
            }
            pending.clear();
            committed_tx += 1;
        } else {
            pending.push(op);
        }
    }
    // Frames after the last commit marker (uncommitted tail) are dropped.
    committed_tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::{Pager, PAGE_SIZE};

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_wal_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn commit_set_page(wal: &mut Wal, page_id: u32, byte: u8) {
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = byte;
        wal.append(&WalOp::SetPage { page_id, data }).unwrap();
        wal.commit().unwrap();
    }

    #[test]
    fn commit_and_replay() {
        let path = tmp_path("commit");
        let mut wal = Wal::open(&path).unwrap();
        commit_set_page(&mut wal, 5, 42);
        commit_set_page(&mut wal, 6, 7);
        wal.sync().unwrap();
        let frames = wal.scan().unwrap();
        assert_eq!(frames.len(), 4); // 2 ops + 2 commits
        let mut applied = Vec::new();
        let n = replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, data } = op {
                applied.push((*page_id, data[0]));
            }
        });
        assert_eq!(n, 2);
        assert_eq!(applied, vec![(5, 42), (6, 7)]);
        wal.destroy().unwrap();
    }

    #[test]
    fn uncommitted_tail_is_dropped() {
        let path = tmp_path("uncomm");
        let mut wal = Wal::open(&path).unwrap();
        commit_set_page(&mut wal, 1, 10);
        // Uncommitted group: appended but no commit marker.
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 99;
        wal.append(&WalOp::SetPage { page_id: 2, data }).unwrap();
        wal.sync().unwrap();
        let frames = wal.scan().unwrap();
        let mut applied = Vec::new();
        let n = replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, .. } = op {
                applied.push(*page_id);
            }
        });
        assert_eq!(n, 1);
        assert_eq!(applied, vec![1]); // page 2 op dropped
        wal.destroy().unwrap();
    }

    #[test]
    fn torn_frame_detected() {
        let path = tmp_path("torn");
        {
            let mut wal = Wal::open(&path).unwrap();
            commit_set_page(&mut wal, 1, 10);
            wal.sync().unwrap();
            // Simulate a torn write: append a frame then chop the file mid-payload.
            let mut data = vec![0u8; PAGE_SIZE];
            data[0] = 55;
            wal.append(&WalOp::SetPage { page_id: 2, data }).unwrap();
            wal.sync().unwrap();
        }
        // Truncate the file in the middle of the last frame's payload.
        let raw_path = path.to_string() + "-wal";
        let size = std::fs::metadata(&raw_path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&raw_path).unwrap();
        f.set_len(size - 100).unwrap();
        drop(f);
        let mut wal = Wal::open(&path).unwrap();
        let frames = wal.scan().unwrap();
        assert_eq!(frames.len(), 2); // only the committed op + its commit
        let mut applied = Vec::new();
        replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, .. } = op {
                applied.push(*page_id);
            }
        });
        assert_eq!(applied, vec![1]); // torn op 2 excluded
        wal.destroy().unwrap();
    }

    #[test]
    fn corrupt_crc_stops_replay() {
        let path = tmp_path("crc");
        let mut wal = Wal::open(&path).unwrap();
        commit_set_page(&mut wal, 1, 10);
        wal.sync().unwrap();
        // Flip a byte inside the frame payload region of a second frame.
        let mut data = vec![0u8; PAGE_SIZE];
        data[0] = 77;
        wal.append(&WalOp::SetPage { page_id: 2, data }).unwrap();
        wal.commit().unwrap();
        wal.sync().unwrap();
        drop(wal);
        // Corrupt one byte in the second frame's payload (last 4096+8 bytes).
        let raw_path = path.clone() + "-wal";
        let mut raw = std::fs::read(&raw_path).unwrap();
        let last = raw.len() - PAGE_SIZE; // inside the second payload
        raw[last] ^= 0xFF;
        std::fs::write(&raw_path, &raw).unwrap();
        let mut wal = Wal::open(&path).unwrap();
        let frames = wal.scan().unwrap();
        assert_eq!(frames.len(), 2); // second frame rejected
        let mut applied = Vec::new();
        replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, .. } = op {
                applied.push(*page_id);
            }
        });
        assert_eq!(applied, vec![1]);
        wal.destroy().unwrap();
    }

    #[test]
    fn checkpoint_truncates() {
        let path = tmp_path("ckpt");
        let mut wal = Wal::open(&path).unwrap();
        commit_set_page(&mut wal, 1, 10);
        wal.sync().unwrap();
        assert!(wal.scan().unwrap().len() == 2);
        wal.truncate().unwrap();
        assert!(wal.scan().unwrap().is_empty());
        // Log usable again after truncate.
        commit_set_page(&mut wal, 3, 30);
        wal.sync().unwrap();
        assert_eq!(wal.scan().unwrap().len(), 2);
        wal.destroy().unwrap();
    }

    #[test]
    fn deterministic_replay() {
        let path = tmp_path("det");
        let mut wal = Wal::open(&path).unwrap();
        commit_set_page(&mut wal, 1, 10);
        commit_set_page(&mut wal, 2, 20);
        wal.sync().unwrap();
        let frames = wal.scan().unwrap();
        let mut run1 = Vec::new();
        let mut run2 = Vec::new();
        let n1 = replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, data } = op {
                run1.push((*page_id, data[0]));
            }
        });
        let n2 = replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, data } = op {
                run2.push((*page_id, data[0]));
            }
        });
        assert_eq!(n1, n2);
        assert_eq!(run1, run2);
        wal.destroy().unwrap();
    }

    /// Spec §38.5: crash during WAL flush -> recovery replays what was
    /// committed before the crash and nothing more.
    #[test]
    fn recovery_after_crash_mid_wal() {
        let path = tmp_path("crash");
        let mut pager = Pager::open(&path).unwrap();
        let mut wal = Wal::open(&path).unwrap();
        // Two committed transactions, then a crash (no further writes).
        let page1 = pager.alloc().unwrap();
        let page2 = pager.alloc().unwrap();
        // Fold the reservations (commit-only protocol), then flush.
        pager.commit_alloc(page1);
        pager.commit_alloc(page2);
        pager.flush().unwrap();
        commit_set_page(&mut wal, page1, 11);
        commit_set_page(&mut wal, page2, 22);
        wal.sync().unwrap();
        // Crash: process dies here. Recovery = reopen + replay.
        let mut pager2 = Pager::open(&path).unwrap();
        let mut wal2 = Wal::open(&path).unwrap();
        let frames = wal2.scan().unwrap();
        let mut recovered = Vec::new();
        let n = replay(&frames, &mut |op| {
            if let WalOp::SetPage { page_id, data } = op {
                let p = pager2.get_for_write(*page_id).unwrap();
                p.data.copy_from_slice(data);
                recovered.push(*page_id);
            }
        });
        assert_eq!(n, 2);
        assert_eq!(recovered, vec![page1, page2]);
        assert_eq!(pager2.read(page2).unwrap().data[0], 22);
        // Checkpoint: flush recovered pages, truncate log.
        pager2.flush().unwrap();
        wal2.truncate().unwrap();
        wal2.destroy().unwrap();
        drop(pager2);
        drop(pager);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }
}

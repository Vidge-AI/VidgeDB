//! Phase 90 — PORTABLE single-writer lockfile (cross-OS DbWriteLock).
//!
//! Phase 89 shipped the single-writer guard with the stale-lock steal
//! driven by a LINUX-ONLY oracle: `/proc/<pid>` existence. On Windows and
//! macOS that path does not exist, so a crashed writer (SIGKILL, power
//! loss, panic) left a lockfile its holder could NEVER be proven dead
//! with — the `.vdg` refused every further writer open, forever. This
//! file pins the Phase-90 redesign:
//!
//! - the lockfile is a JSON record `{pid, acquired_at, heartbeat_at}`
//!   (v2; the v1 content was the bare pid),
//! - the holder runs a HEARTBEAT thread (default 30 s, injectable — the
//!   tests drive 50 ms beats) that re-publishes the lockfile ATOMICALLY
//!   (temp + rename, never in place),
//! - the steal rule is PORTABLE (zero OS syscalls): a v2 lock with a
//!   heartbeat older than 75 s is dead; a legacy (v1) lock with an mtime
//!   age over 15 s is dead; unparsable content is dead outright. The
//!   Linux `/proc` read survives only as a BONUS fast path that can
//!   accelerate a steal, never as the oracle it depends on.
//!
//! Every test uses the injected 50 ms heartbeat; the only wall-clock
//! waits are the sub-second windows documented per test.

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};
    use vidgedb::tools::{AgentApi, Role};

    /// Injected heartbeat period (prod default: 30 s = WRITER_HEARTBEAT).
    const HB: Duration = Duration::from_millis(50);

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir();
        let sub = d.join(format!("vidgedb_p90_{}_{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&sub);
        std::fs::create_dir_all(&sub).unwrap();
        sub
    }

    fn wlock(db: &std::path::Path) -> String {
        format!("{}-wlock", db.display())
    }

    /// A fresh writer on a fresh twin (single entity, engine warmed).
    fn fresh_writer(db: &std::path::Path) -> AgentApi {
        std::fs::remove_file(db).ok();
        std::fs::remove_file(format!("{}-wal", db.display())).ok();
        std::fs::remove_file(wlock(db)).ok();
        let mut w =
            AgentApi::open_with_role_hb(db, "w90", Role::Ingest, HB).expect("fresh writer opens");
        w.upsert_entity("M", "Motor", &[], &[], "plc").unwrap();
        w
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn open_writer(db: &std::path::Path) -> Result<AgentApi, vidgedb::tools::AgentApiError> {
        AgentApi::open_with_role_hb(db, "w90", Role::Ingest, HB)
    }

    /// Backdate `path`'s mtime by `secs` (hand-fakes a leaked old
    /// legacy lockfile). Portable std::fs::FileTimes.
    fn backdate_mtime(path: &str, secs: u64) {
        let old = std::time::UNIX_EPOCH + Duration::from_secs((now() - secs as i64) as u64);
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();
    }

    // (a) -------------------------------------------------------------------
    /// HOLD: a live holder's lockfile heartbeat is MUTATED — at least two
    /// mtime changes inside 150 ms at the injected 50 ms period (every
    /// beat rewrites the file atomically, so the mtime is the beat).
    #[test]
    fn hold_lockfile_heartbeat_mtime_moves() {
        let dir = tmp_dir("hold");
        let db = dir.join("hold.vdg");
        let _w = fresh_writer(&db);
        let lock = wlock(&db);
        assert!(
            std::path::Path::new(&lock).exists(),
            "the O_EXCL lockfile must exist while the holder lives"
        );
        // Three mtime samples, 50 ms apart: with a 50 ms heartbeat the
        // consecutive samples must differ at least twice.
        let mut mtimes: Vec<u128> = Vec::new();
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(50));
            let m = std::fs::metadata(&lock)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis())
                .expect("lockfile readable while held");
            mtimes.push(m);
        }
        let moves = (mtimes[0] != mtimes[1]) as u32 + (mtimes[1] != mtimes[2]) as u32;
        assert!(
            moves >= 2,
            "heartbeat must mutate the lockfile >=2x over 150ms (mtime samples {mtimes:?})"
        );
        drop(_w);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // (b) -------------------------------------------------------------------
    /// STEAL: a v2 lockfile whose heartbeat is 120 s old (injected by
    /// hand, with an ALIVE pid — the steal must key on the HEARTBEAT AGE
    /// alone, the portable rule, not on any pid probe) is dead: a second
    /// writer steals it without error, publishes its own fresh v2 record,
    /// and the steal lands in the thief's AUDIT trail (spec §56: a steal
    /// is never silent).
    #[test]
    fn steal_old_heartbeat_by_age_not_by_pid_probe() {
        let dir = tmp_dir("steal");
        let db = dir.join("steal.vdg");
        std::fs::remove_file(&db).ok();
        std::fs::remove_file(format!("{}-wal", db.display())).ok();
        // No live holder at all: hand-write the stale v2 record, pid =
        // THIS process (verifiably alive via /proc — must NOT matter).
        let lock = wlock(&db);
        let stale = serde_json::json!({
            "pid": std::process::id(),
            "acquired_at": now() - 200,
            "heartbeat_at": now() - 120,
        });
        std::fs::write(&lock, serde_json::to_string(&stale).unwrap()).unwrap();

        let mut rescue = open_writer(&db).expect("a 120s-old heartbeat must be stolen");
        // Steal audited (the AuditEntry is the contract-visible record).
        let audit = rescue.audit_log();
        assert!(
            audit
                .iter()
                .any(|e| e.params_summary.contains("STALE WRITER LOCK STOLEN")),
            "a steal must leave an audit trail entry, got {audit:?}"
        );
        rescue
            .ingest_points("M", "vib", &[(1_750_000_000, 9.0)])
            .expect("the stolen database keeps serving writers");
        // The v2 record now belongs to the rescuer, heartbeat fresh.
        let cur: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&lock).unwrap()).unwrap();
        assert!(
            (now() - cur["heartbeat_at"].as_i64().unwrap()).abs() < 5,
            "the thief must publish a fresh heartbeat, got {cur}"
        );
        drop(rescue);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // (c) -------------------------------------------------------------------
    /// LEGACY: a v1 pid-only lockfile (the Phase-89 shape) whose mtime is
    /// 20 s old (> the 15 s legacy threshold) is stolen on the spot, and
    /// the steal publishes a v2 JSON record.
    #[test]
    fn legacy_v1_lockfile_mtime_rule_steals() {
        let dir = tmp_dir("legacy");
        let db = dir.join("legacy.vdg");
        std::fs::remove_file(&db).ok();
        std::fs::remove_file(format!("{}-wal", db.display())).ok();
        let mut w =
            AgentApi::open_with_role_hb(&db, "seed", Role::Ingest, HB).expect("seed writer opens");
        w.upsert_entity("M", "Motor", &[], &[], "plc").unwrap();
        drop(w);
        // Recreate a v1 lockfile: pid only, mtime backdated 20 s.
        let lock = wlock(&db);
        std::fs::write(&lock, b"4194303\n").unwrap();
        backdate_mtime(&lock, 20);

        let mut w2 = open_writer(&db).expect("a 20s-old legacy lock must be stolen");
        w2.ingest_points("M", "vib", &[(1_750_000_000, 1.0)])
            .unwrap();
        let body = std::fs::read_to_string(&lock).unwrap();
        assert!(
            body.contains("heartbeat_at"),
            "the steal must publish a v2 record, got {body}"
        );
        drop(w2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // (d) -------------------------------------------------------------------
    /// CONCURRENT: two writers at once — the second is REFUSED with the
    /// SAME single-writer message as Phase 89 (refusal behavior unchanged
    /// by the heartbeat redesign).
    #[test]
    fn concurrent_second_writer_refused_with_same_error() {
        let dir = tmp_dir("conc");
        let db = dir.join("conc.vdg");
        let _w = fresh_writer(&db);
        let err2 = open_writer(&db).err().expect("second writer refused");
        assert!(
            err2.error.contains("write-locked by process"),
            "the phase-89 refusal must stay identical, got: {}",
            err2.error
        );
        drop(_w);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // (e) -------------------------------------------------------------------
    /// DROP: the heartbeat thread STOPS — Drop joins it BEFORE the unlink
    /// (no resurrection): the lockfile stays gone over a 200 ms window.
    #[test]
    fn drop_holder_stops_heartbeat_thread() {
        let dir = tmp_dir("dropst");
        let db = dir.join("dropst.vdg");
        let w = fresh_writer(&db);
        drop(w);
        assert!(
            !std::path::Path::new(&wlock(&db)).exists(),
            "drop must unlink the lockfile"
        );
        // A surviving heartbeat thread would re-publish (temp+rename) the
        // lockfile within one period; over 200 ms (4 periods) none may.
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !std::path::Path::new(&wlock(&db)).exists(),
            "post-drop the heartbeat thread must not resurrect the lockfile"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // (f) -------------------------------------------------------------------
    /// CLOCK SKEW: a heartbeat 80 s in the FUTURE is LIVE — the age
    /// `now - heartbeat_at` is negative and clamps to 0; the lock is NOT
    /// stolen (an age-based rule never steals a fresh-looking lock) and
    /// the holder keeps serving.
    #[test]
    fn clock_skew_future_heartbeat_is_never_stolen() {
        let dir = tmp_dir("skew");
        let db = dir.join("skew.vdg");
        let _w = fresh_writer(&db);
        let lock = wlock(&db);
        let body = std::fs::read_to_string(&lock).unwrap();
        let mut rec: serde_json::Value = serde_json::from_str(&body).unwrap();
        rec["heartbeat_at"] = serde_json::json!(now() + 80);
        std::fs::write(&lock, serde_json::to_string(&rec).unwrap()).unwrap();

        let err2 = open_writer(&db).err().expect("a future beat is not stolen");
        assert!(
            err2.error.contains("write-locked by process"),
            "a FUTURE heartbeat must stay a LIVE lock, got: {}",
            err2.error
        );
        drop(_w);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // (g) -------------------------------------------------------------------
    /// REGRESSION: acquisition is IMMEDIATE after a normal drop — the
    /// next writer opens without waiting out any staleness window
    /// (Phase 89 speed parity; nothing heartbeat-shaped slows the clean
    /// handover).
    #[test]
    fn acquisition_immediate_after_normal_drop() {
        let dir = tmp_dir("reacq");
        let db = dir.join("reacq.vdg");
        {
            let mut w = fresh_writer(&db);
            w.ingest_points("M", "vib", &[(1_750_000_000, 1.0)])
                .unwrap();
            // normal drop at scope end
        }
        let t0 = Instant::now();
        let mut w2 = open_writer(&db).expect("a clean drop leaves an open slot");
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "post-drop acquisition must be immediate, took {elapsed:?}"
        );
        w2.ingest_points("M", "vib", &[(1_750_000_010, 2.0)])
            .expect("the re-acquired writer serves");
        drop(w2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- (b) companion: a holder whose lock was stolen STOPS beating --------
    /// A DEAD holder never writes again: after an external steal (the
    /// lockfile content replaced by a new holder's record), the victim's
    /// in-memory heartbeat thread detects the foreign identity on its
    /// next beat and EXITS instead of overwriting the new holder's
    /// record — the thief's pid stays the lockfile's holder.
    #[test]
    fn dead_holder_does_not_rewrite_the_thiefs_record() {
        let dir = tmp_dir("deadholder");
        let db = dir.join("deadholder.vdg");
        let _victim = fresh_writer(&db);
        let lock = wlock(&db);

        // External steal, test-driven: publish OUR record over the file.
        let thief = serde_json::json!({
            "pid": 9_999_999_u64,
            "acquired_at": now(),
            "heartbeat_at": now(),
        });
        std::fs::write(&lock, serde_json::to_string(&thief).unwrap()).unwrap();

        // Give the victim's beat ONE to THREE periods: if it still beat,
        // its ownership check must fail and the thread must exit — the
        // record stays the thief's.
        std::thread::sleep(HB * 3);
        let cur: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&lock).unwrap()).unwrap();
        assert_eq!(
            cur["pid"], thief["pid"],
            "the victim must NOT rewrite the new holder's record (dead holder: no more writes)"
        );
        drop(_victim);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

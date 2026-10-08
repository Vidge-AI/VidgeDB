//! Phase 89 — TS crash-WEDGE probe converted (the soak-2 FAIL diagnostic).
//!
//! The soak-2 harness reported 2517 divergences: a `PageOutOfBounds`
//! oracle class on reopen after a mid-batch kill, plus a constant
//! `-1336491` data wipe on one series. This file pins the CONTRACT with
//! deterministic probes instead of guessing a window:
//!
//! P1 (kill grid on the real soak shape): 15-series cyclic grid, a warm
//!    session driven to 300 full batches (the twin at soak-like depth),
//!    then the crash-cycle kill — 17 back-to-back `ingest_points` RPCs
//!    sent unwaited, SIGKILL at fine offsets INSIDE their processing —
//!    and the reopen audit through the FULL AgentApi stack. R1..R5:
//!    open replays clean; the count oracle == the streamed points; the
//!    kill batch survives as a clean whole (all 17 groups committed) or
//!    not at all (rolled back — never torn/duplicated); a post-reopen
//!    append commits and is visible.
//! P2 (single-writer): a second concurrent Writer on the same .vdg is
//!    REFUSED fail-closed (the soak's real wedge: two engines interleaving
//!    commit streams on one file rewound each other's sb/layout — the
//!    permanent oracle death); a concurrent reader still opens; a stale
//!    lock (SIGKILLed owner) is stolen on the next open.
//!
//! Evidence (probe runs against the pre-fix tree, 40 kills over offsets
//! 1..100ms x reps): every reopen healthy — the mid-batch flush_buffer
//! commit protocol IS atomic; the wedge the soak saw is the missing
//! single-writer guard, fixed in this round (DbWriteLock).

#[cfg(test)]
#[cfg(unix)]
mod p1_kill_grid {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;

    const T0: i64 = 1_750_000_000;
    /// Line1.vib hits per 256-point batch on the 15-row cyclic grid
    /// (k ≡ 0 mod 15 → 18 hits per batch, incl. the kill batch).
    const L1_VIB_PER_BATCH: u64 = 18;
    const GRID_ROWS: u64 = 15;

    pub(super) fn tmp_dir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("vidgedb_p89tw_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    pub(super) fn clean_db(db: &std::path::Path) {
        let _ = std::fs::remove_file(db);
        let _ = std::fs::remove_file(format!("{}-wal", db.display()));
        let _ = std::fs::remove_file(format!("{}-wlock", db.display()));
    }

    struct Svc {
        stdin: std::process::ChildStdin,
        stdout: Option<std::process::ChildStdout>,
        child: Child,
    }

    impl Svc {
        /// Write one raw RPC request (no reply drain — the kill path).
        fn rpc_nowait(&mut self, id: u64, method: &str, params: &str) {
            let req = if params.is_empty() {
                format!(r#"{{"jsonrpc":"2.0","id":{},"method":"{}"}}"#, id, method)
            } else {
                format!(
                    r#"{{"jsonrpc":"2.0","id":{},"method":"{}","params":{}}}"#,
                    id, method, params
                )
            };
            self.stdin.write_all(req.as_bytes()).unwrap();
            self.stdin.write_all(b"\n").unwrap();
            self.stdin.flush().unwrap();
        }

        /// Start the REAL service binary on `db`.
        fn spawn(db: &std::path::Path, role: &str) -> Svc {
            let mut child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
                .args([
                    "--service",
                    db.to_str().unwrap(),
                    "--agent-id",
                    "p89",
                    "--role",
                    role,
                ])
                .stdin(Stdio::piped())
                // stdout PIPED (the warm path reads replies here); stderr
                // null. The kill path never drains stdout — it only kills.
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn service");
            let stdin = child.stdin.take().expect("stdin");
            Svc {
                stdin,
                stdout: child.stdout.take(),
                child,
            }
        }

        /// Drain one reply matching `id` (blocking); used on the warm path.
        fn rpc_reply(&mut self, id: u64) -> serde_json::Value {
            let mut out = self.stdout.take().expect("stdout");
            let mut r = BufReader::new(&mut out);
            for _ in 0..64 {
                let mut line = String::new();
                let n = r.read_line(&mut line).unwrap_or(0);
                if n == 0 {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    if v.get("id").and_then(|i| i.as_u64()) == Some(id) {
                        self.stdout = Some(out);
                        return v;
                    }
                }
            }
            self.stdout = Some(out);
            serde_json::Value::Null
        }
    }

    impl Drop for Svc {
        fn drop(&mut self) {
            let _ = self.child.kill(); // SIGKILL on unix
            let _ = self.child.wait();
        }
    }

    /// The 256-point batch of cycle `idx`, grouped per (entity, signal).
    fn batch_groups(idx: i64) -> Vec<(String, String, Vec<(i64, f64)>)> {
        let rows: Vec<(u16, &str)> = (1..=5u16)
            .flat_map(|n| ["vib", "temp", "curr"].map(move |s| (n, s)))
            .collect();
        let mut g: std::collections::BTreeMap<(u16, &str), Vec<(i64, f64)>> =
            std::collections::BTreeMap::new();
        for k in 0..256i64 {
            let (n, s) = rows[(k % GRID_ROWS as i64) as usize % rows.len()];
            g.entry((n, s))
                .or_default()
                .push((T0 + idx * 2, 100.0 + (k % 1000) as f64 * 0.5));
        }
        g.into_iter()
            .map(|((n, s), pts)| (format!("Line{}.MotorM", n), s.to_string(), pts))
            .collect()
    }

    /// THE PROBE (P1): one full soak crash-cycle iteration.
    /// Fresh twin, warm session to `warm_batches`, kill mid-batch at
    /// `delay_ms` after the kill batch's wire write, reopen + audit.
    fn probe_one(db: &std::path::Path, delay_ms: u64, warm_batches: i64) -> Result<u64, String> {
        clean_db(db);
        let mut w = Svc::spawn(db, "ingest");
        // Entity bootstrap BEFORE the batches (the kill window stays
        // strictly inside the ingest_points processing).
        w.rpc_nowait(
            40,
            "upsert_entity",
            r#"{"name":"Line1.MotorM","type":"Motor","props":{},"relations":[],"source":"plc"}"#,
        );
        let _ = w.rpc_reply(40);
        // Warm: `warm_batches` full batches, replies consumed (each batch =
        // 17 RPCs; each RPC = its own tx + commit — the steady soak path).
        let mut id: u64 = 100;
        for idx in 0..warm_batches {
            for (ent, sig, pts) in batch_groups(idx) {
                let ts: Vec<String> = pts.iter().map(|(t, v)| format!("[{},{}]", t, v)).collect();
                w.rpc_nowait(
                    id,
                    "ingest_points",
                    &format!(
                        r#"{{"entity":"{}","signal":"{}","points":[{}]}}"#,
                        ent,
                        sig,
                        ts.join(",")
                    ),
                );
                let rep = w.rpc_reply(id);
                if rep.get("result").and_then(|r| r.get("error")).is_some() {
                    return Err(format!("warm ingest failed at batch {}: {}", idx, rep));
                }
                if rep.is_null() {
                    return Err(format!("warm ingest died at batch {} (no reply)", idx));
                }
                id += 1;
            }
        }
        let base_vib = warm_batches as u64 * L1_VIB_PER_BATCH;
        // THE CRASH-CYCLE KILL: the LAST batch's RPCs are sent UNWAITED and
        // the session is SIGKILLed `delay_ms` into their processing — the
        // kill can land anywhere from the first frame parse to a commit's
        // pager flush. The Line1.vib group (if its commit made it) adds
        // exactly 18 points.
        let kill_groups: Vec<(String, String, Vec<(i64, f64)>)> = batch_groups(warm_batches);
        let l1_vib_extra = kill_groups
            .iter()
            .filter(|(e, s, _)| e == "Line1.MotorM" && s == "vib")
            .map(|(_, _, pts)| pts.len() as u64)
            .sum::<u64>();
        for (ent, sig, pts) in &kill_groups {
            let ts: Vec<String> = pts.iter().map(|(t, v)| format!("[{},{}]", t, v)).collect();
            w.rpc_nowait(
                id,
                "ingest_points",
                &format!(
                    r#"{{"entity":"{}","signal":"{}","points":[{}]}}"#,
                    ent,
                    sig,
                    ts.join(",")
                ),
            );
            id += 1;
        }
        // Busy-spin the exact delay (ms resolution, no scheduler slop).
        let t_kill = Instant::now();
        let deadline = t_kill + std::time::Duration::from_millis(delay_ms);
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
        let kill_at_us = t_kill.elapsed().as_micros() as u64;
        drop(w); // Child::drop = SIGKILL + reap
        std::thread::sleep(std::time::Duration::from_millis(40)); // reap grace

        audit_reopen(db, base_vib, l1_vib_extra).map_err(|e| {
            format!(
                "wedge at kill@{}ms ({}us after wire-write): {}",
                delay_ms, kill_at_us, e
            )
        })
    }

    /// The reopen audit (R1..R5) over a fresh handle — the exact soak
    /// oracle (count_temporal + payload round-trip + append).
    fn audit_reopen(db: &std::path::Path, base_vib: u64, l1_vib_extra: u64) -> Result<u64, String> {
        // R1: the FULL stack open must succeed (residual WAL replay
        // fail-closed-clean, every store rebuilt from the slabs).
        let mut w =
            vidgedb::tools::AgentApi::open_with_role(db, "audit", vidgedb::tools::Role::Ingest)
                .map_err(|e| format!("R1 open failed: {}", e))?;
        // R2: the count oracle (the exact soak query) must READ — no
        // PageOutOfBounds, no wedged slab.
        let total = count_oracle(&mut w, T0 + 5_000)?.ok_or("R2 count oracle unreadable")?;
        // R3/R4: total == streamed truth. The kill batch is a CLEAN WHOLE
        // (its 17 groups each committed or none did — one RPC = one tx):
        // total - base ∈ {0, l1_vib_extra} exactly; anything else is a
        // torn/duplicated commit (the wedge signature).
        let delta = total as i64 - base_vib as i64;
        if delta != 0 && delta != l1_vib_extra as i64 {
            return Err(format!(
                "R4 torn commit state: count={} (base={}, extra={})",
                total, base_vib, l1_vib_extra
            ));
        }
        if total != stream_count(&mut w, "Line1.MotorM", "vib", T0 + 5_000)? {
            return Err(format!(
                "R3 counter/index divergence: count={} != stream={}",
                total,
                stream_count(&mut w, "Line1.MotorM", "vib", T0 + 5_000)?
            ));
        }
        // R5: a post-reopen ingest commits cleanly and is visible.
        w.ingest_points(
            "Line1.MotorM",
            "vib",
            &[(T0 + 10_000, 42.0), (T0 + 10_002, 42.5)],
        )
        .map_err(|e| format!("R5 append refused: {}", e))?;
        let total2 = count_oracle(&mut w, T0 + 20_000)?.expect("R5 count");
        if total2 != total + 2 {
            return Err(format!(
                "R5 count drifted after append: {} -> {}",
                total, total2
            ));
        }
        Ok(total)
    }

    fn count_oracle(w: &mut vidgedb::tools::AgentApi, now: i64) -> Result<Option<u64>, String> {
        let res = w
            .query_temporal(
                r#"MATCH (m:Motor) MEASURE m.vib DURING 0..4611686018427387904 RETURN count(m.vib) AS total"#,
                now,
            )
            .map_err(|e| format!("count rpc: {}", e))?;
        if res.get("error").is_some() {
            return Err(format!("count wedged: {}", res));
        }
        let rows = match res.get("rows").and_then(|r| r.as_array()) {
            Some(r) => r.clone(),
            None => return Ok(None),
        };
        if rows.is_empty() {
            return Ok(None); // the Motor entity MUST bind
        }
        Ok(Some(
            rows.iter()
                .filter_map(|row| {
                    row.get("count")
                        .and_then(|c| c.get("value").and_then(|v| v.as_f64()))
                        .map(|f| f as u64)
                })
                .sum::<u64>(),
        ))
    }

    fn stream_count(
        w: &mut vidgedb::tools::AgentApi,
        entity: &str,
        signal: &str,
        now: i64,
    ) -> Result<u64, String> {
        let res = w
            .query_temporal(
                &format!(
                    r#"MATCH (m:Motor) WHERE m.name = "{}" MEASURE m.{} DURING 0..4611686018427387904 RETURN count(m.{}) AS total"#,
                    entity, signal, signal
                ),
                now,
            )
            .map_err(|e| format!("stream rpc: {}", e))?;
        let rows = res
            .get("rows")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(rows
            .iter()
            .filter_map(|row| {
                row.get("count")
                    .and_then(|c| c.get("value").and_then(|v| v.as_f64()))
                    .map(|f| f as u64)
            })
            .sum::<u64>())
    }

    // -- P1 (Unix only — see the module-level cfg + Windows note at the bottom) ------------------------------------------------------------------

    /// The kill grid around the mid-batch flush: every kill must reopen
    /// healthy. 20+ kills over the 1..100 ms band (fresh twin each);
    /// the FAST variant (SOAK_P89_FAST=1) keeps 8 of them for CI smoke.
    #[test]
    fn probe89_kill_grid_around_flush_is_always_healthy() {
        let dir = tmp_dir("grid");
        let db = dir.join("wedge.vdg");
        let delays: Vec<u64> = if std::env::var("SOAK_P89_FAST").ok().as_deref() == Some("1") {
            vec![1, 2, 4, 8, 16, 40, 80, 120]
        } else {
            vec![1, 2, 3, 5, 8, 13, 21, 34, 55, 88, 100, 120]
        };
        let warm = 30; // warm depth per iteration (probe budget: ~11 s/kill)
        let mut wedges = Vec::new();
        let mut kills = 0usize;
        // Two full passes over the offset grid = 24 varied-delay kills
        // (the task contract: 20+, the window measured in ms and reps).
        for round in 0..2u32 {
            for &ms in delays.iter() {
                let total = match probe_one(&db, ms, warm) {
                    Ok(t) => t,
                    Err(e) => {
                        kills += 1;
                        wedges.push(format!("round{}: {}", round, e));
                        println!("round{} kill@{:>4}ms: WEDGE ({})", round, ms, e);
                        continue;
                    }
                };
                kills += 1;
                println!("round{} kill@{:>4}ms: ok ({} pts)", round, ms, total);
            }
        }
        assert!(
            wedges.is_empty(),
            "wedge window found: {} of {} kills wedged (first: {})",
            wedges.len(),
            kills,
            wedges.first().map(String::as_str).unwrap_or("")
        );
        // The task contract: 20+ kill probes at varied delays.
        assert!(kills >= 20, "probe must run 20+ kills, ran {}", kills);
    }

    /// Repeats AT the same offsets (the soak's flaky-killer shape): the
    /// window is not a one-off race, every repetition reopens healthy.
    #[test]
    fn probe89_kill_repeats_at_the_window_are_healthy() {
        let dir = tmp_dir("reps");
        let db = dir.join("wedge.vdg");
        for rep in 0..6 {
            for &ms in [2u64, 5, 20].iter() {
                probe_one(&db, ms, 20)
                    .map_err(|e| format!("rep{} @{}ms: {}", rep, ms, e))
                    .unwrap();
            }
        }
    }
    // Windows build note (Phase 90 CI): the P1 kill-grid probes fork the
    // service child and SIGKILL it mid-batch — a POSIX shape with no
    // std::process::Termination equivalent (cmd /c taskkill can wait through
    // the whole batch). This whole module is cfg(unix): the Phase-90 CI
    // triple-OS matrix runs it on linux/macos; Windows builds run P2
    // (process-local, portable).
}

#[cfg(test)]
mod tests {
    // P2 lives outside the cfg(unix) P1 module but reuses its helpers
    // (tmp_dir/clean_db) — cross-module use inside this same test file.
    use crate::p1_kill_grid::{clean_db, tmp_dir};

    /// T0 for the P2 fixtures (same epoch family as the P1 grid).
    const T0: i64 = 1_750_000_000;

    /// The soak-2 WEDGE: a second concurrent writer on a .vdg a live
    /// writer keeps open must be REFUSED fail-closed (before it can fork
    /// the sb/freelist/TsLayout against the owner), while a concurrent
    /// READER still opens — and the writer slot frees on clean exit.
    #[test]
    fn second_concurrent_writer_refused_reader_allowed() {
        let dir = tmp_dir("p2");
        let db = dir.join("lock.vdg");
        clean_db(&db);
        let mut w =
            vidgedb::tools::AgentApi::open_with_role(&db, "owner", vidgedb::tools::Role::Ingest)
                .expect("first writer opens");
        w.upsert_entity("M", "Motor", &[], &[], "plc").unwrap();
        // Second writer: refused, with an actionable message.
        let err =
            vidgedb::tools::AgentApi::open_with_role(&db, "intruder", vidgedb::tools::Role::Writer)
                .err()
                .expect("second writer must be refused");
        assert!(
            err.error.contains("write-locked"),
            "refusal must be the single-writer error, got: {}",
            err.error
        );
        // Concurrent reader: allowed at any time.
        let mut r = vidgedb::tools::AgentApi::open(&db, "peek").expect("reader opens");
        let sch = r.schema().unwrap();
        assert!(
            sch.get("error").is_none(),
            "reader schema must read, got {}",
            sch
        );
        // Clean owner exit: the slot frees.
        drop(w);
        let mut w2 =
            vidgedb::tools::AgentApi::open_with_role(&db, "next", vidgedb::tools::Role::Ingest)
                .expect("writer re-opens after clean exit");
        w2.upsert_entity("M2", "Motor", &[], &[], "plc").unwrap();
        std::fs::remove_file(format!("{}-wlock", db.display())).ok();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A SIGKILLed writer leaves a STALE lock: the next open steals it
    /// (a crash must never brick the file behind a dead owner) and the
    /// recovered file keeps serving writers (R5 of the crash contract at
    /// the lock layer).
    #[test]
    fn stale_lock_from_killed_writer_is_stolen() {
        let dir = tmp_dir("stale");
        let db = dir.join("stale.vdg");
        clean_db(&db);
        let mut w =
            vidgedb::tools::AgentApi::open_with_role(&db, "victim", vidgedb::tools::Role::Ingest)
                .expect("writer opens");
        w.upsert_entity("M", "Motor", &[], &[], "plc").unwrap();
        w.ingest_points("M", "vib", &[(T0, 1.0), (T0 + 2, 2.0)])
            .unwrap();
        drop(w); // NOT a clean exit: the API is leaked, so take the lock
                 // path by hand: SIGKILL-shaped stale lock simulation.
                 // (A real SIGKILL of a service child is the P1 grid's job;
                 // here the lockfile is the observable: recreate it with a
                 // DEAD pid after the Drop removed it.)
        let lock_path = dir.join("stale.vdg-wlock");
        std::fs::write(&lock_path, b"4194303\n").unwrap(); // pid: long dead
        let mut w2 =
            vidgedb::tools::AgentApi::open_with_role(&db, "rescuer", vidgedb::tools::Role::Ingest)
                .expect("stale lock must be stolen");
        w2.ingest_points("M", "vib", &[(T0 + 10_000, 3.0)]).unwrap();
        let mut r = vidgedb::tools::AgentApi::open(&db, "verify").unwrap();
        let res = r.get_measurements("M", "vib", 0, i64::MAX).unwrap();
        let pts = res
            .get("points")
            .and_then(|p| p.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(pts.len(), 3, "recovered file keeps serving, got {}", res);
    }
}

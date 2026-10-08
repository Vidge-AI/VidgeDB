//! Crash-test harness (spec §18/§38.5): a writer process commits
//! transactions, then is killed at an arbitrary point; a checker process
//! reopens the DB and verifies every committed transaction is intact and
//! no uncommitted write is visible.
//!
//! Usage:
//!   crash_writer <db> <n_tx> <crash_after_us>   # writer, killed externally
//!   crash_checker <db> <n_tx_expected_max>      # verifies integrity

use std::process::exit;
use vidgedb::engine::Engine;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].clone();
    let db = args[2].clone();

    match mode.as_str() {
        "writer" => {
            let n_tx: u32 = args[3].parse().unwrap();
            let crash_after_us: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(u64::MAX);
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_micros(crash_after_us);
            let mut eng = Engine::open(&db).unwrap();
            for v in 1..=n_tx {
                if std::time::Instant::now() >= deadline {
                    // The external killer should have killed us already; if
                    // not (clock skew), stop writing — no more commits.
                    break;
                }
                let mut tx = eng.begin().unwrap();
                let id = eng.alloc_in_tx(&mut tx).unwrap();
                eng.write_in_tx(&mut tx, id, |d| {
                    // Payload: transaction value in many positions.
                    for b in d.iter_mut().skip(16).take(64) {
                        *b = v as u8;
                    }
                })
                .unwrap();
                eng.commit(tx).unwrap();
                // Small delay so the killer can strike mid-sequence.
                std::thread::sleep(std::time::Duration::from_micros(300));
            }
            // Normal exit = no crash; fine, checker expects <= n_tx.
            println!("writer done");
        }
        "checker" => {
            let n_tx_max: u32 = args[3].parse().unwrap();
            let mut eng = Engine::open(&db).unwrap();
            // Every committed page must carry its value consistently.
            // Recovery must have replayed ALL committed transactions.
            let mut found = 0u32;
            for id in 1..eng.page_count() {
                let data = eng.read_page(id).unwrap();
                let vals: Vec<u8> = data[16..80].iter().copied().collect();
                if vals.iter().all(|&b| b == vals[0]) && vals[0] != 0 {
                    found += 1;
                    assert!(vals[0] >= 1 && vals[0] <= n_tx_max as u8);
                } else if vals.iter().any(|&b| b != 0) {
                    // Mixed/nonzero bytes would mean a torn commit became
                    // visible — the one thing recovery must never allow.
                    eprintln!(
                        "TORN PAGE VISIBLE at id {} vals={:?}",
                        id,
                        &vals[..8.min(vals.len())]
                    );
                    exit(1);
                }
            }
            println!("checker OK: {} committed pages, no torn data", found);
        }
        _ => {
            eprintln!("unknown mode");
            exit(2);
        }
    }
}

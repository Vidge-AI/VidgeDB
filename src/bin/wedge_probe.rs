//! Phase 89 — TS crash-WEDGE probe (fork-style child kill at fine offsets).
//!
//! Child ("writer"): opens the db through the REAL ingest surface
//! (AgentApi::open_with_role(Ingest)), creates one series, then ingests
//! exactly 2500 points in one call — flush_buffer runs INSIDE that tx
//! (dynamic stream/slab allocs while the caller tx is open).
//!
//! The parent side (the kill grid + reopen audit) lives as the phase89
//! tests; this binary is the single-purpose writer child.

use std::io::Write;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].clone();
    match mode.as_str() {
        "writer" => writer_main(&args[2].clone()),
        _ => {
            eprintln!("usage: wedge_probe writer <db>");
            std::process::exit(2);
        }
    }
}

const T0: i64 = 1_760_000_000;

fn writer_main(db: &str) {
    let mut api = match vidgedb::tools::AgentApi::open_with_role(
        db,
        "wedge-probe",
        vidgedb::tools::Role::Ingest,
    ) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("open failed: {}", e);
            std::process::exit(1);
        }
    };
    let _ = api.upsert_entity("MotorW", "Motor", &[], &[], "plc");
    println!("READY");
    let _ = std::io::stdout().flush();
    let points: Vec<(i64, f64)> = (0..2500)
        .map(|k| (T0 + k as i64 * 2, 100.0 + (k % 1000) as f64 * 0.5))
        .collect();
    match api.ingest_points("MotorW", "vib", &points) {
        Ok(_) => println!("INGEST_DONE"),
        Err(e) => println!("INGEST_ERR {}", e),
    }
    let _ = std::io::stdout().flush();
}

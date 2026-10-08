// D.3 — diagnose EMOT01 in the OVERHEAT scenario (the 8 steps + hypotheses
// with provenance) — exercised via the public Engine/GraphStore/TimeSeriesStore
// layer used by the (already-green) test tests/phase71_diagnose.rs.
//
// Because of the known same-session staleness (the phase71 fixture reopens
// before reading), the twin built by pack_load.py in THIS process's earlier
// phases is re-opened here fresh (the twin still holds the engine's commit).
use vidgedb::diagnose::diagnose;
use vidgedb::engine::Engine;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;

fn main() {
    let path = match std::env::args().nth(1) {
        Some(p) => p,
        None => {
            eprintln!("usage: pack_diag <twin.vdg>");
            std::process::exit(2);
        }
    };
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();

    // The overheat window [t1..t2] is read from the caller (t1 t2).
    let t1: i64 = std::env::args().nth(2).unwrap().parse().unwrap();
    let t2: i64 = std::env::args().nth(3).unwrap().parse().unwrap();
    let r = diagnose(&mut eng, &mut gs, &mut ts, "EMOT01", t1, t2).unwrap();
    let rep = serde_json::to_string_pretty(&r).unwrap();
    println!("{}", rep);
    // Exit 1 if the report misses the overheat signature.
    let temp_viol = r
        .checks
        .iter()
        .any(|c| c.signal == "temperature" && c.status == "VIOLATION");
    let cur_ok = r
        .checks
        .iter()
        .any(|c| c.signal == "current" && c.status == "OK");
    let hyp = !r.causal_paths.is_empty();
    if !(temp_viol && cur_ok && hyp) {
        eprintln!("OVERHEAT-SIGNATURE MISS");
        std::process::exit(1);
    }
    eprintln!("OVERHEAT-SIGNATURE OK");
}

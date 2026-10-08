//! SDK test-fixture generator (Phase 11) — one synthetic machine line
//! via the crate's own deterministic benchgen (v0 has no ingest surface
//! through the JSON-RPC service; graph writes are read-only there).
//! Build: cargo build --release --bin sdkfixturegen && target/release/sdkfixturegen <db.vdg>
use vidgedb::benchgen::{generate, BenchConfig};
use vidgedb::engine::Engine;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: sdkfixturegen <db.vdg>");
    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
    let cfg = BenchConfig {
        machines: 1,
        signals: 1,
        points_per_signal: 300,
        ..Default::default()
    };
    let sum = generate(&mut eng, &mut gs, &mut ts, &cfg).unwrap();
    println!("fixture: {:?}", sum);
}

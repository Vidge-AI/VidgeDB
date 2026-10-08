// Phase 10 RAM-bench: 10 machines x 20K points, measure process RSS
// before/after a retention pass (measured, not estimated).
//
// Run: cargo test --release --test dbg83_ram -- --nocapture
use vidgedb::engine::Engine;
use vidgedb::stores::GraphStore;
use vidgedb::timeseries::TimeSeriesStore;

const T0: i64 = 1_700_000_000;
const MACHINES: usize = 10;
const PTS: usize = 20_000;

fn rss_kb() -> u64 {
    // /proc/self/status VmRSS (kB).
    let txt = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in txt.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let num: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return num.parse().unwrap_or(0);
        }
    }
    0
}

#[test]
fn phase83b_ram_after_retention() {
    let path = std::env::temp_dir().join(format!("vidgedb_ram83_{}.vdg", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("vdg-wal"));

    let mut eng = Engine::open(&path).unwrap();
    let mut gs = GraphStore::open(&mut eng).unwrap();
    let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();

    // Build: 10 series x 20K pts = 200K points, one tx per series.
    for m in 0..MACHINES {
        let name = format!("Machine{:02}.load", m);
        let mut tx = eng.begin().unwrap();
        let sid = ts.create_series(&mut eng, &mut tx, &name).unwrap();
        for i in 0..PTS {
            ts.append(&mut eng, &mut tx, sid, T0 + (i as i64) * 5, (i % 11) as f64)
                .unwrap();
        }
        ts.flush_series(&mut eng, &mut tx, sid).unwrap();
        eng.commit(tx).unwrap();
    }
    ts.persist(&mut eng).unwrap();
    gs.persist(&mut eng).unwrap();

    // Force the RSS accounting to be meaningful: drop buffers' noise by
    // recording BEFORE (after build) and AFTER (post-retain).
    let rss_before = rss_kb();
    let chunks_before: usize = (0..MACHINES as u32)
        .map(|sid| ts.committed_chunks(sid))
        .sum();
    let cutoff = T0 + (PTS as i64 / 2) * 5; // mid-chronology

    let (pts_removed, chunks_removed) = ts.retain(&mut eng, cutoff).unwrap();
    ts.persist(&mut eng).unwrap();
    let rss_after = rss_kb();

    println!(
        "RETENTION BENCH: {} machines x {}K pts ({} points, {} chunks)",
        MACHINES,
        PTS / 1000,
        PTS * MACHINES,
        chunks_before
    );
    println!(
        "  removed: {} points / {} chunks (cutoff={})",
        pts_removed, chunks_removed, cutoff
    );
    println!("  RSS before: {} kB / after: {} kB", rss_before, rss_after);

    // Sanity: some chunks retired (cutoff at mid-chronology pasts the
    // first chunks' ends), and retention must not balloon RSS.
    assert!(chunks_removed > 0, "at least some chunks retired");
    assert!(
        rss_after <= rss_before + 4096,
        "retention must not balloon RSS (before={}, after={})",
        rss_before,
        rss_after
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("vdg-wal"));
}

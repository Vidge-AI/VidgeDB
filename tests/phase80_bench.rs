//! Phase 8 integration tests — benchmark suite & synthetic twin generator
//! (spec §37/§38/§39/§42).
//!
//! Deliberately LIGHT (reduced config: 5 machines, 2 000 points): the slow
//! performance measurement lives in the `bench` binary (src/bin/bench.rs),
//! NOT in `cargo test`. What is verified here:
//! 1. the generator is deterministic — two runs into two fresh DBs produce
//!    byte-identical `.vdg` files (same size, same content hash);
//! 2. generated counts match the config (entities/relations/series/points);
//! 3. a time-series query over the generated telemetry returns exactly the
//!    configured number of points, values matching the deterministic model.
//!
//! Every fixture REOPENS before asserting (same-session reads after commit
//! are stale — known Phase 2 engine bug class, see DEVELOPING.md).

#[cfg(test)]
mod tests {
    use vidgedb::benchgen::{self, BenchConfig, T0};
    use vidgedb::engine::Engine;
    use vidgedb::stores::GraphStore;
    use vidgedb::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("vidgedb_p80_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    /// Reduced config for fast tests (spec §42 generator, small scale).
    fn small_cfg() -> BenchConfig {
        BenchConfig {
            machines: 5,
            signals: 2,
            points_per_signal: 2000,
            seed: benchgen::DEFAULT_SEED,
        }
    }

    /// Generate into a fresh DB at `path`; returns the summary.
    fn generate_at(path: &str, cfg: &BenchConfig) -> vidgedb::benchgen::Summary {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let sum = benchgen::generate(&mut eng, &mut gs, &mut ts, cfg).unwrap();
        drop(ts);
        drop(gs);
        drop(eng);
        sum
    }

    /// 1. Determinism: same config -> same bytes (spec §42).
    ///
    /// Two runs into two fresh files must yield files of identical size AND
    /// identical content (the size check alone could hide payload drift).
    #[test]
    fn generator_is_deterministic_same_config_same_bytes() {
        let a = tmp_path("det_a");
        let b = tmp_path("det_b");
        cleanup(&a);
        cleanup(&b);
        let cfg = small_cfg();
        let sa = generate_at(&a, &cfg);
        let sb = generate_at(&b, &cfg);
        assert_eq!(sa, sb, "summaries must match for identical configs");

        let bytes_a = std::fs::read(&a).unwrap();
        let bytes_b = std::fs::read(&b).unwrap();
        assert_eq!(
            bytes_a.len(),
            bytes_b.len(),
            "deterministic runs must produce same-size files"
        );
        // Byte-level equality: the strongest determinism guarantee.
        assert_eq!(
            bytes_a, bytes_b,
            "same config must produce byte-identical .vdg files"
        );
        // And no WAL residue: the generator ends with a checkpoint.
        assert_eq!(
            std::fs::metadata(format!("{}-wal", a))
                .map(|m| m.len())
                .unwrap_or(0),
            0,
            "WAL must be empty after generation (checkpoint)"
        );

        // A different seed MUST produce different bytes (variation exists).
        let c = tmp_path("det_c");
        cleanup(&c);
        let mut cfg2 = small_cfg();
        cfg2.seed = cfg.seed ^ 0xFEED;
        let _ = generate_at(&c, &cfg2);
        let bytes_c = std::fs::read(&c).unwrap();
        assert_ne!(bytes_a, bytes_c, "a different seed must vary the bytes");

        cleanup(&a);
        cleanup(&b);
        cleanup(&c);
    }

    /// 2. Counts match the config: 4 entities + 3 relations + `signals`
    /// series + `points_per_signal` points per machine.
    #[test]
    fn generated_counts_match_config() {
        let path = tmp_path("counts");
        cleanup(&path);
        let cfg = small_cfg();
        let sum = generate_at(&path, &cfg);

        // Summary-level (spec §42 counters).
        assert_eq!(sum.machines, 5);
        assert_eq!(sum.entities, 5 * 4);
        assert_eq!(sum.relations, 5 * 3);
        assert_eq!(sum.series, 5 * cfg.signals as u64);
        assert_eq!(
            sum.points,
            5 * cfg.signals as u64 * cfg.points_per_signal as u64
        );

        // Reopen and verify the committed state (never trust the same session).
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        assert_eq!(gs.entity_count() as u64, 5 * 4);
        assert_eq!(gs.relations.len() as u64, 5 * 3);
        assert_eq!(ts.series.len() as u64, 5 * cfg.signals as u64);
        assert_eq!(
            ts.series.iter().map(|s| s.total_points).sum::<u64>(),
            5 * cfg.signals as u64 * cfg.points_per_signal as u64
        );

        // Topology per machine: PLC -network:profinet-> Drive
        //                      -electrical:feeds-> Motor -mechanical:drives-> Pump.
        // The three topology classes must all be present.
        let mut topos = std::collections::BTreeSet::new();
        for r in &gs.relations {
            let t = gs
                .get_str(&mut eng, vidgedb::stores::StrId(r.topo_sid))
                .unwrap();
            topos.insert(t);
        }
        let want: std::collections::BTreeSet<String> = ["network", "electrical", "mechanical"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(topos, want, "the three topology classes must be wired");

        // Motor spec props (spec §25 keys) present on every machine's motor.
        for m in 0..cfg.machines {
            let name = format!("Motor_{:06}", m + 1);
            let sid = gs.str_lookup(&name).expect("motor name interned");
            let mut found = false;
            for key in 0..gs.entity_count() {
                if gs.entity_name_sid(&mut eng, key).unwrap() == sid {
                    let props = gs.entity_props(&mut eng, key).unwrap();
                    let max = props
                        .iter()
                        .find(|(k, _)| k == "spec.current.max")
                        .expect("spec.current.max on every motor");
                    // Varies deterministically: 10.00 + 0.25*(m mod 4).
                    let want_max = format!("{:.2}", 10.0 + (m % 4) as f64 * 0.25);
                    assert_eq!(max.1, want_max);
                    let unit = props
                        .iter()
                        .find(|(k, _)| k == "spec.current.unit")
                        .expect("spec.current.unit on every motor");
                    assert_eq!(unit.1, "A");
                    found = true;
                }
            }
            assert!(found, "motor {} must exist", name);
        }
        cleanup(&path);
    }

    /// 3. A ts range query over generated data returns exactly the expected
    /// number of points with the deterministic values (ramp + LCG noise).
    #[test]
    fn ts_query_on_generated_data_returns_configured_points() {
        let path = tmp_path("tsq");
        cleanup(&path);
        let cfg = small_cfg();
        let _sum = generate_at(&path, &cfg);

        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();

        // Full range: every generated point of machine 1's current series.
        let series_name = "Motor_000001.current";
        let sid = ts
            .series
            .iter()
            .position(|s| s.name == series_name)
            .expect("generated series present") as u32;
        let pts = ts.query(&mut eng, sid, 0, i64::MAX / 2).unwrap();
        assert_eq!(
            pts.len() as u64,
            cfg.points_per_signal as u64,
            "full-range query must return every generated point"
        );
        // Timestamp grid: T0 + i*60.
        for (i, (t, _)) in pts.iter().enumerate() {
            assert_eq!(*t, T0 + (i as i64) * 60, "60 s sampling grid");
        }
        // Values: pure function of (seed, kind, i) — recompute independently.
        let seed = benchgen::series_seed(&cfg, 0, 0);
        for (i, (_, v)) in pts.iter().enumerate() {
            let want = benchgen::point_value(seed, 0, i, cfg.points_per_signal);
            assert_eq!(*v, want, "point {} must match the deterministic model", i);
        }

        // Windowed query: exactly half the series (inclusive bounds).
        let half = (cfg.points_per_signal / 2) as i64;
        let from = T0;
        let to = T0 + half * 60;
        let win = ts.query(&mut eng, sid, from, to).unwrap();
        assert_eq!(win.len() as i64, half + 1, "inclusive window bounds");
        // First and last of the window sit on the grid.
        assert_eq!(win.first().unwrap().0, from);
        assert_eq!(win.last().unwrap().0, to);

        // Aggregation over the whole series matches the recomputed model.
        let agg = ts.aggregate(&mut eng, sid, 0, i64::MAX / 2).unwrap();
        assert_eq!(agg.count as u64, cfg.points_per_signal as u64);
        let want_sum: f64 = (0..cfg.points_per_signal)
            .map(|i| benchgen::point_value(seed, 0, i, cfg.points_per_signal))
            .sum();
        assert!(
            (agg.sum - want_sum).abs() < 1e-6 * want_sum.abs().max(1.0),
            "aggregate sum must equal the recomputed sum"
        );
        cleanup(&path);
    }
}

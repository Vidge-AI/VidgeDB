//! Synthetic twin generator — Phase 8 (spec §42) + benchmark fixture.
//!
//! Procedural generator producing artificial production lines without
//! exposing industrial secrets (spec §42). One run = `machines` production
//! lines, each a PLC -> Drive -> Motor -> Pump chain wired through the three
//! topology classes:
//!
//! ```text
//! PLC_m -network:profinet-> Drive_m -electrical:feeds-> Motor_m -mechanical:drives-> Pump_m
//! ```
//!
//! Each machine's MOTOR carries the spec properties used by the CHECK engine
//! (spec §25): `spec.current.max` (varying deterministically per machine) and
//! `spec.current.unit = "A"`. Each machine gets `signals` telemetry series on
//! its motor (`Motor_XXXXXX.<signal>`), sampled on a regular 60 s grid from
//! [`T0`].
//!
//! # Determinism (spec §42 reproducibility)
//!
//! ZÉRO hasard non reproductible: every "random" value comes from a seeded
//! LCG ([`Lcg`]) and every value of every series is a pure function of
//! `(seed, signal kind, index, count)` — [`point_value`]. There is no
//! external RNG, no clock, no HashMap-order dependence on disk: the same
//! [`BenchConfig`] produces byte-identical `.vdg` files (verified in
//! tests/phase80_bench.rs).
//!
//! Variation (spec §42 SHOULD-vary list), all deterministic:
//! - machine type is fixed (v0 pins the PLC->Drive->Motor->Pump topology the
//!   benchmark suite needs), but component counts scale with `machines`;
//! - protocols: `network:profinet` (fixed per benchmark contract);
//! - sensor density: `signals` series per machine (1..=3 named signals,
//!   extra ones get `sig<N>` names);
//! - specifications: `spec.current.max` varies as 10.00 + 0.25·(m mod 4);
//! - historical behavior: the ramp + noise shape varies per signal kind
//!   (current ramp, vibration sine, temperature ramp) and per-machine seed.

use crate::engine::{Engine, EngineError};
use crate::stores::GraphStore;
use crate::timeseries::TimeSeriesStore;

// ---------------------------------------------------------------------------
// Config / summary
// ---------------------------------------------------------------------------

/// Generator configuration. Same config => same bytes (spec §42).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchConfig {
    /// Number of production lines (each PLC->Drive->Motor->Pump).
    pub machines: usize,
    /// Telemetry signals per machine (sensor density).
    pub signals: usize,
    /// Points per signal series.
    pub points_per_signal: usize,
    /// LCG seed base. Fixed default: reproducible by construction.
    pub seed: u64,
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig {
            machines: 50,
            signals: 1,
            points_per_signal: 2000,
            seed: DEFAULT_SEED,
        }
    }
}

/// Fixed LCG seed (any run with the default config is byte-reproducible).
pub const DEFAULT_SEED: u64 = 0x56_49_44_47_45_44_42_38; // "VIDGEDB8"

/// First timestamp of every generated series (unix seconds, 60 s grid).
pub const T0: i64 = 1_700_000_000;

/// Counters of one generation run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub machines: u64,
    pub entities: u64,
    pub relations: u64,
    pub series: u64,
    pub points: u64,
}

/// Signal names for the first three signals; extras get `sig<N>`.
pub const SIGNAL_NAMES: [&str; 3] = ["current", "vibration", "temp"];

/// Name of signal `idx` (deterministic).
pub fn signal_name(idx: usize) -> String {
    if idx < SIGNAL_NAMES.len() {
        SIGNAL_NAMES[idx].to_string()
    } else {
        format!("sig{}", idx)
    }
}

// ---------------------------------------------------------------------------
// Deterministic LCG (spec §42: zero non-reproducible randomness)
// ---------------------------------------------------------------------------

/// 64-bit linear congruential generator (Knuth MMIX constants).
///
/// Pure arithmetic on `u64` — no OS entropy, no clock, no thread state.
pub struct Lcg {
    s: u64,
}

const LCG_A: u64 = 6364136223846793005;
const LCG_C: u64 = 1442695040888963407;

impl Lcg {
    /// New generator; the seed is mixed with 4 warmup draws so small or
    /// related seeds (0, 1, 2, ...) never produce degenerate low bits.
    pub fn new(seed: u64) -> Lcg {
        let mut l = Lcg { s: seed };
        for _ in 0..4 {
            l.next();
        }
        l
    }

    /// One draw (advances the state deterministically).
    pub fn next(&mut self) -> u64 {
        self.s = self.s.wrapping_mul(LCG_A).wrapping_add(LCG_C);
        self.s
    }

    /// Uniform noise in `[-amplitude, +amplitude]` (one draw).
    pub fn noise(&mut self, amplitude: f64) -> f64 {
        let u = ((self.next() >> 11) as f64) / ((1u64 << 53) as f64); // [0, 1)
        (u - 0.5) * 2.0 * amplitude
    }

    /// Jump the state forward by `n` draws in O(log n) via the affine
    /// closed form `s_k = a^k·s + c·(a^k-1)/(a-1) mod 2^64` (binary
    /// exponentiation of the (a, c) step pair).
    pub fn skip(&mut self, n: u64) {
        let (mut a, mut c) = (LCG_A, LCG_C);
        let (mut acc_a, mut acc_c) = (1u64, 0u64);
        let mut n = n;
        while n > 0 {
            if n & 1 == 1 {
                // Compose step(acc) with the current step (a, c):
                // s' = a·s + c  =>  (acc_a·a, acc_a·c + acc_c).
                acc_c = acc_c.wrapping_add(acc_a.wrapping_mul(c));
                acc_a = acc_a.wrapping_mul(a);
            }
            // Square the step: step²(s) = a²·s + (a·c + c).
            c = a.wrapping_mul(c).wrapping_add(c);
            a = a.wrapping_mul(a);
            n >>= 1;
        }
        self.s = self.s.wrapping_mul(acc_a).wrapping_add(acc_c);
    }
}

/// Per-series seed: base seed mixed with machine and signal indices.
pub fn series_seed(cfg: &BenchConfig, machine: usize, signal_idx: usize) -> u64 {
    cfg.seed
        ^ (machine as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ ((signal_idx as u64 + 1).wrapping_mul(0xBF58_476D_1CE4_E5B9))
}

/// Value of point `i` of a series (ramp + deterministic LCG noise).
///
/// Every point consumes exactly ONE LCG draw, so this function is the single
/// source of truth for generated telemetry: the generator writes exactly
/// these bytes and any verifier can recompute any point independently.
pub fn point_value(seed: u64, kind: usize, i: usize, n: usize) -> f64 {
    let mut rng = Lcg::new(seed);
    rng.skip(i as u64);
    let frac = if n <= 1 {
        0.0
    } else {
        i as f64 / (n as f64 - 1.0)
    };
    match kind % 3 {
        0 => 4.0 + 6.0 * frac + rng.noise(0.4), // current: ramp 4..10 A
        1 => 2.0 + (i as f64 * 0.37).sin() * 1.5 + rng.noise(0.2), // vibration mm/s
        _ => 35.0 + 10.0 * frac + rng.noise(0.5), // temperature °C
    }
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// Generate `cfg.machines` production lines into `(eng, gs, ts)`.
///
/// Per machine (transactional, committed machine by machine):
/// 1. entities `PLC_m`, `Drive_m`, `Motor_m`, `Pump_m` (motor carries the
///    spec properties, spec §25/§42);
/// 2. relations `network:profinet`, `electrical:feeds`, `mechanical:drives`;
/// 3. `cfg.signals` series `Motor_m.<signal>` with `points_per_signal`
///    points each (auto-chunked at BATCH=256, flushed before commit).
///
/// Durability order per machine: commit the data tx FIRST, then persist the
/// graph layout, then the TS layout (Phase 3 contract). Ends with a
/// checkpoint so the `.vdg` file is complete and the WAL empty — required
/// for byte-level reproducibility and honest storage-size reporting.
pub fn generate(
    eng: &mut Engine,
    gs: &mut GraphStore,
    ts: &mut TimeSeriesStore,
    cfg: &BenchConfig,
) -> Result<Summary, EngineError> {
    let mut sum = Summary {
        machines: cfg.machines as u64,
        entities: 0,
        relations: 0,
        series: 0,
        points: 0,
    };
    for m in 0..cfg.machines {
        let mut tx = eng.begin()?;
        // Specs vary deterministically per machine (spec §42 "specifications").
        let spec_max = format!("{:.2}", 10.0 + (m % 4) as f64 * 0.25);
        let plc = gs.add_entity(
            eng,
            &mut tx,
            "PLC",
            &format!("PLC_{:06}", m + 1),
            &[("vendor", "Siemens")],
        )?;
        let drive = gs.add_entity(eng, &mut tx, "Drive", &format!("Drive_{:06}", m + 1), &[])?;
        let motor = gs.add_entity(
            eng,
            &mut tx,
            "Motor",
            &format!("Motor_{:06}", m + 1),
            // Phase 2 inline-props cap is 44 bytes: 23 + 20 = 43 fits.
            &[
                ("spec.current.max", spec_max.as_str()),
                ("spec.current.unit", "A"),
            ],
        )?;
        let pump = gs.add_entity(eng, &mut tx, "Pump", &format!("Pump_{:06}", m + 1), &[])?;
        gs.add_relation(eng, &mut tx, plc, drive, "network:profinet", 0, -1, 0)?;
        gs.add_relation(eng, &mut tx, drive, motor, "electrical:feeds", 0, -1, 0)?;
        gs.add_relation(eng, &mut tx, motor, pump, "mechanical:drives", 0, -1, 1)?;

        for j in 0..cfg.signals {
            let sname = format!("Motor_{:06}.{}", m + 1, signal_name(j));
            let sid = ts.create_series(eng, &mut tx, &sname)?;
            let seed = series_seed(cfg, m, j);
            for i in 0..cfg.points_per_signal {
                let t = T0 + (i as i64) * 60;
                let v = point_value(seed, j, i, cfg.points_per_signal);
                ts.append(eng, &mut tx, sid, t, v)?;
            }
            ts.flush_series(eng, &mut tx, sid)?;
            sum.series += 1;
            sum.points += cfg.points_per_signal as u64;
        }

        // Data tx first, then the layout persists (Phase 3 durability order).
        eng.commit(tx)?;
        gs.persist(eng)?;
        ts.persist(eng)?;
        sum.entities += 4;
        sum.relations += 3;
    }
    // Clean WAL + flushed pager: the .vdg file is now a pure function of cfg.
    eng.checkpoint()?;
    Ok(sum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::stores::GraphStore;
    use crate::timeseries::TimeSeriesStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "vidgedb_benchgen_{}_{}.vdg",
            name,
            std::process::id()
        ));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn lcg_is_deterministic_and_skips_exactly() {
        let mut a = Lcg::new(42);
        let mut b = Lcg::new(42);
        for _ in 0..10 {
            assert_eq!(a.next(), b.next());
        }
        // skip(k) == k manual draws.
        let mut c = Lcg::new(7);
        let mut d = Lcg::new(7);
        d.skip(100);
        for _ in 0..5 {
            c.next();
        }
        let mut e = Lcg::new(7);
        for _ in 0..100 {
            e.next();
        }
        assert_eq!(d.next(), e.next());
        let _ = c;
        // Distinct seeds give distinct streams.
        let mut x = Lcg::new(1);
        let mut y = Lcg::new(2);
        assert_ne!(x.next(), y.next());
    }

    #[test]
    fn point_value_is_a_pure_function_of_index() {
        let seed = 1234u64;
        // Any index recomputes to the same value (no sequential dependency).
        let a = point_value(seed, 0, 500, 1000);
        let b = point_value(seed, 0, 500, 1000);
        assert_eq!(a, b);
        // And the O(1)-jump stream matches naive sequential generation:
        // point i consumes exactly draw #(i+1), so one plain LCG walked
        // point by point must reproduce every value.
        let mut rng = Lcg::new(seed);
        for i in 0..1000usize {
            let frac = i as f64 / 999.0;
            let expect = 4.0 + 6.0 * frac + rng.noise(0.4);
            assert_eq!(point_value(seed, 0, i, 1000), expect);
        }
    }

    #[test]
    fn generate_counts_and_reopen_roundtrip() {
        let path = tmp_path("counts");
        cleanup(&path);
        let cfg = BenchConfig {
            machines: 3,
            signals: 2,
            points_per_signal: 300,
            seed: 99,
        };
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        let sum = generate(&mut eng, &mut gs, &mut ts, &cfg).unwrap();
        assert_eq!(
            sum,
            Summary {
                machines: 3,
                entities: 12,
                relations: 9,
                series: 6,
                points: 1800,
            }
        );
        // Reopen: committed + persisted state must be fully visible.
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        assert_eq!(gs.entity_count(), 12);
        assert_eq!(gs.relations.len(), 9);
        assert_eq!(ts.series.len(), 6);
        assert_eq!(ts.series.iter().map(|s| s.total_points).sum::<u64>(), 1800);
        // Motor of machine 2 carries the varying spec.
        let m2 = gs.str_lookup("Motor_000002").unwrap();
        let mut found_spec = false;
        for key in 0..gs.entity_count() {
            if gs.entity_name_sid(&mut eng, key).unwrap() == m2 {
                let props = gs.entity_props(&mut eng, key).unwrap();
                let max = props.iter().find(|(k, _)| k == "spec.current.max");
                found_spec = max.map(|(_, v)| v.as_str()) == Some("10.25"); // 10.0 + 0.25*(1%4)
            }
        }
        assert!(found_spec);
        cleanup(&path);
    }
}

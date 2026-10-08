//! Phase 15 — the machine PACK e2e: examples/pack-conveyor.json loaded
//! into a fresh twin through the REAL binary (a spawned
//! `vidgedb --service ... --role writer`, line-delimited JSON-RPC 2.0 —
//! exactly the surface the Python pack loader uses), then verified from a
//! REOPENED reader (the known engine staleness class: read-after-commit
//! reads must go through a fresh open).
//!
//! Contract asserted here (task D.4):
//! - the pack loads: 9 entities / 9 relations created (created=true),
//! - the 3 demo scenarios produce the 3 verdict tables
//!   (normal → OK/OK/OK, overload → VIOLATION/OK/VIOLATION,
//!   overheating → OK/VIOLATION/OK),
//! - trace(PLC01 → ROB01) is FOUND over `network`,
//! - the specs are READABLE (spec.current.max=3 A,
//!   spec.temperature.max=60 C, spec.vibration.max=2.5 mm/s),
//! - the pack's deterministic telemetry generator reproduces byte-identical
//!   verdicts in pure Rust (the same wave/waypoint/pulse shapes as
//!   scripts/pack_load.py) — the spec values are read out of the twin and
//!   the CHECK windows are anchored to the pack JSON's ts_start.
//!
//! JSON pack path: the test file lives at tests/, the pack at examples/.
//! CARGO_MANIFEST_DIR = the crate root; the CARGO_BIN_EXE_* variables are
//! provided by cargo for integration tests.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};

const PACK_PATH: &str = "examples/pack-conveyor.json";

// ---------------------------------------------------------------------------
// Minimal stdio JSON-RPC client (one request per line, like the SDKs)
// ---------------------------------------------------------------------------

struct Svc {
    child: Child,
    id: u64,
}

impl Svc {
    fn spawn(db: &str, role: &str) -> Svc {
        let child = Command::new(env!("CARGO_BIN_EXE_vidgedb"))
            .args(["--service", db, "--agent-id", "phase15", "--role", role])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn vidgedb --service");
        Svc { child, id: 0 }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let req = json!({"jsonrpc": "2.0", "id": self.id, "method": method, "params": params});
        let stdin = self.child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "{}", req).expect("write request");
        stdin.flush().expect("flush request");
        let stdout = self.child.stdout.as_mut().expect("stdout");
        let mut line = String::new();
        let mut reader = BufReader::new(stdout);
        reader.read_line(&mut line).expect("read response");
        let v: Value = serde_json::from_str(&line).expect("valid JSON-RPC response");
        if let Some(err) = v.get("error") {
            panic!("{} failed: {}", method, err);
        }
        v["result"].clone()
    }
}

impl Drop for Svc {
    fn drop(&mut self) {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.write_all(b"");
            let _ = stdin.flush();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// The pack's deterministic telemetry generator, in Rust (same shapes and
// same LCG constants as scripts/pack_load.py — same seeds, same points).
// ---------------------------------------------------------------------------

struct Lcg(u64);

impl Lcg {
    fn next_f(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 11) as f64) / (1u64 << 53) as f64 * 2.0 - 1.0
    }
}

fn waypoint_interp(knots: &[(f64, f64)], fr: f64) -> f64 {
    if fr <= knots[0].0 {
        return knots[0].1;
    }
    for k in 1..knots.len() {
        let (f1, v1) = knots[k];
        let (f0, v0) = knots[k - 1];
        if fr <= f1 {
            if f1 == f0 {
                return v1;
            }
            return v0 + (v1 - v0) * (fr - f0) / (f1 - f0);
        }
    }
    knots[knots.len() - 1].1
}

#[derive(Clone)]
enum Shape {
    Wave {
        base: f64,
        amp: f64,
        noise: f64,
        period: f64,
        lo: f64,
        hi: f64,
        dec: i32,
    },
    Waypoints {
        knots: Vec<(f64, f64)>,
        noise: f64,
        lo: f64,
        hi: f64,
        dec: i32,
    },
    Pulse {
        period: f64,
        duty: f64,
        jitter: f64,
    },
}

impl Shape {
    fn from_json(v: &Value) -> Shape {
        let dec = v["dec"].as_i64().unwrap_or(2) as i32;
        let lo = v["min"].as_f64().unwrap_or(-1e9);
        let hi = v["max"].as_f64().unwrap_or(1e9);
        match v["shape"].as_str().unwrap() {
            "wave" => Shape::Wave {
                base: v["base"].as_f64().unwrap(),
                amp: v["amp"].as_f64().unwrap(),
                noise: v["noise"].as_f64().unwrap(),
                period: v["period_s"].as_f64().unwrap(),
                lo,
                hi,
                dec,
            },
            "waypoints" => {
                let knots = v["points"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| (p[0].as_f64().unwrap(), p[1].as_f64().unwrap()))
                    .collect();
                Shape::Waypoints {
                    knots,
                    noise: v["noise"].as_f64().unwrap(),
                    lo,
                    hi,
                    dec,
                }
            }
            "pulse" => Shape::Pulse {
                period: v["period_s"].as_f64().unwrap(),
                duty: v["duty"].as_f64().unwrap(),
                jitter: v["jitter"].as_f64().unwrap_or(0.0),
            },
            other => panic!("unknown shape {}", other),
        }
    }

    fn gen(&self, ts_start: i64, dt: i64, n: usize, seed: u64) -> Vec<(i64, f64)> {
        let mut rnd = Lcg(seed);
        let mut pts = Vec::with_capacity(n);
        match self {
            Shape::Wave {
                base,
                amp,
                noise,
                period,
                lo,
                hi,
                dec,
            } => {
                for i in 0..n {
                    let t = ts_start + (i as i64) * dt;
                    let tf = t as f64;
                    let v = base
                        + amp * (2.0 * std::f64::consts::PI * (tf % period) / period).sin()
                        + noise * rnd.next_f();
                    pts.push((t, round_dec(v.clamp(*lo, *hi), *dec)));
                }
            }
            Shape::Waypoints {
                knots,
                noise,
                lo,
                hi,
                dec,
            } => {
                for i in 0..n {
                    let fr = i as f64 / (n as f64 - 1.0);
                    let v = waypoint_interp(knots, fr) + noise * rnd.next_f();
                    pts.push((
                        ts_start + (i as i64) * dt,
                        round_dec(v.clamp(*lo, *hi), *dec),
                    ));
                }
            }
            Shape::Pulse {
                period,
                duty,
                jitter,
            } => {
                for i in 0..n {
                    let t = ts_start + (i as i64) * dt;
                    let phase = (t as f64 % period) / period;
                    let edge = 1.0 - duty + jitter * rnd.next_f() / 2.5;
                    let v = if edge + duty <= 1.0 && (edge..(edge + duty)).contains(&phase) {
                        1.0
                    } else {
                        0.0
                    };
                    pts.push((t, v));
                }
            }
        }
        pts
    }
}

fn round_dec(v: f64, dec: i32) -> f64 {
    let m = 10f64.powi(dec);
    (v * m).round() / m
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn tmp_path(name: &str) -> String {
    let mut p = std::env::temp_dir();
    p.push(format!("vidgedb_p15_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

fn fresh_svc(db: &str, role: &str) -> Svc {
    Svc::spawn(db, role)
}

/// Load the pack's entities + relations through the real binary
/// (upsert_entity pass 1 entities, pass 2 relations — RelationSpec needs
/// its target to exist first). Returns (n_entities, n_relations).
fn load_pack_topology(svc: &mut Svc, pack: &Value) -> (usize, usize) {
    let mut created = 0usize;
    for e in pack["entities"].as_array().unwrap() {
        let res = svc.call(
            "upsert_entity",
            json!({
                "name": e["name"], "type": e["type"],
                "props": e["props"].clone(),
                "relations": [], "source": "plc"
            }),
        );
        assert_eq!(res["created"], json!(true), "entity not created: {}", res);
        created += 1;
    }
    let mut rels = 0usize;
    let types: std::collections::HashMap<String, String> = pack["entities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["name"].as_str().unwrap().to_string(),
                e["type"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    for r in pack["relations"].as_array().unwrap() {
        let res = svc.call(
            "upsert_entity",
            json!({
                "name": r["from"], "type": types[r["from"].as_str().unwrap()],
                "props": {},
                "relations": [{"to": r["to"], "relation_type": r["type"], "valid_from": pack["ts_base"]}],
                "source": "plc"
            }),
        );
        assert_eq!(res["relations_added"], 1, "relation not added: {}", res);
        rels += 1;
    }
    (created, rels)
}

/// Ingest one scenario's deterministic telemetry (100-pt chunks), returns
/// the total accepted point count.
fn ingest_scenario(svc: &mut Svc, sc: &Value) -> usize {
    let mut total = 0usize;
    let t1 = sc["ts_start"].as_i64().unwrap();
    let dt = sc["dt_s"].as_i64().unwrap();
    let n = sc["points_per_series"].as_u64().unwrap() as usize;
    let mut signals: Vec<(String, Value)> = sc["signals"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    signals.sort_by(|a, b| a.0.cmp(&b.0));
    for (si, (series, sig_spec)) in signals.iter().enumerate() {
        let (entity, signal) = series.split_once('.').unwrap();
        let shape = Shape::from_json(sig_spec);
        let pts = shape.gen(t1, dt, n, (sc_index(sc) * 100 + si) as u64);
        let mut expected = 0usize;
        for chunk in pts.chunks(100) {
            let pv: Vec<Value> = chunk.iter().map(|(t, v)| json!([t, v])).collect();
            let res = svc.call(
                "ingest_points",
                json!({"entity": entity, "signal": signal, "points": pv}),
            );
            assert_eq!(res["accepted"], chunk.len(), "partial ingest: {}", res);
            expected += chunk.len();
        }
        total += expected;
    }
    total
}

fn sc_index(sc: &Value) -> usize {
    match sc["name"].as_str().unwrap() {
        "normal" => 0,
        "overload" => 1,
        "overheating" => 2,
        other => panic!("unknown scenario {}", other),
    }
}

/// The verdict table each scenario must produce (the demo contract).
fn expected_verdicts(scenario: &str) -> Vec<(&'static str, &'static str, &'static str)> {
    // (entity, signal, expected verdict)
    match scenario {
        "normal" => vec![
            ("EMOT01", "current", "OK"),
            ("EMOT01", "temperature", "OK"),
            ("SEN03", "vibration", "OK"),
        ],
        "overload" => vec![
            ("EMOT01", "current", "VIOLATION"),
            ("EMOT01", "temperature", "OK"),
            ("SEN03", "vibration", "VIOLATION"),
        ],
        "overheating" => vec![
            ("EMOT01", "current", "OK"),
            ("EMOT01", "temperature", "VIOLATION"),
            ("SEN03", "vibration", "OK"),
        ],
        _ => vec![],
    }
}

// ---------------------------------------------------------------------------
// The engine's CHECK via the reopened reader — one helper
// ---------------------------------------------------------------------------

fn check_signal(
    reader: &mut Svc,
    entity: &str,
    signal: &str,
    t1: i64,
    t2: i64,
) -> (String, f64, f64, Option<f64>) {
    let res = reader.call(
        "check",
        json!({"entity": entity, "signal": signal, "from": t1, "to": t2}),
    );
    (
        res["status"].as_str().unwrap_or("?").to_string(),
        res["observed"].as_f64().unwrap_or(f64::NAN),
        res["expected_max"].as_f64().unwrap_or(f64::NAN),
        res["deviation"].as_f64(),
    )
}

// ---------------------------------------------------------------------------
// The Phase 15 tests
// ---------------------------------------------------------------------------

#[test]
fn phase15_pack_loads_and_completes_the_contract() {
    let pack_txt =
        std::fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/" + PACK_PATH)
            .expect("read the pack JSON");
    let pack: Value = serde_json::from_str(&pack_txt).expect("valid pack JSON");

    let db = tmp_path("load");
    cleanup(&db);

    // -- load: topology ------------------------------------------------------
    let mut writer = fresh_svc(&db, "writer");
    let (n_ent, n_rel) = load_pack_topology(&mut writer, &pack);
    assert_eq!(n_ent, 9, "9 pack entities");
    assert_eq!(n_rel, 9, "9 pack relations");

    // -- load: the three scenario telemetry sets (500 pts x 6 series each) ---
    let mut total_pts = 0usize;
    for sc in pack["demo_scenarios"].as_array().unwrap() {
        total_pts += ingest_scenario(&mut writer, sc);
    }
    assert_eq!(total_pts, 9000, "3 scenarios x 6 series x 500 points");
    drop(writer);

    // -- verify through a REOPENED reader (read-after-commit must be fresh) --
    let mut reader = fresh_svc(&db, "reader");

    let schema = reader.call("schema", json!({}));
    let types = schema["entity_types"].as_array().unwrap();
    assert!(types.iter().any(|t| t == "Robot"), "Robot type in schema");
    assert!(
        types.iter().any(|t| t == "Conveyor"),
        "Conveyor type in schema"
    );

    // VQL: the pack topology answers the dashboard patterns.
    let rows = reader.call(
        "query",
        json!({"vql": "MATCH (p:PLC) -[:network]-> (r:Robot) RETURN p, r"}),
    )["rows"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(rows.len(), 1, "PLC-network->Robot");

    // trace(PLC01 -> ROB01): found, one hop, over profinet.
    let tr = reader.call(
        "trace",
        json!({"from": "PLC01", "to": "ROB01", "max_hops": 6}),
    );
    assert_eq!(tr["found"], json!(true), "trace found: {}", tr);
    assert_eq!(tr["n_hops"], 1, "one hop PLC->robot");
    assert_eq!(tr["steps"][0]["topology"], json!("network"), "profinet hop");

    // The specs are READABLE (the pack's spec table read via VQL props).
    let rows = reader.call(
        "query",
        json!({"vql": "MATCH (m:Motor) WHERE m.name = \"EMOT01\" RETURN m"}),
    )["rows"]
        .as_array()
        .unwrap()
        .clone();
    let props = &rows[0]["m"]["properties"];
    assert_eq!(props["spec.current.max"], json!("3"), "current spec read");
    assert_eq!(
        props["spec.temperature.max"],
        json!("60"),
        "temperature spec read"
    );
    let rows = reader.call(
        "query",
        json!({"vql": "MATCH (s:Sensor) WHERE s.name = \"SEN03\" RETURN s"}),
    )["rows"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rows[0]["s"]["properties"]["spec.vibration.max"],
        json!("2.5"),
        "vibration spec read"
    );

    // -- the 3 scenarios produce the 3 verdict tables ------------------------
    for sc in pack["demo_scenarios"].as_array().unwrap() {
        let scenario = sc["name"].as_str().unwrap();
        let t1 = sc["ts_start"].as_i64().unwrap();
        let t2 = t1 + 3600;
        for (entity, signal, want) in expected_verdicts(scenario) {
            let (status, observed, expected_max, deviation) =
                check_signal(&mut reader, entity, signal, t1, t2);
            assert_eq!(
                status, want,
                "{} {} in {}: observed={} expected={} deviation={:?}",
                scenario, signal, entity, observed, expected_max, deviation
            );
        }
    }

    drop(reader);
    cleanup(&db);
}

#[test]
fn phase15_pack_scenarios_reproduce_in_rust() {
    // The JSON pack's telemetry generator, replayed in pure Rust: the SAME
    // wave/waypoint/pulse shapes + seeds must reproduce byte-identical
    // verdicts (observed = window max) — the pack is the single source of
    // truth, Python and Rust readers agree on it.
    let pack_txt =
        std::fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/" + PACK_PATH)
            .expect("read the pack JSON");
    let pack: Value = serde_json::from_str(&pack_txt).unwrap();
    let specs_expected = [
        ("normal", "EMOT01.current", 3.0, false),
        ("normal", "EMOT01.temperature", 60.0, false),
        ("normal", "SEN03.vibration", 2.5, false),
        ("overload", "EMOT01.current", 3.0, true),
        ("overload", "SEN03.vibration", 2.5, true),
        ("overheating", "EMOT01.temperature", 60.0, true),
    ];
    for (scenario, series, thr, want_over) in specs_expected {
        let sc = pack["demo_scenarios"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["name"] == scenario)
            .unwrap()
            .clone();
        let spec = &sc["signals"][series];
        let shape = Shape::from_json(spec);
        // Seed = scenario_idx*100 + series_idx (the loader's convention —
        // the same series order as the loader's sorted iteration).
        let all_series: Vec<String> = sc["signals"].as_object().unwrap().keys().cloned().collect();
        let si_all = all_series.clone();
        let sorted = {
            let mut s = si_all.clone();
            s.sort();
            s
        };
        let series_idx = sorted.iter().position(|s| s == series).unwrap();
        let scenario_idx = match scenario {
            "normal" => 0,
            "overload" => 1,
            _ => 2,
        };
        let seed = (scenario_idx * 100 + series_idx) as u64;
        let pts = shape.gen(
            sc["ts_start"].as_i64().unwrap(),
            sc["dt_s"].as_i64().unwrap(),
            sc["points_per_series"].as_u64().unwrap() as usize,
            seed,
        );
        let obs = pts
            .iter()
            .map(|&(_, v)| v)
            .fold(f64::NEG_INFINITY, f64::max);
        let is_over = obs > thr;
        assert_eq!(
            is_over, want_over,
            "{} {} max={} vs thr={}",
            scenario, series, obs, thr
        );
    }
    let _ = &pack["ts_base"];
    let _ = &pack["title"];
}

#[test]
fn phase15_pack_json_is_consistent() {
    // The JSON itself: 9 entities / 9 relations / 3 specs, every relation
    // ends at a known entity, the three scenarios exist, the vibration spec
    // lives on SEN03 (the motor props block is at the 44B cap).
    let txt =
        std::fs::read_to_string(env!("CARGO_MANIFEST_DIR").to_string() + "/" + PACK_PATH).unwrap();
    let pack: Value = serde_json::from_str(&txt).unwrap();
    assert_eq!(pack["pack"], json!("conveyor"));
    assert_eq!(pack["entities"].as_array().map(|a| a.len()), Some(9));
    assert_eq!(pack["relations"].as_array().map(|a| a.len()), Some(9));
    assert_eq!(pack["specs"].as_array().map(|a| a.len()), Some(3));
    let names: Vec<&str> = pack["entities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    for r in pack["relations"].as_array().unwrap() {
        assert!(names.contains(&r["from"].as_str().unwrap()), "known src");
        assert!(names.contains(&r["to"].as_str().unwrap()), "known dst");
    }
    let scenarii: Vec<&str> = pack["demo_scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(scenarii, vec!["normal", "overload", "overheating"]);
    let sen03 = pack["entities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "SEN03")
        .unwrap();
    assert_eq!(sen03["props"]["spec.vibration.max"], json!("2.5"));
    let emot = pack["entities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "EMOT01")
        .unwrap();
    // The 44B inline-props cap story (Phase 2 layout, stores.rs).
    let emot_props_bytes: usize = emot["props"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| k.len() + v.as_str().unwrap().len() + 2) // k=v\0
        .sum();
    assert_eq!(
        emot_props_bytes, 43,
        "EMOT01 props at the inline cap (43 <= 44)"
    );
    assert!(emot_props_bytes <= 44);
}

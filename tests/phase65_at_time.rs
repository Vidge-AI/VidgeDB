//! Phase 6.5 integration tests: temporal time-travel (spec §9 + §24).
//!
//! A query at instant T traverses only relations valid at T:
//! `valid_from <= T < valid_to`, with `valid_to == -1` meaning open-ended
//! (sentinel preserved from Phase 2 — NOT an Option, documented in
//! `ttemporal` and `stores::RelationRecord`).
//!
//! Coverage:
//! - bounded validity [1000, 2000]: visible at AT 1500, invisible at 2500 and 500;
//! - open-ended (-1): visible at every instant;
//! - tombstoned: invisible even inside its validity window;
//! - GraphStore::neighbors_at / multi_hop_at (index rebuilt from memory);
//! - e2e VidgeQL `AT <unix>`: MATCH without AT sees everything, with AT
//!   only the still-valid relation binds;
//! - boundary semantics: t == valid_from inclusive, t == valid_to exclusive;
//! - parse: AT accepted at WHERE level, rejected without a number;
//! - retro-compat: the default adjacency (no AT) is unchanged.

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::executor;
    use vidgedb::stores::{Direction, GraphStore};
    use vidgedb::temporal::{execute_t, parse_tquery};
    use vidgedb::timeseries::TimeSeriesStore;
    use vidgedb::vidgeql::Parser;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_p65_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    /// Machine: Motor --mechanical--> Pump --electrical--> PLC
    /// with per-relation validity windows. Returns entity keys.
    fn build_machine(path: &str) -> (u32, u32, u32) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let motor = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[])
            .unwrap();
        let pump = gs
            .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[])
            .unwrap();
        let plc = gs
            .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[])
            .unwrap();
        // motor -> pump: valid [1000, 2000) (bounded, expires).
        gs.add_relation(
            &mut eng,
            &mut tx,
            motor,
            pump,
            "mechanical:drives",
            1000,
            2000,
            0,
        )
        .unwrap();
        // pump -> plc: open-ended (valid_to = -1 sentinel).
        gs.add_relation(&mut eng, &mut tx, pump, plc, "electrical:feeds", 0, -1, 0)
            .unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
        (motor, pump, plc)
    }

    fn reopen(path: &str) -> (Engine, GraphStore) {
        let mut eng = Engine::open(path).unwrap();
        let gs = GraphStore::open(&mut eng).unwrap();
        (eng, gs)
    }

    /// Bounded validity [1000, 2000]: visible at AT 1500, invisible at
    /// AT 2500 (after) and AT 500 (before).
    #[test]
    fn bounded_window_visibility() {
        let path = tmp_path("bounded");
        let (motor, pump, _plc) = build_machine(&path);
        let (_eng, gs) = reopen(&path);
        // Inside the window: edge exists.
        assert_eq!(
            gs.neighbors_at(motor, None, Direction::Out, 1500),
            vec![pump]
        );
        // After valid_to: gone.
        assert!(gs
            .neighbors_at(motor, None, Direction::Out, 2500)
            .is_empty());
        // Before valid_from: not yet connected.
        assert!(gs.neighbors_at(motor, None, Direction::Out, 500).is_empty());
        cleanup(&path);
    }

    /// Half-open boundaries: t == valid_from visible, t == valid_to not.
    #[test]
    fn half_open_boundaries() {
        let path = tmp_path("bounds");
        let (motor, pump, _plc) = build_machine(&path);
        let (_eng, gs) = reopen(&path);
        assert_eq!(
            gs.neighbors_at(motor, None, Direction::Out, 1000),
            vec![pump]
        );
        assert!(gs
            .neighbors_at(motor, None, Direction::Out, 2000)
            .is_empty());
        cleanup(&path);
    }

    /// Open-ended (valid_to = -1): traversable at every instant.
    #[test]
    fn open_ended_valid_at_all_times() {
        let path = tmp_path("open");
        let (_motor, pump, plc) = build_machine(&path);
        let (_eng, gs) = reopen(&path);
        for t in [0, 500, 1500, 2000, 2500, 1_700_000_000] {
            assert_eq!(
                gs.neighbors_at(pump, None, Direction::Out, t),
                vec![plc],
                "open-ended edge missing at t={t}"
            );
        }
        cleanup(&path);
    }

    /// Tombstoned relations are invisible even inside a valid window.
    #[test]
    fn tombstone_invisible_even_when_valid() {
        let path = tmp_path("tomb");
        let (motor, pump, _plc) = build_machine(&path);
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut tx = eng.begin().unwrap();
            // Rel idx 0 = motor -> pump (valid [1000,2000)); tombstone it.
            gs.remove_relation(&mut eng, &mut tx, 0).unwrap();
            gs.persist(&mut eng).unwrap();
            eng.commit(tx).unwrap();
        }
        let (_eng, gs) = reopen(&path);
        assert!(gs
            .neighbors_at(motor, None, Direction::Out, 1500)
            .is_empty());
        // The record is still physically there, alive_at filters it.
        assert_eq!(gs.relations.len(), 2);
        assert!(!gs.relations[0].is_alive());
        let _ = pump;
        cleanup(&path);
    }

    /// neighbors_at honors the topology filter like the current index.
    #[test]
    fn neighbors_at_topology_filter() {
        let path = tmp_path("topo");
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let m = gs.add_entity(&mut eng, &mut tx, "Motor", "M", &[]).unwrap();
        let p = gs.add_entity(&mut eng, &mut tx, "Pump", "P", &[]).unwrap();
        let d = gs.add_entity(&mut eng, &mut tx, "Drive", "D", &[]).unwrap();
        gs.add_relation(&mut eng, &mut tx, m, p, "mechanical:drives", 0, -1, 0)
            .unwrap();
        gs.add_relation(&mut eng, &mut tx, m, d, "electrical:feeds", 0, -1, 0)
            .unwrap();
        eng.commit(tx).unwrap();
        let mech = gs.str_lookup("mechanical").unwrap().0;
        assert_eq!(gs.neighbors_at(m, Some(mech), Direction::Out, 100), vec![p]);
        let elec = gs.str_lookup("electrical").unwrap().0;
        assert_eq!(gs.neighbors_at(m, Some(elec), Direction::Out, 100), vec![d]);
        // multi_hop_at through the open-ended chain.
        assert_eq!(gs.multi_hop_at(m, None, Direction::Out, 5, 100), vec![p, d]);
        cleanup(&path);
    }

    /// Time travel breaks a 2-hop chain: at T the middle edge is expired,
    /// so the PLC is unreachable even though its own edge is open-ended.
    #[test]
    fn chain_breaks_when_middle_edge_expired() {
        let path = tmp_path("chain");
        let (motor, _pump, plc) = build_machine(&path);
        let (_eng, gs) = reopen(&path);
        // At 1500: full path motor -> pump -> plc.
        assert_eq!(
            gs.multi_hop_at(motor, None, Direction::Out, 5, 1500),
            vec![_pump, plc]
        );
        // At 2500: motor->pump expired; only nothing reachable from motor.
        assert!(gs
            .multi_hop_at(motor, None, Direction::Out, 5, 2500)
            .is_empty());
        // But pump -> plc remains valid at 2500 (open-ended).
        assert_eq!(
            gs.multi_hop_at(_pump, None, Direction::Out, 5, 2500),
            vec![plc]
        );
        cleanup(&path);
    }

    /// e2e VidgeQL: machine with 2 relations, one expired — MATCH without
    /// AT sees everything (current semantics), with AT t only the valid one.
    #[test]
    fn vidgeql_at_filters_expired_relation() {
        let path = tmp_path("e2e");
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let m = gs
            .add_entity(&mut eng, &mut tx, "Motor", "M1", &[])
            .unwrap();
        let p_old = gs
            .add_entity(&mut eng, &mut tx, "Pump", "PumpOLD", &[])
            .unwrap();
        let p_new = gs
            .add_entity(&mut eng, &mut tx, "Pump", "PumpNEW", &[])
            .unwrap();
        // M1 -> PumpOLD: valid [0, 500) — long expired.
        gs.add_relation(&mut eng, &mut tx, m, p_old, "mechanical:drives", 0, 500, 0)
            .unwrap();
        // M1 -> PumpNEW: valid [1000, 2000) — alive at 1500.
        gs.add_relation(
            &mut eng,
            &mut tx,
            m,
            p_new,
            "mechanical:drives",
            1000,
            2000,
            0,
        )
        .unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();

        // Without AT: both pumps bind (validity ignored — Phase 6 behavior).
        let q = Parser::parse("MATCH (m:Motor) -[:mechanical]-> (p:Pump) RETURN p").unwrap();
        assert!(q.at.is_none());
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 2);

        // With AT 1500: only PumpNEW binds.
        let q =
            Parser::parse("MATCH (m:Motor) -[:mechanical]-> (p:Pump) AT 1500 RETURN p").unwrap();
        assert_eq!(q.at, Some(1500));
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 1);
        let bound = rows[0].get("p").unwrap();
        let name_sid = gs.entity_name_sid(&mut eng, bound).unwrap();
        assert_eq!(gs.get_str(&mut eng, name_sid).unwrap(), "PumpNEW");

        // With AT 450 (inside [0,500)): only PumpOLD binds.
        let q = Parser::parse("MATCH (m:Motor) -[:mechanical]-> (p:Pump) AT 450 RETURN p").unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 1);
        let name_sid = gs
            .entity_name_sid(&mut eng, rows[0].get("p").unwrap())
            .unwrap();
        assert_eq!(gs.get_str(&mut eng, name_sid).unwrap(), "PumpOLD");

        // With AT 2500: nothing binds (both expired).
        let q =
            Parser::parse("MATCH (m:Motor) -[:mechanical]-> (p:Pump) AT 2500 RETURN p").unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert!(rows.is_empty());
        cleanup(&path);
    }

    /// e2e via the temporal layer: AT clause flows through parse_tquery /
    /// execute_t to hop binding.
    #[test]
    fn tquery_at_propagates_through_temporal_layer() {
        let path = tmp_path("tq");
        let (motor, _pump, _plc) = build_machine(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut ts = TimeSeriesStore::open(&mut eng, &mut gs).unwrap();
        // Without AT: motor binds (single node pattern is validity-blind).
        let q = parse_tquery("MATCH (m:Motor) RETURN m", 0).unwrap();
        assert!(q.at.is_none());
        let rows = execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bindings[0].1, motor);

        // Single-node MATCH is unaffected by AT (no hops), but the clause
        // parses and lands in TQuery.at.
        let q = parse_tquery("MATCH (m:Motor) AT 1500 RETURN m", 0).unwrap();
        assert_eq!(q.at, Some(1500));
        let rows = execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 1);

        // With AT 2500 the mechanical edge is expired: the 2nd node cannot
        // bind even though the Pump entity itself still exists.
        let q = parse_tquery(
            "MATCH (m:Motor) -[:mechanical]-> (p:Pump) AT 2500 RETURN m, p",
            0,
        )
        .unwrap();
        assert_eq!(q.at, Some(2500));
        let rows = execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert!(rows.is_empty());

        // At 1500 the hop binds.
        let q = parse_tquery(
            "MATCH (m:Motor) -[:mechanical]-> (p:Pump) AT 1500 RETURN m, p",
            0,
        )
        .unwrap();
        let rows = execute_t(&mut eng, &mut gs, &mut ts, &q).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bindings[1].1, _pump);
        cleanup(&path);
    }

    /// Parser details: AT accepted at the WHERE level (order-insensitive
    /// with WHERE), rejected without a number.
    #[test]
    fn parse_at_clause_shapes() {
        let q = Parser::parse("MATCH (m:Motor) AT 1700000000 RETURN m").unwrap();
        assert_eq!(q.at, Some(1_700_000_000));
        // AT after WHERE.
        let q = Parser::parse("MATCH (m:Motor) WHERE m.name = \"Motor42\" AT 99 RETURN m").unwrap();
        assert_eq!(q.at, Some(99));
        assert_eq!(q.where_.len(), 1);
        // Missing number is a parse error.
        assert!(Parser::parse("MATCH (m:Motor) AT RETURN m").is_err());
        assert!(Parser::parse("MATCH (m:Motor) AT foo RETURN m").is_err());
        // parse_tquery keeps the field.
        let tq = parse_tquery("MATCH (m:Motor) AT 123 RETURN m", 0).unwrap();
        assert_eq!(tq.at, Some(123));
    }

    /// Retro-compat: without AT the executor still uses the Phase 2
    /// "now" adjacency (tombstone-filtered, validity-blind) — expired
    /// relations remain traversable exactly as before Phase 6.5.
    #[test]
    fn default_adjacency_unchanged_without_at() {
        let path = tmp_path("compat");
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let m = gs.add_entity(&mut eng, &mut tx, "Motor", "M", &[]).unwrap();
        let p = gs.add_entity(&mut eng, &mut tx, "Pump", "P", &[]).unwrap();
        // Expired since long ago — yet current semantics keep it visible.
        gs.add_relation(&mut eng, &mut tx, m, p, "mechanical:drives", 0, 500, 0)
            .unwrap();
        eng.commit(tx).unwrap();
        assert_eq!(gs.adjacency.neighbors(m, None, Direction::Out), vec![p]);
        let q = Parser::parse("MATCH (m:Motor) -[:mechanical]-> (p:Pump) RETURN p").unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 1);
        cleanup(&path);
    }
}

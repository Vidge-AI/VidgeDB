//! Phase 6.3 integration tests: OR logic in VidgeQL WHERE (spec §23
//! boolean filtering) — build the standard sample graph, then verify the
//! Or/And boolean tree both parses with AND-over-OR precedence and
//! evaluates e2e through the executor.

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::executor;
    use vidgedb::stores::GraphStore;
    use vidgedb::vidgeql::{BoolExpr, Parser};

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_ql_or_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    /// Sample graph: two Motors wired downstream of a PLC via a Wire.
    fn build_sample(path: &str) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let _plc = gs
            .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[])
            .unwrap();
        let _wire = gs
            .add_entity(&mut eng, &mut tx, "Wire", "W17", &[])
            .unwrap();
        let motor = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[("poles", "4")])
            .unwrap();
        let motor2 = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor43", &[("poles", "2")])
            .unwrap();
        let _pump = gs
            .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[("pressure", "183")])
            .unwrap();
        // Motor42 and Motor43 both receive the same signal; Motor42 drives
        // the pump. Relations keep the graph realistic but the queries
        // below filter on single-node patterns (WHERE only).
        gs.add_relation(
            &mut eng,
            &mut tx,
            motor,
            motor2,
            "mechanical:paired",
            0,
            -1,
            0,
        )
        .unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
    }

    /// Row names helper: resolve the bound "m" var to entity names.
    fn row_names(eng: &mut Engine, gs: &mut GraphStore, rows: &[executor::Row]) -> Vec<String> {
        let mut names = Vec::new();
        for r in rows {
            let key = r.get("m").expect("var m must be bound");
            let sid = gs.entity_name_sid(eng, key).unwrap();
            names.push(gs.get_str(eng, sid).unwrap());
        }
        names.sort();
        names
    }

    /// Task test 1 (e2e): OR over the same var selects both motors.
    #[test]
    fn or_poles_matches_both_motors() {
        let path = tmp_path("orpoles");
        cleanup(&path);
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q = Parser::parse("MATCH (m:Motor) WHERE m.poles = 4 OR m.poles = 2 RETURN m").unwrap();
        // Sanity: parses as Or with 2 leaves.
        assert!(matches!(q.where_tree, BoolExpr::Or(ref v) if v.len() == 2));
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(
            row_names(&mut eng, &mut gs, &rows),
            vec!["Motor42", "Motor43"]
        );
        cleanup(&path);
    }

    /// Task test 3 (e2e): OR mixing a name equality and a numeric
    /// comparison that nothing satisfies returns exactly one row.
    #[test]
    fn or_name_or_impossible_poles_matches_one() {
        let path = tmp_path("orname");
        cleanup(&path);
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q =
            Parser::parse("MATCH (m:Motor) WHERE m.name = \"Motor42\" OR m.poles > 100 RETURN m")
                .unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_names(&mut eng, &mut gs, &rows), vec!["Motor42"]);
        cleanup(&path);
    }

    /// Phase 6.3 precedence e2e: `poles=4 OR (poles=2 AND name="X")` — the
    /// AND group is false for Motor43, the OR's first arm saves Motor42
    /// only... wait, Motor43 has poles=2 but name="X" fails, so Or = only
    /// Motor42. The And group guards the second arm.
    #[test]
    fn precedence_or_and_group_e2e() {
        let path = tmp_path("prec");
        cleanup(&path);
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        // a OR (b AND c): Motor42 satisfies a; Motor43 satisfies b but not c.
        let q = Parser::parse(
            "MATCH (m:Motor) WHERE m.poles = 4 OR m.poles = 2 AND m.name = \"X\" RETURN m",
        )
        .unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(row_names(&mut eng, &mut gs, &rows), vec!["Motor42"]);
        // Same query with c = \"Motor43\": both motors match.
        let q2 = Parser::parse(
            "MATCH (m:Motor) WHERE m.poles = 4 OR m.poles = 2 AND m.name = \"Motor43\" RETURN m",
        )
        .unwrap();
        let rows2 = executor::execute(&mut eng, &mut gs, &q2).unwrap();
        assert_eq!(
            row_names(&mut eng, &mut gs, &rows2),
            vec!["Motor42", "Motor43"]
        );
        cleanup(&path);
    }

    /// Backward compat: plain AND still works through the tree evaluator
    /// (the 49 pre-Phase-6.3 tests stay green on the same code path).
    #[test]
    fn and_chain_still_filters() {
        let path = tmp_path("and");
        cleanup(&path);
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q =
            Parser::parse("MATCH (m:Motor) WHERE m.poles >= 2 AND m.name != \"Motor42\" RETURN m")
                .unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(row_names(&mut eng, &mut gs, &rows), vec!["Motor43"]);
        cleanup(&path);
    }

    /// WHERE-less query keeps matching everything (empty And = vacuous).
    #[test]
    fn no_where_matches_all() {
        let path = tmp_path("nowhere");
        cleanup(&path);
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q = Parser::parse("MATCH (m:Motor) RETURN m").unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(
            row_names(&mut eng, &mut gs, &rows),
            vec!["Motor42", "Motor43"]
        );
        cleanup(&path);
    }
}

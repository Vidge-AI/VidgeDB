//! Phase 6 integration tests: VidgeQL end-to-end — build a graph, run
//! queries (MATCH pattern / WHERE / RETURN / LIMIT) through the executor.

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::executor;
    use vidgedb::stores::GraphStore;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_ql_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    fn build_sample(path: &str) {
        let mut eng = Engine::open(path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let mut tx = eng.begin().unwrap();
        let plc = gs
            .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[("vendor", "Siemens")])
            .unwrap();
        let wire = gs
            .add_entity(&mut eng, &mut tx, "Wire", "W17", &[])
            .unwrap();
        let motor = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[("poles", "4")])
            .unwrap();
        let motor2 = gs
            .add_entity(&mut eng, &mut tx, "Motor", "Motor43", &[("poles", "2")])
            .unwrap();
        let pump = gs
            .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[("pressure", "183")])
            .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            plc,
            wire,
            "electrical:connected_via",
            0,
            -1,
            0,
        )
        .unwrap();
        gs.add_relation(&mut eng, &mut tx, wire, motor, "electrical:feeds", 0, -1, 0)
            .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            wire,
            motor2,
            "electrical:feeds",
            0,
            -1,
            0,
        )
        .unwrap();
        gs.add_relation(
            &mut eng,
            &mut tx,
            motor,
            pump,
            "mechanical:drives",
            0,
            -1,
            0,
        )
        .unwrap();
        gs.persist(&mut eng).unwrap();
        eng.commit(tx).unwrap();
    }

    /// Spec §23 traversal example: PLC -ELECTRICAL-> Wire -ELECTRICAL-> Motor.
    #[test]
    fn two_hop_traversal() {
        let path = tmp_path("trav");
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q = vidgedb::vidgeql::Parser::parse(
            "MATCH (plc:PLC) -[:ELECTRICAL]-> (w:Wire) -[:ELECTRICAL]-> (m:Motor) RETURN plc, w, m",
        )
        .unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 2); // Motor42 AND Motor43 downstream
                                   // Each row binds all three variables.
        assert!(rows
            .iter()
            .all(|r| r.get("plc").is_some() && r.get("m").is_some()));
        // The returned motors are the two we created.
        let mut motors = rows
            .iter()
            .map(|r| {
                let k = r.get("m").unwrap();
                let name_sid = gs.entity_name_sid(&mut eng, k).unwrap();
                gs.get_str(&mut eng, name_sid).unwrap()
            })
            .collect::<Vec<_>>();
        motors.sort();
        assert_eq!(motors, vec!["Motor42", "Motor43"]);
        cleanup(&path);
    }

    /// WHERE by name + LIMIT.
    #[test]
    fn where_and_limit() {
        let path = tmp_path("where");
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q =
            vidgedb::vidgeql::Parser::parse("MATCH (m:Motor) WHERE m.name = \"Motor42\" RETURN m")
                .unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("m"), Some(2)); // Motor42 is key 2

        let q2 =
            vidgedb::vidgeql::Parser::parse("MATCH (m:Motor) WHERE m.poles >= 2 RETURN m LIMIT 1")
                .unwrap();
        let rows2 = executor::execute(&mut eng, &mut gs, &q2).unwrap();
        assert_eq!(rows2.len(), 1);
        cleanup(&path);
    }

    /// Unknown topology matches nothing (no silent all-topology match).
    #[test]
    fn unknown_topology_matches_nothing() {
        let path = tmp_path("topo");
        build_sample(&path);
        let mut eng = Engine::open(&path).unwrap();
        let mut gs = GraphStore::open(&mut eng).unwrap();
        let q =
            vidgedb::vidgeql::Parser::parse("MATCH (m:Motor) -[:HYDRAULIC]-> (p:Pump) RETURN m, p")
                .unwrap();
        let rows = executor::execute(&mut eng, &mut gs, &q).unwrap();
        assert!(rows.is_empty());
        cleanup(&path);
    }
}

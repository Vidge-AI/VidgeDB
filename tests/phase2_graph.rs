//! Phase 2 integration tests: GraphStore end-to-end over the transactional
//! engine — build the spec §58 machine, persist, reopen, traverse.

#[cfg(test)]
mod tests {
    use vidgedb::engine::Engine;
    use vidgedb::stores::{Direction, GraphStore};

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir().to_path_buf();
        p.push(format!("vidgedb_p2_{}_{}.vdg", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    /// Spec §58 example machine persisted and recovered.
    #[test]
    fn build_persist_reopen_traverse() {
        let path = tmp_path("e2e");
        let (plc, motor, pump, rel_pump) = {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut tx = eng.begin().unwrap();
            // Entities
            let plc = gs
                .add_entity(&mut eng, &mut tx, "PLC", "PLC01", &[("vendor", "Siemens")])
                .unwrap();
            let drive = gs
                .add_entity(&mut eng, &mut tx, "Drive", "Drive12", &[])
                .unwrap();
            let motor = gs
                .add_entity(&mut eng, &mut tx, "Motor", "Motor42", &[("poles", "4")])
                .unwrap();
            let pump = gs
                .add_entity(&mut eng, &mut tx, "Pump", "Pump17", &[])
                .unwrap();
            // Relations (spec §58)
            gs.add_relation(&mut eng, &mut tx, plc, drive, "network:profinet", 0, -1, 0)
                .unwrap();
            gs.add_relation(
                &mut eng,
                &mut tx,
                drive,
                motor,
                "electrical:feeds",
                0,
                -1,
                0,
            )
            .unwrap();
            let rel_pump = gs
                .add_relation(
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
            (plc, motor, pump, rel_pump)
        };
        // Reopen: everything must be reconstructed from the slabs.
        {
            let mut eng = Engine::open(&path).unwrap();
            let gs = GraphStore::open(&mut eng).unwrap();
            assert_eq!(gs.relations.len(), 3);
            // Adjacency rebuilt: PLC --network--> Drive --electrical--> Motor --mechanical--> Pump
            let out_plc = gs.adjacency.neighbors(plc, None, Direction::Out);
            assert_eq!(out_plc.len(), 1);
            // Motor's mechanical out-edge reaches the pump.
            let mech = gs.adjacency.neighbors(motor, None, Direction::Out);
            assert_eq!(mech, vec![pump]);
            // Pump's mechanical in-edge comes from the motor.
            assert_eq!(
                gs.adjacency.neighbors(pump, None, Direction::In),
                vec![motor]
            );
            // Tombstone the mechanical relation and reopen: traversal shrinks.
            drop(gs);
            let mut gs2 = GraphStore::open(&mut eng).unwrap();
            let mut tx = eng.begin().unwrap();
            gs2.remove_relation(&mut eng, &mut tx, rel_pump).unwrap();
            gs2.persist(&mut eng).unwrap();
            eng.commit(tx).unwrap();
        }
        {
            let mut eng = Engine::open(&path).unwrap();
            let gs = GraphStore::open(&mut eng).unwrap();
            assert_eq!(gs.relations.len(), 3); // tombstone kept physically
            assert_eq!(
                gs.adjacency.neighbors(motor, None, Direction::Out),
                Vec::<u32>::new()
            );
            assert_eq!(
                gs.adjacency.neighbors(pump, None, Direction::In),
                Vec::<u32>::new()
            );
        }
        cleanup(&path);
    }

    /// String dedup + interning across slabs survives reopen.
    #[test]
    fn string_arena_dedup_and_reopen() {
        let path = tmp_path("str");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut tx = eng.begin().unwrap();
            let a = gs.intern(&mut eng, &mut tx, "mechanical").unwrap();
            let b = gs.intern(&mut eng, &mut tx, "mechanical").unwrap();
            assert_eq!(a, b); // dedup
            gs.persist(&mut eng).unwrap();
            eng.commit(tx).unwrap();
        }
        {
            let mut eng = Engine::open(&path).unwrap();
            let gs = GraphStore::open(&mut eng).unwrap();
            let sid = gs.str_lookup("mechanical").unwrap();
            assert_eq!(gs.get_str(&mut eng, sid).unwrap(), "mechanical");
            assert!(gs.str_lookup("nonexistent").is_none());
        }
        cleanup(&path);
    }

    /// Many entities/relations: packing across pages + recovery.
    /// Pairs are chained Pi -> M(i+1) so multi-hop traversal has a path.
    #[test]
    fn bulk_write_recovery() {
        let path = tmp_path("bulk");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut tx = eng.begin().unwrap();
            let mut prev_pump: Option<u32> = None;
            for i in 0..300u32 {
                let m = gs
                    .add_entity(&mut eng, &mut tx, "Motor", &format!("M{}", i), &[])
                    .unwrap();
                if let Some(p) = prev_pump {
                    gs.add_relation(&mut eng, &mut tx, p, m, "mechanical:drives", 0, -1, 0)
                        .unwrap();
                }
                let p = gs
                    .add_entity(&mut eng, &mut tx, "Pump", &format!("P{}", i), &[])
                    .unwrap();
                gs.add_relation(&mut eng, &mut tx, m, p, "mechanical:drives", 0, -1, 0)
                    .unwrap();
                prev_pump = Some(p);
            }
            gs.persist(&mut eng).unwrap();
            eng.commit(tx).unwrap();
        }
        let mut eng = Engine::open(&path).unwrap();
        let gs = GraphStore::open(&mut eng).unwrap();
        assert_eq!(gs.relations.len(), 599); // 600 drive + 299 chain = 599? 300 drive + 299 links
                                             // BFS multi-hop from entity 0: whole mechanical chain reachable.
        let reach = gs.adjacency.multi_hop(0, None, Direction::Out, 1000);
        assert_eq!(reach.len(), 599); // 600 entities - start = 599
        cleanup(&path);
    }

    /// Provenance byte round-trips through the slab.
    #[test]
    fn provenance_roundtrip() {
        let path = tmp_path("prov");
        {
            let mut eng = Engine::open(&path).unwrap();
            let mut gs = GraphStore::open(&mut eng).unwrap();
            let mut tx = eng.begin().unwrap();
            let a = gs.add_entity(&mut eng, &mut tx, "Motor", "M", &[]).unwrap();
            let b = gs.add_entity(&mut eng, &mut tx, "Pump", "P", &[]).unwrap();
            // provenance 4 = Hypothesis (model::Provenance discriminant).
            gs.add_relation(&mut eng, &mut tx, a, b, "mechanical:drives", 0, -1, 4)
                .unwrap();
            gs.persist(&mut eng).unwrap();
            eng.commit(tx).unwrap();
        }
        let mut eng = Engine::open(&path).unwrap();
        let gs = GraphStore::open(&mut eng).unwrap();
        assert_eq!(gs.relations[0].provenance, 4);
        cleanup(&path);
    }
}

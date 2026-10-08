//! Core data model for VidgeDB — Phase 0.
//!
//! Encodes the spec's fundamental invariants in the type system:
//! - Provenance is mandatory on every observation/inference (spec §14, §29).
//! - A hypothesis SHALL NOT silently become a fact (spec §29, Principle 3).
//! - Expected behavior (specification) is first-class and queryable (Principle 7).
//! - Topology is first-class: relations carry a topology class (Principle 6).
//! - Time is first-class: relations may carry validity intervals (spec §9).
#![allow(dead_code)] // Phase 0: model types exist ahead of the storage engine using them.

/// Provenance class of a piece of information (spec §14).
/// This enum is exhaustive on purpose: adding a new class is a schema-level
/// decision, not something ingestion code can do silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provenance {
    /// Structural fact (wiring, mounting). Ground truth, never inferred.
    Fact,
    /// Sensor/PLC reading. Carries a confidence of 1.0 by definition.
    Observation,
    /// Design intent / datasheet value. Expected, not observed.
    Specification,
    /// Derived by an agent or rule engine from other facts.
    Inference,
    /// Diagnostic hypothesis. MUST never be promoted to Fact/Observation
    /// by the engine — only a human or an explicit external write may.
    Hypothesis,
    /// Discrete occurrence (alarm, config change, part replacement).
    Event,
    /// A command issued to the system (distinct from its effect).
    Command,
    /// A configuration change record.
    Configuration,
}

impl Provenance {
    /// Whether values of this class may be compared against specifications.
    pub fn is_comparable(&self) -> bool {
        matches!(self, Provenance::Observation)
    }
}

/// Timestamp convention: UTC nanoseconds since Unix epoch (i64).
/// Spec §24 distinguishes event time / ingestion time / validity time;
/// Phase 0 carries event time only — the other two arrive with the WAL
/// (ingestion time is stamped by the storage engine, not the data model).
pub type Timestamp = i64;

/// Validity interval for temporal relations (spec §9). Half-open: [from, to).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Validity {
    pub valid_from: Timestamp,
    /// None = open-ended (still valid).
    pub valid_to: Option<Timestamp>,
}

impl Validity {
    pub fn always() -> Self {
        Validity {
            valid_from: 0,
            valid_to: None,
        }
    }

    pub fn contains(&self, t: Timestamp) -> bool {
        t >= self.valid_from && self.valid_to.map_or(true, |to| t < to)
    }
}

/// A uniquely identifiable object in the twin (spec §6).
/// `properties` stays a simple typed map in Phase 0; schema enforcement
/// (templates, spec §30) is layered later and must not change this shape.
#[derive(Debug, Clone)]
pub struct Entity {
    pub id: EntityId,
    pub type_name: String,
    pub name: String,
}

pub type EntityId = u32;

/// A physical/logical interface on an entity (spec §7).
#[derive(Debug, Clone)]
pub struct Port {
    pub entity: EntityId,
    pub port_id: String,
    pub direction: PortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortDirection {
    Input,
    Output,
    Bidirectional,
}

/// A typed edge between two entities, tagged with its topology class
/// (spec §8). Topology is NOT metadata — it is part of the edge identity,
/// so that `MATCH (m)-[:ELECTRICAL]->` traverses a different graph than
/// `[:MECHANICAL]` over the same node set (Principle 6).
#[derive(Debug, Clone)]
pub struct Relation {
    pub source: EntityId,
    pub target: EntityId,
    /// e.g. "electrical:connected_via", "mechanical:drives", "network:profinet"
    pub relation_type: String,
    /// Topology class before the colon, split out for indexed traversal.
    pub topology: String,
    pub validity: Validity,
    pub provenance: Provenance,
}

/// A specification: an expected bound for a signal (spec §12, §25).
/// The comparison model is EXPECTED → OBSERVED → COMPARE → DEVIATION.
/// The bound applies to the *signal* (e.g. "current"), not to a derived
/// name — spec signal and measured signal must match exactly.
#[derive(Debug, Clone)]
pub struct SpecBound {
    pub entity: EntityId,
    pub signal: String,
    pub expected_max: f64,
    pub unit: String,
    pub provenance: Provenance,
}

/// An observation: a measured value with mandatory provenance (spec §11).
#[derive(Debug, Clone)]
pub struct Measurement {
    pub entity: EntityId,
    pub signal: String,
    pub value: f64,
    pub unit: String,
    pub timestamp: Timestamp,
    pub provenance: Provenance,
}

/// A constraint violation result (spec §13, §25).
#[derive(Debug, Clone)]
pub struct Violation {
    pub entity: EntityId,
    pub signal: String,
    pub observed: f64,
    pub expected_max: f64,
    pub unit: String,
    pub timestamp: Timestamp,
}

/// The Phase 0 in-memory database. No persistence yet (Phase 1/WAL).
/// All writes go through methods so invariants are checkable in one place.
#[derive(Debug, Default)]
pub struct Vdb {
    entities: Vec<Entity>,
    ports: Vec<Port>,
    relations: Vec<Relation>,
    specs: Vec<SpecBound>,
    measurements: Vec<Measurement>,
    /// Index: (entity, signal) -> indices into measurements. Derived structure,
    /// rebuildable (Principle 4).
    measure_index: std::collections::HashMap<(EntityId, String), Vec<usize>>,
}

impl Vdb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_entity(&mut self, id: &str, type_name: &str) -> EntityId {
        let eid = self.entities.len() as EntityId;
        self.entities.push(Entity {
            id: eid,
            type_name: type_name.to_string(),
            name: id.to_string(),
        });
        eid
    }

    pub fn add_relation(&mut self, source: EntityId, target: EntityId, relation_type: &str) {
        let topology = relation_type.split(':').next().unwrap_or("").to_string();
        self.relations.push(Relation {
            source,
            target,
            relation_type: relation_type.to_string(),
            topology,
            validity: Validity::always(),
            provenance: Provenance::Fact,
        });
    }

    /// Record a specification (expected behavior). Provenance is forced to
    /// Specification — an expected value can never be an observation.
    pub fn set_spec(&mut self, entity: EntityId, signal: &str, expected_max: f64, unit: &str) {
        self.specs.push(SpecBound {
            entity,
            signal: signal.to_string(),
            expected_max,
            unit: unit.to_string(),
            provenance: Provenance::Specification,
        });
    }

    /// Record an observation. Refuses non-Observation provenance for the
    /// live series: hypotheses about values are stored elsewhere (Phase 4).
    pub fn set_measurement(&mut self, entity: EntityId, signal: &str, value: f64, unit: &str) {
        let m = Measurement {
            entity,
            signal: signal.to_string(),
            value,
            unit: unit.to_string(),
            timestamp: now_ns(),
            provenance: Provenance::Observation,
        };
        let idx = self.measurements.len();
        self.measurements.push(m);
        self.measure_index
            .entry((entity, signal.to_string()))
            .or_default()
            .push(idx);
    }

    /// EXPECTED vs OBSERVED vs COMPARE (spec §25): check every measured
    /// signal that has a spec bound, return violations.
    pub fn check_constraints(&self) -> Vec<Violation> {
        let mut out = Vec::new();
        for spec in &self.specs {
            if let Some(idxs) = self.measure_index.get(&(spec.entity, spec.signal.clone())) {
                for &i in idxs {
                    let m = &self.measurements[i];
                    if m.value > spec.expected_max {
                        out.push(Violation {
                            entity: m.entity,
                            signal: m.signal.clone(),
                            observed: m.value,
                            expected_max: spec.expected_max,
                            unit: m.unit.clone(),
                            timestamp: m.timestamp,
                        });
                    }
                }
            }
        }
        out
    }

    /// Traverse one hop through a given topology class (graph is first-class).
    pub fn neighbors(&self, source: EntityId, topology: &str) -> Vec<EntityId> {
        let t = now_ns();
        self.relations
            .iter()
            .filter(|r| r.source == source && r.topology == topology && r.validity.contains(t))
            .map(|r| r.target)
            .collect()
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    pub fn relation_count(&self) -> usize {
        self.relations.len()
    }
}

/// UTC nanoseconds since epoch. In Phase 1 this becomes engine-assigned
/// (ingestion time) with explicit event-time parameters on the write path.
pub fn now_ns() -> Timestamp {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invariant: hypothesis provenance exists but is never attached to
    /// live measurements (spec §29: no silent hypothesis→fact promotion).
    #[test]
    fn hypothesis_not_comparable() {
        assert!(!Provenance::Hypothesis.is_comparable());
        assert!(Provenance::Observation.is_comparable());
    }

    #[test]
    fn validity_intervals() {
        let v = Validity {
            valid_from: 100,
            valid_to: Some(200),
        };
        assert!(v.contains(100));
        assert!(v.contains(199));
        assert!(!v.contains(200));
        assert!(Validity::always().contains(i64::MAX));
    }

    #[test]
    fn topology_traversal_is_separate_per_class() {
        let mut db = Vdb::new();
        let a = db.add_entity("A", "PLC");
        let b = db.add_entity("B", "Drive");
        db.add_relation(a, b, "network:profinet");
        assert_eq!(db.neighbors(a, "network"), vec![b]);
        assert!(db.neighbors(a, "electrical").is_empty());
    }

    #[test]
    fn spec58_two_violations() {
        let mut db = Vdb::new();
        let motor = db.add_entity("Motor42", "Motor");
        let pump = db.add_entity("Pump17", "Pump");
        db.add_relation(motor, pump, "mechanical:drives");
        db.set_spec(motor, "current", 10.0, "A");
        db.set_spec(pump, "pressure", 180.0, "bar");
        db.set_measurement(motor, "current", 11.7, "A");
        db.set_measurement(pump, "pressure", 183.0, "bar");
        let v = db.check_constraints();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].observed, 11.7);
        assert_eq!(v[1].expected_max, 180.0);
    }

    #[test]
    fn no_violation_when_within_spec() {
        let mut db = Vdb::new();
        let motor = db.add_entity("M1", "Motor");
        db.set_spec(motor, "current", 10.0, "A");
        db.set_measurement(motor, "current", 9.9, "A");
        assert!(db.check_constraints().is_empty());
    }
}

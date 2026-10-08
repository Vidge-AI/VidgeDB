//! Phase 93 — les trois méthodes manquantes du contrat plateforme.
//!
//! La plateforme VPM (et tout client tiers) a besoin de :
//!
//! 1. `list_entities` — énumérer les entités SANS les connaître d'avance.
//!    Aujourd'hui il faut appeler `MATCH (x:Type) RETURN x` type par type, et
//!    `MATCH (x) RETURN x` (sans type) renvoie 0 ligne : l'inventaire est
//!    impossible en un appel, et l'éditeur de graphe ne peut pas peupler sa
//!    liste de nœuds.
//! 2. `graph` — tout le graphe (entités + relations) en UN aller-retour.
//!    Sans cela, dessiner N nœuds coûte O(N) requêtes plus une par relation.
//! 3. `diagnose` — le pipeline 8 étapes existe en bibliothèque Rust mais n'est
//!    exposé nulle part : la plateforme devrait être écrite en Rust pour
//!    l'appeler. Exposé en JSON-RPC, n'importe quel client y accède.
//!
//! Ces tests tournent contre le VRAI binaire en mode service (JSON-RPC sur
//! stdin), comme un client : ils mesurent le contrat, pas l'interne.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};

fn bin() -> String {
    std::env::var("VIDGEDB_BIN").unwrap_or_else(|_| "vidgedb".to_string())
}

fn tmp(name: &str) -> String {
    let mut p = std::env::temp_dir();
    p.push(format!("vidgedb_p93_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

struct Svc {
    child: Child,
    path: String,
}

impl Svc {
    fn open(path: &str, role: &str) -> Self {
        let child = Command::new(bin())
            .args(["--service", path, "--role", role, "--agent-id", "p93"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn vidgedb --service");
        Svc {
            child,
            path: path.to_string(),
        }
    }

    fn call(&mut self, id: u32, method: &str, params: serde_json::Value) -> serde_json::Value {
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params
        });
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{}", req).unwrap();
        stdin.flush().unwrap();
        let stdout = self.child.stdout.as_mut().unwrap();
        let mut line = String::new();
        BufReader::new(stdout).read_line(&mut line).unwrap();
        serde_json::from_str(&line).expect("valid JSON-RPC response")
    }

    /// Petit plant : 2 entités liées, plus des mesures sur la seconde.
    fn seed(&mut self) {
        let r = self.call(
            10,
            "upsert_entity",
            serde_json::json!({
                "name": "PLANT01", "type": "Plant",
                "props": {"site": "Lyon"}, "source": "agent"
            }),
        );
        assert!(
            r["result"]["error"].is_null(),
            "seed PLANT01 refuse: {:?}",
            r
        );
        let r = self.call(
            11,
            "upsert_entity",
            serde_json::json!({
                "name": "EMOT01", "type": "Motor",
                "props": {"spec.current.max": "10"},
                "relations": [{"to": "PLANT01", "relation_type": "mechanical:feeds"}],
                "source": "agent"
            }),
        );
        assert!(
            r["result"]["error"].is_null(),
            "seed EMOT01 refuse: {:?}",
            r
        );
        let pts: Vec<serde_json::Value> = (0..20)
            .map(|i| serde_json::json!([1700000000 + i * 60, 8.0 + (i % 5) as f64]))
            .collect();
        let r = self.call(
            12,
            "ingest_points",
            serde_json::json!({"entity": "EMOT01", "signal": "current", "points": pts}),
        );
        assert!(
            r["result"]["error"].is_null(),
            "seed ingest_points refuse: {:?}",
            r
        );
    }
}

impl Drop for Svc {
    fn drop(&mut self) {
        let _ = self.child.stdin.take();
        let _ = self.child.wait();
        for suf in ["", "-wal", "-wlock"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path, suf));
        }
    }
}

/// `list_entities` doit rendre les entités SANS que l'appelant les connaisse,
/// avec de quoi peupler un sélecteur : nom, type, clé.
#[test]
fn list_entities_enumerates_without_prior_knowledge() {
    let path = tmp("list");
    let mut s = Svc::open(&path, "ingest");
    s.seed();

    let r = s.call(20, "list_entities", serde_json::json!({}));
    assert!(
        r["error"].is_null(),
        "list_entities doit exister (aujourd'hui -32601): {:?}",
        r
    );
    let arr = r["result"]["entities"]
        .as_array()
        .unwrap_or_else(|| panic!("result.entities doit etre un tableau, recu: {:?}", r));

    let names: Vec<&str> = arr.iter().filter_map(|e| e["name"].as_str()).collect();
    assert!(
        names.contains(&"PLANT01") && names.contains(&"EMOT01"),
        "les 2 entites doivent apparaitre, recu: {:?}",
        names
    );
    assert_eq!(arr.len(), 2, "exactement 2 entites, recu: {:?}", names);

    // Le type doit etre present : c'est lui qui pilote l'icone du noeud.
    assert!(
        arr.iter().any(|e| e["type"].as_str() == Some("Motor")),
        "le type doit etre rendu, recu: {:?}",
        arr
    );

    // Filtre par type : la plateforme liste par famille de composants.
    let r = s.call(21, "list_entities", serde_json::json!({"type": "Motor"}));
    let arr = r["result"]["entities"]
        .as_array()
        .unwrap_or_else(|| panic!("result.entities attendu, recu: {:?}", r));
    assert_eq!(
        arr.len(),
        1,
        "filtre type=Motor -> 1 entite, recu: {:?}",
        arr
    );
    assert_eq!(arr[0]["name"].as_str(), Some("EMOT01"));
}

/// `graph` doit rendre tout le graphe en UN appel : c'est ce que dessine
/// l'éditeur. Les relations doivent porter leurs extremites par NOM (pas par
/// clé interne), sinon le client doit resoudre chaque clé un par un.
#[test]
fn graph_returns_the_whole_graph_in_one_call() {
    let path = tmp("graph");
    let mut s = Svc::open(&path, "ingest");
    s.seed();

    let r = s.call(30, "graph", serde_json::json!({}));
    assert!(
        r["error"].is_null(),
        "graph doit exister (aujourd'hui -32601): {:?}",
        r
    );

    let ents = r["result"]["entities"].as_array().expect("entities[]");
    let rels = r["result"]["relations"].as_array().expect("relations[]");
    assert_eq!(ents.len(), 2, "2 noeuds, recu: {:?}", ents.len());
    assert_eq!(rels.len(), 1, "1 arete, recu: {:?}", rels);

    let rel = &rels[0];
    assert_eq!(rel["from"].as_str(), Some("EMOT01"), "extremite par nom");
    assert_eq!(rel["to"].as_str(), Some("PLANT01"), "extremite par nom");
    assert_eq!(
        rel["relation_type"].as_str(),
        Some("mechanical:feeds"),
        "le type de relation doit etre rendu tel que declare"
    );
}

/// `diagnose` : le rapport 8 étapes doit sortir en JSON-RPC, avec les données
/// reelles du jumeau (ici une violation de la specification courant).
#[test]
fn diagnose_is_reachable_over_jsonrpc() {
    let path = tmp("diag");
    let mut s = Svc::open(&path, "ingest");
    s.seed();

    let r = s.call(
        40,
        "diagnose",
        serde_json::json!({"entity": "EMOT01", "from": 1699999000, "to": 1700005000}),
    );
    assert!(
        r["error"].is_null(),
        "diagnose doit exister (aujourd'hui -32601): {:?}",
        r
    );

    let rep = &r["result"];
    assert_eq!(rep["entity"].as_str(), Some("EMOT01"), "entite echoee");
    assert!(
        rep["component"]["name"].as_str() == Some("EMOT01"),
        "l'etape 1 doit identifier le composant, recu: {:?}",
        rep["component"]
    );
    // Les mesures ingerees doivent apparaitre dans l'etape 5.
    let meas = rep["measurements"]
        .as_array()
        .unwrap_or_else(|| panic!("measurements[] attendu, recu: {:?}", rep));
    assert!(
        meas.iter().any(|m| m["signal"].as_str() == Some("current")),
        "le signal 'current' doit apparaitre, recu: {:?}",
        meas
    );
    // Le rapport porte sa politique de provenance : sans elle, un consommateur
    // ne peut pas distinguer une mesure d'une specification.
    assert!(
        rep["provenance_note"].as_str().is_some(),
        "provenance_note obligatoire, recu: {:?}",
        rep
    );
    assert!(
        rep["causal_paths"].is_array(),
        "l'etape 8 doit rendre un tableau (hypotheses, jamais persistees)"
    );
}

/// `set_hypothesis` doit être ATTEIGNABLE et refuser avec un message lisible
/// par machine. Avant, le refus existait dans `tools.rs` mais n'était pas
/// branché : l'appelant recevait `-32601 unknown method`, c'est-à-dire
/// « ça n'existe pas », au lieu de « c'est interdit, et voici pourquoi ».
/// La différence compte : un agent qui reçoit `-32601` va réessayer autrement.
#[test]
fn set_hypothesis_is_reachable_and_refuses_explicitly() {
    let path = tmp("hyp");
    let mut s = Svc::open(&path, "ingest");
    s.seed();

    let r = s.call(
        60,
        "set_hypothesis",
        serde_json::json!({"entity": "EMOT01", "text": "la pompe a lache"}),
    );
    assert!(
        r["error"].is_null(),
        "le refus doit etre un result.error, pas une erreur transport: {:?}",
        r
    );
    let msg = r["result"]["error"]
        .as_str()
        .unwrap_or_else(|| panic!("result.error attendu, recu: {:?}", r));
    assert!(
        msg.contains("refused") && msg.contains("29"),
        "le refus doit citer la spec, recu: {:?}",
        msg
    );

    // Et il ne doit RIEN avoir ecrit : la meme session continue de marcher,
    // et aucun etat nouveau n'est apparu.
    let r = s.call(61, "list_entities", serde_json::json!({}));
    assert_eq!(
        r["result"]["total"].as_u64(),
        Some(2),
        "le refus ne cree aucune entite, recu: {:?}",
        r["result"]
    );
}

/// Une entité inconnue doit produire un rapport EXPLICITE (composant absent),
/// pas une erreur de transport ni un plantage : la plateforme affiche
/// « composant introuvable » plutôt que de casser la page.
#[test]
fn diagnose_on_unknown_entity_is_explicit() {
    let path = tmp("diag_unknown");
    let mut s = Svc::open(&path, "ingest");
    s.seed();

    let r = s.call(
        50,
        "diagnose",
        serde_json::json!({"entity": "NOPE", "from": 0, "to": 1700005000}),
    );
    assert!(r["error"].is_null(), "pas d'erreur transport: {:?}", r);
    assert!(
        r["result"]["component"].is_null(),
        "component doit etre null pour une entite inconnue, recu: {:?}",
        r["result"]["component"]
    );
    assert_eq!(r["result"]["entity"].as_str(), Some("NOPE"), "nom echoé");
}

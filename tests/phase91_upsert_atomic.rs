//! Phase 91 — atomicité de `upsert_entity` (correctif du bug de session
//! empoisonnée, découvert le 2026-10-06).
//!
//! Le défaut : appeler `upsert_entity` avec une relation dont la cible
//! n'existe pas renvoyait bien l'erreur attendue, MAIS laissait la session
//! vivante dans un état corrompu — l'entité à demi-créée restait à la clé
//! suivante avec un nom NUL, invisible en VQL, et le `upsert_entity` VALIDE
//! suivant échouait avec `PageOutOfBounds`. Le fichier sur disque restait sain
//! (une réouverture récupérait), mais un processus longue durée (`--http`,
//! `--opcua`) pouvait se figer sur une simple entrée invalide.
//!
//! Cause : la transaction était ouverte AVANT la validation des relations ;
//! le `drop(tx)` rendait les PAGES au pager mais pas les tableaux en mémoire
//! des stores.
//!
//! Ces tests tournent contre le VRAI service (binaire lancé en sous-process,
//! JSON-RPC sur stdin) parce que c'est là que le défaut se manifestait : dans
//! un processus qui vit plus longtemps qu'un appel.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};

fn bin() -> String {
    match std::env::var("VIDGEDB_BIN") {
        Ok(v) if !v.is_empty() => v,
        _ => panic!(
            "VIDGEDB_BIN is not set — these tests drive the real `vidgedb --service` binary; \
             run with VIDGEDB_BIN=$PWD/target/release/vidgedb (see DEVELOPING.md)"
        ),
    }
}

fn tmp(name: &str) -> String {
    let mut p = std::env::temp_dir();
    p.push(format!("vidgedb_p91_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

struct Svc {
    child: Child,
    path: String,
}

impl Svc {
    fn open(path: &str, role: &str) -> Self {
        let child = Command::new(bin())
            .args(["--service", path, "--role", role, "--agent-id", "p91"])
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

    /// Envoie un appel et rend la reponse brute (objet JSON complet).
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
}

impl Drop for Svc {
    fn drop(&mut self) {
        let _ = self.child.stdin.take(); // EOF -> sortie propre
        let _ = self.child.wait();
        for suf in ["", "-wal", "-wlock"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path, suf));
        }
    }
}

/// Le scenario exact du bug : relation vers une cible absente, puis on
/// continue de travailler dans la MEME session.
#[test]
fn refused_relation_does_not_poison_the_session() {
    let path = tmp("poison");
    let mut s = Svc::open(&path, "ingest");

    // 1) refus attendu : la cible GHOST n'existe pas
    let r = s.call(
        1,
        "upsert_entity",
        serde_json::json!({
            "name": "EMOT01", "type": "Motor",
            "props": {"spec.current.max": "3"},
            "relations": [{"to": "GHOST", "relation_type": "mechanical:drives"}],
            "source": "plc"
        }),
    );
    let msg = r["result"]["error"].as_str().unwrap_or("");
    assert!(
        msg.contains("relation target 'GHOST' does not exist"),
        "le refus doit dire quelle cible manque, recu: {:?}",
        r
    );

    // 2) LE BUG : l'entite a demi-creee restait accessible avec un nom NUL.
    //    Elle ne doit plus exister du tout.
    let e0 = s.call(2, "get_entity", serde_json::json!({"key": 0}));
    let name = e0["result"]["name"].as_str().unwrap_or("");
    assert!(
        !name.contains('\u{0}'),
        "une entite refusee ne doit pas laisser de nom NUL a la cle 0, recu: {:?}",
        e0
    );

    // 3) elle doit etre invisible en VQL (elle l'etait deja avant, mais si
    //    elle reapparait un jour corrompue, ce test le dit)
    let q = s.call(
        3,
        "query",
        serde_json::json!({"vql": "MATCH (m:Motor) RETURN m"}),
    );
    assert_eq!(
        q["result"]["n"], 0,
        "aucun Motor ne doit exister apres un upsert refuse, recu: {:?}",
        q
    );

    // 4) LE BUG (le plus grave) : l'upsert VALIDE suivant echouait en
    //    PageOutOfBounds. Il doit maintenant reussir.
    let ok = s.call(
        4,
        "upsert_entity",
        serde_json::json!({
            "name": "CONV01", "type": "Conveyor", "props": {}, "source": "plc"
        }),
    );
    assert!(
        ok["result"]["created"] == true,
        "l'upsert suivant un refus doit reussir (PageOutOfBounds avant correctif), recu: {:?}",
        ok
    );

    // 5) et le twin doit rester pleinement utilisable : telemetrie + check
    let ing = s.call(
        5,
        "ingest_points",
        serde_json::json!({
            "entity": "CONV01", "signal": "speed", "points": [[1760000000, 0.42]]
        }),
    );
    assert_eq!(
        ing["result"]["accepted"], 1,
        "ingest apres refus: {:?}",
        ing
    );

    let chk = s.call(
        6,
        "check",
        serde_json::json!({
            "entity": "CONV01", "signal": "speed",
            "from": 1760000000, "to": 1760003600
        }),
    );
    // pas de spec -> NO_SPEC, mais surtout PAS une erreur ni un nom corrompu
    let entity = chk["result"]["entity"].as_str().unwrap_or("");
    assert!(
        !entity.contains('\u{0}'),
        "check ne doit jamais rendre un nom NUL, recu: {:?}",
        chk
    );
}

/// Le refus doit intervenir AVANT toute ecriture : rien ne doit etre cree,
/// meme partiellement, y compris quand l'entite existe deja (chemin UPDATE).
#[test]
fn refused_relation_writes_nothing_on_the_update_path_too() {
    let path = tmp("update");
    let mut s = Svc::open(&path, "ingest");

    // une entite valide, seule
    let a = s.call(
        1,
        "upsert_entity",
        serde_json::json!({"name": "M1", "type": "Motor", "props": {}, "source": "plc"}),
    );
    assert_eq!(a["result"]["created"], true);

    let n_before = s.call(2, "schema", serde_json::json!({}))["result"]["entity_types"]
        .as_array()
        .map(|v| v.len())
        .unwrap_or(0);

    // UPDATE de M1 + relation vers une cible absente -> refus
    let r = s.call(
        3,
        "upsert_entity",
        serde_json::json!({
            "name": "M1", "type": "Motor",
            "props": {"nouveau": "x"},
            "relations": [{"to": "ABSENT", "relation_type": "mechanical:drives"}],
            "source": "plc"
        }),
    );
    assert!(
        r["result"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("does not exist"),
        "refus attendu, recu: {:?}",
        r
    );

    // l'entite existe toujours, et la session est saine
    let n_after = s.call(4, "schema", serde_json::json!({}))["result"]["entity_types"]
        .as_array()
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        n_before, n_after,
        "un refus ne doit pas changer l'inventaire de types"
    );

    let ok = s.call(
        5,
        "upsert_entity",
        serde_json::json!({"name": "M2", "type": "Motor", "props": {}, "source": "plc"}),
    );
    assert_eq!(
        ok["result"]["created"], true,
        "la session doit rester utilisable apres un refus: {:?}",
        ok
    );
}

/// Un `relation_type` malforme doit etre refuse de la meme facon : avant
/// toute ecriture, et sans laisser de trace.
#[test]
fn malformed_relation_type_is_refused_before_writing() {
    let path = tmp("badtype");
    let mut s = Svc::open(&path, "ingest");

    s.call(
        1,
        "upsert_entity",
        serde_json::json!({"name": "CIBLE", "type": "Motor", "props": {}, "source": "plc"}),
    );

    let r = s.call(
        2,
        "upsert_entity",
        serde_json::json!({
            "name": "SRC1", "type": "Drive", "props": {},
            "relations": [{"to": "CIBLE", "relation_type": "pas_de_topologie"}],
            "source": "plc"
        }),
    );
    assert!(
        // Le refus peut venir de deux couches : le service (parametre
        // malforme -> -32602 transport) ou la couche API (result.error).
        r["result"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("topology:type")
            || r["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("topology:type"),
        "refus attendu sur le format, recu: {:?}",
        r
    );

    // SRC1 ne doit pas exister (refus avant ecriture)
    let q = s.call(
        3,
        "query",
        serde_json::json!({"vql": "MATCH (d:Drive) RETURN d"}),
    );
    assert_eq!(q["result"]["n"], 0, "aucun Drive cree par un appel refuse");

    // et la session reste saine
    let ok = s.call(
        4,
        "upsert_entity",
        serde_json::json!({"name": "SRC2", "type": "Drive", "props": {}, "source": "plc"}),
    );
    assert_eq!(ok["result"]["created"], true, "session saine: {:?}", ok);
}

/// Auto-relation : meme contrat.
#[test]
fn self_relation_is_refused_before_writing() {
    let path = tmp("selfrel");
    let mut s = Svc::open(&path, "ingest");

    let r = s.call(
        1,
        "upsert_entity",
        serde_json::json!({
            "name": "LOOP", "type": "Motor", "props": {},
            "relations": [{"to": "LOOP", "relation_type": "mechanical:drives"}],
            "source": "plc"
        }),
    );
    assert!(
        r["result"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("self-relations"),
        "refus attendu, recu: {:?}",
        r
    );

    let q = s.call(
        2,
        "query",
        serde_json::json!({"vql": "MATCH (m:Motor) RETURN m"}),
    );
    assert_eq!(q["result"]["n"], 0, "rien ne doit avoir ete cree");
}

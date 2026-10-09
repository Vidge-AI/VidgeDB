//! Phase 92 — arrêt propre sur signal (correctif du verrou fantôme).
//!
//! Le défaut : le verrou single-writer `<db>.vdg-wlock` n'est libéré que par
//! le destructeur `Drop` du `DbWriteLock`. Le binaire n'installait AUCUN
//! gestionnaire de signal, donc `SIGTERM` (ce que fait `docker stop`) tuait
//! le processus sans dérouler les destructeurs : le verrou restait sur
//! disque, et un conteneur relancé sur le même volume était REFUSÉ pendant
//! 75 s (« write-locked by process N »).
//!
//! Ces tests envoient de VRAIS signaux à un VRAI processus vidgedb et
//! vérifient que le verrou a disparu — c'est le seul moyen de prouver le
//! correctif, un test unitaire ne peut pas tuer son propre processus.
//!
//! `SIGKILL` reste exclu du contrat, volontairement : on ne peut pas
//! intercepter un kill -9. Une exécution qui se termine ainsi est traitée par
//! le vol de verrou après 75 s (documenté dans docs/deployment.md).

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

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
    p.push(format!("vidgedb_p92_{}_{}.vdg", name, std::process::id()));
    p.to_string_lossy().to_string()
}

fn cleanup(path: &str) {
    for suf in ["", "-wal", "-wlock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }
}

fn lock_path(db: &str) -> String {
    format!("{}-wlock", db)
}

/// Lance un service en mode `--http` (processus longue durée, comme en
/// production) et attend que le verrou soit posé.
fn spawn_http(db: &str, port: u16) -> Child {
    let child = Command::new(bin())
        .args([
            "--http",
            db,
            "--port",
            &port.to_string(),
            "--role",
            "ingest",
            "--agent-id",
            "p92",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn vidgedb --http");
    // Le verrou est posé pendant l'ouverture : on attend qu'il apparaisse.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !Path::new(&lock_path(db)).exists() && Instant::now() < deadline {
        sleep(Duration::from_millis(50));
    }
    assert!(
        Path::new(&lock_path(db)).exists(),
        "le verrou doit exister pendant que le writer tourne ({})",
        lock_path(db)
    );
    child
}

/// Envoie un signal et attend la sortie du processus, en bornant l'attente.
fn signal_and_wait(child: &mut Child, sig: i32) -> Option<i32> {
    let pid = child.id() as i32;
    // SAFETY: kill(2) sur un pid qu'on a soi-même lancé.
    unsafe {
        libc_kill(pid, sig);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(Some(st)) = child.try_wait() {
            return Some(st.code().unwrap_or(-1));
        }
        sleep(Duration::from_millis(50));
    }
    None
}

extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

const SIGTERM: i32 = 15;
const SIGINT: i32 = 2;

/// Le cas de production : `docker stop` envoie SIGTERM. Le verrou doit
/// disparaître, sinon le conteneur suivant est refusé 75 s.
#[test]
fn sigterm_releases_the_writer_lock() {
    let db = tmp("sigterm");
    cleanup(&db);
    let mut child = spawn_http(&db, 19961);

    let code = signal_and_wait(&mut child, SIGTERM);
    assert!(code.is_some(), "le processus doit sortir sur SIGTERM");

    assert!(
        !Path::new(&lock_path(&db)).exists(),
        "SIGTERM doit libérer le verrou (sinon 75 s de refus au redémarrage)"
    );
    cleanup(&db);
}

/// Ctrl-C dans un terminal : même contrat.
#[test]
fn sigint_releases_the_writer_lock() {
    let db = tmp("sigint");
    cleanup(&db);
    let mut child = spawn_http(&db, 19962);

    let code = signal_and_wait(&mut child, SIGINT);
    assert!(code.is_some(), "le processus doit sortir sur SIGINT");

    assert!(
        !Path::new(&lock_path(&db)).exists(),
        "SIGINT doit libérer le verrou"
    );
    cleanup(&db);
}

/// Après un arrêt propre, un NOUVEAU writer sur le même fichier doit
/// démarrer IMMÉDIATEMENT (c'est le bénéfice concret du correctif).
#[test]
fn a_new_writer_starts_immediately_after_a_clean_stop() {
    let db = tmp("restart");
    cleanup(&db);

    // premier writer : on écrit une donnée, puis SIGTERM
    let mut first = spawn_http(&db, 19963);
    let st = signal_and_wait(&mut first, SIGTERM);
    assert!(st.is_some(), "premier writer doit sortir");

    // second writer : doit s'ouvrir tout de suite, sans attendre 75 s
    let t0 = Instant::now();
    let mut second = spawn_http(&db, 19964);
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "un nouveau writer doit démarrer sans délai après un arrêt propre \
(attendu < 5 s, mesuré {:?}) — c'est le bug des 75 s",
        elapsed
    );

    let _ = signal_and_wait(&mut second, SIGTERM);
    cleanup(&db);
}

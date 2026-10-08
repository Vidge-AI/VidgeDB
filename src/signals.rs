//! Arrêt ordonné sur signal (phase 92).
//!
//! POURQUOI CE MODULE EXISTE
//! Le verrou single-writer `<db>.vdg-wlock` n'est libéré que par le
//! destructeur `Drop` du `DbWriteLock`. Sans gestionnaire de signal, un
//! `SIGTERM` (ce qu'envoie `docker stop`) tue le processus sans dérouler les
//! destructeurs : le verrou reste sur disque et le conteneur suivant est
//! REFUSÉ pendant 75 s (« write-locked by process N »).
//!
//! COMMENT ÇA MARCHE
//! Un thread dédié appelle `sigwait` sur le jeu {SIGINT, SIGTERM}. Le
//! signal est donc BLOQUÉ dans tous les threads (masqué au démarrage) et
//! consommé de façon SYNCHRONE par ce thread : pas de code exécuté dans un
//! contexte de signal (pas d'allocation, pas de verrou, aucun risque de
//! interblocage avec l'allocateur ou le pager).
//!
//! À la réception, le drapeau `SHUTDOWN` est armé et le processus sort par
//! `std::process::exit(0)`. On ne peut pas simplement « revenir du main » :
//! les boucles serveur (`--http`, `--opcua`) sont dans d'autres threads, et
//! `process::exit` est le seul moyen d'être sûr que TOUS les destructeurs
//! s'exécutent — dont celui du verrou.
//!
//! PORTÉE
//! `SIGKILL` reste hors contrat, volontairement : il n'est pas interceptable.
//! Une exécution tuée ainsi est traitée par le vol de verrou après 75 s
//! (documented in docs/deployment.md).
//!
//! PLATEFORME
//! `sigwait` est POSIX. Sur Windows la fonction est compilée mais inerte :
//! Windows n'a pas de `SIGTERM` ; la console y reçoit Ctrl-C/Ctrl-Break, qui
//! passent par un autre mécanisme (hors périmètre v1).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Arme quand une demande d'arrêt a été reçue (un signal, ou l'EOF stdin).
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Vrai si une demande d'arrêt a été reçue. Les boucles serveur peuvent
/// consulter ce drapeau pour sortir d'elles-mêmes.
pub fn shutdown_requested() -> bool {
    SHUTDOWN.load(Ordering::SeqCst)
}

/// Arme le drapeau d'arrêt (utilisé aussi par la fin de flux stdin).
pub fn request_shutdown() {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Registre des arrêts ordonnés (phase 92).
///
/// POURQUOI IL EXISTE
/// Sur `SIGTERM` (ce qu'envoie `docker stop`), le service doit quitter en
/// libérant son verrou single-writer. Or les boucles serveur vivent dans des
/// threads qui détiennent un `Arc<Mutex<AgentApi>>` : `std::process::exit`
/// ne déroule PAS leurs destructeurs, donc le `Drop` du verrou ne s'exécute
/// jamais — c'est précisément le bug « verrou fantôme, 75 s de refus ».
///
/// SOLUTION
/// Le thread qui consomme le signal exécute lui-même les actions d'arrêt
/// enregistrées, AVANT de quitter. Ce sont des actions explicites,
/// idempotentes, qui ne dépendent d'aucun déroulement de pile.
static SHUTDOWN_HOOKS: Mutex<Vec<Box<dyn Fn() + Send + Sync>>> = Mutex::new(Vec::new());

/// Enregistre une action d'arrêt (exécutée sur signal, dans l'ordre inverse
/// d'enregistrement). L'action DOIT être idempotente : elle peut aussi
/// s'exécuter par la voie normale (`Drop`) sur une sortie par EOF.
pub fn register_shutdown_hook(f: Box<dyn Fn() + Send + Sync>) {
    if let Ok(mut hooks) = SHUTDOWN_HOOKS.lock() {
        hooks.push(f);
    }
}

/// Exécute toutes les actions d'arrêt enregistrées (LIFO). Chaque action est
/// isolée : un échec n'empêche pas les suivantes.
pub fn run_shutdown_hooks() {
    let hooks: Vec<Box<dyn Fn() + Send + Sync>> = match SHUTDOWN_HOOKS.lock() {
        Ok(mut guard) => std::mem::take(&mut *guard),
        Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
    };
    for hook in hooks.iter().rev() {
        hook();
    }
}

#[cfg(unix)]
pub fn install_signal_handlers() {
    use std::thread;

    // Bloquer SIGINT/SIGTERM dans TOUS les threads existants et futurs :
    // le signal est ainsi livré uniquement au thread qui l'attend
    // explicitement. À appeler au tout début du main, avant tout spawn.
    //
    // SAFETY: sigemptyset/sigaddset initialisent un sigset_t local, et
    // pthread_sigmask ne fait que lire le pointeur fourni.
    unsafe {
        let mut set: SigSet = std::mem::zeroed();
        sigemptyset(&mut set);
        sigaddset(&mut set, SIGINT);
        sigaddset(&mut set, SIGTERM);
        if pthread_sigmask(SIG_BLOCK, &set, std::ptr::null_mut()) != 0 {
            eprintln!(
                "vidgedb: warning: could not block signals; Ctrl-C/SIGTERM \
may not release the writer lock"
            );
            return;
        }
        thread::spawn(move || {
            let mut pending: SigSet = std::mem::zeroed();
            loop {
                // Attente bloquante et synchrone du signal.
                // Le 2e argument de sigwait est un `int *` : on passe
                // l'adresse de la structure, le noyau n'y ecrit que l'entier.
                let rc = sigwait(&set, &mut pending as *mut SigSet as *mut i32);
                if rc != 0 {
                    // Un EINTR ne doit pas faire sortir le thread.
                    continue;
                }
                let sig = first_signal(&pending);
                SHUTDOWN.store(true, Ordering::SeqCst);
                eprintln!(
                    "vidgedb: {} received — shutting down cleanly \
(releasing the writer lock)",
                    if sig == SIGINT { "SIGINT" } else { "SIGTERM" }
                );
                // Les actions d'arrêt (dont la libération du verrou) sont
                // exécutées ICI : leurs threads ne peuvent pas être déroulés
                // par process::exit.
                run_shutdown_hooks();
                std::process::exit(0);
            }
        });
    }
}

#[cfg(not(unix))]
pub fn install_signal_handlers() {
    // Windows : pas de SIGTERM. Rien à installer en v1 ; l'EOF stdin libère
    // déjà le verrou (chemin documenté pour les clients stdio).
}

// ---------------------------------------------------------------------------
// Declarations POSIX minimales (aucune dependance Cargo ajoutee)
// ---------------------------------------------------------------------------

#[cfg(unix)]
const SIGINT: i32 = 2;
#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIG_BLOCK: i32 = 0;

/// `sigset_t` fait 128 octets sur les plateformes Linux/glibc courantes.
/// On surdimensionne largement pour rester sûr partout où c'est plus grand.
#[cfg(unix)]
#[repr(C)]
#[derive(Clone, Copy)]
struct SigSet {
    bits: [u64; 32],
}

#[cfg(unix)]
extern "C" {
    fn sigemptyset(set: *mut SigSet) -> i32;
    fn sigaddset(set: *mut SigSet, signum: i32) -> i32;
    fn sigwait(set: *const SigSet, sig: *mut i32) -> i32;
    fn pthread_sigmask(how: i32, set: *const SigSet, old: *mut SigSet) -> i32;
}

#[cfg(unix)]
fn first_signal(set: &SigSet) -> i32 {
    // Bit 0 du mot 0 = signal 1 ; on cherche le premier bit arme.
    for (word_idx, word) in set.bits.iter().enumerate() {
        if *word != 0 {
            let bit = word.trailing_zeros() as i32;
            return (word_idx as i32) * 64 + bit + 1;
        }
    }
    SIGTERM
}

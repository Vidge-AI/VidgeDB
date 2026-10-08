//! VidgeDB — embedded temporal graph database for digital twins.
//!
//! Library crate holding the engine layers (spec §17 storage architecture):
//! - `model`  : Phase 0 data model + invariants
//! - `pager`  : Phase 1 page store (superblock, freelist, pages)
//! - `wal`    : Phase 1 write-ahead log (frames, commit, replay, checkpoint)
//! - `engine` : Phase 1 transactional wrapper binding pager + WAL
//!
//! The binary target keeps the Phase 0 smoke test (spec §58 example).

pub mod benchgen;
pub mod check;
pub mod diagnose;
pub mod engine;
pub mod executor;
pub mod http;
pub mod model;
/// Phase 16: the OPC-UA server surface (feature `opcua`, on by default).
#[cfg(feature = "opcua")]
pub mod opcua_server;
pub mod pager;
/// Phase 10: JSON-RPC 2.0 over stdin/stdout for AI agents (service mode).
pub mod service;
pub mod signals;
pub mod statestore;
pub mod stores;
pub mod temporal;
pub mod timeseries;
pub mod tools;
pub mod ttemporal;
pub mod vidgeql;
pub mod wal;

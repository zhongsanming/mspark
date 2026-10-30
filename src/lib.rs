//! `mspark`: reimplementation of the MindStudio Profiler analysis pipeline.
//!
//! The library holds everything worth testing: raw decoding and nodes
//! (`mspark-model`), sessions, report builders and the verifiers. The binary
//! only parses the command line and dispatches.

pub mod artifacts;
pub mod commands;
pub mod errors;
pub use mspark_model::memtrace;
pub mod reports;
pub mod session;
pub mod text;
pub mod trace;
pub mod verification;
pub mod verify;

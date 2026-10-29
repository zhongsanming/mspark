//! Artifact exporters.
//!
//! Each submodule owns one artifact family: pure row builders that take decoded
//! records, plus the writer that resolves a device session and formats the file.
//! The writers share [`write_text`] and the timeline tracks share [`round3`].

pub mod communication;
pub mod hbm;
pub mod hccs;
pub mod host;
pub mod inspect;
pub mod llc;
pub mod memory;
pub mod net;
pub mod pcie;
pub mod summary;
pub mod task;
pub mod timeline;
pub mod tscpu;

use std::path::Path;

use anyhow::Result;

/// Write a fully-built table, creating the output directory if needed.
pub(crate) fn write_text(out: &Path, body: &str) -> Result<()> {
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    Ok(())
}

/// Round to the 3-decimal grid used by the timeline tracks (`round_ties_even`,
/// matching Python's `round`).
pub(crate) fn round3(value: f64) -> f64 {
    (value * 1000.0).round_ties_even() / 1000.0
}

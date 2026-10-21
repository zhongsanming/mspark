//! Zero-copy decoders for the raw Ascend streams.
//!
//! The on-disk records are extern `#pragma pack(1)` C structs with bitfields and
//! little-endian fields. They are *not* `bitcode`/serde data: the layout belongs
//! to the runtime that produced them, so we reinterpret the packed bytes directly
//! with `zerocopy` and expose bitfields as shift/mask accessors.

mod device;
mod host;

pub use device::*;
pub use host::*;

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

fn read_concat(dir: &Path, prefix: &str, trunk: usize) -> Result<Vec<u8>> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(Vec::new());
    };

    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(prefix)
            || name.ends_with(".done")
            || name.ends_with(".complete")
            || name.ends_with(".json")
        {
            continue;
        }
        let slice = name
            .rsplit('_')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        files.push((slice, entry.path()));
    }
    if files.is_empty() {
        return Ok(Vec::new());
    }
    files.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut bytes = Vec::new();
    for (_, path) in &files {
        bytes.extend_from_slice(&std::fs::read(path)?);
    }

    let offset = bytes.len() % trunk;
    let end = offset + (bytes.len() / trunk) * trunk;
    Ok(bytes[offset..end].to_vec())
}

fn cast(e: impl core::fmt::Display) -> anyhow::Error {
    anyhow!("raw record cast failed: {e}")
}

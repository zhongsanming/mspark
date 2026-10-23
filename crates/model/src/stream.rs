//! Reading and framing of the raw stream files.
//!
//! Every stream is a directory of `prefix.slice_N` files that must be
//! concatenated in slice order; the record decoders themselves live next to the
//! node that consumes them.

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::Ctx;

/// Read all slice files for `prefix` from `<device>/data`, concatenate them, and
/// trim the leading partial chunk exactly like `Parser::ReadDataEntry`.
///
/// A missing stream is *not* an error: absence just yields an empty table.
pub fn read_stream(ctx: &Ctx, prefix: &str, trunk: usize) -> Result<Vec<u8>> {
    read_concat(&ctx.device_data_dir()?, prefix, trunk)
}

/// Read a `host/data` stream (same slice-concat semantics).
pub fn read_host_stream(ctx: &Ctx, prefix: &str, trunk: usize) -> Result<Vec<u8>> {
    read_concat(&ctx.host_data_dir(), prefix, trunk)
}

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

pub(crate) fn cast(e: impl core::fmt::Display) -> anyhow::Error {
    anyhow!("raw record cast failed: {e}")
}

// ---------------------------------------------------------------------------
// Host compact streams (`MsprofCompactInfo`, 64-byte records)
// ---------------------------------------------------------------------------

pub const COMPACT_TRUNK: usize = 64;

pub const COMPACT_MAGIC: u16 = 0x5a5a;

/// `MsprofCompactInfo` prefix (24 bytes) + 40-byte `data` union.
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub(crate) struct CompactHead {
    pub(crate) magic: U16,
    pub(crate) _level: U16,
    pub(crate) _ty: U32,
    pub(crate) _thread: U32,
    pub(crate) _data_len: U32,
    pub(crate) _timestamp: U64,
    pub(crate) data: [u8; 40],
}

pub const ADDITIONAL_TRUNK: usize = 256;

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub(crate) struct AdditionalHead {
    pub(crate) magic: U16,
    pub(crate) _level: U16,
    pub(crate) _ty: U32,
    pub(crate) thread_id: U32,
    pub(crate) _data_len: U32,
    pub(crate) timestamp: U64,
    pub(crate) data: [u8; 232],
}
const _: () = assert!(core::mem::size_of::<AdditionalHead>() == ADDITIONAL_TRUNK);

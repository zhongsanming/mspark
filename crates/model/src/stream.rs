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

/// Files under `dir` whose name starts with one of `prefixes`, in slice order.
///
/// This was implemented four times (`time::find_one`, `ctx::find_info_json`,
/// `host::files_with_prefix`, `stream::read_concat`); the rules are the same:
/// skip partial-write markers, order by the trailing slice index.
pub fn files_with_prefix(dir: &Path, prefixes: &[&str]) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".done") || name.ends_with(".complete") {
            continue;
        }
        // A slice file is `<prefix>.<suffix>` (e.g. `start_info.1234.json`) or
        // `<prefix>_<n>` (e.g. `...type_info_dic.slice_0`); a bare `<prefix>` is
        // also accepted.
        if !prefixes
            .iter()
            .any(|prefix| match name.strip_prefix(prefix) {
                Some(rest) => rest.is_empty() || rest.starts_with('.') || rest.starts_with('_'),
                None => false,
            })
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
    files.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    files.into_iter().map(|(_, path)| path).collect()
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

    // Drop the leading partial chunk in place: `to_vec()` used to duplicate the
    // whole stream (these files reach hundreds of MB).
    let offset = bytes.len() % trunk;
    let end = offset + (bytes.len() / trunk) * trunk;
    bytes.copy_within(offset..end, 0);
    bytes.truncate(end - offset);
    Ok(bytes)
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

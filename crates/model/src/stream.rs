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

/// Streaming reader over one stream's slices.
///
/// [`CtxStreams::stream`] returns the whole stream as one `Vec<u8>`; these
/// captures have streams of 80–160 MB (and `read_concat` held the concatenation
/// *plus* each slice), which is the largest single allocation in a full export.
/// This reader instead hands out record-aligned chunks of roughly
/// [`CHUNK_BYTES`], so a decoder sees the same bytes with only one slice resident.
///
/// Alignment mirrors `read_concat` exactly:
/// - the leading `total % trunk` bytes (a partial first record left by log
///   rotation) are dropped once, before anything is yielded;
/// - fixed-record streams only ever yield whole records, carrying the remainder
///   across slice boundaries;
/// - text streams (`Trunk::Any`) yield up to the last newline, so a line-oriented
///   decoder never sees a split line (the final partial line is flushed at EOF,
///   matching `str::lines`).
pub struct StreamReader {
    files: std::vec::IntoIter<PathBuf>,
    trunk: usize,
    /// Bytes still to drop from the front (the initial partial record).
    skip: usize,
    /// Bytes at the front already handed out; dropped on the next call.
    spent: usize,
    buffer: Vec<u8>,
    finished: bool,
}

/// Target chunk size: large enough that per-chunk overhead is irrelevant, small
/// enough that a chunk plus a slice stays well inside L3.
pub const CHUNK_BYTES: usize = 1 << 21;

/// Refuse to buffer more than this while waiting for a record boundary. Only a
/// pathological input (a "line" larger than this, or a corrupt record size) can
/// reach it; without the guard such an input would grow memory without bound.
const MAX_BUFFER: usize = 8 * CHUNK_BYTES;

impl StreamReader {
    /// Open a stream without reading it. An absent stream yields no chunks,
    /// matching [`CtxStreams::stream`].
    pub fn open(ctx: &Ctx, stream: Stream) -> Result<Self> {
        let dir = match stream.location() {
            Location::Device => ctx.device_data_dir()?,
            Location::Host => ctx.host_data_dir(),
        };
        let version = ctx
            .get::<crate::ctx::DeviceInfo>()
            .map(version_of)
            .unwrap_or("1.0");
        Self::open_in(&dir, stream.prefix(), stream.trunk(version).size())
    }

    fn open_in(dir: &Path, prefix: &str, trunk: usize) -> Result<Self> {
        let files = files_with_prefix(dir, &[prefix]);
        let total: usize = files
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|meta| meta.len() as usize)
            .sum();
        Ok(Self {
            files: files.into_iter(),
            trunk,
            skip: if trunk > 1 { total % trunk } else { 0 },
            spent: 0,
            buffer: Vec::with_capacity(CHUNK_BYTES + trunk.max(4096)),
            finished: false,
        })
    }

    /// Next record-aligned chunk, or `None` at the end of the stream.
    ///
    /// The borrow is tied to `&mut self`: the caller must finish with a chunk
    /// before asking for the next one, which is the point — only one chunk is
    /// ever alive. Each call **consumes** the previous chunk.
    pub fn next_chunk(&mut self) -> Result<Option<&[u8]>> {
        // Retire the previously yielded bytes.
        if self.spent > 0 {
            self.buffer.drain(..self.spent);
            self.spent = 0;
        }
        loop {
            // Drop the leading partial record.
            if self.skip > 0 {
                let take = self.skip.min(self.buffer.len());
                self.buffer.drain(..take);
                self.skip -= take;
                if self.skip > 0 {
                    self.fill()?;
                    continue;
                }
            }
            let usable = self.usable();
            if usable > 0 {
                self.spent = usable;
                return Ok(Some(&self.buffer[..usable]));
            }
            if self.finished {
                // Flush a trailing partial record / final unterminated line.
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                if self.trunk > 1 {
                    // A whole-record stream is trimmed to whole records, so this
                    // can only be a corrupt short tail: drop it like `read_concat`.
                    self.buffer.clear();
                    return Ok(None);
                }
                self.spent = self.buffer.len();
                return Ok(Some(&self.buffer[..self.spent]));
            }
            self.fill()?;
        }
    }

    /// How many bytes at the front of `buffer` form whole records.
    fn usable(&self) -> usize {
        if self.trunk <= 1 {
            // Text: stop at the last newline so no line is split.
            match self.buffer.iter().rposition(|byte| *byte == b'\n') {
                Some(index) => index + 1,
                None => 0,
            }
        } else {
            self.buffer.len() / self.trunk * self.trunk
        }
    }

    /// Append the next slice, or mark the stream finished.
    fn fill(&mut self) -> Result<()> {
        match self.files.next() {
            Some(path) => {
                let mut bytes = crate::errors::read(&path)?;
                self.buffer.append(&mut bytes);
            }
            None => self.finished = true,
        }
        // Safety valve: if nothing is consumable but the buffer keeps growing,
        // hand out what we have instead of buffering forever.
        if self.buffer.len() >= MAX_BUFFER && self.usable() == 0 {
            self.spent = self.buffer.len();
            return Ok(());
        }
        Ok(())
    }

    /// Report how many bytes are currently buffered (tests and diagnostics).
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

/// Decode a record stream chunk by chunk without materialising it.
///
/// `decode` is called once per aligned chunk; decoders are written against
/// `&[u8]` and use `as_chunks::<N>()`, so they work unchanged.
pub fn for_each_chunk(
    mut reader: StreamReader,
    mut decode: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    while let Some(chunk) = reader.next_chunk()? {
        decode(chunk)?;
    }
    Ok(())
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

// ---------------------------------------------------------------------------
// Typed stream registry
// ---------------------------------------------------------------------------

/// How a stream is framed into records.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Trunk {
    /// Trim the file to a whole number of `n`-byte records (msprof drops the
    /// leading partial record left behind by file rotation).
    Fixed(usize),
    /// No framing: text or line-oriented streams.
    Any,
}

impl Trunk {
    fn size(self) -> usize {
        match self {
            Trunk::Fixed(n) => n,
            Trunk::Any => 1,
        }
    }
}

/// Which directory a stream lives in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Location {
    /// `<device>/data`
    Device,
    /// `<root>/host/data`
    Host,
}

/// Every raw stream mspark reads.
///
/// The prefix, the directory and the record size live **here and nowhere else**:
/// a node asks for `Stream::StarsSoc` and cannot disagree with the file it
/// actually needs. Previously each of the 25 read sites spelled out
/// `read_stream(ctx, "stars_soc.", STARS_SOC_TRUNK)`, so a typo produced an
/// empty table instead of a compile error.
///
/// Record sizes stay public because the decoders slice the returned bytes with
/// `as_chunks::<N>()`; they are imported from here rather than redefined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    // --- device streams ---
    StarsSoc,
    FftsProfile,
    StarsSocProfile,
    TsTrack,
    LpmFreq,
    Tscpu,
    Qos,
    L2Cache,
    Llc,
    Hbm,
    Pcie,
    NpuMem,
    NpuModuleMem,
    Dvpp,
    Hccs,
    Nic,
    Roce,
    // --- host streams ---
    ApiEvent,
    HcclInfo,
    HcclOpInfo,
    NodeBasicInfo,
    TaskTrack,
    TensorInfo,
    HostCpuData,
    HostMemData,
    HostDiskData,
}

impl Stream {
    /// Every stream, for tests and tooling.
    pub const ALL: &'static [Stream] = &[
        Stream::StarsSoc,
        Stream::FftsProfile,
        Stream::StarsSocProfile,
        Stream::TsTrack,
        Stream::LpmFreq,
        Stream::Tscpu,
        Stream::Qos,
        Stream::L2Cache,
        Stream::Llc,
        Stream::Hbm,
        Stream::Pcie,
        Stream::NpuMem,
        Stream::NpuModuleMem,
        Stream::Dvpp,
        Stream::Hccs,
        Stream::Nic,
        Stream::Roce,
        Stream::ApiEvent,
        Stream::HcclInfo,
        Stream::HcclOpInfo,
        Stream::NodeBasicInfo,
        Stream::TaskTrack,
        Stream::TensorInfo,
        Stream::HostCpuData,
        Stream::HostMemData,
        Stream::HostDiskData,
    ];

    /// File-name prefix (slice files are `<prefix>.<suffix>` / `<prefix>_<n>`).
    pub const fn prefix(self) -> &'static str {
        match self {
            Stream::StarsSoc => "stars_soc.",
            Stream::FftsProfile => "ffts_profile.",
            Stream::StarsSocProfile => "stars_soc_profile.",
            Stream::TsTrack => "ts_track.",
            Stream::LpmFreq => "lpmFreqConv.",
            Stream::Tscpu => "tscpu.",
            Stream::Qos => "qos.",
            Stream::L2Cache => "l2_cache.",
            Stream::Llc => "llc.",
            Stream::Hbm => "hbm.",
            Stream::Pcie => "pcie.",
            Stream::NpuMem => "npu_mem.",
            Stream::NpuModuleMem => "npu_module_mem.",
            Stream::Dvpp => "dvpp.data.",
            Stream::Hccs => "hccs.",
            Stream::Nic => "nic.",
            Stream::Roce => "roce.",
            Stream::ApiEvent => "aging.api_event.data.",
            Stream::HcclInfo => "unaging.additional.hccl_info.",
            Stream::HcclOpInfo => "aging.compact.hccl_op_info.",
            Stream::NodeBasicInfo => "aging.compact.node_basic_info.",
            Stream::TaskTrack => "aging.compact.task_track.",
            Stream::TensorInfo => "aging.additional.tensor_info.",
            Stream::HostCpuData => "host_cpu.data.",
            Stream::HostMemData => "host_mem.data.",
            Stream::HostDiskData => "host_disk.data.",
        }
    }

    pub const fn location(self) -> Location {
        match self {
            Stream::ApiEvent
            | Stream::HcclInfo
            | Stream::HcclOpInfo
            | Stream::NodeBasicInfo
            | Stream::TaskTrack
            | Stream::TensorInfo
            | Stream::HostCpuData
            | Stream::HostMemData
            | Stream::HostDiskData => Location::Host,
            _ => Location::Device,
        }
    }

    /// Record framing. `version` is `info.json:version`, which selects the
    /// layout of the versioned streams (msprof's `VersionedStructParser`).
    pub fn trunk(self, version: &str) -> Trunk {
        let v2 = version.starts_with('2');
        match self {
            Stream::StarsSoc => Trunk::Fixed(STARS_SOC_TRUNK),
            Stream::FftsProfile => Trunk::Fixed(FFTS_PROFILE_TRUNK),
            Stream::StarsSocProfile => Trunk::Fixed(STARS_SOC_PROFILE_TRUNK),
            Stream::TsTrack => Trunk::Fixed(TS_TRACK_TRUNK),
            Stream::LpmFreq => Trunk::Fixed(LPM_TRUNK),
            Stream::Tscpu => Trunk::Fixed(TSCPU_TRUNK),
            Stream::Qos => Trunk::Fixed(QOS_TRUNK),
            Stream::L2Cache => Trunk::Fixed(L2_CACHE_TRUNK),
            Stream::Llc => Trunk::Fixed(LLC_TRUNK),
            Stream::Hbm if v2 => Trunk::Fixed(HBM_TRUNK_V2),
            Stream::Hbm => Trunk::Fixed(HBM_TRUNK),
            Stream::Pcie => Trunk::Fixed(PCIE_TRUNK),
            Stream::Hccs => Trunk::Any,
            Stream::NpuMem => Trunk::Fixed(NPU_MEM_TRUNK),
            Stream::NpuModuleMem => Trunk::Fixed(NPU_MODULE_MEM_TRUNK),
            Stream::ApiEvent => Trunk::Fixed(API_EVENT_TRUNK),
            Stream::HcclInfo => Trunk::Fixed(ADDITIONAL_TRUNK),
            Stream::HcclOpInfo => Trunk::Fixed(COMPACT_TRUNK),
            Stream::NodeBasicInfo => Trunk::Fixed(COMPACT_TRUNK),
            Stream::TaskTrack => Trunk::Fixed(COMPACT_TRUNK),
            Stream::TensorInfo => Trunk::Fixed(COMPACT_TRUNK),
            Stream::Dvpp
            | Stream::Nic
            | Stream::Roce
            | Stream::HostCpuData
            | Stream::HostMemData
            | Stream::HostDiskData => Trunk::Any,
        }
    }
}

/// Record sizes, one definition per stream, referenced by [`Stream::trunk`] and
/// by the decoders (`as_chunks::<STARS_SOC_TRUNK>()`).
pub const STARS_SOC_TRUNK: usize = 64;
pub const FFTS_PROFILE_TRUNK: usize = 128;
pub const STARS_SOC_PROFILE_TRUNK: usize = 64;
pub const TS_TRACK_TRUNK: usize = 40;
pub const LPM_TRUNK: usize = 888;
pub const TSCPU_TRUNK: usize = 304;
pub const QOS_TRUNK: usize = 64;
pub const L2_CACHE_TRUNK: usize = 72;
pub const LLC_TRUNK: usize = 24;
pub const HBM_TRUNK: usize = 24;
pub const HBM_TRUNK_V2: usize = 32;
pub const PCIE_TRUNK: usize = 96;
pub const NPU_MEM_TRUNK: usize = 32;
pub const NPU_MODULE_MEM_TRUNK: usize = 24;
pub const API_EVENT_TRUNK: usize = 40;

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

/// Stream access on `Ctx`.
///
/// A trait rather than an inherent `impl` because `Ctx` lives in `mspark-df`;
/// call sites read exactly like an inherent method (`ctx.stream(…)`).
pub trait CtxStreams {
    /// Bytes of one stream: slice files concatenated in order, trimmed to whole
    /// records.
    ///
    /// This is the only way nodes obtain raw input; they never build paths or
    /// decide a record size. The bytes are **not** cached — the parsed table is
    /// what `Store` memoises, and the streams reach hundreds of MB.
    fn stream(&self, stream: Stream) -> anyhow::Result<Vec<u8>>;
    fn text(&self, stream: Stream) -> anyhow::Result<String>;
}

impl CtxStreams for Ctx {
    fn stream(&self, stream: Stream) -> anyhow::Result<Vec<u8>> {
        let data_dir = match stream.location() {
            Location::Device => self.device_data_dir()?,
            Location::Host => self.host_data_dir(),
        };
        let version = self
            .get::<crate::ctx::DeviceInfo>()
            .map(version_of)
            .unwrap_or("1.0");
        let bytes = read_concat(&data_dir, stream.prefix(), stream.trunk(version).size())?;
        Ok(bytes)
    }

    fn text(&self, stream: Stream) -> anyhow::Result<String> {
        let bytes = self.stream(stream)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

/// `info.json:version`, defaulting to the v1 layouts.
fn version_of(info: &crate::ctx::DeviceInfo) -> &str {
    info.collection_version.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two streams in the same directory must not share a prefix: a copy-paste
    /// slip here would make two nodes read the same file and produce plausible
    /// but wrong tables.
    #[test]
    fn prefixes_are_unique_per_directory() {
        for (i, a) in Stream::ALL.iter().enumerate() {
            for b in &Stream::ALL[i + 1..] {
                if a.location() == b.location() {
                    assert_ne!(
                        a.prefix(),
                        b.prefix(),
                        "{a:?} and {b:?} share a prefix in {:?}",
                        a.location()
                    );
                }
            }
        }
    }

    /// Every stream must have a prefix, and a prefix must not be a prefix of
    /// another in the same directory (it would match the wrong slice files).
    #[test]
    fn prefixes_are_disjoint() {
        for (i, a) in Stream::ALL.iter().enumerate() {
            assert!(!a.prefix().is_empty(), "{a:?} has no prefix");
            for b in &Stream::ALL[i + 1..] {
                if a.location() == b.location() {
                    assert!(
                        !a.prefix().starts_with(b.prefix()) && !b.prefix().starts_with(a.prefix()),
                        "{a:?} ({}) and {b:?} ({}) overlap",
                        a.prefix(),
                        b.prefix()
                    );
                }
            }
        }
    }

    /// Chunks must be record-aligned across slice boundaries, and the leading
    /// partial record must be dropped exactly like `read_concat` does.
    #[test]
    fn stream_reader_matches_read_concat() {
        let dir = std::env::temp_dir().join(format!("mspark-reader-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // 3 slices of 8-byte records, with a 3-byte fragment at the very front.
        let records: Vec<Vec<u8>> = (0..30u8).map(|i| vec![i; 8]).collect();
        let mut all = vec![1u8, 2, 3];
        for record in &records {
            all.extend_from_slice(record);
        }
        for (index, slice) in all.chunks(64).enumerate() {
            std::fs::write(dir.join(format!("test.data.7.slice_{index}")), slice).unwrap();
        }

        let expected = read_concat(&dir, "test.data", 8).unwrap();
        let mut reader = StreamReader::open_in(&dir, "test.data", 8).unwrap();
        let mut collected = Vec::new();
        // Bounded on purpose: a reader that never consumes its chunk would spin
        // here forever (that regression is what this test exists to catch).
        for round in 0.. {
            assert!(round < 1_000, "reader did not terminate");
            let Some(chunk) = reader.next_chunk().unwrap() else {
                break;
            };
            assert_eq!(chunk.len() % 8, 0, "chunk must hold whole records");
            collected.extend_from_slice(chunk);
            assert!(
                reader.buffered() < 1 << 20,
                "buffer must not grow without bound"
            );
        }
        assert_eq!(collected, expected);
        assert_eq!(expected.len() % 8, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Text streams must never hand out a partial line.
    #[test]
    fn stream_reader_does_not_split_lines() {
        let dir = std::env::temp_dir().join(format!("mspark-reader-lines-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let body = "a:1\nb:2\nc:3\npartial";
        for (index, slice) in body.as_bytes().chunks(5).enumerate() {
            std::fs::write(dir.join(format!("kv.data.7.slice_{index}")), slice).unwrap();
        }
        let mut reader = StreamReader::open_in(&dir, "kv.data", 1).unwrap();
        let mut lines = Vec::new();
        for round in 0.. {
            assert!(round < 1_000, "reader did not terminate");
            let Some(chunk) = reader.next_chunk().unwrap() else {
                break;
            };
            for line in chunk.split(|byte| *byte == b'\n') {
                if !line.is_empty() {
                    lines.push(String::from_utf8_lossy(line).into_owned());
                }
            }
        }
        assert_eq!(lines, ["a:1", "b:2", "c:3", "partial"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// msprof writes a `<name>.done` marker next to every finished file, with
    /// contents like `filesize:178`. Reading one as data yields either an empty
    /// table or a parse error, so slice listing must skip them.
    #[test]
    fn slice_listing_skips_done_markers() {
        let dir = std::env::temp_dir().join(format!("mspark-slice-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in [
            "start_info.7",
            "start_info.7.done",
            "start_info.8",
            "start_info.8.done",
            "stars_soc.data.7.slice_0",
            "stars_soc.data.7.slice_0.done",
            "stars_soc.data.7.slice_1",
            "stars_soc.data.7.slice_10",
            "stars_soc.data.7.slice_2",
            "unaging.additional.hccl_info.slice_0.complete",
        ] {
            std::fs::write(dir.join(name), b"x").unwrap();
        }
        let starts = files_with_prefix(&dir, &["start_info"]);
        let names: Vec<String> = starts
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["start_info.7", "start_info.8"]);

        let slices = files_with_prefix(&dir, &["stars_soc.data"]);
        let names: Vec<String> = slices
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        // Numeric slice order, no `.done`.
        assert_eq!(
            names,
            [
                "stars_soc.data.7.slice_0",
                "stars_soc.data.7.slice_1",
                "stars_soc.data.7.slice_2",
                "stars_soc.data.7.slice_10",
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The versioned streams must switch record size with `info.json:version`.
    #[test]
    fn versioned_trunks() {
        assert_eq!(Stream::Hbm.trunk("1.0"), Trunk::Fixed(HBM_TRUNK));
        assert_eq!(Stream::Hbm.trunk("2.0"), Trunk::Fixed(HBM_TRUNK_V2));
        assert_eq!(Stream::StarsSoc.trunk("2.0"), Trunk::Fixed(STARS_SOC_TRUNK));
        // Text streams carry no framing.
        assert_eq!(Stream::Nic.trunk("1.0"), Trunk::Any);
        assert_eq!(Stream::HostCpuData.trunk("1.0"), Trunk::Any);
    }
}

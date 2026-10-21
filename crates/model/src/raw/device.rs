//! Device stream decoders: `stars_soc`, `ffts_profile`, `ts_track`, `lpmFreqConv`.

use anyhow::Result;
use mspark_df::Ctx;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use super::{cast, read_concat};

pub const STARS_SOC_TRUNK: usize = 64;
pub const FFTS_PROFILE_TRUNK: usize = 128;
pub const TS_TRACK_TRUNK: usize = 40;
pub const LPM_TRUNK: usize = 888;

/// `PARSER_ITEM_*` values (6-bit `funcType` for the stars_soc / ffts_profile streams).
pub const FUNC_ACSQ_LOG_START: u16 = 0b000000;
pub const FUNC_ACSQ_LOG_END: u16 = 0b000001;
pub const FUNC_FFTS_PLUS_LOG_START: u16 = 0b100010;
pub const FUNC_FFTS_PLUS_LOG_END: u16 = 0b100011;
pub const FUNC_CONTEXT_PMU: u16 = 0b101000;
/// 8-bit `funcType` for the ts_track stream (`PARSER_ITEM_TASK_FLIP`).
pub const FUNC_TASK_FLIP: u16 = 0x0e;

// --- `StarsCommon` id rules (parser_item/stars_common.h), expandStatus == 0 ---
const STREAM_LOW_OPERATOR: u16 = 1 << 11;
const STREAM_JUDGE_BIT12_OPERATOR: u16 = 0x1000;
const STREAM_JUDGE_BIT13_OPERATOR: u16 = 0x2000;
const STREAM_JUDGE_BIT15_OPERATOR: u16 = 0x8000;
const TASK_LOW_OPERATOR: u16 = 0x1fff;
const STREAM_HIGH_OPERATOR: u16 = 0xe000;
const COMMON_LOW_OPERATOR: u16 = 0x0fff;
const COMMON_HIGH_OPERATOR: u16 = 0xf000;
const EXPANDING_LOW_OPERATOR: u16 = 0x7fff;
const PLACE_HOLDER_SQE: u16 = 3;
const EVENT_RECORD_SQE: u16 = 4;

/// Normalise a raw stream id exactly like `StarsCommon::GetStreamId`.
pub fn normalize_stream_id(stream_id: u16, task_id: u16, expand: u16, sqe_type: u16) -> u16 {
    if expand == 1 {
        if sqe_type == PLACE_HOLDER_SQE || sqe_type == EVENT_RECORD_SQE {
            return stream_id & EXPANDING_LOW_OPERATOR;
        }
        if stream_id & STREAM_JUDGE_BIT15_OPERATOR != 0 {
            task_id & EXPANDING_LOW_OPERATOR
        } else {
            stream_id & EXPANDING_LOW_OPERATOR
        }
    } else {
        if stream_id & STREAM_JUDGE_BIT12_OPERATOR != 0 {
            return stream_id % STREAM_LOW_OPERATOR;
        }
        let effective = if stream_id & STREAM_JUDGE_BIT13_OPERATOR != 0 {
            task_id & COMMON_LOW_OPERATOR
        } else {
            stream_id
        };
        effective % STREAM_LOW_OPERATOR
    }
}

/// Normalise a raw task id exactly like `StarsCommon::GetTaskId`.
pub fn normalize_task_id(stream_id: u16, task_id: u16, expand: u16, sqe_type: u16) -> u16 {
    if expand == 1 {
        if sqe_type == PLACE_HOLDER_SQE || sqe_type == EVENT_RECORD_SQE {
            return task_id;
        }
        if stream_id & STREAM_JUDGE_BIT15_OPERATOR != 0 {
            (task_id & STREAM_JUDGE_BIT15_OPERATOR) | (stream_id & EXPANDING_LOW_OPERATOR)
        } else {
            task_id
        }
    } else if stream_id & STREAM_JUDGE_BIT12_OPERATOR != 0 {
        (task_id & TASK_LOW_OPERATOR) | (stream_id & STREAM_HIGH_OPERATOR)
    } else if stream_id & STREAM_JUDGE_BIT13_OPERATOR != 0 {
        (stream_id & COMMON_LOW_OPERATOR) | (task_id & COMMON_HIGH_OPERATOR)
    } else {
        task_id
    }
}

// ---------------------------------------------------------------------------
// Raw packed layouts
// ---------------------------------------------------------------------------

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct AcsqLog {
    w0: U16, // funcType:6 | cnt:4 | taskType:6
    _resv2: U16,
    stream_id: U16,
    task_id: U16,
    timestamp: U64,
    _resv4: U16,
    _resv5: U16,
    _resv6: [U32; 11],
}
const _: () = assert!(core::mem::size_of::<AcsqLog>() == STARS_SOC_TRUNK);

impl AcsqLog {
    fn func_type(&self) -> u16 {
        self.w0.get() & 0x3f
    }
    fn task_type(&self) -> u16 {
        (self.w0.get() >> 10) & 0x3f
    }
    fn stream_id(&self) -> u16 {
        self.stream_id.get()
    }
    fn task_id(&self) -> u16 {
        self.task_id.get()
    }
    fn timestamp(&self) -> u64 {
        self.timestamp.get()
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct FftsPlusLog {
    w0: U16, // funcType:6 | cnt:4 | taskType:6
    _resv2: U16,
    stream_id: U16,
    task_id: U16,
    timestamp: U64,
    w4: U16, // subTaskType:8 | resv3:8
    sub_task_id: U16,
    w6: U16, // resv4:13 | fftsType:3
    thread_id: U16,
    _resv5: [U32; 10],
}
const _: () = assert!(core::mem::size_of::<FftsPlusLog>() == STARS_SOC_TRUNK);

impl FftsPlusLog {
    fn func_type(&self) -> u16 {
        self.w0.get() & 0x3f
    }
    fn task_type(&self) -> u16 {
        (self.w0.get() >> 10) & 0x3f
    }
    fn sub_task_type(&self) -> u16 {
        self.w4.get() & 0xff
    }
    fn ffts_type(&self) -> u16 {
        (self.w6.get() >> 13) & 0x7
    }
    fn stream_id(&self) -> u16 {
        self.stream_id.get()
    }
    fn task_id(&self) -> u16 {
        self.task_id.get()
    }
    fn timestamp(&self) -> u64 {
        self.timestamp.get()
    }
    fn thread_id(&self) -> u16 {
        self.thread_id.get()
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct ContextPmu {
    w0: U16, // funcType:6 | cnt:4 | resv1:6
    _resv2: U16,
    stream_id: U16,
    task_id: U16,
    _resv4: U64,
    w4: U16, // subTaskType:8 | resv5:2 | ovFlag:1 | resv6:5
    sub_task_id: U16,
    w6: U16, // resv7:13 | fftsType:3
    _resv8: U16,
    _resv9: U64,
    total_cycle: U64,
    _resv10: U64,
    pmu_list: [U64; 8],
    time_list: [U64; 2],
}
const _: () = assert!(core::mem::size_of::<ContextPmu>() == FFTS_PROFILE_TRUNK);

impl ContextPmu {
    fn func_type(&self) -> u16 {
        self.w0.get() & 0x3f
    }
    fn stream_id(&self) -> u16 {
        self.stream_id.get()
    }
    fn task_id(&self) -> u16 {
        self.task_id.get()
    }
    fn sub_task_id(&self) -> u16 {
        self.sub_task_id.get()
    }
    fn sub_task_type(&self) -> u16 {
        self.w4.get() & 0xff
    }
    fn ov_flag(&self) -> bool {
        (self.w4.get() >> 10) & 1 != 0
    }
    fn ffts_type(&self) -> u16 {
        (self.w6.get() >> 13) & 0x7
    }
    fn total_cycle(&self) -> u64 {
        self.total_cycle.get()
    }
    fn pmu_list(&self) -> [u64; 8] {
        core::array::from_fn(|i| self.pmu_list[i].get())
    }
    fn time_list(&self) -> [u64; 2] {
        core::array::from_fn(|i| self.time_list[i].get())
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct TaskFlip {
    _resv1: u8,
    func_type: u8,
    _resv2: U16,
    _resv3: U32,
    timestamp: U64,
    stream_id: U16,
    flip_num: U16,
    _resv5: U16,
    task_id: U16,
    _resv6: [u8; 16],
}
const _: () = assert!(core::mem::size_of::<TaskFlip>() == TS_TRACK_TRUNK);

impl TaskFlip {
    fn func_type(&self) -> u16 {
        self.func_type as u16
    }
    fn timestamp(&self) -> u64 {
        self.timestamp.get()
    }
    fn stream_id(&self) -> u16 {
        self.stream_id.get()
    }
    fn flip_num(&self) -> u16 {
        self.flip_num.get()
    }
    fn task_id(&self) -> u16 {
        self.task_id.get()
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct LpmValue {
    sys_cnt: U64,
    value: U32,
    _reserved: U32,
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct LpmInfoRaw {
    count: U32,
    kind: U32,
    values: [LpmValue; 55],
}
const _: () = assert!(core::mem::size_of::<LpmInfoRaw>() == LPM_TRUNK);

// ---------------------------------------------------------------------------
// Decoded rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct Acsq {
    pub is_end: bool,
    pub task_type: u16,
    pub timestamp: u64,
    pub stream_id: u16,
    pub task_id: u16,
}

#[derive(Debug, Clone, Copy)]
pub struct FftsPlus {
    pub is_end: bool,
    pub timestamp: u64,
    pub stream_id: u16,
    pub task_id: u16,
    pub sub_task_id: u16,
    pub sub_task_type: u16,
    pub ffts_type: u16,
    pub thread_id: u16,
}

/// One row of the `stars_soc` log stream.
#[derive(Debug, Clone, Copy)]
pub enum StarsSocEntry {
    Acsq(Acsq),
    FftsPlus(FftsPlus),
}

impl StarsSocEntry {
    pub fn timestamp(&self) -> u64 {
        match self {
            Self::Acsq(a) => a.timestamp,
            Self::FftsPlus(f) => f.timestamp,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PmuData {
    pub stream_id: u16,
    pub task_id: u16,
    pub sub_task_id: u16,
    pub sub_task_type: u16,
    pub ov_flag: bool,
    pub ffts_type: u16,
    pub total_cycle: u64,
    pub pmu_list: [u64; 8],
    pub time_list: [u64; 2],
}

#[derive(Debug, Clone, Copy)]
pub struct TrackFlip {
    pub timestamp: u64,
    pub stream_id: u16,
    pub flip_num: u16,
    pub task_id: u16,
}

#[derive(Debug, Clone, Copy)]
pub struct LpmSample {
    pub sys_cnt: u64,
    pub value: u32,
}

#[derive(Debug, Clone)]
pub struct LpmInfo {
    pub count: u32,
    pub kind: u32,
    pub samples: Vec<LpmSample>,
}

/// Read all slice files for `prefix` from `<device>/data`, concatenate them, and
/// trim the leading partial chunk exactly like `Parser::ReadDataEntry`.
///
/// A missing stream is *not* an error: absence just yields an empty table.
pub fn read_stream(ctx: &Ctx, prefix: &str, trunk: usize) -> Result<Vec<u8>> {
    read_concat(&ctx.device_data_dir()?, prefix, trunk)
}

/// Decode the 64-byte `stars_soc` stream. Returns the decoded rows plus the full
/// 6-bit `funcType` histogram (so unsupported item types are visible, not silently dropped).
pub fn decode_stars_soc(bytes: &[u8], expand: u16) -> Result<(Vec<StarsSocEntry>, [u32; 64])> {
    let mut rows = Vec::with_capacity(bytes.len() / STARS_SOC_TRUNK);
    let mut hist = [0u32; 64];
    for chunk in bytes.as_chunks::<STARS_SOC_TRUNK>().0 {
        let func = u16::from_le_bytes([chunk[0], chunk[1]]) & 0x3f;
        hist[func as usize] = hist[func as usize].saturating_add(1);
        match func {
            FUNC_ACSQ_LOG_START | FUNC_ACSQ_LOG_END => {
                let r = AcsqLog::ref_from_bytes(chunk).map_err(cast)?;
                debug_assert_eq!(r.func_type(), func);
                rows.push(StarsSocEntry::Acsq(Acsq {
                    is_end: func != FUNC_ACSQ_LOG_START,
                    task_type: r.task_type(),
                    timestamp: r.timestamp(),
                    stream_id: normalize_stream_id(
                        r.stream_id(),
                        r.task_id(),
                        expand,
                        r.task_type(),
                    ),
                    task_id: normalize_task_id(r.stream_id(), r.task_id(), expand, r.task_type()),
                }));
            }
            FUNC_FFTS_PLUS_LOG_START | FUNC_FFTS_PLUS_LOG_END => {
                let r = FftsPlusLog::ref_from_bytes(chunk).map_err(cast)?;
                debug_assert_eq!(r.func_type(), func);
                rows.push(StarsSocEntry::FftsPlus(FftsPlus {
                    is_end: func != FUNC_FFTS_PLUS_LOG_START,
                    timestamp: r.timestamp(),
                    stream_id: normalize_stream_id(
                        r.stream_id(),
                        r.task_id(),
                        expand,
                        r.task_type(),
                    ),
                    task_id: normalize_task_id(r.stream_id(), r.task_id(), expand, r.task_type()),
                    sub_task_id: r.sub_task_id.get(),
                    sub_task_type: r.sub_task_type(),
                    ffts_type: r.ffts_type(),
                    thread_id: r.thread_id(),
                }));
            }
            _ => {}
        }
    }
    Ok((rows, hist))
}

/// Decode the 128-byte `ffts_profile` (context PMU) stream.
pub fn decode_ffts_profile(bytes: &[u8], expand: u16) -> Result<(Vec<PmuData>, [u32; 64])> {
    let mut rows = Vec::with_capacity(bytes.len() / FFTS_PROFILE_TRUNK);
    let mut hist = [0u32; 64];
    for chunk in bytes.as_chunks::<FFTS_PROFILE_TRUNK>().0 {
        let func = u16::from_le_bytes([chunk[0], chunk[1]]) & 0x3f;
        hist[func as usize] = hist[func as usize].saturating_add(1);
        if func != FUNC_CONTEXT_PMU {
            continue;
        }
        let r = ContextPmu::ref_from_bytes(chunk).map_err(cast)?;
        debug_assert_eq!(r.func_type(), func);
        rows.push(PmuData {
            stream_id: normalize_stream_id(r.stream_id(), r.task_id(), expand, 0),
            task_id: normalize_task_id(r.stream_id(), r.task_id(), expand, 0),
            sub_task_id: r.sub_task_id(),
            sub_task_type: r.sub_task_type(),
            ov_flag: r.ov_flag(),
            ffts_type: r.ffts_type(),
            total_cycle: r.total_cycle(),
            pmu_list: r.pmu_list(),
            time_list: r.time_list(),
        });
    }
    Ok((rows, hist))
}

/// Decode the 40-byte `ts_track` stream (8-bit `funcType`).
pub fn decode_ts_track(bytes: &[u8]) -> Result<(Vec<TrackFlip>, [u32; 256])> {
    let mut rows = Vec::with_capacity(bytes.len() / TS_TRACK_TRUNK);
    let mut hist = [0u32; 256];
    for chunk in bytes.as_chunks::<TS_TRACK_TRUNK>().0 {
        let func = chunk[1] as u16;
        hist[func as usize] = hist[func as usize].saturating_add(1);
        if func != FUNC_TASK_FLIP {
            continue;
        }
        let r = TaskFlip::ref_from_bytes(chunk).map_err(cast)?;
        debug_assert_eq!(r.func_type(), func);
        rows.push(TrackFlip {
            timestamp: r.timestamp(),
            stream_id: r.stream_id(),
            flip_num: r.flip_num(),
            task_id: r.task_id(),
        });
    }
    Ok((rows, hist))
}

/// Decode the single 888-byte `lpmFreqConv` record.
pub fn decode_lpm(bytes: &[u8]) -> Result<LpmInfo> {
    if bytes.len() < LPM_TRUNK {
        return Ok(LpmInfo {
            count: 0,
            kind: 0,
            samples: Vec::new(),
        });
    }
    let r = LpmInfoRaw::ref_from_bytes(&bytes[..LPM_TRUNK]).map_err(cast)?;
    let count = r.count.get().min(r.values.len() as u32);
    let samples = (0..count as usize)
        .map(|i| LpmSample {
            sys_cnt: r.values[i].sys_cnt.get(),
            value: r.values[i].value.get(),
        })
        .collect();
    Ok(LpmInfo {
        count: r.count.get(),
        kind: r.kind.get(),
        samples,
    })
}

// ---------------------------------------------------------------------------
// `stars_soc_profile` (64-byte records, dispatched by `funcType`)
// ---------------------------------------------------------------------------

pub const STARS_SOC_PROFILE_TRUNK: usize = 64;
pub const FUNC_SOC_PROFILE_SIO: u16 = 0b011001;
pub const FUNC_SOC_PROFILE_ACC_PMU: u16 = 0b011010;
pub const FUNC_SOC_PROFILE_CHIP_TRANS: u16 = 0b011011;
pub const FUNC_SOC_PROFILE_INTER_SOC: u16 = 0b011100;

#[derive(Debug, Clone, Copy)]
pub enum SocProfileRaw {
    Sio {
        sys_cnt: u64,
        acc_id: u16,
        bandwidth: [u32; 8],
    },
    AccPmu {
        timestamp: u64,
        acc_id: u16,
        bandwidth: [u64; 2],
        ost: [u64; 2],
    },
    ChipTrans {
        sys_cnt: u64,
        acc_type: u16,
        event_id: u16,
        value1: u64,
        value2: u64,
    },
    InterSoc {
        sys_cnt: u64,
        l2_buffer_bw_level: u32,
        mata_bw_level: u32,
    },
}

pub fn decode_stars_soc_profile(bytes: &[u8]) -> Result<Vec<SocProfileRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<STARS_SOC_PROFILE_TRUNK>().0 {
        let u16_at = |o: usize| u16::from_le_bytes([chunk[o], chunk[o + 1]]);
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        let u64_at = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        let w0 = u16_at(0);
        match w0 & 0x3f {
            FUNC_SOC_PROFILE_SIO => out.push(SocProfileRaw::Sio {
                sys_cnt: u64_at(8),
                acc_id: u16_at(18),
                bandwidth: core::array::from_fn(|i| u32_at(32 + i * 4)),
            }),
            FUNC_SOC_PROFILE_ACC_PMU => out.push(SocProfileRaw::AccPmu {
                timestamp: u64_at(8),
                acc_id: u16_at(18),
                bandwidth: [u64_at(32), u64_at(40)],
                ost: [u64_at(48), u64_at(56)],
            }),
            FUNC_SOC_PROFILE_CHIP_TRANS => out.push(SocProfileRaw::ChipTrans {
                sys_cnt: u64_at(8),
                acc_type: (w0 >> 10) & 0x3f,
                event_id: u16_at(18) & 0x3f,
                value1: u64_at(32),
                value2: u64_at(40),
            }),
            FUNC_SOC_PROFILE_INTER_SOC => {
                let level = u16_at(32);
                out.push(SocProfileRaw::InterSoc {
                    sys_cnt: u64_at(8),
                    l2_buffer_bw_level: (level & 0x7) as u32,
                    mata_bw_level: ((level & 0x7f8) >> 3) as u32,
                });
            }
            _ => {}
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// NPU memory and module memory
// ---------------------------------------------------------------------------

pub const NPU_MEM_TRUNK: usize = 32;
pub const NPU_MODULE_MEM_TRUNK: usize = 24;

#[derive(Debug, Clone, Copy)]
pub struct NpuMemRaw {
    pub event: u32,
    /// V1 records store a `u32` microsecond offset at offset 0.
    pub timestamp_us: u32,
    pub ddr: u64,
    pub hbm: u64,
}

pub fn decode_npu_mem(bytes: &[u8]) -> Result<Vec<NpuMemRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<NPU_MEM_TRUNK>().0 {
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        let u64_at = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        out.push(NpuMemRaw {
            event: u32_at(4),
            timestamp_us: u32_at(0),
            ddr: u64_at(16),
            hbm: u64_at(24),
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
pub struct NpuModuleMemRaw {
    pub module_id: u32,
    pub syscnt: u64,
    pub total_size: u64,
}

pub fn decode_npu_module_mem(bytes: &[u8]) -> Result<Vec<NpuModuleMemRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<NPU_MODULE_MEM_TRUNK>().0 {
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        let u64_at = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        out.push(NpuModuleMemRaw {
            module_id: u32_at(0),
            syscnt: u64_at(8),
            total_size: u64_at(16),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// L2 cache (`<HHHH8Q`, 72-byte records)
// ---------------------------------------------------------------------------

pub const L2_CACHE_TRUNK: usize = 72;

#[derive(Debug, Clone, Copy)]
pub struct L2CacheRaw {
    pub stream_id: u16,
    pub task_id: u16,
    pub events: [u64; 8],
}

pub fn decode_l2_cache(bytes: &[u8]) -> Result<Vec<L2CacheRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<L2_CACHE_TRUNK>().0 {
        let h = |o: usize| u16::from_le_bytes([chunk[o], chunk[o + 1]]);
        let q = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        out.push(L2CacheRaw {
            stream_id: h(2),
            task_id: h(4),
            events: core::array::from_fn(|i| q(8 + i * 8)),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// TS CPU perf records (`L L 20Q L L Q Q 14Q`, 304 bytes, header 0xabababab)
// ---------------------------------------------------------------------------

pub const TSCPU_TRUNK: usize = 304;
pub const TSCPU_HEADER: u32 = 0xabab_abab;

#[derive(Debug, Clone, Copy)]
pub struct TscpuRaw {
    pub pc: u64,
    pub events: [u64; 7],
    pub counts: [u64; 7],
}

pub fn decode_tscpu(bytes: &[u8]) -> Result<Vec<TscpuRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<TSCPU_TRUNK>().0 {
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        if u32_at(0) != TSCPU_HEADER {
            continue;
        }
        let u64_at = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        let pc = u64_at(176);
        let pmu: [u64; 14] = core::array::from_fn(|i| u64_at(192 + i * 8));
        let events: [u64; 7] = core::array::from_fn(|i| pmu[i * 2]);
        if events.iter().any(|&event| event > 0xec) {
            continue;
        }
        let counts: [u64; 7] = core::array::from_fn(|i| pmu[i * 2 + 1]);
        out.push(TscpuRaw { pc, events, counts });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// QoS bandwidth (`QosRawRecord`, 64 bytes)
// ---------------------------------------------------------------------------

pub const QOS_TRUNK: usize = 64;

#[derive(Debug, Clone, Copy)]
pub struct QosRaw {
    pub syscnt: u64,
    pub metrics: [u32; 10],
}

pub fn decode_qos(bytes: &[u8]) -> Result<Vec<QosRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<QOS_TRUNK>().0 {
        let u64_at = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        out.push(QosRaw {
            syscnt: u64_at(16),
            metrics: core::array::from_fn(|i| u32_at(24 + i * 4)),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// HBM bandwidth (`HbmRecord` v1, `QQII`, 24 bytes)
// ---------------------------------------------------------------------------

pub const HBM_TRUNK: usize = 24;

// ---------------------------------------------------------------------------
// PCIe (`PcieRecord`, `Q22I`, 96 bytes)
// ---------------------------------------------------------------------------

pub const PCIE_TRUNK: usize = 96;

// ---------------------------------------------------------------------------
// LLC (`LlcRecord` v1, `QQII`, 24 bytes)
// ---------------------------------------------------------------------------

pub const LLC_TRUNK: usize = 24;

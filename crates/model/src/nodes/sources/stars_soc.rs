use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::stream::STARS_SOC_TRUNK;
use crate::stream::Stream;

impl Node for super::stars_soc::StarsSocEntry {
    const NAME: &'static str = "stars_soc";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let expand = ctx.require::<DeviceInfo>()?.expand_status;
        let mut rows = Vec::new();
        crate::stream::for_each_chunk(
            crate::stream::StreamReader::open(ctx, Stream::StarsSoc)?,
            |chunk| {
                rows.extend(super::stars_soc::decode_stars_soc(chunk, expand)?.0);
                Ok(())
            },
        )?;
        Ok(rows)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// A 64-byte ACSQ log record. `w0` packs
    /// `funcType:6 | cnt:4 | taskType:6` in the low 16 bits.
    fn acsq(func: u16, task_type: u16, stream_id: u16, task_id: u16, timestamp: u64) -> [u8; 64] {
        let mut bytes = [0u8; 64];
        let w0 = func | (task_type << 10);
        bytes[0..2].copy_from_slice(&w0.to_le_bytes());
        bytes[4..6].copy_from_slice(&stream_id.to_le_bytes());
        bytes[6..8].copy_from_slice(&task_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&timestamp.to_le_bytes());
        bytes
    }

    #[test]
    fn decodes_acsq_start_and_end_records() {
        let bytes = [
            acsq(FUNC_ACSQ_LOG_START, 35, 5, 7, 100),
            acsq(FUNC_ACSQ_LOG_END, 35, 5, 7, 250),
        ]
        .concat();
        let (rows, hist) = decode_stars_soc(&bytes, 0).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(hist[FUNC_ACSQ_LOG_START as usize], 1);
        assert_eq!(hist[FUNC_ACSQ_LOG_END as usize], 1);
        let acsq_rows: Vec<Acsq> = rows
            .into_iter()
            .map(|row| match row {
                StarsSocEntry::Acsq(row) => row,
                StarsSocEntry::FftsPlus(_) => panic!("expected ACSQ"),
            })
            .collect();
        assert!(!acsq_rows[0].is_end);
        assert!(acsq_rows[1].is_end);
        assert_eq!(acsq_rows[0].task_type, 35);
        assert_eq!(acsq_rows[0].stream_id, 5);
        assert_eq!(acsq_rows[0].task_id, 7);
        assert_eq!(acsq_rows[1].timestamp, 250);
    }

    #[test]
    fn histogram_keeps_unsupported_item_types() {
        let mut bytes = acsq(0x3f, 0, 0, 0, 0);
        bytes[0] = 0x3f;
        let (rows, hist) = decode_stars_soc(&bytes, 0).unwrap();
        assert!(rows.is_empty(), "unknown func types are not decoded");
        assert_eq!(hist[0x3f], 1, "but they remain visible in the histogram");
    }

    #[test]
    fn expand_status_1_uses_the_dynamic_stream_rules() {
        // Event-record SQE (4) keeps the raw ids; the low 15 bits are masked for
        // other SQE types.
        assert_eq!(normalize_stream_id(0x8005, 0x1234, 1, 4), 0x0005);
        assert_eq!(normalize_task_id(0x8005, 0x1234, 1, 4), 0x1234);
        assert_eq!(normalize_stream_id(0x8005, 0x1234, 1, 0), 0x1234);
        assert_eq!(normalize_task_id(0x8005, 0x1234, 1, 0), 0x0005);
    }

    #[test]
    fn non_expanded_rule_carries_high_task_bits_into_the_stream() {
        // bit 12 set: the stream id is taken modulo 2^11.
        assert_eq!(normalize_stream_id(0x1005, 0x0123, 0, 0), 0x0005);
        assert_eq!(normalize_task_id(0x1005, 0x0123, 0, 0), 0x0123);
        // bit 13 set: task id low 12 bits replace the stream id, and its high
        // bits are supplied by task_id.
        assert_eq!(normalize_stream_id(0x2005, 0x0abc, 0, 0), 0x02bc);
        assert_eq!(normalize_task_id(0x2005, 0x0abc, 0, 0), 0x0005);
        // neither: ids pass through.
        assert_eq!(normalize_stream_id(0x0005, 0x0007, 0, 0), 0x0005);
        assert_eq!(normalize_task_id(0x0005, 0x0007, 0, 0), 0x0007);
    }
}

use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use super::stars_soc::FUNC_TASK_FLIP;
use mspark_df::{Ctx, Node};

impl Node for super::ts_track::TrackFlip {
    const NAME: &'static str = "ts_track";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(ctx, "ts_track.", super::ts_track::TS_TRACK_TRUNK)?;
        Ok(super::ts_track::decode_ts_track(&bytes)?.0)
    }
}

pub const TS_TRACK_TRUNK: usize = 40;

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

#[derive(Debug, Clone, Copy)]
pub struct TrackFlip {
    pub timestamp: u64,
    pub stream_id: u16,
    pub flip_num: u16,
    pub task_id: u16,
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

use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U16, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

use super::stars_soc::FUNC_CONTEXT_PMU;
use super::stars_soc::normalize_stream_id;
use super::stars_soc::normalize_task_id;
use crate::ctx::DeviceInfo;

impl Node for super::ffts_profile::PmuData {
    const NAME: &'static str = "ffts_profile";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(
            ctx,
            "ffts_profile.",
            super::ffts_profile::FFTS_PROFILE_TRUNK,
        )?;
        let expand = ctx.require::<DeviceInfo>()?.expand_status;
        Ok(super::ffts_profile::decode_ffts_profile(&bytes, expand)?.0)
    }
}

pub const FFTS_PROFILE_TRUNK: usize = 128;

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

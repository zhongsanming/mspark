use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::Clock;

#[derive(Debug, Clone, Copy)]
pub struct NpuMemRow {
    pub event: u32,
    pub ddr: u64,
    pub hbm: u64,
    pub ts_ns: u64,
}

impl Node for NpuMemRow {
    const NAME: &'static str = "npu_mem";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(ctx, "npu_mem.", super::npu_mem::NPU_MEM_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in super::npu_mem::decode_npu_mem(&bytes)? {
            // `NpuMemParser::ConvertTimestamp`: raw us * 1000 + host monotonic + wall offset.
            let ts_ns =
                clock.offset_ns + clock.host_mono as i128 + record.timestamp_us as i128 * 1000;
            if ts_ns < begin_ns {
                continue;
            }
            out.push(NpuMemRow {
                event: record.event,
                ddr: record.ddr,
                hbm: record.hbm,
                ts_ns: ts_ns as u64,
            });
        }
        Ok(out)
    }
}

pub const NPU_MEM_TRUNK: usize = 32;

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

pub fn npu_mem_event(event: u32) -> &'static str {
    match event {
        0 => "APP",
        1 => "Device",
        _ => "UNKNOWN",
    }
}

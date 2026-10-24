use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::time::load_host_syscnt_clock;

#[derive(Debug, Clone, Copy)]
pub struct NpuModuleMemRow {
    pub module_id: u32,
    pub ts_ns: u64,
    pub total_size: u64,
}

impl Node for NpuModuleMemRow {
    const NAME: &'static str = "npu_module_mem";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(
            ctx,
            "npu_module_mem.",
            super::npu_module_mem::NPU_MODULE_MEM_TRUNK,
        )?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let info = ctx.require::<DeviceInfo>()?;
        let clock = load_host_syscnt_clock(ctx.device_dir()?, info.host_freq_mhz)?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in super::npu_module_mem::decode_npu_module_mem(&bytes)? {
            let ts_ns = clock.to_ns(record.syscnt);
            if (ts_ns as i128) < begin_ns {
                continue;
            }
            out.push(NpuModuleMemRow {
                module_id: record.module_id,
                ts_ns,
                total_size: record.total_size,
            });
        }
        out.sort_by_key(|row| (row.module_id, row.ts_ns));
        Ok(out)
    }
}

pub const NPU_MODULE_MEM_TRUNK: usize = 24;

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

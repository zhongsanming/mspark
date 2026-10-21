use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::raw;
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
        let bytes = raw::read_stream(ctx, "npu_module_mem.", raw::NPU_MODULE_MEM_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let info = ctx.require::<DeviceInfo>()?;
        let clock = load_host_syscnt_clock(ctx.device_dir()?, info.host_freq_mhz)?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in raw::decode_npu_module_mem(&bytes)? {
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

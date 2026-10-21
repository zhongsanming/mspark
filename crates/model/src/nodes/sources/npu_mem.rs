use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;
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
        let bytes = raw::read_stream(ctx, "npu_mem.", raw::NPU_MEM_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in raw::decode_npu_mem(&bytes)? {
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

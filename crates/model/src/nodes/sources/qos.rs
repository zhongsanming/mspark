use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;
use crate::time::{Clock, fmt_us};

#[derive(Debug, Clone)]
pub struct QosRow {
    pub ts_us: compact_str::CompactString,
    pub values: [u32; 10],
}

impl Node for QosRow {
    const NAME: &'static str = "qos";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "qos.", raw::QOS_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in raw::decode_qos(&bytes)? {
            let ts_ns = clock.to_ns(record.syscnt);
            if (ts_ns as i128) < begin_ns {
                continue;
            }
            out.push(QosRow {
                ts_us: fmt_us(ts_ns, false),
                values: record.metrics,
            });
        }
        Ok(out)
    }
}

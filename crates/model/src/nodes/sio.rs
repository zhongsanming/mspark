use std::collections::BTreeMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::{Clock, fmt_us};

use super::sources::SocProfileRecord;

/// One per-die SIO bandwidth sample (bytes are converted to MB/s from deltas).
#[derive(Debug, Clone)]
pub struct SioSample {
    pub ts_us: compact_str::CompactString,
    pub die: u16,
    pub bandwidth: [u32; 8],
}

impl Node for SioSample {
    const NAME: &'static str = "Sio";
    type Deps = (SocProfileRecord,);

    fn compute(
        ctx: &Ctx,
        (records,): (std::sync::Arc<Vec<SocProfileRecord>>,),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let mut per_die: BTreeMap<u16, Vec<(u64, [u32; 8])>> = BTreeMap::new();
        for record in records.iter() {
            if let super::sources::stars_soc_profile::SocProfileRaw::Sio {
                sys_cnt,
                acc_id,
                bandwidth,
            } = record.0
            {
                per_die
                    .entry(acc_id)
                    .or_default()
                    .push((sys_cnt, bandwidth));
            }
        }
        let begin_ns = clock.begin_us as i128 * 1000;
        let scale = 1e9 / (1024.0 * 1024.0);
        let mut out = Vec::new();
        for (die, mut rows) in per_die {
            rows.sort_by_key(|(sys_cnt, _)| *sys_cnt);
            let mut prev: Option<u64> = None;
            for (sys_cnt, bytes) in rows {
                let ts_ns = clock.to_ns(sys_cnt);
                let bandwidth = match prev {
                    Some(prev_cnt) => {
                        let dt = ts_ns as i128 - clock.to_ns(prev_cnt) as i128;
                        if dt > 0 {
                            core::array::from_fn(|i| (bytes[i] as f64 * scale / dt as f64) as u32)
                        } else {
                            [0u32; 8]
                        }
                    }
                    None => [0u32; 8],
                };
                if (ts_ns as i128) >= begin_ns {
                    out.push(SioSample {
                        ts_us: fmt_us(ts_ns, false),
                        die,
                        bandwidth,
                    });
                }
                prev = Some(sys_cnt);
            }
        }
        Ok(out)
    }
}

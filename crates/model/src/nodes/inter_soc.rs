use std::collections::HashMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::{Clock, fmt_us};

use super::sources::SocProfileRecord;

#[derive(Debug, Clone)]
pub struct InterSocSample {
    pub ts_us: compact_str::CompactString,
    pub l2_buffer_bw_level: u32,
    pub mata_bw_level: u32,
}

impl Node for InterSocSample {
    const NAME: &'static str = "InterSoc";
    type Deps = (SocProfileRecord,);

    fn compute(
        ctx: &Ctx,
        (records,): (std::sync::Arc<Vec<SocProfileRecord>>,),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out: Vec<InterSocSample> = Vec::new();
        let mut index: HashMap<compact_str::CompactString, usize> = HashMap::new();
        for record in records.iter() {
            if let super::sources::stars_soc_profile::SocProfileRaw::InterSoc {
                sys_cnt,
                l2_buffer_bw_level,
                mata_bw_level,
            } = record.0
            {
                let ts_ns = clock.to_ns(sys_cnt);
                if (ts_ns as i128) < begin_ns {
                    continue;
                }
                let ts_us = fmt_us(ts_ns, false);
                let sample = InterSocSample {
                    ts_us: ts_us.clone(),
                    l2_buffer_bw_level,
                    mata_bw_level,
                };
                if let Some(&i) = index.get(&ts_us) {
                    out[i] = sample;
                } else {
                    index.insert(ts_us, out.len());
                    out.push(sample);
                }
            }
        }
        Ok(out)
    }
}

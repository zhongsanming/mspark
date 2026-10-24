use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::{Clock, fmt_us};

use super::sources::SocProfileRecord;

/// `[read_bandwidth, read_ost, write_bandwidth, write_ost]`, max per timestamp.
#[derive(Debug, Clone)]
pub struct AccPmuSample {
    pub ts_us: compact_str::CompactString,
    pub values: [u64; 4],
}

impl Node for AccPmuSample {
    const NAME: &'static str = "AccPmu";
    type Deps = (SocProfileRecord,);

    fn compute(
        ctx: &Ctx,
        (records,): (std::sync::Arc<Vec<SocProfileRecord>>,),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut rows: Vec<(u64, u16, [u64; 4])> = Vec::new();
        for record in records.iter() {
            if let super::sources::stars_soc_profile::SocProfileRaw::AccPmu {
                timestamp,
                acc_id,
                bandwidth,
                ost,
            } = record.0
            {
                rows.push((
                    timestamp,
                    acc_id,
                    [bandwidth[0], ost[0], bandwidth[1], ost[1]],
                ));
            }
        }
        rows.retain(|(timestamp, _, _)| (clock.to_ns(*timestamp) as i128) >= begin_ns);
        rows.sort_by_key(|(timestamp, acc_id, _)| (*timestamp, *acc_id));

        let mut out: Vec<AccPmuSample> = Vec::new();
        for (timestamp, _, values) in rows {
            let ts_us = fmt_us(clock.to_ns(timestamp), false);
            match out.last_mut() {
                Some(last) if last.ts_us == ts_us => {
                    for (dst, src) in last.values.iter_mut().zip(values) {
                        *dst = (*dst).max(src);
                    }
                }
                _ => out.push(AccPmuSample { ts_us, values }),
            }
        }
        Ok(out)
    }
}

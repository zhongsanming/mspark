use std::collections::BTreeMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

type DvppAgg = (f64, f64, f64, f64, String);

#[derive(Debug, Clone)]
pub struct DvppRow {
    pub dvpp_id: u32,
    pub engine_type: u32,
    pub engine_id: u32,
    pub all_time: f64,
    pub all_frame: f64,
    pub all_utilization: String,
}

impl Node for DvppRow {
    const NAME: &'static str = "dvpp";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "dvpp.data.", 1)?;
        let text = String::from_utf8_lossy(&bytes);
        let mut agg: BTreeMap<(u32, u32, u32), DvppAgg> = BTreeMap::new();
        for line in text.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 7 {
                continue;
            }
            let (Ok(dvpp_id), Ok(engine_type), Ok(engine_id)) = (
                fields[1].parse::<u32>(),
                fields[2].parse::<u32>(),
                fields[3].parse::<u32>(),
            ) else {
                continue;
            };
            let all_time = fields[4].parse::<f64>().unwrap_or(0.0);
            let all_frame = fields[5].parse::<f64>().unwrap_or(0.0);
            let utilization = fields[6].trim_end_matches('%').to_string();
            agg.entry((engine_type, engine_id, dvpp_id))
                .and_modify(|entry| {
                    entry.0 = entry.0.min(all_time);
                    entry.1 = entry.1.max(all_time);
                    entry.2 = entry.2.min(all_frame);
                    entry.3 = entry.3.max(all_frame);
                    entry.4 = utilization.clone();
                })
                .or_insert((all_time, all_time, all_frame, all_frame, utilization));
        }
        Ok(agg
            .into_iter()
            .map(
                |((engine_type, engine_id, dvpp_id), (t_min, t_max, f_min, f_max, util))| DvppRow {
                    dvpp_id,
                    engine_type,
                    engine_id,
                    all_time: t_max - t_min,
                    all_frame: f_max - f_min,
                    all_utilization: util,
                },
            )
            .collect())
    }
}

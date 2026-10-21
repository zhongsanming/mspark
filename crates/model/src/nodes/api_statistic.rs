use std::collections::BTreeMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::format::{double_to_str, round3};
use crate::time::load_host_clock;

use super::sources::ApiEventRow;

#[derive(Debug, Clone)]
pub struct ApiStatisticRow {
    pub level: u16,
    pub api_name: String,
    pub time: String,
    pub count: usize,
    pub avg: String,
    pub min: String,
    pub max: String,
    pub variance: String,
}

impl Node for ApiStatisticRow {
    const NAME: &'static str = "ApiStatistic";
    type Deps = (ApiEventRow,);

    fn compute(ctx: &Ctx, (events,): (std::sync::Arc<Vec<ApiEventRow>>,)) -> Result<Vec<Self>> {
        let clock = load_host_clock(ctx.root())?;
        let mut grouped: BTreeMap<(String, u16), Vec<f64>> = BTreeMap::new();
        for event in events.iter() {
            let start = clock.to_ns(event.begin);
            let end = clock.to_ns(event.end);
            let duration = round3((end as i128 - start as i128) as f64 / 1000.0);
            grouped
                .entry((event.api_name.clone(), event.level))
                .or_default()
                .push(duration);
        }
        Ok(grouped
            .into_iter()
            .map(|((api_name, level), durations)| {
                let count = durations.len();
                let sum: f64 = durations.iter().sum();
                let avg = round3(sum / count as f64);
                let min = durations.iter().copied().fold(f64::INFINITY, f64::min);
                let max = durations.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                let variance = round3(
                    durations
                        .iter()
                        .map(|value| (value - avg) * (value - avg))
                        .sum::<f64>()
                        / count as f64,
                );
                ApiStatisticRow {
                    level,
                    api_name,
                    time: double_to_str(sum),
                    count,
                    avg: double_to_str(avg),
                    min: double_to_str(min),
                    max: double_to_str(max),
                    variance: double_to_str(variance),
                }
            })
            .collect())
    }
}

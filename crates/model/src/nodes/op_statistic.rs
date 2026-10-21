use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::format::{double_to_str, round3};
use crate::host::ge_task_type;
use crate::time::Clock;

use super::device_task::DeviceTask;
use super::sources::NodeBasicInfoRow;

#[derive(Default)]
struct Agg {
    count: u64,
    total_ns: f64,
    min_ns: f64,
    max_ns: f64,
}

#[derive(Debug, Clone)]
pub struct OpStatisticRow {
    pub device_id: u16,
    pub op_type: String,
    pub core_type: String,
    pub count: String,
    pub total_time: String,
    pub min: String,
    pub avg: String,
    pub max: String,
    pub ratio: String,
}

impl Node for OpStatisticRow {
    const NAME: &'static str = "OpStatistic";
    type Deps = (DeviceTask, NodeBasicInfoRow);

    fn compute(
        ctx: &Ctx,
        (tasks, nbis): (
            std::sync::Arc<Vec<DeviceTask>>,
            std::sync::Arc<Vec<NodeBasicInfoRow>>,
        ),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let mut ordered: Vec<&DeviceTask> = tasks.iter().collect();
        ordered.sort_by_key(|task| (task.device_id, task.op_start_raw));

        let mut aggregated: BTreeMap<(u16, String, String), Agg> = BTreeMap::new();
        let mut device_total: HashMap<u16, f64> = HashMap::new();
        let mut next_nbi = 0usize;
        for task in ordered {
            if task.task_type == 1 || task.task_type == 3 {
                continue;
            }
            let Some(nbi) = nbis.get(next_nbi) else {
                continue;
            };
            next_nbi += 1;
            let core_type = ge_task_type(nbi.task_type).to_string();
            if core_type == "COMMUNICATION" || core_type == "HCCL_AI_CPU" {
                continue;
            }
            let duration_ns = (clock.to_ns(task.op_end_raw) as i128
                - clock.to_ns(task.op_start_raw) as i128) as f64;
            let entry = aggregated
                .entry((task.device_id, nbi.op_type.clone(), core_type))
                .or_insert_with(|| Agg {
                    min_ns: f64::INFINITY,
                    ..Agg::default()
                });
            entry.count += 1;
            entry.total_ns += duration_ns;
            entry.min_ns = entry.min_ns.min(duration_ns);
            entry.max_ns = entry.max_ns.max(duration_ns);
            *device_total.entry(task.device_id).or_insert(0.0) += duration_ns;
        }

        let mut rows: Vec<OpStatisticRow> = Vec::new();
        for ((device_id, op_type, core_type), agg) in aggregated {
            if op_type == "N/A" || core_type == "WRITE_BACK" || core_type == "INVALID" {
                continue;
            }
            let total = device_total.get(&device_id).copied().unwrap_or(0.0);
            let ratio = if total > 0.0 {
                agg.total_ns * 100.0 / total
            } else {
                0.0
            };
            rows.push(OpStatisticRow {
                device_id,
                op_type,
                core_type,
                count: agg.count.to_string(),
                total_time: double_to_str(round3(agg.total_ns / 1000.0)),
                min: double_to_str(round3(agg.min_ns / 1000.0)),
                avg: double_to_str(round3(agg.total_ns / agg.count as f64 / 1000.0)),
                max: double_to_str(round3(agg.max_ns / 1000.0)),
                ratio: double_to_str(round3(ratio)),
            });
        }
        rows.sort_by(|a, b| {
            let ta: f64 = a.total_time.parse().unwrap_or(0.0);
            let tb: f64 = b.total_time.parse().unwrap_or(0.0);
            tb.partial_cmp(&ta).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(rows)
    }
}

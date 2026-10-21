use std::collections::HashMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::host::{RUNTIME_LEVEL, TypeDic, ge_task_type};
use crate::time::{Clock, fmt_us};

use super::device_task::DeviceTask;
use super::sources::{NodeBasicInfoRow, TaskTrackRow};

#[derive(Debug, Clone)]
pub struct TimelineTask {
    pub name: String,
    pub tid: u32,
    pub ts_us: String,
    pub dur_us: f64,
    pub task_type: String,
    pub stream_id: u32,
    pub task_id: u32,
    pub subtask_id: u32,
    pub model_id: u32,
    pub batch_id: u32,
}

impl Node for TimelineTask {
    const NAME: &'static str = "TimelineTask";
    type Deps = (DeviceTask, NodeBasicInfoRow, TaskTrackRow);

    fn compute(
        ctx: &Ctx,
        (tasks, nbis, track): (
            std::sync::Arc<Vec<DeviceTask>>,
            std::sync::Arc<Vec<NodeBasicInfoRow>>,
            std::sync::Arc<Vec<TaskTrackRow>>,
        ),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let types = ctx.require::<TypeDic>()?;
        let track_map: HashMap<(u16, u32), u64> = track
            .iter()
            .map(|t| ((t.stream_id, t.task_id), t.task_type))
            .collect();
        let kernel_map: HashMap<(u16, u32), &str> = track
            .iter()
            .filter(|t| !t.kernel_name.is_empty())
            .map(|t| ((t.stream_id, t.task_id), t.kernel_name.as_str()))
            .collect();
        // msprof prefers the host-side kernel name over the device op name.

        let mut ordered: Vec<&DeviceTask> = tasks.iter().collect();
        ordered.sort_by_key(|t| (t.device_id, t.op_start_raw));

        let mut rows = Vec::with_capacity(ordered.len());
        let mut next_nbi = 0usize;
        for t in ordered {
            let special = t.task_type == 1 || t.task_type == 3;
            let (name, task_type, tid, task_id, subtask_id) = if special {
                let kt = track_map
                    .get(&(t.stream_id, t.task_id as u32))
                    .and_then(|tt| types.get(RUNTIME_LEVEL, *tt))
                    .unwrap_or(match t.task_type {
                        1 => "TASK_TIMEOUT_SET",
                        _ => "PROFILING_ENABLE",
                    });
                (
                    kt.to_string(),
                    kt.to_string(),
                    0u32,
                    t.task_id as u32,
                    u32::MAX,
                )
            } else {
                let Some(nbi) = nbis.get(next_nbi) else {
                    continue;
                };
                next_nbi += 1;
                let kernel = kernel_map
                    .get(&(t.stream_id, t.task_id as u32))
                    .map(|name| (*name).to_string())
                    .unwrap_or_else(|| nbi.op_name.clone());
                (
                    kernel,
                    ge_task_type(nbi.task_type).to_string(),
                    t.stream_id as u32,
                    t.task_id as u32,
                    if t.mix { 0 } else { u32::MAX },
                )
            };
            rows.push(TimelineTask {
                name,
                tid,
                ts_us: fmt_us(clock.to_ns(t.op_start_raw), false),
                dur_us: clock.duration_us_f64(t.op_start_raw, t.op_end_raw),
                task_type,
                // Special tasks report stream 0.
                stream_id: tid,
                task_id,
                subtask_id,
                model_id: u32::MAX,
                batch_id: 0,
            });
        }
        Ok(rows)
    }
}

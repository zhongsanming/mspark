use std::collections::HashMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::host::{RUNTIME_LEVEL, TypeDic, ge_task_type};
use crate::time::{Clock, fmt_us};

use super::device_task::DeviceTask;
use super::sources::{NodeBasicInfoRow, TaskTrackRow};

#[derive(Debug, Clone)]
pub struct TaskTimeRow {
    pub device_id: u16,
    pub kernel_name: String,
    pub kernel_type: String,
    pub stream_id: u32,
    pub task_id: u32,
    pub duration_ns: u64,
    pub start_ns: u64,
    pub stop_ns: u64,
}

impl TaskTimeRow {
    pub const HEADER: &'static str = "Device_id,kernel_name,kernel_type,stream_id,task_id,task_time(us),task_start(us),task_stop(us)";

    pub fn to_csv(&self) -> String {
        let mut out = String::with_capacity(96);
        out.push_str(&self.device_id.to_string());
        out.push(',');
        // Kernel names come from the host dictionary and may contain commas or
        // quotes; msprof quotes them like any other field.
        crate::format::push_csv_field(&mut out, &self.kernel_name);
        out.push(',');
        crate::format::push_csv_field(&mut out, &self.kernel_type);
        out.push_str(&format!(
            ",{},{},{},{},{}\n",
            self.stream_id,
            self.task_id,
            fmt_us(self.duration_ns, false),
            fmt_us(self.start_ns, true),
            fmt_us(self.stop_ns, true),
        ));
        out
    }
}

impl Node for TaskTimeRow {
    const NAME: &'static str = "TaskTime";
    type Deps = (DeviceTask, NodeBasicInfoRow, TaskTrackRow);

    fn compute(
        ctx: &Ctx,
        (tasks, nbi, track): (
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
        // msprof prefers the host-side kernel name (resolved from the host hash
        // dictionary) over the device op name; see `task_time_assembler.cpp`.
        let kernel_map: HashMap<(u16, u32), &str> = track
            .iter()
            .filter(|t| !t.kernel_name.is_empty())
            .map(|t| ((t.stream_id, t.task_id), t.kernel_name.as_str()))
            .collect();

        let mut ordered: Vec<&DeviceTask> = tasks.iter().collect();
        ordered.sort_by_key(|t| (t.device_id, t.start_raw));

        let mut rows = Vec::with_capacity(ordered.len());
        let mut next_nbi = 0usize;
        for t in ordered {
            // The special ACSQ tasks (profiling enable/disable, timeout) are not
            // in node_basic_info; their type comes from task_track + type_info_dic.
            let special = t.task_type == 1 || t.task_type == 3;
            let (kernel_name, kernel_type) = if special {
                let kt = track_map
                    .get(&(t.stream_id, t.task_id as u32))
                    .and_then(|tt| types.get(RUNTIME_LEVEL, *tt))
                    .unwrap_or(match t.task_type {
                        1 => "TASK_TIMEOUT_SET",
                        _ => "PROFILING_ENABLE",
                    });
                ("N/A".to_string(), kt.to_string())
            } else if let Some(info) = nbi.get(next_nbi) {
                // node_basic_info is emitted in the same order as the named
                // tasks (which are ordered by start time); the special tasks are
                // skipped above without consuming an entry.
                next_nbi += 1;
                // The core type comes from `node_basic_info`; the FFTS+
                // presence (`mix`) does not change it.
                let kt = ge_task_type(info.task_type);
                let name = kernel_map
                    .get(&(t.stream_id, t.task_id as u32))
                    .map(|name| (*name).to_string())
                    .unwrap_or_else(|| info.op_name.clone());
                (name, kt.to_string())
            } else {
                // No host GE data: keep the device-only placeholder.
                let kt = "N/A";
                ("N/A".to_string(), kt.to_string())
            };

            // A mix task reports the AIV sub-task window, not the whole op window.
            let (start_raw, end_raw) = if t.mix {
                (t.op_start_raw, t.op_end_raw)
            } else {
                (t.start_raw, t.end_raw)
            };
            rows.push(TaskTimeRow {
                device_id: t.device_id,
                kernel_name,
                kernel_type,
                stream_id: t.stream_id as u32,
                task_id: t.task_id as u32,
                duration_ns: clock.dur_ns(start_raw, end_raw),
                start_ns: clock.to_ns(start_raw),
                stop_ns: clock.to_ns(end_raw),
            });
        }
        // msprof keeps rows ordered by (device, start time).
        rows.sort_by_key(|r| (r.device_id, r.start_ns));
        Ok(rows)
    }
}

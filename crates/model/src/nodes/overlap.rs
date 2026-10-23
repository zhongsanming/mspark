use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::{Clock, fmt_us};

use super::device_task::DeviceTask;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapKind {
    Compute,
    Free,
}

#[derive(Debug, Clone)]
pub struct OverlapRow {
    pub kind: OverlapKind,
    pub ts_us: compact_str::CompactString,
    pub dur_us: f64,
}

impl Node for OverlapRow {
    const NAME: &'static str = "Overlap";
    type Deps = (DeviceTask,);

    fn compute(ctx: &Ctx, (tasks,): (std::sync::Arc<Vec<DeviceTask>>,)) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let mut begin_ns = u64::MAX;
        let mut end_ns = 0u64;
        let mut compute: Vec<(u64, u64)> = Vec::new();
        for task in tasks.iter() {
            let start = clock.to_ns(task.op_start_raw);
            let end = clock.to_ns(task.op_end_raw);
            // `UpdateTaskTimeExtremes` excludes PROFILING_* (raw taskType 3) but keeps
            // TASK_TIMEOUT_SET (raw taskType 1).
            if task.task_type != 3 {
                begin_ns = begin_ns.min(start);
                end_ns = end_ns.max(end);
            }
            // Compute sections come from `TaskInfoData` (named tasks only).
            if task.task_type == 0 {
                compute.push((start, end));
            }
        }
        compute.sort_unstable();

        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (start, end) in compute {
            if let Some(last) = merged.last_mut()
                && last.1 >= start
            {
                last.1 = last.1.max(end);
                continue;
            }
            merged.push((start, end));
        }

        let mut rows = Vec::with_capacity(merged.len() * 2);
        for (start, end) in &merged {
            if end > start {
                rows.push(OverlapRow {
                    kind: OverlapKind::Compute,
                    ts_us: fmt_us(*start, false),
                    dur_us: (*end - *start) as f64 / 1000.0,
                });
            }
        }
        if end_ns > begin_ns {
            let mut cursor = begin_ns;
            for (start, end) in &merged {
                if *start > cursor {
                    rows.push(OverlapRow {
                        kind: OverlapKind::Free,
                        ts_us: fmt_us(cursor, false),
                        dur_us: (*start - cursor) as f64 / 1000.0,
                    });
                }
                cursor = cursor.max(*end);
            }
            if end_ns > cursor {
                rows.push(OverlapRow {
                    kind: OverlapKind::Free,
                    ts_us: fmt_us(cursor, false),
                    dur_us: (end_ns - cursor) as f64 / 1000.0,
                });
            }
        }
        Ok(rows)
    }
}

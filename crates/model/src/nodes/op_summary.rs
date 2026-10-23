use std::collections::{HashMap, VecDeque};

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::format::{csv_field, fmt_metric};
use crate::host::ge_task_type;
use crate::nodes::sources::tensor_info::{format_str, join_or_na, shapes_field, take_tensors};
use crate::pmu::{AIC_EVENTS, AIV_EVENTS, get_freq, pmu_div, pmu_ratio};
use crate::raw;
use crate::tables;
use crate::time::{Clock, fmt_us};

use super::device_task::DeviceTask;
use super::sources::{NodeBasicInfoRow, TensorInfoRow};

#[derive(Debug, Clone)]
pub struct OpSummaryRow {
    pub cols: Vec<String>,
}

impl OpSummaryRow {
    pub const HEADER: [&'static str; 46] = [
        "Device_id",
        "Model ID",
        "Task ID",
        "Stream ID",
        "Op Name",
        "OP Type",
        "OP State",
        "Task Type",
        "Task Start Time(us)",
        "Task Duration(us)",
        "Task Wait Time(us)",
        "Block Dim",
        "Mix Block Dim",
        "HF32 Eligible",
        "Input Shapes",
        "Input Data Types",
        "Input Formats",
        "Output Shapes",
        "Output Data Types",
        "Output Formats",
        "Context ID",
        "aicore_time(us)",
        "aic_total_cycles",
        "aic_mac_time(us)",
        "aic_mac_ratio",
        "aic_scalar_time(us)",
        "aic_scalar_ratio",
        "aic_mte1_time(us)",
        "aic_mte1_ratio",
        "aic_mte2_time(us)",
        "aic_mte2_ratio",
        "aic_fixpipe_time(us)",
        "aic_fixpipe_ratio",
        "aic_icache_miss_rate",
        "aiv_time(us)",
        "aiv_total_cycles",
        "aiv_vec_time(us)",
        "aiv_vec_ratio",
        "aiv_scalar_time(us)",
        "aiv_scalar_ratio",
        "aiv_mte2_time(us)",
        "aiv_mte2_ratio",
        "aiv_mte3_time(us)",
        "aiv_mte3_ratio",
        "aiv_icache_miss_rate",
        "cube_utilization(%)",
    ];

    pub fn to_csv(&self) -> String {
        let mut s = self
            .cols
            .iter()
            .map(|field| csv_field(field))
            .collect::<Vec<_>>()
            .join(",");
        s.push('\n');
        s
    }
}

impl Node for OpSummaryRow {
    const NAME: &'static str = "OpSummary";
    type Deps = (
        DeviceTask,
        NodeBasicInfoRow,
        TensorInfoRow,
        raw::PmuData,
        raw::LpmInfo,
    );

    fn compute(
        ctx: &Ctx,
        (tasks, nbis, tensor_infos, pmus, lpms): (
            std::sync::Arc<Vec<DeviceTask>>,
            std::sync::Arc<Vec<NodeBasicInfoRow>>,
            std::sync::Arc<Vec<TensorInfoRow>>,
            std::sync::Arc<Vec<raw::PmuData>>,
            std::sync::Arc<Vec<raw::LpmInfo>>,
        ),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let info = ctx.require::<DeviceInfo>()?;
        let mut tensor_queues: HashMap<u64, VecDeque<&TensorInfoRow>> = HashMap::new();
        for tensor_info in tensor_infos.iter() {
            tensor_queues
                .entry(tensor_info.op_name_hash)
                .or_default()
                .push_back(tensor_info);
        }
        let samples: Vec<raw::LpmSample> = lpms
            .first()
            .map(|lpm| lpm.samples.clone())
            .unwrap_or_default();
        let pmu_map: HashMap<(u16, u16), &raw::PmuData> = pmus
            .iter()
            .map(|pmu| ((pmu.stream_id, pmu.task_id), pmu))
            .collect();

        // op_summary only contains the named tasks (special ACSQ tasks excluded).
        let mut ordered: Vec<&DeviceTask> = tasks
            .iter()
            .filter(|task| !(task.task_type == 1 || task.task_type == 3))
            .collect();
        ordered.sort_by_key(|task| (task.device_id, task.op_start_raw));

        let mut rows = Vec::with_capacity(ordered.len());
        for (idx, task) in ordered.into_iter().enumerate() {
            let Some(nbi) = nbis.get(idx) else {
                break;
            };
            let Some(pmu) = pmu_map.get(&(task.stream_id, task.task_id)) else {
                continue;
            };

            let start_ns = clock.to_ns(task.op_start_raw);
            let dur_ns = clock.dur_ns(task.op_start_raw, task.op_end_raw);
            let total = pmu.total_cycle;
            let is_aic = matches!(nbi.task_type, 0 | 4);
            let core_num = if is_aic {
                info.ai_core_num as u64
            } else {
                info.aiv_num as u64
            };
            let blk = nbi.block_num as u64;
            let freq_hz = get_freq(&samples, info.aic_freq_mhz * 1_000_000.0, pmu.time_list[1]);
            let total_time = if blk > 0 && core_num > 0 && freq_hz > 0.0 {
                total as f64 * 1_000_000.0 / freq_hz / blk as f64 * blk.div_ceil(core_num) as f64
            } else {
                0.0
            };

            let mut cols: Vec<String> = Vec::with_capacity(46);
            cols.push(task.device_id.to_string());
            cols.push("4294967295".to_string());
            cols.push(task.task_id.to_string());
            cols.push(task.stream_id.to_string());
            cols.push(nbi.op_name.clone());
            cols.push(nbi.op_type.clone());
            cols.push("dynamic".to_string());
            cols.push(ge_task_type(nbi.task_type).to_string());
            cols.push(fmt_us(start_ns, true).to_string());
            cols.push(fmt_us(dur_ns, false).to_string());
            cols.push("0".to_string()); // wait, filled after sorting
            cols.push(blk.to_string());
            cols.push("0".to_string()); // Mix Block Dim
            cols.push("NO".to_string()); // HF32 Eligible
            let (inputs, outputs) = tensor_queues
                .get_mut(&nbi.op_name_hash)
                .map(take_tensors)
                .unwrap_or_default();
            cols.push(shapes_field(&inputs));
            cols.push(join_or_na(
                inputs
                    .iter()
                    .map(|t| tables::data_type_name(t.data_type))
                    .collect(),
            ));
            cols.push(join_or_na(
                inputs.iter().map(|t| format_str(t.format)).collect(),
            ));
            cols.push(shapes_field(&outputs));
            cols.push(join_or_na(
                outputs
                    .iter()
                    .map(|t| tables::data_type_name(t.data_type))
                    .collect(),
            ));
            cols.push(join_or_na(
                outputs.iter().map(|t| format_str(t.format)).collect(),
            ));
            cols.push(if task.mix { "0" } else { "N/A" }.to_string()); // Context ID

            let events: &[u32; 8] = if is_aic { &AIC_EVENTS } else { &AIV_EVENTS };
            if is_aic {
                let mac = pmu_ratio(total, &pmu.pmu_list, events, &[0x416, 0x417]);
                let scalar = pmu_ratio(total, &pmu.pmu_list, events, &[0x9]);
                let mte1 = pmu_ratio(total, &pmu.pmu_list, events, &[0x302]);
                let mte2 = pmu_ratio(total, &pmu.pmu_list, events, &[0xc]);
                let fixpipe = pmu_ratio(total, &pmu.pmu_list, events, &[0x303]);
                let icache = pmu_div(total, &pmu.pmu_list, events, 0x55, 0x54);
                cols.push(fmt_metric(total_time));
                cols.push(total.to_string());
                cols.push(fmt_metric(total_time * mac));
                cols.push(fmt_metric(mac));
                cols.push(fmt_metric(total_time * scalar));
                cols.push(fmt_metric(scalar));
                cols.push(fmt_metric(total_time * mte1));
                cols.push(fmt_metric(mte1));
                cols.push(fmt_metric(total_time * mte2));
                cols.push(fmt_metric(mte2));
                cols.push(fmt_metric(total_time * fixpipe));
                cols.push(fmt_metric(fixpipe));
                cols.push(fmt_metric(icache));
                cols.push("0.0".to_string());
                cols.push("0".to_string());
                for _ in 0..9 {
                    cols.push("0.0".to_string());
                }
            } else {
                let vec = pmu_ratio(total, &pmu.pmu_list, events, &[0x8]);
                let scalar = pmu_ratio(total, &pmu.pmu_list, events, &[0x9]);
                let mte2 = pmu_ratio(total, &pmu.pmu_list, events, &[0xc]);
                let mte3 = pmu_ratio(total, &pmu.pmu_list, events, &[0xd]);
                let icache = pmu_div(total, &pmu.pmu_list, events, 0x55, 0x54);
                cols.push("0.0".to_string());
                cols.push("0".to_string());
                for _ in 0..11 {
                    cols.push("0.0".to_string());
                }
                cols.push(fmt_metric(total_time));
                cols.push(total.to_string());
                cols.push(fmt_metric(total_time * vec));
                cols.push(fmt_metric(vec));
                cols.push(fmt_metric(total_time * scalar));
                cols.push(fmt_metric(scalar));
                cols.push(fmt_metric(total_time * mte2));
                cols.push(fmt_metric(mte2));
                cols.push(fmt_metric(total_time * mte3));
                cols.push(fmt_metric(mte3));
                cols.push(fmt_metric(icache));
            }

            // cube_utilization = aic_total_cycles / (aic_freq * ai_core_num) * 100 / duration
            let cube = if is_aic && dur_ns > 0 && info.ai_core_num > 0 && info.aic_freq_mhz > 0.0 {
                total as f64 / (info.aic_freq_mhz * info.ai_core_num as f64) * 100.0
                    / (dur_ns as f64 / 1000.0)
            } else {
                0.0
            };
            cols.push(format!("{cube:.3}"));

            rows.push(OpSummaryRow { cols });
        }

        // C++ sorts by (device, start string), then computes the wait time from
        // the already-formatted start/duration strings.
        rows.sort_by(|a, b| {
            a.cols[0]
                .cmp(&b.cols[0])
                .then_with(|| a.cols[8].cmp(&b.cols[8]))
        });
        for i in 1..rows.len() {
            let curr: f64 = rows[i].cols[8].trim().parse().unwrap_or(0.0);
            let prev: f64 = rows[i - 1].cols[8].trim().parse().unwrap_or(0.0);
            let prev_dur: f64 = rows[i - 1].cols[9].parse().unwrap_or(0.0);
            let wait = (curr - prev - prev_dur).max(0.0);
            rows[i].cols[10] = format!("{wait:.6}");
        }
        Ok(rows)
    }
}

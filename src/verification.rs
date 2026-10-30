//! Verification commands: golden-file comparison, per-column baselines and the
//! timeline signature checks.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::artifacts::{ALL_ARTIFACTS, Artifact, write_artifact};
use crate::reports::hbm::hbm_track_rows;
use crate::reports::hccs::hccs_rows;
use crate::reports::host::{host_cpu_rows, host_local_offset_milli};
use crate::reports::llc::llc_track_rows;
use crate::reports::net::net_rows;
use crate::reports::pcie::pcie_track_rows;
use crate::reports::timeline::{
    acc_pmu_rows, chip_trans_rows, inter_soc_rows, npu_mem_rows, qos_rows, sample_csv_list,
    sio_rows,
};
use crate::session::{devices, session_for};
use crate::text::parse_us_to_ns;
use crate::trace::{CounterRow, HOST_PID, Track};
use crate::verify::{ColumnMatch, compare_generated, gate, read_rows};
use mspark_model::{
    AccPmuSample, ApiEventRow, ChipTransSample, HbmRecord, HccsRateRow, HostCpuUsageRow,
    InterSocSample, LlcRecord, NicRow, NpuMemRow, OverlapKind, OverlapRow, PcieRecord, QosRow,
    RoceRow, SioSample, TaskTimeRow, TimelineTask, fmt_us,
};

pub fn verify(prof: &Path, only: Option<&str>, golden: &Path) -> Result<()> {
    let golden_rows = read_rows(golden)?;
    let Some((header, golden_rows)) = golden_rows.split_first() else {
        anyhow::bail!("golden file is empty");
    };
    let expected: Vec<&str> = TaskTimeRow::HEADER.split(',').collect();
    if header.as_slice() != expected.as_slice() {
        anyhow::bail!("header mismatch:\n  golden: {header:?}\n  mspark: {expected:?}");
    }

    let mut produced: HashMap<(u32, u32), Vec<TaskTimeRow>> = HashMap::new();
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        for row in session.resolve::<TaskTimeRow>()?.iter() {
            produced
                .entry((row.stream_id, row.task_id))
                .or_default()
                .push(row.clone());
        }
    }

    let mut matched = 0usize;
    let mut missing = 0usize;
    let mut names_ok = 0usize;
    let mut types_ok = 0usize;
    let mut max_start_ns = 0i128;
    let mut max_dur_ns = 0i128;
    let mut start_exact = 0usize;
    let mut start_close = 0usize;
    let mut dur_exact = 0usize;
    let mut dur_close = 0usize;

    for row in golden_rows {
        let stream: u32 = row[3].parse()?;
        let task: u32 = row[4].parse()?;
        let g_start = parse_us_to_ns(&row[6])?;
        let g_dur = parse_us_to_ns(&row[5])?;
        let Some(candidates) = produced.get(&(stream, task)) else {
            missing += 1;
            continue;
        };
        // A key can have both an ACSQ and an FFTS+ row (the "mix" tasks); pick
        // the candidate whose start matches the golden row.
        let p = candidates
            .iter()
            .min_by_key(|p| (p.start_ns as i128 - g_start).abs())
            .expect("non-empty candidate list");
        matched += 1;
        if p.kernel_name == row[1] {
            names_ok += 1;
        }
        if p.kernel_type == row[2] {
            types_ok += 1;
        }
        let ds = (g_start - p.start_ns as i128).abs();
        let dd = (g_dur - p.duration_ns as i128).abs();
        max_start_ns = max_start_ns.max(ds);
        max_dur_ns = max_dur_ns.max(dd);
        start_exact += (ds == 0) as usize;
        start_close += (ds <= 2) as usize;
        dur_exact += (dd == 0) as usize;
        dur_close += (dd <= 2) as usize;
    }

    let golden_keys: std::collections::HashSet<(u32, u32)> = golden_rows
        .iter()
        .filter_map(|r| Some((r[3].parse().ok()?, r[4].parse().ok()?)))
        .collect();
    let extras = produced.keys().filter(|k| !golden_keys.contains(k)).count();
    let duplicate_keys = produced.values().filter(|v| v.len() > 1).count();

    println!("golden rows:   {}", golden_rows.len());
    println!(
        "produced keys: {} ({duplicate_keys} with both ACSQ and FFTS+ rows)",
        produced.len()
    );
    println!("matched:       {matched}");
    println!("missing:       {missing}");
    println!("extras:        {extras}");
    println!("kernel_name exact: {names_ok}/{matched}");
    println!("kernel_type exact: {types_ok}/{matched}");
    println!(
        "task_time:  exact {dur_exact}/{matched}, within 2ns {dur_close}/{matched}, max |Δ| = {max_dur_ns} ns"
    );
    println!(
        "task_start: exact {start_exact}/{matched}, within 2ns {start_close}/{matched}, max |Δ| = {max_start_ns} ns"
    );
    let clean = missing == 0
        && extras == 0
        && matched == golden_rows.len()
        && names_ok == matched
        && types_ok == matched
        && dur_exact == matched
        && start_exact == matched;
    gate(clean, || {
        format!(
            "task_time is not an exact match: {matched} matched, {missing} missing, {extras} extra, \
             {names_ok}/{matched} names, {dur_exact}/{matched} durations, {start_exact}/{matched} starts"
        )
    })
}
pub fn signature(name: &str, tid: u32, ts: &str, args: &[(String, f64)]) -> String {
    let mut args = args.to_vec();
    args.sort_by(|a, b| a.0.cmp(&b.0));
    format!(
        "{name}|{tid}|{ts}|{}",
        args.iter()
            .map(|(key, value)| format!("{key}={value:.6}"))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub fn produced_signatures(rows: &[CounterRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            let args: Vec<(String, f64)> = row
                .args
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.as_f64()))
                .collect();
            signature(&row.name, row.tid, &row.ts, &args)
        })
        .collect()
}

pub fn golden_counter_signatures(array: &[serde_json::Value], pid: u64) -> Vec<String> {
    array
        .iter()
        .filter(|event| {
            event.get("pid").and_then(|v| v.as_u64()) == Some(pid)
                && event.get("ph").and_then(|v| v.as_str()) == Some("C")
        })
        .map(|event| {
            let name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let tid = event.get("tid").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let ts = event.get("ts").and_then(|v| v.as_str()).unwrap_or("");
            let mut args = Vec::new();
            if let Some(map) = event.get("args").and_then(|v| v.as_object()) {
                for (key, value) in map {
                    let value = value
                        .as_f64()
                        .or_else(|| value.as_u64().map(|u| u as f64))
                        .unwrap_or(0.0);
                    args.push((key.clone(), value));
                }
            }
            signature(name, tid, ts, &args)
        })
        .collect()
}

pub fn multiset_match(a: &[String], b: &[String]) -> usize {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort();
    b.sort();
    let (mut i, mut j, mut matched) = (0, 0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                matched += 1;
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    matched
}
/// Map process pid -> (process_name, process_labels) from the metadata events.
pub fn golden_process_meta(array: &[serde_json::Value]) -> HashMap<u64, (String, String)> {
    let mut names: HashMap<u64, String> = HashMap::new();
    let mut labels: HashMap<u64, String> = HashMap::new();
    for event in array {
        if event.get("ph").and_then(|v| v.as_str()) != Some("M") {
            continue;
        }
        let Some(pid) = event.get("pid").and_then(|v| v.as_u64()) else {
            continue;
        };
        let args = event.get("args");
        match event.get("name").and_then(|v| v.as_str()) {
            Some("process_name") => {
                if let Some(name) = args.and_then(|a| a.get("name")).and_then(|v| v.as_str()) {
                    names.insert(pid, name.to_string());
                }
            }
            Some("process_labels") => {
                if let Some(label) = args.and_then(|a| a.get("labels")).and_then(|v| v.as_str()) {
                    labels.insert(pid, label.to_string());
                }
            }
            _ => {}
        }
    }
    let mut out = HashMap::new();
    for (pid, name) in names {
        let label = labels.get(&pid).cloned().unwrap_or_default();
        out.insert(pid, (name, label));
    }
    out
}

pub fn golden_pid_for(meta: &HashMap<u64, (String, String)>, name: &str, dev: u32) -> Option<u64> {
    let label = format!("NPU {dev}");
    // Device tracks carry `NPU <dev>`; host tracks (`CPU Usage`, `CANN`) carry
    // their own label, so fall back to a name-only match.
    meta.iter()
        .find(|(_, (track, lbl))| track == name && (lbl.is_empty() || *lbl == label))
        .or_else(|| meta.iter().find(|(_, (track, _))| track == name))
        .map(|(pid, _)| *pid)
}

pub fn verify_timeline(prof: &Path, only: Option<&str>, golden: &Path) -> Result<()> {
    let text = mspark_model::errors::read_to_string(golden)?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| mspark_model::json_error(golden, &text, &error))?;
    let array = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("golden timeline is not a JSON array"))?;

    let mut golden_rows = 0usize;
    let mut produced_rows = 0usize;
    let mut name_ok = 0usize;
    let mut tid_ok = 0usize;
    let mut ts_ok = 0usize;
    let mut dur_ok = 0usize;
    let mut type_ok = 0usize;
    let mut stream_ok = 0usize;
    let mut task_ok = 0usize;
    let mut subtask_ok = 0usize;
    let mut max_dur = 0.0f64;
    let mut api_golden = 0usize;
    let mut api_produced = 0usize;
    let mut api_name_ok = 0usize;
    let mut api_tid_ok = 0usize;
    let mut api_ts_ok = 0usize;
    let mut api_dur_ok = 0usize;
    let mut api_mode_ok = 0usize;
    let mut api_level_ok = 0usize;
    let mut api_id_ok = 0usize;
    let mut api_item_ok = 0usize;
    let golden_meta = golden_process_meta(array);
    let mut track_stats: Vec<(&'static str, usize, usize, usize)> = Vec::new();
    let mut overlap_golden = 0usize;
    let mut overlap_produced = 0usize;
    let mut overlap_matched = 0usize;
    let mut overlap_max_dur = 0.0f64;

    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let hw_pid = Track::Hardware.pid(info.pid, dev) as u64;
        let expected: Vec<&serde_json::Value> = array
            .iter()
            .filter(|event| {
                event.get("pid").and_then(|p| p.as_u64()) == Some(hw_pid)
                    && event.get("ph").and_then(|p| p.as_str()) == Some("X")
            })
            .collect();
        let produced = session.resolve::<TimelineTask>()?;

        golden_rows += expected.len();
        produced_rows += produced.len();
        for (golden, ours) in expected.iter().zip(produced.iter()) {
            if golden.get("name").and_then(|v| v.as_str()) == Some(ours.name.as_str()) {
                name_ok += 1;
            }
            if golden.get("tid").and_then(|v| v.as_u64()) == Some(ours.tid as u64) {
                tid_ok += 1;
            }
            if golden.get("ts").and_then(|v| v.as_str()) == Some(ours.ts_us.as_str()) {
                ts_ok += 1;
            }
            let dur = golden
                .get("dur")
                .and_then(|v| v.as_f64())
                .unwrap_or(f64::NAN);
            let delta = (dur - ours.dur_us).abs();
            max_dur = max_dur.max(delta);
            if delta <= 0.001 {
                dur_ok += 1;
            }
            let args = &golden["args"];
            if args.get("Task Type").and_then(|v| v.as_str()) == Some(ours.task_type.as_str()) {
                type_ok += 1;
            }
            if args.get("Physic Stream Id").and_then(|v| v.as_u64()) == Some(ours.stream_id as u64)
            {
                stream_ok += 1;
            }
            if args.get("Task Id").and_then(|v| v.as_u64()) == Some(ours.task_id as u64) {
                task_ok += 1;
            }
            if args.get("Subtask Id").and_then(|v| v.as_u64()) == Some(ours.subtask_id as u64) {
                subtask_ok += 1;
            }
        }

        // CANN host track
        let host_clock = ctx
            .require::<mspark_model::HostStartLogs>()?
            .0
            .host_clock(ctx.require::<mspark_model::DeviceInfo>()?.host_freq_mhz)?;
        let cann_pid = Track::Cann.pid(info.pid, HOST_PID) as u64;
        let expected_api: Vec<&serde_json::Value> = array
            .iter()
            .filter(|event| {
                event.get("pid").and_then(|p| p.as_u64()) == Some(cann_pid)
                    && event.get("ph").and_then(|p| p.as_str()) == Some("X")
            })
            .collect();
        let produced_api = session.resolve::<ApiEventRow>()?;
        api_golden += expected_api.len();
        api_produced += produced_api.len();
        for (golden, ours) in expected_api.iter().zip(produced_api.iter()) {
            let (prefix, level_name) = match ours.level {
                20_000 => ("AscendCL", "acl"),
                10_000 => ("Node", "node"),
                5_000 => ("Runtime", "runtime"),
                15_000 => ("Model", "model"),
                _ => ("Api", ""),
            };
            let mode = ours.tag.clone();
            let id = if ours.level == 20_000 {
                ours.api_name.clone()
            } else {
                "0".to_string()
            };
            let trace = format!("{prefix}@{}", ours.api_name);
            let ts = fmt_us(host_clock.to_ns(ours.begin), false);
            let dur = host_clock.duration_us_f64(ours.begin, ours.end);
            if golden.get("name").and_then(|v| v.as_str()) == Some(trace.as_str()) {
                api_name_ok += 1;
            }
            if golden.get("tid").and_then(|v| v.as_u64()) == Some(ours.thread_id as u64) {
                api_tid_ok += 1;
            }
            if golden.get("ts").and_then(|v| v.as_str()) == Some(ts.as_str()) {
                api_ts_ok += 1;
            }
            let golden_dur = golden
                .get("dur")
                .and_then(|v| v.as_f64())
                .unwrap_or(f64::NAN);
            if (golden_dur - dur).abs() <= 0.001 {
                api_dur_ok += 1;
            }
            let args = &golden["args"];
            if args.get("Mode").and_then(|v| v.as_str()) == Some(mode.as_str()) {
                api_mode_ok += 1;
            }
            if args.get("level").and_then(|v| v.as_str()) == Some(level_name) {
                api_level_ok += 1;
            }
            if args.get("id").and_then(|v| v.as_str()) == Some(id.as_str()) {
                api_id_ok += 1;
            }
            if args.get("item_id").and_then(|v| v.as_str()) == Some(ours.item_name.as_str()) {
                api_item_ok += 1;
            }
        }

        // SOC profile counter tracks
        let sio_rows_v = sio_rows(&session.resolve::<SioSample>()?);
        let acc_rows_v = acc_pmu_rows(&session.resolve::<AccPmuSample>()?);
        let inter_rows_v = inter_soc_rows(&session.resolve::<InterSocSample>()?);
        let chip_rows_v = chip_trans_rows(&session.resolve::<ChipTransSample>()?);
        let qos_events = sample_csv_list(ctx.device_dir()?, "qosEvents")?;
        let qos_rows_v = qos_rows(&session.resolve::<QosRow>()?, &qos_events);
        let npu_mem_rows_v = npu_mem_rows(&session.resolve::<NpuMemRow>()?);
        let hccs_rows_v = hccs_rows(&session.resolve::<HccsRateRow>()?);
        for (sort, name, rows) in [
            (Track::Sio, "SIO", sio_rows_v),
            (Track::AccPmu, "Acc PMU", acc_rows_v),
            (Track::StarsSocInfo, "Stars Soc Info", inter_rows_v),
            (Track::ChipTrans, "Stars Chip Trans", chip_rows_v),
            (Track::Qos, "QoS", qos_rows_v),
            (Track::Communication, "NPU MEM", npu_mem_rows_v),
            (Track::Hccs, "HCCS", hccs_rows_v),
            (
                Track::CpuUsage,
                "CPU Usage",
                host_cpu_rows(
                    &session.resolve::<HostCpuUsageRow>()?,
                    host_local_offset_milli(ctx.root())?,
                ),
            ),
            (
                Track::Nic,
                "NIC",
                net_rows(
                    &session
                        .resolve::<NicRow>()?
                        .iter()
                        .map(|row| row.0)
                        .collect::<Vec<_>>(),
                    "NIC",
                ),
            ),
            (
                Track::Roce,
                "RoCE",
                net_rows(
                    &session
                        .resolve::<RoceRow>()?
                        .iter()
                        .map(|row| row.0)
                        .collect::<Vec<_>>(),
                    "RoCE",
                ),
            ),
            (
                Track::Llc,
                "LLC",
                llc_track_rows(
                    &session.resolve::<LlcRecord>()?,
                    &ctx.require::<mspark_model::DeviceStartLogs>()?
                        .0
                        .sampling()?,
                ),
            ),
            (
                Track::Hbm,
                "HBM",
                hbm_track_rows(&session.resolve::<HbmRecord>()?),
            ),
            (
                Track::Pcie,
                "PCIe",
                pcie_track_rows(&session.resolve::<PcieRecord>()?),
            ),
        ] {
            let pid =
                golden_pid_for(&golden_meta, name, dev).unwrap_or(sort.pid(info.pid, dev) as u64);
            let golden = golden_counter_signatures(array, pid);
            let produced = produced_signatures(&rows);
            let matched = multiset_match(&golden, &produced);
            track_stats.push((name, golden.len(), produced.len(), matched));
        }

        // Overlap Analysis
        let overlap_pid = golden_pid_for(&golden_meta, "Overlap Analysis", dev)
            .unwrap_or(Track::Overlap.pid(info.pid, dev) as u64);
        let mut golden_overlap: Vec<(String, u32, compact_str::CompactString, f64)> = array
            .iter()
            .filter(|event| {
                event.get("pid").and_then(|v| v.as_u64()) == Some(overlap_pid)
                    && event.get("ph").and_then(|v| v.as_str()) == Some("X")
            })
            .map(|event| {
                (
                    event
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    event.get("tid").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                    compact_str::CompactString::from(
                        event.get("ts").and_then(|v| v.as_str()).unwrap_or(""),
                    ),
                    event.get("dur").and_then(|v| v.as_f64()).unwrap_or(0.0),
                )
            })
            .collect();
        let produced_overlap = session.resolve::<OverlapRow>()?;
        let mut produced_rows: Vec<(String, u32, compact_str::CompactString, f64)> =
            produced_overlap
                .iter()
                .map(|row| {
                    let (name, tid) = match row.kind {
                        OverlapKind::Compute => ("Computing", 2),
                        OverlapKind::Free => ("Free", 3),
                    };
                    (name.to_string(), tid, row.ts_us.clone(), row.dur_us)
                })
                .collect();
        overlap_golden += golden_overlap.len();
        overlap_produced += produced_rows.len();
        let key =
            |r: &(String, u32, compact_str::CompactString, f64)| (r.0.clone(), r.1, r.2.clone());
        golden_overlap.sort_by_key(key);
        produced_rows.sort_by_key(key);
        for (golden, produced) in golden_overlap.iter().zip(produced_rows.iter()) {
            if golden.0 == produced.0 && golden.1 == produced.1 {
                let delta = (golden.3 - produced.3).abs();
                let ts_delta = match (parse_us_to_ns(&golden.2), parse_us_to_ns(&produced.2)) {
                    (Ok(a), Ok(b)) => (a - b).abs(),
                    _ => i128::MAX,
                };
                overlap_max_dur = overlap_max_dur.max(delta);
                if ts_delta <= 2 && delta <= 0.0021 {
                    overlap_matched += 1;
                }
            }
        }
    }

    println!("golden hardware X events: {golden_rows}");
    println!("produced hardware X events: {produced_rows}");
    println!("name exact:     {name_ok}");
    println!("tid exact:      {tid_ok}");
    println!("ts exact:       {ts_ok}");
    println!("dur within 1ns: {dur_ok}  max |delta| = {max_dur:.6} us");
    println!("task type:      {type_ok}");
    println!("stream id:      {stream_ok}");
    println!("task id:        {task_ok}");
    println!("subtask id:     {subtask_ok}");
    println!("CANN golden X events:   {api_golden}");
    println!("CANN produced X events: {api_produced}");
    println!("CANN name:      {api_name_ok}");
    println!("CANN tid:       {api_tid_ok}");
    println!("CANN ts:        {api_ts_ok}");
    println!("CANN dur ≤1ns:  {api_dur_ok}");
    println!("CANN Mode:      {api_mode_ok}");
    println!("CANN level:     {api_level_ok}");
    println!("CANN id:        {api_id_ok}");
    println!("CANN item_id:   {api_item_ok}");
    for &(name, golden, produced, matched) in &track_stats {
        println!("track {name:<16} golden={golden:<6} produced={produced:<6} matched={matched}");
    }
    println!(
        "overlap golden={overlap_golden} produced={overlap_produced} matched={overlap_matched} max_dur={overlap_max_dur:.6}"
    );
    let mut clean = produced_rows == golden_rows
        && name_ok == produced_rows
        && tid_ok == produced_rows
        && ts_ok == produced_rows
        && dur_ok == produced_rows
        && type_ok == produced_rows
        && stream_ok == produced_rows
        && task_ok == produced_rows
        && subtask_ok == produced_rows
        && api_produced == api_golden
        && api_name_ok == api_produced
        && api_tid_ok == api_produced
        && api_ts_ok == api_produced
        && api_dur_ok == api_produced
        && api_mode_ok == api_produced
        && api_level_ok == api_produced
        && api_id_ok == api_produced
        && api_item_ok == api_produced
        && overlap_golden == overlap_produced
        && overlap_matched == overlap_golden;
    for &(name, golden, produced, matched) in &track_stats {
        if golden != produced || matched != golden {
            clean = false;
            eprintln!("strict: track {name} is not an exact match");
        }
    }
    gate(clean, || {
        "timeline is not an exact match (see the counters above)".to_string()
    })
}
pub fn find_golden(dir: &Path, artifact: Artifact) -> Option<PathBuf> {
    let (prefix, extension) = match artifact {
        Artifact::Timeline => ("msprof_", "json"),
        other => (other.name(), "csv"),
    };
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with(prefix)
                && name.ends_with(&format!(".{extension}"))
                // `op_summary_…` must not match `op_summary_x_…`
                && (name.starts_with(&format!("{prefix}_")) || name.starts_with(prefix))
        })
        .collect();
    candidates.sort();
    candidates.pop()
}

/// One artifact's per-column distance from its golden file.
struct ArtifactCheck {
    name: &'static str,
    header: Vec<String>,
    columns: Vec<ColumnMatch>,
    note: String,
}

impl ArtifactCheck {
    fn mean_ratio(&self) -> f64 {
        if self.columns.is_empty() {
            return 1.0;
        }
        self.columns
            .iter()
            .map(|column| column.ratio())
            .sum::<f64>()
            / self.columns.len() as f64
    }
}

/// Compare every artifact against the capture's goldens and the checked-in
/// baseline.
///
/// Byte-exactness is not the goal: the baseline records how close each *column*
/// currently is, and this command fails when one gets worse — a dropped row, a
/// broken column, a stream that stopped decoding. Numeric cells are compared on
/// a fixed grid (`--places`), so float noise is not a regression.
pub fn verify_all(
    prof: &Path,
    only: Option<&str>,
    golden_dir: Option<&Path>,
    baseline: Option<&Path>,
    update_baseline: bool,
    places: u32,
) -> Result<()> {
    let golden_dir = golden_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| prof.join("mindstudio_profiler_output"));
    let baseline_path = baseline
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("spec/baseline.txt"));
    let (baseline_capture, previous) = read_baseline(&baseline_path)?;
    // The recorded closeness is capture-specific (different model, different
    // device mix), so comparing a different capture against it would report a
    // pile of meaningless "regressions".
    let capture = prof
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let capture_matches = match &baseline_capture {
        Some(recorded) => *recorded == capture,
        // Older baselines had no capture line: keep them usable.
        None => true,
    };
    if !capture_matches && !update_baseline {
        eprintln!(
            "warning: baseline {} was recorded on {} but this run compares {}; \
             reporting only (use --update-baseline to re-record)",
            baseline_path.display(),
            baseline_capture.as_deref().unwrap_or("<unknown>"),
            capture
        );
    }

    let dir = std::env::temp_dir().join(format!("mspark-verify-all-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;

    let mut checks: Vec<ArtifactCheck> = Vec::new();
    for artifact in ALL_ARTIFACTS {
        if matches!(artifact, Artifact::Timeline | Artifact::Summary) {
            // The timeline is compared structurally by `verify-timeline`; the
            // summary has no msprof counterpart.
            continue;
        }
        let Some(golden) = find_golden(&golden_dir, artifact) else {
            continue;
        };
        let golden_rows = read_rows(&golden)?;
        let Some(golden_header) = golden_rows.first() else {
            continue;
        };
        let golden_header = golden_header.clone();
        write_artifact(artifact, prof, only, &dir)?;
        let produced = read_rows(&dir.join(artifact.file()))?;
        let (produced_header, produced_body) = match produced.split_first() {
            Some((header, body)) => (header, body),
            None => (&golden_header, &[][..]),
        };
        if produced_header.len() != golden_header.len() {
            checks.push(ArtifactCheck {
                name: artifact.name(),
                header: golden_header.clone(),
                columns: Vec::new(),
                note: format!(
                    "header width {} vs {}",
                    golden_header.len(),
                    produced_header.len()
                ),
            });
            continue;
        }
        let columns = crate::verify::compare_columns(&golden_rows[1..], produced_body, places);
        checks.push(ArtifactCheck {
            name: artifact.name(),
            header: golden_header,
            columns,
            note: String::new(),
        });
    }
    let _ = std::fs::remove_dir_all(&dir);

    println!(
        "{:<24} {:>8} {:>8}  closeness",
        "artifact", "golden", "produced"
    );
    let mut regressions = Vec::new();
    for check in &checks {
        let key = |column: usize| format!("{}.{}", check.name, check.header[column]);
        let mut worst = 1.0f64;
        for (index, column) in check.columns.iter().enumerate() {
            let ratio = column.ratio();
            worst = worst.min(ratio);
            if let Some(recorded) = previous.get(&key(index))
                && !update_baseline
                && capture_matches
                && ratio + 1e-9 < *recorded
            {
                regressions.push((key(index), *recorded, ratio));
            }
        }
        if !check.note.is_empty() {
            println!("{:<24} {:>8} {:>8}  GAP: {}", check.name, 0, 0, check.note);
            continue;
        }
        println!(
            "{:<24} {:>8} {:>8}  mean {:>6.2}%  worst {:>6.2}%",
            check.name,
            check.columns.first().map(|c| c.golden).unwrap_or(0),
            check.columns.first().map(|c| c.produced).unwrap_or(0),
            check.mean_ratio() * 100.0,
            worst * 100.0
        );
    }

    if update_baseline {
        write_baseline(&baseline_path, &checks, &capture)?;
        println!("\nbaseline written to {}", baseline_path.display());
        return Ok(());
    }
    if regressions.is_empty() {
        println!("\nno column regressed against {}", baseline_path.display());
        return Ok(());
    }
    for (key, recorded, ratio) in &regressions {
        eprintln!(
            "regression: {key} {:.2}% < baseline {:.2}%",
            ratio * 100.0,
            recorded * 100.0
        );
    }
    anyhow::bail!("{} column(s) regressed", regressions.len())
}

/// Baseline contents: the capture it was recorded on (if recorded) and the
/// per-column ratios.
fn read_baseline(path: &Path) -> Result<(Option<String>, std::collections::HashMap<String, f64>)> {
    let mut map = std::collections::HashMap::new();
    let mut capture = None;
    let Ok(text) = std::fs::read_to_string(path) else {
        return Ok((capture, map));
    };
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("# capture:") {
            capture = Some(rest.trim().to_string());
            continue;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // `<artifact>.<column>  <matched>/<denominator>  <ratio%>`
        // The counts are authoritative: recomputing the ratio from two integers
        // avoids comparing two rounded percentages (which made a clean run look
        // like a regression, and a stale baseline look fine).
        let mut fields = line.split_whitespace();
        let (Some(name), Some(counts)) = (fields.next(), fields.next()) else {
            continue;
        };
        let Some((matched, total)) = counts.split_once('/') else {
            continue;
        };
        let (Ok(matched), Ok(total)) = (matched.parse::<f64>(), total.parse::<f64>()) else {
            continue;
        };
        map.insert(
            name.to_string(),
            if total > 0.0 { matched / total } else { 1.0 },
        );
    }
    Ok((capture, map))
}

fn write_baseline(path: &Path, checks: &[ArtifactCheck], capture: &str) -> Result<()> {
    let mut text = format!("# capture: {capture}\n");
    text.push_str(
        "# Closeness baseline, one line per artifact column.\n\
         # `mspark verify-all` fails when a column drops below its recorded value;\n\
         # regenerate with `mspark verify-all PROF --update-baseline`.\n\
         # Values are the fraction of values matching the golden file at 3 decimals.\n",
    );
    for check in checks {
        if !check.note.is_empty() {
            text.push_str(&format!("# gap: {} — {}\n", check.name, check.note));
            continue;
        }
        for (index, column) in check.columns.iter().enumerate() {
            let denominator = column.golden.max(column.produced);
            text.push_str(&format!(
                "{}.{:<24} {:>8}/{}  {:>7.2}%\n",
                check.name,
                check.header[index],
                column.matched,
                denominator,
                column.ratio() * 100.0
            ));
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, text)?;
    Ok(())
}

/// Verify any CSV artifact by exporting it exactly as production does and
/// comparing the result against the golden file. Replaces the per-artifact
/// verifiers, which were copy-paste twins of the exporters and could drift.
pub fn verify_csv(
    artifact: Artifact,
    prof: &Path,
    only: Option<&str>,
    golden: &Path,
) -> Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "mspark-verify-{}-{}",
        std::process::id(),
        artifact.name()
    ));
    std::fs::create_dir_all(&dir)?;
    write_artifact(artifact, prof, only, &dir)?;
    let produced = read_rows(&dir.join(artifact.file()))?;
    let _ = std::fs::remove_dir_all(&dir);
    // Columns that only need to agree within an HPFloat residual.
    match artifact {
        Artifact::OpSummary => compare_generated(golden, &produced, Some((&[8, 9, 10], 0.001))),
        Artifact::ApiStatistic | Artifact::OpStatistic => {
            compare_generated(golden, &produced, Some((&[3, 4, 5, 6, 7, 8], 0.05)))
        }
        _ => compare_generated(golden, &produced, None),
    }
}

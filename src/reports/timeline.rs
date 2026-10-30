//! The Chrome-trace timeline: per-device event assembly and the `timeline.json`
//! writer.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::Result;
use mspark_model::{
    AccPmuSample, ApiEventRow, ChipTransKind, ChipTransSample, Clock, HbmRecord, HccsRateRow,
    HostCpuUsageRow, InterSocSample, LlcRecord, NetSample, NicRow, NpuMemRow, OverlapKind,
    OverlapRow, PcieRecord, QosRow, RoceRow, SioSample, TimelineTask, fmt_us, load_sample_string,
};
use rayon::prelude::*;

use crate::reports::{hbm, hccs, host, llc, net, pcie};
use crate::session::{devices, session_for};
use crate::trace::{
    Args, CounterRow, HOST_PID, Num, TraceEvent, Track, args, push_process_meta, push_thread_meta,
    write_counter,
};

const SIO_COUNTERS: [&str; 8] = [
    "req_rx", "rsp_rx", "snp_rx", "dat_rx", "req_tx", "rsp_tx", "snp_tx", "dat_tx",
];
const ACC_PMU_COUNTERS: [&str; 4] = ["read_bandwidth", "read_ost", "write_bandwidth", "write_ost"];

pub fn sio_rows(samples: &[SioSample]) -> Vec<CounterRow> {
    let mut grouped: BTreeMap<(compact_str::CompactString, usize), BTreeMap<u16, u32>> =
        BTreeMap::new();
    for sample in samples {
        for (i, value) in sample.bandwidth.iter().enumerate() {
            grouped
                .entry((sample.ts_us.clone(), i))
                .or_default()
                .insert(sample.die, *value);
        }
    }
    grouped
        .into_iter()
        .map(|((ts, i), dies)| CounterRow {
            name: compact_str::CompactString::from(SIO_COUNTERS[i]),
            tid: 0,
            ts,
            args: dies
                .into_iter()
                .map(|(die, value)| {
                    let key: &'static str = match die {
                        0 => "die 0",
                        1 => "die 1",
                        _ => "die other",
                    };
                    (key, Num::Float(value as f64))
                })
                .collect(),
        })
        .collect()
}

pub fn acc_pmu_rows(samples: &[AccPmuSample]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 4);
    for sample in samples {
        for (i, name) in ACC_PMU_COUNTERS.iter().enumerate() {
            rows.push(CounterRow {
                name: compact_str::CompactString::from(*name),
                tid: 0,
                ts: sample.ts_us.clone(),
                args: smallvec::smallvec![("value", Num::Int(sample.values[i]))],
            });
        }
    }
    rows
}

pub fn inter_soc_rows(samples: &[InterSocSample]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        rows.push(CounterRow {
            name: compact_str::CompactString::from("L2 Buffer Bw Level"),
            tid: 0,
            ts: sample.ts_us.clone(),
            args: smallvec::smallvec![(
                "L2 Buffer Bw Level",
                Num::Int(sample.l2_buffer_bw_level as u64),
            )],
        });
        rows.push(CounterRow {
            name: compact_str::CompactString::from("Mata Bw Level"),
            tid: 0,
            ts: sample.ts_us.clone(),
            args: smallvec::smallvec![("Mata Bw Level", Num::Int(sample.mata_bw_level as u64))],
        });
    }
    rows
}

pub fn chip_trans_rows(samples: &[ChipTransSample]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let (tid, counters): (u32, [(&str, u64); 2]) = match sample.kind {
            ChipTransKind::Pa => (
                111111,
                [("PA Link Rx", sample.value1), ("PA Link Tx", sample.value2)],
            ),
            ChipTransKind::Pcie => (
                100010,
                [
                    ("PCIE Read Bandwidth", sample.value2),
                    ("PCIE Write Bandwidth", sample.value1),
                ],
            ),
        };
        let id_key = if sample.kind == ChipTransKind::Pa {
            "PA Link ID"
        } else {
            "PCIE ID"
        };
        for (name, value) in counters {
            rows.push(CounterRow {
                name: compact_str::CompactString::from(name),
                tid,
                ts: sample.ts_us.clone(),
                args: smallvec::smallvec![
                    (name, Num::Int(value)),
                    (id_key, Num::Int(sample.id as u64)),
                ],
            });
        }
    }
    rows
}
pub fn sample_csv_list(device: &Path, key: &str) -> Result<Vec<String>> {
    Ok(load_sample_string(device, key)?
        .unwrap_or_default()
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect())
}

pub fn qos_rows(samples: &[QosRow], events: &[String]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * events.len());
    for sample in samples {
        for (i, event) in events.iter().enumerate().take(10) {
            rows.push(CounterRow {
                name: compact_str::format_compact!("QoS {event}"),
                tid: 0,
                ts: sample.ts_us.clone(),
                args: smallvec::smallvec![("value", Num::Int(sample.values[i] as u64))],
            });
        }
    }
    rows
}
pub fn npu_mem_rows(samples: &[NpuMemRow]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 3);
    for sample in samples {
        let owner = if sample.event == 0 { "APP" } else { "Device" };
        let ts = fmt_us(sample.ts_ns, false);
        for (metric, value) in [
            ("DDR", sample.ddr),
            ("HBM", sample.hbm),
            ("Memory", sample.ddr + sample.hbm),
        ] {
            rows.push(CounterRow {
                name: compact_str::format_compact!("{owner}/{metric}"),
                tid: 0,
                ts: ts.clone(),
                args: smallvec::smallvec![("KB", Num::Float(value as f64 / 1024.0))],
            });
        }
    }
    rows
}

pub fn timeline(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let devices = devices(prof, only)?;
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Each device streams its events straight to its own part file, so no
    // `Vec<TraceEvent>` is ever materialised: a full capture produces 2 M+ events
    // and the vector (with its doubling slack) was ~1.9 GB of the peak.
    let parts_dir = out.with_extension("parts");
    std::fs::create_dir_all(&parts_dir)?;
    let parts: Vec<Result<(PathBuf, usize)>> = devices
        .par_iter()
        .enumerate()
        .map(|(index, device)| {
            let path = parts_dir.join(format!("{index:04}.part"));
            let mut writer =
                std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(&path)?);
            let mut first = true;
            let count = device_timeline_into(device, &mut |event| {
                write_event(&mut writer, &mut first, &event)
            })?;
            writer.flush()?;
            Ok((path, count))
        })
        .collect();

    let file = std::fs::File::create(out)?;
    let mut writer = std::io::BufWriter::with_capacity(1 << 20, file);
    writer.write_all(b"[")?;
    let mut first = true;
    let mut total = 0usize;
    for part in parts {
        let (path, count) = part?;
        let mut reader = std::io::BufReader::with_capacity(1 << 20, std::fs::File::open(&path)?);
        if count > 0 {
            if !first {
                writer.write_all(b",")?;
            }
            first = false;
            std::io::copy(&mut reader, &mut writer)?;
        }
        total += count;
        let _ = std::fs::remove_file(&path);
    }
    writer.write_all(b"]")?;
    writer.flush()?;
    let _ = std::fs::remove_dir_all(&parts_dir);
    println!("wrote {total} timeline events to {}", out.display());
    Ok(())
}

fn write_event(
    writer: &mut impl std::io::Write,
    first: &mut bool,
    event: &TraceEvent,
) -> Result<()> {
    if !*first {
        writer.write_all(b",")?;
    }
    *first = false;
    serde_json::to_writer(writer, event)?;
    Ok(())
}

/// Collect one device's events, for the verifier and the summary.
pub fn device_timeline(device: &Path) -> Result<(Vec<TraceEvent>, usize)> {
    let mut events = Vec::new();
    let count = device_timeline_into(device, &mut |event| {
        events.push(event);
        Ok(())
    })?;
    Ok((events, count))
}

/// Build one device's timeline events, calling `emit` for each one in order.
///
/// Events are produced grouped by track, so the caller can serialise immediately
/// and never hold the whole track in memory.
pub fn device_timeline_into(
    device: &Path,
    emit: &mut dyn FnMut(TraceEvent) -> Result<()>,
) -> Result<usize> {
    let session = session_for(device)?;
    let ctx = session.ctx();
    let info = &session.info;
    let mut events = crate::trace::Sink::new(emit);
    let mut total = 0usize;
    {
        let clock = ctx.require::<Clock>()?;
        crate::memtrace::trace("timeline: start");
        let tasks = session.resolve::<TimelineTask>()?;
        crate::memtrace::trace("timeline: TimelineTask resolved");

        let dev = info.device_id.unwrap_or(0);
        let hw_pid = Track::Hardware.pid(info.pid, dev);
        let freq_pid = Track::AiCoreFreq.pid(info.pid, dev);

        for ts in [clock.begin_us, clock.end_us] {
            events.push(TraceEvent::counter(
                compact_str::CompactString::from("AI Core Freq"),
                freq_pid,
                0,
                fmt_us(ts * 1000, false),
                args([("MHz", Num::Int(info.aic_freq_mhz as u64))]),
            ));
        }

        push_process_meta(
            &mut events,
            hw_pid,
            "Ascend Hardware",
            &format!("NPU {dev}"),
            13,
        );

        let mut tids: Vec<u32> = tasks.iter().map(|t| t.tid).collect();
        tids.sort_unstable();
        tids.dedup();
        for tid in tids {
            push_thread_meta(&mut events, hw_pid, tid, &format!("Stream {tid}"), tid);
        }

        for task in tasks.iter() {
            events.push(TraceEvent::complete(
                compact_str::CompactString::from(task.name.as_str()),
                hw_pid,
                task.tid,
                task.ts_us.clone(),
                task.dur_us,
                args([
                    ("Model Id", Num::Int(task.model_id as u64)),
                    ("Task Type", Num::Str(task.task_type.clone())),
                    ("Physic Stream Id", Num::Int(task.stream_id as u64)),
                    ("Task Id", Num::Int(task.task_id as u64)),
                    ("Batch Id", Num::Int(task.batch_id as u64)),
                    ("Subtask Id", Num::Int(task.subtask_id as u64)),
                ]),
            ));
            total += 1;
        }

        // --- CANN host track ---
        crate::memtrace::trace("timeline: cannn phase start");
        let api_events = session.resolve::<ApiEventRow>()?;
        crate::memtrace::trace("timeline: ApiEventRow resolved");
        let host_clock = ctx
            .require::<mspark_model::HostStartLogs>()?
            .0
            .host_clock(ctx.require::<mspark_model::DeviceInfo>()?.host_freq_mhz)?;
        let cann_pid = Track::Cann.pid(info.pid, HOST_PID);
        push_process_meta(&mut events, cann_pid, "CANN", "CPU", 7);
        let mut api_tids: Vec<u32> = api_events.iter().map(|e| e.thread_id).collect();
        api_tids.sort_unstable();
        api_tids.dedup();
        for tid in api_tids {
            push_thread_meta(&mut events, cann_pid, tid, &format!("Thread {tid}"), tid);
        }
        for event in api_events.iter() {
            let (prefix, level_name) = match event.level {
                20_000 => ("AscendCL", "acl"),
                10_000 => ("Node", "node"),
                5_000 => ("Runtime", "runtime"),
                15_000 => ("Model", "model"),
                _ => ("Api", ""),
            };
            let mode = event.tag.clone();
            let id = if event.level == 20_000 {
                event.api_name.clone()
            } else {
                "0".to_string()
            };
            let trace = format!("{prefix}@{}", event.api_name);
            let ts = fmt_us(host_clock.to_ns(event.begin), false);
            let dur = host_clock.duration_us_f64(event.begin, event.end);
            events.push(TraceEvent::complete(
                compact_str::CompactString::from(trace.as_str()),
                cann_pid,
                event.thread_id,
                ts,
                dur,
                args([
                    ("Thread Id", Num::Int(event.thread_id as u64)),
                    ("Mode", Num::Str(mode)),
                    ("level", Num::Str(level_name.to_string())),
                    ("id", Num::Str(id)),
                    ("item_id", Num::Str(event.item_name.clone())),
                ]),
            ));
        }

        // --- SOC profile tracks ---
        let label = format!("NPU {dev}");
        crate::memtrace::trace("timeline: hardware+SOC phase start");
        let sio = session.resolve::<SioSample>()?;
        let acc = session.resolve::<AccPmuSample>()?;
        let inter = session.resolve::<InterSocSample>()?;
        let chip = session.resolve::<ChipTransSample>()?;
        let track_pid = |track: Track| track.pid(info.pid, dev);
        for (track, name) in [
            (Track::Sio, "SIO"),
            (Track::AccPmu, "Acc PMU"),
            (Track::StarsSocInfo, "Stars Soc Info"),
            (Track::ChipTrans, "Stars Chip Trans"),
        ] {
            push_process_meta(&mut events, track_pid(track), name, &label, track as u32);
        }
        for row in sio_rows(&sio) {
            write_counter(&mut events, track_pid(Track::Sio), row);
            total += 1;
        }
        for row in acc_pmu_rows(&acc) {
            write_counter(&mut events, track_pid(Track::AccPmu), row);
            total += 1;
        }
        for row in inter_soc_rows(&inter) {
            write_counter(&mut events, track_pid(Track::StarsSocInfo), row);
            total += 1;
        }
        for row in chip_trans_rows(&chip) {
            write_counter(&mut events, track_pid(Track::ChipTrans), row);
            total += 1;
        }

        // --- Overlap Analysis ---
        let overlap = session.resolve::<OverlapRow>()?;
        let overlap_pid = track_pid(Track::Overlap);
        push_process_meta(&mut events, overlap_pid, "Overlap Analysis", &label, 29);
        for (tid, name) in [
            (0u32, "Communication"),
            (1, "Communication(Not Overlapped)"),
            (2, "Computing"),
            (3, "Free"),
        ] {
            push_thread_meta(&mut events, overlap_pid, tid, name, tid);
        }
        for row in overlap.iter() {
            let (name, tid) = match row.kind {
                OverlapKind::Compute => ("Computing", 2),
                OverlapKind::Free => ("Free", 3),
            };
            events.push(TraceEvent::complete(
                compact_str::CompactString::from(name),
                overlap_pid,
                tid,
                row.ts_us.clone(),
                row.dur_us,
                Args(smallvec::SmallVec::new()),
            ));
            total += 1;
        }

        // --- QoS ---
        crate::memtrace::trace("timeline: SOC counters done");
        let qos = session.resolve::<QosRow>()?;
        let qos_events = sample_csv_list(ctx.device_dir()?, "qosEvents")?;
        let qos_pid = track_pid(Track::Qos);
        push_process_meta(&mut events, qos_pid, "QoS", &label, 28);
        for row in qos_rows(&qos, &qos_events) {
            write_counter(&mut events, qos_pid, row);
            total += 1;
        }

        // --- NPU MEM ---
        let npu_mem = session.resolve::<NpuMemRow>()?;
        let npu_mem_pid = track_pid(Track::Communication);
        // --- CPU Usage (host track) ---
        crate::memtrace::trace("timeline: npu_mem resolved");
        let cpu_usage = session.resolve::<HostCpuUsageRow>()?;
        crate::memtrace::trace("timeline: HostCpuUsageRow resolved");
        let cpu_pid = Track::CpuUsage.pid(info.pid, HOST_PID);
        events.push(TraceEvent::meta(
            "process_name",
            cpu_pid,
            0,
            args([("name", Num::Str("CPU Usage".to_string()))]),
        ));
        events.push(TraceEvent::meta(
            "process_labels",
            cpu_pid,
            0,
            args([("labels", Num::Str("CPU".to_string()))]),
        ));
        events.push(TraceEvent::meta(
            "process_sort_index",
            cpu_pid,
            0,
            args([("sort_index", Num::Int(9))]),
        ));
        let cpu_offset = host::host_local_offset_milli(ctx.root())?;
        for row in host::host_cpu_rows(&cpu_usage, cpu_offset) {
            write_counter(&mut events, cpu_pid, row);
            total += 1;
        }

        // --- HCCS ---
        let hccs = session.resolve::<HccsRateRow>()?;
        let hccs_pid = track_pid(Track::Hccs);
        push_process_meta(&mut events, hccs_pid, "HCCS", &label, 19);
        let nic: Vec<NetSample> = session
            .resolve::<NicRow>()?
            .iter()
            .map(|row| row.0)
            .collect();
        let nic_pid = track_pid(Track::Nic);
        push_process_meta(&mut events, nic_pid, "NIC", &label, 20);
        for row in net::net_rows(&nic, "NIC") {
            write_counter(&mut events, nic_pid, row);
            total += 1;
        }
        let roce: Vec<NetSample> = session
            .resolve::<RoceRow>()?
            .iter()
            .map(|row| row.0)
            .collect();
        let roce_pid = track_pid(Track::Roce);
        push_process_meta(&mut events, roce_pid, "RoCE", &label, 21);
        for row in net::net_rows(&roce, "RoCE") {
            write_counter(&mut events, roce_pid, row);
            total += 1;
        }
        let llc = session.resolve::<LlcRecord>()?;
        let llc_clock = ctx
            .require::<mspark_model::DeviceStartLogs>()?
            .0
            .sampling()?;
        let llc_pid = track_pid(Track::Llc);
        push_process_meta(&mut events, llc_pid, "LLC", &label, 26);
        for row in llc::llc_track_rows(&llc, &llc_clock) {
            write_counter(&mut events, llc_pid, row);
            total += 1;
        }
        let hbm = session.resolve::<HbmRecord>()?;
        let hbm_pid = track_pid(Track::Hbm);
        push_process_meta(&mut events, hbm_pid, "HBM", &label, 23);
        for row in hbm::hbm_track_rows(&hbm) {
            write_counter(&mut events, hbm_pid, row);
            total += 1;
        }
        let pcie = session.resolve::<PcieRecord>()?;
        let pcie_pid = track_pid(Track::Pcie);
        push_process_meta(&mut events, pcie_pid, "PCIe", &label, 22);
        for row in pcie::pcie_track_rows(&pcie) {
            write_counter(&mut events, pcie_pid, row);
            total += 1;
        }
        for row in hccs::hccs_rows(&hccs) {
            write_counter(&mut events, hccs_pid, row);
            total += 1;
        }

        push_process_meta(&mut events, npu_mem_pid, "NPU MEM", &label, 16);
        for row in npu_mem_rows(&npu_mem) {
            write_counter(&mut events, npu_mem_pid, row);
            total += 1;
        }
    }
    let emitted = events.finish()?;
    crate::memtrace::trace("timeline: events emitted");
    debug_assert_eq!(emitted, total, "event count drifted");
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sio_rows_group_dies_under_one_counter_per_slot() {
        let samples = [
            SioSample {
                ts_us: "1.000".into(),
                die: 0,
                bandwidth: [10, 1, 2, 3, 4, 5, 6, 7],
            },
            SioSample {
                ts_us: "1.000".into(),
                die: 1,
                bandwidth: [20, 11, 12, 13, 14, 15, 16, 17],
            },
        ];
        let rows = sio_rows(&samples);
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0].name, "req_rx");
        assert_eq!(rows[0].args.len(), 2);
        assert_eq!(rows[0].args[0].0, "die 0");
        assert_eq!(rows[0].args[1].0, "die 1");
        match rows[0].args[0].1 {
            Num::Float(value) => assert_eq!(value, 10.0),
            _ => panic!("expected a float"),
        }
        match rows[0].args[1].1 {
            Num::Float(value) => assert_eq!(value, 20.0),
            _ => panic!("expected a float"),
        }
        assert_eq!(rows[1].name, "rsp_rx");
    }

    #[test]
    fn acc_pmu_rows_use_the_documented_counter_order() {
        let rows = acc_pmu_rows(&[AccPmuSample {
            ts_us: "1.000".into(),
            values: [1, 2, 3, 4],
        }]);
        let names: Vec<_> = rows.iter().map(|row| row.name.to_string()).collect();
        assert_eq!(
            names,
            ["read_bandwidth", "read_ost", "write_bandwidth", "write_ost"]
        );
        match rows[2].args[0].1 {
            Num::Int(value) => assert_eq!(value, 3),
            _ => panic!("expected an integer"),
        }
    }

    #[test]
    fn chip_trans_rows_keep_the_pa_and_pcie_directions() {
        let pa = chip_trans_rows(&[ChipTransSample {
            ts_us: "1.000".into(),
            kind: ChipTransKind::Pa,
            id: 7,
            value1: 10,
            value2: 20,
        }]);
        assert_eq!(pa.len(), 2);
        assert_eq!(pa[0].name, "PA Link Rx");
        assert_eq!(pa[0].tid, 111111);
        assert_eq!(pa[0].args[0].0, "PA Link Rx");
        assert_eq!(pa[1].name, "PA Link Tx");
        assert_eq!(pa[1].args[1].0, "PA Link ID");

        let pcie = chip_trans_rows(&[ChipTransSample {
            ts_us: "1.000".into(),
            kind: ChipTransKind::Pcie,
            id: 9,
            value1: 10,
            value2: 20,
        }]);
        assert_eq!(pcie.len(), 2);
        assert_eq!(pcie[0].name, "PCIE Read Bandwidth");
        assert_eq!(pcie[0].tid, 100010);
        // PCIe read is value2 and write is value1.
        match pcie[0].args[0].1 {
            Num::Int(value) => assert_eq!(value, 20),
            _ => panic!("expected an integer"),
        }
        match pcie[1].args[0].1 {
            Num::Int(value) => assert_eq!(value, 10),
            _ => panic!("expected an integer"),
        }
        assert_eq!(pcie[0].args[1].0, "PCIE ID");
    }

    #[test]
    fn qos_rows_emit_at_most_ten_events() {
        let mut values = [0u32; 10];
        values[3] = 42;
        let events: Vec<String> = (0..12).map(|i| format!("event{i}")).collect();
        let rows = qos_rows(
            &[QosRow {
                ts_us: "1.000".into(),
                values,
            }],
            &events,
        );
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[3].name, "QoS event3");
        match rows[3].args[0].1 {
            Num::Int(value) => assert_eq!(value, 42),
            _ => panic!("expected an integer"),
        }
    }

    #[test]
    fn npu_mem_rows_name_the_owner_and_convert_to_kb() {
        let rows = npu_mem_rows(&[
            NpuMemRow {
                event: 0,
                ddr: 1024,
                hbm: 2048,
                ts_ns: 1_000,
            },
            NpuMemRow {
                event: 1,
                ddr: 0,
                hbm: 1024,
                ts_ns: 2_000,
            },
        ]);
        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0].name, "APP/DDR");
        assert_eq!(rows[2].name, "APP/Memory");
        assert_eq!(rows[3].name, "Device/DDR");
        match rows[2].args[0].1 {
            Num::Float(value) => assert_eq!(value, 3.0),
            _ => panic!("expected a float"),
        }
    }
}

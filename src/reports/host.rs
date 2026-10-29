//! Host-side reports: `host_cpu_usage` and `host_mem_usage`, plus the counter
//! rows shared with the `CPU Usage` timeline track.

use std::path::Path;

use anyhow::Result;
use mspark_model::{HostCpuUsageRow, HostMemUsageRow};

use crate::reports::write_text;
use crate::session::{devices, session_for};
use crate::trace::{CounterRow, Num};

/// `local_time_offset` for the *host* domain, in milli-microseconds.
///
/// Host tracks use `host/start_info` (the host process's own report), which is a
/// few microseconds away from the device one.
pub fn host_local_offset_milli(prof_root: &Path) -> Result<i128> {
    let text = std::fs::read_to_string(prof_root.join("host").join("start_info"))?;
    let root: serde_json::Value = serde_json::from_str(&text)?;
    let get = |key: &str| -> Result<i128> {
        root.get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i128>().ok())
            .ok_or_else(|| anyhow::anyhow!("host/start_info has no {key}"))
    };
    Ok(get("collectionTimeBegin")? * 1000 - get("clockMonotonicRaw")?)
}

pub fn host_ts_us(start_ns: u64, offset_milli: i128) -> compact_str::CompactString {
    let milli = start_ns as i128 + offset_milli;
    compact_str::format_compact!("{}.{:03}", milli / 1000, milli % 1000)
}

pub fn host_cpu_rows(samples: &[HostCpuUsageRow], offset_milli: i128) -> Vec<CounterRow> {
    samples
        .iter()
        .map(|sample| CounterRow {
            name: compact_str::format_compact!("CPU {}", sample.cpu_no),
            tid: 0,
            ts: host_ts_us(sample.start_ns, offset_milli),
            args: smallvec::smallvec![("Usage(%)", Num::Float(sample.usage))],
        })
        .collect()
}
/// `host_mem_usage.csv` — `Total Memory`, `Peak Used Memory`, `Recommend
/// Memory` in KB.
///
/// Code-derived and **unverified**: no capture with `--host-sys=mem` has been
/// available, so this has never produced a row (see spec/TODO.md, T25).
pub fn host_mem_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Total Memory,Peak Used Memory,Recommend Memory\n");
    let mut rows = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let samples = session.resolve::<HostMemUsageRow>()?;
        let total_kb = ctx
            .require::<mspark_model::HostInfo>()
            .map(|info| info.memory_total_kb)
            .unwrap_or(0);
        let peak = samples
            .iter()
            .map(|sample| sample.usage)
            .fold(0.0f64, f64::max);
        if !samples.is_empty() {
            let used_kb = (peak * total_kb as f64 / 100.0).round() as u64;
            // TODO(T-host-mem): the `Device_id` column is written as `host`
            // because no golden `host_mem_usage` file exists in any capture we
            // have, so the literal cannot be checked. msprof may write the
            // numeric device id here like every other host-side table.
            body.push_str(&format!("host,{total_kb},{used_kb},{used_kb}\n"));
            rows += 1;
        }
    }
    write_text(out, &body)?;
    println!("wrote {rows} rows to {}", out.display());
    Ok(())
}
pub fn host_cpu_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body =
        String::from("Device_id,Total Cpu Numbers,Occupied Cpu Numbers,Recommend Cpu Numbers\n");
    let mut total_rows = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let samples = session.resolve::<HostCpuUsageRow>()?;
        use std::collections::BTreeMap;
        let mut peak: BTreeMap<u64, f64> = BTreeMap::new();
        let mut total = 0u64;
        for sample in samples.iter() {
            if sample.cpu_no == "Avg" {
                continue;
            }
            if let Ok(cpu) = sample.cpu_no.parse::<u64>() {
                total = total.max(cpu + 1);
                let entry = peak.entry(cpu).or_insert(0.0);
                *entry = entry.max(sample.usage);
            }
        }
        // `Total` is the machine's logical CPU count from `host/info.json`
        // (`cpuNums`), not the highest CPU id that happened to be sampled.
        // `HostInfo` is already parsed once into `Ctx`; do not re-read the file.
        let info_cpus = ctx
            .require::<mspark_model::HostInfo>()
            .map(|info| info.cpu_nums as u64)
            .unwrap_or(0);
        let total = info_cpus.max(total);
        let occupied = peak.len();
        let recommend = peak.values().filter(|usage| **usage > 0.0).count();
        body.push_str(&format!("host,{total},{occupied},{recommend}\n"));
        total_rows += 1;
    }
    write_text(out, &body)?;
    println!("wrote {total_rows} rows to {}", out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_add_the_host_offset_in_milli_microseconds() {
        assert_eq!(host_ts_us(1_000, 500), "1.500");
        assert_eq!(host_ts_us(1_000, 0), "1.000");
        assert_eq!(host_ts_us(0, 123), "0.123");
    }

    #[test]
    fn cpu_rows_keep_the_cpu_label_and_usage() {
        let rows = host_cpu_rows(
            &[HostCpuUsageRow {
                start_ns: 2_000,
                end_ns: 3_000,
                cpu_no: "3".to_string(),
                usage: 42.5,
            }],
            0,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "CPU 3");
        assert_eq!(rows[0].ts, "2.000");
        assert_eq!(rows[0].args[0].0, "Usage(%)");
        match rows[0].args[0].1 {
            Num::Float(value) => assert_eq!(value, 42.5),
            _ => panic!("expected a float"),
        }
    }
}

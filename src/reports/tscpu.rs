//! TSCPU reports: `ts_cpu_pmu_events` and `ts_cpu_top_function`.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use mspark_model::{TscpuRecord, load_sample_string};

use crate::reports::write_text;
use crate::session::{devices, session_for};

pub fn ts_cpu_event_name(event: u64) -> &'static str {
    match event {
        0x8 => "Inst_retired",
        0x11 => "Cpu_cycles",
        _ => " ",
    }
}

pub fn ts_cpu_event_ids(device: &Path) -> Result<Vec<u64>> {
    let raw = load_sample_string(device, "ts_cpu_profiling_events")?.unwrap_or_default();
    Ok(raw
        .split(',')
        .filter_map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return None;
            }
            let hex = item.trim_start_matches("0x").trim_start_matches("0X");
            u64::from_str_radix(hex, 16).ok()
        })
        .collect())
}

pub fn ts_cpu_pmu_events(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Event,Name,Count\n");
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let records = session.resolve::<TscpuRecord>()?;
        let mut rows: Vec<(u64, u64)> = ts_cpu_event_ids(&device)?
            .into_iter()
            .map(|event| {
                let total = records
                    .iter()
                    .flat_map(|record| record.0.events.iter().zip(record.0.counts.iter()))
                    .filter(|(value, _)| **value == event)
                    .map(|(_, count)| *count)
                    .sum();
                (event, total)
            })
            .collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.1.to_string()));
        for (event, total) in rows {
            body.push_str(&format!(
                "{dev},0x{event:x},{},{total}\n",
                ts_cpu_event_name(event)
            ));
        }
    }
    write_text(out, &body)?;
    println!("wrote ts_cpu_pmu_events to {}", out.display());
    Ok(())
}

pub fn ts_cpu_top_function(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Function,Cycles,Cycles(%)\n");
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let records = session.resolve::<TscpuRecord>()?;
        let mut counts: Vec<(u64, u64)> = Vec::new();
        let mut index: HashMap<u64, usize> = HashMap::new();
        for record in records.iter() {
            for (&event, &count) in record.0.events.iter().zip(record.0.counts.iter()) {
                if event == 0x11 {
                    match index.get(&record.0.pc) {
                        Some(&i) => counts[i].1 += count,
                        None => {
                            index.insert(record.0.pc, counts.len());
                            counts.push((record.0.pc, count));
                        }
                    }
                }
            }
        }
        let total: u64 = counts.iter().map(|(_, count)| *count).sum();
        if total == 0 {
            continue;
        }
        let mut items = counts;
        items.sort_by_key(|item| std::cmp::Reverse(item.1));
        for (pc, cycles) in items.into_iter().take(5) {
            let rate: f64 = format!("{:.3}", cycles as f64 * 100.0 / total as f64)
                .parse()
                .unwrap_or(0.0);
            body.push_str(&format!("{dev},0x{pc:x},{cycles},{rate}\n"));
        }
    }
    write_text(out, &body)?;
    println!("wrote ts_cpu_top_function to {}", out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_events_get_names_and_unknown_ones_a_blank() {
        assert_eq!(ts_cpu_event_name(0x8), "Inst_retired");
        assert_eq!(ts_cpu_event_name(0x11), "Cpu_cycles");
        assert_eq!(ts_cpu_event_name(0x99), " ");
    }
}

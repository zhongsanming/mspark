//! HBM report and the `HBM` counter track.

use std::path::Path;

use crate::text::py_round3;
use anyhow::Result;
use mspark_model::{HbmRecord, fmt_us};

use crate::reports::write_text;
use crate::session::{devices, session_for};
use crate::trace::{CounterRow, Num};

pub fn hbm_track_rows(rows: &[HbmRecord]) -> Vec<CounterRow> {
    use std::collections::BTreeMap;
    const SCALE: f64 = 256.0 / 8.0 / 1024.0 / 1024.0;

    let mut by_ts: BTreeMap<i128, Vec<(u32, u32, u64)>> = BTreeMap::new();
    for row in rows {
        by_ts
            .entry(row.ts_ns)
            .or_default()
            .push((row.event, row.hbm_id, row.count));
    }
    let mut out = Vec::with_capacity(rows.len());
    let mut prev: Option<i128> = None;
    for (ts_ns, items) in by_ts {
        let delta_ns = prev.map(|p| (ts_ns - p) as f64);
        for &(event, hbm_id, count) in &items {
            // The first sample has no predecessor; msprof clamps it to zero.
            let value = match delta_ns {
                Some(dt) if dt > 0.0 => count as f64 * SCALE / dt * 1e9,
                _ => 0.0,
            };
            let (kind, key): (&str, &'static str) = if event == 0 {
                ("Read", "Read(MB/s)")
            } else {
                ("Write", "Write(MB/s)")
            };
            out.push(CounterRow {
                name: compact_str::format_compact!("HBM {hbm_id}/{kind}"),
                tid: 0,
                ts: fmt_us(ts_ns as u64, false),
                args: smallvec::smallvec![(key, Num::Float(value))],
            });
        }
        prev = Some(ts_ns);
    }
    out
}

pub fn hbm_table(rows: &[HbmRecord]) -> Vec<[String; 3]> {
    use std::collections::BTreeMap;
    const SCALE: f64 = 256.0 / 8.0 / 1024.0 / 1024.0;

    let mut by_ts: BTreeMap<u64, Vec<(u32, u32, u64)>> = BTreeMap::new();
    for row in rows {
        by_ts
            .entry(row.ts_us)
            .or_default()
            .push((row.event, row.hbm_id, row.count));
    }
    let stamps: Vec<u64> = by_ts.keys().copied().collect();
    let mut sums = [[0.0f64; 2]; 4];
    let samples = stamps.len() as f64;
    for window in stamps.windows(2) {
        let dur = (window[1] - window[0]) as f64 * 1000.0;
        if dur <= 0.0 {
            continue;
        }
        let Some(items) = by_ts.get(&window[1]) else {
            continue;
        };
        for &(event, hbm_id, count) in items {
            if hbm_id >= 4 || event >= 2 {
                continue;
            }
            sums[hbm_id as usize][event as usize] += count as f64 * SCALE / dur * 1e9;
        }
    }

    let mean = |hbm: usize, event: usize| {
        if samples > 0.0 {
            sums[hbm][event] / samples
        } else {
            0.0
        }
    };
    let round3 = |v: f64| (v * 1000.0).round() / 1000.0;
    let lanes: [[f64; 2]; 4] = core::array::from_fn(|h| [round3(mean(h, 0)), round3(mean(h, 1))]);
    let mut out = Vec::new();
    let units: [[f64; 2]; 4] = core::array::from_fn(|h| {
        [
            (lanes[h][0] * 1000.0).round(),
            (lanes[h][1] * 1000.0).round(),
        ]
    });
    let avg_read = units.iter().map(|u| u[0]).sum::<f64>() / 4.0 / 1000.0;
    let avg_write = units.iter().map(|u| u[1]).sum::<f64>() / 4.0 / 1000.0;
    out.push([
        "Average".to_string(),
        py_round3(avg_read),
        py_round3(avg_write),
    ]);
    for (hbm, lane) in lanes.iter().enumerate() {
        out.push([hbm.to_string(), py_round3(lane[0]), py_round3(lane[1])]);
    }
    out
}

pub fn hbm_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Metric,Read(MB/s),Write(MB/s)\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        for [metric, read, write] in hbm_table(&session.resolve::<HbmRecord>()?) {
            body.push_str(&format!("{dev},{metric},{read},{write}\n"));
            total += 1;
        }
    }
    write_text(out, &body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCALE: f64 = 256.0 / 8.0 / 1024.0 / 1024.0;

    fn record(ts_us: u64, event: u32, hbm_id: u32, count: u64) -> HbmRecord {
        HbmRecord {
            ts_us,
            ts_ns: ts_us as i128 * 1000,
            count,
            event,
            hbm_id,
        }
    }

    #[test]
    fn track_rows_zero_the_first_sample_and_rate_the_rest() {
        let rows = [record(100, 0, 2, 1_000_000), record(200, 0, 2, 1_000_000)];
        let counters = hbm_track_rows(&rows);
        assert_eq!(counters.len(), 2);
        assert_eq!(counters[0].name, "HBM 2/Read");
        assert_eq!(counters[0].args[0].0, "Read(MB/s)");
        match counters[0].args[0].1 {
            Num::Float(value) => assert_eq!(value, 0.0),
            _ => panic!("expected a float"),
        }
        let expected = 1_000_000.0 * SCALE / 100_000.0 * 1e9;
        match counters[1].args[0].1 {
            Num::Float(value) => {
                assert!(
                    (value - expected).abs() < 1e-6,
                    "value={value} expected={expected}"
                )
            }
            _ => panic!("expected a float"),
        }
    }

    #[test]
    fn write_events_use_the_write_label() {
        let counters = hbm_track_rows(&[record(1, 1, 0, 0)]);
        assert_eq!(counters[0].name, "HBM 0/Write");
        assert_eq!(counters[0].args[0].0, "Write(MB/s)");
    }

    #[test]
    fn table_averages_the_four_lanes() {
        let rows = [
            record(100, 0, 0, 1_000_000),
            record(200, 0, 0, 1_000_000),
            record(100, 1, 0, 1_000_000),
            record(200, 1, 0, 1_000_000),
        ];
        let table = hbm_table(&rows);
        assert_eq!(table.len(), 5);
        assert_eq!(table[0][0], "Average");
        assert_eq!(table[1][0], "0");
        // Read rate over one 100 us window, divided by two samples and four lanes.
        let read = 1_000_000.0 * SCALE / 100_000.0 * 1e9;
        let expected = py_round3(((read / 2.0 * 1000.0).round() / 4.0) / 1000.0);
        assert_eq!(table[0][1], expected);
    }

    #[test]
    fn table_ignores_out_of_range_lanes_and_smaller_stamps() {
        let rows = [
            record(100, 0, 4, 1_000_000),
            record(90, 0, 0, 1_000_000),
            record(200, 0, 0, 1_000_000),
        ];
        let table = hbm_table(&rows);
        // Only the 100→200 window contributes; lane 4 and event 3 are dropped.
        assert_eq!(table.len(), 5);
        assert_eq!(table[0][0], "Average");
    }

    #[test]
    fn empty_table_still_has_an_average_row() {
        let table = hbm_table(&[]);
        assert_eq!(table.len(), 5, "Average plus the four lanes");
        assert_eq!(table[0], ["Average", "0.0", "0.0"]);
    }
}

//! LLC reports and the `LLC` counter track.

use std::path::Path;

use crate::text::py_round3;
use anyhow::Result;
use mspark_model::{LlcRecord, fmt_us};

use crate::reports::{round3, write_text};
use crate::session::{devices, session_for};
use crate::trace::{CounterRow, Num};

pub fn llc_samples(
    rows: &[LlcRecord],
    clock: &mspark_model::SamplingClock,
) -> Vec<(u32, i128, f64, f64)> {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    let mut slots: BTreeMap<(u32, u64), [u64; 8]> = BTreeMap::new();
    for row in rows {
        if let Some(slot) = mspark_model::LLC_READ_SLOTS
            .iter()
            .position(|e| *e == row.event)
        {
            slots.entry((row.l3t_id, row.ts_us)).or_insert([0; 8])[slot] = row.count;
        }
    }
    let mut by_bank: BTreeMap<u32, Vec<(u64, [u64; 8])>> = BTreeMap::new();
    for ((bank, ts_us), counts) in slots {
        by_bank.entry(bank).or_default().push((ts_us, counts));
    }

    let mut out: Vec<(u32, i128, f64, f64)> = Vec::new();
    for (bank, samples) in by_bank {
        let _: BTreeSet<u64> = samples.iter().map(|(ts, _)| *ts).collect();
        let mut prev_ts: Option<u64> = None;
        for (ts_us, e) in samples.iter().copied() {
            let den = e[4] + e[6];
            let hit = if den == 0 {
                0.0
            } else {
                let raw = (e[2] + e[3] + e[5] + e[7]) as f64 / den as f64;
                (raw * 1000.0).round_ties_even() / 1000.0
            };
            let dt_ns = prev_ts.map(|prev| clock.to_ns(ts_us) - clock.to_ns(prev));
            let total = (e[0] + e[1]) as f64;
            // The first sample has no predecessor; msprof reports zero there.
            let tp = match dt_ns {
                Some(dt) if dt > 0 => {
                    round3(total * 1024.0 * 1024.0 * 1e9 / dt as f64 / 256.0 / 1024.0 / 1024.0)
                }
                _ => 0.0,
            };
            out.push((bank, clock.to_ns(ts_us), hit, tp));
            prev_ts = Some(ts_us);
        }
    }
    out
}

pub fn llc_track_rows(rows: &[LlcRecord], clock: &mspark_model::SamplingClock) -> Vec<CounterRow> {
    let mut out = Vec::new();
    for (bank, ts_ns, hit, tp) in llc_samples(rows, clock) {
        let ts = fmt_us(ts_ns as u64, false);
        out.push(CounterRow {
            name: compact_str::format_compact!("LLC {bank} Read/Hit Rate"),
            tid: 0,
            ts: ts.clone(),
            args: smallvec::smallvec![("Hit Rate(%)", Num::Float(hit))],
        });
        out.push(CounterRow {
            name: compact_str::format_compact!("LLC {bank} Read/Throughput"),
            tid: 0,
            ts,
            args: smallvec::smallvec![("Throughput(MB/s)", Num::Float(tp))],
        });
    }
    out
}

pub fn llc_csv_table(rows: &[LlcRecord], clock: &mspark_model::SamplingClock) -> Vec<[String; 3]> {
    use std::collections::BTreeMap;
    let mut per_bank: BTreeMap<u32, (f64, f64, f64)> = BTreeMap::new();
    for (bank, _, hit, tp) in llc_samples(rows, clock) {
        let entry = per_bank.entry(bank).or_insert((0.0, 0.0, 0.0));
        entry.0 += hit;
        entry.1 += tp;
        entry.2 += 1.0;
    }
    let lanes: Vec<(u32, f64, f64)> = per_bank
        .into_iter()
        .map(|(bank, (hit, tp, n))| {
            let n = if n > 0.0 { n } else { 1.0 };
            (bank, hit / n, tp / n)
        })
        .collect();
    let mut out = Vec::new();
    if !lanes.is_empty() {
        let avg_hit = lanes.iter().map(|(_, h, _)| *h).sum::<f64>() / lanes.len() as f64;
        let avg_tp = lanes.iter().map(|(_, _, t)| *t).sum::<f64>() / lanes.len() as f64;
        out.push([
            "Average".to_string(),
            py_round3(avg_hit * 100.0),
            py_round3(avg_tp),
        ]);
    }
    for (bank, hit, tp) in lanes {
        out.push([bank.to_string(), py_round3(hit * 100.0), py_round3(tp)]);
    }
    out
}

pub fn llc_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Mode,Task,Hit Rate(%),Throughput(MB/s)\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let rows = session.resolve::<LlcRecord>()?;
        let clock = ctx
            .require::<mspark_model::DeviceStartLogs>()?
            .0
            .sampling()?;
        for [task, hit, tp] in llc_csv_table(&rows, &clock) {
            body.push_str(&format!("{dev},read,{task},{hit},{tp}\n"));
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

    /// A `SamplingClock` with zero start/delta and a 1 ms wall offset, so
    /// `to_ns(raw_us) == raw_us * 1000 + 1_000_000`.
    fn clock() -> mspark_model::SamplingClock {
        use std::fs;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "mspark-llc-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("start_info"),
            r#"{"collectionTimeBegin":"1000","clockMonotonicRaw":"0"}"#,
        )
        .unwrap();
        fs::write(
            dir.join("dev_start.log"),
            "clock_monotonic_raw:1000000000\n",
        )
        .unwrap();
        fs::write(
            dir.join("host_start.log"),
            "clock_monotonic_raw:1000000000\n",
        )
        .unwrap();
        let clock = mspark_model::StartLogs::load(&dir)
            .unwrap()
            .sampling()
            .unwrap();
        fs::remove_dir_all(&dir).ok();
        clock
    }

    fn record(ts_us: u64, counts: [u64; 8]) -> Vec<LlcRecord> {
        mspark_model::LLC_READ_SLOTS
            .iter()
            .zip(counts)
            .map(|(event, count)| LlcRecord {
                ts_us,
                count,
                event: *event,
                l3t_id: 0,
            })
            .collect()
    }

    #[test]
    fn samples_compute_hit_rate_and_first_throughput_is_zero() {
        let mut rows = record(100, [100, 50, 25, 5, 10, 20, 10, 10]);
        rows.extend(record(200, [100, 50, 25, 5, 10, 20, 10, 10]));
        let samples = llc_samples(&rows, &clock());
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].0, 0);
        assert_eq!(samples[0].2, 3.0);
        assert_eq!(samples[0].3, 0.0, "the first sample has no predecessor");
        assert_eq!(samples[1].3, 5859.375);
    }

    #[test]
    fn track_rows_emit_hit_rate_and_throughput_per_bank() {
        let rows = llc_track_rows(&record(100, [0; 8]), &clock());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "LLC 0 Read/Hit Rate");
        assert_eq!(rows[0].args[0].0, "Hit Rate(%)");
        assert_eq!(rows[1].name, "LLC 0 Read/Throughput");
        assert_eq!(rows[1].args[0].0, "Throughput(MB/s)");
    }

    #[test]
    fn csv_table_has_an_average_row_before_the_banks() {
        let mut rows = record(100, [0; 8]);
        rows.extend(record(200, [0; 8]));
        let table = llc_csv_table(&rows, &clock());
        assert_eq!(table.len(), 2);
        assert_eq!(table[0][0], "Average");
        assert_eq!(table[1][0], "0");
    }

    #[test]
    fn csv_table_is_empty_without_known_events() {
        let unknown = LlcRecord {
            ts_us: 1,
            count: 1,
            event: 0xff,
            l3t_id: 0,
        };
        assert!(llc_csv_table(&[unknown], &clock()).is_empty());
    }
}

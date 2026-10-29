//! HCCS report and the `HCCS` counter track.

use std::path::Path;

use crate::text::py_round3;
use anyhow::Result;
use mspark_model::{HccsRateRow, fmt_us};

use crate::reports::{round3, write_text};
use crate::session::{devices, session_for};
use crate::trace::{CounterRow, Num};

pub fn hccs_rows(samples: &[HccsRateRow]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let ts = fmt_us(sample.ts_ns as u64, false);
        rows.push(CounterRow {
            name: compact_str::CompactString::from("Tx"),
            tid: 0,
            ts: ts.clone(),
            args: smallvec::smallvec![("Tx(MB/s)", Num::Float(round3(sample.tx)))],
        });
        rows.push(CounterRow {
            name: compact_str::CompactString::from("Rx"),
            tid: 0,
            ts,
            args: smallvec::smallvec![("Rx(MB/s)", Num::Float(round3(sample.rx)))],
        });
    }
    rows
}
pub fn hccs_table(rows: &[HccsRateRow]) -> Vec<[String; 4]> {
    let mut out = Vec::new();
    for (mode, values) in [
        (
            "Tx (MB/s)",
            rows.iter().map(|row| row.tx).collect::<Vec<_>>(),
        ),
        (
            "Rx (MB/s)",
            rows.iter().map(|row| row.rx).collect::<Vec<_>>(),
        ),
    ] {
        let (max, min, avg) = if values.is_empty() {
            (0.0, 0.0, 0.0)
        } else {
            let max = values.iter().copied().fold(f64::MIN, f64::max);
            let min = values.iter().copied().fold(f64::MAX, f64::min);
            let avg = values.iter().sum::<f64>() / values.len() as f64;
            (max, min, avg)
        };
        out.push([
            mode.to_string(),
            py_round3(max),
            py_round3(min),
            py_round3(avg),
        ]);
    }
    out
}

pub fn hccs_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Mode,Max,Min,Average\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let rows = session.resolve::<HccsRateRow>()?;
        for [mode, max, min, avg] in hccs_table(&rows) {
            body.push_str(&format!("{dev},{mode},{max},{min},{avg}\n"));
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

    fn sample(ts_ns: i128, tx: f64, rx: f64) -> HccsRateRow {
        HccsRateRow { ts_ns, tx, rx }
    }

    #[test]
    fn track_rows_round_and_pair_tx_rx() {
        let rows = hccs_rows(&[sample(1_000_000, 1.23456, 2.34567)]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "Tx");
        assert_eq!(rows[0].args[0].0, "Tx(MB/s)");
        assert_eq!(rows[1].name, "Rx");
        match rows[0].args[0].1 {
            Num::Float(value) => assert_eq!(value, 1.235),
            _ => panic!("expected a float"),
        }
        assert_eq!(rows[0].ts, rows[1].ts);
    }

    #[test]
    fn table_reports_max_min_and_average() {
        let rows = [
            sample(1, 1.0, 4.0),
            sample(2, 2.0, 6.0),
            sample(3, 3.0, 5.0),
        ];
        let table = hccs_table(&rows);
        assert_eq!(table.len(), 2);
        assert_eq!(table[0], ["Tx (MB/s)", "3.0", "1.0", "2.0"]);
        assert_eq!(table[1], ["Rx (MB/s)", "6.0", "4.0", "5.0"]);
    }

    #[test]
    fn empty_table_still_has_both_modes() {
        assert_eq!(
            hccs_table(&[]),
            vec![
                ["Tx (MB/s)", "0.0", "0.0", "0.0"],
                ["Rx (MB/s)", "0.0", "0.0", "0.0"],
            ]
        );
    }
}

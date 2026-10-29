//! PCIe report and the `PCIe` counter track.

use std::path::Path;

use crate::text::py_round3;
use anyhow::Result;
use mspark_model::{PcieRecord, fmt_us};

use crate::reports::write_text;
use crate::session::{devices, session_for};
use crate::trace::{CounterRow, Num};

const PCIE_MEASURES: [(&str, bool); 7] = [
    ("Tx_p_avg(MB/s)", true),
    ("Tx_np_avg(MB/s)", true),
    ("Tx_cpl_avg(MB/s)", true),
    ("Tx_latency_avg(us)", false),
    ("Rx_p_avg(MB/s)", true),
    ("Rx_np_avg(MB/s)", true),
    ("Rx_cpl_avg(MB/s)", true),
];

const BYTE_US_TO_MB_S: f64 = 1e9 / 1000.0 / 1024.0 / 1024.0;

pub fn pcie_value(raw: u32, bandwidth: bool) -> f64 {
    if raw == 0xF_FFFF {
        return 0.0;
    }
    if bandwidth {
        raw as f64 * BYTE_US_TO_MB_S
    } else {
        raw as f64 / 1000.0
    }
}
pub fn pcie_track_rows(rows: &[PcieRecord]) -> Vec<CounterRow> {
    let mut out = Vec::with_capacity(rows.len() * 4);
    for row in rows {
        let ts = fmt_us(row.ts_ns as u64, false);
        // (name, tx field, rx field, is_latency); field index = table column - 1.
        for (name, tx, rx, latency) in [
            ("PCIe_post", 3usize, 15usize, false),
            ("PCIe_nonpost", 6, 18, false),
            ("PCIe_cpl", 9, 21, false),
            ("PCIe_nonpost_latency", 12, 0, true),
        ] {
            let value = |idx: usize| {
                let raw = row.fields[idx];
                if raw == 0xF_FFFF {
                    0.0
                } else if latency {
                    raw as f64 / 1000.0
                } else {
                    // The track keeps full precision; only the CSV rounds.
                    raw as f64 * 1e6 / 1024.0 / 1024.0
                }
            };
            let rx_value = if latency { 0.0 } else { value(rx) };
            out.push(CounterRow {
                name: compact_str::CompactString::from(name),
                tid: 0,
                ts: ts.clone(),
                args: smallvec::smallvec![
                    ("Rx", Num::Float(rx_value)),
                    ("Tx", Num::Float(value(tx)))
                ],
            });
        }
    }
    out
}

pub fn pcie_table(rows: &[PcieRecord]) -> Vec<[String; 4]> {
    // msprof reports the newest sample: `order by timestamp desc limit 1`.
    let Some(last) = rows.iter().max_by_key(|row| row.ts) else {
        return Vec::new();
    };
    PCIE_MEASURES
        .iter()
        .enumerate()
        .map(|(i, (name, bandwidth))| {
            let base = 1 + i * 3;
            [
                name.to_string(),
                py_round3(pcie_value(last.fields[base], *bandwidth)),
                py_round3(pcie_value(last.fields[base + 1], *bandwidth)),
                py_round3(pcie_value(last.fields[base + 2], *bandwidth)),
            ]
        })
        .collect()
}

pub fn pcie_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Mode,Min,Max,Avg\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        for [mode, min, max, avg] in pcie_table(&session.resolve::<PcieRecord>()?) {
            body.push_str(&format!("{dev},{mode},{min},{max},{avg}\n"));
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

    fn record(ts: u64, fields: [u32; 22]) -> PcieRecord {
        PcieRecord {
            ts,
            ts_ns: ts as i128 * 1000,
            fields,
        }
    }

    #[test]
    fn sentinel_is_reported_as_zero() {
        assert_eq!(pcie_value(0xF_FFFF, true), 0.0);
        assert_eq!(pcie_value(0xF_FFFF, false), 0.0);
    }

    #[test]
    fn bandwidth_is_converted_from_bytes_per_microsecond() {
        assert_eq!(pcie_value(1024, true), 976.5625);
        assert_eq!(pcie_value(1000, false), 1.0);
    }

    #[test]
    fn table_uses_the_newest_sample() {
        let mut older = [1; 22];
        older[1] = 1000;
        let mut newer = [2; 22];
        newer[1] = 1024;
        let table = pcie_table(&[record(1, older), record(2, newer)]);
        assert_eq!(table.len(), PCIE_MEASURES.len());
        assert_eq!(table[0][0], "Tx_p_avg(MB/s)");
        assert_eq!(table[0][1], py_round3(pcie_value(1024, true)));
        assert_eq!(table[0][2], py_round3(pcie_value(2, true)));
        assert_eq!(table[0][3], py_round3(pcie_value(2, true)));
    }

    #[test]
    fn table_has_a_latency_row_in_microseconds() {
        let table = pcie_table(&[record(1, [1000; 22])]);
        let latency = &table[3];
        assert_eq!(latency[0], "Tx_latency_avg(us)");
        assert_eq!(latency[1], "1.0");
    }

    #[test]
    fn track_rows_emit_four_counters_per_sample() {
        let rows = pcie_track_rows(&[record(5, [10; 22])]);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].name, "PCIe_post");
        assert_eq!(rows[3].name, "PCIe_nonpost_latency");
        assert_eq!(rows[0].args.len(), 2);
        assert_eq!(rows[0].args[0].0, "Rx");
        assert_eq!(rows[0].args[1].0, "Tx");
        // The latency track has no Rx value.
        match rows[3].args[0].1 {
            Num::Float(value) => assert_eq!(value, 0.0),
            _ => panic!("expected a float"),
        }
    }
}

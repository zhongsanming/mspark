//! NIC/RoCE reports and counter tracks.

use std::path::Path;

use crate::text::{nic_timestamp_column, py_round3};
use anyhow::{Result, bail};
use mspark_model::{NetSample, NicRow, RoceRow, fmt_us};

use crate::reports::{round3, write_text};
use crate::session::{devices, session_for};
use crate::trace::{CounterRow, Num};

/// `start_info` fields needed by the NIC/RoCE summary.
pub fn start_info_times(device: &Path) -> Result<(i128, i128)> {
    let path = find_start_info(device)?;
    let text = mspark_model::errors::read_to_string(&path)?;
    let root: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| mspark_model::json_error(&path, &text, &error))?;
    let get = |key: &str| -> Result<i128> {
        root.get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i128>().ok())
            .ok_or_else(|| anyhow::anyhow!("start_info has no {key}"))
    };
    Ok((get("collectionTimeBegin")?, get("clockMonotonicRaw")?))
}

pub fn find_start_info(device: &Path) -> Result<std::path::PathBuf> {
    for entry in std::fs::read_dir(device)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("start_info")
        {
            return Ok(entry.path());
        }
    }
    bail!("no start_info in {}", device.display())
}
pub fn net_summary_rows(samples: &[NetSample], device: &Path) -> Result<Vec<[String; 10]>> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let ts: Vec<i128> = samples.iter().map(|s| s.ts_ns).collect();
    let duration_ns = (*ts.iter().max().unwrap() - *ts.iter().min().unwrap()) as u64;
    let (begin_us, cmr_ns) = start_info_times(device)?;
    let stamp = nic_timestamp_column(begin_us, cmr_ns, duration_ns);
    let last = samples.last().unwrap();
    let bandwidth = last.bandwidth;
    let efficiency = |byte_rate: u64| {
        if bandwidth > 0 {
            round3(byte_rate as f64 * 8.0 / (bandwidth as f64 * 1024.0 * 1024.0))
        } else {
            0.0
        }
    };
    Ok(vec![[
        stamp,
        py_round3(bandwidth as f64),
        py_round3(efficiency(last.rx[1])),
        py_round3(last.rx[0] as f64),
        "0.0".to_string(),
        "0.0".to_string(),
        py_round3(efficiency(last.tx[1])),
        py_round3(last.tx[0] as f64),
        "0.0".to_string(),
        "0.0".to_string(),
    ]])
}

pub fn net_csv(prof: &Path, only: Option<&str>, out: &Path, roce: bool) -> Result<()> {
    let mut body = String::from(
        "Device_id,Timestamp(us),Bandwidth(MB/s),Rx Bandwidth efficiency(%),rxPacket/s,\
         rxError rate(%),rxDropped rate(%),Tx Bandwidth efficiency(%),txPacket/s,\
         txError rate(%),txDropped rate(%),funcId\n",
    );
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let samples: Vec<NetSample> = if roce {
            session.resolve::<RoceRow>()?.iter().map(|r| r.0).collect()
        } else {
            session.resolve::<NicRow>()?.iter().map(|r| r.0).collect()
        };
        for row in net_summary_rows(&samples, &device)? {
            body.push_str(&format!("{dev},{},0\n", row.join(",")));
            total += 1;
        }
    }
    write_text(out, &body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

pub fn nic_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    net_csv(prof, only, out, false)
}

pub fn roce_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    net_csv(prof, only, out, true)
}
pub fn net_rows(samples: &[NetSample], ty: &str) -> Vec<CounterRow> {
    let mut out = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let ts = fmt_us(sample.ts_ns as u64, false);
        for (dir, counters) in [("Rx", &sample.rx), ("Tx", &sample.tx)] {
            let bytes_per_s = counters[1] as f64;
            let efficiency = if sample.bandwidth > 0 {
                round3(bytes_per_s * 8.0 / (sample.bandwidth as f64 * 1024.0 * 1024.0))
            } else {
                0.0
            };
            let (eff_key, pkt_key, err_key, drop_key): (
                &'static str,
                &'static str,
                &'static str,
                &'static str,
            ) = if dir == "Rx" {
                (
                    "Rx Bandwidth Efficiency",
                    "Rx Packets",
                    "Rx Error Rate",
                    "Rx Dropped Rate",
                )
            } else {
                (
                    "Tx Bandwidth Efficiency",
                    "Tx Packets",
                    "Tx Error Rate",
                    "Tx Dropped Rate",
                )
            };
            out.push(CounterRow {
                name: compact_str::format_compact!("Port 0/{dir}"),
                tid: 0,
                ts: ts.clone(),
                args: smallvec::smallvec![
                    (eff_key, Num::Float(efficiency)),
                    (pkt_key, Num::Float(counters[0] as f64)),
                    (err_key, Num::Float(0.0)),
                    (drop_key, Num::Float(0.0)),
                ],
            });
        }
        let _ = ty;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(bandwidth: u64, rx_bytes: u64, tx_bytes: u64) -> NetSample {
        NetSample {
            ts_ns: 1,
            bandwidth,
            rx: [10, rx_bytes, 0, 0, 1, 2],
            tx: [20, tx_bytes, 0, 0, 3, 4],
        }
    }

    #[test]
    fn track_rows_compute_efficiency_and_keep_the_packet_counts() {
        let rows = net_rows(&[sample(1000, 128_000_000, 64_000_000)], "NIC");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "Port 0/Rx");
        assert_eq!(rows[1].name, "Port 0/Tx");
        let args = &rows[0].args;
        assert_eq!(args[0].0, "Rx Bandwidth Efficiency");
        match args[0].1 {
            Num::Float(value) => assert_eq!(value, 0.977),
            _ => panic!("expected a float"),
        }
        match args[1].1 {
            Num::Float(value) => assert_eq!(value, 10.0),
            _ => panic!("expected a float"),
        }
        // Error/drop rates are always zero in the raw sample.
        for key in ["Rx Error Rate", "Rx Dropped Rate"] {
            assert!(args.iter().any(|(name, _)| *name == key));
        }
    }

    #[test]
    fn zero_bandwidth_does_not_divide_by_zero() {
        let rows = net_rows(&[sample(0, 100, 100)], "RoCE");
        match rows[0].args[0].1 {
            Num::Float(value) => assert_eq!(value, 0.0),
            _ => panic!("expected a float"),
        }
    }
}

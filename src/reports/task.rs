//! Task-level reports: `task_time`, `op_summary`, `api_statistic`, `op_statistic`.

use std::path::Path;

use anyhow::Result;
use mspark_model::{ApiStatisticRow, OpStatisticRow, OpSummaryRow, TaskTimeRow};

use crate::artifacts::write_chunks;
use crate::reports::write_text;
use crate::session::{devices, per_device, session_for};

pub fn task_time(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let chunks = per_device(prof, only, |session| {
        let mut body = String::new();
        for row in session.resolve::<TaskTimeRow>()?.iter() {
            body.push_str(&row.to_csv());
        }
        Ok(body)
    })?;
    write_chunks(out, TaskTimeRow::HEADER, chunks)?;
    Ok(())
}
pub fn op_summary(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    // Each device streams its rows straight into a part file: the table is 300 k+
    // rows of 46 strings and collecting it cost ~800 MB of the export's peak.
    let parts = per_device(prof, only, |session| {
        let path = out.with_extension(format!("{}.part", session.info.device_id.unwrap_or(0)));
        let mut writer = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(&path)?);
        let mut rows = 0usize;
        session.resolve_into::<OpSummaryRow>(&mut |row| {
            use std::io::Write as _;
            writer.write_all(row.to_csv().as_bytes())?;
            rows += 1;
            Ok(())
        })?;
        writer.flush()?;
        Ok((path, rows))
    })?;
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Write the header, then append each device's part file: with per-device part
    // files we never hold more than one row per device in memory.
    let file = std::fs::File::create(out)?;
    let mut writer = std::io::BufWriter::with_capacity(1 << 20, file);
    use std::io::Write as _;
    let header = OpSummaryRow::HEADER.join(",");
    writer.write_all(header.as_bytes())?;
    writer.write_all(b"\n")?;
    let mut total = 0usize;
    for (path, rows) in parts {
        total += rows;
        let mut part = std::fs::File::open(&path)?;
        std::io::copy(&mut part, &mut writer)?;
        let _ = std::fs::remove_file(&path);
    }
    writer.flush()?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}
pub fn api_statistic(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body =
        String::from("Device_id,Level,API Name,Time(us),Count,Avg(us),Min(us),Max(us),Variance\n");
    if let Some(device) = devices(prof, only)?.first() {
        let session = session_for(device)?;
        for row in session.resolve::<ApiStatisticRow>()?.iter() {
            body.push_str(&format!(
                "host,{},{},{},{},{},{},{},{}\n",
                mspark_model::api_level_name(row.level),
                row.api_name,
                row.time,
                row.count,
                row.avg,
                row.min,
                row.max,
                row.variance
            ));
        }
    }
    write_text(out, &body)?;
    println!("wrote api_statistic to {}", out.display());
    Ok(())
}

pub fn op_statistic(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from(
        "Device_id,OP Type,Core Type,Count,Total Time(us),Min Time(us),Avg Time(us),Max Time(us),Ratio(%)\n",
    );
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        for row in session.resolve::<OpStatisticRow>()?.iter() {
            body.push_str(&format!(
                "{},{},{},{},{},{},{},{},{}\n",
                row.device_id,
                row.op_type,
                row.core_type,
                row.count,
                row.total_time,
                row.min,
                row.avg,
                row.max,
                row.ratio
            ));
        }
    }
    write_text(out, &body)?;
    println!("wrote op_statistic to {}", out.display());
    Ok(())
}

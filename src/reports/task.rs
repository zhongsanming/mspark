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
    let chunks = per_device(prof, only, |session| {
        let mut body = String::new();
        for row in session.resolve::<OpSummaryRow>()?.iter() {
            body.push_str(&row.to_csv());
        }
        Ok(body)
    })?;
    write_chunks(out, &OpSummaryRow::HEADER.join(","), chunks)?;
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

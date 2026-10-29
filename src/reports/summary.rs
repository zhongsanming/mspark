//! `summary.csv`: one row per device plus a `Total` row.

use std::path::Path;

use anyhow::Result;
use mspark_model::{NpuMemRow, OpSummaryRow, TaskTimeRow};

use crate::reports::timeline::device_timeline;
use crate::reports::write_text;
use crate::session::{devices, session_for};

pub fn summary(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Task Count,OP Count,Timeline Events,Peak Memory(KB)\n");
    let (mut tasks_total, mut ops_total, mut events_total) = (0u64, 0u64, 0u64);
    let mut peak_max = 0u64;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        // Each `resolve` takes the store lock for one computation only, so the
        // "do not let the guard escape" hazard that used to live here is gone.
        let tasks = session.resolve::<TaskTimeRow>()?.len() as u64;
        let ops = session.resolve::<OpSummaryRow>()?.len() as u64;
        let (_, events) = device_timeline(&device)?;
        let peak = session
            .resolve::<NpuMemRow>()?
            .iter()
            .map(|row| (row.ddr + row.hbm) / 1024)
            .max()
            .unwrap_or(0);
        body.push_str(&format!("{dev},{tasks},{ops},{events},{peak}\n"));
        tasks_total += tasks;
        ops_total += ops;
        events_total += events as u64;
        peak_max = peak_max.max(peak);
    }
    body.push_str(&format!(
        "Total,{tasks_total},{ops_total},{events_total},{peak_max}\n"
    ));
    write_text(out, &body)?;
    println!("wrote summary for all devices to {}", out.display());
    Ok(())
}

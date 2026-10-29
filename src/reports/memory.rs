//! Memory and cache reports: `npu_mem`, `npu_module_mem`, `l2_cache`, `dvpp`.

use std::collections::HashMap;
use std::path::Path;

use crate::text::py_float;
use anyhow::Result;
use mspark_model::{DvppRow, L2CacheRow, NpuMemRow, NpuModuleMemRow, TimelineTask, fmt_us};

use crate::artifacts::write_chunks;
use crate::reports::write_text;
use crate::session::{devices, per_device, session_for};

pub fn l2_cache(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let chunks = per_device(prof, only, |session| {
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let rows = session.resolve::<L2CacheRow>()?;
        let tasks = session.resolve::<TimelineTask>()?;
        let op_map: HashMap<(u32, u32), String> = tasks
            .iter()
            .map(|task| ((task.stream_id, task.task_id), task.name.clone()))
            .collect();
        let mut body = String::new();
        for row in rows.iter() {
            let op_name = op_map
                .get(&(row.stream_id as u32, row.task_id as u32))
                .cloned()
                .unwrap_or_else(|| "N/A".to_string());
            body.push_str(&format!(
                "{dev},{},{},N/A,{},{}\n",
                row.stream_id,
                row.task_id,
                py_float(row.victim_rate),
                op_name
            ));
        }
        Ok(body)
    })?;

    let mut body = String::from("Device_id,Stream Id,Task Id,Hit Rate,Victim Rate,Op Name\n");
    let mut total = 0usize;
    for chunk in chunks {
        total += chunk.lines().count();
        body.push_str(&chunk);
    }
    write_text(out, &body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

pub fn dvpp(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from(
        "Device_id,Dvpp Id,Engine Type,Engine ID,All Time(us),All Frame,All Utilization(%)\n",
    );
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        for row in session.resolve::<DvppRow>()?.iter() {
            body.push_str(&format!(
                "{dev},{},{},{},{:?},{:?},{}\n",
                row.dvpp_id,
                mspark_model::dvpp_engine_name(row.engine_type),
                row.engine_id,
                row.all_time,
                row.all_frame,
                row.all_utilization
            ));
            total += 1;
        }
    }
    write_text(out, &body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

pub fn npu_mem(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,event,ddr(KB),hbm(KB),memory(KB),timestamp(us)\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        for row in session.resolve::<NpuMemRow>()?.iter() {
            body.push_str(&format!(
                "{dev},{},{},{},{},{}\n",
                mspark_model::npu_mem_event(row.event),
                row.ddr / 1024,
                row.hbm / 1024,
                (row.ddr + row.hbm) / 1024,
                fmt_us(row.ts_ns, true)
            ));
            total += 1;
        }
    }
    write_text(out, &body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

pub fn npu_module_mem(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let chunks = per_device(prof, only, |session| {
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let mut body = String::new();
        for row in session.resolve::<NpuModuleMemRow>()?.iter() {
            body.push_str(&format!(
                "{dev},{},{},{},NPU:{dev}\n",
                mspark_model::module_name(row.module_id),
                fmt_us(row.ts_ns, true),
                row.total_size / 1024
            ));
        }
        Ok(body)
    })?;
    write_chunks(
        out,
        "Device_id,Component,Timestamp(us),Total Reserved(KB),Device",
        chunks,
    )?;
    Ok(())
}

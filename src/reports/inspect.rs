//! Inspection commands: `graph`, `stats`, and their formatting helpers.

use std::path::Path;

use anyhow::{Result, bail};
use mspark_model::{CtxStreams, DeviceInfo, LpmInfo, PmuData, StarsSocEntry, TrackFlip};

use crate::session::{devices, session_for};

/// Print the node graph (`mspark_df::graph!` holds every edge at compile time).
pub fn graph(format: &str) -> Result<()> {
    let nodes = mspark_model::NODE_GRAPH;
    match format {
        "mermaid" => {
            println!("graph TD");
            for (name, deps) in nodes {
                if deps.is_empty() {
                    println!("  {name}[(\"{name}\")]");
                }
                for dep in *deps {
                    println!("  {dep} --> {name}");
                }
            }
        }
        "dot" => {
            println!("digraph mspark {{");
            println!("  rankdir=LR;");
            println!("  node [shape=box, fontsize=10];");
            for (name, deps) in nodes {
                if deps.is_empty() {
                    println!("  \"{name}\";");
                }
                for dep in *deps {
                    println!("  \"{dep}\" -> \"{name}\";");
                }
            }
            println!("}}");
        }
        other => bail!("unknown graph format `{other}`; valid: dot, mermaid"),
    }
    Ok(())
}

pub fn stats(prof: &Path, only: Option<&str>) -> Result<()> {
    let devices = devices(prof, only)?;
    println!("profile: {}", prof.display());
    if devices.is_empty() {
        println!("no device_* directories found");
        return Ok(());
    }

    for device in &devices {
        let session = session_for(device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let soc = session.resolve::<StarsSocEntry>()?;
        let pmu = session.resolve::<PmuData>()?;
        let track = session.resolve::<TrackFlip>()?;
        let lpm = session.resolve::<LpmInfo>()?;

        let soc_hist = mspark_model::nodes::sources::stars_soc::decode_stars_soc(
            &ctx.stream(mspark_model::Stream::StarsSoc)?,
            info.expand_status,
        )?
        .1;
        let pmu_hist = mspark_model::nodes::sources::ffts_profile::decode_ffts_profile(
            &ctx.stream(mspark_model::Stream::FftsProfile)?,
            info.expand_status,
        )?
        .1;
        let track_hist = mspark_model::nodes::sources::ts_track::decode_ts_track(
            &ctx.stream(mspark_model::Stream::TsTrack)?,
        )?
        .1;

        println!(
            "\n== {} ==",
            device.file_name().unwrap_or_default().to_string_lossy()
        );
        print_info(info);
        println!(
            "  stars_soc   {:>6} rows  funcs={}",
            soc.len(),
            hist(&soc_hist)
        );
        println!(
            "  ffts_profile{:>6} rows  funcs={}",
            pmu.len(),
            hist(&pmu_hist)
        );
        println!(
            "  ts_track    {:>6} rows  funcs={}",
            track.len(),
            hist(&track_hist)
        );
        if let Some(first) = lpm.first() {
            println!(
                "  lpmFreqConv  count={} kind={} samples={}",
                first.count,
                first.kind,
                first.samples.len()
            );
        } else {
            println!("  lpmFreqConv  absent");
        }
        println!("  store: {} nodes memoised", session.memoised_nodes());
    }
    Ok(())
}
pub fn print_info(info: &DeviceInfo) {
    println!(
        "  chip={} device_id={:?} ai_core={} aiv={} aic={}MHz aiv={}MHz hwts={}MHz host={}MHz expand={}",
        info.chip.name(),
        info.device_id,
        info.ai_core_num,
        info.aiv_num,
        info.aic_freq_mhz,
        info.aiv_freq_mhz,
        info.hwts_freq_mhz,
        info.host_freq_mhz,
        info.expand_status,
    );
}

pub fn hist<const N: usize>(hist: &[u32; N]) -> String {
    let mut items: Vec<(usize, u32)> = hist
        .iter()
        .enumerate()
        .filter(|(_, count)| **count != 0)
        .map(|(i, &count)| (i, count))
        .collect();
    items.sort();
    items
        .into_iter()
        .map(|(i, count)| format!("{i}:{count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

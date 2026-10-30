//! Artifact selection and the export fan-out.

use std::io::Write as _;
use std::path::Path;

use anyhow::{Context as _, Result};
use rayon::prelude::*;

use crate::reports::communication::communication_csv;
use crate::reports::hbm::hbm_csv;
use crate::reports::hccs::hccs_csv;
use crate::reports::host::{host_cpu_csv, host_mem_csv};
use crate::reports::llc::llc_csv;
use crate::reports::memory::{dvpp, l2_cache, npu_mem, npu_module_mem};
use crate::reports::net::{nic_csv, roce_csv};
use crate::reports::pcie::pcie_csv;
use crate::reports::summary::summary;
use crate::reports::task::{api_statistic, op_statistic, op_summary, task_time};
use crate::reports::timeline::timeline;
use crate::reports::tscpu::{ts_cpu_pmu_events, ts_cpu_top_function};
use crate::text::civil_from_days;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Artifact {
    TaskTime,
    OpSummary,
    ApiStatistic,
    OpStatistic,
    NpuMem,
    NpuModuleMem,
    Dvpp,
    L2Cache,
    TsCpuPmuEvents,
    TsCpuTopFunction,
    Hccs,
    Hbm,
    Communication,
    HostCpuUsage,
    HostMemUsage,
    Llc,
    Nic,
    Pcie,
    Roce,
    Summary,
    Timeline,
}

impl Artifact {
    pub fn name(self) -> &'static str {
        match self {
            Artifact::TaskTime => "task_time",
            Artifact::OpSummary => "op_summary",
            Artifact::ApiStatistic => "api_statistic",
            Artifact::OpStatistic => "op_statistic",
            Artifact::NpuMem => "npu_mem",
            Artifact::NpuModuleMem => "npu_module_mem",
            Artifact::Dvpp => "dvpp",
            Artifact::L2Cache => "l2_cache",
            Artifact::TsCpuPmuEvents => "ts_cpu_pmu_events",
            Artifact::TsCpuTopFunction => "ts_cpu_top_function",
            Artifact::Hccs => "hccs",
            Artifact::Hbm => "hbm",
            Artifact::Communication => "communication_statistic",
            Artifact::HostCpuUsage => "host_cpu_usage",
            Artifact::HostMemUsage => "host_mem_usage",
            Artifact::Llc => "llc_read_write",
            Artifact::Nic => "nic",
            Artifact::Pcie => "pcie",
            Artifact::Roce => "roce",
            Artifact::Summary => "summary",
            Artifact::Timeline => "timeline",
        }
    }

    pub fn file(self) -> &'static str {
        match self {
            Artifact::TaskTime => "task_time.csv",
            Artifact::OpSummary => "op_summary.csv",
            Artifact::ApiStatistic => "api_statistic.csv",
            Artifact::OpStatistic => "op_statistic.csv",
            Artifact::NpuMem => "npu_mem.csv",
            Artifact::NpuModuleMem => "npu_module_mem.csv",
            Artifact::Dvpp => "dvpp.csv",
            Artifact::L2Cache => "l2_cache.csv",
            Artifact::TsCpuPmuEvents => "ts_cpu_pmu_events.csv",
            Artifact::TsCpuTopFunction => "ts_cpu_top_function.csv",
            Artifact::Hccs => "hccs.csv",
            Artifact::Hbm => "hbm.csv",
            Artifact::Communication => "communication_statistic.csv",
            Artifact::HostCpuUsage => "host_cpu_usage.csv",
            Artifact::HostMemUsage => "host_mem_usage.csv",
            Artifact::Llc => "llc_read_write.csv",
            Artifact::Nic => "nic.csv",
            Artifact::Pcie => "pcie.csv",
            Artifact::Roce => "roce.csv",
            Artifact::Summary => "summary.csv",
            Artifact::Timeline => "timeline.json",
        }
    }
}

pub const CSV_ARTIFACTS: [Artifact; 20] = [
    Artifact::TaskTime,
    Artifact::OpSummary,
    Artifact::ApiStatistic,
    Artifact::OpStatistic,
    Artifact::NpuMem,
    Artifact::NpuModuleMem,
    Artifact::Dvpp,
    Artifact::L2Cache,
    Artifact::TsCpuPmuEvents,
    Artifact::TsCpuTopFunction,
    Artifact::Hccs,
    Artifact::Hbm,
    Artifact::Communication,
    Artifact::HostCpuUsage,
    Artifact::HostMemUsage,
    Artifact::Llc,
    Artifact::Nic,
    Artifact::Pcie,
    Artifact::Roce,
    Artifact::Summary,
];

pub const ALL_ARTIFACTS: [Artifact; 21] = [
    Artifact::TaskTime,
    Artifact::OpSummary,
    Artifact::ApiStatistic,
    Artifact::OpStatistic,
    Artifact::NpuMem,
    Artifact::NpuModuleMem,
    Artifact::Dvpp,
    Artifact::L2Cache,
    Artifact::TsCpuPmuEvents,
    Artifact::TsCpuTopFunction,
    Artifact::Hccs,
    Artifact::Hbm,
    Artifact::Communication,
    Artifact::HostCpuUsage,
    Artifact::HostMemUsage,
    Artifact::Llc,
    Artifact::Nic,
    Artifact::Pcie,
    Artifact::Roce,
    Artifact::Summary,
    Artifact::Timeline,
];

pub fn parse_outputs(spec: &str) -> Result<Vec<Artifact>> {
    let mut selected: Vec<Artifact> = Vec::new();
    for raw in spec.split(',') {
        let token = raw.trim().to_ascii_lowercase();
        if token.is_empty() {
            continue;
        }
        let group: Vec<Artifact> = match token.as_str() {
            "all" => ALL_ARTIFACTS.to_vec(),
            "csv" => CSV_ARTIFACTS.to_vec(),
            "timeline" => vec![Artifact::Timeline],
            other => {
                let artifact = ALL_ARTIFACTS
                    .iter()
                    .copied()
                    .find(|artifact| artifact.name() == other)
                    .ok_or_else(|| {
                        let known = ALL_ARTIFACTS
                            .iter()
                            .map(|artifact| artifact.name())
                            .collect::<Vec<_>>()
                            .join(", ");
                        anyhow::anyhow!(
                            "unknown output `{other}`; valid: all, csv, timeline, {known}"
                        )
                    })?;
                vec![artifact]
            }
        };
        for artifact in group {
            if !selected.contains(&artifact) {
                selected.push(artifact);
            }
        }
    }
    if selected.is_empty() {
        return Err(anyhow::anyhow!("no outputs selected"));
    }
    Ok(selected)
}

pub fn timestamp_dir() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    let (year, month, day) = civil_from_days((secs / 86_400) as i64);
    let rem = secs % 86_400;
    format!(
        "mspark_{year:04}{month:02}{day:02}_{:02}{:02}{:02}",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Write per-device CSV chunks straight to disk instead of building the whole
/// file in memory first: these tables reach hundreds of MB and the extra copy
/// used to double the peak.
pub fn write_chunks(
    out: &Path,
    header: &str,
    chunks: impl IntoIterator<Item = String>,
) -> Result<usize> {
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(out).with_context(|| format!("creating {}", out.display()))?;
    let mut writer = std::io::BufWriter::with_capacity(1 << 20, file);
    writer.write_all(header.as_bytes())?;
    writer.write_all(b"\n")?;
    let mut total = 0usize;
    for chunk in chunks {
        total += chunk.lines().count();
        writer.write_all(chunk.as_bytes())?;
    }
    writer.flush()?;
    println!("wrote {total} rows to {}", out.display());
    Ok(total)
}

pub fn export(
    prof: &Path,
    outputs: Option<&str>,
    out_dir: Option<&Path>,
    flat: bool,
    only: Option<&str>,
) -> Result<()> {
    let artifacts = parse_outputs(outputs.unwrap_or("all"))?;
    let base = out_dir.unwrap_or_else(|| Path::new("."));
    let dir = if flat {
        base.to_path_buf()
    } else {
        base.join(timestamp_dir())
    };
    std::fs::create_dir_all(&dir)?;
    // Artifacts write independent files, so fan them out. They share the
    // per-device session (and therefore its memoised tables), so a node needed
    // by several reports is computed once; the store lock serialises conflicting
    // computations on one device.
    let results: Vec<Result<()>> = artifacts
        .par_iter()
        .map(|artifact| {
            write_artifact(*artifact, prof, only, &dir)
                .with_context(|| format!("exporting `{}`", artifact.name()))
        })
        .collect();
    // Report every failing artifact, not just the first: they run in parallel and
    // one bad stream usually breaks several.
    let failures: Vec<anyhow::Error> = results.into_iter().filter_map(Result::err).collect();
    if !failures.is_empty() {
        for failure in &failures[1..] {
            eprintln!("{failure:?}\n");
        }
        return Err(failures.into_iter().next().expect("checked non-empty"));
    }
    println!("outputs written to {}", dir.display());
    Ok(())
}

pub fn write_artifact(
    artifact: Artifact,
    prof: &Path,
    only: Option<&str>,
    dir: &Path,
) -> Result<()> {
    let out = dir.join(artifact.file());
    match artifact {
        Artifact::TaskTime => task_time(prof, only, &out),
        Artifact::OpSummary => op_summary(prof, only, &out),
        Artifact::ApiStatistic => api_statistic(prof, only, &out),
        Artifact::OpStatistic => op_statistic(prof, only, &out),
        Artifact::NpuMem => npu_mem(prof, only, &out),
        Artifact::NpuModuleMem => npu_module_mem(prof, only, &out),
        Artifact::Dvpp => dvpp(prof, only, &out),
        Artifact::L2Cache => l2_cache(prof, only, &out),
        Artifact::TsCpuPmuEvents => ts_cpu_pmu_events(prof, only, &out),
        Artifact::TsCpuTopFunction => ts_cpu_top_function(prof, only, &out),
        Artifact::Hccs => hccs_csv(prof, only, &out),
        Artifact::Hbm => hbm_csv(prof, only, &out),
        Artifact::Communication => communication_csv(prof, only, &out),
        Artifact::HostCpuUsage => host_cpu_csv(prof, only, &out),
        Artifact::HostMemUsage => host_mem_csv(prof, only, &out),
        Artifact::Llc => llc_csv(prof, only, &out),
        Artifact::Nic => nic_csv(prof, only, &out),
        Artifact::Pcie => pcie_csv(prof, only, &out),
        Artifact::Roce => roce_csv(prof, only, &out),
        Artifact::Summary => summary(prof, only, &out),
        Artifact::Timeline => timeline(prof, only, &out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn artifact_names_and_files_are_unique() {
        let mut names = HashSet::new();
        let mut files = HashSet::new();
        for artifact in ALL_ARTIFACTS {
            assert!(
                names.insert(artifact.name()),
                "duplicate name {}",
                artifact.name()
            );
            assert!(
                files.insert(artifact.file()),
                "duplicate file {}",
                artifact.file()
            );
        }
    }

    /// `csv` must stay "everything except the timeline". Kept as a test because
    /// the two lists are hand-written; adding an artifact to only one of them
    /// used to be an easy slip.
    #[test]
    fn csv_artifacts_are_all_without_timeline() {
        let expected: Vec<Artifact> = ALL_ARTIFACTS
            .into_iter()
            .filter(|artifact| *artifact != Artifact::Timeline)
            .collect();
        assert_eq!(CSV_ARTIFACTS.to_vec(), expected);
    }

    #[test]
    fn parse_outputs_is_ordered_and_deduplicated() {
        assert_eq!(
            parse_outputs("timeline,task_time,timeline").unwrap(),
            vec![Artifact::Timeline, Artifact::TaskTime]
        );
        assert_eq!(parse_outputs("csv").unwrap(), CSV_ARTIFACTS.to_vec());
        assert!(
            parse_outputs("")
                .unwrap_err()
                .to_string()
                .contains("no outputs selected")
        );
    }

    #[test]
    fn parse_outputs_rejects_unknown_tokens() {
        let error = parse_outputs("task_time,nope").unwrap_err().to_string();
        assert!(error.contains("unknown output `nope`"), "{error}");
        assert!(error.contains("task_time"), "{error}");
    }
}

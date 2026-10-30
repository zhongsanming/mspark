//! Command-line interface. The implementation lives in the library so it can be
//! unit-tested; this file only defines the verbs and dispatches them.

use std::path::PathBuf;

use anyhow::Result;
use mspark::commands::*;
use palc::{Parser, Subcommand};

/// Ascend profiling dataflow reimplementation.
#[derive(Parser)]
#[command(name = "mspark")]
struct Cli {
    /// Print the version and exit.
    #[arg(long)]
    version: bool,
    /// A PROF_* directory. Runs the default export when no subcommand is given.
    prof: Option<PathBuf>,
    /// Comma-separated artifacts: `all`, `csv`, `timeline`, or report names.
    #[arg(long)]
    outputs: Option<String>,
    /// Base directory for generated files (default: current directory).
    #[arg(long)]
    out_dir: Option<PathBuf>,
    /// Write directly into `--out-dir` instead of a timestamped subdirectory.
    #[arg(long)]
    flat: bool,
    /// Restrict to one device directory, e.g. `device_7`.
    #[arg(long)]
    device: Option<String>,
    /// Make verification commands exit non-zero on any mismatch.
    #[arg(long)]
    strict: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print the node dependency graph (generated from the `graph!` table).
    Graph {
        /// Output format: `dot` (default) or `mermaid`.
        #[arg(long, default_value = "dot")]
        format: String,
    },
    /// Decode the raw device streams and report per-stream statistics.
    Stats {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
    },
    /// Export `task_time.csv` from the device streams.
    TaskTime {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `task_time` table against a golden CSV.
    Verify {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `task_time_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `op_summary.csv` from the device + host streams.
    OpSummary {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `op_summary` table against a golden CSV.
    VerifyOpSummary {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `op_summary_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export the Ascend Hardware timeline (`msprof.json`).
    Timeline {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output JSON path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated hardware timeline against a golden `msprof_*.json`.
    VerifyTimeline {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `msprof_*.json`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `npu_mem.csv`.
    NpuMem {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `npu_mem` table against a golden CSV.
    VerifyNpuMem {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `npu_mem_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `npu_module_mem.csv`.
    NpuModuleMem {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `npu_module_mem` table against a golden CSV.
    VerifyNpuModuleMem {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `npu_module_mem_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `dvpp.csv`.
    Dvpp {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `dvpp` table against a golden CSV.
    VerifyDvpp {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `dvpp_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export the `hccs` summary table.
    HccsCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the `hccs` summary table against a golden CSV.
    VerifyHccsCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `hccs_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export the `hbm` summary table.
    HbmCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the `llc_read_write` table against a golden CSV.
    VerifyLlcCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `llc_read_write_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Compare the `nic` summary table against a golden CSV.
    VerifyNicCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `nic_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Compare the `roce` summary table against a golden CSV.
    VerifyRoceCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `roce_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Compare the `pcie` summary table against a golden CSV.
    VerifyPcieCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `pcie_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Compare the `hbm` summary table against a golden CSV.
    VerifyHbmCsv {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `hbm_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `l2_cache.csv`.
    L2Cache {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `l2_cache` table against a golden CSV.
    VerifyL2Cache {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `l2_cache_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `ts_cpu_pmu_events.csv`.
    TsCpuPmuEvents {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `ts_cpu_pmu_events` table against a golden CSV.
    VerifyTsCpuPmuEvents {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `ts_cpu_pmu_events_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `ts_cpu_top_function.csv`.
    TsCpuTopFunction {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `ts_cpu_top_function` table against a golden CSV.
    VerifyTsCpuTopFunction {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `ts_cpu_top_function_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `api_statistic.csv`.
    ApiStatistic {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `api_statistic` table against a golden CSV.
    VerifyApiStatistic {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `api_statistic_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Export `op_statistic.csv`.
    OpStatistic {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Output CSV path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Compare the generated `op_statistic` table against a golden CSV.
    VerifyOpStatistic {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Golden `op_statistic_*.csv`.
        #[arg(long)]
        golden: PathBuf,
    },
    /// Check every artifact against the capture's goldens and the recorded
    /// baseline, failing when any artifact gets *less* close than it was.
    VerifyAll {
        /// A PROF_* directory.
        prof: PathBuf,
        /// Restrict to one device directory, e.g. `device_7`.
        #[arg(long)]
        device: Option<String>,
        /// Directory holding the golden `mindstudio_profiler_output` files.
        #[arg(long)]
        golden_dir: Option<PathBuf>,
        /// Baseline file (default: `spec/baseline.txt`).
        #[arg(long)]
        baseline: Option<PathBuf>,
        /// Write the current distances to the baseline file instead of checking.
        #[arg(long)]
        update_baseline: bool,
        /// Decimal places numeric cells are compared at (default: 3).
        #[arg(long, default_value_t = 3)]
        places: u32,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{}", mspark::errors::report(&error));
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    mspark::verify::set_strict(cli.strict);
    if cli.version {
        println!("mspark {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let Some(command) = cli.command else {
        let prof = cli.prof.as_deref().ok_or_else(|| {
            anyhow::anyhow!("missing PROF_* directory (or pass a subcommand, e.g. `verify`)")
        })?;
        return export(
            prof,
            cli.outputs.as_deref(),
            cli.out_dir.as_deref(),
            cli.flat,
            cli.device.as_deref(),
        );
    };
    match command {
        Command::Graph { format } => graph(&format),
        Command::Stats { prof, device } => stats(&prof, device.as_deref()),
        Command::TaskTime { prof, device, out } => task_time(&prof, device.as_deref(), &out),
        Command::Verify {
            prof,
            device,
            golden,
        } => verify(&prof, device.as_deref(), &golden),
        Command::OpSummary { prof, device, out } => op_summary(&prof, device.as_deref(), &out),
        Command::VerifyOpSummary {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::OpSummary, &prof, device.as_deref(), &golden),
        Command::NpuMem { prof, device, out } => npu_mem(&prof, device.as_deref(), &out),
        Command::VerifyNpuMem {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::NpuMem, &prof, device.as_deref(), &golden),
        Command::NpuModuleMem { prof, device, out } => {
            npu_module_mem(&prof, device.as_deref(), &out)
        }
        Command::VerifyNpuModuleMem {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::NpuModuleMem, &prof, device.as_deref(), &golden),
        Command::Dvpp { prof, device, out } => dvpp(&prof, device.as_deref(), &out),
        Command::VerifyDvpp {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Dvpp, &prof, device.as_deref(), &golden),
        Command::HccsCsv { prof, device, out } => hccs_csv(&prof, device.as_deref(), &out),
        Command::VerifyHccsCsv {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Hccs, &prof, device.as_deref(), &golden),
        Command::HbmCsv { prof, device, out } => hbm_csv(&prof, device.as_deref(), &out),
        Command::VerifyHbmCsv {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Hbm, &prof, device.as_deref(), &golden),
        Command::VerifyLlcCsv {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Llc, &prof, device.as_deref(), &golden),
        Command::VerifyNicCsv {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Nic, &prof, device.as_deref(), &golden),
        Command::VerifyRoceCsv {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Roce, &prof, device.as_deref(), &golden),
        Command::VerifyPcieCsv {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::Pcie, &prof, device.as_deref(), &golden),
        Command::L2Cache { prof, device, out } => l2_cache(&prof, device.as_deref(), &out),
        Command::VerifyL2Cache {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::L2Cache, &prof, device.as_deref(), &golden),
        Command::TsCpuPmuEvents { prof, device, out } => {
            ts_cpu_pmu_events(&prof, device.as_deref(), &out)
        }
        Command::VerifyTsCpuPmuEvents {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::TsCpuPmuEvents, &prof, device.as_deref(), &golden),
        Command::TsCpuTopFunction { prof, device, out } => {
            ts_cpu_top_function(&prof, device.as_deref(), &out)
        }
        Command::VerifyTsCpuTopFunction {
            prof,
            device,
            golden,
        } => verify_csv(
            Artifact::TsCpuTopFunction,
            &prof,
            device.as_deref(),
            &golden,
        ),
        Command::ApiStatistic { prof, device, out } => {
            api_statistic(&prof, device.as_deref(), &out)
        }
        Command::VerifyApiStatistic {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::ApiStatistic, &prof, device.as_deref(), &golden),
        Command::OpStatistic { prof, device, out } => op_statistic(&prof, device.as_deref(), &out),
        Command::VerifyOpStatistic {
            prof,
            device,
            golden,
        } => verify_csv(Artifact::OpStatistic, &prof, device.as_deref(), &golden),
        Command::VerifyAll {
            prof,
            device,
            golden_dir,
            baseline,
            update_baseline,
            places,
        } => verify_all(
            &prof,
            device.as_deref(),
            golden_dir.as_deref(),
            baseline.as_deref(),
            update_baseline,
            places,
        ),
        Command::Timeline { prof, device, out } => timeline(&prof, device.as_deref(), &out),
        Command::VerifyTimeline {
            prof,
            device,
            golden,
        } => verify_timeline(&prof, device.as_deref(), &golden),
    }
}

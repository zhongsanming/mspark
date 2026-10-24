use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context as _, Result, bail};
use mspark::text::{civil_from_days, nic_timestamp_column, parse_us_to_ns, py_float, py_round3};
use mspark::verify::{compare_generated, gate, read_rows};
use mspark_df::{Ctx, Store, resolve};
use mspark_model::CtxStreams;
use mspark_model::NetSample;
use mspark_model::{
    AccPmuSample, ApiEventRow, ApiStatisticRow, ChipTransKind, ChipTransSample, Clock, DeviceInfo,
    DvppRow, HbmRecord, HcclInfoRow, HcclOpRow, HccsRateRow, HostCpuUsageRow, HostMemUsageRow,
    InterSocSample, L2CacheRow, LlcRecord, LpmInfo, NicRow, NpuMemRow, NpuModuleMemRow,
    OpStatisticRow, OpSummaryRow, OverlapKind, OverlapRow, PcieRecord, PmuData, QosRow, RoceRow,
    SioSample, StarsSocEntry, TaskTimeRow, TimelineTask, TrackFlip, TscpuRecord, fmt_us,
    load_device_info, load_dictionaries, load_sample_string,
};
use palc::{Parser, Subcommand};
use rayon::prelude::*;
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};

/// Ascend profiling dataflow reimplementation.
#[derive(Parser)]
#[command(name = "mspark")]
struct Cli {
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    mspark::verify::set_strict(cli.strict);
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

/// Track order in the msprof viewer. The value is written to
/// `process_sort_index` *and* packed into bits 5..10 of the track pid, which is
/// why it appears in both the writer and the verifier; naming it once removes a
/// whole class of copy-paste mistakes.
#[derive(Clone, Copy)]
#[repr(u32)]
enum Track {
    Cann = 7,
    CpuUsage = 9,
    Hardware = 13,
    AiCoreFreq = 14,
    Communication = 16,
    AccPmu = 18,
    Hccs = 19,
    Nic = 20,
    Roce = 21,
    Pcie = 22,
    Hbm = 23,
    StarsSocInfo = 24,
    ChipTrans = 25,
    Llc = 26,
    Sio = 27,
    Qos = 28,
    Overlap = 29,
}

/// Host-side tracks (CANN, CPU usage, …) live on this pseudo device id.
const HOST_PID: u32 = 31;

impl Track {
    /// `(pid << 10) | (sort_index << 5) | device`.
    fn pid(self, host_pid: u32, device: u32) -> u32 {
        (host_pid << 10) | ((self as u32) << 5) | device
    }
}

fn list_devices(prof: &Path) -> Result<Vec<PathBuf>> {
    let mut devices = Vec::new();
    for entry in std::fs::read_dir(prof)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("device_") {
            devices.push(entry.path());
        }
    }
    devices.sort();
    Ok(devices)
}

/// The `PROF_*` directory that owns a `device_N` directory.
fn prof_root_of(device: &Path) -> &Path {
    device.parent().unwrap_or(device)
}

/// All device directories under `prof`.
///
/// A single capture holds `device_*` directly. A multi-process run (e.g. one
/// DDP rank per process) produces one `PROF_*` directory per rank instead, so a
/// directory of such captures is accepted as one logical profile.
fn collect_devices(prof: &Path) -> Result<Vec<PathBuf>> {
    let mut devices = list_devices(prof)?;
    if devices.is_empty() {
        for entry in std::fs::read_dir(prof)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("PROF_") {
                devices.extend(list_devices(&entry.path())?);
            }
        }
    }
    devices.sort();
    devices.dedup();
    Ok(devices)
}

fn devices(prof: &Path, only: Option<&str>) -> Result<Vec<PathBuf>> {
    let all = collect_devices(prof)?;
    match only {
        Some(name) => {
            let matched: Vec<PathBuf> = all
                .into_iter()
                .filter(|device| device.file_name().is_some_and(|n| n == name))
                .collect();
            if matched.is_empty() {
                bail!("no device named `{name}` under {}", prof.display());
            }
            Ok(matched)
        }
        None => Ok(all),
    }
}

/// Build the immutable context: device info + syscnt conversion clock.
/// One [`Ctx`] per device, built once.
///
/// Building a context parses `info.json`, the start logs and the host
/// dictionaries, so doing it per artifact (35 call sites) is pure waste. Like
/// [`store_for`], the cache is keyed by the device directory.
/// One device's parsed context and memoised node tables.
///
/// A device owns its `Store`, so artifacts never share mutable state by
/// accident and the tables are dropped when the device is done. Each
/// `resolve` takes the lock for the duration of one node computation, which
/// removes the old "do not let the guard outlive this call" hazard.
struct DeviceSession {
    ctx: Arc<Ctx>,
    info: DeviceInfo,
    store: Mutex<Store>,
}

impl DeviceSession {
    fn resolve<T: mspark_df::Node>(&self) -> Result<Arc<Vec<T>>> {
        let mut store = self.store.lock().expect("store lock");
        resolve::<T>(&mut store, &self.ctx)
    }

    fn ctx(&self) -> &Ctx {
        &self.ctx
    }
}

/// Per-device session, built at most once per process.
///
/// The lock is held **across** the build on purpose: artifacts run in parallel,
/// and releasing it before `build_ctx` made every racing artifact miss the cache
/// and rebuild (20 artifacts → 20 loads of the same start logs, visible in
/// `strace` as 20+ opens of one `start_info`).
fn session_for(device: &Path) -> Result<Arc<DeviceSession>> {
    static SESSIONS: OnceLock<Mutex<HashMap<PathBuf, Arc<DeviceSession>>>> = OnceLock::new();
    let registry = SESSIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = registry.lock().expect("session registry");
    if let Some(session) = guard.get(device) {
        return Ok(session.clone());
    }
    let (ctx, info) = build_ctx(device)?;
    let session = Arc::new(DeviceSession {
        ctx: Arc::new(ctx),
        info,
        store: Mutex::new(Store::new()),
    });
    guard.insert(device.to_path_buf(), session.clone());
    Ok(session)
}

/// Run `f` for every selected device, in parallel.
fn per_session<R, F>(prof: &Path, only: Option<&str>, f: F) -> Result<Vec<R>>
where
    R: Send,
    F: Fn(&DeviceSession) -> Result<R> + Sync + Send,
{
    devices(prof, only)?
        .par_iter()
        .map(|device| {
            let session = session_for(device)?;
            f(&session)
        })
        .collect()
}

fn build_ctx(device: &Path) -> Result<(Ctx, DeviceInfo)> {
    let info = load_device_info(device)
        .with_context(|| format!("loading context for {}", device.display()))?;
    // One parse of the start logs feeds both the device clock and every node
    // that needs a different view of the same four files.
    let device_logs = mspark_model::StartLogs::load(device)
        .with_context(|| format!("loading start logs for {}", device.display()))?;
    let clock = device_logs
        .clock(info.host_freq_mhz, info.hwts_freq_mhz)
        .with_context(|| format!("deriving the device clock for {}", device.display()))?;
    // Each device carries its own capture root, so a merged multi-process
    // profile still reads host data from the right directory.
    let root = prof_root_of(device);
    let mut ctx = Ctx::new(root).with_device(device);
    ctx.insert(info.clone());
    ctx.insert(clock);
    // The four start logs are shared by all five clock derivations; parsing them
    // once here keeps the nodes from re-reading the same files (a single export
    // used to open `start_info` 20+ times per device).
    ctx.insert(mspark_model::DeviceStartLogs(device_logs));
    ctx.insert(mspark_model::HostStartLogs(
        mspark_model::StartLogs::load(&root.join("host"))
            .with_context(|| format!("loading host start logs for {}", root.display()))?,
    ));
    ctx.insert(mspark_model::HostInfo::load(root)?);
    ctx.insert(mspark_model::SampleConfig::load(device)?);
    // Host id→text dictionaries are loaded before any node runs.
    let (hash, types) = load_dictionaries(&root.join("host").join("data"));
    ctx.insert(hash);
    ctx.insert(types);
    Ok((ctx, info))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Artifact {
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
    fn name(self) -> &'static str {
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

    fn file(self) -> &'static str {
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

const CSV_ARTIFACTS: [Artifact; 20] = [
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

const ALL_ARTIFACTS: [Artifact; 21] = [
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

fn parse_outputs(spec: &str) -> Result<Vec<Artifact>> {
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

fn timestamp_dir() -> String {
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
fn write_chunks(
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

fn export(
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
    // Artifacts are independent (own file, own `Store`), so fan them out.
    let results: Vec<Result<()>> = artifacts
        .par_iter()
        .map(|artifact| write_artifact(*artifact, prof, only, &dir))
        .collect();
    for result in results {
        result?;
    }
    println!("outputs written to {}", dir.display());
    Ok(())
}

fn write_artifact(artifact: Artifact, prof: &Path, only: Option<&str>, dir: &Path) -> Result<()> {
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

fn pcie_value(raw: u32, bandwidth: bool) -> f64 {
    if raw == 0xF_FFFF {
        return 0.0;
    }
    if bandwidth {
        raw as f64 * BYTE_US_TO_MB_S
    } else {
        raw as f64 / 1000.0
    }
}

/// One LLC sample: `(bank, wall_ns, hit fraction, MiB/s)`.
/// `start_info` fields needed by the NIC/RoCE summary.
fn start_info_times(device: &Path) -> Result<(i128, i128)> {
    let text = std::fs::read_to_string(find_start_info(device)?)?;
    let root: serde_json::Value = serde_json::from_str(&text)?;
    let get = |key: &str| -> Result<i128> {
        root.get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i128>().ok())
            .ok_or_else(|| anyhow::anyhow!("start_info has no {key}"))
    };
    Ok((get("collectionTimeBegin")?, get("clockMonotonicRaw")?))
}

fn find_start_info(device: &Path) -> Result<std::path::PathBuf> {
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

fn net_summary_rows(samples: &[NetSample], device: &Path) -> Result<Vec<[String; 10]>> {
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

fn net_csv(prof: &Path, only: Option<&str>, out: &Path, roce: bool) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn nic_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    net_csv(prof, only, out, false)
}

fn roce_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    net_csv(prof, only, out, true)
}

fn net_rows(samples: &[NetSample], ty: &str) -> Vec<CounterRow> {
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

/// `local_time_offset` for the *host* domain, in milli-microseconds.
///
/// Host tracks use `host/start_info` (the host process's own report), which is a
/// few microseconds away from the device one.
fn host_local_offset_milli(prof_root: &Path) -> Result<i128> {
    let text = std::fs::read_to_string(prof_root.join("host").join("start_info"))?;
    let root: serde_json::Value = serde_json::from_str(&text)?;
    let get = |key: &str| -> Result<i128> {
        root.get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i128>().ok())
            .ok_or_else(|| anyhow::anyhow!("host/start_info has no {key}"))
    };
    Ok(get("collectionTimeBegin")? * 1000 - get("clockMonotonicRaw")?)
}

fn host_ts_us(start_ns: u64, offset_milli: i128) -> compact_str::CompactString {
    let milli = start_ns as i128 + offset_milli;
    compact_str::format_compact!("{}.{:03}", milli / 1000, milli % 1000)
}

fn host_cpu_rows(samples: &[HostCpuUsageRow], offset_milli: i128) -> Vec<CounterRow> {
    samples
        .iter()
        .map(|sample| CounterRow {
            name: compact_str::format_compact!("CPU {}", sample.cpu_no),
            tid: 0,
            ts: host_ts_us(sample.start_ns, offset_milli),
            args: smallvec::smallvec![("Usage(%)", Num::Float(sample.usage))],
        })
        .collect()
}

/// `communication_statistic.csv`: op types and counts only.
///
/// The time columns are deliberately **not** emitted: msprof's per-op
/// `start`/`end` come from the host-produced `HCCLOpSingleDevice` table, and the
/// raw streams carry no op window (every task of an op shares the op stamp).
/// See spec/TODO.md (T22).
/// `host_mem_usage.csv` — `Total Memory`, `Peak Used Memory`, `Recommend
/// Memory` in KB.
///
/// Code-derived and **unverified**: no capture with `--host-sys=mem` has been
/// available, so this has never produced a row (see spec/TODO.md, T25).
fn host_mem_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Total Memory,Peak Used Memory,Recommend Memory\n");
    let mut rows = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let samples = session.resolve::<HostMemUsageRow>()?;
        let total_kb = ctx
            .require::<mspark_model::HostInfo>()
            .map(|info| info.memory_total_kb)
            .unwrap_or(0);
        let peak = samples
            .iter()
            .map(|sample| sample.usage)
            .fold(0.0f64, f64::max);
        if !samples.is_empty() {
            let used_kb = (peak * total_kb as f64 / 100.0).round() as u64;
            // TODO(T-host-mem): the `Device_id` column is written as `host`
            // because no golden `host_mem_usage` file exists in any capture we
            // have, so the literal cannot be checked. msprof may write the
            // numeric device id here like every other host-side table.
            body.push_str(&format!("host,{total_kb},{used_kb},{used_kb}\n"));
            rows += 1;
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {rows} rows to {}", out.display());
    Ok(())
}

fn communication_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,OP Type,Count\n");
    let mut rows = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let ops = session.resolve::<HcclOpRow>()?;
        let tasks = session.resolve::<HcclInfoRow>()?;
        let dic = ctx.require::<mspark_model::HashDic>()?;

        // Tasks belong to the last op that started before them (validated: this
        // reproduces msprof's per-type op counts exactly).
        let mut stamps: Vec<u64> = ops.iter().map(|op| op.ts_ns).collect();
        stamps.sort_unstable();
        let mut per_op: Vec<Vec<u64>> = vec![Vec::new(); ops.len()];
        for task in tasks.iter() {
            let idx = stamps.partition_point(|stamp| *stamp <= task.ts_ns);
            if idx == 0 {
                continue;
            }
            // Map the sorted position back to the original op index.
            let stamp = stamps[idx - 1];
            if let Some(pos) = ops.iter().position(|op| op.ts_ns == stamp) {
                per_op[pos].push(task.ccl_tag);
            }
        }

        let mut counts: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for (idx, _op) in ops.iter().enumerate() {
            // An op with no tasks cannot be attributed to a type; skip it rather
            // than guess (recorded in spec/TODO.md).
            if per_op[idx].is_empty() {
                continue;
            }
            let mut tally: HashMap<u64, usize> = HashMap::new();
            for tag in per_op[idx].iter() {
                *tally.entry(*tag).or_insert(0) += 1;
            }
            let Some((tag, _)) = tally.into_iter().max_by_key(|(_, n)| *n) else {
                continue;
            };
            let name = dic.get(tag).unwrap_or("N/A");
            // `Broadcast_group_name_0` -> `hcom_broadcast_`
            let kind = name.split('_').next().unwrap_or("N/A");
            let mut chars = kind.chars();
            let op_type = match chars.next() {
                Some(first) => format!("hcom_{}{}_", first.to_lowercase(), chars.as_str()),
                None => "hcom_unknown_".to_string(),
            };
            *counts.entry(op_type).or_insert(0) += 1;
        }
        for (op_type, count) in counts {
            body.push_str(&format!("{dev},{op_type},{count}\n"));
            rows += 1;
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {rows} rows to {}", out.display());
    Ok(())
}

fn host_cpu_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body =
        String::from("Device_id,Total Cpu Numbers,Occupied Cpu Numbers,Recommend Cpu Numbers\n");
    let mut total_rows = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let samples = session.resolve::<HostCpuUsageRow>()?;
        use std::collections::BTreeMap;
        let mut peak: BTreeMap<u64, f64> = BTreeMap::new();
        let mut total = 0u64;
        for sample in samples.iter() {
            if sample.cpu_no == "Avg" {
                continue;
            }
            if let Ok(cpu) = sample.cpu_no.parse::<u64>() {
                total = total.max(cpu + 1);
                let entry = peak.entry(cpu).or_insert(0.0);
                *entry = entry.max(sample.usage);
            }
        }
        // `Total` is the machine's logical CPU count from `host/info.json`
        // (`cpuNums`), not the highest CPU id that happened to be sampled.
        let info_cpus = std::fs::read_to_string(ctx.root().join("host").join("info.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
            .and_then(|root| root.get("cpuNums").and_then(|v| v.as_u64()))
            .unwrap_or(total);
        let total = info_cpus.max(total);
        let occupied = peak.len();
        let recommend = peak.values().filter(|usage| **usage > 0.0).count();
        body.push_str(&format!("host,{total},{occupied},{recommend}\n"));
        total_rows += 1;
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total_rows} rows to {}", out.display());
    Ok(())
}

fn llc_samples(
    rows: &[LlcRecord],
    clock: &mspark_model::SamplingClock,
) -> Vec<(u32, i128, f64, f64)> {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    let mut slots: BTreeMap<(u32, u64), [u64; 8]> = BTreeMap::new();
    for row in rows {
        if let Some(slot) = mspark_model::LLC_READ_SLOTS
            .iter()
            .position(|e| *e == row.event)
        {
            slots.entry((row.l3t_id, row.ts_us)).or_insert([0; 8])[slot] = row.count;
        }
    }
    let mut by_bank: BTreeMap<u32, Vec<(u64, [u64; 8])>> = BTreeMap::new();
    for ((bank, ts_us), counts) in slots {
        by_bank.entry(bank).or_default().push((ts_us, counts));
    }

    let mut out: Vec<(u32, i128, f64, f64)> = Vec::new();
    for (bank, samples) in by_bank {
        let _: BTreeSet<u64> = samples.iter().map(|(ts, _)| *ts).collect();
        let mut prev_ts: Option<u64> = None;
        for (ts_us, e) in samples.iter().copied() {
            let den = e[4] + e[6];
            let hit = if den == 0 {
                0.0
            } else {
                let raw = (e[2] + e[3] + e[5] + e[7]) as f64 / den as f64;
                (raw * 1000.0).round_ties_even() / 1000.0
            };
            let dt_ns = prev_ts.map(|prev| clock.to_ns(ts_us) - clock.to_ns(prev));
            let total = (e[0] + e[1]) as f64;
            // The first sample has no predecessor; msprof reports zero there.
            let tp = match dt_ns {
                Some(dt) if dt > 0 => {
                    round3(total * 1024.0 * 1024.0 * 1e9 / dt as f64 / 256.0 / 1024.0 / 1024.0)
                }
                _ => 0.0,
            };
            out.push((bank, clock.to_ns(ts_us), hit, tp));
            prev_ts = Some(ts_us);
        }
    }
    out
}

fn llc_track_rows(rows: &[LlcRecord], clock: &mspark_model::SamplingClock) -> Vec<CounterRow> {
    let mut out = Vec::new();
    for (bank, ts_ns, hit, tp) in llc_samples(rows, clock) {
        let ts = fmt_us(ts_ns as u64, false);
        out.push(CounterRow {
            name: compact_str::format_compact!("LLC {bank} Read/Hit Rate"),
            tid: 0,
            ts: ts.clone(),
            args: smallvec::smallvec![("Hit Rate(%)", Num::Float(hit))],
        });
        out.push(CounterRow {
            name: compact_str::format_compact!("LLC {bank} Read/Throughput"),
            tid: 0,
            ts,
            args: smallvec::smallvec![("Throughput(MB/s)", Num::Float(tp))],
        });
    }
    out
}

fn llc_csv_table(rows: &[LlcRecord], clock: &mspark_model::SamplingClock) -> Vec<[String; 3]> {
    use std::collections::BTreeMap;
    let mut per_bank: BTreeMap<u32, (f64, f64, f64)> = BTreeMap::new();
    for (bank, _, hit, tp) in llc_samples(rows, clock) {
        let entry = per_bank.entry(bank).or_insert((0.0, 0.0, 0.0));
        entry.0 += hit;
        entry.1 += tp;
        entry.2 += 1.0;
    }
    let lanes: Vec<(u32, f64, f64)> = per_bank
        .into_iter()
        .map(|(bank, (hit, tp, n))| {
            let n = if n > 0.0 { n } else { 1.0 };
            (bank, hit / n, tp / n)
        })
        .collect();
    let mut out = Vec::new();
    if !lanes.is_empty() {
        let avg_hit = lanes.iter().map(|(_, h, _)| *h).sum::<f64>() / lanes.len() as f64;
        let avg_tp = lanes.iter().map(|(_, _, t)| *t).sum::<f64>() / lanes.len() as f64;
        out.push([
            "Average".to_string(),
            py_round3(avg_hit * 100.0),
            py_round3(avg_tp),
        ]);
    }
    for (bank, hit, tp) in lanes {
        out.push([bank.to_string(), py_round3(hit * 100.0), py_round3(tp)]);
    }
    out
}

fn llc_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Mode,Task,Hit Rate(%),Throughput(MB/s)\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let rows = session.resolve::<LlcRecord>()?;
        let clock = ctx
            .require::<mspark_model::DeviceStartLogs>()?
            .0
            .sampling()?;
        for [task, hit, tp] in llc_csv_table(&rows, &clock) {
            body.push_str(&format!("{dev},read,{task},{hit},{tp}\n"));
            total += 1;
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn pcie_track_rows(rows: &[PcieRecord]) -> Vec<CounterRow> {
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

fn pcie_table(rows: &[PcieRecord]) -> Vec<[String; 4]> {
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

fn pcie_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn summary(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote summary for all devices to {}", out.display());
    Ok(())
}

/// Print the node graph (`mspark_df::graph!` holds every edge at compile time).
fn graph(format: &str) -> Result<()> {
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

fn stats(prof: &Path, only: Option<&str>) -> Result<()> {
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
        println!(
            "  store: {} nodes memoised",
            session.store.lock().expect("store lock").len()
        );
    }
    Ok(())
}

/// Run `f` once per device in parallel and return the per-device chunks in
/// device order, so output stays deterministic.
fn per_device<R, F>(prof: &Path, only: Option<&str>, f: F) -> Result<Vec<R>>
where
    R: Send,
    F: Fn(&DeviceSession) -> Result<R> + Send + Sync,
{
    per_session(prof, only, f)
}

fn task_time(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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

fn verify(prof: &Path, only: Option<&str>, golden: &Path) -> Result<()> {
    let golden_rows = read_rows(golden)?;
    let Some((header, golden_rows)) = golden_rows.split_first() else {
        anyhow::bail!("golden file is empty");
    };
    let expected: Vec<&str> = TaskTimeRow::HEADER.split(',').collect();
    if header.as_slice() != expected.as_slice() {
        anyhow::bail!("header mismatch:\n  golden: {header:?}\n  mspark: {expected:?}");
    }

    let mut produced: HashMap<(u32, u32), Vec<TaskTimeRow>> = HashMap::new();
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        for row in session.resolve::<TaskTimeRow>()?.iter() {
            produced
                .entry((row.stream_id, row.task_id))
                .or_default()
                .push(row.clone());
        }
    }

    let mut matched = 0usize;
    let mut missing = 0usize;
    let mut names_ok = 0usize;
    let mut types_ok = 0usize;
    let mut max_start_ns = 0i128;
    let mut max_dur_ns = 0i128;
    let mut start_exact = 0usize;
    let mut start_close = 0usize;
    let mut dur_exact = 0usize;
    let mut dur_close = 0usize;

    for row in golden_rows {
        let stream: u32 = row[3].parse()?;
        let task: u32 = row[4].parse()?;
        let g_start = parse_us_to_ns(&row[6])?;
        let g_dur = parse_us_to_ns(&row[5])?;
        let Some(candidates) = produced.get(&(stream, task)) else {
            missing += 1;
            continue;
        };
        // A key can have both an ACSQ and an FFTS+ row (the "mix" tasks); pick
        // the candidate whose start matches the golden row.
        let p = candidates
            .iter()
            .min_by_key(|p| (p.start_ns as i128 - g_start).abs())
            .expect("non-empty candidate list");
        matched += 1;
        if p.kernel_name == row[1] {
            names_ok += 1;
        }
        if p.kernel_type == row[2] {
            types_ok += 1;
        }
        let ds = (g_start - p.start_ns as i128).abs();
        let dd = (g_dur - p.duration_ns as i128).abs();
        max_start_ns = max_start_ns.max(ds);
        max_dur_ns = max_dur_ns.max(dd);
        start_exact += (ds == 0) as usize;
        start_close += (ds <= 2) as usize;
        dur_exact += (dd == 0) as usize;
        dur_close += (dd <= 2) as usize;
    }

    let golden_keys: std::collections::HashSet<(u32, u32)> = golden_rows
        .iter()
        .filter_map(|r| Some((r[3].parse().ok()?, r[4].parse().ok()?)))
        .collect();
    let extras = produced.keys().filter(|k| !golden_keys.contains(k)).count();
    let duplicate_keys = produced.values().filter(|v| v.len() > 1).count();

    println!("golden rows:   {}", golden_rows.len());
    println!(
        "produced keys: {} ({duplicate_keys} with both ACSQ and FFTS+ rows)",
        produced.len()
    );
    println!("matched:       {matched}");
    println!("missing:       {missing}");
    println!("extras:        {extras}");
    println!("kernel_name exact: {names_ok}/{matched}");
    println!("kernel_type exact: {types_ok}/{matched}");
    println!(
        "task_time:  exact {dur_exact}/{matched}, within 2ns {dur_close}/{matched}, max |Δ| = {max_dur_ns} ns"
    );
    println!(
        "task_start: exact {start_exact}/{matched}, within 2ns {start_close}/{matched}, max |Δ| = {max_start_ns} ns"
    );
    let clean = missing == 0
        && extras == 0
        && matched == golden_rows.len()
        && names_ok == matched
        && types_ok == matched
        && dur_exact == matched
        && start_exact == matched;
    gate(clean, || {
        format!(
            "task_time is not an exact match: {matched} matched, {missing} missing, {extras} extra, \
             {names_ok}/{matched} names, {dur_exact}/{matched} durations, {start_exact}/{matched} starts"
        )
    })
}

fn op_summary(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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

/// A JSON value attached to a trace event argument.
#[derive(Clone)]
enum Num {
    Int(u64),
    Float(f64),
    Str(String),
}

impl Num {
    fn as_f64(&self) -> f64 {
        match self {
            Num::Int(v) => *v as f64,
            Num::Float(v) => *v,
            Num::Str(_) => 0.0,
        }
    }
}

impl Serialize for Num {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Num::Int(v) => serializer.serialize_u64(*v),
            Num::Float(v) => serializer.serialize_f64(*v),
            Num::Str(v) => serializer.serialize_str(v),
        }
    }
}

/// Ordered argument map (insertion order is part of the output contract).
/// Argument list with static keys, inline for the common 1-4 arguments.
struct Args(smallvec::SmallVec<[(&'static str, Num); 4]>);

impl Serialize for Args {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// Argument list with static keys: the keys are literals, so this avoids a
/// `String` allocation per argument per event.
/// Build the ordered trace arguments without a heap allocation for the common
/// four-argument event.
fn args<const N: usize>(items: [(&'static str, Num); N]) -> Args {
    Args(smallvec::SmallVec::from_iter(items))
}

/// One Chrome-trace event. Field order here is the serialized key order.
#[derive(Serialize)]
struct TraceEvent {
    name: compact_str::CompactString,
    pid: u32,
    tid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    ts: Option<compact_str::CompactString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dur: Option<f64>,
    ph: &'static str,
    args: Args,
}

impl TraceEvent {
    fn meta(name: &str, pid: u32, tid: u32, args: Args) -> Self {
        Self {
            name: compact_str::CompactString::from(name),
            pid,
            tid,
            ts: None,
            dur: None,
            ph: "M",
            args,
        }
    }

    fn complete(
        name: compact_str::CompactString,
        pid: u32,
        tid: u32,
        ts: compact_str::CompactString,
        dur: f64,
        args: Args,
    ) -> Self {
        Self {
            name,
            pid,
            tid,
            ts: Some(ts),
            dur: Some(dur),
            ph: "X",
            args,
        }
    }

    fn counter(
        name: compact_str::CompactString,
        pid: u32,
        tid: u32,
        ts: compact_str::CompactString,
        args: Args,
    ) -> Self {
        Self {
            name,
            pid,
            tid,
            ts: Some(ts),
            dur: None,
            ph: "C",
            args,
        }
    }
}

struct CounterRow {
    name: compact_str::CompactString,
    tid: u32,
    ts: compact_str::CompactString,
    args: smallvec::SmallVec<[(&'static str, Num); 4]>,
}

const SIO_COUNTERS: [&str; 8] = [
    "req_rx", "rsp_rx", "snp_rx", "dat_rx", "req_tx", "rsp_tx", "snp_tx", "dat_tx",
];
const ACC_PMU_COUNTERS: [&str; 4] = ["read_bandwidth", "read_ost", "write_bandwidth", "write_ost"];

fn sio_rows(samples: &[SioSample]) -> Vec<CounterRow> {
    let mut grouped: BTreeMap<(compact_str::CompactString, usize), BTreeMap<u16, u32>> =
        BTreeMap::new();
    for sample in samples {
        for (i, value) in sample.bandwidth.iter().enumerate() {
            grouped
                .entry((sample.ts_us.clone(), i))
                .or_default()
                .insert(sample.die, *value);
        }
    }
    grouped
        .into_iter()
        .map(|((ts, i), dies)| CounterRow {
            name: compact_str::CompactString::from(SIO_COUNTERS[i]),
            tid: 0,
            ts,
            args: dies
                .into_iter()
                .map(|(die, value)| {
                    let key: &'static str = match die {
                        0 => "die 0",
                        1 => "die 1",
                        _ => "die other",
                    };
                    (key, Num::Float(value as f64))
                })
                .collect(),
        })
        .collect()
}

fn acc_pmu_rows(samples: &[AccPmuSample]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 4);
    for sample in samples {
        for (i, name) in ACC_PMU_COUNTERS.iter().enumerate() {
            rows.push(CounterRow {
                name: compact_str::CompactString::from(*name),
                tid: 0,
                ts: sample.ts_us.clone(),
                args: smallvec::smallvec![("value", Num::Int(sample.values[i]))],
            });
        }
    }
    rows
}

fn inter_soc_rows(samples: &[InterSocSample]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        rows.push(CounterRow {
            name: compact_str::CompactString::from("L2 Buffer Bw Level"),
            tid: 0,
            ts: sample.ts_us.clone(),
            args: smallvec::smallvec![(
                "L2 Buffer Bw Level",
                Num::Int(sample.l2_buffer_bw_level as u64),
            )],
        });
        rows.push(CounterRow {
            name: compact_str::CompactString::from("Mata Bw Level"),
            tid: 0,
            ts: sample.ts_us.clone(),
            args: smallvec::smallvec![("Mata Bw Level", Num::Int(sample.mata_bw_level as u64))],
        });
    }
    rows
}

fn chip_trans_rows(samples: &[ChipTransSample]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let (tid, counters): (u32, [(&str, u64); 2]) = match sample.kind {
            ChipTransKind::Pa => (
                111111,
                [("PA Link Rx", sample.value1), ("PA Link Tx", sample.value2)],
            ),
            ChipTransKind::Pcie => (
                100010,
                [
                    ("PCIE Read Bandwidth", sample.value2),
                    ("PCIE Write Bandwidth", sample.value1),
                ],
            ),
        };
        let id_key = if sample.kind == ChipTransKind::Pa {
            "PA Link ID"
        } else {
            "PCIE ID"
        };
        for (name, value) in counters {
            rows.push(CounterRow {
                name: compact_str::CompactString::from(name),
                tid,
                ts: sample.ts_us.clone(),
                args: smallvec::smallvec![
                    (name, Num::Int(value)),
                    (id_key, Num::Int(sample.id as u64)),
                ],
            });
        }
    }
    rows
}

fn write_counter(events: &mut Vec<TraceEvent>, pid: u32, row: CounterRow) {
    let CounterRow {
        name,
        tid,
        ts,
        args,
        ..
    } = row;
    events.push(TraceEvent::counter(
        name,
        pid,
        tid,
        ts,
        Args(smallvec::SmallVec::from_iter(args)),
    ));
}

fn signature(name: &str, tid: u32, ts: &str, args: &[(String, f64)]) -> String {
    let mut args = args.to_vec();
    args.sort_by(|a, b| a.0.cmp(&b.0));
    format!(
        "{name}|{tid}|{ts}|{}",
        args.iter()
            .map(|(key, value)| format!("{key}={value:.6}"))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn produced_signatures(rows: &[CounterRow]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            let args: Vec<(String, f64)> = row
                .args
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.as_f64()))
                .collect();
            signature(&row.name, row.tid, &row.ts, &args)
        })
        .collect()
}

fn golden_counter_signatures(array: &[serde_json::Value], pid: u64) -> Vec<String> {
    array
        .iter()
        .filter(|event| {
            event.get("pid").and_then(|v| v.as_u64()) == Some(pid)
                && event.get("ph").and_then(|v| v.as_str()) == Some("C")
        })
        .map(|event| {
            let name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let tid = event.get("tid").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let ts = event.get("ts").and_then(|v| v.as_str()).unwrap_or("");
            let mut args = Vec::new();
            if let Some(map) = event.get("args").and_then(|v| v.as_object()) {
                for (key, value) in map {
                    let value = value
                        .as_f64()
                        .or_else(|| value.as_u64().map(|u| u as f64))
                        .unwrap_or(0.0);
                    args.push((key.clone(), value));
                }
            }
            signature(name, tid, ts, &args)
        })
        .collect()
}

fn multiset_match(a: &[String], b: &[String]) -> usize {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort();
    b.sort();
    let (mut i, mut j, mut matched) = (0, 0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                matched += 1;
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    matched
}

fn push_process_meta(
    events: &mut Vec<TraceEvent>,
    pid: u32,
    name: &str,
    label: &str,
    sort_index: u32,
) {
    events.push(TraceEvent::meta(
        "process_name",
        pid,
        0,
        args([("name", Num::Str(name.to_string()))]),
    ));
    events.push(TraceEvent::meta(
        "process_labels",
        pid,
        0,
        args([("labels", Num::Str(label.to_string()))]),
    ));
    events.push(TraceEvent::meta(
        "process_sort_index",
        pid,
        0,
        args([("sort_index", Num::Int(sort_index as u64))]),
    ));
}

fn push_thread_meta(events: &mut Vec<TraceEvent>, pid: u32, tid: u32, name: &str, sort_index: u32) {
    events.push(TraceEvent::meta(
        "thread_name",
        pid,
        tid,
        args([("name", Num::Str(name.to_string()))]),
    ));
    events.push(TraceEvent::meta(
        "thread_sort_index",
        pid,
        tid,
        args([("sort_index", Num::Int(sort_index as u64))]),
    ));
}

fn sample_csv_list(device: &Path, key: &str) -> Result<Vec<String>> {
    Ok(load_sample_string(device, key)?
        .unwrap_or_default()
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect())
}

fn qos_rows(samples: &[QosRow], events: &[String]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * events.len());
    for sample in samples {
        for (i, event) in events.iter().enumerate().take(10) {
            rows.push(CounterRow {
                name: compact_str::format_compact!("QoS {event}"),
                tid: 0,
                ts: sample.ts_us.clone(),
                args: smallvec::smallvec![("value", Num::Int(sample.values[i] as u64))],
            });
        }
    }
    rows
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round_ties_even() / 1000.0
}

fn hccs_rows(samples: &[HccsRateRow]) -> Vec<CounterRow> {
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

fn npu_mem_rows(samples: &[NpuMemRow]) -> Vec<CounterRow> {
    let mut rows = Vec::with_capacity(samples.len() * 3);
    for sample in samples {
        let owner = if sample.event == 0 { "APP" } else { "Device" };
        let ts = fmt_us(sample.ts_ns, false);
        for (metric, value) in [
            ("DDR", sample.ddr),
            ("HBM", sample.hbm),
            ("Memory", sample.ddr + sample.hbm),
        ] {
            rows.push(CounterRow {
                name: compact_str::format_compact!("{owner}/{metric}"),
                tid: 0,
                ts: ts.clone(),
                args: smallvec::smallvec![("KB", Num::Float(value as f64 / 1024.0))],
            });
        }
    }
    rows
}

fn timeline(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let devices = devices(prof, only)?;
    let chunks: Vec<(Vec<TraceEvent>, usize)> = devices
        .par_iter()
        .map(|device| device_timeline(device))
        .collect::<Result<Vec<_>>>()?;

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Stream device chunks straight into the JSON array: the merged vector used to
    // be built (and then dropped) in full, which costs a large allocation, a copy
    // of every event and the peak memory of all devices at once.
    let file = std::fs::File::create(out)?;
    let mut writer = std::io::BufWriter::with_capacity(1 << 20, file);
    writer.write_all(b"[")?;
    let mut first = true;
    let mut total = 0usize;
    for (chunk, count) in chunks {
        for event in &chunk {
            if !first {
                writer.write_all(b",")?;
            }
            first = false;
            serde_json::to_writer(&mut writer, event)?;
        }
        total += count;
    }
    writer.write_all(b"]")?;
    writer.flush()?;
    println!("wrote {total} timeline events to {}", out.display());
    Ok(())
}

/// Build one device's timeline events. Devices are independent, so this runs
/// in parallel; the caller concatenates chunks in device order.
fn device_timeline(device: &Path) -> Result<(Vec<TraceEvent>, usize)> {
    let session = session_for(device)?;
    let ctx = session.ctx();
    let info = &session.info;
    let mut events: Vec<TraceEvent> = Vec::new();
    let mut total = 0usize;
    {
        let clock = ctx.require::<Clock>()?;
        let tasks = session.resolve::<TimelineTask>()?;

        let dev = info.device_id.unwrap_or(0);
        let hw_pid = Track::Hardware.pid(info.pid, dev);
        let freq_pid = Track::AiCoreFreq.pid(info.pid, dev);

        for ts in [clock.begin_us, clock.end_us] {
            events.push(TraceEvent::counter(
                compact_str::CompactString::from("AI Core Freq"),
                freq_pid,
                0,
                fmt_us(ts * 1000, false),
                args([("MHz", Num::Int(info.aic_freq_mhz as u64))]),
            ));
        }

        push_process_meta(
            &mut events,
            hw_pid,
            "Ascend Hardware",
            &format!("NPU {dev}"),
            13,
        );

        let mut tids: Vec<u32> = tasks.iter().map(|t| t.tid).collect();
        tids.sort_unstable();
        tids.dedup();
        for tid in tids {
            push_thread_meta(&mut events, hw_pid, tid, &format!("Stream {tid}"), tid);
        }

        for task in tasks.iter() {
            events.push(TraceEvent::complete(
                compact_str::CompactString::from(task.name.as_str()),
                hw_pid,
                task.tid,
                task.ts_us.clone(),
                task.dur_us,
                args([
                    ("Model Id", Num::Int(task.model_id as u64)),
                    ("Task Type", Num::Str(task.task_type.clone())),
                    ("Physic Stream Id", Num::Int(task.stream_id as u64)),
                    ("Task Id", Num::Int(task.task_id as u64)),
                    ("Batch Id", Num::Int(task.batch_id as u64)),
                    ("Subtask Id", Num::Int(task.subtask_id as u64)),
                ]),
            ));
            total += 1;
        }

        // --- CANN host track ---
        let api_events = session.resolve::<ApiEventRow>()?;
        let host_clock = ctx
            .require::<mspark_model::HostStartLogs>()?
            .0
            .host_clock(ctx.require::<mspark_model::DeviceInfo>()?.host_freq_mhz)?;
        let cann_pid = Track::Cann.pid(info.pid, HOST_PID);
        push_process_meta(&mut events, cann_pid, "CANN", "CPU", 7);
        let mut api_tids: Vec<u32> = api_events.iter().map(|e| e.thread_id).collect();
        api_tids.sort_unstable();
        api_tids.dedup();
        for tid in api_tids {
            push_thread_meta(&mut events, cann_pid, tid, &format!("Thread {tid}"), tid);
        }
        for event in api_events.iter() {
            let (prefix, level_name) = match event.level {
                20_000 => ("AscendCL", "acl"),
                10_000 => ("Node", "node"),
                5_000 => ("Runtime", "runtime"),
                15_000 => ("Model", "model"),
                _ => ("Api", ""),
            };
            let mode = event.tag.clone();
            let id = if event.level == 20_000 {
                event.api_name.clone()
            } else {
                "0".to_string()
            };
            let trace = format!("{prefix}@{}", event.api_name);
            let ts = fmt_us(host_clock.to_ns(event.begin), false);
            let dur = host_clock.duration_us_f64(event.begin, event.end);
            events.push(TraceEvent::complete(
                compact_str::CompactString::from(trace.as_str()),
                cann_pid,
                event.thread_id,
                ts,
                dur,
                args([
                    ("Thread Id", Num::Int(event.thread_id as u64)),
                    ("Mode", Num::Str(mode)),
                    ("level", Num::Str(level_name.to_string())),
                    ("id", Num::Str(id)),
                    ("item_id", Num::Str(event.item_name.clone())),
                ]),
            ));
        }

        // --- SOC profile tracks ---
        let label = format!("NPU {dev}");
        let sio = session.resolve::<SioSample>()?;
        let acc = session.resolve::<AccPmuSample>()?;
        let inter = session.resolve::<InterSocSample>()?;
        let chip = session.resolve::<ChipTransSample>()?;
        let track_pid = |track: Track| track.pid(info.pid, dev);
        for (track, name) in [
            (Track::Sio, "SIO"),
            (Track::AccPmu, "Acc PMU"),
            (Track::StarsSocInfo, "Stars Soc Info"),
            (Track::ChipTrans, "Stars Chip Trans"),
        ] {
            push_process_meta(&mut events, track_pid(track), name, &label, track as u32);
        }
        for row in sio_rows(&sio) {
            write_counter(&mut events, track_pid(Track::Sio), row);
            total += 1;
        }
        for row in acc_pmu_rows(&acc) {
            write_counter(&mut events, track_pid(Track::AccPmu), row);
            total += 1;
        }
        for row in inter_soc_rows(&inter) {
            write_counter(&mut events, track_pid(Track::StarsSocInfo), row);
            total += 1;
        }
        for row in chip_trans_rows(&chip) {
            write_counter(&mut events, track_pid(Track::ChipTrans), row);
            total += 1;
        }

        // --- Overlap Analysis ---
        let overlap = session.resolve::<OverlapRow>()?;
        let overlap_pid = track_pid(Track::Overlap);
        push_process_meta(&mut events, overlap_pid, "Overlap Analysis", &label, 29);
        for (tid, name) in [
            (0u32, "Communication"),
            (1, "Communication(Not Overlapped)"),
            (2, "Computing"),
            (3, "Free"),
        ] {
            push_thread_meta(&mut events, overlap_pid, tid, name, tid);
        }
        for row in overlap.iter() {
            let (name, tid) = match row.kind {
                OverlapKind::Compute => ("Computing", 2),
                OverlapKind::Free => ("Free", 3),
            };
            events.push(TraceEvent::complete(
                compact_str::CompactString::from(name),
                overlap_pid,
                tid,
                row.ts_us.clone(),
                row.dur_us,
                Args(smallvec::SmallVec::new()),
            ));
            total += 1;
        }

        // --- QoS ---
        let qos = session.resolve::<QosRow>()?;
        let qos_events = sample_csv_list(ctx.device_dir()?, "qosEvents")?;
        let qos_pid = track_pid(Track::Qos);
        push_process_meta(&mut events, qos_pid, "QoS", &label, 28);
        for row in qos_rows(&qos, &qos_events) {
            write_counter(&mut events, qos_pid, row);
            total += 1;
        }

        // --- NPU MEM ---
        let npu_mem = session.resolve::<NpuMemRow>()?;
        let npu_mem_pid = track_pid(Track::Communication);
        // --- CPU Usage (host track) ---
        let cpu_usage = session.resolve::<HostCpuUsageRow>()?;
        let cpu_pid = Track::CpuUsage.pid(info.pid, HOST_PID);
        events.push(TraceEvent::meta(
            "process_name",
            cpu_pid,
            0,
            args([("name", Num::Str("CPU Usage".to_string()))]),
        ));
        events.push(TraceEvent::meta(
            "process_labels",
            cpu_pid,
            0,
            args([("labels", Num::Str("CPU".to_string()))]),
        ));
        events.push(TraceEvent::meta(
            "process_sort_index",
            cpu_pid,
            0,
            args([("sort_index", Num::Int(9))]),
        ));
        let cpu_offset = host_local_offset_milli(ctx.root())?;
        for row in host_cpu_rows(&cpu_usage, cpu_offset) {
            write_counter(&mut events, cpu_pid, row);
            total += 1;
        }

        // --- HCCS ---
        let hccs = session.resolve::<HccsRateRow>()?;
        let hccs_pid = track_pid(Track::Hccs);
        push_process_meta(&mut events, hccs_pid, "HCCS", &label, 19);
        let nic: Vec<NetSample> = session
            .resolve::<NicRow>()?
            .iter()
            .map(|row| row.0)
            .collect();
        let nic_pid = track_pid(Track::Nic);
        push_process_meta(&mut events, nic_pid, "NIC", &label, 20);
        for row in net_rows(&nic, "NIC") {
            write_counter(&mut events, nic_pid, row);
            total += 1;
        }
        let roce: Vec<NetSample> = session
            .resolve::<RoceRow>()?
            .iter()
            .map(|row| row.0)
            .collect();
        let roce_pid = track_pid(Track::Roce);
        push_process_meta(&mut events, roce_pid, "RoCE", &label, 21);
        for row in net_rows(&roce, "RoCE") {
            write_counter(&mut events, roce_pid, row);
            total += 1;
        }
        let llc = session.resolve::<LlcRecord>()?;
        let llc_clock = ctx
            .require::<mspark_model::DeviceStartLogs>()?
            .0
            .sampling()?;
        let llc_pid = track_pid(Track::Llc);
        push_process_meta(&mut events, llc_pid, "LLC", &label, 26);
        for row in llc_track_rows(&llc, &llc_clock) {
            write_counter(&mut events, llc_pid, row);
            total += 1;
        }
        let hbm = session.resolve::<HbmRecord>()?;
        let hbm_pid = track_pid(Track::Hbm);
        push_process_meta(&mut events, hbm_pid, "HBM", &label, 23);
        for row in hbm_track_rows(&hbm) {
            write_counter(&mut events, hbm_pid, row);
            total += 1;
        }
        let pcie = session.resolve::<PcieRecord>()?;
        let pcie_pid = track_pid(Track::Pcie);
        push_process_meta(&mut events, pcie_pid, "PCIe", &label, 22);
        for row in pcie_track_rows(&pcie) {
            write_counter(&mut events, pcie_pid, row);
            total += 1;
        }
        for row in hccs_rows(&hccs) {
            write_counter(&mut events, hccs_pid, row);
            total += 1;
        }

        push_process_meta(&mut events, npu_mem_pid, "NPU MEM", &label, 16);
        for row in npu_mem_rows(&npu_mem) {
            write_counter(&mut events, npu_mem_pid, row);
            total += 1;
        }
    }
    Ok((events, total))
}

/// Map process pid -> (process_name, process_labels) from the metadata events.
fn golden_process_meta(array: &[serde_json::Value]) -> HashMap<u64, (String, String)> {
    let mut names: HashMap<u64, String> = HashMap::new();
    let mut labels: HashMap<u64, String> = HashMap::new();
    for event in array {
        if event.get("ph").and_then(|v| v.as_str()) != Some("M") {
            continue;
        }
        let Some(pid) = event.get("pid").and_then(|v| v.as_u64()) else {
            continue;
        };
        let args = event.get("args");
        match event.get("name").and_then(|v| v.as_str()) {
            Some("process_name") => {
                if let Some(name) = args.and_then(|a| a.get("name")).and_then(|v| v.as_str()) {
                    names.insert(pid, name.to_string());
                }
            }
            Some("process_labels") => {
                if let Some(label) = args.and_then(|a| a.get("labels")).and_then(|v| v.as_str()) {
                    labels.insert(pid, label.to_string());
                }
            }
            _ => {}
        }
    }
    let mut out = HashMap::new();
    for (pid, name) in names {
        let label = labels.get(&pid).cloned().unwrap_or_default();
        out.insert(pid, (name, label));
    }
    out
}

fn golden_pid_for(meta: &HashMap<u64, (String, String)>, name: &str, dev: u32) -> Option<u64> {
    let label = format!("NPU {dev}");
    // Device tracks carry `NPU <dev>`; host tracks (`CPU Usage`, `CANN`) carry
    // their own label, so fall back to a name-only match.
    meta.iter()
        .find(|(_, (track, lbl))| track == name && (lbl.is_empty() || *lbl == label))
        .or_else(|| meta.iter().find(|(_, (track, _))| track == name))
        .map(|(pid, _)| *pid)
}

fn verify_timeline(prof: &Path, only: Option<&str>, golden: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(golden).with_context(|| format!("reading {}", golden.display()))?;
    let value: serde_json::Value = serde_json::from_str(&text)?;
    let array = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("golden timeline is not a JSON array"))?;

    let mut golden_rows = 0usize;
    let mut produced_rows = 0usize;
    let mut name_ok = 0usize;
    let mut tid_ok = 0usize;
    let mut ts_ok = 0usize;
    let mut dur_ok = 0usize;
    let mut type_ok = 0usize;
    let mut stream_ok = 0usize;
    let mut task_ok = 0usize;
    let mut subtask_ok = 0usize;
    let mut max_dur = 0.0f64;
    let mut api_golden = 0usize;
    let mut api_produced = 0usize;
    let mut api_name_ok = 0usize;
    let mut api_tid_ok = 0usize;
    let mut api_ts_ok = 0usize;
    let mut api_dur_ok = 0usize;
    let mut api_mode_ok = 0usize;
    let mut api_level_ok = 0usize;
    let mut api_id_ok = 0usize;
    let mut api_item_ok = 0usize;
    let golden_meta = golden_process_meta(array);
    let mut track_stats: Vec<(&'static str, usize, usize, usize)> = Vec::new();
    let mut overlap_golden = 0usize;
    let mut overlap_produced = 0usize;
    let mut overlap_matched = 0usize;
    let mut overlap_max_dur = 0.0f64;

    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let hw_pid = Track::Hardware.pid(info.pid, dev) as u64;
        let expected: Vec<&serde_json::Value> = array
            .iter()
            .filter(|event| {
                event.get("pid").and_then(|p| p.as_u64()) == Some(hw_pid)
                    && event.get("ph").and_then(|p| p.as_str()) == Some("X")
            })
            .collect();
        let produced = session.resolve::<TimelineTask>()?;

        golden_rows += expected.len();
        produced_rows += produced.len();
        for (golden, ours) in expected.iter().zip(produced.iter()) {
            if golden.get("name").and_then(|v| v.as_str()) == Some(ours.name.as_str()) {
                name_ok += 1;
            }
            if golden.get("tid").and_then(|v| v.as_u64()) == Some(ours.tid as u64) {
                tid_ok += 1;
            }
            if golden.get("ts").and_then(|v| v.as_str()) == Some(ours.ts_us.as_str()) {
                ts_ok += 1;
            }
            let dur = golden
                .get("dur")
                .and_then(|v| v.as_f64())
                .unwrap_or(f64::NAN);
            let delta = (dur - ours.dur_us).abs();
            max_dur = max_dur.max(delta);
            if delta <= 0.001 {
                dur_ok += 1;
            }
            let args = &golden["args"];
            if args.get("Task Type").and_then(|v| v.as_str()) == Some(ours.task_type.as_str()) {
                type_ok += 1;
            }
            if args.get("Physic Stream Id").and_then(|v| v.as_u64()) == Some(ours.stream_id as u64)
            {
                stream_ok += 1;
            }
            if args.get("Task Id").and_then(|v| v.as_u64()) == Some(ours.task_id as u64) {
                task_ok += 1;
            }
            if args.get("Subtask Id").and_then(|v| v.as_u64()) == Some(ours.subtask_id as u64) {
                subtask_ok += 1;
            }
        }

        // CANN host track
        let host_clock = ctx
            .require::<mspark_model::HostStartLogs>()?
            .0
            .host_clock(ctx.require::<mspark_model::DeviceInfo>()?.host_freq_mhz)?;
        let cann_pid = Track::Cann.pid(info.pid, HOST_PID) as u64;
        let expected_api: Vec<&serde_json::Value> = array
            .iter()
            .filter(|event| {
                event.get("pid").and_then(|p| p.as_u64()) == Some(cann_pid)
                    && event.get("ph").and_then(|p| p.as_str()) == Some("X")
            })
            .collect();
        let produced_api = session.resolve::<ApiEventRow>()?;
        api_golden += expected_api.len();
        api_produced += produced_api.len();
        for (golden, ours) in expected_api.iter().zip(produced_api.iter()) {
            let (prefix, level_name) = match ours.level {
                20_000 => ("AscendCL", "acl"),
                10_000 => ("Node", "node"),
                5_000 => ("Runtime", "runtime"),
                15_000 => ("Model", "model"),
                _ => ("Api", ""),
            };
            let mode = ours.tag.clone();
            let id = if ours.level == 20_000 {
                ours.api_name.clone()
            } else {
                "0".to_string()
            };
            let trace = format!("{prefix}@{}", ours.api_name);
            let ts = fmt_us(host_clock.to_ns(ours.begin), false);
            let dur = host_clock.duration_us_f64(ours.begin, ours.end);
            if golden.get("name").and_then(|v| v.as_str()) == Some(trace.as_str()) {
                api_name_ok += 1;
            }
            if golden.get("tid").and_then(|v| v.as_u64()) == Some(ours.thread_id as u64) {
                api_tid_ok += 1;
            }
            if golden.get("ts").and_then(|v| v.as_str()) == Some(ts.as_str()) {
                api_ts_ok += 1;
            }
            let golden_dur = golden
                .get("dur")
                .and_then(|v| v.as_f64())
                .unwrap_or(f64::NAN);
            if (golden_dur - dur).abs() <= 0.001 {
                api_dur_ok += 1;
            }
            let args = &golden["args"];
            if args.get("Mode").and_then(|v| v.as_str()) == Some(mode.as_str()) {
                api_mode_ok += 1;
            }
            if args.get("level").and_then(|v| v.as_str()) == Some(level_name) {
                api_level_ok += 1;
            }
            if args.get("id").and_then(|v| v.as_str()) == Some(id.as_str()) {
                api_id_ok += 1;
            }
            if args.get("item_id").and_then(|v| v.as_str()) == Some(ours.item_name.as_str()) {
                api_item_ok += 1;
            }
        }

        // SOC profile counter tracks
        let sio_rows_v = sio_rows(&session.resolve::<SioSample>()?);
        let acc_rows_v = acc_pmu_rows(&session.resolve::<AccPmuSample>()?);
        let inter_rows_v = inter_soc_rows(&session.resolve::<InterSocSample>()?);
        let chip_rows_v = chip_trans_rows(&session.resolve::<ChipTransSample>()?);
        let qos_events = sample_csv_list(ctx.device_dir()?, "qosEvents")?;
        let qos_rows_v = qos_rows(&session.resolve::<QosRow>()?, &qos_events);
        let npu_mem_rows_v = npu_mem_rows(&session.resolve::<NpuMemRow>()?);
        let hccs_rows_v = hccs_rows(&session.resolve::<HccsRateRow>()?);
        for (sort, name, rows) in [
            (Track::Sio, "SIO", sio_rows_v),
            (Track::AccPmu, "Acc PMU", acc_rows_v),
            (Track::StarsSocInfo, "Stars Soc Info", inter_rows_v),
            (Track::ChipTrans, "Stars Chip Trans", chip_rows_v),
            (Track::Qos, "QoS", qos_rows_v),
            (Track::Communication, "NPU MEM", npu_mem_rows_v),
            (Track::Hccs, "HCCS", hccs_rows_v),
            (
                Track::CpuUsage,
                "CPU Usage",
                host_cpu_rows(
                    &session.resolve::<HostCpuUsageRow>()?,
                    host_local_offset_milli(ctx.root())?,
                ),
            ),
            (
                Track::Nic,
                "NIC",
                net_rows(
                    &session
                        .resolve::<NicRow>()?
                        .iter()
                        .map(|row| row.0)
                        .collect::<Vec<_>>(),
                    "NIC",
                ),
            ),
            (
                Track::Roce,
                "RoCE",
                net_rows(
                    &session
                        .resolve::<RoceRow>()?
                        .iter()
                        .map(|row| row.0)
                        .collect::<Vec<_>>(),
                    "RoCE",
                ),
            ),
            (
                Track::Llc,
                "LLC",
                llc_track_rows(
                    &session.resolve::<LlcRecord>()?,
                    &ctx.require::<mspark_model::DeviceStartLogs>()?
                        .0
                        .sampling()?,
                ),
            ),
            (
                Track::Hbm,
                "HBM",
                hbm_track_rows(&session.resolve::<HbmRecord>()?),
            ),
            (
                Track::Pcie,
                "PCIe",
                pcie_track_rows(&session.resolve::<PcieRecord>()?),
            ),
        ] {
            let pid =
                golden_pid_for(&golden_meta, name, dev).unwrap_or(sort.pid(info.pid, dev) as u64);
            let golden = golden_counter_signatures(array, pid);
            let produced = produced_signatures(&rows);
            let matched = multiset_match(&golden, &produced);
            track_stats.push((name, golden.len(), produced.len(), matched));
        }

        // Overlap Analysis
        let overlap_pid = golden_pid_for(&golden_meta, "Overlap Analysis", dev)
            .unwrap_or(Track::Overlap.pid(info.pid, dev) as u64);
        let mut golden_overlap: Vec<(String, u32, compact_str::CompactString, f64)> = array
            .iter()
            .filter(|event| {
                event.get("pid").and_then(|v| v.as_u64()) == Some(overlap_pid)
                    && event.get("ph").and_then(|v| v.as_str()) == Some("X")
            })
            .map(|event| {
                (
                    event
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    event.get("tid").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                    compact_str::CompactString::from(
                        event.get("ts").and_then(|v| v.as_str()).unwrap_or(""),
                    ),
                    event.get("dur").and_then(|v| v.as_f64()).unwrap_or(0.0),
                )
            })
            .collect();
        let produced_overlap = session.resolve::<OverlapRow>()?;
        let mut produced_rows: Vec<(String, u32, compact_str::CompactString, f64)> =
            produced_overlap
                .iter()
                .map(|row| {
                    let (name, tid) = match row.kind {
                        OverlapKind::Compute => ("Computing", 2),
                        OverlapKind::Free => ("Free", 3),
                    };
                    (name.to_string(), tid, row.ts_us.clone(), row.dur_us)
                })
                .collect();
        overlap_golden += golden_overlap.len();
        overlap_produced += produced_rows.len();
        let key =
            |r: &(String, u32, compact_str::CompactString, f64)| (r.0.clone(), r.1, r.2.clone());
        golden_overlap.sort_by_key(key);
        produced_rows.sort_by_key(key);
        for (golden, produced) in golden_overlap.iter().zip(produced_rows.iter()) {
            if golden.0 == produced.0 && golden.1 == produced.1 {
                let delta = (golden.3 - produced.3).abs();
                let ts_delta = match (parse_us_to_ns(&golden.2), parse_us_to_ns(&produced.2)) {
                    (Ok(a), Ok(b)) => (a - b).abs(),
                    _ => i128::MAX,
                };
                overlap_max_dur = overlap_max_dur.max(delta);
                if ts_delta <= 2 && delta <= 0.0021 {
                    overlap_matched += 1;
                }
            }
        }
    }

    println!("golden hardware X events: {golden_rows}");
    println!("produced hardware X events: {produced_rows}");
    println!("name exact:     {name_ok}");
    println!("tid exact:      {tid_ok}");
    println!("ts exact:       {ts_ok}");
    println!("dur within 1ns: {dur_ok}  max |delta| = {max_dur:.6} us");
    println!("task type:      {type_ok}");
    println!("stream id:      {stream_ok}");
    println!("task id:        {task_ok}");
    println!("subtask id:     {subtask_ok}");
    println!("CANN golden X events:   {api_golden}");
    println!("CANN produced X events: {api_produced}");
    println!("CANN name:      {api_name_ok}");
    println!("CANN tid:       {api_tid_ok}");
    println!("CANN ts:        {api_ts_ok}");
    println!("CANN dur ≤1ns:  {api_dur_ok}");
    println!("CANN Mode:      {api_mode_ok}");
    println!("CANN level:     {api_level_ok}");
    println!("CANN id:        {api_id_ok}");
    println!("CANN item_id:   {api_item_ok}");
    for &(name, golden, produced, matched) in &track_stats {
        println!("track {name:<16} golden={golden:<6} produced={produced:<6} matched={matched}");
    }
    println!(
        "overlap golden={overlap_golden} produced={overlap_produced} matched={overlap_matched} max_dur={overlap_max_dur:.6}"
    );
    let mut clean = produced_rows == golden_rows
        && name_ok == produced_rows
        && tid_ok == produced_rows
        && ts_ok == produced_rows
        && dur_ok == produced_rows
        && type_ok == produced_rows
        && stream_ok == produced_rows
        && task_ok == produced_rows
        && subtask_ok == produced_rows
        && api_produced == api_golden
        && api_name_ok == api_produced
        && api_tid_ok == api_produced
        && api_ts_ok == api_produced
        && api_dur_ok == api_produced
        && api_mode_ok == api_produced
        && api_level_ok == api_produced
        && api_id_ok == api_produced
        && api_item_ok == api_produced
        && overlap_golden == overlap_produced
        && overlap_matched == overlap_golden;
    for &(name, golden, produced, matched) in &track_stats {
        if golden != produced || matched != golden {
            clean = false;
            eprintln!("strict: track {name} is not an exact match");
        }
    }
    gate(clean, || {
        "timeline is not an exact match (see the counters above)".to_string()
    })
}

fn ts_cpu_event_name(event: u64) -> &'static str {
    match event {
        0x8 => "Inst_retired",
        0x11 => "Cpu_cycles",
        _ => " ",
    }
}

fn ts_cpu_event_ids(device: &Path) -> Result<Vec<u64>> {
    let raw = load_sample_string(device, "ts_cpu_profiling_events")?.unwrap_or_default();
    Ok(raw
        .split(',')
        .filter_map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return None;
            }
            let hex = item.trim_start_matches("0x").trim_start_matches("0X");
            u64::from_str_radix(hex, 16).ok()
        })
        .collect())
}

fn ts_cpu_pmu_events(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Event,Name,Count\n");
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let records = session.resolve::<TscpuRecord>()?;
        let mut rows: Vec<(u64, u64)> = ts_cpu_event_ids(&device)?
            .into_iter()
            .map(|event| {
                let total = records
                    .iter()
                    .flat_map(|record| record.0.events.iter().zip(record.0.counts.iter()))
                    .filter(|(value, _)| **value == event)
                    .map(|(_, count)| *count)
                    .sum();
                (event, total)
            })
            .collect();
        rows.sort_by_key(|row| std::cmp::Reverse(row.1.to_string()));
        for (event, total) in rows {
            body.push_str(&format!(
                "{dev},0x{event:x},{},{total}\n",
                ts_cpu_event_name(event)
            ));
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote ts_cpu_pmu_events to {}", out.display());
    Ok(())
}

fn ts_cpu_top_function(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Function,Cycles,Cycles(%)\n");
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let records = session.resolve::<TscpuRecord>()?;
        let mut counts: Vec<(u64, u64)> = Vec::new();
        let mut index: HashMap<u64, usize> = HashMap::new();
        for record in records.iter() {
            for (&event, &count) in record.0.events.iter().zip(record.0.counts.iter()) {
                if event == 0x11 {
                    match index.get(&record.0.pc) {
                        Some(&i) => counts[i].1 += count,
                        None => {
                            index.insert(record.0.pc, counts.len());
                            counts.push((record.0.pc, count));
                        }
                    }
                }
            }
        }
        let total: u64 = counts.iter().map(|(_, count)| *count).sum();
        if total == 0 {
            continue;
        }
        let mut items = counts;
        items.sort_by_key(|item| std::cmp::Reverse(item.1));
        for (pc, cycles) in items.into_iter().take(5) {
            let rate: f64 = format!("{:.3}", cycles as f64 * 100.0 / total as f64)
                .parse()
                .unwrap_or(0.0);
            body.push_str(&format!("{dev},0x{pc:x},{cycles},{rate}\n"));
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote ts_cpu_top_function to {}", out.display());
    Ok(())
}

fn api_statistic(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote api_statistic to {}", out.display());
    Ok(())
}

fn op_statistic(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote op_statistic to {}", out.display());
    Ok(())
}

fn l2_cache(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn dvpp(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn hccs_table(rows: &[HccsRateRow]) -> Vec<[String; 4]> {
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

fn hccs_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn hbm_track_rows(rows: &[HbmRecord]) -> Vec<CounterRow> {
    use std::collections::BTreeMap;
    const SCALE: f64 = 256.0 / 8.0 / 1024.0 / 1024.0;

    let mut by_ts: BTreeMap<i128, Vec<(u32, u32, u64)>> = BTreeMap::new();
    for row in rows {
        by_ts
            .entry(row.ts_ns)
            .or_default()
            .push((row.event, row.hbm_id, row.count));
    }
    let mut out = Vec::with_capacity(rows.len());
    let mut prev: Option<i128> = None;
    for (ts_ns, items) in by_ts {
        let delta_ns = prev.map(|p| (ts_ns - p) as f64);
        for &(event, hbm_id, count) in &items {
            // The first sample has no predecessor; msprof clamps it to zero.
            let value = match delta_ns {
                Some(dt) if dt > 0.0 => count as f64 * SCALE / dt * 1e9,
                _ => 0.0,
            };
            let (kind, key): (&str, &'static str) = if event == 0 {
                ("Read", "Read(MB/s)")
            } else {
                ("Write", "Write(MB/s)")
            };
            out.push(CounterRow {
                name: compact_str::format_compact!("HBM {hbm_id}/{kind}"),
                tid: 0,
                ts: fmt_us(ts_ns as u64, false),
                args: smallvec::smallvec![(key, Num::Float(value))],
            });
        }
        prev = Some(ts_ns);
    }
    out
}

fn hbm_table(rows: &[HbmRecord]) -> Vec<[String; 3]> {
    use std::collections::BTreeMap;
    const SCALE: f64 = 256.0 / 8.0 / 1024.0 / 1024.0;

    let mut by_ts: BTreeMap<u64, Vec<(u32, u32, u64)>> = BTreeMap::new();
    for row in rows {
        by_ts
            .entry(row.ts_us)
            .or_default()
            .push((row.event, row.hbm_id, row.count));
    }
    let stamps: Vec<u64> = by_ts.keys().copied().collect();
    let mut sums = [[0.0f64; 2]; 4];
    let samples = stamps.len() as f64;
    for window in stamps.windows(2) {
        let dur = (window[1] - window[0]) as f64 * 1000.0;
        if dur <= 0.0 {
            continue;
        }
        let Some(items) = by_ts.get(&window[1]) else {
            continue;
        };
        for &(event, hbm_id, count) in items {
            if hbm_id >= 4 || event >= 2 {
                continue;
            }
            sums[hbm_id as usize][event as usize] += count as f64 * SCALE / dur * 1e9;
        }
    }

    let mean = |hbm: usize, event: usize| {
        if samples > 0.0 {
            sums[hbm][event] / samples
        } else {
            0.0
        }
    };
    let round3 = |v: f64| (v * 1000.0).round() / 1000.0;
    let lanes: [[f64; 2]; 4] = core::array::from_fn(|h| [round3(mean(h, 0)), round3(mean(h, 1))]);
    let mut out = Vec::new();
    let units: [[f64; 2]; 4] = core::array::from_fn(|h| {
        [
            (lanes[h][0] * 1000.0).round(),
            (lanes[h][1] * 1000.0).round(),
        ]
    });
    let avg_read = units.iter().map(|u| u[0]).sum::<f64>() / 4.0 / 1000.0;
    let avg_write = units.iter().map(|u| u[1]).sum::<f64>() / 4.0 / 1000.0;
    out.push([
        "Average".to_string(),
        py_round3(avg_read),
        py_round3(avg_write),
    ]);
    for (hbm, lane) in lanes.iter().enumerate() {
        out.push([hbm.to_string(), py_round3(lane[0]), py_round3(lane[1])]);
    }
    out
}

fn hbm_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,Metric,Read(MB/s),Write(MB/s)\n");
    let mut total = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        for [metric, read, write] in hbm_table(&session.resolve::<HbmRecord>()?) {
            body.push_str(&format!("{dev},{metric},{read},{write}\n"));
            total += 1;
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn npu_mem(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, body)?;
    println!("wrote {total} rows to {}", out.display());
    Ok(())
}

fn npu_module_mem(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
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

/// Comparison strictness for the `--strict` flag: verifiers always print their
/// per-column statistics, but only a strict run turns a mismatch into a non-zero
/// exit status (so CI can gate on regressions while interactive use still shows
/// partial matches).
/// Golden file for an artifact inside `mindstudio_profiler_output`.
fn find_golden(dir: &Path, artifact: Artifact) -> Option<PathBuf> {
    let (prefix, extension) = match artifact {
        Artifact::Timeline => ("msprof_", "json"),
        other => (other.name(), "csv"),
    };
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with(prefix)
                && name.ends_with(&format!(".{extension}"))
                // `op_summary_…` must not match `op_summary_x_…`
                && (name.starts_with(&format!("{prefix}_")) || name.starts_with(prefix))
        })
        .collect();
    candidates.sort();
    candidates.pop()
}

/// One artifact's per-column distance from its golden file.
struct ArtifactCheck {
    name: &'static str,
    header: Vec<String>,
    columns: Vec<mspark::verify::ColumnMatch>,
    note: String,
}

impl ArtifactCheck {
    fn mean_ratio(&self) -> f64 {
        if self.columns.is_empty() {
            return 1.0;
        }
        self.columns
            .iter()
            .map(|column| column.ratio())
            .sum::<f64>()
            / self.columns.len() as f64
    }
}

/// Compare every artifact against the capture's goldens and the checked-in
/// baseline.
///
/// Byte-exactness is not the goal: the baseline records how close each *column*
/// currently is, and this command fails when one gets worse — a dropped row, a
/// broken column, a stream that stopped decoding. Numeric cells are compared on
/// a fixed grid (`--places`), so float noise is not a regression.
fn verify_all(
    prof: &Path,
    only: Option<&str>,
    golden_dir: Option<&Path>,
    baseline: Option<&Path>,
    update_baseline: bool,
    places: u32,
) -> Result<()> {
    let golden_dir = golden_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| prof.join("mindstudio_profiler_output"));
    let baseline_path = baseline
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("spec/baseline.txt"));
    let previous = read_baseline(&baseline_path)?;

    let dir = std::env::temp_dir().join(format!("mspark-verify-all-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;

    let mut checks: Vec<ArtifactCheck> = Vec::new();
    for artifact in ALL_ARTIFACTS {
        if matches!(artifact, Artifact::Timeline | Artifact::Summary) {
            // The timeline is compared structurally by `verify-timeline`; the
            // summary has no msprof counterpart.
            continue;
        }
        let Some(golden) = find_golden(&golden_dir, artifact) else {
            continue;
        };
        let golden_rows = read_rows(&golden)?;
        let Some(golden_header) = golden_rows.first() else {
            continue;
        };
        let golden_header = golden_header.clone();
        write_artifact(artifact, prof, only, &dir)?;
        let produced = read_rows(&dir.join(artifact.file()))?;
        let (produced_header, produced_body) = match produced.split_first() {
            Some((header, body)) => (header, body),
            None => (&golden_header, &[][..]),
        };
        if produced_header.len() != golden_header.len() {
            checks.push(ArtifactCheck {
                name: artifact.name(),
                header: golden_header.clone(),
                columns: Vec::new(),
                note: format!(
                    "header width {} vs {}",
                    golden_header.len(),
                    produced_header.len()
                ),
            });
            continue;
        }
        let columns = mspark::verify::compare_columns(&golden_rows[1..], produced_body, places);
        checks.push(ArtifactCheck {
            name: artifact.name(),
            header: golden_header,
            columns,
            note: String::new(),
        });
    }
    let _ = std::fs::remove_dir_all(&dir);

    println!(
        "{:<24} {:>8} {:>8}  closeness",
        "artifact", "golden", "produced"
    );
    let mut regressions = Vec::new();
    for check in &checks {
        let key = |column: usize| format!("{}.{}", check.name, check.header[column]);
        let mut worst = 1.0f64;
        for (index, column) in check.columns.iter().enumerate() {
            let ratio = column.ratio();
            worst = worst.min(ratio);
            if let Some(recorded) = previous.get(&key(index))
                && !update_baseline
                && ratio + 1e-9 < *recorded
            {
                regressions.push((key(index), *recorded, ratio));
            }
        }
        if !check.note.is_empty() {
            println!("{:<24} {:>8} {:>8}  GAP: {}", check.name, 0, 0, check.note);
            continue;
        }
        println!(
            "{:<24} {:>8} {:>8}  mean {:>6.2}%  worst {:>6.2}%",
            check.name,
            check.columns.first().map(|c| c.golden).unwrap_or(0),
            check.columns.first().map(|c| c.produced).unwrap_or(0),
            check.mean_ratio() * 100.0,
            worst * 100.0
        );
    }

    if update_baseline {
        write_baseline(&baseline_path, &checks)?;
        println!("\nbaseline written to {}", baseline_path.display());
        return Ok(());
    }
    if regressions.is_empty() {
        println!("\nno column regressed against {}", baseline_path.display());
        return Ok(());
    }
    for (key, recorded, ratio) in &regressions {
        eprintln!(
            "regression: {key} {:.2}% < baseline {:.2}%",
            ratio * 100.0,
            recorded * 100.0
        );
    }
    anyhow::bail!("{} column(s) regressed", regressions.len())
}

fn read_baseline(path: &Path) -> Result<std::collections::HashMap<String, f64>> {
    let mut map = std::collections::HashMap::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return Ok(map);
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // `<artifact>.<column>  <matched>/<denominator>  <ratio%>`
        // The counts are authoritative: recomputing the ratio from two integers
        // avoids comparing two rounded percentages (which made a clean run look
        // like a regression, and a stale baseline look fine).
        let mut fields = line.split_whitespace();
        let (Some(name), Some(counts)) = (fields.next(), fields.next()) else {
            continue;
        };
        let Some((matched, total)) = counts.split_once('/') else {
            continue;
        };
        let (Ok(matched), Ok(total)) = (matched.parse::<f64>(), total.parse::<f64>()) else {
            continue;
        };
        map.insert(
            name.to_string(),
            if total > 0.0 { matched / total } else { 1.0 },
        );
    }
    Ok(map)
}

fn write_baseline(path: &Path, checks: &[ArtifactCheck]) -> Result<()> {
    let mut text = String::from(
        "# Closeness baseline, one line per artifact column.\n\
         # `mspark verify-all` fails when a column drops below its recorded value;\n\
         # regenerate with `mspark verify-all PROF --update-baseline`.\n\
         # Values are the fraction of values matching the golden file at 3 decimals.\n",
    );
    for check in checks {
        if !check.note.is_empty() {
            text.push_str(&format!("# gap: {} — {}\n", check.name, check.note));
            continue;
        }
        for (index, column) in check.columns.iter().enumerate() {
            let denominator = column.golden.max(column.produced);
            text.push_str(&format!(
                "{}.{:<24} {:>8}/{}  {:>7.2}%\n",
                check.name,
                check.header[index],
                column.matched,
                denominator,
                column.ratio() * 100.0
            ));
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, text)?;
    Ok(())
}

/// Verify any CSV artifact by exporting it exactly as production does and
/// comparing the result against the golden file. Replaces the per-artifact
/// verifiers, which were copy-paste twins of the exporters and could drift.
fn verify_csv(artifact: Artifact, prof: &Path, only: Option<&str>, golden: &Path) -> Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "mspark-verify-{}-{}",
        std::process::id(),
        artifact.name()
    ));
    std::fs::create_dir_all(&dir)?;
    write_artifact(artifact, prof, only, &dir)?;
    let produced = read_rows(&dir.join(artifact.file()))?;
    let _ = std::fs::remove_dir_all(&dir);
    // Columns that only need to agree within an HPFloat residual.
    match artifact {
        Artifact::OpSummary => compare_generated(golden, &produced, Some((&[8, 9, 10], 0.001))),
        Artifact::ApiStatistic | Artifact::OpStatistic => {
            compare_generated(golden, &produced, Some((&[3, 4, 5, 6, 7, 8], 0.05)))
        }
        _ => compare_generated(golden, &produced, None),
    }
}

fn print_info(info: &DeviceInfo) {
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

fn hist<const N: usize>(hist: &[u32; N]) -> String {
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

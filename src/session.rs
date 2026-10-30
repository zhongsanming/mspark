//! Device discovery and per-device sessions.
//!
//! One [`DeviceSession`] owns the immutable [`Ctx`] and the memoised [`Store`]
//! for one `device_*` directory. Sessions are built once per process and shared
//! by every artifact, so the context files and the expensive node tables are
//! parsed once even when several reports need them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context as _, Result, bail};
use mspark_df::{Ctx, Store, resolve};
use mspark_model::{DeviceInfo, load_device_info, load_dictionaries};
use rayon::prelude::*;

/// A numeric `device_N` suffix, so `device_10` sorts after `device_2` and not
/// before it. The parent is the sort key first so a merged multi-process
/// profile keeps each capture's devices together.
fn device_order(path: &Path) -> (String, u64) {
    let parent = path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_string_lossy()
        .into_owned();
    let index = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("device_"))
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    (parent, index)
}

pub fn list_devices(prof: &Path) -> Result<Vec<PathBuf>> {
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
    devices.sort_by_key(|path| device_order(path));
    Ok(devices)
}

/// The `PROF_*` directory that owns a `device_N` directory.
pub fn prof_root_of(device: &Path) -> &Path {
    device.parent().unwrap_or(device)
}

/// All device directories under `prof`.
///
/// A single capture holds `device_*` directly. A multi-process run (e.g. one
/// DDP rank per process) produces one `PROF_*` directory per rank instead, so a
/// directory of such captures is accepted as one logical profile.
pub fn collect_devices(prof: &Path) -> Result<Vec<PathBuf>> {
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
    devices.sort_by_key(|path| device_order(path));
    devices.dedup();
    Ok(devices)
}

pub fn devices(prof: &Path, only: Option<&str>) -> Result<Vec<PathBuf>> {
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

/// One device's parsed context and memoised node tables.
///
/// The session lives in a process-wide registry: every artifact asks for the
/// same device session, so shared upstream nodes are computed once. The tables
/// are therefore retained for the lifetime of the process; releasing them per
/// artifact would re-read and re-decode the streams.
///
/// `resolve` holds the store lock for the duration of one node computation. Two
/// artifacts that need different sinks on the same device therefore compute
/// them one at a time, but the work is deduplicated: a node shared by both is
/// built once.
pub struct DeviceSession {
    pub ctx: Arc<Ctx>,
    pub info: DeviceInfo,
    store: Mutex<Store>,
}

impl DeviceSession {
    pub fn resolve<T: mspark_df::Node>(&self) -> Result<Arc<Vec<T>>> {
        let mut store = self.store.lock().expect("store lock");
        resolve::<T>(&mut store, &self.ctx).with_context(|| format!("on {}", self.label()))
    }

    /// Resolve a node and hand its rows to `out` without memoising the table.
    ///
    /// For reports whose only consumer is the writer: `op_summary` emits 300 k+
    /// rows of 46 strings, which is far more than the inputs it needs.
    pub fn resolve_into<T: mspark_df::Node>(
        &self,
        out: &mut dyn FnMut(T) -> Result<()>,
    ) -> Result<()> {
        let mut store = self.store.lock().expect("store lock");
        mspark_df::resolve_into::<T>(&mut store, &self.ctx, out)
            .with_context(|| format!("on {}", self.label()))
    }

    /// Human-readable device identity for error messages.
    pub fn label(&self) -> String {
        let device = self
            .ctx
            .device_dir()
            .map(|dir| dir.display().to_string())
            .unwrap_or_else(|_| "<unknown device>".to_string());
        match self.info.device_id {
            Some(id) => format!("device {id} ({device})"),
            None => device,
        }
    }

    pub fn ctx(&self) -> &Ctx {
        &self.ctx
    }

    /// Number of nodes memoised for this device (`mspark stats`).
    pub fn memoised_nodes(&self) -> usize {
        self.store.lock().expect("store lock").len()
    }
}

/// Per-device session, built at most once per process.
///
/// The lock is held **across** the build on purpose: artifacts run in parallel,
/// and releasing it before `build_ctx` made every racing artifact miss the cache
/// and rebuild (20 artifacts → 20 loads of the same start logs, visible in
/// `strace` as 20+ opens of one `start_info`).
pub fn session_for(device: &Path) -> Result<Arc<DeviceSession>> {
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
pub fn per_session<R, F>(prof: &Path, only: Option<&str>, f: F) -> Result<Vec<R>>
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

/// Run `f` once per device in parallel and return the per-device chunks in
/// device order, so output stays deterministic.
pub fn per_device<R, F>(prof: &Path, only: Option<&str>, f: F) -> Result<Vec<R>>
where
    R: Send,
    F: Fn(&DeviceSession) -> Result<R> + Sync + Send,
{
    per_session(prof, only, f)
}

pub fn build_ctx(device: &Path) -> Result<(Ctx, DeviceInfo)> {
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
    ctx.insert(
        mspark_model::HostInfo::load(root)
            .with_context(|| format!("loading host info for {}", root.display()))?,
    );
    ctx.insert(
        mspark_model::SampleConfig::load(device)
            .with_context(|| format!("loading sample config for {}", device.display()))?,
    );
    // Host id→text dictionaries are loaded before any node runs.
    let (hash, types) = load_dictionaries(&root.join("host").join("data"));
    ctx.insert(hash);
    ctx.insert(types);
    Ok((ctx, info))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_order_is_numeric() {
        let path = |name: &str| PathBuf::from("/p/PROF_1").join(name);
        let mut names = [
            path("device_10"),
            path("device_2"),
            path("device_1"),
            path("device_0"),
        ];
        names.sort_by_key(|p| device_order(p));
        assert_eq!(
            names
                .iter()
                .map(|p| p.file_name().unwrap().to_str().unwrap())
                .collect::<Vec<_>>(),
            ["device_0", "device_1", "device_2", "device_10"]
        );
    }

    #[test]
    fn unknown_device_suffix_sorts_last() {
        let path = |name: &str, parent: &str| PathBuf::from(parent).join(name);
        let mut names = [
            path("device_other", "/p/PROF_1"),
            path("device_3", "/p/PROF_1"),
        ];
        names.sort_by_key(|p| device_order(p));
        assert_eq!(names[0].file_name().unwrap(), "device_3");
    }
}

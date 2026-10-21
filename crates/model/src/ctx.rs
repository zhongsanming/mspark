//! Device context: parsed once, before any node runs.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow};
use serde_json::Value;

/// Chip family. Extend as support is ported; v1 validates against `V4_1_0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Chip {
    V4_1_0,
}

impl Chip {
    pub fn from_platform_version(v: i64) -> Option<Self> {
        match v {
            5 => Some(Self::V4_1_0),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::V4_1_0 => "V4_1_0",
        }
    }
}

/// Everything a node needs to know about the device it is parsing.
#[derive(Clone, Debug)]
pub struct DeviceInfo {
    pub chip: Chip,
    pub device_id: Option<u32>,
    pub ai_core_num: u32,
    pub aiv_num: u32,
    pub aic_freq_mhz: f64,
    pub aiv_freq_mhz: f64,
    pub hwts_freq_mhz: f64,
    /// Host CPU frequency (MHz) from `info.json` `CPU[0].Frequency`; used for the
    /// `hostCntvctDiff` correction.
    pub host_freq_mhz: f64,
    /// Host process id from `info.json` (`pid`).
    pub pid: u32,
    /// `StreamExpandSpec.expand_status`; msprof defaults to 0 when the table is absent.
    pub expand_status: u16,
}

/// Load `info.json` (or `info.json.<id>`) from a device directory.
pub fn load_device_info(device_dir: &Path) -> Result<DeviceInfo> {
    let path = find_info_json(device_dir)?;
    let text = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let root: Value =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    let platform_version = root
        .get("platform_version")
        .and_then(as_i64)
        .ok_or_else(|| anyhow!("{} has no platform_version", path.display()))?;
    let chip = Chip::from_platform_version(platform_version).ok_or_else(|| {
        anyhow!("unsupported platform_version {platform_version} (expected 5 = V4_1_0)")
    })?;

    let device = root
        .get("DeviceInfo")
        .and_then(Value::as_array)
        .and_then(|a| a.first());

    Ok(DeviceInfo {
        chip,
        device_id: device
            .and_then(|d| d.get("id"))
            .and_then(as_i64)
            .map(|v| v as u32),
        ai_core_num: device
            .and_then(|d| d.get("ai_core_num"))
            .and_then(as_i64)
            .unwrap_or(0) as u32,
        aiv_num: device
            .and_then(|d| d.get("aiv_num"))
            .and_then(as_i64)
            .unwrap_or(0) as u32,
        aic_freq_mhz: device
            .and_then(|d| d.get("aic_frequency"))
            .and_then(as_f64)
            .unwrap_or(0.0),
        aiv_freq_mhz: device
            .and_then(|d| d.get("aiv_frequency"))
            .and_then(as_f64)
            .unwrap_or(0.0),
        hwts_freq_mhz: device
            .and_then(|d| d.get("hwts_frequency"))
            .and_then(as_f64)
            .unwrap_or(0.0),
        host_freq_mhz: root
            .get("CPU")
            .and_then(Value::as_array)
            .and_then(|a| a.last())
            .and_then(|c| c.get("Frequency"))
            .and_then(as_f64)
            .unwrap_or(1000.0),
        pid: root
            .get("pid")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0) as u32,
        expand_status: 0,
    })
}

fn find_info_json(dir: &Path) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".done") {
            continue;
        }
        if name == "info.json" || name.starts_with("info.json.") {
            candidates.push(entry.path());
        }
    }
    candidates.sort();
    candidates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no info.json under {}", dir.display()))
}

fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Read a string field from the device `sample.json`.
pub fn load_sample_string(
    device_dir: &std::path::Path,
    key: &str,
) -> anyhow::Result<Option<String>> {
    let path = device_dir.join("sample.json");
    let text = std::fs::read_to_string(&path)?;
    let value: serde_json::Value = serde_json::from_str(&text)?;
    Ok(value
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

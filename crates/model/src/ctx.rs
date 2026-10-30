//! Device context: parsed once, before any node runs.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow};
use serde_json::Value;

/// Chip family, keyed by the `platform_version` reported in `info.json`.
///
/// The values match `ChipId` in msprof (`infrastructure/resource/chip_id.h`).
/// Only [`Chip::V4_1_0`] has been verified against a capture; the rest are
/// ported from the reference implementation and are **unverified** — see the
/// "Chip support" section of the README.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Chip {
    V1_1_0,
    V1_1_1,
    V1_1_2,
    V1_1_3,
    V2_1_0,
    V3_1_0,
    V3_2_0,
    V3_3_0,
    V4_1_0,
    V5_1_0,
    V6_1_0,
    V6_1_1,
    V6_2_0,
}

impl Chip {
    fn from_version(v: i64) -> Option<Self> {
        match v {
            0 => Some(Self::V1_1_0),
            1 => Some(Self::V2_1_0),
            2 => Some(Self::V3_1_0),
            3 => Some(Self::V3_2_0),
            4 => Some(Self::V3_3_0),
            5 => Some(Self::V4_1_0),
            7 => Some(Self::V1_1_1),
            8 => Some(Self::V1_1_2),
            9 => Some(Self::V5_1_0),
            11 => Some(Self::V1_1_3),
            15 => Some(Self::V6_1_0),
            16 => Some(Self::V6_2_0),
            19 => Some(Self::V6_1_1),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::V1_1_0 => "V1_1_0",
            Self::V1_1_1 => "V1_1_1",
            Self::V1_1_2 => "V1_1_2",
            Self::V1_1_3 => "V1_1_3",
            Self::V2_1_0 => "V2_1_0",
            Self::V3_1_0 => "V3_1_0",
            Self::V3_2_0 => "V3_2_0",
            Self::V3_3_0 => "V3_3_0",
            Self::V4_1_0 => "V4_1_0",
            Self::V5_1_0 => "V5_1_0",
            Self::V6_1_0 => "V6_1_0",
            Self::V6_1_1 => "V6_1_1",
            Self::V6_2_0 => "V6_2_0",
        }
    }

    /// `Atlas 200/500/300` family (`IsChipV1` upstream).
    pub fn is_v1(self) -> bool {
        matches!(
            self,
            Self::V1_1_0 | Self::V1_1_1 | Self::V1_1_2 | Self::V1_1_3
        )
    }

    /// `910B` family (`IsChipV4` upstream).
    pub fn is_v4(self) -> bool {
        matches!(self, Self::V4_1_0)
    }

    /// Next-generation datacenter parts (`IsChipV6` upstream).
    pub fn is_v6(self) -> bool {
        matches!(self, Self::V6_1_0 | Self::V6_1_1 | Self::V6_2_0)
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
    /// `info.json:version` — the layout version of the versioned streams.
    pub collection_version: String,
}

/// Load `info.json` (or `info.json.<id>`) from a device directory.
pub fn load_device_info(device_dir: &Path) -> Result<DeviceInfo> {
    let path = find_info_json(device_dir)?;
    let text = crate::errors::read_to_string(&path)?;
    let root: Value = serde_json::from_str(&text)
        .map_err(|error| crate::errors::json_error(&path, &text, &error))?;

    let platform_version = root
        .get("platform_version")
        .and_then(as_i64)
        .ok_or_else(|| anyhow!("{} has no platform_version", path.display()))?;
    let chip = Chip::try_from(platform_version)
        .with_context(|| format!("reading platform_version from {}", path.display()))?;
    // `info.json:version` selects the binary layout of the versioned streams
    // (`hbm`, `llc`, `npu_mem`); msprof defaults to "1.0" when absent.
    let collection_version = root
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("1.0")
        .to_string();

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
        expand_status: load_expand_status(device_dir),
        collection_version,
    })
}

/// `StreamExpandSpec.expand_status`: `1` when the capture hit the stream-expand
/// (dynamic stream) scenario, `0` otherwise.
///
/// The stream holds 64-byte records laid out as `<2H3IQ4B9I`; msprof reads the
/// seventh unpacked field, i.e. the first of the four bytes at offset 24.
/// Absent stream → `0` (msprof's default).
fn load_expand_status(device_dir: &Path) -> u16 {
    let data = device_dir.join("data");
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    let Ok(entries) = fs::read_dir(&data) else {
        return 0;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("stream_expand_spec.") || name.ends_with(".done") {
            continue;
        }
        let slice = name
            .rsplit('_')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        files.push((slice, entry.path()));
    }
    files.sort_by_key(|entry| entry.0);
    for (_, path) in files {
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        if bytes.len() >= 25 {
            return u16::from(bytes[24]);
        }
    }
    0
}

fn find_info_json(dir: &Path) -> Result<PathBuf> {
    crate::stream::files_with_prefix(dir, &["info.json"])
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
    let text = crate::errors::read_to_string(&path)?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| crate::errors::json_error(&path, &text, &error))?;
    Ok(value
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

impl TryFrom<i64> for Chip {
    type Error = anyhow::Error;

    /// `info.json:platform_version` → chip generation.
    fn try_from(version: i64) -> Result<Self> {
        Chip::from_version(version).ok_or_else(|| {
            anyhow!(
                "unknown platform_version {version}; known values are \
                 0,1,2,3,4,5,7,8,9,11,15,16,19"
            )
        })
    }
}

/// The `host/info.json` fields that are not part of [`DeviceInfo`].
#[derive(Debug, Clone, Default)]
pub struct HostInfo {
    /// `memoryTotal` (KB) — the denominator for the memory-percentage columns.
    pub memory_total_kb: u64,
    /// `cpuNums` — the per-interval CPU count for the `Total` usage rows.
    pub cpu_nums: u32,
}

impl HostInfo {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join("host").join("info.json");
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            // Host mem/disk artefacts are optional: an absent file is not an error.
            Err(_) => return Ok(Self::default()),
        };
        let root: Value = serde_json::from_str(&text)
            .map_err(|error| crate::errors::json_error(&path, &text, &error))?;
        let as_u64 = |key: &str| {
            root.get(key).and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
        };
        Ok(Self {
            memory_total_kb: as_u64("memoryTotal").unwrap_or(0),
            cpu_nums: as_u64("cpuNums").unwrap_or(0) as u32,
        })
    }
}

/// `sample.json` for a device: the collection switches and sample settings
/// (`l2CacheTaskProfilingEvents`, `host_disk_freq`, `msproftx`, …).
#[derive(Debug, Clone, Default)]
pub struct SampleConfig(pub Value);

impl SampleConfig {
    pub fn load(device_dir: &Path) -> Result<Self> {
        let path = device_dir.join("sample.json");
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(_) => return Ok(Self(Value::Null)),
        };
        let value = serde_json::from_str(&text)
            .map_err(|error| crate::errors::json_error(&path, &text, &error))?;
        Ok(Self(value))
    }

    pub fn string(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(Value::as_str)
    }

    pub fn f64(&self, key: &str) -> Option<f64> {
        self.0.get(key).and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
    }

    /// A comma-separated list, trimmed and lowercased, empty entries dropped.
    pub fn list(&self, key: &str) -> Vec<String> {
        self.string(key)
            .unwrap_or("")
            .split(',')
            .map(|item| item.trim().to_lowercase())
            .filter(|item| !item.is_empty())
            .collect()
    }
}

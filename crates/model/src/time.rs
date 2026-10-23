//! Device syscnt → wall-clock conversion.
//!
//! Mirrors msprof's `GetTimeFromSyscnt` + `GetLocalTime`:
//!
//! ```text
//! t_ns       = host_monotonic_ns + (raw_tick - devCntvct) * 1000 / hwts_frequency
//! wall_ns    = (startCollectionTimeBegin*1000 - startClockMonotonicRaw) + t_ns
//! host_mono += hostCntvctDiff * 1000 / hostFreq
//! ```
//!
//! msprof stores absolute timestamps as `double` (via `HPFloat::Double()`), so
//! its last nanosecond is quantised. We reproduce the same shape of computation;
//! the residual against the golden CSV is ≤1 ns per field.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow};
use serde_json::Value;

/// All constants needed to map a device tick to wall-clock nanoseconds.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    /// `startCollectionTimeBegin*1000 - startClockMonotonicRaw`.
    pub offset_ns: i128,
    /// `host_start.log:clock_monotonic_raw` plus the `cntvct_diff` correction.
    pub host_mono: u64,
    /// `dev_start.log:cntvct`.
    pub ref_cnt: u64,
    /// `info.json:DeviceInfo[0].hwts_frequency` (MHz).
    pub freq_mhz: f64,
    /// `start_info.collectionTimeBegin` (us).
    pub begin_us: u64,
    /// `end_info.collectionTimeEnd` (us).
    pub end_us: u64,
}

impl Clock {
    /// `GetTimeFromSyscnt(raw).Double()`: absolute syscnt time in ns.
    fn hp(&self, raw: u64) -> f64 {
        let diff = raw as f64 - self.ref_cnt as f64;
        // `HPFloat(q) << 3` is exact; then add the integer and round once.
        (diff / self.freq_mhz).mul_add(1000.0, self.host_mono as f64)
    }

    /// Absolute wall-clock time (ns) for a device syscnt tick.
    pub fn to_ns(&self, raw: u64) -> u64 {
        (self.offset_ns + self.hp(raw) as i128).max(0) as u64
    }

    /// Duration (ns) between two device syscnt ticks.
    pub fn dur_ns(&self, start: u64, end: u64) -> u64 {
        (self.hp(end) - self.hp(start)).max(0.0) as u64
    }

    /// Duration in microseconds with the same 0.5 ns quantisation as msprof's
    /// `double` timestamps (used by the timeline JSON `dur` field).
    pub fn duration_us_f64(&self, start: u64, end: u64) -> f64 {
        (self.hp(end) - self.hp(start)) / 1000.0
    }
}

/// Load the conversion parameters from `start_info.*`, `dev_start.log.*`,
/// `host_start.log.*`, and the host CPU frequency from `info.json`.
pub fn load_clock(device_dir: &Path, host_freq_mhz: f64, hwts_freq_mhz: f64) -> Result<Clock> {
    let start_info = read_json(&find_one(device_dir, "start_info")?)?;
    let collection_begin_us = as_u64(&start_info, "collectionTimeBegin")
        .ok_or_else(|| anyhow!("start_info has no collectionTimeBegin"))?;
    let start_clock_mono = as_u64(&start_info, "clockMonotonicRaw")
        .ok_or_else(|| anyhow!("start_info has no clockMonotonicRaw"))?;

    let dev = read_kv(&find_one(device_dir, "dev_start.log")?)?;
    let ref_cnt = dev
        .get("cntvct")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("dev_start.log has no cntvct"))?;

    let host = read_kv(&find_one(device_dir, "host_start.log")?)?;
    let host_mono = host
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no clock_monotonic_raw"))?;
    let diff = host
        .get("cntvct_diff")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let host_mono = host_mono + (diff as f64 * 1000.0 / host_freq_mhz) as u64;

    let end_us = find_one(device_dir, "end_info")
        .ok()
        .and_then(|path| read_json(&path).ok())
        .and_then(|value| as_u64(&value, "collectionTimeEnd"))
        .unwrap_or(collection_begin_us);

    Ok(Clock {
        offset_ns: collection_begin_us as i128 * 1000 - start_clock_mono as i128,
        host_mono,
        ref_cnt,
        freq_mhz: hwts_freq_mhz,
        begin_us: collection_begin_us,
        end_us,
    })
}

/// Load the host clock (`Context::GetSyscntConversionParams(HOST_ID)`):
/// `hostCntvct` / `clock_monotonic_raw` from `host/host_start.log`, the wall
/// offset from `host/start_info`, and the CPU frequency from `host/info.json`.
pub fn load_host_clock(prof: &Path) -> Result<Clock> {
    let host_dir = prof.join("host");
    let start_info = read_json(&find_one(&host_dir, "start_info")?)?;
    let collection_begin_us = as_u64(&start_info, "collectionTimeBegin")
        .ok_or_else(|| anyhow!("host start_info has no collectionTimeBegin"))?;
    let start_clock_mono = as_u64(&start_info, "clockMonotonicRaw")
        .ok_or_else(|| anyhow!("host start_info has no clockMonotonicRaw"))?;

    let info = read_json(&find_one(&host_dir, "info.json")?)?;
    let host_freq_mhz = info
        .get("CPU")
        .and_then(serde_json::Value::as_array)
        .and_then(|a| a.last())
        .and_then(|c| c.get("Frequency"))
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(1000.0);

    let host = read_kv(&find_one(&host_dir, "host_start.log")?)?;
    let ref_cnt = host
        .get("cntvct")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no cntvct"))?;
    let mono = host
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no clock_monotonic_raw"))?;
    let diff = host
        .get("cntvct_diff")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let host_mono = mono + (diff as f64 * 1000.0 / host_freq_mhz) as u64;
    let end_us = find_one(&host_dir, "end_info")
        .ok()
        .and_then(|path| read_json(&path).ok())
        .and_then(|value| as_u64(&value, "collectionTimeEnd"))
        .unwrap_or(collection_begin_us);

    Ok(Clock {
        offset_ns: collection_begin_us as i128 * 1000 - start_clock_mono as i128,
        host_mono,
        ref_cnt,
        freq_mhz: host_freq_mhz,
        begin_us: collection_begin_us,
        end_us,
    })
}

/// Clock using the **host** counter (`hostCntvct` / host frequency) but the
/// device wall offset — this is what `npu_module_mem` timestamps use
/// (`GetTimeFromHostCnt` + `GetLocalTime` with the device `ProfTimeRecord`).
pub fn load_host_syscnt_clock(device_dir: &Path, host_freq_mhz: f64) -> Result<Clock> {
    let start_info = read_json(&find_one(device_dir, "start_info")?)?;
    let collection_begin_us = as_u64(&start_info, "collectionTimeBegin")
        .ok_or_else(|| anyhow!("start_info has no collectionTimeBegin"))?;
    let start_clock_mono = as_u64(&start_info, "clockMonotonicRaw")
        .ok_or_else(|| anyhow!("start_info has no clockMonotonicRaw"))?;

    let host = read_kv(&find_one(device_dir, "host_start.log")?)?;
    let ref_cnt = host
        .get("cntvct")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no cntvct"))?;
    let mono = host
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no clock_monotonic_raw"))?;
    let diff = host
        .get("cntvct_diff")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let host_mono = mono + (diff as f64 * 1000.0 / host_freq_mhz) as u64;
    let end_us = find_one(device_dir, "end_info")
        .ok()
        .and_then(|path| read_json(&path).ok())
        .and_then(|value| as_u64(&value, "collectionTimeEnd"))
        .unwrap_or(collection_begin_us);

    Ok(Clock {
        offset_ns: collection_begin_us as i128 * 1000 - start_clock_mono as i128,
        host_mono,
        ref_cnt,
        freq_mhz: host_freq_mhz,
        begin_us: collection_begin_us,
        end_us,
    })
}

fn find_one(dir: &Path, prefix: &str) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".done") {
            continue;
        }
        if name == prefix || name.starts_with(&format!("{prefix}.")) {
            candidates.push(entry.path());
        }
    }
    candidates.sort();
    candidates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no {prefix} under {}", dir.display()))
}

fn read_json(path: &Path) -> Result<Value> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn read_kv(path: &Path) -> Result<HashMap<String, String>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut map = HashMap::new();
    for line in text.lines() {
        if let Some((k, v)) = line.split_once(':') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(map)
}

fn as_u64(v: &Value, key: &str) -> Option<u64> {
    v.get(key).and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
    })
}

/// `DivideByPowersOfTenWithPrecision(value, highPrecision, 3, 3)`.
///
/// `high = true` appends a tab (absolute-time columns, so Excel keeps text).
pub fn fmt_us(ns: u64, high: bool) -> compact_str::CompactString {
    // Written by hand: the previous `to_string` + slicing + `format!` version
    // allocated three times per event, and this runs once per timeline event.
    let mut s = compact_str::CompactString::new("");
    let int = ns / 1000;
    let frac = ns % 1000;
    if int == 0 {
        s.push('0');
    } else {
        let mut digits = [0u8; 20];
        let mut n = int;
        let mut count = 0;
        while n > 0 {
            digits[count] = b'0' + (n % 10) as u8;
            n /= 10;
            count += 1;
        }
        while count > 0 {
            count -= 1;
            s.push(digits[count] as char);
        }
    }
    s.push('.');
    s.push((b'0' + (frac / 100) as u8) as char);
    s.push((b'0' + ((frac / 10) % 10) as u8) as char);
    s.push((b'0' + (frac % 10) as u8) as char);
    if high {
        s.push('\t');
    }
    s
}

/// Clock for the device streams that record **monotonic ns** (`hbm`, `hccs`,
/// `llc`, `pcie`, `nic`, `roce`, `qos`) instead of hwts counts.
///
/// Verified against a fresh V4 profile: every `HCCSEvents.timestampNs` in the
/// intermediate `msprof_*.db` equals
///
/// ```text
/// raw_ns + (host_mono_ns - dev_start_mono_ns) + (begin_us*1000 - start_info.cmr)
/// ```
///
/// (the first `HCCS` row is raw sample 1, because the model pairs each sample
/// with its predecessor).
pub struct MonoClock {
    /// Added to a raw monotonic-ns stamp to obtain absolute wall ns.
    pub offset_ns: i128,
    /// `start_info.collectionTimeBegin` (us).
    pub begin_us: u64,
}

impl MonoClock {
    pub fn to_ns(&self, raw_ns: u64) -> i128 {
        raw_ns as i128 + self.offset_ns
    }
}

pub fn load_mono_clock(device_dir: &Path, host_freq_mhz: f64) -> Result<MonoClock> {
    let start_info = read_json(&find_one(device_dir, "start_info")?)?;
    let begin_us = as_u64(&start_info, "collectionTimeBegin")
        .ok_or_else(|| anyhow!("start_info has no collectionTimeBegin"))?;
    let cmr = as_u64(&start_info, "clockMonotonicRaw")
        .ok_or_else(|| anyhow!("start_info has no clockMonotonicRaw"))?;

    let dev = read_kv(&find_one(device_dir, "dev_start.log")?)?;
    let dev_mono = dev
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("dev_start.log has no clock_monotonic_raw"))?;

    let host = read_kv(&find_one(device_dir, "host_start.log")?)?;
    let host_mono = host
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no clock_monotonic_raw"))?;
    let diff = host
        .get("cntvct_diff")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let correction = if host_freq_mhz > 0.0 {
        (diff as f64 * 1000.0 / host_freq_mhz).round() as i128
    } else {
        0
    };

    let offset_ns = (host_mono as i128 + correction - dev_mono as i128)
        + (begin_us as i128 * 1000 - cmr as i128);
    Ok(MonoClock {
        offset_ns,
        begin_us,
    })
}

/// Clock for the streams whose timestamps are **microsecond sampling stamps**
/// (`hbm`, `llc`, `nic`, `roce`).
///
/// msprof computes `int(us*1000 + start + delta*1e9 + offset*1000)` in `f64`;
/// reproducing that order matters, because at ~1.79e18 ns a single `f64` ulp is
/// 256 ns and the result is 129 ns away from exact integer arithmetic.
pub struct SamplingClock {
    start_ns: u64,
    delta_ns: f64,
    offset_us: f64,
}

impl SamplingClock {
    pub fn to_ns(&self, raw_us: u64) -> i128 {
        (raw_us as f64 * 1000.0 + self.start_ns as f64 + self.delta_ns + self.offset_us * 1000.0)
            as i128
    }
}

pub fn load_sampling_clock(device_dir: &Path) -> Result<SamplingClock> {
    let start_info = read_json(&find_one(device_dir, "start_info")?)?;
    let begin_us = as_u64(&start_info, "collectionTimeBegin")
        .ok_or_else(|| anyhow!("start_info has no collectionTimeBegin"))?;
    let cmr = as_u64(&start_info, "clockMonotonicRaw")
        .ok_or_else(|| anyhow!("start_info has no clockMonotonicRaw"))?;

    let dev = read_kv(&find_one(device_dir, "dev_start.log")?)?;
    let start_ns = dev
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("dev_start.log has no clock_monotonic_raw"))?;

    let host = read_kv(&find_one(device_dir, "host_start.log")?)?;
    let host_ns = host
        .get("clock_monotonic_raw")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("host_start.log has no clock_monotonic_raw"))?;

    let delta_seconds = (host_ns as f64 - start_ns as f64) / 1e9;
    let offset_us = begin_us as f64 - cmr as f64 / 1000.0;
    Ok(SamplingClock {
        start_ns,
        delta_ns: delta_seconds * 1e9,
        offset_us,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_msprof() {
        assert_eq!(fmt_us(0, false), "0.000");
        assert_eq!(fmt_us(999, true), "0.999\t");
        assert_eq!(fmt_us(1_234_567, false), "1234.567");
    }

    #[test]
    fn converts_ticks_at_frequency() {
        let clock = Clock {
            offset_ns: 0,
            host_mono: 0,
            ref_cnt: 1_000_000,
            freq_mhz: 50.0,
            begin_us: 0,
            end_us: 0,
        };
        // 1e6 ticks at 50 MHz = 20 ms = 20_000_000 ns.
        assert_eq!(clock.to_ns(2_000_000), 20_000_000);
        assert_eq!(clock.dur_ns(1_000_000, 2_000_000), 20_000_000);
    }
}

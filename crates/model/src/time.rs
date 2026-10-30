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
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
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
/// The four small logs every clock below is derived from.
///
/// They used to be re-read — and re-parsed — by each of the five clock loaders,
/// which showed up as 20+ opens of the same `start_info` per device in a single
/// export. Loading them once (into `Ctx`) and deriving every clock from the
/// parsed values keeps the arithmetic identical and touches the disk once.
#[derive(Debug, Clone, Default)]
pub struct StartLogs {
    begin_us: u64,
    /// `start_info.clockMonotonicRaw` (ns).
    cmr: u64,
    end_us: u64,
    /// `dev_start.log` — absent for host-side logs.
    dev: Option<HashMap<String, String>>,
    /// `host_start.log`.
    host: Option<HashMap<String, String>>,
}

/// Device-side start logs (`<device>/start_info`, `dev_start.log`, …).
///
/// A newtype so `Ctx` can also hold [`HostStartLogs`]; units of the same type
/// would otherwise collide in the type-keyed context.
#[derive(Debug, Clone)]
pub struct DeviceStartLogs(pub StartLogs);

/// Host-side start logs (`<root>/host/start_info`, `host_start.log`, …).
#[derive(Debug, Clone)]
pub struct HostStartLogs(pub StartLogs);

impl StartLogs {
    /// Read `start_info`, `dev_start.log`, `host_start.log` and `end_info` from
    /// `dir` (a device directory).
    pub fn load(dir: &Path) -> Result<Self> {
        let start_info = read_json(&find_one(dir, "start_info")?)?;
        let begin_us = as_u64(&start_info, "collectionTimeBegin")
            .ok_or_else(|| anyhow!("start_info has no collectionTimeBegin"))?;
        let cmr = as_u64(&start_info, "clockMonotonicRaw")
            .ok_or_else(|| anyhow!("start_info has no clockMonotonicRaw"))?;
        let end_us = find_one(dir, "end_info")
            .ok()
            .and_then(|path| read_json(&path).ok())
            .and_then(|value| as_u64(&value, "collectionTimeEnd"))
            .unwrap_or(begin_us);
        let dev = find_one(dir, "dev_start.log")
            .ok()
            .and_then(|path| read_kv(&path).ok());
        let host = find_one(dir, "host_start.log")
            .ok()
            .and_then(|path| read_kv(&path).ok());
        Ok(Self {
            begin_us,
            cmr,
            end_us,
            dev,
            host,
        })
    }

    /// `start_info.collectionTimeBegin` (us).
    pub fn begin_us(&self) -> u64 {
        self.begin_us
    }

    /// `start_info.clockMonotonicRaw` (ns).
    pub fn clock_monotonic_raw(&self) -> u64 {
        self.cmr
    }

    /// `end_info.collectionTimeEnd` (us), defaulting to the begin stamp.
    pub fn end_us(&self) -> u64 {
        self.end_us
    }

    fn dev_field(&self, key: &str, file: &str) -> Result<u64> {
        self.dev
            .as_ref()
            .and_then(|map| map.get(key))
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("{file} has no {key}"))
    }

    fn host_field(&self, key: &str, file: &str) -> Result<u64> {
        self.host
            .as_ref()
            .and_then(|map| map.get(key))
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("{file} has no {key}"))
    }

    fn host_field_or_zero(&self, key: &str) -> u64 {
        self.host
            .as_ref()
            .and_then(|map| map.get(key))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    /// `host_start.log` values corrected by `cntvct_diff` (ns).
    fn corrected_host_mono(&self, host_freq_mhz: f64) -> Result<u64> {
        let mono = self.host_field("clock_monotonic_raw", "host_start.log")?;
        let diff = self.host_field_or_zero("cntvct_diff");
        Ok(mono + (diff as f64 * 1000.0 / host_freq_mhz) as u64)
    }

    /// Device-side `Clock`: hwts ticks against the device `cntvct`.
    pub fn clock(&self, host_freq_mhz: f64, hwts_freq_mhz: f64) -> Result<Clock> {
        let ref_cnt = self.dev_field("cntvct", "dev_start.log")?;
        Ok(Clock {
            offset_ns: self.begin_us as i128 * 1000 - self.cmr as i128,
            host_mono: self.corrected_host_mono(host_freq_mhz)?,
            ref_cnt,
            freq_mhz: hwts_freq_mhz,
            begin_us: self.begin_us,
            end_us: self.end_us,
        })
    }

    /// Host `Clock`: host `cntvct` ticks against the host frequency.
    ///
    /// Used for both the host API events (`host/…`) and `npu_module_mem`
    /// (device directory, but host-timed), which is why it takes the frequency.
    pub fn host_clock(&self, host_freq_mhz: f64) -> Result<Clock> {
        let ref_cnt = self.host_field("cntvct", "host_start.log")?;
        Ok(Clock {
            offset_ns: self.begin_us as i128 * 1000 - self.cmr as i128,
            host_mono: self.corrected_host_mono(host_freq_mhz)?,
            ref_cnt,
            freq_mhz: host_freq_mhz,
            begin_us: self.begin_us,
            end_us: self.end_us,
        })
    }

    /// Microsecond-sampling clock (`hbm`, `llc`, `nic`, `roce`).
    pub fn sampling(&self) -> Result<SamplingClock> {
        let start_ns = self.dev_field("clock_monotonic_raw", "dev_start.log")?;
        let host_ns = self.host_field("clock_monotonic_raw", "host_start.log")?;
        let delta_seconds = (host_ns as f64 - start_ns as f64) / 1e9;
        let offset_us = self.begin_us as f64 - self.cmr as f64 / 1000.0;
        Ok(SamplingClock {
            start_ns,
            delta_ns: delta_seconds * 1e9,
            offset_us,
        })
    }

    /// Raw-tick clock for the streams msprof converts without the host
    /// correction (`pcie`, `hccs`, `nic`/`roce` bandwidth).
    pub fn mono(&self, correction_freq_mhz: Option<f64>) -> Result<MonoClock> {
        let dev_mono = self.dev_field("clock_monotonic_raw", "dev_start.log")?;
        let host_mono = self.host_field("clock_monotonic_raw", "host_start.log")?;
        let diff = self.host_field_or_zero("cntvct_diff");
        let correction = match correction_freq_mhz {
            Some(freq) if freq > 0.0 => (diff as f64 * 1000.0 / freq).round() as i128,
            _ => 0,
        };
        Ok(MonoClock {
            offset_ns: (host_mono as i128 + correction - dev_mono as i128)
                + (self.begin_us as i128 * 1000 - self.cmr as i128),
            begin_us: self.begin_us,
        })
    }
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

/// Load the device clock (kept for callers that have no `Ctx` at hand).
pub fn load_clock(device_dir: &Path, host_freq_mhz: f64, hwts_freq_mhz: f64) -> Result<Clock> {
    StartLogs::load(device_dir)?.clock(host_freq_mhz, hwts_freq_mhz)
}

/// Load the host clock from `<root>/host`.
pub fn load_host_clock(root: &Path) -> Result<Clock> {
    let logs = StartLogs::load(&root.join("host"))?;
    let info = read_json(&find_one(&root.join("host"), "info.json")?)?;
    let host_freq_mhz = info
        .get("CPU")
        .and_then(Value::as_array)
        .and_then(|a| a.last())
        .and_then(|c| c.get("Frequency"))
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(1000.0);
    logs.host_clock(host_freq_mhz)
}

/// Load the host-timed clock from a device directory (`npu_module_mem`).
pub fn load_host_syscnt_clock(device_dir: &Path, host_freq_mhz: f64) -> Result<Clock> {
    StartLogs::load(device_dir)?.host_clock(host_freq_mhz)
}

pub fn load_sampling_clock(device_dir: &Path) -> Result<SamplingClock> {
    StartLogs::load(device_dir)?.sampling()
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

/// `host_freq_mhz` enables the `hostCntvctDiff` correction; pass `None` for the
/// streams that msprof converts without it (PCIe, HCCS, NIC/RoCE).
pub fn load_mono_clock(device_dir: &Path, host_freq_mhz: Option<f64>) -> Result<MonoClock> {
    StartLogs::load(device_dir)?.mono(host_freq_mhz)
}

fn find_one(dir: &Path, prefix: &str) -> Result<PathBuf> {
    crate::stream::files_with_prefix(dir, &[prefix])
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no {prefix} under {}", dir.display()))
}

fn read_json(path: &Path) -> Result<Value> {
    let text = crate::errors::read_to_string(path)?;
    serde_json::from_str(&text).map_err(|error| crate::errors::json_error(path, &text, &error))
}

fn read_kv(path: &Path) -> Result<HashMap<String, String>> {
    let text = crate::errors::read_to_string(path)?;
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

#[cfg(test)]
mod tests {
    /// Regression: `device_N/start_info.N.done` contains `filesize:178`, not
    /// JSON. A hand-rolled directory scan picked it up depending on readdir
    /// order and failed with "expected ident at line 1 column 2".
    #[test]
    fn start_logs_ignore_done_markers() {
        let dir = std::env::temp_dir().join(format!("mspark-startlogs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("start_info.7"),
            r#"{"collectionTimeBegin":"1792632287145380","clockMonotonicRaw":"5396596301416840"}"#,
        )
        .unwrap();
        std::fs::write(dir.join("start_info.7.done"), "filesize:178").unwrap();
        std::fs::write(
            dir.join("dev_start.log.7"),
            "cntvct:123\nclock_monotonic_raw:456\n",
        )
        .unwrap();
        std::fs::write(dir.join("dev_start.log.7.done"), "filesize:40").unwrap();
        std::fs::write(
            dir.join("host_start.log.7"),
            "cntvct:1\nclock_monotonic_raw:2\n",
        )
        .unwrap();
        std::fs::write(dir.join("host_start.log.7.done"), "filesize:30").unwrap();
        std::fs::write(
            dir.join("end_info.7"),
            r#"{"collectionTimeEnd":"1792632310813987"}"#,
        )
        .unwrap();
        std::fs::write(dir.join("end_info.7.done"), "filesize:33").unwrap();

        let logs = StartLogs::load(&dir).expect("the `.done` markers must be ignored");
        assert_eq!(logs.begin_us(), 1792632287145380);
        assert_eq!(logs.clock_monotonic_raw(), 5396596301416840);
        assert_eq!(logs.end_us(), 1792632310813987);
        let mono = logs
            .mono(None)
            .expect("dev_start.log/host_start.log parsed");
        assert_eq!(mono.begin_us, 1792632287145380);
        let _ = std::fs::remove_dir_all(&dir);
    }

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

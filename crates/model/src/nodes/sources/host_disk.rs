use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One host disk sample (`host_disk.data`, text).
///
/// Mirrors `HostDiskUsagePresenter`: a `start_time <ts>` line seeds the clock,
/// each following row contributes `read`/`write` (converted to KB/s) and an
/// `io%`, and consecutive samples are stamped `[start, start + interval]` where
/// `interval = 1 / disk_freq * 1e9`.
///
/// **Code-derived and unverified** — no capture with `--host-sys=disk` exists.
#[derive(Debug, Clone)]
pub struct HostDiskUsageRow {
    pub start_ns: f64,
    pub end_ns: f64,
    pub read_kb: f64,
    pub write_kb: f64,
    pub swap_in: String,
    pub io_percent: f64,
}

/// Unit → KB/s multiplier (`HostDiskUsagePresenter.TYPE_CONVERSION_DICT`).
fn unit_scale(unit: &str) -> f64 {
    match unit {
        "B/s" => 1.0 / 1024.0,
        "KB/s" => 1.0,
        "MB/s" => 1024.0,
        "GB/s" => 1024.0 * 1024.0,
        "TB/s" => 1024.0 * 1024.0 * 1024.0,
        _ => 1.0,
    }
}

impl Node for HostDiskUsageRow {
    const NAME: &'static str = "host_disk_usage";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_host_stream(ctx, "host_disk.data.", 1)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let interval = disk_interval_ns(ctx).unwrap_or(0.0);
        if interval <= 0.0 {
            return Ok(Vec::new());
        }
        let text = String::from_utf8_lossy(&bytes);
        let mut out = Vec::new();
        let mut end = 0.0f64;
        for line in text.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 2 || !fields[1].chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            if fields[0] == "start_time" {
                end = fields[1].parse().unwrap_or(0.0);
                continue;
            }
            if fields.len() < 8 {
                continue;
            }
            let read = fields[4].parse::<f64>().unwrap_or(0.0) * unit_scale(fields[5]);
            let write = fields[6].parse::<f64>().unwrap_or(0.0) * unit_scale(fields[7]);
            let (swap_in, io_percent) = if fields.len() < 11 {
                ("0.00".to_string(), 0.0)
            } else {
                (fields[8].to_string(), fields[10].parse().unwrap_or(0.0))
            };
            let clock = end;
            end = clock + interval;
            out.push(HostDiskUsageRow {
                start_ns: clock,
                end_ns: end,
                read_kb: read,
                write_kb: write,
                swap_in,
                io_percent,
            });
        }
        Ok(out)
    }
}

/// `sample.json: host_disk_freq` — samples per second.
fn disk_interval_ns(ctx: &Ctx) -> Option<f64> {
    let freq = ctx
        .require::<crate::ctx::SampleConfig>()
        .ok()?
        .f64("host_disk_freq")?;
    if freq > 0.0 { Some(1e9 / freq) } else { None }
}

use std::path::Path;

use anyhow::{Context as _, Result};
use mspark_df::{Ctx, Node};

use crate::raw;

/// V4 L2-cache event selection (`msconfig/l2_cache_config.py`).
const REQUEST_EVENTS: [&str; 2] = ["0xfb", "0xfc"];
const VICTIM_EVENTS: [&str; 1] = ["0x9c"];

#[derive(Debug, Clone, Copy)]
pub struct L2CacheRow {
    pub stream_id: u16,
    pub task_id: u16,
    /// `Hit Rate` is reported as N/A on stars chips (V4).
    pub victim_rate: f64,
}

impl Node for L2CacheRow {
    const NAME: &'static str = "l2_cache";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "l2_cache.", raw::L2_CACHE_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let events = load_l2_events(ctx.device_dir()?)?;
        let index = |name: &str| events.iter().position(|event| event == name);
        let request: Vec<usize> = REQUEST_EVENTS.iter().filter_map(|e| index(e)).collect();
        let victim: Vec<usize> = VICTIM_EVENTS.iter().filter_map(|e| index(e)).collect();
        let mut out = Vec::new();
        for record in raw::decode_l2_cache(&bytes)? {
            let request_value: u64 = request
                .iter()
                .filter(|&&i| i < record.events.len())
                .map(|&i| record.events[i])
                .sum();
            let victim_value: u64 = victim
                .iter()
                .filter(|&&i| i < record.events.len())
                .map(|&i| record.events[i])
                .sum();
            let victim_rate = if request_value == 0 {
                0.0
            } else {
                round6(victim_value as f64 / request_value as f64)
            };
            out.push(L2CacheRow {
                stream_id: record.stream_id,
                task_id: record.task_id,
                victim_rate,
            });
        }
        Ok(out)
    }
}

fn round6(value: f64) -> f64 {
    // Python round(x, 6) is round-half-even; Rust {:.6} matches it.
    format!("{value:.6}").parse().unwrap_or(value)
}

fn load_l2_events(device_dir: &Path) -> Result<Vec<String>> {
    let path = device_dir.join("sample.json");
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&text)?;
    let raw = value
        .get("l2CacheTaskProfilingEvents")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    Ok(raw
        .split(',')
        .map(|event| event.trim().to_lowercase())
        .filter(|event| !event.is_empty())
        .collect())
}

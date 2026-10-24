use crate::format::round6;
use anyhow::Result;
use mspark_df::{Ctx, Node};

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
        let bytes = crate::stream::read_stream(ctx, "l2_cache.", super::l2_cache::L2_CACHE_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let events = load_l2_events(ctx)?;
        let index = |name: &str| events.iter().position(|event| event == name);
        let request: Vec<usize> = REQUEST_EVENTS.iter().filter_map(|e| index(e)).collect();
        let victim: Vec<usize> = VICTIM_EVENTS.iter().filter_map(|e| index(e)).collect();
        let mut out = Vec::new();
        for record in super::l2_cache::decode_l2_cache(&bytes)? {
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

fn load_l2_events(ctx: &Ctx) -> Result<Vec<String>> {
    Ok(ctx
        .require::<crate::ctx::SampleConfig>()?
        .list("l2CacheTaskProfilingEvents"))
}

pub const L2_CACHE_TRUNK: usize = 72;

#[derive(Debug, Clone, Copy)]
pub struct L2CacheRaw {
    pub stream_id: u16,
    pub task_id: u16,
    pub events: [u64; 8],
}

pub fn decode_l2_cache(bytes: &[u8]) -> Result<Vec<L2CacheRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<L2_CACHE_TRUNK>().0 {
        let h = |o: usize| u16::from_le_bytes([chunk[o], chunk[o + 1]]);
        let q = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        out.push(L2CacheRaw {
            stream_id: h(2),
            task_id: h(4),
            events: core::array::from_fn(|i| q(8 + i * 8)),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// TS CPU perf records (`L L 20Q L L Q Q 14Q`, 304 bytes, header 0xabababab)
// ---------------------------------------------------------------------------

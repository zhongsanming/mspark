use crate::format::round6;
use anyhow::Result;
use mspark_df::{Ctx, Node};

/// Host memory utilisation sample (`host_mem.data`, text).
///
/// Mirrors `HostMemUsagePresenter._parse_mem_data`:
/// `usage = Decimal(field[2]) * 4 / mem_total * 100`, emitted as the pair
/// `[previous_ts, current_ts, usage]` (the first line only seeds `previous_ts`).
///
/// **Code-derived and unverified**: no capture with `--host-sys=mem` has been
/// seen yet, so this node is faithful to the reference implementation but has
/// never produced a row.
#[derive(Debug, Clone)]
pub struct HostMemUsageRow {
    pub start_ns: u64,
    pub end_ns: u64,
    pub usage: f64,
}

impl Node for HostMemUsageRow {
    const NAME: &'static str = "host_mem_usage";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let mem_total = host_mem_total(ctx).unwrap_or(0);
        if mem_total == 0 {
            return Ok(Vec::new());
        }
        let bytes = crate::stream::read_host_stream(ctx, "host_mem.data.", 1)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let text = String::from_utf8_lossy(&bytes);
        let mut out = Vec::new();
        let mut last: Option<u64> = None;
        for line in text.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 3 {
                continue;
            }
            let Ok(ts) = fields[0].parse::<u64>() else {
                continue;
            };
            let Ok(value) = fields[2].parse::<f64>() else {
                continue;
            };
            let usage = round6(value * 4.0 / mem_total as f64 * 100.0);
            if let Some(prev) = last {
                out.push(HostMemUsageRow {
                    start_ns: prev,
                    end_ns: ts,
                    usage,
                });
            }
            last = Some(ts);
        }
        Ok(out)
    }
}

/// `info.json: memoryTotal` (KB), used as the denominator.
fn host_mem_total(ctx: &Ctx) -> Option<u64> {
    let total = ctx.require::<crate::ctx::HostInfo>().ok()?.memory_total_kb;
    (total > 0).then_some(total)
}

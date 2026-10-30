use crate::format::round6;
use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One per-CPU (or `Avg`) usage sample between two host snapshots.
#[derive(Debug, Clone)]
pub struct HostCpuUsageRow {
    /// Start of the interval (raw ns from the `time` header).
    pub start_ns: u64,
    pub end_ns: u64,
    /// `"<n>"` or `"Avg"`.
    pub cpu_no: String,
    pub usage: f64,
}

/// One snapshot: its `time` stamp, the uptime in jiffies, and its processes.
type Block = (u64, i64, Vec<(i64, ProcTimes)>);

/// `/proc/<pid>/stat` fields, by the indices msprof uses (`line.split()`).
#[derive(Clone, Copy)]
struct ProcTimes {
    utime: i64,
    stime: i64,
    cpu_no: i64,
}

impl Node for HostCpuUsageRow {
    const NAME: &'static str = "host_cpu_usage";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        // msprof parses each `slice_*` file independently, so no interval spans
        // a slice boundary (the first block of a slice has no predecessor).
        let dir = ctx.host_data_dir();
        // `files_with_prefix` already returns slice order.
        let files = crate::stream::files_with_prefix(&dir, &["host_cpu.data"]);
        let mut out = Vec::new();
        for file in files {
            let bytes = crate::errors::read(&file)?;
            if bytes.is_empty() {
                continue;
            }
            let text = String::from_utf8_lossy(&bytes);
            out.extend(Self::parse_slice(&text));
        }
        Ok(out)
    }
}

impl HostCpuUsageRow {
    fn parse_slice(text: &str) -> Vec<HostCpuUsageRow> {
        // Split into blocks and remember the CPU count for the average.
        let mut blocks: Vec<Block> = Vec::new();
        let mut cur_ts = 0u64;
        let mut cur_jiffies = 0i64;
        let mut cur: Vec<(i64, ProcTimes)> = Vec::new();
        let mut expect_uptime = false;
        let mut cpu_num = 1i64;

        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("time ") {
                blocks.push((cur_ts, cur_jiffies, std::mem::take(&mut cur)));
                cur_ts = rest.trim().parse().unwrap_or(0);
                expect_uptime = true;
                continue;
            }
            if expect_uptime {
                // `Decimal(uptime) * clk_jiffies`; uptime has 2 decimals, clk is 100.
                let uptime = line.split_whitespace().next().unwrap_or("0");
                let hundredths = parse_hundredths(uptime);
                cur_jiffies = hundredths; // Decimal(uptime) * 100
                expect_uptime = false;
                continue;
            }
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() <= 38 {
                continue;
            }
            let (Ok(pid), Ok(utime), Ok(stime), Ok(cpu)) = (
                f[0].parse::<i64>(),
                f[13].parse::<i64>(),
                f[14].parse::<i64>(),
                f[38].parse::<i64>(),
            ) else {
                continue;
            };
            cpu_num = cpu_num.max(cpu + 1);
            cur.push((
                pid,
                ProcTimes {
                    utime,
                    stime,
                    cpu_no: cpu,
                },
            ));
        }
        blocks.push((cur_ts, cur_jiffies, cur));

        // Interval deltas, mirroring `_process_per_usage`.
        let mut out = Vec::new();
        for pair in blocks.windows(2) {
            let (start_ns, last_jiffies, last_data) = &pair[0];
            let (end_ns, curr_jiffies, curr_data) = &pair[1];
            if *start_ns == 0 {
                continue;
            }
            let delta = curr_jiffies - last_jiffies;
            if delta <= 0 {
                continue;
            }
            // O(procs) instead of the previous O(procs^2): index the previous
            // block by pid and accumulate per CPU in a map keyed by CPU number.
            let previous: std::collections::HashMap<i64, &ProcTimes> =
                last_data.iter().map(|(pid, times)| (*pid, times)).collect();
            let mut per_cpu: std::collections::BTreeMap<i64, i64> =
                std::collections::BTreeMap::new();
            let mut total = 0i64;
            for (pid, cur_times) in curr_data {
                let Some(prev) = previous.get(pid) else {
                    continue;
                };
                let raw = (cur_times.utime - prev.utime) + (cur_times.stime - prev.stime);
                let jiffies = raw.min(delta);
                *per_cpu.entry(cur_times.cpu_no).or_insert(0) += jiffies;
                total += jiffies;
            }
            for (cpu, jiffies) in per_cpu {
                let usage = round6(jiffies as f64 * 100.0 / delta as f64);
                if usage > 100.0 {
                    continue;
                }
                out.push(HostCpuUsageRow {
                    start_ns: *start_ns,
                    end_ns: *end_ns,
                    cpu_no: cpu.to_string(),
                    usage,
                });
            }
            let avg = round6(total as f64 * 100.0 / (delta as f64 * cpu_num as f64));
            out.push(HostCpuUsageRow {
                start_ns: *start_ns,
                end_ns: *end_ns,
                cpu_no: "Avg".to_string(),
                usage: avg,
            });
        }
        out
    }
}

/// Parse `"5396615.64"` into hundredths (`539661564`).
fn parse_hundredths(text: &str) -> i64 {
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    let mut frac = frac.to_string();
    frac.truncate(2);
    while frac.len() < 2 {
        frac.push('0');
    }
    let int: i64 = int.parse().unwrap_or(0);
    let frac: i64 = frac.parse().unwrap_or(0);
    int * 100 + frac
}

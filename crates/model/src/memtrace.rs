//! Opt-in memory tracing (`MSPARK_MEM_TRACE=1`).
//!
//! Peak RSS alone does not say *which* phase allocated it, and the exporters go
//! through several large intermediate tables. Every call prints the current and
//! peak resident set with a label, so a run can be profiled without a heap
//! profiler.

/// Print `label` with the current and peak RSS, if `MSPARK_MEM_TRACE` is set.
pub fn trace(label: &str) {
    if std::env::var_os("MSPARK_MEM_TRACE").is_none() {
        return;
    }
    let (current, peak) = rss_kb().unwrap_or((0, 0));
    eprintln!("mem {current:>8} kB peak {peak:>8} kB  {label}");
}

fn rss_kb() -> Option<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut current = None;
    let mut peak = None;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            current = value.split_whitespace().next()?.parse().ok();
        } else if let Some(value) = line.strip_prefix("VmHWM:") {
            peak = value.split_whitespace().next()?.parse().ok();
        }
    }
    Some((current.unwrap_or(0), peak.unwrap_or(0)))
}

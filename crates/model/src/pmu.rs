//! PMU metric helpers: event tables and the per-chip formulas.

use crate::raw;

pub(crate) const AIC_EVENTS: [u32; 8] = [0x416, 0x417, 0x9, 0x302, 0xc, 0x303, 0x54, 0x55];
pub(crate) const AIV_EVENTS: [u32; 8] = [0x8, 0xa, 0x9, 0xb, 0xc, 0xd, 0x54, 0x55];

pub(crate) fn get_freq(samples: &[raw::LpmSample], base_hz: f64, end: u64) -> f64 {
    if samples.iter().any(|s| s.value == 0) {
        return base_hz;
    }
    let mut freq = base_hz;
    for sample in samples {
        if end < sample.sys_cnt {
            break;
        }
        freq = sample.value as f64 * 1_000_000.0;
    }
    freq
}

pub(crate) fn pmu_ratio(total: u64, pmu_list: &[u64; 8], events: &[u32; 8], regs: &[u32]) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let mut sum = 0u64;
    for reg in regs {
        if let Some(pos) = events.iter().position(|event| event == reg) {
            sum += pmu_list[pos];
        }
    }
    sum as f64 / total as f64
}

pub(crate) fn pmu_div(
    total: u64,
    pmu_list: &[u64; 8],
    events: &[u32; 8],
    num: u32,
    den: u32,
) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let Some(n) = events.iter().position(|event| *event == num) else {
        return 0.0;
    };
    let Some(d) = events.iter().position(|event| *event == den) else {
        return 0.0;
    };
    if pmu_list[d] == 0 {
        0.0
    } else {
        pmu_list[n] as f64 / pmu_list[d] as f64
    }
}

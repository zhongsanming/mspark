use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::stream::NPU_MODULE_MEM_TRUNK;
use crate::stream::{CtxStreams, Stream};
use crate::time::DeviceStartLogs;

#[derive(Debug, Clone, Copy)]
pub struct NpuModuleMemRow {
    pub module_id: u32,
    pub ts_ns: u64,
    pub total_size: u64,
}

impl Node for NpuModuleMemRow {
    const NAME: &'static str = "npu_module_mem";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = ctx.stream(Stream::NpuModuleMem)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let info = ctx.require::<DeviceInfo>()?;
        let clock = ctx
            .require::<DeviceStartLogs>()?
            .0
            .host_clock(info.host_freq_mhz)?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in super::npu_module_mem::decode_npu_module_mem(&bytes)? {
            let ts_ns = clock.to_ns(record.syscnt);
            if (ts_ns as i128) < begin_ns {
                continue;
            }
            out.push(NpuModuleMemRow {
                module_id: record.module_id,
                ts_ns,
                total_size: record.total_size,
            });
        }
        out.sort_by_key(|row| (row.module_id, row.ts_ns));
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NpuModuleMemRaw {
    pub module_id: u32,
    pub syscnt: u64,
    pub total_size: u64,
}

pub fn decode_npu_module_mem(bytes: &[u8]) -> Result<Vec<NpuModuleMemRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<NPU_MODULE_MEM_TRUNK>().0 {
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        let u64_at = |o: usize| {
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
        out.push(NpuModuleMemRaw {
            module_id: u32_at(0),
            syscnt: u64_at(8),
            total_size: u64_at(16),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// L2 cache (`<HHHH8Q`, 72-byte records)
// ---------------------------------------------------------------------------

/// Module id → component name, as emitted by msprof's `npu_module_mem` export.
pub fn module_name(id: u32) -> &'static str {
    match id {
        0 => "SLOG",
        1 => "IDEDD",
        2 => "SCC",
        3 => "HCCL",
        4 => "FMK",
        5 => "CCU",
        6 => "DVPP",
        7 => "RUNTIME",
        8 => "CCE",
        9 => "HDC",
        10 => "DRV",
        11 => "NET",
        22 => "DEVMM",
        23 => "KERNEL",
        24 => "LIBMEDIA",
        25 => "CCECPU",
        27 => "ROS",
        28 => "HCCP",
        29 => "ROCE",
        30 => "TEFUSION",
        31 => "PROFILING",
        32 => "DP",
        33 => "APP",
        34 => "TS",
        35 => "TSDUMP",
        36 => "AICPU",
        37 => "LP",
        38 => "TDT",
        39 => "FE",
        40 => "MD",
        41 => "MB",
        42 => "ME",
        43 => "IMU",
        44 => "IMP",
        45 => "GE",
        47 => "CAMERA",
        48 => "ASCENDCL",
        49 => "TEEOS",
        50 => "ISP",
        51 => "SIS",
        52 => "HSM",
        53 => "DSS",
        54 => "PROCMGR",
        55 => "BBOX",
        56 => "AIVECTOR",
        57 => "TBE",
        58 => "FV",
        60 => "TUNE",
        61 => "HSS",
        62 => "FFTS",
        63 => "OP",
        64 => "UDF",
        65 => "HICAID",
        66 => "TSYNC",
        67 => "AUDIO",
        68 => "TPRT",
        69 => "ASCENDCKERNEL",
        70 => "ASYS",
        71 => "ATRACE",
        72 => "RTC",
        73 => "SYSMONITOR",
        74 => "AMP",
        75 => "ADETECT",
        76 => "MBUFF",
        _ => "Unknown",
    }
}

use crate::stream::TSCPU_TRUNK;
use crate::stream::{CtxStreams, Stream};
use anyhow::Result;
use mspark_df::{Ctx, Node};

#[derive(Debug, Clone, Copy)]
pub struct TscpuRecord(pub super::tscpu::TscpuRaw);

impl Node for TscpuRecord {
    const NAME: &'static str = "tscpu";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = ctx.stream(Stream::Tscpu)?;
        Ok(super::tscpu::decode_tscpu(&bytes)?
            .into_iter()
            .map(TscpuRecord)
            .collect())
    }
}

pub const TSCPU_HEADER: u32 = 0xabab_abab;

#[derive(Debug, Clone, Copy)]
pub struct TscpuRaw {
    pub pc: u64,
    pub events: [u64; 7],
    pub counts: [u64; 7],
}

pub fn decode_tscpu(bytes: &[u8]) -> Result<Vec<TscpuRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<TSCPU_TRUNK>().0 {
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        if u32_at(0) != TSCPU_HEADER {
            continue;
        }
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
        let pc = u64_at(176);
        let pmu: [u64; 14] = core::array::from_fn(|i| u64_at(192 + i * 8));
        let events: [u64; 7] = core::array::from_fn(|i| pmu[i * 2]);
        if events.iter().any(|&event| event > 0xec) {
            continue;
        }
        let counts: [u64; 7] = core::array::from_fn(|i| pmu[i * 2 + 1]);
        out.push(TscpuRaw { pc, events, counts });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// QoS bandwidth (`QosRawRecord`, 64 bytes)
// ---------------------------------------------------------------------------

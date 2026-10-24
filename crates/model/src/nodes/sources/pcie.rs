use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One `pcie.data` record (`Q22I` = 96 bytes):
/// `timestamp, then the 21 per-window PCIe measurements`.
///
/// Field layout after the timestamp:
/// `2..=4` tx non-posted min/max/avg, `5..=7` tx completion, `8..=10` tx posted
/// (the parser overwrites index 1 with the device id, which we don't need).
#[derive(Debug, Clone, Copy)]
pub struct PcieRecord {
    pub ts: u64,
    /// Absolute wall time (ns).
    pub ts_ns: i128,
    pub fields: [u32; 22],
}

impl Node for PcieRecord {
    const NAME: &'static str = "pcie";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(ctx, "pcie.", super::pcie::PCIE_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = crate::time::load_mono_clock(ctx.device_dir()?, 0.0)?;
        Ok(bytes
            .as_chunks::<{ super::pcie::PCIE_TRUNK }>()
            .0
            .iter()
            .map(|chunk| {
                let ts = u64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ]);
                PcieRecord {
                    ts,
                    ts_ns: clock.to_ns(ts),
                    fields: core::array::from_fn(|i| {
                        let o = 8 + i * 4;
                        u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]])
                    }),
                }
            })
            .collect())
    }
}

pub const PCIE_TRUNK: usize = 96;

// ---------------------------------------------------------------------------
// LLC (`LlcRecord` v1, `QQII`, 24 bytes)
// ---------------------------------------------------------------------------

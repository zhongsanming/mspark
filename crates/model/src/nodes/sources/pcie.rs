use crate::stream::PCIE_TRUNK;
use crate::stream::{CtxStreams, Stream};
use crate::time::DeviceStartLogs;
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
        let bytes = ctx.stream(Stream::Pcie)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = ctx.require::<DeviceStartLogs>()?.0.mono(None)?;
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

// ---------------------------------------------------------------------------
// LLC (`LlcRecord` v1, `QQII`, 24 bytes)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::sources::testing::FakeDevice;

    fn record(ts: u64, fields: [u32; 22]) -> [u8; 96] {
        let mut bytes = [0u8; 96];
        bytes[0..8].copy_from_slice(&ts.to_le_bytes());
        for (i, field) in fields.iter().enumerate() {
            let offset = 8 + i * 4;
            bytes[offset..offset + 4].copy_from_slice(&field.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn decodes_the_timestamp_and_all_22_fields() {
        let device = FakeDevice::new();
        let fields: [u32; 22] = core::array::from_fn(|i| i as u32 + 1);
        device.write("pcie.slice_0", &record(500, fields));
        let ctx = device.ctx();
        let rows = PcieRecord::compute(&ctx, ()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, 500);
        assert_eq!(rows[0].fields, fields);
        let clock = device
            .ctx()
            .require::<DeviceStartLogs>()
            .unwrap()
            .0
            .mono(None)
            .unwrap();
        assert_eq!(rows[0].ts_ns, clock.to_ns(500));
    }
}

use crate::stream::{CtxStreams, HBM_TRUNK, Stream};
use crate::time::DeviceStartLogs;
use anyhow::Result;
use mspark_df::{Ctx, Node};

/// Raw `hbm.data` sample (v1 layout, `QQII` = 24 bytes).
#[derive(Debug, Clone, Copy)]
pub struct HbmRecord {
    pub ts_us: u64,
    /// Absolute wall time (ns).
    pub ts_ns: i128,
    pub count: u64,
    pub event: u32,
    pub hbm_id: u32,
}

impl Node for HbmRecord {
    const NAME: &'static str = "hbm";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        // Layout version comes from `info.json:version` (msprof's
        // `VersionedStructParser`): 2.x stores the stamp last and in absolute ns.
        // `Stream::Hbm` picks the matching record size.
        let bytes = ctx.stream(Stream::Hbm)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = ctx.require::<DeviceStartLogs>()?.0.sampling()?;
        let mut out = Vec::new();
        for chunk in bytes.as_chunks::<{ super::hbm::HBM_TRUNK }>().0 {
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
            let u32_at =
                |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
            out.push(HbmRecord {
                ts_us: u64_at(0),
                ts_ns: clock.to_ns(u64_at(0)),
                count: u64_at(8),
                event: u32_at(16),
                hbm_id: u32_at(20),
            });
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// PCIe (`PcieRecord`, `Q22I`, 96 bytes)
// ---------------------------------------------------------------------------

// Collection-version 2.x layouts of the versioned streams.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::sources::testing::FakeDevice;

    fn v1(ts_us: u64, count: u64, event: u32, hbm_id: u32) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[0..8].copy_from_slice(&ts_us.to_le_bytes());
        bytes[8..16].copy_from_slice(&count.to_le_bytes());
        bytes[16..20].copy_from_slice(&event.to_le_bytes());
        bytes[20..24].copy_from_slice(&hbm_id.to_le_bytes());
        bytes
    }

    #[test]
    fn decodes_v1_records_and_converts_the_sampling_stamp() {
        let device = FakeDevice::new();
        device.write(
            "hbm.slice_0",
            &[v1(100, 42, 1, 3), v1(200, 84, 0, 2)].concat(),
        );
        let ctx = device.ctx();
        let rows = HbmRecord::compute(&ctx, ()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0].ts_us, rows[0].count, rows[0].event, rows[0].hbm_id),
            (100, 42, 1, 3)
        );
        assert_eq!((rows[1].ts_us, rows[1].count), (200, 84));
        let clock = device
            .ctx()
            .require::<DeviceStartLogs>()
            .unwrap()
            .0
            .sampling()
            .unwrap();
        assert_eq!(rows[0].ts_ns, clock.to_ns(100));
        assert_eq!(rows[1].ts_ns, clock.to_ns(200));
    }

    #[test]
    fn missing_stream_decodes_to_an_empty_table() {
        let device = FakeDevice::new();
        assert!(HbmRecord::compute(&device.ctx(), ()).unwrap().is_empty());
    }
}

use crate::stream::LLC_TRUNK;
use crate::stream::{CtxStreams, Stream};
use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One `llc.data` record (v1 `QQII` = 24 bytes): sampling stamp (µs), count,
/// event id, l3t id.
#[derive(Debug, Clone, Copy)]
pub struct LlcRecord {
    pub ts_us: u64,
    pub count: u64,
    pub event: u32,
    pub l3t_id: u32,
}

impl Node for LlcRecord {
    const NAME: &'static str = "llc";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        // v1: ts_us, count, event, l3t | v2: reserved, count, event, l3t, ts_ns.
        let version = ctx
            .require::<crate::ctx::DeviceInfo>()?
            .collection_version
            .clone();
        let v2 = version.starts_with('2');
        let bytes = ctx.stream(Stream::Llc)?;
        let mut out = Vec::new();
        for chunk in bytes.as_chunks::<{ super::llc::LLC_TRUNK }>().0 {
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
            out.push(if v2 {
                LlcRecord {
                    ts_us: u64_at(16),
                    // `IIIIQ`: reserved, count, event, l3t_id, absolute-ns stamp.
                    // The count is a `u32` here, not the `u64` used by v1.
                    count: u32_at(4) as u64,
                    event: u32_at(8),
                    l3t_id: u32_at(12),
                }
            } else {
                LlcRecord {
                    ts_us: u64_at(0),
                    count: u64_at(8),
                    event: u32_at(16),
                    l3t_id: u32_at(20),
                }
            });
        }
        Ok(out)
    }
}

/// Read-mode event ids in slot order (write mode uses a different table).
pub const LLC_READ_SLOTS: [u32; 8] = [0x00, 0x01, 0x02, 0x13, 0x20, 0x22, 0x34, 0x36];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::sources::testing::{FakeDevice, device_info};

    fn v1(ts_us: u64, count: u64, event: u32, l3t_id: u32) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[0..8].copy_from_slice(&ts_us.to_le_bytes());
        bytes[8..16].copy_from_slice(&count.to_le_bytes());
        bytes[16..20].copy_from_slice(&event.to_le_bytes());
        bytes[20..24].copy_from_slice(&l3t_id.to_le_bytes());
        bytes
    }

    /// v2: reserved, count, event, l3t_id, absolute-ns stamp.
    fn v2(ts_ns: u64, count: u32, event: u32, l3t_id: u32) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[4..8].copy_from_slice(&count.to_le_bytes());
        bytes[8..12].copy_from_slice(&event.to_le_bytes());
        bytes[12..16].copy_from_slice(&l3t_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&ts_ns.to_le_bytes());
        bytes
    }

    #[test]
    fn decodes_the_v1_layout() {
        let device = FakeDevice::new();
        device.write("llc.slice_0", &v1(1234, 99, 0x20, 2));
        let mut ctx = device.ctx();
        ctx.insert(device_info("1.0"));
        let rows = LlcRecord::compute(&ctx, ()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].ts_us, rows[0].count, rows[0].event, rows[0].l3t_id),
            (1234, 99, 0x20, 2)
        );
    }

    #[test]
    fn decodes_the_v2_layout_when_info_says_version_2() {
        let device = FakeDevice::new();
        device.write("llc.slice_0", &v2(5555, 7, 0x36, 1));
        let mut ctx = device.ctx();
        ctx.insert(device_info("2.0"));
        let rows = LlcRecord::compute(&ctx, ()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].ts_us, rows[0].count, rows[0].event, rows[0].l3t_id),
            (5555, 7, 0x36, 1)
        );
    }
}

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

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
        let bytes = raw::read_stream(ctx, "llc.", raw::LLC_TRUNK)?;
        let mut out = Vec::new();
        for chunk in bytes.as_chunks::<{ raw::LLC_TRUNK }>().0 {
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
                    count: u64_at(4),
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

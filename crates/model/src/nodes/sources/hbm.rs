use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

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
        let version = ctx
            .require::<crate::ctx::DeviceInfo>()?
            .collection_version
            .clone();
        let v2 = version.starts_with('2');
        let trunk = if v2 {
            raw::HBM_TRUNK_V2
        } else {
            raw::HBM_TRUNK
        };
        let bytes = raw::read_stream(ctx, "hbm.", trunk)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = crate::time::load_sampling_clock(ctx.device_dir()?)?;
        let mut out = Vec::new();
        for chunk in bytes.as_chunks::<{ raw::HBM_TRUNK }>().0 {
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

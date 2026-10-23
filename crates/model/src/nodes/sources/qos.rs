use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::{Clock, fmt_us};

#[derive(Debug, Clone)]
pub struct QosRow {
    pub ts_us: compact_str::CompactString,
    pub values: [u32; 10],
}

impl Node for QosRow {
    const NAME: &'static str = "qos";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(ctx, "qos.", super::qos::QOS_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out = Vec::new();
        for record in super::qos::decode_qos(&bytes)? {
            let ts_ns = clock.to_ns(record.syscnt);
            if (ts_ns as i128) < begin_ns {
                continue;
            }
            out.push(QosRow {
                ts_us: fmt_us(ts_ns, false),
                values: record.metrics,
            });
        }
        Ok(out)
    }
}

pub const QOS_TRUNK: usize = 64;

#[derive(Debug, Clone, Copy)]
pub struct QosRaw {
    pub syscnt: u64,
    pub metrics: [u32; 10],
}

pub fn decode_qos(bytes: &[u8]) -> Result<Vec<QosRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<QOS_TRUNK>().0 {
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
        out.push(QosRaw {
            syscnt: u64_at(16),
            metrics: core::array::from_fn(|i| u32_at(24 + i * 4)),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// HBM bandwidth (`HbmRecord` v1, `QQII`, 24 bytes)
// ---------------------------------------------------------------------------

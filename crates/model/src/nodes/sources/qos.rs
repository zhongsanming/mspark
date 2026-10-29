use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::stream::QOS_TRUNK;
use crate::stream::{CtxStreams, Stream};
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
        let bytes = ctx.stream(Stream::Qos)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(syscnt: u64, metrics: [u32; 10]) -> [u8; QOS_TRUNK] {
        let mut bytes = [0u8; QOS_TRUNK];
        bytes[16..24].copy_from_slice(&syscnt.to_le_bytes());
        for (i, metric) in metrics.iter().enumerate() {
            let offset = 24 + i * 4;
            bytes[offset..offset + 4].copy_from_slice(&metric.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn decodes_syscnt_and_all_ten_metrics() {
        let bytes = record(
            0x1234_5678_9abc_def0,
            core::array::from_fn(|i| i as u32 + 1),
        );
        let rows = decode_qos(&bytes).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].syscnt, 0x1234_5678_9abc_def0);
        assert_eq!(rows[0].metrics, core::array::from_fn(|i| i as u32 + 1));
    }

    #[test]
    fn drops_a_trailing_partial_record() {
        let mut bytes = record(1, [0; 10]).to_vec();
        bytes.extend_from_slice(&[0xff; QOS_TRUNK - 1]);
        let rows = decode_qos(&bytes).unwrap();
        assert_eq!(rows.len(), 1, "a partial trailing record must be ignored");
        assert_eq!(rows[0].syscnt, 1);
    }

    #[test]
    fn empty_stream_decodes_to_no_rows() {
        assert!(decode_qos(&[]).unwrap().is_empty());
    }
}

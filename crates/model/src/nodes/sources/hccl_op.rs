use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One HCCL op record (`aging.compact.hccl_op_info`, 64 B compact record).
///
/// `MsprofHcclOPInfo` at data+0: `relay:1|retry:1` (byte 0), `dataType` (byte 1),
/// `algType` (u64), `count` (u64), `groupName` (u64) — offsets verified against
/// `COMMUNICATION_OP`.
#[derive(Debug, Clone, Copy)]
pub struct HcclOpRow {
    /// Op start stamp (raw ns, from the compact header).
    pub ts_ns: u64,
    pub relay: bool,
    pub retry: bool,
    pub data_type: u8,
    pub alg_type: u64,
    pub count: u64,
    pub group_hash: u64,
}

impl Node for HcclOpRow {
    const NAME: &'static str = "hccl_op";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_host_stream(ctx, "aging.compact.hccl_op_info.", 1)?;
        let mut out = Vec::new();
        for chunk in bytes.as_chunks::<{ crate::stream::COMPACT_TRUNK }>().0 {
            if u16::from_le_bytes([chunk[0], chunk[1]]) != crate::stream::COMPACT_MAGIC {
                continue;
            }
            let d = &chunk[24..];
            let u64_at = |o: usize| {
                u64::from_le_bytes([
                    d[o],
                    d[o + 1],
                    d[o + 2],
                    d[o + 3],
                    d[o + 4],
                    d[o + 5],
                    d[o + 6],
                    d[o + 7],
                ])
            };
            out.push(HcclOpRow {
                ts_ns: u64::from_le_bytes([
                    chunk[16], chunk[17], chunk[18], chunk[19], chunk[20], chunk[21], chunk[22],
                    chunk[23],
                ]),
                relay: d[0] & 0x1 != 0,
                retry: d[0] & 0x2 != 0,
                data_type: d[1],
                alg_type: u64_at(2),
                count: u64_at(10),
                group_hash: u64_at(18),
            });
        }
        Ok(out)
    }
}

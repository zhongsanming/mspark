use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{F64, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

use crate::host::HashDic;

/// One HCCL task-info record (`unaging.additional.hccl_info`, 256 B, `dlen 120`).
///
/// Field layout (`MsprofHcclInfo`, verified against `COMMUNICATION_TASK_INFO`):
/// `itemId, cclTag, groupName(u64)`, then `localRank, remoteRank, rankSize,
/// workFlowMode, planeID, ctxID (u32)`, `notifyID(u64)`, `stage, role (u32)`,
/// `durationEstimated(f64)`, `srcAddr, dstAddr, dataSize(u64)`,
/// `opType, dataType, linkType (u32)`.
#[derive(Debug, Clone)]
pub struct HcclInfoRow {
    /// Record timestamp (raw ns).
    pub ts_ns: u64,
    /// `cclTag` — the op *type* hash (only three distinct values in practice).
    pub ccl_tag: u64,
    pub group: String,
    pub local_rank: u32,
    pub remote_rank: u32,
    pub rank_size: u32,
    pub plane_id: u32,
    pub notify_id: u64,
    pub size: u64,
    pub op_type: u32,
    pub data_type: u32,
    pub link_type: u32,
    pub duration_est: f64,
}

impl Node for HcclInfoRow {
    const NAME: &'static str = "hccl_info";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_host_stream(
            ctx,
            "unaging.additional.hccl_info.",
            crate::stream::ADDITIONAL_TRUNK,
        )?;
        let dic = ctx.require::<HashDic>()?;
        Ok(super::hccl_info::decode_hccl_info(&bytes)?
            .into_iter()
            .map(|r| HcclInfoRow {
                ts_ns: r.timestamp,
                ccl_tag: r.ccl_tag,
                group: dic.get(r.group_name).unwrap_or("N/A").to_string(),
                local_rank: r.local_rank,
                remote_rank: r.remote_rank,
                rank_size: r.rank_size,
                plane_id: r.plane_id,
                notify_id: r.notify_id,
                size: r.data_size,
                op_type: r.op_type,
                data_type: r.data_type,
                link_type: r.link_type,
                duration_est: r.duration_estimated,
            })
            .collect())
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct HcclInfoData {
    item_id: U64,
    ccl_tag: U64,
    group_name: U64,
    local_rank: U32,
    remote_rank: U32,
    rank_size: U32,
    work_flow_mode: U32,
    plane_id: U32,
    ctx_id: U32,
    notify_id: U64,
    stage: U32,
    role: U32,
    duration_estimated: F64,
    src_addr: U64,
    dst_addr: U64,
    data_size: U64,
    op_type: U32,
    data_type: U32,
    link_type: U32,
}

#[derive(Debug, Clone, Copy)]
pub struct HcclInfoRaw {
    pub timestamp: u64,
    pub ccl_tag: u64,
    pub group_name: u64,
    pub local_rank: u32,
    pub remote_rank: u32,
    pub rank_size: u32,
    pub plane_id: u32,
    pub notify_id: u64,
    pub duration_estimated: f64,
    pub data_size: u64,
    pub op_type: u32,
    pub data_type: u32,
    pub link_type: u32,
}

pub fn decode_hccl_info(bytes: &[u8]) -> Result<Vec<HcclInfoRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<ADDITIONAL_TRUNK>().0 {
        let head = AdditionalHead::ref_from_bytes(chunk).map_err(cast)?;
        if head.magic.get() != COMPACT_MAGIC {
            continue;
        }
        // The union is 232 bytes; the HCCL struct occupies a 120-byte prefix.
        let (data, _) = HcclInfoData::ref_from_prefix(&head.data).map_err(cast)?;
        out.push(HcclInfoRaw {
            timestamp: head.timestamp.get(),
            ccl_tag: data.ccl_tag.get(),
            group_name: data.group_name.get(),
            local_rank: data.local_rank.get(),
            remote_rank: data.remote_rank.get(),
            rank_size: data.rank_size.get(),
            plane_id: data.plane_id.get(),
            notify_id: data.notify_id.get(),
            duration_estimated: data.duration_estimated.get(),
            data_size: data.data_size.get(),
            op_type: data.op_type.get(),
            data_type: data.data_type.get(),
            link_type: data.link_type.get(),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Host API events (`MsprofApi`, 40-byte records)
// ---------------------------------------------------------------------------

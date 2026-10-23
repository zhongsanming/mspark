use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::host::HashDic;
use crate::raw;

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
        let bytes =
            raw::read_host_stream(ctx, "unaging.additional.hccl_info.", raw::ADDITIONAL_TRUNK)?;
        let dic = ctx.require::<HashDic>()?;
        Ok(raw::decode_hccl_info(&bytes)?
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

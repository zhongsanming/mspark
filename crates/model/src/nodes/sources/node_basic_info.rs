use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

use crate::host::HashDic;

#[derive(Debug, Clone)]
pub struct NodeBasicInfoRow {
    pub op_name_hash: u64,
    pub op_name: String,
    pub op_type: String,
    pub task_type: u32,
    pub block_num: u32,
}

impl Node for NodeBasicInfoRow {
    const NAME: &'static str = "node_basic_info";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_host_stream(
            ctx,
            "aging.compact.node_basic_info.",
            crate::stream::COMPACT_TRUNK,
        )?;
        let raws = super::node_basic_info::decode_node_basic_info(&bytes)?;
        let dic = ctx.require::<HashDic>()?;
        Ok(raws
            .into_iter()
            .map(|r| NodeBasicInfoRow {
                op_name_hash: r.op_name_hash,
                op_name: dic.get(r.op_name_hash).unwrap_or("N/A").to_string(),
                op_type: dic.get(r.op_type_hash).unwrap_or("N/A").to_string(),
                task_type: r.task_type,
                block_num: r.block_num,
            })
            .collect())
    }
}

/// `MsprofNodeBasicInfo` (packed: 29 bytes, padded to the 40-byte union).
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct NodeBasicInfoData {
    op_name: U64,
    task_type: U32,
    op_type: U64,
    block_num: U32,
    _op_flag: U32,
    _op_state: u8,
    _pad: [u8; 11],
}

#[derive(Debug, Clone, Copy)]
pub struct NodeBasicInfoRaw {
    pub op_name_hash: u64,
    pub op_type_hash: u64,
    pub task_type: u32,
    pub block_num: u32,
}

pub fn decode_node_basic_info(bytes: &[u8]) -> Result<Vec<NodeBasicInfoRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<COMPACT_TRUNK>().0 {
        let head = CompactHead::ref_from_bytes(chunk).map_err(cast)?;
        if head.magic.get() != COMPACT_MAGIC {
            continue;
        }
        let data = NodeBasicInfoData::ref_from_bytes(&head.data).map_err(cast)?;
        out.push(NodeBasicInfoRaw {
            op_name_hash: data.op_name.get(),
            op_type_hash: data.op_type.get(),
            task_type: data.task_type.get(),
            block_num: data.block_num.get(),
        });
    }
    Ok(out)
}

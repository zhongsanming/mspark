use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::host::HashDic;
use crate::raw;

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
        let bytes =
            raw::read_host_stream(ctx, "aging.compact.node_basic_info.", raw::COMPACT_TRUNK)?;
        let raws = raw::decode_node_basic_info(&bytes)?;
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

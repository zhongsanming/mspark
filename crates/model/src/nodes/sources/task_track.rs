use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

#[derive(Debug, Clone)]
pub struct TaskTrackRow {
    pub stream_id: u16,
    pub task_id: u32,
    pub task_type: u64,
    /// Host kernel name (preferred over the device op name when present).
    pub kernel_name: String,
}

impl Node for TaskTrackRow {
    const NAME: &'static str = "task_track";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_host_stream(ctx, "aging.compact.task_track.", raw::COMPACT_TRUNK)?;
        let dic = ctx.require::<crate::host::HashDic>()?;
        Ok(raw::decode_task_track(&bytes)?
            .into_iter()
            .map(|r| TaskTrackRow {
                stream_id: r.stream_id,
                task_id: r.task_id,
                task_type: r.task_type,
                kernel_name: dic.get(r.kernel_name).unwrap_or_default().to_string(),
            })
            .collect())
    }
}

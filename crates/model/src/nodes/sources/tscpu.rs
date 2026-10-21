use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

#[derive(Debug, Clone, Copy)]
pub struct TscpuRecord(pub raw::TscpuRaw);

impl Node for TscpuRecord {
    const NAME: &'static str = "tscpu";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "tscpu.", raw::TSCPU_TRUNK)?;
        Ok(raw::decode_tscpu(&bytes)?
            .into_iter()
            .map(TscpuRecord)
            .collect())
    }
}

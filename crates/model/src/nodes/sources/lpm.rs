use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

impl Node for raw::LpmInfo {
    const NAME: &'static str = "lpmFreqConv";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "lpmFreqConv.", raw::LPM_TRUNK)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![raw::decode_lpm(&bytes)?])
    }
}

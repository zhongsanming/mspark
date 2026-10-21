use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::raw;

impl Node for raw::StarsSocEntry {
    const NAME: &'static str = "stars_soc";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "stars_soc.", raw::STARS_SOC_TRUNK)?;
        let expand = ctx.require::<DeviceInfo>()?.expand_status;
        Ok(raw::decode_stars_soc(&bytes, expand)?.0)
    }
}

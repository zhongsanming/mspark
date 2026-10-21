use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::ctx::DeviceInfo;
use crate::raw;

impl Node for raw::PmuData {
    const NAME: &'static str = "ffts_profile";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "ffts_profile.", raw::FFTS_PROFILE_TRUNK)?;
        let expand = ctx.require::<DeviceInfo>()?.expand_status;
        Ok(raw::decode_ffts_profile(&bytes, expand)?.0)
    }
}

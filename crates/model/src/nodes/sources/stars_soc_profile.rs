use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

/// One decoded `stars_soc_profile` record (SIO, Acc PMU, Chip Trans, Inter-SOC).
#[derive(Debug, Clone, Copy)]
pub struct SocProfileRecord(pub raw::SocProfileRaw);

impl Node for SocProfileRecord {
    const NAME: &'static str = "stars_soc_profile";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "stars_soc_profile.", raw::STARS_SOC_PROFILE_TRUNK)?;
        Ok(raw::decode_stars_soc_profile(&bytes)?
            .into_iter()
            .map(SocProfileRecord)
            .collect())
    }
}

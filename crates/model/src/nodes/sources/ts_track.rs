use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;

impl Node for raw::TrackFlip {
    const NAME: &'static str = "ts_track";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = raw::read_stream(ctx, "ts_track.", raw::TS_TRACK_TRUNK)?;
        Ok(raw::decode_ts_track(&bytes)?.0)
    }
}

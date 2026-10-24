use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use crate::stream::LPM_TRUNK;
use crate::stream::{CtxStreams, Stream};
use mspark_df::{Ctx, Node};

impl Node for super::lpm::LpmInfo {
    const NAME: &'static str = "lpmFreqConv";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = ctx.stream(Stream::LpmFreq)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![super::lpm::decode_lpm(&bytes)?])
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct LpmValue {
    sys_cnt: U64,
    value: U32,
    _reserved: U32,
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct LpmInfoRaw {
    count: U32,
    kind: U32,
    values: [LpmValue; 55],
}

#[derive(Debug, Clone, Copy)]
pub struct LpmSample {
    pub sys_cnt: u64,
    pub value: u32,
}

#[derive(Debug, Clone)]
pub struct LpmInfo {
    pub count: u32,
    pub kind: u32,
    pub samples: Vec<LpmSample>,
}

/// Decode the single 888-byte `lpmFreqConv` record.
pub fn decode_lpm(bytes: &[u8]) -> Result<LpmInfo> {
    if bytes.len() < LPM_TRUNK {
        return Ok(LpmInfo {
            count: 0,
            kind: 0,
            samples: Vec::new(),
        });
    }
    let r = LpmInfoRaw::ref_from_bytes(&bytes[..LPM_TRUNK]).map_err(cast)?;
    let count = r.count.get().min(r.values.len() as u32);
    let samples = (0..count as usize)
        .map(|i| LpmSample {
            sys_cnt: r.values[i].sys_cnt.get(),
            value: r.values[i].value.get(),
        })
        .collect();
    Ok(LpmInfo {
        count: r.count.get(),
        kind: r.kind.get(),
        samples,
    })
}

// ---------------------------------------------------------------------------
// `stars_soc_profile` (64-byte records, dispatched by `funcType`)
// ---------------------------------------------------------------------------

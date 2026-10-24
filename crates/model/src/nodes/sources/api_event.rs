use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

use crate::host::{HashDic, TypeDic};
use crate::stream::API_EVENT_TRUNK;
use crate::stream::{CtxStreams, Stream};

#[derive(Debug, Clone)]
pub struct ApiEventRow {
    pub level: u16,
    /// Trace label: the resolved `type_info_dic` name.
    pub api_name: String,
    /// `api_statistic` label. msprof's grouping SQL is
    /// `CASE WHEN level = 'acl' THEN id
    ///       WHEN level IN ('runtime','model','node') THEN struct_type
    ///       ELSE item_id END`, so communication rows are named by their
    /// `item_id` hash, not by the dictionary entry (`master`/`slave`).
    pub stat_name: String,
    pub tag: String,
    pub item_name: String,
    pub thread_id: u32,
    pub begin: u64,
    pub end: u64,
}

fn acl_api_tag(id: u32) -> String {
    let name = match id {
        1 => "ACL_OP",
        2 => "ACL_MODEL",
        3 => "ACL_RTS",
        4 => "ACL_OTHERS",
        5 => "ACL_NN",
        6 => "ACL_ASCENDC",
        7 => "HOST_HCCL",
        9 => "ACL_DVPP",
        10 => "ACL_GRAPH",
        11 => "ACL_ATB",
        _ => return id.to_string(),
    };
    name.to_string()
}

impl Node for ApiEventRow {
    const NAME: &'static str = "api_event";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = ctx.stream(Stream::ApiEvent)?;
        let mut raws = super::api_event::decode_api_event(&bytes)?;
        // msprof emits the CANN track sorted by the API start timestamp.
        raws.sort_by_key(|r| r.begin);
        let types = ctx.require::<TypeDic>()?;
        let hash = ctx.require::<HashDic>()?;
        Ok(raws
            .into_iter()
            .map(|r| {
                let api_name = types.get(r.level, r.ty as u64).unwrap_or("N/A").to_string();
                let stat_name = if r.level == 5_500 {
                    hash.get(r.item_id).unwrap_or("N/A").to_string()
                } else {
                    api_name.clone()
                };
                let tag = if r.level == 20_000 {
                    acl_api_tag(r.ty >> 16)
                } else {
                    api_name.clone()
                };
                ApiEventRow {
                    level: r.level,
                    api_name,
                    stat_name,
                    tag,
                    item_name: if r.level == 10_000 {
                        hash.get(r.item_id).unwrap_or("N/A").to_string()
                    } else {
                        "0".to_string()
                    },
                    thread_id: r.thread_id,
                    begin: r.begin,
                    end: r.end,
                }
            })
            .collect())
    }
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct ApiEventData {
    magic: U16,
    level: U16,
    ty: U32,
    thread_id: U32,
    _reserve: U32,
    begin: U64,
    end: U64,
    item_id: U64,
}

#[derive(Debug, Clone, Copy)]
pub struct ApiEventRaw {
    pub level: u16,
    pub ty: u32,
    pub thread_id: u32,
    pub begin: u64,
    pub end: u64,
    pub item_id: u64,
}

pub fn decode_api_event(bytes: &[u8]) -> Result<Vec<ApiEventRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<API_EVENT_TRUNK>().0 {
        let record = ApiEventData::ref_from_bytes(chunk).map_err(cast)?;
        if record.magic.get() != COMPACT_MAGIC {
            continue;
        }
        out.push(ApiEventRaw {
            level: record.level.get(),
            ty: record.ty.get(),
            thread_id: record.thread_id.get(),
            begin: record.begin.get(),
            end: record.end.get(),
            item_id: record.item_id.get(),
        });
    }
    Ok(out)
}

pub fn api_level_name(level: u16) -> &'static str {
    match level {
        20000 => "acl",
        15000 => "model",
        10000 => "node",
        5500 => "communication",
        5000 => "runtime",
        _ => "",
    }
}

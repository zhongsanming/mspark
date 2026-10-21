use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::host::{HashDic, TypeDic};
use crate::raw;

#[derive(Debug, Clone)]
pub struct ApiEventRow {
    pub level: u16,
    pub api_name: String,
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
        let bytes = raw::read_host_stream(ctx, "aging.api_event.data.", raw::API_EVENT_TRUNK)?;
        let mut raws = raw::decode_api_event(&bytes)?;
        // msprof emits the CANN track sorted by the API start timestamp.
        raws.sort_by_key(|r| r.begin);
        let types = ctx.require::<TypeDic>()?;
        let hash = ctx.require::<HashDic>()?;
        Ok(raws
            .into_iter()
            .map(|r| {
                let api_name = types.get(r.level, r.ty as u64).unwrap_or("N/A").to_string();
                let tag = if r.level == 20_000 {
                    acl_api_tag(r.ty >> 16)
                } else {
                    api_name.clone()
                };
                ApiEventRow {
                    level: r.level,
                    api_name,
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

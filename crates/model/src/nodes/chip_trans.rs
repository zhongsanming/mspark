use std::collections::HashMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;
use crate::time::{Clock, fmt_us};

use super::sources::SocProfileRecord;

const ACC_TYPE_PA: u16 = 0b111111;
const ACC_TYPE_PCIE: u16 = 0b100010;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChipTransKind {
    Pa,
    Pcie,
}

#[derive(Debug, Clone)]
pub struct ChipTransSample {
    pub ts_us: compact_str::CompactString,
    pub kind: ChipTransKind,
    pub id: u32,
    /// PA: rx (`value1`), tx (`value2`). PCIe: write (`value1`), read (`value2`).
    pub value1: u64,
    pub value2: u64,
}

impl Node for ChipTransSample {
    const NAME: &'static str = "ChipTrans";
    type Deps = (SocProfileRecord,);

    fn compute(
        ctx: &Ctx,
        (records,): (std::sync::Arc<Vec<SocProfileRecord>>,),
    ) -> Result<Vec<Self>> {
        let clock = ctx.require::<Clock>()?;
        let begin_ns = clock.begin_us as i128 * 1000;
        let mut out: Vec<ChipTransSample> = Vec::new();
        let mut index: HashMap<(compact_str::CompactString, ChipTransKind, u32), usize> =
            HashMap::new();
        for record in records.iter() {
            if let raw::SocProfileRaw::ChipTrans {
                sys_cnt,
                acc_type,
                event_id,
                value1,
                value2,
            } = record.0
            {
                let kind = match acc_type {
                    ACC_TYPE_PA => ChipTransKind::Pa,
                    ACC_TYPE_PCIE => ChipTransKind::Pcie,
                    _ => continue,
                };
                let ts_ns = clock.to_ns(sys_cnt);
                if (ts_ns as i128) < begin_ns {
                    continue;
                }
                let ts_us = fmt_us(ts_ns, false);
                let id = event_id as u32;
                let sample = ChipTransSample {
                    ts_us: ts_us.clone(),
                    kind,
                    id,
                    value1,
                    value2,
                };
                let key = (ts_us, kind, id);
                if let Some(&i) = index.get(&key) {
                    out[i] = sample;
                } else {
                    index.insert(key, out.len());
                    out.push(sample);
                }
            }
        }
        Ok(out)
    }
}

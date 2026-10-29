//! `communication_statistic.csv`.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use mspark_model::{HcclInfoRow, HcclOpRow};

use crate::reports::write_text;
use crate::session::{devices, session_for};

/// `Broadcast_group_name_0` → `hcom_broadcast_`.
///
/// Only the first character is lowercased, matching msprof; the rest of the
/// tag prefix is kept as-is.
pub fn hcom_op_type(name: &str) -> String {
    let kind = name.split('_').next().unwrap_or("N/A");
    let mut chars = kind.chars();
    match chars.next() {
        Some(first) => format!("hcom_{}{}_", first.to_lowercase(), chars.as_str()),
        None => "hcom_unknown_".to_string(),
    }
}

/// `communication_statistic.csv`: op types and counts only.
///
/// The time columns are deliberately **not** emitted: msprof's per-op
/// `start`/`end` come from the host-produced `HCCLOpSingleDevice` table, and the
/// raw streams carry no op window (every task of an op shares the op stamp).
/// See spec/TODO.md (T22).
pub fn communication_csv(prof: &Path, only: Option<&str>, out: &Path) -> Result<()> {
    let mut body = String::from("Device_id,OP Type,Count\n");
    let mut rows = 0usize;
    for device in devices(prof, only)? {
        let session = session_for(&device)?;
        let ctx = session.ctx();
        let info = &session.info;
        let dev = info.device_id.unwrap_or(0);
        let ops = session.resolve::<HcclOpRow>()?;
        let tasks = session.resolve::<HcclInfoRow>()?;
        let dic = ctx.require::<mspark_model::HashDic>()?;

        // Tasks belong to the last op that started before them (validated: this
        // reproduces msprof's per-type op counts exactly).
        let mut stamps: Vec<u64> = ops.iter().map(|op| op.ts_ns).collect();
        stamps.sort_unstable();
        let mut per_op: Vec<Vec<u64>> = vec![Vec::new(); ops.len()];
        for task in tasks.iter() {
            let idx = stamps.partition_point(|stamp| *stamp <= task.ts_ns);
            if idx == 0 {
                continue;
            }
            // Map the sorted position back to the original op index.
            let stamp = stamps[idx - 1];
            if let Some(pos) = ops.iter().position(|op| op.ts_ns == stamp) {
                per_op[pos].push(task.ccl_tag);
            }
        }

        let mut counts: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for (idx, _op) in ops.iter().enumerate() {
            // An op with no tasks cannot be attributed to a type; skip it rather
            // than guess (recorded in spec/TODO.md).
            if per_op[idx].is_empty() {
                continue;
            }
            let mut tally: HashMap<u64, usize> = HashMap::new();
            for tag in per_op[idx].iter() {
                *tally.entry(*tag).or_insert(0) += 1;
            }
            let Some((tag, _)) = tally.into_iter().max_by_key(|(_, n)| *n) else {
                continue;
            };
            let name = dic.get(tag).unwrap_or("N/A");
            // `Broadcast_group_name_0` -> `hcom_broadcast_`
            *counts.entry(hcom_op_type(name)).or_insert(0) += 1;
        }
        for (op_type, count) in counts {
            body.push_str(&format!("{dev},{op_type},{count}\n"));
            rows += 1;
        }
    }
    write_text(out, &body)?;
    println!("wrote {rows} rows to {}", out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hcom_tag_prefix_becomes_the_op_type() {
        assert_eq!(hcom_op_type("Broadcast_group_name_0"), "hcom_broadcast_");
        assert_eq!(hcom_op_type("AllReduce_0"), "hcom_allReduce_");
        assert_eq!(hcom_op_type(""), "hcom_unknown_");
    }
}

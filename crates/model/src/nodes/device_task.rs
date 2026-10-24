use std::collections::BTreeMap;

use anyhow::Result;
use mspark_df::{Ctx, Node};

use super::sources::stars_soc::{Acsq, StarsSocEntry};
use crate::ctx::DeviceInfo;

// ---------------------------------------------------------------------------
// DeviceTask: start/end pairing, per stream
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogKind {
    Acsq,
    FftsPlus,
}

/// A device task with raw syscnt ticks (converted to wall clock downstream).
///
/// Pairing mirrors `LogModeling::MergeStartAndEnd`: group ACSQ start/end records
/// per `stream_id`, sort each side by timestamp, and pair them index-wise.
/// FFTS+ records are the AIV sub-tasks of a *mix* task; they do not become rows,
/// they only mark the owning ACSQ task (`mix`), exactly as `LogModeling` folds
/// them into the ACSQ `DeviceTask`.
#[derive(Debug, Clone, Copy)]
pub struct DeviceTask {
    pub device_id: u16,
    pub log_type: LogKind,
    pub task_type: u16,
    pub stream_id: u16,
    pub task_id: u16,
    pub context_id: u32,
    pub start_raw: u64,
    pub end_raw: u64,
    /// Timing used by `op_summary`: the FFTS+ pair for a mix task, else ACSQ.
    pub op_start_raw: u64,
    pub op_end_raw: u64,
    pub mix: bool,
}

impl Node for DeviceTask {
    const NAME: &'static str = "DeviceTask";
    type Deps = (StarsSocEntry,);

    fn compute(ctx: &Ctx, (soc,): (std::sync::Arc<Vec<StarsSocEntry>>,)) -> Result<Vec<Self>> {
        let device_id = ctx.require::<DeviceInfo>()?.device_id.unwrap_or(0) as u16;

        // `LogModeling::MergeStartAndEnd` groups by the full task key, not by
        // stream: two tasks on one stream must not be paired with each other.
        let mut acsq_start: BTreeMap<(u16, u16), Vec<Acsq>> = BTreeMap::new();
        let mut acsq_end: BTreeMap<(u16, u16), Vec<Acsq>> = BTreeMap::new();
        // FFTS+ records are AIV sub-tasks of a mix task. They annotate the ACSQ
        // task (`mix`) and provide the timing that `op_summary` reports for it.
        let mut ffts_start: BTreeMap<(u16, u16), u64> = BTreeMap::new();
        let mut ffts_end: BTreeMap<(u16, u16), u64> = BTreeMap::new();

        for entry in soc.iter() {
            match *entry {
                StarsSocEntry::Acsq(a) if a.is_end => acsq_end
                    .entry((a.stream_id, a.task_id))
                    .or_default()
                    .push(a),
                StarsSocEntry::Acsq(a) => acsq_start
                    .entry((a.stream_id, a.task_id))
                    .or_default()
                    .push(a),
                StarsSocEntry::FftsPlus(f) if f.sub_task_id == 0 => {
                    let key = (f.stream_id, f.task_id);
                    if f.is_end {
                        ffts_end.insert(key, f.timestamp);
                    } else {
                        ffts_start.insert(key, f.timestamp);
                    }
                }
                StarsSocEntry::FftsPlus(_) => {}
            }
        }
        let mix: BTreeMap<(u16, u16), (u64, u64)> = ffts_start
            .into_iter()
            .filter_map(|(key, start)| ffts_end.get(&key).map(|end| (key, (start, *end))))
            .collect();

        let mut tasks = Vec::new();
        for ((stream, task_id), mut starts) in acsq_start {
            let Some(mut ends) = acsq_end.remove(&(stream, task_id)) else {
                continue;
            };
            starts.sort_by_key(|a| a.timestamp);
            ends.sort_by_key(|a| a.timestamp);
            let push = |start: &Acsq, end: &Acsq, tasks: &mut Vec<DeviceTask>| {
                let key = (stream, task_id);
                let (op_start_raw, op_end_raw) = mix
                    .get(&key)
                    .copied()
                    .unwrap_or((start.timestamp, end.timestamp));
                tasks.push(DeviceTask {
                    device_id,
                    log_type: LogKind::Acsq,
                    task_type: start.task_type,
                    stream_id: stream,
                    task_id,
                    context_id: u32::MAX,
                    start_raw: start.timestamp,
                    end_raw: end.timestamp,
                    op_start_raw,
                    op_end_raw,
                    mix: mix.contains_key(&key),
                });
            };

            if starts.len() == ends.len() {
                for (start, end) in starts.iter().zip(ends.iter()) {
                    push(start, end, &mut tasks);
                }
                continue;
            }
            // `MergeStartAndEndByQueue`: pair an end with the start it falls in.
            let (mut s, mut e) = (0usize, 0usize);
            while s < starts.len() && e < ends.len() {
                let (start, end) = (&starts[s], &ends[e]);
                let next = starts.get(s + 1).map(|a| a.timestamp);
                if end.timestamp >= start.timestamp && next.is_none_or(|next| end.timestamp < next)
                {
                    push(start, end, &mut tasks);
                    s += 1;
                    e += 1;
                } else if end.timestamp >= start.timestamp {
                    s += 1;
                } else {
                    e += 1;
                }
            }
        }

        Ok(tasks)
    }
}

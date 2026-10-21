use std::collections::{HashMap, VecDeque};

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::raw;
use crate::tables;

// Tensor descriptors (input/output shapes, data types, formats)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TensorInfoRow {
    pub op_name_hash: u64,
    pub timestamp: u64,
    pub thread_id: u32,
    pub tensors: Vec<raw::TensorDescRaw>,
}

impl Node for TensorInfoRow {
    const NAME: &'static str = "tensor_info";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes =
            raw::read_host_stream(ctx, "aging.additional.tensor_info.", raw::ADDITIONAL_TRUNK)?;
        let raws = raw::decode_tensor_info(&bytes)?;
        // A task's tensors span several 256-byte records that share
        // (op hash, timestamp, threadId); merge them into one descriptor set.
        let mut groups: Vec<TensorInfoRow> = Vec::new();
        let mut index: HashMap<(u64, u64, u32), usize> = HashMap::new();
        for r in raws {
            let key = (r.op_name_hash, r.timestamp, r.thread_id);
            if let Some(&i) = index.get(&key) {
                groups[i].tensors.extend(r.tensors);
            } else {
                index.insert(key, groups.len());
                groups.push(TensorInfoRow {
                    op_name_hash: r.op_name_hash,
                    timestamp: r.timestamp,
                    thread_id: r.thread_id,
                    tensors: r.tensors,
                });
            }
        }
        Ok(groups)
    }
}

pub(crate) fn shape_str(tensor: &raw::TensorDescRaw) -> String {
    let mut dims = Vec::new();
    for &dim in &tensor.shape {
        if dim == 0 {
            break;
        }
        dims.push(dim.to_string());
    }
    dims.join(",")
}

pub(crate) fn format_str(format: u32) -> String {
    let id = format & 0xff;
    let sub = (format & 0xffff00) >> 8;
    let name = tables::format_name(id);
    if sub > 0 {
        format!("{name}:{sub}")
    } else {
        name
    }
}

pub(crate) fn shapes_field(tensors: &[raw::TensorDescRaw]) -> String {
    if tensors.is_empty() {
        "N/A".to_string()
    } else {
        format!(
            "\"{}\"",
            tensors.iter().map(shape_str).collect::<Vec<_>>().join(";")
        )
    }
}

pub(crate) fn join_or_na(values: Vec<String>) -> String {
    if values.is_empty() {
        "N/A".to_string()
    } else {
        values.join(";")
    }
}

pub(crate) fn take_tensors(
    queue: &mut VecDeque<&TensorInfoRow>,
) -> (Vec<raw::TensorDescRaw>, Vec<raw::TensorDescRaw>) {
    let Some(group) = queue.pop_front() else {
        return (Vec::new(), Vec::new());
    };
    let inputs = group
        .tensors
        .iter()
        .filter(|tensor| tensor.tensor_type == 0)
        .copied()
        .collect();
    let outputs = group
        .tensors
        .iter()
        .filter(|tensor| tensor.tensor_type == 1)
        .copied()
        .collect();
    (inputs, outputs)
}

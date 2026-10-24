use std::collections::{HashMap, VecDeque};

use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

use crate::tables;

// Tensor descriptors (input/output shapes, data types, formats)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TensorInfoRow {
    pub op_name_hash: u64,
    pub timestamp: u64,
    pub thread_id: u32,
    pub tensors: Vec<super::tensor_info::TensorDescRaw>,
}

impl Node for TensorInfoRow {
    const NAME: &'static str = "tensor_info";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_host_stream(
            ctx,
            "aging.additional.tensor_info.",
            crate::stream::ADDITIONAL_TRUNK,
        )?;
        let raws = super::tensor_info::decode_tensor_info(&bytes)?;
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

pub(crate) fn shape_str(tensor: &super::tensor_info::TensorDescRaw) -> String {
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

pub(crate) fn shapes_field(tensors: &[super::tensor_info::TensorDescRaw]) -> String {
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
) -> (
    Vec<super::tensor_info::TensorDescRaw>,
    Vec<super::tensor_info::TensorDescRaw>,
) {
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

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct TensorData {
    tensor_type: U32,
    format: U32,
    data_type: U32,
    shape: [U32; 8],
}

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct TensorInfoData {
    op_name: U64,
    tensor_num: U32,
    tensors: [TensorData; 5],
}

#[derive(Debug, Clone, Copy)]
pub struct TensorDescRaw {
    pub tensor_type: u32,
    pub format: u32,
    pub data_type: u32,
    pub shape: [u32; 8],
}

#[derive(Debug, Clone)]
pub struct TensorInfoRaw {
    pub op_name_hash: u64,
    pub timestamp: u64,
    pub thread_id: u32,
    pub tensors: Vec<TensorDescRaw>,
}

pub fn decode_tensor_info(bytes: &[u8]) -> Result<Vec<TensorInfoRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<ADDITIONAL_TRUNK>().0 {
        let head = AdditionalHead::ref_from_bytes(chunk).map_err(cast)?;
        if head.magic.get() != COMPACT_MAGIC {
            continue;
        }
        let data = TensorInfoData::ref_from_bytes(&head.data).map_err(cast)?;
        let count = (data.tensor_num.get() as usize).min(data.tensors.len());
        let mut tensors = Vec::with_capacity(count);
        for tensor in data.tensors.iter().take(count) {
            tensors.push(TensorDescRaw {
                tensor_type: tensor.tensor_type.get(),
                format: tensor.format.get(),
                data_type: tensor.data_type.get(),
                shape: core::array::from_fn(|j| tensor.shape[j].get()),
            });
        }
        out.push(TensorInfoRaw {
            op_name_hash: data.op_name.get(),
            timestamp: head.timestamp.get(),
            thread_id: head.thread_id.get(),
            tensors,
        });
    }
    Ok(out)
}

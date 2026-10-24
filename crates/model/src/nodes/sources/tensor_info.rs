use std::collections::{HashMap, VecDeque};

use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

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
    let name = format_name(id);
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

pub fn format_name(id: u32) -> String {
    match id {
        0 => "NCHW".to_string(),
        1 => "NHWC".to_string(),
        2 => "ND".to_string(),
        3 => "NC1HWC0".to_string(),
        4 => "FRACTAL_Z".to_string(),
        5 => "NC1C0HWPAD".to_string(),
        6 => "NHWC1C0".to_string(),
        7 => "FSR_NCHW".to_string(),
        8 => "FRACTAL_DECONV".to_string(),
        9 => "C1HWNC0".to_string(),
        10 => "FRACTAL_DECONV_TRANSPOSE".to_string(),
        11 => "FRACTAL_DECONV_SP_STRIDE_TRANS".to_string(),
        12 => "NC1HWC0_C04".to_string(),
        13 => "FRACTAL_Z_C04".to_string(),
        14 => "CHWN".to_string(),
        15 => "FRACTAL_DECONV_SP_STRIDE8_TRANS".to_string(),
        16 => "HWCN".to_string(),
        17 => "NC1KHKWHWC0".to_string(),
        18 => "BN_WEIGHT".to_string(),
        19 => "FILTER_HWCK".to_string(),
        20 => "HASHTABLE_LOOKUP_LOOKUPS".to_string(),
        21 => "HASHTABLE_LOOKUP_KEYS".to_string(),
        22 => "HASHTABLE_LOOKUP_VALUE".to_string(),
        23 => "HASHTABLE_LOOKUP_OUTPUT".to_string(),
        24 => "HASHTABLE_LOOKUP_HITS".to_string(),
        25 => "C1HWNCoC0".to_string(),
        26 => "MD".to_string(),
        27 => "NDHWC".to_string(),
        28 => "FRACTAL_ZZ".to_string(),
        29 => "FRACTAL_NZ".to_string(),
        30 => "NCDHW".to_string(),
        31 => "DHWCN".to_string(),
        32 => "NDC1HWC0".to_string(),
        33 => "FRACTAL_Z_3D".to_string(),
        34 => "CN".to_string(),
        35 => "NC".to_string(),
        36 => "DHWNC".to_string(),
        37 => "FRACTAL_Z_3D_TRANSPOSE".to_string(),
        38 => "FRACTAL_ZN_LSTM".to_string(),
        39 => "FRACTAL_Z_G".to_string(),
        40 => "RESERVED".to_string(),
        41 => "ALL".to_string(),
        42 => "NULL".to_string(),
        43 => "ND_RNN_BIAS".to_string(),
        44 => "FRACTAL_ZN_RNN".to_string(),
        45 => "NYUV".to_string(),
        46 => "NYUV_A".to_string(),
        47 => "NCL".to_string(),
        48 => "FRACTAL_Z_WINO".to_string(),
        49 => "C1HWC0".to_string(),
        50 => "FRACTAL_NZ_C0_16".to_string(),
        51 => "FRACTAL_NZ_C0_32".to_string(),
        52 => "FRACTAL_NZ_C0_2".to_string(),
        53 => "FRACTAL_NZ_C0_4".to_string(),
        54 => "FRACTAL_NZ_C0_8".to_string(),
        55 => "END".to_string(),
        200 => "UNKNOWN_".to_string(),
        201 => "DEFAULT_".to_string(),
        202 => "NC1KHKWHWC0_".to_string(),
        203 => "ND_".to_string(),
        204 => "NCHW_".to_string(),
        205 => "NHWC_".to_string(),
        206 => "HWCN_".to_string(),
        207 => "NC1HWC0_".to_string(),
        208 => "FRAC_Z_".to_string(),
        209 => "C1HWNCOC0_".to_string(),
        210 => "FRAC_NZ_".to_string(),
        211 => "NC1HWC0_C04_".to_string(),
        212 => "FRACTAL_Z_C04_".to_string(),
        213 => "NDHWC_".to_string(),
        214 => "FRACTAL_ZN_LSTM_".to_string(),
        215 => "FRACTAL_ZN_RNN_".to_string(),
        216 => "ND_RNN_BIAS_".to_string(),
        217 => "NDC1HWC0_".to_string(),
        218 => "NCDHW_".to_string(),
        219 => "FRACTAL_Z_3D_".to_string(),
        220 => "DHWNC_".to_string(),
        221 => "DHWCN_".to_string(),
        _ => id.to_string(),
    }
}

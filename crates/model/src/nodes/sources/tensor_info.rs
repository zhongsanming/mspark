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
        let mut raws = Vec::new();
        crate::stream::for_each_chunk(
            crate::stream::StreamReader::open(ctx, Stream::TensorInfo)?,
            |chunk| {
                raws.extend(super::tensor_info::decode_tensor_info(chunk)?);
                Ok(())
            },
        )?;
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

pub(crate) fn format_str(format: u32) -> std::borrow::Cow<'static, str> {
    let id = format & 0xff;
    let sub = (format & 0xffff00) >> 8;
    let name = format_name(id);
    if sub > 0 {
        std::borrow::Cow::Owned(format!("{name}:{sub}"))
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

/// Join tensor fields with `;`, or `N/A` when there are none. Takes any
/// string-ish item so `&'static str` table hits never allocate.
pub(crate) fn join_or_na<T: AsRef<str>>(values: Vec<T>) -> String {
    if values.is_empty() {
        return "N/A".to_string();
    }
    let mut out = String::new();
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(';');
        }
        out.push_str(value.as_ref());
    }
    out
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

pub fn format_name(id: u32) -> std::borrow::Cow<'static, str> {
    match id {
        0 => "NCHW".into(),
        1 => "NHWC".into(),
        2 => "ND".into(),
        3 => "NC1HWC0".into(),
        4 => "FRACTAL_Z".into(),
        5 => "NC1C0HWPAD".into(),
        6 => "NHWC1C0".into(),
        7 => "FSR_NCHW".into(),
        8 => "FRACTAL_DECONV".into(),
        9 => "C1HWNC0".into(),
        10 => "FRACTAL_DECONV_TRANSPOSE".into(),
        11 => "FRACTAL_DECONV_SP_STRIDE_TRANS".into(),
        12 => "NC1HWC0_C04".into(),
        13 => "FRACTAL_Z_C04".into(),
        14 => "CHWN".into(),
        15 => "FRACTAL_DECONV_SP_STRIDE8_TRANS".into(),
        16 => "HWCN".into(),
        17 => "NC1KHKWHWC0".into(),
        18 => "BN_WEIGHT".into(),
        19 => "FILTER_HWCK".into(),
        20 => "HASHTABLE_LOOKUP_LOOKUPS".into(),
        21 => "HASHTABLE_LOOKUP_KEYS".into(),
        22 => "HASHTABLE_LOOKUP_VALUE".into(),
        23 => "HASHTABLE_LOOKUP_OUTPUT".into(),
        24 => "HASHTABLE_LOOKUP_HITS".into(),
        25 => "C1HWNCoC0".into(),
        26 => "MD".into(),
        27 => "NDHWC".into(),
        28 => "FRACTAL_ZZ".into(),
        29 => "FRACTAL_NZ".into(),
        30 => "NCDHW".into(),
        31 => "DHWCN".into(),
        32 => "NDC1HWC0".into(),
        33 => "FRACTAL_Z_3D".into(),
        34 => "CN".into(),
        35 => "NC".into(),
        36 => "DHWNC".into(),
        37 => "FRACTAL_Z_3D_TRANSPOSE".into(),
        38 => "FRACTAL_ZN_LSTM".into(),
        39 => "FRACTAL_Z_G".into(),
        40 => "RESERVED".into(),
        41 => "ALL".into(),
        42 => "NULL".into(),
        43 => "ND_RNN_BIAS".into(),
        44 => "FRACTAL_ZN_RNN".into(),
        45 => "NYUV".into(),
        46 => "NYUV_A".into(),
        47 => "NCL".into(),
        48 => "FRACTAL_Z_WINO".into(),
        49 => "C1HWC0".into(),
        50 => "FRACTAL_NZ_C0_16".into(),
        51 => "FRACTAL_NZ_C0_32".into(),
        52 => "FRACTAL_NZ_C0_2".into(),
        53 => "FRACTAL_NZ_C0_4".into(),
        54 => "FRACTAL_NZ_C0_8".into(),
        55 => "END".into(),
        200 => "UNKNOWN_".into(),
        201 => "DEFAULT_".into(),
        202 => "NC1KHKWHWC0_".into(),
        203 => "ND_".into(),
        204 => "NCHW_".into(),
        205 => "NHWC_".into(),
        206 => "HWCN_".into(),
        207 => "NC1HWC0_".into(),
        208 => "FRAC_Z_".into(),
        209 => "C1HWNCOC0_".into(),
        210 => "FRAC_NZ_".into(),
        211 => "NC1HWC0_C04_".into(),
        212 => "FRACTAL_Z_C04_".into(),
        213 => "NDHWC_".into(),
        214 => "FRACTAL_ZN_LSTM_".into(),
        215 => "FRACTAL_ZN_RNN_".into(),
        216 => "ND_RNN_BIAS_".into(),
        217 => "NDC1HWC0_".into(),
        218 => "NCDHW_".into(),
        219 => "FRACTAL_Z_3D_".into(),
        220 => "DHWNC_".into(),
        221 => "DHWCN_".into(),
        _ => std::borrow::Cow::Owned(id.to_string()),
    }
}

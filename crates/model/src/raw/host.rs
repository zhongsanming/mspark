//! Host stream decoders: compact records, tensor descriptors, API events.

use anyhow::Result;
use mspark_df::Ctx;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use super::{cast, read_concat};

/// Read a `host/data` stream (same slice-concat semantics).
pub fn read_host_stream(ctx: &Ctx, prefix: &str, trunk: usize) -> Result<Vec<u8>> {
    read_concat(&ctx.host_data_dir(), prefix, trunk)
}

// ---------------------------------------------------------------------------
// Host compact streams (`MsprofCompactInfo`, 64-byte records)
// ---------------------------------------------------------------------------

pub const COMPACT_TRUNK: usize = 64;
pub const COMPACT_MAGIC: u16 = 0x5a5a;

/// `MsprofCompactInfo` prefix (24 bytes) + 40-byte `data` union.
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct CompactHead {
    magic: U16,
    _level: U16,
    _ty: U32,
    _thread: U32,
    _data_len: U32,
    _timestamp: U64,
    data: [u8; 40],
}
const _: () = assert!(core::mem::size_of::<CompactHead>() == COMPACT_TRUNK);

/// `MsprofNodeBasicInfo` (packed: 29 bytes, padded to the 40-byte union).
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct NodeBasicInfoData {
    op_name: U64,
    task_type: U32,
    op_type: U64,
    block_num: U32,
    _op_flag: U32,
    _op_state: u8,
    _pad: [u8; 11],
}
const _: () = assert!(core::mem::size_of::<NodeBasicInfoData>() == 40);

/// `MsprofRuntimeTrack` (V1) inside the `MsprofCompactInfo` union.
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct TaskTrackData {
    _device_id: U16,
    stream_id: U16,
    task_id: U32,
    task_type: U64,
    kernel_name: U64,
    _ext: [u8; 16],
}
const _: () = assert!(core::mem::size_of::<TaskTrackData>() == 40);

#[derive(Debug, Clone, Copy)]
pub struct NodeBasicInfoRaw {
    pub op_name_hash: u64,
    pub op_type_hash: u64,
    pub task_type: u32,
    pub block_num: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct TaskTrackRaw {
    pub stream_id: u16,
    pub task_id: u32,
    pub task_type: u64,
    /// Hash of the host-side kernel name (resolved through `HashDic`).
    pub kernel_name: u64,
}

// ---------------------------------------------------------------------------
// Host `additional` streams (`MsprofAdditionalInfo`, 256-byte records)
// ---------------------------------------------------------------------------

pub const ADDITIONAL_TRUNK: usize = 256;

/// `MsprofAdditionalInfo` prefix (24 bytes) + 232-byte `data` union.
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct AdditionalHead {
    magic: U16,
    _level: U16,
    _ty: U32,
    thread_id: U32,
    _data_len: U32,
    timestamp: U64,
    data: [u8; 232],
}
const _: () = assert!(core::mem::size_of::<AdditionalHead>() == ADDITIONAL_TRUNK);

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct TensorData {
    tensor_type: U32,
    format: U32,
    data_type: U32,
    shape: [U32; 8],
}
const _: () = assert!(core::mem::size_of::<TensorData>() == 44);

#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct TensorInfoData {
    op_name: U64,
    tensor_num: U32,
    tensors: [TensorData; 5],
}
const _: () = assert!(core::mem::size_of::<TensorInfoData>() == 232);

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

pub fn decode_node_basic_info(bytes: &[u8]) -> Result<Vec<NodeBasicInfoRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<COMPACT_TRUNK>().0 {
        let head = CompactHead::ref_from_bytes(chunk).map_err(cast)?;
        if head.magic.get() != COMPACT_MAGIC {
            continue;
        }
        let data = NodeBasicInfoData::ref_from_bytes(&head.data).map_err(cast)?;
        out.push(NodeBasicInfoRaw {
            op_name_hash: data.op_name.get(),
            op_type_hash: data.op_type.get(),
            task_type: data.task_type.get(),
            block_num: data.block_num.get(),
        });
    }
    Ok(out)
}

pub fn decode_task_track(bytes: &[u8]) -> Result<Vec<TaskTrackRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<COMPACT_TRUNK>().0 {
        let head = CompactHead::ref_from_bytes(chunk).map_err(cast)?;
        if head.magic.get() != COMPACT_MAGIC {
            continue;
        }
        let data = TaskTrackData::ref_from_bytes(&head.data).map_err(cast)?;
        out.push(TaskTrackRaw {
            stream_id: data.stream_id.get(),
            task_id: data.task_id.get(),
            task_type: data.task_type.get(),
            kernel_name: data.kernel_name.get(),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Host API events (`MsprofApi`, 40-byte records)
// ---------------------------------------------------------------------------

pub const API_EVENT_TRUNK: usize = 40;

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
const _: () = assert!(core::mem::size_of::<ApiEventData>() == API_EVENT_TRUNK);

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

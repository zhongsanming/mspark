use crate::stream::*;
use anyhow::Result;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use mspark_df::{Ctx, Node};

#[derive(Debug, Clone)]
pub struct TaskTrackRow {
    pub stream_id: u16,
    pub task_id: u32,
    pub task_type: u64,
    /// Host kernel name (preferred over the device op name when present).
    pub kernel_name: String,
}

impl Node for TaskTrackRow {
    const NAME: &'static str = "task_track";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_host_stream(
            ctx,
            "aging.compact.task_track.",
            crate::stream::COMPACT_TRUNK,
        )?;
        let dic = ctx.require::<crate::host::HashDic>()?;
        Ok(super::task_track::decode_task_track(&bytes)?
            .into_iter()
            .map(|r| TaskTrackRow {
                stream_id: r.stream_id,
                task_id: r.task_id,
                task_type: r.task_type,
                kernel_name: dic.get(r.kernel_name).unwrap_or_default().to_string(),
            })
            .collect())
    }
}

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
// HCCL task info (`MsprofHcclInfo` inside an `MsprofAdditionalInfo`)
// ---------------------------------------------------------------------------

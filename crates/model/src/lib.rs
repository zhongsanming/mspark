//! Ascend profiling model: raw record decoding, the immutable context, and the
//! dataflow nodes that turn raw `.done` streams into typed tables.
//!
//! v1 targets the `V4_1_0` (910B) device streams that are actually present in
//! the reference profile: `stars_soc`, `ffts_profile`, `ts_track`, `lpmFreqConv`.

pub mod ctx;
pub(crate) mod format;
pub mod host;
pub mod nodes;
pub(crate) mod pmu;
pub mod raw;
pub mod tables;
pub mod time;

pub use ctx::{Chip, DeviceInfo, load_device_info, load_sample_string};
pub use host::{HashDic, TypeDic, ge_task_type, load_dictionaries};
pub use nodes::sources::{LLC_READ_SLOTS, NetSample};
pub use nodes::*;

pub use raw::{Acsq, FftsPlus, LpmInfo, LpmSample, PmuData, StarsSocEntry, TrackFlip};
pub use time::{
    Clock, MonoClock, SamplingClock, fmt_us, load_clock, load_host_clock, load_host_syscnt_clock,
    load_mono_clock, load_sampling_clock,
};

// The node graph is declared once, here. Edges come from each `impl Node`'s
// `Deps` tuple, so nothing is duplicated; a cycle or an unknown edge is a
// compile error.
mspark_df::graph!(
    AccPmuSample,
    ApiEventRow,
    ApiStatisticRow,
    ChipTransSample,
    DeviceTask,
    DvppRow,
    HbmRecord,
    HccsRateRow,
    HostCpuUsageRow,
    InterSocSample,
    L2CacheRow,
    NodeBasicInfoRow,
    NpuMemRow,
    NicRow,
    NpuModuleMemRow,
    RoceRow,
    PcieRecord,
    OpStatisticRow,
    OpSummaryRow,
    OverlapRow,
    QosRow,
    LlcRecord,
    LpmInfo,
    PmuData,
    StarsSocEntry,
    TrackFlip,
    SioSample,
    SocProfileRecord,
    TaskTimeRow,
    TaskTrackRow,
    TensorInfoRow,
    TimelineTask,
    TscpuRecord,
);

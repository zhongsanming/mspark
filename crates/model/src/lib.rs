//! Ascend profiling model: raw record decoding, the immutable context, and the
//! dataflow nodes that turn raw `.done` streams into typed tables.
//!
//! v1 targets the `V4_1_0` (910B) device streams that are actually present in
//! the reference profile: `stars_soc`, `ffts_profile`, `ts_track`, `lpmFreqConv`.

pub mod ctx;
pub(crate) mod format;
pub mod host;
pub mod nodes;
pub mod stream;
pub mod time;

pub use ctx::{Chip, DeviceInfo, load_device_info, load_sample_string};
pub use host::{HashDic, TypeDic, ge_task_type, load_dictionaries};
pub use nodes::sources::api_event::api_level_name;
pub use nodes::sources::dvpp::dvpp_engine_name;
pub use nodes::sources::npu_mem::npu_mem_event;
pub use nodes::sources::npu_module_mem::module_name;
pub use nodes::sources::{LLC_READ_SLOTS, NetSample};
pub use nodes::*;

pub use nodes::sources::ffts_profile::PmuData;
pub use nodes::sources::lpm::{LpmInfo, LpmSample};
pub use nodes::sources::stars_soc::{Acsq, FftsPlus, StarsSocEntry};
pub use nodes::sources::ts_track::TrackFlip;
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
    HcclInfoRow,
    HcclOpRow,
    HcclInfoRow,
    HcclOpRow,
    HccsRateRow,
    HostCpuUsageRow,
    HostDiskUsageRow,
    HostMemUsageRow,
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

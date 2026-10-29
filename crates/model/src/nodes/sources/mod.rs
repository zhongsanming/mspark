//! Source nodes: each raw stream is a graph root.

pub mod api_event;
pub mod dvpp;
pub mod ffts_profile;
pub mod hbm;
pub mod hccl_info;
pub mod hccl_op;
pub mod hccs;
pub mod host_cpu;
pub mod host_disk;
pub mod host_mem;
pub mod l2_cache;
pub mod llc;
pub mod lpm;
pub mod net;
pub mod node_basic_info;
pub mod npu_mem;
pub mod npu_module_mem;
pub mod pcie;
pub mod qos;
pub mod stars_soc;
pub mod stars_soc_profile;
pub mod task_track;
pub mod tensor_info;
pub mod ts_track;
pub mod tscpu;

pub use api_event::ApiEventRow;
pub use dvpp::DvppRow;
pub use hbm::HbmRecord;
pub use hccl_info::HcclInfoRow;
pub use hccl_op::HcclOpRow;
pub use hccs::HccsRateRow;
pub use host_cpu::HostCpuUsageRow;
pub use host_disk::HostDiskUsageRow;
pub use host_mem::HostMemUsageRow;
pub use l2_cache::L2CacheRow;
pub use llc::{LLC_READ_SLOTS, LlcRecord};
pub use net::{NetSample, NicRow, RoceRow};
pub use node_basic_info::NodeBasicInfoRow;
pub use npu_mem::NpuMemRow;
pub use npu_module_mem::NpuModuleMemRow;
pub use pcie::PcieRecord;
pub use qos::QosRow;
pub use stars_soc_profile::SocProfileRecord;
pub use task_track::TaskTrackRow;
pub use tensor_info::TensorInfoRow;
pub use tscpu::TscpuRecord;

/// Temporary on-disk device used by decoder tests.
///
/// Nodes read streams from `<device>/data` and clocks from the start logs, so a
/// decoder test has to build the same four files a capture would contain. The
/// directory is removed on drop.
#[cfg(test)]
pub(crate) mod testing {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use mspark_df::Ctx;

    use crate::ctx::{Chip, DeviceInfo};
    use crate::time::{DeviceStartLogs, StartLogs};

    pub struct FakeDevice {
        pub dir: PathBuf,
    }

    impl FakeDevice {
        pub fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "mspark-decode-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(dir.join("data")).unwrap();
            fs::create_dir_all(dir.join("host")).unwrap();
            fs::write(
                dir.join("start_info"),
                r#"{"collectionTimeBegin":"1000","clockMonotonicRaw":"0"}"#,
            )
            .unwrap();
            fs::write(
                dir.join("dev_start.log"),
                "clock_monotonic_raw:1000000000\n",
            )
            .unwrap();
            fs::write(
                dir.join("host_start.log"),
                "clock_monotonic_raw:1000000000\n",
            )
            .unwrap();
            Self { dir }
        }

        /// Write one slice of a device stream (`name` includes the leading
        /// prefix, e.g. `hbm.slice_0`).
        pub fn write(&self, name: &str, bytes: &[u8]) {
            fs::write(self.dir.join("data").join(name), bytes).unwrap();
        }

        /// Context with the start logs but no `DeviceInfo`; callers that need a
        /// stream layout version insert [`device_info`] themselves.
        pub fn ctx(&self) -> Ctx {
            let mut ctx = Ctx::new(&self.dir).with_device(&self.dir);
            ctx.insert(DeviceStartLogs(StartLogs::load(&self.dir).unwrap()));
            ctx
        }
    }

    impl Drop for FakeDevice {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.dir).ok();
        }
    }

    /// Minimal `DeviceInfo` for layout/version tests.
    pub fn device_info(collection_version: &str) -> DeviceInfo {
        DeviceInfo {
            chip: Chip::V4_1_0,
            device_id: Some(0),
            ai_core_num: 0,
            aiv_num: 0,
            aic_freq_mhz: 0.0,
            aiv_freq_mhz: 0.0,
            hwts_freq_mhz: 0.0,
            host_freq_mhz: 0.0,
            pid: 0,
            expand_status: 0,
            collection_version: collection_version.to_string(),
        }
    }
}

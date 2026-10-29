//! Re-exports for the CLI: one import in `main.rs`, one name per command.

pub use crate::artifacts::{
    ALL_ARTIFACTS, Artifact, CSV_ARTIFACTS, export, parse_outputs, write_artifact,
};
pub use crate::reports::communication::communication_csv;
pub use crate::reports::hbm::hbm_csv;
pub use crate::reports::hccs::hccs_csv;
pub use crate::reports::host::{host_cpu_csv, host_mem_csv};
pub use crate::reports::inspect::{graph, stats};
pub use crate::reports::llc::llc_csv;
pub use crate::reports::memory::{dvpp, l2_cache, npu_mem, npu_module_mem};
pub use crate::reports::net::{nic_csv, roce_csv};
pub use crate::reports::pcie::pcie_csv;
pub use crate::reports::summary::summary;
pub use crate::reports::task::{api_statistic, op_statistic, op_summary, task_time};
pub use crate::reports::timeline::timeline;
pub use crate::reports::tscpu::{ts_cpu_pmu_events, ts_cpu_top_function};
pub use crate::verification::{verify, verify_all, verify_csv, verify_timeline};

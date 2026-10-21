# msprof dependency matrix (first pass)

Extracted from `REGISTER_PROCESS_SEQUENCE`, `REGISTER_PROCESS_DEPENDENT_DATA`, `MAKE_SHARED*` and `GetPtr<T>` in the reference C++ tree. Template args may be truncated; treat as a starting map, not ground truth.

## AccPmuPersistence
- file: `domain/services/persistence/device/acc_pmu_persistence.cpp`
- deps: StarsSocParser, StarsSocProfileParser
- produces: -
- consumes: HalSocProfileData, std::vector<HalLogData
- keeps: HalSocProfileData, std::vector<HalLogData>

## AicpuParser
- file: `domain/services/parser/aicpu/aicpu_parser.cpp`
- deps: LoadStreamExpandSpec
- produces: DeviceStreamInfo, std::vector<AicpuData>
- consumes: StreamExpandSpec
- keeps: StreamExpandSpec

## AicpuPersistence
- file: `domain/services/persistence/device/aicpu_persistence.cpp`
- deps: AicpuParser, LoadHostData, TsTrackParser
- produces: std::vector<KfcInfoData>, std::vector<MasterStreamTaskData>
- consumes: DeviceStreamInfo, GeHashMap, HostStreamInfo, std::vector<AicpuData, std::vector<HalTrackData
- keeps: DeviceStreamInfo, GeHashMap, HostStreamInfo, std::vector<AicpuData>, std::vector<HalTrackData>, std::vector<KfcInfoData>, std::vector<MasterStreamTaskData>

## AscendTaskAssociation
- file: `domain/services/association/ascend_task_association.cpp`
- deps: LoadHostData
- produces: std::vector<TopDownTask>
- consumes: HostStreamInfo, std::map<TaskId, std::vector<DeviceTask, std::map<TaskId, std::vector<HostTask
- keeps: HostStreamInfo, std::map<TaskId, std::vector<DeviceTask>>, std::map<TaskId, std::vector<HostTask>>

## AscendTaskPersistence
- file: `domain/services/persistence/device/ascend_task_persistence.cpp`
- deps: HcclCalculator
- produces: -
- consumes: std::vector<TopDownTask
- keeps: std::vector<TopDownTask>

## CcuChannelParser
- file: `domain/services/parser/ccu_parser.cpp`
- deps: -
- produces: CcuChannelDataSet, CcuMissionDataSet
- consumes: -
- keeps: -

## CcuMissionParser
- file: `domain/services/parser/ccu_parser.cpp`
- deps: -
- produces: CcuChannelDataSet, CcuMissionDataSet
- consumes: -
- keeps: -

## CcuPersistence
- file: `domain/services/persistence/device/ccu_persistence.cpp`
- deps: CcuChannelParser, CcuMissionParser
- produces: -
- consumes: CcuChannelDataSet, CcuMissionDataSet
- keeps: CcuChannelDataSet, CcuMissionDataSet

## ChipTransPersistence
- file: `domain/services/persistence/device/chip_trans_persistence.cpp`
- deps: StarsSocProfileParser
- produces: -
- consumes: HalSocProfileData
- keeps: HalSocProfileData

## DeviceHcclPersistence
- file: `domain/services/persistence/device/device_hccl_persistence.cpp`
- deps: HcclCalculator, KfcCalculator
- produces: -
- consumes: std::vector<DeviceHcclOp, std::vector<DeviceHcclTask, std::vector<HcclStatistics, std::vector<KfcOpRecord, std::vector<KfcOpStatistics, std::vector<KfcTaskRecord
- keeps: std::vector<DeviceHcclOp>, std::vector<DeviceHcclTask>, std::vector<HcclStatistics>, std::vector<KfcOpRecord>, std::vector<KfcOpStatistics>, std::vector<KfcTaskRecord>

## DeviceTaskProcess
- file: `domain/services/init/device_task_process.cpp`
- deps: -
- produces: DeviceTaskSummary
- consumes: -
- keeps: -

## FftsProfileParser
- file: `domain/services/parser/pmu/ffts_profile_parser.cpp`
- deps: LoadStreamExpandSpec
- produces: std::vector<HalPmuData>
- consumes: StreamExpandSpec
- keeps: StreamExpandSpec

## FreqParser
- file: `domain/services/parser/freq/freq_parser.cpp`
- deps: -
- produces: std::vector<HalFreqLpmData>
- consumes: -
- keeps: -

## FreqPersistence
- file: `domain/services/persistence/device/freq_persistence.cpp`
- deps: FreqParser
- produces: -
- consumes: std::vector<HalFreqLpmData
- keeps: std::vector<HalFreqLpmData>

## HcclCalculator
- file: `domain/services/association/calculator/hccl/hccl_calculator.cpp`
- deps: AscendTaskAssociation, LoadHostData
- produces: std::vector<DeviceHcclOp>, std::vector<DeviceHcclTask>, std::vector<HcclStatistics>
- consumes: std::vector<HcclOp, std::vector<HcclTask, std::vector<TopDownTask
- keeps: std::vector<HcclOp>, std::vector<HcclTask>, std::vector<TopDownTask>

## InterSocPersistence
- file: `domain/services/persistence/device/inter_soc_persistence.cpp`
- deps: StarsSocProfileParser
- produces: -
- consumes: HalSocProfileData
- keeps: HalSocProfileData

## KfcCalculator
- file: `domain/services/association/calculator/hccl/kfc_calculator.cpp`
- deps: AicpuPersistence, AscendTaskPersistence
- produces: std::vector<KfcOpRecord>, std::vector<KfcOpStatistics>, std::vector<KfcTaskRecord>
- consumes: AicpuOpNameMap, std::vector<HcclOp, std::vector<KfcInfoData, std::vector<MasterStreamTaskData, std::vector<Mc2CommInfo, std::vector<TopDownTask
- keeps: AicpuOpNameMap, std::vector<HcclOp>, std::vector<KfcInfoData>, std::vector<MasterStreamTaskData>, std::vector<Mc2CommInfo>, std::vector<TopDownTask>

## LlcParser
- file: `domain/services/parser/llc/llc_parser.cpp`
- deps: -
- produces: std::vector<HalLlcData>
- consumes: -
- keeps: -

## LlcPersistence
- file: `domain/services/persistence/device/llc_persistence.cpp`
- deps: LlcParser
- produces: -
- consumes: std::vector<HalLlcData
- keeps: std::vector<HalLlcData>

## LoadHostData
- file: `domain/services/device_context/load_host_data.cpp`
- deps: LogModeling, LogModelingV6
- produces: AicpuOpNameMap, GeHashMap, HostStreamInfo, TaskId2HostTask, std::vector<HcclOp>, std::vector<HcclTask>, std::vector<Mc2CommInfo>
- consumes: std::map<TaskId, std::vector<DeviceTask
- keeps: AicpuOpNameMap, std::map<TaskId, std::vector<Domain::DeviceTask>>, std::vector<Mc2CommInfo>

## LoadStreamExpandSpec
- file: `domain/services/device_context/load_stream_expand_spec_data.cpp`
- deps: -
- produces: StreamExpandSpec
- consumes: -
- keeps: -

## LogModeling
- file: `domain/services/modeling/log_modeling.cpp`
- deps: StarsSocParser, TsTrackParser
- produces: -
- consumes: std::map<TaskId, std::vector<DeviceTask, std::vector<HalLogData, std::vector<HalTrackData
- keeps: std::map<Domain::TaskId, std::vector<Domain::DeviceTask>>, std::vector<Domain::HalLogData>, std::vector<Domain::HalTrackData>

## LogModelingV6
- file: `domain/services/modeling/log_modeling.cpp`
- deps: StarsSocParserV6, TsTrackParser
- produces: -
- consumes: std::map<TaskId, std::vector<DeviceTask, std::vector<HalLogData, std::vector<HalTrackData
- keeps: std::map<Domain::TaskId, std::vector<Domain::DeviceTask>>, std::vector<Domain::HalLogData>, std::vector<Domain::HalTrackData>

## LowPowerPersistence
- file: `domain/services/persistence/device/low_power_persistence.cpp`
- deps: StarsSocProfileParser
- produces: -
- consumes: HalSocProfileData
- keeps: HalSocProfileData

## LpmInfoParser
- file: `domain/services/parser/freq/lpm_info_parser.cpp`
- deps: -
- produces: HalLpmInfoData
- consumes: -
- keeps: -

## LpmInfoPersistence
- file: `domain/services/persistence/device/lpm_info_persistence.cpp`
- deps: LpmInfoParser
- produces: DBRunner, FreqDB, VoltageDB
- consumes: HalLpmInfoData
- keeps: HalLpmInfoData

## MetricSummaryPersistence
- file: `domain/services/persistence/device/metric_summary_persistence.cpp`
- deps: PmuAssociation
- produces: -
- consumes: std::map<TaskId, std::vector<DeviceTask, std::vector<HalPmuData
- keeps: HostStreamInfo, std::map<TaskId, std::vector<Domain::DeviceTask>>, std::vector<HalPmuData>

## NpuMemParser
- file: `domain/services/parser/npu_mem/npu_mem_parser.cpp`
- deps: -
- produces: std::vector<HalNpuMemData>
- consumes: -
- keeps: -

## NpuMemPersistence
- file: `domain/services/persistence/device/npu_mem_persistence.cpp`
- deps: NpuMemParser
- produces: -
- consumes: std::vector<HalNpuMemData
- keeps: std::vector<HalNpuMemData>

## NpuModuleMemParser
- file: `domain/services/parser/npu_module_mem/npu_module_mem_parser.cpp`
- deps: -
- produces: std::vector<HalNpuModuleMemData>
- consumes: -
- keeps: -

## NpuModuleMemPersistence
- file: `domain/services/persistence/device/npu_module_mem_persistence.cpp`
- deps: NpuModuleMemParser
- produces: -
- consumes: std::vector<HalNpuModuleMemData
- keeps: std::vector<HalNpuModuleMemData>

## PcieParser
- file: `domain/services/parser/pcie/pcie_parser.cpp`
- deps: -
- produces: std::vector<HalPcieData>
- consumes: -
- keeps: -

## PciePersistence
- file: `domain/services/persistence/device/pcie_persistence.cpp`
- deps: PcieParser
- produces: -
- consumes: std::vector<HalPcieData
- keeps: std::vector<HalPcieData>

## PmuAssociation
- file: `domain/services/association/pmu_association.cpp`
- deps: AscendTaskAssociation, LoadHostData, PmuModeling
- produces: -
- consumes: HostStreamInfo, std::map<TaskId, std::vector<DeviceTask, std::vector<HalPmuData
- keeps: HostStreamInfo, std::map<TaskId, std::vector<DeviceTask>>, std::vector<HalFreqLpmData>, std::vector<HalPmuData>

## PmuModeling
- file: `domain/services/modeling/pmu_modeling.cpp`
- deps: FftsProfileParser, TsTrackParser
- produces: -
- consumes: std::vector<HalPmuData, std::vector<HalTrackData
- keeps: std::vector<HalPmuData>, std::vector<HalTrackData>

## ProcessClassType
- file: `infrastructure/process/include/process_register.h`
- deps: ...
- produces: -
- consumes: -
- keeps: ...

## QosParser
- file: `domain/services/parser/qos/qos_parser.cpp`
- deps: -
- produces: std::vector<HalQosBwData>
- consumes: -
- keeps: -

## QosPersistence
- file: `domain/services/persistence/device/qos_persistence.cpp`
- deps: QosParser, StarsSocProfileParser
- produces: -
- consumes: HalSocProfileData, std::vector<HalQosBwData
- keeps: HalSocProfileData, std::vector<HalQosBwData>

## SioPersistence
- file: `domain/services/persistence/device/sio_persistence.cpp`
- deps: StarsSocProfileParser
- produces: -
- consumes: HalSocProfileData
- keeps: HalSocProfileData

## StarsSocParser
- file: `domain/services/parser/log/stars_soc_parser.cpp`
- deps: LoadStreamExpandSpec
- produces: std::vector<HalLogData>
- consumes: StreamExpandSpec
- keeps: StreamExpandSpec

## StarsSocParserV6
- file: `domain/services/parser/log/stars_soc_parser.cpp`
- deps: -
- produces: std::vector<HalLogData>
- consumes: StreamExpandSpec
- keeps: -

## StarsSocProfileParser
- file: `domain/services/parser/log/stars_soc_profile_parser.cpp`
- deps: -
- produces: HalSocProfileData
- consumes: -
- keeps: -

## StepTracePersistence
- file: `domain/services/persistence/device/step_trace_persistence.cpp`
- deps: StepTraceProcess, TsTrackParser
- produces: -
- consumes: std::map<uint32_t, std::vector<StepTraceTasks, std::vector<HalTrackData
- keeps: std::map<uint32_t, std::vector<StepTraceTasks>>, std::vector<HalTrackData>

## StepTraceProcess
- file: `domain/services/modeling/step_trace/step_trace_process.cpp`
- deps: TsTrackParser
- produces: StepTraceTaskMap
- consumes: std::vector<HalTrackData
- keeps: std::vector<HalTrackData>

## TracePersistence
- file: `domain/services/persistence/device/trace_persistence.cpp`
- deps: StepTraceProcess, TsTrackParser
- produces: -
- consumes: std::map<uint32_t, std::vector<StepTraceTasks, std::vector<HalTrackData
- keeps: std::map<uint32_t, std::vector<StepTraceTasks>>, std::vector<HalTrackData>

## TsTrackParser
- file: `domain/services/parser/track/ts_track_parser.cpp`
- deps: LoadStreamExpandSpec
- produces: std::vector<HalTrackData>
- consumes: StreamExpandSpec
- keeps: StreamExpandSpec

## TsTrackPersistence
- file: `domain/services/persistence/device/ts_track_persistence.cpp`
- deps: TsTrackParser
- produces: -
- consumes: std::vector<HalTrackData
- keeps: std::vector<HalTrackData>

## UbParser
- file: `domain/services/parser/ub/ub_parser.cpp`
- deps: -
- produces: std::vector<HalUbBwData>
- consumes: -
- keeps: -

## UbPersistence
- file: `domain/services/persistence/device/ub_persistence.cpp`
- deps: UbParser
- produces: -
- consumes: std::vector<HalUbBwData
- keeps: std::vector<HalUbBwData>

# Milestone 7 — CSV cluster (complete)

Status: **all six CSV deliverables implemented and byte-exact** on profile B.

| deliverable | rows | result |
| --- | --- | --- |
| `npu_mem` | 459 | all columns 459/459 |
| `npu_module_mem` | 17,941 | all columns 17,941/17,941 |
| `dvpp` | 41 | all columns 41/41 |
| `l2_cache` | 8,103 | all columns 8,103/8,103 |
| `ts_cpu_pmu_events` | 2 | all columns 2/2 |
| `ts_cpu_top_function` | 5 | all columns 5/5 |

## `npu_mem`

`npu_mem.app.*` then `npu_mem.data.*`, 32-byte V1 records
(`timestamp_us: u32@0`, `event: u32@4`, `ddr: u64@16`, `hbm: u64@24`).

```
wall_ns = timestamp_us * 1000 + host_monotonic + wall_offset
```

Drop rows before `collectionTimeBegin*1000`; emit
`Device_id,event,ddr(KB),hbm(KB),memory(KB),timestamp(us)` with
`0 → APP`, `1 → Device`, sizes `/1024`, `memory = ddr + hbm`, and the trailing tab.

## `npu_module_mem`

24-byte records: `moduleId@0`, `syscnt@8`, `totalSize@16`. Timestamps use the
**host counter** (`GetTimeFromHostCnt`) with the device wall offset, drop rows
before collection start, sort by `(moduleId, timestamp)`.
The module-id table is NOT the C++ `MODULE_NAME_TABLE`: golden uses
`0 = SLOG`, `2 = SCC`, `5 = CCU`, `9 = HDC`, `10 = DRV`, `23 = KERNEL`,
`34 = TS`, `37 = LP`, `43 = IMU`, `44 = IMP`, `47 = CAMERA`, …, unknown →
`Unknown`. Encoded in `tables::module_name`.

## `dvpp`

Text stream (`dvpp.data.*`, 12 whitespace-separated fields). Aggregate per
`(engine_type, engine_id, dvpp_id)`:

```
All Time  = max(all_time)  - min(all_time)
All Frame = max(all_frame) - min(all_frame)
Util      = last all_utilization (strip '%')
```

Sort by `(engine_type, engine_id)`; engine names from the VDEC/JPEGD/PNGD/JPEGE/VPC/VENC/SCD map.

## `l2_cache`

72-byte records `<HHHH8Q`: `stream_id@2`, `task_id@4`, 8 event counters `@8`.
V4 event positions (from `sample.json` `l2CacheTaskProfilingEvents`):
request `0xfb,0xfc`, victim `0x9c`; `Hit Rate` is always `N/A` on stars chips.
`Victim Rate = round(victim / request, 6)` with Python round-half-even.
Op name joined from the task table by `(stream, task)`.
Victim float formatting matches Python `str(float)` (`0.0`, `0.033333`, `2e-05`).

## `tscpu` → `ts_cpu_pmu_events`, `ts_cpu_top_function`

304-byte perf records `L L 20Q L L Q Q 14Q`, accepted only when
`header == 0xabababab`; `pc@176`, 7 × `(event, count)` pairs at `@192`.
Events between `0x0` and `0xec`; sample config `ts_cpu_profiling_events`.

- `ts_cpu_pmu_events`: sum counts per configured event; names
  `0x8 → Inst_retired`, `0x11 → Cpu_cycles`; sort by count **string** desc.
- `ts_cpu_top_function`: sum `0x11` per `pc`, rate `round(cycles*100/total, 3)`,
  top 5 by cycles with **stable first-occurrence** tie-breaking (matches the
  golden order when many functions share the same cycle count).

## New CLI surface

`npu-mem`, `npu-module-mem`, `dvpp`, `l2-cache`, `ts-cpu-pmu-events`,
`ts-cpu-top-function`, each with a matching `verify-*` command.

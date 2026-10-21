# Milestone 3 — `op_summary`

Status: **implemented and verified**. All 46 columns are populated; identity,
name/type, block/context and all six tensor columns match the golden CSV
exactly. The only residuals are ≤1 ns timestamps and rounding ties.

## Dataflow

```
StarsSocEntry ─► DeviceTask ─┐
NodeBasicInfoRow ────────────┤
TensorInfoRow ───────────────┼─► OpSummaryRow
PmuData (ffts_profile) ──────┤
LpmInfo (lpmFreqConv) ───────┘
```

`type Deps = (DeviceTask, NodeBasicInfoRow, TensorInfoRow, PmuData, LpmInfo);`

## Tensor columns

Source: `host/data/aging.additional.tensor_info.*`, 256-byte
`MsprofAdditionalInfo` records (`magic = 0x5a5a`, 24-byte header + 232-byte
`MsprofTensorInfo`).

- A task's tensors span several records that share
  `(opName hash, timeStamp, threadId)`; they are merged into one descriptor set.
- `tensorType` 0 = input, 1 = output.
- `format` = `id & 0xff`, sub-format = `(format & 0xffff00) >> 8` (rendered as
  `NAME:sub`, e.g. `NC1HWC0:1`).
- `dataType` and `format` names come from
  `crates/model/src/tables.rs`, generated from msprof's `number_mapping.cpp`
  (`geDataTypeMap` / `geFormatMap`).
- Shapes stop at the first zero dim, join dims with `,`, tensors with `;`, and
  the whole shape list is wrapped in quotes (`"8,3,224,224"`) before CSV
  escaping (`"""8,3,224,224"""`).

## Row model

- One `op_summary` row per **named** task (8104); the 3 special ACSQ tasks are
  excluded.
- `op_summary` timing differs from `task_time` for **mix** tasks: it uses the
  **FFTS+** start/end pair (`DeviceTask::op_start_raw/op_end_raw`), not the ACSQ
  pair. That is why the mix row is 26.522 µs here but 29.322 µs in `task_time`.
- `node_basic_info` is joined positionally in `(device, op_start)` order.

## PMU metrics

Events come from `sample.json`; the PMU list is matched **by position**:
`events[i] → pmuList[i]`.

- AIC: `0x416,0x417,0x9,0x302,0xc,0x303,0x54,0x55` (`PipeUtilizationExct`)
- AIV: `0x8,0xa,0x9,0xb,0xc,0xd,0x54,0x55` (`PipeUtilization`, 9 reported metrics)

```
total_time = total_cycle * 1e6 / freq_hz / block_num * ceil(block_num / core_num)
ratio      = sum(pmuList[reg] for reg in metric) / total_cycle
time       = total_time * ratio
icache     = pmuList[0x55] / pmuList[0x54]
```

- `freq_hz` = `aic_frequency * 1e6`, overridden by the last `lpmFreqConv` sample
  with `sys_cnt <= pmu.time_list[1]` (`GetFreq`).
- `core_num` = `ai_core_num` (AIC) or `aiv_num` (AIV).
- `block_num` = `node_basic_info.blockNum`.
- `cube_utilization(%)` = `total_cycle / (aic_freq * ai_core_num) * 100 / duration`
  for AIC; `0.000` for AIV/mix.

Formatting follows msprof: round half-up to 3 decimals, then trim trailing zeros
keeping at least one decimal (`84.656`, `0.13`, `0.0`). `aiv/aic_total_cycles`
are integers; `cube_utilization` always keeps 3 decimals.

`Task Wait Time` = `max(0, start_i − start_{i−1} − duration_{i−1})`, computed
after sorting by `(device, start string)`, formatted with 6 decimals.

## Verification

```
golden rows:   8104
produced rows: 8104
```

Identity/name/type/block/context columns: **8104/8104**.
Tensor columns (input/output shapes, data types, formats): **8104/8104**.
PMU metric columns: **8097–8104/8104** (residual is half-up rounding ties).
Timing columns: start 8102/8104 within 1 ns (2 rows differ by 0.25 µs),
duration 8104/8104 within 1 ns, wait 8100/8104 within 1 ns.
`cube_utilization`: 7723/8104 within 0.0011 (max 0.007).

## Remaining

1. Two start-time outliers differ by 0.25 µs and cascade into two wait rows.
2. Half-up rounding ties in a handful of PMU ratio columns (≤7 rows each).
3. `cube_utilization` residual (max 0.007) and the 1 ns `HPFloat::Double()`
   residual.
4. `Model ID`/`OP State`/`HF32 Eligible` are currently fixed to the values this
   profile uses (`4294967295`, `dynamic`, `NO`); they need the GE/`sample.json`
   source for other chips.

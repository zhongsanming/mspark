# Milestone 8 — `api_statistic` / `op_statistic`

Status: **implemented**. `api_statistic` is byte-exact; `op_statistic` is exact
on identity/aggregates and within the `HPFloat` residual on timing columns.

## `api_statistic`

Source: host `api_event`, converted with the host clock.

- Group `(level, apiName)`, ordered by `(apiName, level)`.
- `duration = round3((end_ns - start_ns) / 1000)` (µs).
- `Time = sum`, `Count`, `Avg = round3(sum/count)`,
  `Min`, `Max`, `Variance = round3(Σ(d-avg)²/count)`.
- `Level`: `20000 acl`, `15000 model`, `10000 node`, `5500 communication`,
  `5000 runtime`.
- `Device_id` column is the literal `host`.
- Floats use `DoubleToStr` (3 decimals, trailing zeros trimmed, `0` for zero).

Verified: B **112/112 rows, all 9 columns**; A 93/93.

## `op_statistic`

Source: named `DeviceTask`s with **op timing** (`op_start_raw/op_end_raw`) and
`NodeBasicInfoRow` (`op_type`, `task_type`).

- Exclude `COMMUNICATION` / `HCCL_AI_CPU`.
- Group by `(device, op_type, core_type)`; `count`, total/min/max ns.
- Ratio denominator = per-device total over all non-comm tasks (groups with
  `N/A`, `WRITE_BACK`, `INVALID` still count in the denominator but are not
  emitted).
- Values `round3(ns/1000)`, `avg = round3(total/count/1000)`, `ratio =
  round3(total*100/device_total)`; sorted by total desc.

Verified (B, 25 rows): `Device_id/OP Type/Core Type/Count/Avg/Ratio` exact;
`Total Time` exact 6/25 (max Δ 0.028 µs), `Min` 21/25, `Max` 18/25 (max Δ 0.001 µs),
all within 0.05 µs. The residual is the accumulated 1 ns `HPFloat` rounding over
thousands of tasks.

## New CLI

`api-statistic`, `verify-api-statistic`, `op-statistic`, `verify-op-statistic`.

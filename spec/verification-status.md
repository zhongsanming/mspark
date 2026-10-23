# Verification status (multi-device capture)

Sweep over the seven captures in `artifacts/mspark_prof`, each verified against
its own `mindstudio_profiler_output` goldens. Tracks not listed match exactly.

| device | task_time within 2ns | tracks not matching (matched/golden) |
|---|---|---|
| device_5 | 84914/85241 | NPU MEM 0/6714, CPU Usage 80877/81733 |
| device_3 | 84992/85319 | NPU MEM 0/5490, HBM 7391/7392 |
| device_2 | 84992/85320 | NPU MEM 0/6780, CPU Usage 82792/83540 |
| device_1 | 84992/85320 | SIO 3336/8928, Acc PMU 6704/17872, Stars Soc Info 832/2234, Stars Chip Trans 6704/17872, QoS 85180/227544, NPU MEM 0/6831, CPU Usage 95088/95783, HBM 9239/9240 |
| device_7 | 84990/85318 | SIO 2208/5880, Acc PMU 4420/11776, Stars Soc Info 540/1472, Stars Chip Trans 4416/11776, QoS 55914/149324, NPU MEM 0/4482, CPU Usage 64214/64860 |
| device_4 | 84989/85316 | SIO 3320/8912, Acc PMU 6696/17840, Stars Soc Info 836/2230, Stars Chip Trans 6640/17840, QoS 85232/227466, NPU MEM 0/6828 |
| device_6 | 84734/85058 | NPU MEM 0/6570, CPU Usage 92474/93318 |

## Reading the table

- **`task_time`** is within 2 ns on 84 734–84 992 of ~85 300 rows per device
  (99.5 %), matching the single-capture result. The residual is the task
  attribution issue (T1j), not clock conversion.
- **`NPU MEM` is 0/… everywhere** — the 17–25 ns timestamp difference (T1c) makes
  every signature mismatch. Values are correct; only `ts` differs.
- **`CPU Usage`** is 98.5–99.5 % everywhere (the `f64` vs `Decimal` rounding
  class, T21).
- **`HBM`** misses by a single row on two devices (rounding, 1 milli-unit).
- **`Overlap`** matched 0 on all seven — the two-thread vs four-thread gap (T26).
- **`SIO`, `Acc PMU`, `Stars Soc Info`, `Stars Chip Trans`, `QoS`** match exactly
  on devices 5, 3, 2 and 6, but only ~37 % on devices 1, 7 and 4. Counts are
  identical there. **Diagnosed (T27): it is not a value difference and not die
  aggregation.** For device_1's Acc PMU, the 4 468 golden and 4 468 produced
  samples carry *identical* values wherever their timestamps coincide
  (6 704 keys, 0 value differences), and the timestamp sets differ by exactly
  one unit everywhere:

  ```
  |Δt| = 0.001 us  on 11 168 rows
  |Δt| = 0.000 us  on  6 704 rows
  ```

  So this is the **1 ns `HPFloat` rounding** (T9), amplified because the SOC
  profile emits ~4.5 k dense samples: on devices where the counter value lands on
  a rounding boundary, every sample misses by 1 ns; on the others it happens to
  agree. Same class as `task_time`'s 2 ns residual and `npu_mem`'s 25 ns — which
  means T27 needs no separate fix, only the `HPFloat` port (T9).

## Caveat

`Overlap`, `Communication` (absent) and `NPU MEM` were previously reported on a
single capture only; the merged layout exercises paths that a single capture does
not, and one of those (dictionary loading) turned out to be broken for merged
profiles until `47d8bb0`.

# Milestone 6 — Overlap Analysis track

Status: **implemented and verified** on both reference profiles.

## Dataflow

```
DeviceTask ─► OverlapRow (Compute | Free)
```

## Algorithm

1. **Intervals** use the **op timing** (`op_start_raw`/`op_end_raw`), not the
   `task_time` timing. For mix tasks that is the FFTS+ pair, which is what the
   golden `Computing` blocks use (`25.662 µs` for task 384, not `28.102 µs`).
2. **Compute sections**: every named task (`raw taskType == 0`) becomes
   `[start, end)`; sort and union (`last.end >= next.start` merges).
3. **Window**: `begin` = min start and `end` = max end over tasks whose raw
   `taskType != 3` (i.e. keep `TASK_TIMEOUT_SET`, drop `PROFILING_ENABLE` /
   `PROFILING_DISABLE`), matching `UpdateTaskTimeExtremes` + `FILTER_TYPE`.
4. **Free sections** = `[begin, end] \ compute` (leading gap, inter-task gaps,
   trailing gap).
5. Events are emitted compute-first, then free:
   `{"name":"Computing"|"Free","pid":…,"tid":2|3,"ts":"<us>","dur":<double>,"ph":"X","args":{}}`
   with pid sort index `29` and thread metadata for tids 0–3.

## Verification

```
profile A: overlap golden=16208 produced=16208 matched=16208 max_dur=0.002
profile B: overlap golden=16208 produced=16208 matched=16208 max_dur=0.002
```

The 2 ns bound is the accumulated `HPFloat` rounding on interval start/end
(each timestamp is within 1 ns; a duration is a difference of two).

## Timeline coverage now

Ascend Hardware, CANN, SIO, Acc PMU, Stars Soc Info, Stars Chip Trans,
Overlap Analysis.

Remaining tracks: NPU MEM, HCCS, NIC, RoCE, PCIe, HBM, LLC, QoS (46k events).

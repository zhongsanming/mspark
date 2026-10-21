# Milestone 1 — `task_time` from raw device streams

Status: **implemented and verified against the golden CSV**. Host GE association is covered in `milestone-2-host-ge.md`.

## Command

The dev shell is `use flake`; if `cargo`/`cc` are not on `PATH` (e.g. after a
reboot), run through the flake:

```bash
PROF=/path/to/PROF_000001_...
GOLDEN=$PROF/mindstudio_profiler_output/task_time_*.csv
cargo run -q -- verify "$PROF" --golden "$GOLDEN"
```

## Dataflow

```
StarsSocEntry (raw 64-byte records)
  -> DeviceTask   (pair ACSQ start/end per stream; FFTS+ marks `mix`)
  -> TaskTimeRow  (wall-clock conversion, ordering, CSV row)
```

`DeviceTask` pairs **ACSQ** records only (`funcType` 0/1). FFTS+ records
(`funcType` 34/35) are the AIV sub-tasks of a mix task: they do not produce rows,
they only set `mix = true` on the owning ACSQ task (`sub_task_id == 0`). This is
what msprof's `LogModeling` folds into the ACSQ `DeviceTask`, and it is what the
golden `MIX_AIV` rows show.

## Clock conversion (`crates/model/src/time.rs`)

Inputs, per device directory:

| value | source |
| --- | --- |
| `collectionTimeBegin` (µs) | `start_info.*` |
| `clockMonotonicRaw` (ns) | `start_info.*` |
| `cntvct` | `dev_start.log.*` |
| `clock_monotonic_raw`, `cntvct_diff` | `host_start.log.*` |
| `hwts_frequency` (MHz) | `info.json` `DeviceInfo[0]` |
| `CPU[0].Frequency` (MHz) | `info.json` |

```text
host_mono  = clock_monotonic_raw + cntvct_diff * 1000 / host_freq
t_ns(raw)  = host_mono + (raw - cntvct) * 1000 / hwts_freq
wall_ns    = (collectionTimeBegin*1000 - clockMonotonicRaw) + t_ns(raw)
```

`DeviceTask` keeps raw ticks; `TaskTimeRow` converts start/stop once and computes
duration from the tick delta (avoids cancellation at ~1.8e18 ns).

msprof routes absolute timestamps through `double` (`HPFloat::Double()`), so its
last nanosecond is quantised. We reproduce the arithmetic shape; the residual is
≤1 ns per field.

## Verification (V4_1_0, 910B, `device_7`)

```
golden rows:   8107
produced keys: 8107
matched:       8107
missing:       0
extras:        0
task_time:  exact 7158/8107, within 2ns 8107/8107, max |Δ| = 1 ns
task_start: exact 7542/8107, within 2ns 8107/8107, max |Δ| = 1 ns
kernel_type exact: 62/8107
kernel_name exact: 3/8107
```

`kernel_type` is exact for all 60 `MIX_AIV` rows plus the `TASK_TIMEOUT_SET` /
`PROFILING_ENABLE` rows. The remaining 8044 rows are `AI_CORE` vs
`AI_VECTOR_CORE`, which is **not derivable from the device stream** (both come
from ACSQ with raw `taskType == 0`).

## Remaining gap (resolved in milestone 2)

`kernel_name` and the `AI_CORE`/`AI_VECTOR_CORE` split come from the host GE
association (`TaskInfoData`), i.e. the host-side id→text context:

- `host/data/unaging.additional.hash_dic.*` — GE hash → op name
- `host/data/unaging.additional.type_info_dic.*` — type id → type name
- `host/data/aging.compact.node_basic_info.*` — task → hash / static graph info
- `host/data/aging.compact.task_track.*` — task → stream / task type

These are exactly the "parse id→text early into `Ctx`" sources; they will be
loaded in `Ctx` before any node runs, and `TaskTimeRow` will resolve
`kernel_name`/`kernel_type` from a `TaskInfo` map keyed by `(stream, task)`.

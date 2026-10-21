# Milestone 2 — host GE association for `task_time`

Status: **implemented and verified**. `task_time` now matches the golden CSV on
every identity/name/type field, with ≤1 ns residual on timestamps.

## Dataflow added

```
StarsSocEntry ──► DeviceTask ──┐
NodeBasicInfoRow ──────────────┼──► TaskTimeRow
TaskTrackRow ──────────────────┘
```

`TaskTimeRow` now depends statically on three nodes:

```rust
type Deps = (DeviceTask, NodeBasicInfoRow, TaskTrackRow);
```

## Host id→text sources (loaded into `Ctx` before any node runs)

| file | format | use |
| --- | --- | --- |
| `host/data/unaging.additional.hash_dic.*` | text `hash:name` | GE hash → op name |
| `host/data/unaging.additional.type_info_dic.*` | text `level_type:name` | runtime task types (`level = 5000`) |
| `host/data/aging.compact.node_basic_info.*` | 64 B `MsprofCompactInfo` | op name hash + GE task type |
| `host/data/aging.compact.task_track.*` | 64 B `MsprofCompactInfo` | task type for the special tasks |

`MsprofCompactInfo` = 24-byte header (`magic:u16 = 0x5a5a`, `level`, `type`,
`threadId`, `dataLen`, `timestamp`) + 40-byte union.

- `MsprofNodeBasicInfo`: `opName:u64`, `taskType:u32`, `opType:u64`,
  `blockNum:u32`, `opFlag:u32`, `opState:u8`.
- `MsprofRuntimeTrack` (V1): `deviceId:u16`, `streamId:u16`, `taskId:u32`,
  `taskType:u64`, `kernelName:u64`, `extInfo[16]`.

Both are decoded zero-copy with `zerocopy` (`raw::decode_node_basic_info`,
`raw::decode_task_track`).

## Association rules

1. **Named tasks** (`kernel_name != N/A`): `node_basic_info` is emitted in the
   same order as the named tasks sorted by start time, so the join is positional.
   Iterate tasks in `(device, start_tick)` order and consume one
   `NodeBasicInfoRow` per non-special task.
2. **`kernel_name`** = `HashDic[nbi.opName]`.
3. **`kernel_type`** = `geTaskTypeMap[nbi.taskType]`
   (`0 AI_CORE`, `2 AI_VECTOR_CORE`, `5 MIX_AIV`, …). Mix tasks (FFTS+ present)
   are forced to `MIX_AIV`.
4. **Special tasks** (raw ACSQ `taskType` 1/3: profiling enable/disable, timeout)
   are not in `node_basic_info`; their type comes from
   `task_track[(stream, task)] → type_info_dic[5000][taskType]`
   (`MSPROF_REPORT_RUNTIME_LEVEL = 5000`).

## Verification

```
golden rows:   8107
produced keys: 8107
matched:       8107   missing: 0   extras: 0
kernel_name exact: 8107/8107
kernel_type exact: 8107/8107
task_time:  exact 7158/8107, within 2 ns 8107/8107, max |Δ| = 1 ns
task_start: exact 7542/8107, within 2 ns 8107/8107, max |Δ| = 1 ns
```

The 1 ns residual is `HPFloat::Double()` (msprof converts absolute timestamps
through a decimal big-float whose `Double()` is a sequential digit sum). Every
field is within 1 ns; making the CSV byte-identical would require porting
`HPFloat`, which is a bounded but separate task.

## Next

- `op_summary` (8104 rows) from `ffts_profile` PMU + the same host GE join.
- `msprof.json` timeline (Ascend Hardware layer) from `DeviceTask` + `HalTrackData`.

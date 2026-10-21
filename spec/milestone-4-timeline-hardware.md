# Milestone 4 — Timeline report (`msprof.json`): Ascend Hardware + CANN

Status: **implemented and verified** for the Ascend Hardware and CANN host tracks.

## Dataflow

```
StarsSocEntry ─► DeviceTask ─┐
NodeBasicInfoRow ────────────┼─► TimelineTask      (Ascend Hardware X events)
TaskTrackRow ────────────────┘

ApiEventRow (api_event.data) ─► CANN host X events
```

## Ascend Hardware track

Pid: `GetFormatPid(pid, index, deviceId) = (pid << 10) | (index << 5) | deviceId`
(index `13`). Task `X` event:

```json
{"name":"<op>","pid":<hw_pid>,"tid":<stream>,"ts":"<us 3dp>","dur":<double us>,"ph":"X",
 "args":{"Model Id":4294967295,"Task Type":"AI_CORE","Physic Stream Id":2,
         "Task Id":0,"Batch Id":0,"Subtask Id":4294967295}}
```

- `tid` = stream; the 3 special ACSQ tasks use `tid = 0`.
- `dur` is the **double** (0.5 ns-quantised) duration, not the integer-ns CSV value.
- `Subtask Id` = `0` for mix tasks, `4294967295` otherwise.
- Metadata: `Ascend Hardware`, `NPU <id>`, `Stream <tid>`; two `AI Core Freq`
  counters from `start_info`/`end_info`.

Verified: 8107/8107 on name, tid, task type, stream id, task id, subtask id;
ts 7540 exact (rest ≤1 ns); dur 8056 within 1 ns.

## CANN host track

Source: `host/data/aging.api_event.data.*` — 40-byte `MsprofApi` records
(`magic`, `level`, `type`, `threadId`, `begin`, `end`, `itemId`).

Pid: `(pid << 10) | (7 << 5) | 31` (`LAYER_CANN_SORT = 7`, `HOST_PID = 31`).

- Events are sorted by `begin` (msprof emits the track in timestamp order).
- Host clock: `host/start_info` (`collectionTimeBegin`, `clockMonotonicRaw`),
  `host/host_start.log` (`cntvct`, `clock_monotonic_raw`, `cntvct_diff`), and
  `host/info.json` (`CPU[0].Frequency`):

  ```text
  host_mono = clock_monotonic_raw + cntvct_diff*1000/host_freq
  ts_ns     = (collectionTimeBegin*1000 - clockMonotonicRaw) + host_mono
              + (begin - cntvct)*1000/host_freq
  ```

- Level: `20000 → acl`, `10000 → node`, `5000 → runtime`, `15000 → model`.
- Name: `<prefix>@<apiName>`, prefix `AscendCL`/`Node`/`Runtime`/`Model`.
- `apiName = type_info_dic[level][type]`.
- `Mode`: ACL events use `aclApiTagMap[type >> 16]`
  (`3 ACL_RTS`, `5 ACL_NN`, `6 ACL_ASCENDC`, …); other levels use the API name.
- `id`: ACL = API name; otherwise `"0"`.
- `item_id`: node level = `hash_dic[itemId]` (the op name); otherwise `"0"`.

Verified: **33488/33488** on name, tid, ts, dur, Mode, level, id and item_id.

## Output

`./out/timeline.json` — 41,613 events (41,595 X events + metadata/counters),
valid JSON.

## Remaining

1. **Flow events** (`ph: s`/`f`, `cat: HostToDevice`) and `connection_id`: these
   use msprof's `IdPool`/`ConnectionIdPool` synthetic counters, which are seeded
   at runtime and not derivable from the raw data.
2. **Overlap Analysis** (`Computing`/`Free`) track.
3. One-nanosecond `HPFloat` residual on timestamps.

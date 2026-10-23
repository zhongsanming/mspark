# TODO / backlog

Status snapshot (dataset B):

- **Tracks exact (9/16):** CANN, Ascend Hardware, Overlap Analysis,
  Stars Chip Trans, Acc PMU, SIO, QoS, NPU MEM, Stars Soc Info.
- **CSVs exact (12/17):** api_statistic, dvpp, hbm, hccs, l2_cache, npu_mem,
  npu_module_mem, op_summary, task_time, ts_cpu_pmu_events,
  ts_cpu_top_function, op_statistic (identity/aggregates exact; timing within
  the HPFloat residual).

---

### T1. Device monotonic-ns → wall timestamp — **SOLVED**
Verified against a fresh V4 profile with its intermediate `msprof_*.db`:

```text
wall_ns = raw_ns + (host_mono_ns - dev_start_mono_ns) + (begin_us*1000 - start_info.clockMonotonicRaw)
```

- 417 raw `hccs` rows → 411 `HCCS` DB rows; **every** `timestampNs` matches
  exactly (the first DB row is raw sample 1, because the model pairs each
  sample with its predecessor).
- Implemented as `MonoClock` / `load_mono_clock` in `crates/model/src/time.rs`.
- Dataset B's `hccs` stream is the outlier: its golden timestamps do not follow
  any affine map of its raw file, so it cannot be used to validate this.

Remaining work is mechanical: route `HbmRecord`, `HccsRateRow`, LLC, PCIe,
NIC and RoCE through `MonoClock` and emit their timeline tracks.

### T2. LLC throughput — **SOLVED**
```
hit   = round3(num/den) * 100          # emitted as a fraction (0.841)
tp    = (e0+e1) * 1024² * 1e9 / Δts_ns / 256 / 1024²   # rounded to 3
```
- The `256` is the missing unit factor (my old value was 256× too large).
- The **first sample is emitted** with `tp = 0` (the model's wrap-around branch),
  which is also why golden has 415 samples for 416 timestamps.
- Verified: LLC track 1660/1660 on the fresh profile.

### T1d. µs-sampling timestamps — **SOLVED** (`SamplingClock`)
`ts_ns = int(raw_us*1000 + start + delta*1e9 + offset_us*1000)` evaluated in
**f64 left-to-right**, exactly like msprof. The f64 order matters: at ~1.79e18 ns
one ulp is 256 ns, and exact integer arithmetic lands 129 ns off.

Applied to `HbmRecord` and `LlcRecord`. Remaining consumers: NIC, RoCE.

### T1f. PCIe raw fields are in MB
`pcie.data` stores MB, the DB scales by `1e6`; the track then divides by `1024²`
**without rounding** (only the CSV rounds). HCCS track values are rounded to 3
decimals; HBM track values are not.

### T1e. Verifier matches tracks by name
The pid encoding `(pid<<10)|(sort<<5)|dev` is **not** stable across chips/results;
`verify-timeline` now resolves each golden track's pid from its
`process_name`/`process_labels` metadata and matches by name.

### T1b. `kernel_name` source — **SOLVED (host-first)**
`task_time_assembler.cpp` resolves the name as
`HostTask.kernelName` **first**, falling back to the device op name, keyed by
`(streamId, batchId, taskId, contextId, deviceId)`; the device task type wins for
`kernel_type` when it is not `N/A`.

- The host name comes from the **raw** host stream: `aging.compact.task_track`
  carries a `kernelName` hash at union offset 16, resolved through
  `unaging.additional.hash_dic` — no sqlite is involved. `task_track.rs` now
  decodes it (`TaskTrackRaw::kernel_name`, `TaskTrackRow::kernel_name`).
- `TaskTimeRow` and `TimelineTask` now prefer that name.
- The raw `taskId` can pack `batchId` in its high 16 bits
  (`ascend_obj.h`: "采集侧的高低16位会被分别用作 batchId 和 TaskId"). On both
  profiles here every record has `batchId == 0`, so `(stream, task)` is unique —
  keep the batch split in mind if collisions appear.

Result: the fresh profile goes from **3/54 981 → 54 917/54 981** exact
`kernel_name`.

Caveat: dataset B's `kernel_name` now diverges (8107 → 3). B's export predates
this preference — its golden uses the device op name for a task whose host
dictionary already resolves to `TransData_590f…`. Everything else about B is
unchanged (timings, `kernel_type`, `op_statistic`).

### T1g. MIX tasks report the AIV window in `task_time` — **done**
For a mix task msprof's `task_time` uses the FFTS+ (AIV) pair, not the whole
ACSQ op window; using ACSQ made 820 rows up to 3 320 ns too long. With the switch
the fresh profile is **within 2 ns on all 54 981 rows**.

Side effect: dataset B drops from 8107 to 8047 rows within 2 ns — the older
release did not do this.

### T1h. `op_statistic` durations on the fresh profile — **open**
Counts match exactly (e.g. `TransData,AI_VECTOR_CORE,21115`) but the totals do
not: golden 398 325.521 µs vs ours 325 989.111, and the minimum is 3.941 vs 0.56.
So the per-op duration `op_statistic` aggregates is *not* the `task_time`
duration. Confirmed semantics (reproduces the golden CSV byte-for-byte from the
`msprof_*.db` we are allowed to *read for reference*, never to consume):

```sql
select c.opType, count(*), sum(t.endNs - t.startNs)/1000, min(...), max(...)
from TASK t join COMPUTE_TASK_INFO c on t.globalTaskId = c.globalTaskId
group by c.opType
-- TransData: n=21115 total=398325.521 min=3.941 max=142.863  (== golden)
```

So `op_statistic` = `TASK.startNs/endNs` grouped by `COMPUTE_TASK_INFO.opType`.
Only MIX keys have two `TASK` rows (820 of 54 981), so the extra length is not a
duplicated AIV row either: for an AIV-only task our ACSQ window is 0.56 µs while
`TASK` says 3.941 µs. That means msprof's **ACSQ start/end pairing differs from
ours** (we zip sorted starts with sorted ends per stream — likely it pairs by
`task_id`). Tried and rejected: taking the min/max span over *all* FFTS+
sub-tasks (no change). Mirrored `LogModeling::MergeStartAndEnd` (`device_task.rs`): starts/ends are now
grouped by `(streamId, taskId)` instead of by stream, and mismatched counts go
through `MergeStartAndEndByQueue` instead of dropping the stream. Behaviour was
unchanged on both profiles (each stream carries one task at a time), so the
pairing is **not** the cause — but the port is now faithful.

Remaining suspects for the 0.56 vs 3.941 µs gap: `TASK` may be built from the
AIV/`ts_track` records rather than ACSQ for vector-core ops, or `contextId`
(which our `Acsq` record does not carry) participates in the key.

### T1i. `kernel_type` — distribution fixed, rows permuted
The `mix` flag must **not** override the core type: golden takes it from
`node_basic_info.task_type` (0 → AI_CORE, 2 → AI_VECTOR_CORE, 5 → MIX_AIV).
With the override removed our histogram matches golden exactly
(45 721 / 8 405 / 820 / 32 MEMCPY_ASYNC), so the mapping is right.

What remains is **row assignment**: `node_basic_info` carries no task id, so we
pair it positionally with the tasks, and ~35 extra tasks shift everything after
them (≈16 k rows get a neighbour's type). Tried: nbi timestamps are in a
different domain (5.34e14) from task starts (1.79e18), so they can't join.
Proper fix: parse the device op-info stream that carries
`(streamId, batchId, taskId, contextId)` — msprof's `TaskInfoData`.

Dead ends checked:
- `ffts_profile` `w0.taskType` (6 bits) is **always 0** on this profile, so the
  core type cannot come from there.
- The profile contains no op-info stream with task ids; the only unparsed host
  streams are `unaging.compact.stream_sq_info` and
  `aging.additional.context_id_info`, which are the remaining join candidates.

### T1c. `npu_mem` timestamps ~25 ns off
Values exact (816/816), timestamps differ in the sub-µs digits
(`…261.785` vs `…261.760`) — the `HPFloat`/Decimal rounding class (see T9).

## Blocked — traced to a dead end in the source

---

## Not started

### T22. `Communication` track + `communication_statistic.csv`
The multi-device capture adds both. Shapes, from the golden and from
`prof_struct.h`:

**Track** — process `Communication`, label `NPU <dev>`, sort **16**,
pid `(host_pid << 10) | (16 << 5) | dev`. 41 562 events for device 5:
40 736 `ph: X` plus 826 `ph: f` flow events. Threads are named per group
(`Group <group> Communication`, tid 0) and per plane (`Plane <n>`, tids 1..4).
X-event `args`: `notify_id, duration estimated(us), stream id, task id,
context id, task type, src rank, dst rank, transport type, size(Byte),
data type, link type, bandwidth(GB/s), model id`.

**CSV** — `Device_id, OP Type, Count, Total/Min/Avg/Max Time(us), Ratio(%)`,
grouped by op type (`hcom_broadcast_`, `hcom_allReduce_`, …).

**Raw sources** (both are host streams, both already in the capture):
- `aging.compact.hccl_op_info` — 64-byte compact records;
  `MsprofHcclOPInfo` at data offset 24: `relay:1|retry:1` (byte 6),
  `dataType` (byte 7), `algType` (u64), `count` (u64), `groupName` (u64).
  Python: `msparser/compact_info/hccl_op_info_bean.py`.
- `unaging.additional.hccl_info` — 256-byte additional records;
  `MsprofHcclInfo`: `itemId, cclTag, groupName, localRank, remoteRank,
  rankSize, workflowMode, planeID, ctxID, notifyID, stage, role,
  durationEstimated(double), srcAddr, dstAddr, dataSize, opType, dataType,
  linkType`.

Calibration already done:

- `aging.compact.hccl_op_info` has **826 records**, exactly `COMMUNICATION_OP`'s
  row count — so that stream *is* the op table's source.
- Record prefix is `magic 0x5a5a, level 10000, type 10, thread, data_len 26,
  timestamp`; the hashes resolve through `unaging.additional.hash_dic`:
  `5862276093215481612 -> group_name_0`, `8452606241940441763 -> MESH-RING-NHR`.
- Layout **confirmed** by locating the dictionary hashes inside the record:
  `byte 24 = flags (relay|retry)`, `byte 25 = dataType`, `byte 26 = algType (u64)`,
  `byte 34 = count (u64)`, `byte 42 = groupName (u64)`. A record with
  `(dtype 4, alg MESH-RING-NHR, count 3847744, grp group_name_0)` matches the
  `COMMUNICATION_OP` row exactly. (The earlier "count = 64" came from comparing
  *different* ops — records are not in DB order.)
- `opName`, `opType`, `startNs`/`endNs`, `connectionId`, `opId` and `rankSize`
  are **not** in this stream; they must come from the task-info join.
- `COMMUNICATION_TASK_INFO` has **39 910 rows** vs 40 736 `ph: X` events, so the
  task table is the track's source.

Not started: the op→task join (`opId`/`notifyId`) to place tasks under
groups/planes, the flow-event generation, and the CSV aggregation.
`COMMUNICATION_OP` / `COMMUNICATION_TASK_INFO` in the reference DB show the
expected joins and are usable as ground truth without being consumed.

### T21. `host_cpu_usage.csv` + `CPU Usage` track — algorithm found
`host_cpu.data` is **text**: repeated dumps of `/proc/<pid>/stat` with an
`uptime` line. msprof parses it in
`analysis/host_prof/host_cpu_usage/` (`model/cpu_time_info.py` +
`presenter/host_cpu_usage_presenter.py`); the analysis C++ only reads the
resulting `host/sqlite/host_cpu_usage.db`.

Structure, per block:

```
time <sample_ts_ns>
<uptime_seconds> <idle>
<pid> (<comm>) <state> ... # per sampled process
```

Fields are `line.split()` indices: **0 = pid, 3 = tid, 13 = utime, 14 = stime,
38 = cpu_no**. `curr_jiffies = Decimal(uptime) * clk_jiffies` (`CpuInfo =
(num=192, clk_jiffies=100)`).

Per consecutive pair of blocks (all `Decimal`, quantized to `USAGE_PLACES`):

```
delta        = curr_jiffies - last_jiffies
proc_jiffies = min((utime+stime)_curr - (utime+stime)_last, delta)
usage        = (proc_jiffies * 100 / delta).quantize(...)
cpu[proc.cpu_no] += proc_jiffies                       # then
cpu_usage    = (cpu[cpu_no] * 100 / delta).quantize(...)   # skipped when > 100
avg          = (sum(proc_jiffies) * 100 / (delta * cpu_num)).quantize(...)
row          = [last_ts, curr_ts, cpu_no | 'Avg', usage]
```

26 slices, 1 153 blocks, the first block of each slice repeating the previous
slice's stamp → 1 127 distinct samples (`CPU Avg` count ✓).

Artifacts:
- track `CPU Usage` (label `CPU`, sort 9): counters `CPU <n>` / `CPU Avg`,
  `args: {"Usage(%)": f64}`;
- CSV: `Total` = 192, `Occupied` = distinct cpu_no excluding `Avg` (148),
  `Recommend` = those with `max(usage) > 0` (92).

Open: the host-domain timestamp. `trans_into_local_time(ts_ns, is_host=True)`
should be `ts_ns/1000 + local_time_offset`, but the golden's first stamp
(`1792632287147377.840`) is ~3.9 µs below that, so `_host_local_time_offset`
needs the same treatment as the NIC offset (exact decimal, not f64).

### T3b. `llc_read_write.csv` — **done**
Per bank: `hit = round3(mean(row_hit)*100)`, `tp = round3(mean(MiB/s))`;
`Average` = mean of the *unrounded* lane means. Exact on the fresh profile
(3/3 rows, 5/5 columns); on dataset B the throughput differs by ≤0.006 MB/s
(old capture, same divergence as T1b).

### T3. PCIe — CSV **done**, track blocked
`pcie_*.csv` is byte-exact (7/7 rows, all 5 columns). The recipe:

- raw `Q22I` (96 B) × 239 records; V4 reports the **newest sample only**
  (`order by timestamp desc limit 1`), so no aggregation over time.
- fields 2..22 are `min/max/avg` triplets for tx_p, tx_np, tx_cpl,
  tx_np_latency, rx_p, rx_np, rx_cpl.
- bandwidth `= v * 1e9/1000/1024²`, latency `= v/1000`, both `round(…,3)`;
  the sentinel `0xFFFFF` maps to `0.0`.

The 916-event PCIe **track** still needs T1 (event timestamps).

### T4. NIC / RoCE — **tracks done**, CSVs open
New `NetSample` / `NicRow` / `RoceRow` nodes parse `nic.data` / `roce.data`
(`time_stamp(s):time_stamp(ns)` + 14 counters, bandwidth in MB/s) and use
`MonoClock`. Tracks are exact on the fresh profile (NIC 168/168,
RoCE 170/170).

Still open: the one-row CSVs. Their `Timestamp(us)` column is **not** any
conversion we can derive — it is ~48 s below `MonoClock`, and
`begin_ns - cmr` is ~8.3 s away, so it comes from a different host-counter
domain (cf. `trans_from_start_info_raw_time_into_host_cnt`). Everything else in
the row (bandwidth 200000, all rates 0, funcId 0) is known.

### Reference profiles and their msprof versions
- **dataset B** (the original capture) — produced by an **older msprof
  release**. Still the reference for stable artifacts (timings, `op_summary`,
  `api_statistic`, memory tables), but *not* for anything whose behaviour
  changed since: `kernel_name` precedence (T1b) and all timestamped device
  tracks (T1).
- **a fresh `PROF_*` capture** — current release,
  the authoritative reference for everything.

### Dataset B is not a reference for timestamped tracks
B's device clock is internally inconsistent (its `hccs` raw stamps follow no
affine map, see T1), so NIC/RoCE/LLC/HCCS *tracks* no longer match B while the
fresh profile matches them exactly. B remains valid for timings
(`task_time`, `op_summary`) and for CSVs without timestamps.

### T5. HBM track (1 800 events)
Values already exact for the CSV; needs T1 for event timestamps.

### T6. LLC track (900 events)
Hit rate exact, throughput pending T2, timestamps pending T1.

---

## Quick win

### T7. `AI Core Freq` track — **done**
Already emitted (two counter events, sort index 14, `MHz` from the device
info); confirmed against the golden timeline.

---

## Framework

### T20. serde trace writer — **done**
`TraceEvent` + ordered `Args` replace the hand-built JSON `String`; `jstr`,
`push_obj` and the `first` comma flag are gone. This is the precondition for
T14 (per-device `Vec<TraceEvent>` instead of one shared buffer).

### T8. Compile-time graph validator — **done**
Detect dependency cycles at compile time instead of monomorphisation recursion
errors. Also emits the static graph for docs.

### T9. `HPFloat` port — **deferred (won't do for now)**
Owner decision: residual magnitudes (1 ns, ≤0.028 µs) are acceptable, so this is
not on the near-term roadmap. Kept here only to explain the remaining
"near-exact" columns in the verifiers.

### T10. Streaming / memory
Large datasets currently materialise every row per node. Consider chunked sinks
for the 50 k-event tracks.

---

## Features / parity

### T11. `--type=db` — **wontfix**
Owner decision: no DB output feature.

### T12. `export` verb + `--outputs` selection
Owner decision (see T18): replaces the separate `--reports` idea. Selects which
artifacts to emit from a comma-separated list.

### T18. Root-level export — **done**
No `export` subcommand: exporting is the default behaviour when a `PROF_*`
path is given.

```
mspark <PROF> [--outputs a,b,c] [--out-dir DIR] [--flat] [--device device_7]
```

Tokens: `all`, `csv`, `timeline`, or any report name; unknown token fails with
the valid list. Verify verbs stay as subcommands.

### T18b (was the `export` verb idea)
`mspark export <prof> --outputs task_time,op_summary,timeline --out-dir <base>`

- Token vocabulary: report names, plus family aliases (`csv`, `timeline`, `all`).
- Unknown token → hard error listing the valid names (unlike id→text, which is
  lenient by design).
- Shares one writer per artifact with the existing single-artifact verbs; those
  stay for verification and one-off use.

### T19. Timestamped output directory — **done**
`export` writes into `<base>/mspark_<YYYYmmdd_HHMMSS>/` with stable file names
(`task_time.csv`, `timeline.json`, …) and prints the directory at the end.

- Needs an escape hatch for CI/diffing: `--flat` writes directly into `<base>`.
- Verifiers keep reading golden paths, so they are unaffected.

### T13. Multi-device aggregate + `summary` CSV — **done**
`summary.csv` carries one row per device plus a `Total` row
(`Task Count`, `OP Count`, `Timeline Events`, `Peak Memory(KB)`); counts sum,
memory takes the max across devices.

### T14. Device-level parallelism — **done**
`timeline()` builds one `Vec<TraceEvent>` per device via `rayon` and joins the
chunks in device order; output is byte-identical to the sequential run.
Artifact-level fan-out: `export()` runs the selected artifacts in parallel
(each writes its own file, own `Store`), so a full export uses all cores even
on a single-device profile.

Per-device chunking in the CSV writers: `per_device()` runs a closure once per
device in parallel and joins chunks in device order. Applied to the four heavy
writers (`task_time`, `op_summary`, `l2_cache`, `npu_module_mem` — all verified
byte-identical). The remaining writers are small enough (≤ 460 rows) that they
still loop sequentially.

---

## Quality

### T15. Golden-fixture test harness
Move the verifiers into `cargo test` with checked-in golden CSVs/timelines so
`cargo test` gates regressions.

### T16. Verify-all command
One command that runs every verifier and prints the summary table.

### T17. Docs
- `spec/README.md` index (milestones 1–9 + this TODO).
- Architecture note describing the `Node`/`dep` model and the row-type-per-node
  convention, with the `Ctx` id→text policy (unknown → `N/A`/`Unknown`).

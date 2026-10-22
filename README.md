# mspark

A Rust reimplementation of the MindStudio Profiler (`msprof`) **analysis**
pipeline: it turns raw Ascend profiling captures (`PROF_*` directories) into the
same user-facing artifacts — statistics CSVs and a Chrome-trace timeline — that
`msprof --export=on` produces.

```
PROF_*/device_N/data/*.data     ──▶  mspark  ──▶  task_time.csv, op_summary.csv,
PROF_*/host/data/*              (dataflow)        api_statistic.csv, …, timeline.json
PROF_*/device_N/*.log, *.json
```

---

## Design: a dataflow graph resolved at compile time

**A node *is* the row type it produces.** Implementing `Node` for `T` declares
its dependencies as a tuple of node types and how to compute the whole table:

```rust
impl Node for TimelineTask {
    const NAME: &'static str = "TimelineTask";
    type Deps = (DeviceTask, NodeBasicInfoRow, TaskTrackRow);

    fn compute(ctx: &Ctx, (tasks, nbis, track): Deps<Self>) -> Result<Vec<Self>> { … }
}
```

`resolve::<T>()` walks that statically-known graph, computes each node at most
once (memoised per `TypeId` in a `Store`) and returns `Arc<Vec<T>>`. There is no
runtime registry and no runtime graph discovery: edges, ordering and type
matching are fixed at monomorphisation time. Failure is allowed at every step —
a failed node propagates `anyhow::Error` with the node name attached.

Two invariants keep nodes honest:

- **`Ctx` holds every id→text mapping** (`hash_dic`, `type_info_dic`, `qosEvents`,
  device info, clocks). Nodes never resolve raw ids themselves; an unknown id
  becomes a placeholder (`N/A` / `Unknown`) rather than a hard error.
- **The graph is validated at compile time.** `mspark_df::graph!(…)` in
  `crates/model/src/lib.rs` derives each node's edges from its `Deps` tuple and
  const-evaluates an acyclicity check, so a dependency cycle is a build error:

  ```
  error[E0080]: evaluation panicked: dependency cycle detected in node graph
  ```

### Layout

```
src/main.rs                 CLI (palc) + the artifact writers
crates/df/                  the framework: Ctx, Node, DepList, Store, resolve, graph!
crates/model/
  raw/                      zerocopy decoders for the packed device/host records
  time.rs                   the three clock conversions (see below)
  tables.rs, format.rs, pmu.rs, host.rs, ctx.rs
  nodes/                    one module per node (+ nodes/sources/ for graph roots)
spec/                       per-milestone notes + TODO backlog
```

### Clocks

Three distinct conversions were reverse-engineered and verified against a
reference capture; picking the wrong one is the single easiest way to get
plausible-looking but wrong output:

| stream kind | rule |
|---|---|
| hwts counters (`stars_soc`, `ffts_profile`, …) | `wall = offset + (raw − dev_cntvct)/hwts_freq · 1000` |
| monotonic-ns streams (`hccs`, `qos`, `nic`, `roce`) | `wall = raw + (host_mono − dev_mono) + (begin − cmr)` |
| µs sampling stamps (`hbm`, `llc`) | `int(us·1000 + start + delta·1e9 + offset·1000)` **in f64** — the order matters |

---

## Usage

```bash
nix develop          # or put a Rust toolchain on PATH
cargo build --release
```

Export everything into a timestamped directory (the default action):

```bash
mspark /path/to/PROF_000001_…                       # → ./mspark_YYYYMMDD_HHMMSS/
mspark /path/to/PROF_… --out-dir ./out --outputs task_time,timeline
mspark /path/to/PROF_… --out-dir ./out --flat       # no timestamp dir (CI/diffing)
mspark /path/to/PROF_… --device device_7            # single device only
```

`--outputs` accepts `all` (default), `csv`, `timeline`, or any report name
(`task_time`, `op_summary`, `api_statistic`, `op_statistic`, `npu_mem`,
`npu_module_mem`, `dvpp`, `l2_cache`, `ts_cpu_pmu_events`, `ts_cpu_top_function`,
`hccs`, `hbm`, `llc_read_write`, `pcie`, `summary`). Unknown names fail with the
valid list.

Artifacts are written concurrently, and devices are processed in parallel with
the chunks joined in device order, so output is deterministic.

### Verification

Every artifact has a `verify-*` subcommand that compares against a golden file
produced by `msprof --export=on`:

```bash
mspark verify        PROF_… --golden …/task_time_*.csv
mspark verify-timeline PROF_… --golden …/msprof_*.json
mspark verify-op-statistic PROF_… --golden …/op_statistic_*.csv
```

`verify-timeline` resolves each golden track's pid from its own
`process_name`/`process_labels` metadata (the pid encoding changes between
chips), then compares counter events as multisets.

---

## Status

Verified against a fresh V4 (910B) capture with the current `msprof`:
54 981 device tasks and 286 137 CANN API events.

**Exact**

| artifact | result |
|---|---|
| `task_time.csv` | all 54 981 rows within **2 ns** (50 963 exact) |
| `api_statistic.csv` | 114/114 rows, all columns |
| `npu_module_mem.csv` | 31 416/31 416 rows, all columns |
| `pcie.csv` | 7/7 rows, all columns |
| `hccs.csv` | 2/2 rows, all columns |
| `llc_read_write.csv` | 3/3 rows, all columns |
| timeline: QoS, HCCS, HBM, LLC, PCIe, NIC, RoCE | 1 660–81 476 events each, all matched |
| timeline: SIO, Acc PMU, Stars Soc Info, Stars Chip Trans, NPU MEM (values) | all matched |

**Close**

| item | delta |
|---|---|
| `kernel_name` | 54 917/54 981 |
| `hbm.csv` | 3 cells off by 1 milli-unit |
| `npu_mem.csv` timestamps | 17–25 ns (Decimal/`HPFloat` rounding) |
| `kernel_type` | distribution exact, ~16 k rows assigned a neighbour's type |
| `op_statistic.csv` | counts exact on B, durations differ on the fresh capture |

**Not implemented**

- `host_cpu_usage.csv` (`host_cpu.data`)
- `nic.csv` / `roce.csv` (tracks are done; the one-row CSVs need a different
  timestamp domain)
- multi-device aggregation is implemented but only exercised single-device

`spec/TODO.md` tracks each of these with the evidence gathered, including the
dead ends — several were investigated to the point of proving they are *not* the
cause, which is usually the expensive part.

## Publishing

Dependencies are hoisted into `[workspace.dependencies]` and the shared package
fields (`version`, `edition`, `rust-version`, `license`, `repository`, `readme`,
`keywords`, `categories`) live in `[workspace.package]`; every crate opts in
with `field.workspace = true`.

MSRV is **1.88** (`slice::as_chunks`).

The internal crates are referenced with both `path` and `version`, so publish in
dependency order — each step must be on crates.io before the next resolves:

```bash
cargo publish -p mspark-df
cargo publish -p mspark-model
cargo publish -p mspark
```

`cargo publish --dry-run -p mspark-df` passes end to end. The other two resolve
their internal dependency from the registry, so they only verify after the
previous step is published.

> `license = "GLWTPL"` is not an SPDX identifier, so crates.io may reject the
> upload. If it does, switch the workspace field to `license-file = "LICENSE"`.

## License

[GLWTPL](./LICENSE)

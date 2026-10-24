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

CSV verification runs the **production exporter** into a temporary directory and
compares that file against the golden one, so a verifier can never drift from
the writer. Columns are matched by header name (a reordered golden file is an
error, not a silent mismatch) and fields are read with a real RFC 4180 parser.

Byte-exactness is **not** a goal. What matters is that nothing gets *worse*, so
the fidelity gate is a ratchet over per-column closeness:

```bash
mspark verify-all PROF_…            # compare every artifact, fail on any regression
mspark verify-all PROF_… --update-baseline   # after a deliberate improvement
```

`spec/baseline.txt` records, for every artifact column, how many of its values
match the golden file (compared as multisets, numeric cells on a 3-decimal grid,
so ordering and float noise do not count). Any column that drops below its
recorded value fails the run — that catches a dropped row, a broken column or a
stream that stopped decoding, without demanding bit-identical output.

`--strict` on the individual verifiers still gives an exact match when you want
one:

```bash
mspark --strict verify-llc-csv PROF_… --golden …/llc_read_write_*.csv
```

### Project layout of the binary

Raw input is requested by **stream identity**, never by path: a node calls
`ctx.stream(Stream::StarsSoc)` and gets the bytes of that stream. The prefix, the
directory (device vs host) and the record size live once in `stream::Stream`, so a
node cannot disagree with the file it needs and a typo is a compile error instead
of an empty table. `Stream::trunk` also owns the versioned layouts (`hbm` v1/v2),
and a unit test keeps every prefix unique and non-overlapping per directory.

Each device is a `DeviceSession { ctx, info, store }`: the context (including the
parsed `StartLogs`, `sample.json` and host `info.json`) is built once per process
and the store memoises node tables for that device only.

`src/main.rs` holds the CLI and the artifact exporters; `src/lib.rs` holds the
parts that are worth unit-testing — `text` (Python-compatible float/rounding
helpers, the NIC timestamp column, µs parsing, civil dates) and `verify` (CSV
reader, comparison, strict gate). `cargo test` covers those plus the node graph
and the dataflow framework.

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
| `nic.csv` / `roce.csv` | 1/1 row, all 12 columns |
| `communication_statistic.csv` | OP Type / Count exact; time columns omitted (see TODO) |
| `host_cpu_usage.csv` | all 4 columns |
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

## Fidelity policy

The target is to **stay aligned with msprof's artifacts** — same files, same
columns, same rows, same semantics. Byte-exactness is *not* required: where a
value lands on a rounding boundary (f64 vs msprof's `Decimal`), being within a
handful of ULPs is acceptable, and such cases are noted in `spec/TODO.md` rather
than chased.

What *is* required is that nothing is silently invented: when we synthesise or
approximate something (a join we cannot reproduce, a rounding difference, a
format we reverse-engineered), it is written down here or in `spec/TODO.md`.

## Code-derived, unverified

A few artifacts are transcribed from msprof's implementation but have never been
exercised against real output, because our captures do not enable the relevant
collection flags:

| artifact | needs |
|---|---|
| `host_mem_usage` (shipped), `host_disk_usage`, `host_network_usage`, `os_runtime_statistic`, `cpu_usage` | `--host-sys=mem/disk/network/osrt` |
| `msproftx` | `--msproftx=on` |
| `step_trace` | iteration markers |

They are implemented only when the algorithm is unambiguous in the source. Treat
their output as untrusted until verified, exactly like the non-V4 chip rows.

## Chip support

Chip detection uses `platform_version` from `info.json`, mapping to the same
`ChipId` values as msprof's `chip_id.h`. An unknown value is a hard error rather
than a guess.

| chip | `platform_version` | status |
|---|---|---|
| `V4_1_0` (910B) | 5 | **verified** against two captures |
| `V1_1_0` / `V1_1_1` / `V1_1_2` / `V1_1_3` | 0 / 7 / 8 / 11 | ported, **unverified** |
| `V2_1_0` | 1 | ported, **unverified** |
| `V3_1_0` / `V3_2_0` / `V3_3_0` | 2 / 3 / 4 | ported, **unverified** |
| `V5_1_0` | 9 | ported, **unverified** |
| `V6_1_0` / `V6_1_1` / `V6_2_0` | 15 / 19 / 16 | ported, **unverified** |

> **These are not tested.** We only have access to V4 (910B) hardware, so every
> non-V4 row is transcribed from the reference implementation and has never been
> run against a real capture. Expect fixes to be needed; treat their output as
> untrusted until verified. Correctness details in this project were repeatedly
> decided by data (the `÷256` in LLC throughput, the AIV window for mix tasks,
> three distinct clock domains), so unverified paths *will* hide similar issues.

The stream layout version is selected from `info.json:version`, mirroring
msprof's `VersionedStructParser`:

| stream | 1.x | 2.x |
|---|---|---|
| `hbm` | `QQII` (24 B), µs stamp first | `IIIIQQ` (32 B), absolute-ns stamp last |
| `llc` | `QQII` (24 B), µs stamp first | `IIIIQ` (24 B), absolute-ns stamp last |

Collection version 1.x is what both of our captures use, so the 2.x layouts are
implemented but likewise **unverified**.

> **`nic.csv` / `roce.csv` `Timestamp(us)` quirk:** msprof's
> `peripheral_report.format_nic_summary` writes the *sample duration*
> (`max(timestamp) − min(timestamp)`) through
> `trans_into_local_time`, i.e. `duration/1000 + local_time_offset`, into the
> column labelled `Timestamp(us)`. We reproduce that verbatim, including the
> Decimal-over-f64-repr rounding, because the goal is to match msprof's output.

Still V4-shaped and needing per-chip work when data becomes available: the V6 QoS
variant (die-indexed series that msprof reads from `stars_soc_profile`, item type
`0x18`), SIO counter layouts, `l2_cache` event ids, the TSCPU 184-byte MDC
layout, DVPP engine tables, and the per-chip `ENUM_*` dictionaries.

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

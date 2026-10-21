# Milestone 5 — `stars_soc_profile` tracks

Status: **implemented and verified** on profile B. Four timeline tracks
(SIO, Acc PMU, Stars Soc Info, Stars Chip Trans) now match the golden JSON
exactly.

## Dataflow

```
StarsSocProfile (64-byte records, functype dispatch)
  ├─► SioSample          → SIO track
  ├─► AccPmuSample       → Acc PMU track
  ├─► InterSocSample     → Stars Soc Info track
  └─► ChipTransSample    → Stars Chip Trans track
```

`functype`: `25` SIO, `26` Acc PMU, `27` Chip Trans, `28` Inter-SOC.

## Decoding (offsets in the 64-byte record)

| item | fields |
| --- | --- |
| SIO | `sysCnt@8`, `accId@18`, 8×`u32` bandwidth `@32` |
| Acc PMU | `timestamp@8`, `accId@18`, `bandwidth[2]@32`, `ost[2]@48` |
| Chip Trans | `sysCnt@8`, `accType = w0>>10`, `eventId@18 & 0x3f`, `value1@32`, `value2@40` |
| Inter-SOC | `sysCnt@8`, `bwLevel@32` |

## Rules

- **Start-time filter**: drop records with `wall_ns < collectionTimeBegin*1000`.
  This is what turns 238→230 (SIO/Inter-SOC/Chip Trans) and 952→920 (Acc PMU).
- **SIO**: per-die delta rate
  `bytes * 1e9/1024² / (t_ns − prev_t_ns)` (truncated to `u32`), first sample 0;
  events grouped by `(timestamp, counter)` with `die 0`/`die 1` series.
- **Acc PMU**: sort by `(timestamp, accId)`, group by timestamp string taking
  the **max** of `[read_bw, read_ost, write_bw, write_ost]`; 4 counters per group.
- **Inter-SOC**: dedup by timestamp string; emit `L2 Buffer Bw Level` and
  `Mata Bw Level` (both integer).
  Note: msprof's golden output swaps the parser's field semantics — golden
  `L2 Buffer Bw Level` = `bwLevel & 0x7`, golden `Mata Bw Level` =
  `(bwLevel & 0x7f8) >> 3`. We match the golden.
- **Chip Trans**: `accType 63 → PA` (tid `111111`, Rx/Tx), `accType 34 → PCIe`
  (tid `100010`, Read/Write); dedup by `(timestamp, kind, id)`.

Pids use `(pid << 10) | (sort_index << 5) | device_id`, with sort indices
`27` SIO, `18` Acc PMU, `24` Stars Soc Info, `25` Stars Chip Trans.

## Verification (profile B)

```
track SIO              golden=1840  produced=1840  matched=1840
track Acc PMU          golden=3680  produced=3680  matched=3680
track Stars Soc Info   golden=460   produced=460   matched=460
track Stars Chip Trans golden=3680  produced=3680  matched=3680
```

Profile A has no `stars_soc_profile` stream; all four tracks are 0/0 there.

## Next

- CSV cluster: `l2_cache`, `npu_mem`, `npu_module_mem`, `dvpp`, `tscpu`.
- Overlap Analysis (derived).
- Dedicated tracks: QoS, PCIe, HBM, HCCS, LLC, NIC, RoCE.

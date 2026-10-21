# Milestone 9 — QoS track

Status: **implemented**, exact.

- Raw: `qos.data` 64-byte records — `u64 syscnt @16`, `u32 metrics[10] @24`.
- Timestamp: device syscnt → wall ns; drop records before `collectionTimeBegin*1000`.
  → 23,262 records (from 24,142 raw).
- Event names come from `device_*/sample.json` `qosEvents`
  (`"0:OTHERS,1:AIC_DAT_QOS"`), mapped 1:1 onto `metrics[0..]`.
- Emitted as counter events `QoS <name>` (tid 0, pid sort 28) with
  `args.value = metrics[i]`; two counters per timestamp → 46,524 events.

Verified: golden 46,524 / produced 46,524, all exact.

## Remaining dedicated tracks (surveyed, not yet ported)

| track | raw file | shape | notes |
|---|---|---|---|
| HCCS | `hccs.data` | text `ts s:ns, tx, rx` cumulative | rate = Δbytes / (Δt_ns / 953.6743) → MiB/s actually; 240 rows → 225 after begin filter |
| NIC / RoCE | `nic.data`, `roce.data` | text, 17 fields | 1 CSV row each (funcId 0), 455 ts × 2 ports |
| netdev | `netdev_stats.data` | text | feeds NIC/RoCE |
| HBM | `hbm.data` | binary, 4 HBM × Read/Write f64 MB/s | 225 ts × 8 = 1800 |
| LLC | `llc.data` | binary, 225 ts × 4 (2 banks × hit-rate/throughput) | 900 |
| PCIe | `pcie.data` | binary, 229 ts × 4 (cpl/nonpost/nonpost_latency/post) | 916 |
| NPU MEM | (npu_mem already parsed) | 230/229 ts × 3 × {APP,Device} | 1377 |

All six are Python-only in msprof (`analysis/msparser/hardware/*.py`); the C++
parsers for PCIe/LLC are present but their registrations are commented out.

## NPU MEM track (implemented, exact)

Built directly from `NpuMemRow` — no new decode. Per sample, three counters in
`DDR, HBM, Memory` order, named `{APP|Device}/<metric>`, `args.KB = bytes/1024`
as a float. pid sort 16, tid 0.

Verified: golden 1,377 / produced 1,377, all exact
(229 APP × 3 + 230 Device × 3).

## Pinned formulas for the six remaining tracks

- **HCCS** (`hccs.data`, text, header + `s:ns tx rx` cumulative):
  `rate = Δcount / (Δt_ns / (1e9/1024/1024))`, skipping rows where the counter
  went backwards; the last wrap pair is dropped. Units are MiB/s despite the
  `MB/s` label. CSV = `max/min/average` per `Tx`/`Rx`.
- **HBM** (`hbm.data`): v2 struct `IIIIQQ` (32 B) → `event_id, hbm_id, count,
  timestamp`; `bw = count * SCALE / Δt * 1e9` with
  `SCALE = 256/8/1024² = 0.000030517578125`; rows grouped by timestamp and
  filtered to the modal per-timestamp record count.
- **LLC** (`llc.data`): v2 struct `IIIIQ` (24 B); hit rate =
  `(e5+e6+e8+e10)/(e7+e9)`, throughput = `(e3+e4)/1*1024*1024/Δt_s`; two banks
  on V4.
- **PCIe** (`pcie.data`): `Q22I` (96 B), 229 samples × 4 counters
  (`cpl`, `nonpost`, `nonpost_latency`, `post`) with `{Rx, Tx}`.
- **NIC / RoCE** (`nic.data`, `roce.data`, text): 455 samples × 2 ports
  (`Port 0/Rx`, `Port 0/Tx`); all efficiency/packet/error counters are 0 in this
  dataset, the CSV is a single `funcId 0` row.

## HCCS — values exact, timestamp blocked

Rates from `rate = Δcount / (Δt_ns / (1e9/1024²))` reproduce the golden
`hccs_*.csv` **digit for digit**:

```
tx  max 280468.923  min 279833.384  avg 280146.737   (golden identical)
rx  all zeros
```

The **event timestamp** is still off. Facts established:

- `hccs.data` raw is a ns-domain monotonic counter (`2.9207e15`) in the **same
  domain as `start_info.clockMonotonicRaw`** (`2.920711854676820e15`).
- msprof's viewer does
  `ts_us = (raw_ns + delta_dev*1e9)/1000 + local_time_offset`,
  `delta_dev = host_mon - start_log_time/1e9`,
  `local_time_offset = collectionTimeBegin - collect_raw_time/1000`.
- With `start_log_time` = `dev_start.log clock_monotonic_raw` and
  `collect_raw_time` = `start_info.clockMonotonicRaw`, the result is a constant
  **199,973.31 µs** short of golden.
- Fitting golden-to-raw over all 225 points gives slope `0.999985` at the start
  but drifts to +89 ms by the end, i.e. it is **not** an affine map — the
  conversion re-synchronises, so a closed form from the sliced inputs alone
  isn't recoverable by fitting.

Unblock options, in order of preference:
1. Run upstream msprof once on this dataset and dump the intermediate
   `hccs.db` — the stored `timestamp` column is the authoritative value.
2. Or trace `InfoConfReader._host_mon` / `_start_log_time` initialisation for
   the device DB (there is a second path through `trans_from_start_info_raw_time_into_host_cnt`
   that we have not exercised).

Once that one mapping is known it applies to HCCS, HBM, LLC and PCIe together,
since they all share the same viewer-side conversion.

## CSV cluster for the hardware tracks

### HCCS — `hccs_*.csv` (exact)
`Device_id,Mode,Max,Min,Average`; `Max/Min/Average` of the rate series
(`HccsRateRow`), Python-style `round(v,3)` formatting. Verified 2/2 rows,
all 5 columns.

### HBM — `hbm_*.csv` (exact)
Raw v1 `QQII` (24 B) → `ts_us, count, event, hbm_id`; 237 timestamps × 8.
Per lane `(hbm_id, event)`:

```
value = count * (256/8/1024²) / Δt_ns * 1e9      Δt_ns = (ts[i]-ts[i-1]) * 1000
average = Σ values / N_timestamps                 (divide by timestamps, not intervals!)
```

`Average` row = mean of the four rounded lane values computed in **integer
milli-units** so the half-even tie at `184.9585` lands on `184.958`.
Verified 5/5 rows, both value columns byte-exact.

### LLC — `llc_read_write_*.csv` (5 of 6 columns)
Raw v1 `QQII` → `ts_us, count, event, l3t_id`; 237 × 2 banks × 8 events.
Event→slot map for `read = {0,1,2,0x13,0x20,0x22,0x34,0x36}`:

```
hit   = (e2+e3+e5+e7) / (e4+e6)      # 0 when denominator is 0
tp    = (e0+e1) * (1/64) * 1024*1024 / Δt_s   (needs the unresolved ts path)
```

Average over all 237 rows. lane1 = `2.067` ✓, lane0 = `87.944` vs golden
`87.945` (1 milli-unit), and throughput needs the same timestamp conversion
as the timeline tracks.

### PCIe — `pcie_*.csv`
Raw `Q22I` (96 B) × 239 samples; per-mode `Min/Max/Avg` of
`byte_per_us2_mb_pers(...)` (and `ns2_us` for latency). Not ported yet.

### LLC hit rate — exact (per-row quantisation)
The missing detail: `calculate_hit_rate` goes through `float_calculate`, which
**quantises every row to 3 decimals** before the average.

```
row_hit = quantize3((e2+e3+e5+e7)/(e4+e6))     # 0 when denominator is 0
lane    = mean(row_hit) * 100
```

- lane0 `87.9447` → 87.945 ✓, lane1 `2.0671` → 2.067 ✓, Average 45.006 ✓
  (Average = mean of the *unrounded* lane values).
- Without the per-row quantise, lane0 comes out 87.944 — exactly the 1
  milli-unit that was missing.

### LLC throughput — one unit factor outstanding
Structurally `total_bytes / seconds` with `total = e0 + e1`,
`time_diff = Δts/64/1e9`, `×(1/64)·1024²`. Our value is **4.212×** golden
(70 331.18 vs 16 697.508), i.e. a single constant conversion is still unknown.
The same `float_calculate` 3-decimal quantise applies to the per-row value.

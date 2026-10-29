# mspark specification and investigation notes

This directory holds the reverse-engineering notes behind the implementation.
The code is the source of truth; these docs record **why** a transformation
looks the way it does, what was verified, and which hypotheses were ruled out.

If you are new to the project, read in this order:

1. [`../README.md`](../README.md) — what mspark does, how the dataflow works, and
   the verified/gap status.
2. [`TODO.md`](TODO.md) — the open backlog, each item with the evidence gathered.
3. The milestone note for the artifact you are touching.

## Milestones

| document | scope | status |
|---|---|---|
| [`milestone-1-task-time.md`](milestone-1-task-time.md) | `task_time` from the raw device streams | implemented, verified |
| [`milestone-2-host-ge.md`](milestone-2-host-ge.md) | host GE association for `task_time` names/types | implemented, verified |
| [`milestone-3-op-summary.md`](milestone-3-op-summary.md) | `op_summary` and its 46 columns | implemented, verified |
| [`milestone-4-timeline-hardware.md`](milestone-4-timeline-hardware.md) | timeline: Ascend Hardware + CANN tracks | implemented, verified |
| [`milestone-5-soc-profile.md`](milestone-5-soc-profile.md) | SIO, Acc PMU, Stars Soc Info, Stars Chip Trans | implemented, verified |
| [`milestone-6-overlap.md`](milestone-6-overlap.md) | Overlap Analysis track | implemented, verified |
| [`milestone-7-csv-cluster.md`](milestone-7-csv-cluster.md) | memory/DVPP/CLI CSV cluster | implemented, verified |
| [`milestone-8-statistics.md`](milestone-8-statistics.md) | `api_statistic` / `op_statistic` | implemented; timing residual |
| [`milestone-9-qos.md`](milestone-9-qos.md) | QoS track | implemented, exact |

## Reference material

- [`TODO.md`](TODO.md) — open problems (T1…T27), solved investigations, and
  review items deliberately not taken.
- [`verification-status.md`](verification-status.md) — per-device sweep over the
  multi-device capture, with the known residuals.
- [`dependency-matrix.md`](dependency-matrix.md) — extract of msprof's parser /
  persistence dependency registrations, used to reconstruct the dataflow.
- [`baseline.txt`](baseline.txt) — per-column fidelity ratchet consumed by
  `mspark verify-all --update-baseline`.

## Conventions used by the notes

- “Dataset A” / “profile A” is the single-device V4 (910B) capture; “dataset B”
  / the multi-device capture is the larger merged profile. Percentages are only
  comparable within the same capture.
- “Exact” means every row and column matches the `msprof --export=on` golden at
  the recorded comparison precision; it does not mean byte-identical files.
- Stream layouts are named by their C struct shorthand, e.g. `QQII` = two
  `u64`s followed by two `u32`s, all little-endian.
- Item numbers (`T1k`, `T26`, …) are stable anchors; search for them in
  [`TODO.md`](TODO.md) and in code comments.

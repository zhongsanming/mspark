use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One inter-sample HCCS bandwidth measurement.
///
/// `hccs.data` is a text stream whose body is `time_stamp(s):time_stamp(ns)
/// send_amount receive_amount` with cumulative byte counters. The rate is the
/// delta over the delta of the ns column, scaled so the result is MiB/s
/// (`1e9 / 1024 / 1024` ns per MiB/s unit).
#[derive(Debug, Clone, Copy)]
pub struct HccsRateRow {
    /// Absolute wall time (ns) of the later sample in the pair.
    pub ts_ns: i128,
    /// MiB/s.
    pub tx: f64,
    pub rx: f64,
}

const NS_PER_MB_S: f64 = 1e9 / 1024.0 / 1024.0;

impl Node for HccsRateRow {
    const NAME: &'static str = "hccs";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(ctx, "hccs.", 1)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let clock = crate::time::load_mono_clock(ctx.device_dir()?, None)?;
        let text = String::from_utf8_lossy(&bytes);
        let mut samples: Vec<(u64, u64, u64)> = Vec::new();
        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let Some(stamp) = fields.next() else { continue };
            let (Some(tx), Some(rx)) = (fields.next(), fields.next()) else {
                continue; // header row
            };
            let (Ok(tx), Ok(rx)) = (tx.parse::<u64>(), rx.parse::<u64>()) else {
                continue;
            };
            let stamp = stamp.replace(':', "");
            let Ok(stamp) = stamp.parse::<u64>() else {
                continue;
            };
            samples.push((stamp, tx, rx));
        }

        let mut out = Vec::with_capacity(samples.len().saturating_sub(1));
        for window in samples.windows(2) {
            let (prev, cur) = (window[0], window[1]);
            let delta_ns = cur.0.saturating_sub(prev.0);
            if delta_ns == 0 {
                continue;
            }
            if cur.1 < prev.1 || cur.2 < prev.2 {
                continue; // counter overflow / reset
            }
            let dt = delta_ns as f64 / NS_PER_MB_S;
            out.push(HccsRateRow {
                ts_ns: clock.to_ns(cur.0),
                tx: (cur.1 - prev.1) as f64 / dt,
                rx: (cur.2 - prev.2) as f64 / dt,
            });
        }
        Ok(out)
    }
}

use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::time::load_mono_clock;

/// One text network sample (`nic.data` / `roce.data`).
///
/// Body layout: `time_stamp(s):time_stamp(ns) bandwidth rxPacketRate rxByteRate
/// rxPackets rxBytes rxErrors rxDropped txPacketRate txByteRate txPackets
/// txBytes txErrors txDropped ...` (bandwidth in MB/s).
#[derive(Debug, Clone, Copy)]
pub struct NetSample {
    /// Absolute wall time (ns).
    pub ts_ns: i128,
    pub bandwidth: u64,
    pub rx: [u64; 6],
    pub tx: [u64; 6],
}

pub fn decode_net(ctx: &Ctx, prefix: &str) -> Result<Vec<NetSample>> {
    let bytes = crate::stream::read_stream(ctx, prefix, 1)?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let clock = load_mono_clock(ctx.device_dir()?, None)?;
    let text = String::from_utf8_lossy(&bytes);
    let mut out = Vec::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 15 {
            continue; // header or short row
        }
        let Some(stamp) = fields[0].replace(':', "").parse::<u64>().ok() else {
            continue;
        };
        let nums: Vec<u64> = fields[1..15]
            .iter()
            .map(|f| f.parse::<u64>().unwrap_or(0))
            .collect();
        // bandwidth, then rx (packet rate, byte rate, packets, bytes, errors, dropped),
        // then tx.
        out.push(NetSample {
            ts_ns: clock.to_ns(stamp),
            bandwidth: nums[0],
            rx: [nums[1], nums[2], nums[3], nums[4], nums[5], nums[6]],
            tx: [nums[7], nums[8], nums[9], nums[10], nums[11], nums[12]],
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
pub struct NicRow(pub NetSample);

impl Node for NicRow {
    const NAME: &'static str = "nic";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        Ok(decode_net(ctx, "nic.")?.into_iter().map(NicRow).collect())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RoceRow(pub NetSample);

impl Node for RoceRow {
    const NAME: &'static str = "roce";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        Ok(decode_net(ctx, "roce.")?.into_iter().map(RoceRow).collect())
    }
}

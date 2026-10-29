use anyhow::Result;
use mspark_df::{Ctx, Node};

use crate::stream::{CtxStreams, Stream};
use crate::time::DeviceStartLogs;

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

pub fn decode_net(ctx: &Ctx, net_stream: Stream) -> Result<Vec<NetSample>> {
    let bytes = ctx.stream(net_stream)?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let clock = ctx.require::<DeviceStartLogs>()?.0.mono(None)?;
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
        Ok(decode_net(ctx, Stream::Nic)?
            .into_iter()
            .map(NicRow)
            .collect())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RoceRow(pub NetSample);

impl Node for RoceRow {
    const NAME: &'static str = "roce";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        Ok(decode_net(ctx, Stream::Roce)?
            .into_iter()
            .map(RoceRow)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::sources::testing::FakeDevice;

    #[test]
    fn decodes_the_15_field_text_row() {
        let device = FakeDevice::new();
        // time(s):time(ns) bandwidth rx{packetRate,byteRate,packets,bytes,errors,dropped}
        // tx{…} plus the trailing field msprof ignores.
        device.write(
            "nic.slice_0",
            b"0:1000000000 500 10 20 30 40 1 2 11 21 31 41 3 4 0\n",
        );
        let ctx = device.ctx();
        let rows = NicRow::compute(&ctx, ()).unwrap();
        assert_eq!(rows.len(), 1);
        let sample = rows[0].0;
        assert_eq!(sample.bandwidth, 500);
        assert_eq!(sample.rx, [10, 20, 30, 40, 1, 2]);
        assert_eq!(sample.tx, [11, 21, 31, 41, 3, 4]);
        let clock = device
            .ctx()
            .require::<DeviceStartLogs>()
            .unwrap()
            .0
            .mono(None)
            .unwrap();
        assert_eq!(sample.ts_ns, clock.to_ns(1_000_000_000));
    }

    #[test]
    fn skips_headers_and_short_rows() {
        let device = FakeDevice::new();
        device.write(
            "nic.slice_0",
            b"timestamp bandwidth\n0:1 2 3\nnot-a-stamp 1 2 3 4 5 6 7 8 9 10 11 12 13 14\n",
        );
        assert!(NicRow::compute(&device.ctx(), ()).unwrap().is_empty());
    }
}

//! Chrome-trace event types shared by the timeline writer and the verifier.
//!
//! The field order of [`TraceEvent`] is part of the output contract: it is the
//! key order in `timeline.json`.

use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};

/// Track order in the msprof viewer. The value is written to
/// `process_sort_index` *and* packed into bits 5..10 of the track pid, which is
/// why it appears in both the writer and the verifier; naming it once removes a
/// whole class of copy-paste mistakes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Track {
    Cann = 7,
    CpuUsage = 9,
    Hardware = 13,
    AiCoreFreq = 14,
    Communication = 16,
    AccPmu = 18,
    Hccs = 19,
    Nic = 20,
    Roce = 21,
    Pcie = 22,
    Hbm = 23,
    StarsSocInfo = 24,
    ChipTrans = 25,
    Llc = 26,
    Sio = 27,
    Qos = 28,
    Overlap = 29,
}

/// Host-side tracks (CANN, CPU usage, …) live on this pseudo device id.
pub const HOST_PID: u32 = 31;

impl Track {
    /// `(pid << 10) | (sort_index << 5) | device`.
    pub fn pid(self, host_pid: u32, device: u32) -> u32 {
        (host_pid << 10) | ((self as u32) << 5) | device
    }
}

/// A JSON value attached to a trace event argument.
#[derive(Clone)]
pub enum Num {
    Int(u64),
    Float(f64),
    Str(String),
}

impl Num {
    pub fn as_f64(&self) -> f64 {
        match self {
            Num::Int(v) => *v as f64,
            Num::Float(v) => *v,
            Num::Str(_) => 0.0,
        }
    }
}

impl Serialize for Num {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Num::Int(v) => serializer.serialize_u64(*v),
            Num::Float(v) => serializer.serialize_f64(*v),
            Num::Str(v) => serializer.serialize_str(v),
        }
    }
}

/// Argument list with static keys, inline for the common 1-4 arguments.
///
/// Ordered: insertion order is part of the output contract.
pub struct Args(pub smallvec::SmallVec<[(&'static str, Num); 4]>);

impl Serialize for Args {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// Build the ordered trace arguments without a heap allocation for the common
/// four-argument event.
pub fn args<const N: usize>(items: [(&'static str, Num); N]) -> Args {
    Args(smallvec::SmallVec::from_iter(items))
}

/// One Chrome-trace event. Field order here is the serialized key order.
#[derive(Serialize)]
pub struct TraceEvent {
    pub name: compact_str::CompactString,
    pub pid: u32,
    pub tid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<compact_str::CompactString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dur: Option<f64>,
    pub ph: &'static str,
    pub args: Args,
}

impl TraceEvent {
    pub fn meta(name: &str, pid: u32, tid: u32, args: Args) -> Self {
        Self {
            name: compact_str::CompactString::from(name),
            pid,
            tid,
            ts: None,
            dur: None,
            ph: "M",
            args,
        }
    }

    pub fn complete(
        name: compact_str::CompactString,
        pid: u32,
        tid: u32,
        ts: compact_str::CompactString,
        dur: f64,
        args: Args,
    ) -> Self {
        Self {
            name,
            pid,
            tid,
            ts: Some(ts),
            dur: Some(dur),
            ph: "X",
            args,
        }
    }

    pub fn counter(
        name: compact_str::CompactString,
        pid: u32,
        tid: u32,
        ts: compact_str::CompactString,
        args: Args,
    ) -> Self {
        Self {
            name,
            pid,
            tid,
            ts: Some(ts),
            dur: None,
            ph: "C",
            args,
        }
    }
}

/// One counter sample, before it is attached to a pid/track.
pub struct CounterRow {
    pub name: compact_str::CompactString,
    pub tid: u32,
    pub ts: compact_str::CompactString,
    pub args: smallvec::SmallVec<[(&'static str, Num); 4]>,
}

/// Batched event sink.
///
/// Timeline events are produced grouped by track; a full capture has 2 M+ of
/// them, and collecting them into one `Vec` (with the doubling slack) was ~1.9 GB
/// of the export's peak. `Sink` keeps only a small batch and pushes it to the
/// caller's writer, while the emitters keep their simple `push` API.
pub struct Sink<'a> {
    pending: Vec<TraceEvent>,
    emit: &'a mut dyn FnMut(TraceEvent) -> anyhow::Result<()>,
    total: usize,
}

impl<'a> Sink<'a> {
    const BATCH: usize = 2048;

    pub fn new(emit: &'a mut dyn FnMut(TraceEvent) -> anyhow::Result<()>) -> Self {
        Self {
            pending: Vec::with_capacity(Self::BATCH),
            emit,
            total: 0,
        }
    }

    pub fn push(&mut self, event: TraceEvent) {
        self.pending.push(event);
        self.total += 1;
        if self.pending.len() >= Self::BATCH {
            // `flush` only fails on write errors, which the caller reports.
            let _ = self.flush();
        }
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        for event in self.pending.drain(..) {
            (self.emit)(event)?;
        }
        Ok(())
    }

    /// Flush the last batch; returns how many events were emitted.
    pub fn finish(mut self) -> anyhow::Result<usize> {
        self.flush()?;
        Ok(self.total)
    }
}

pub fn write_counter(events: &mut Sink<'_>, pid: u32, row: CounterRow) {
    let CounterRow {
        name,
        tid,
        ts,
        args,
        ..
    } = row;
    events.push(TraceEvent::counter(
        name,
        pid,
        tid,
        ts,
        Args(smallvec::SmallVec::from_iter(args)),
    ));
}

pub fn push_process_meta(
    events: &mut Sink<'_>,
    pid: u32,
    name: &str,
    label: &str,
    sort_index: u32,
) {
    events.push(TraceEvent::meta(
        "process_name",
        pid,
        0,
        args([("name", Num::Str(name.to_string()))]),
    ));
    events.push(TraceEvent::meta(
        "process_labels",
        pid,
        0,
        args([("labels", Num::Str(label.to_string()))]),
    ));
    events.push(TraceEvent::meta(
        "process_sort_index",
        pid,
        0,
        args([("sort_index", Num::Int(sort_index as u64))]),
    ));
}

pub fn push_thread_meta(events: &mut Sink<'_>, pid: u32, tid: u32, name: &str, sort_index: u32) {
    events.push(TraceEvent::meta(
        "thread_name",
        pid,
        tid,
        args([("name", Num::Str(name.to_string()))]),
    ));
    events.push(TraceEvent::meta(
        "thread_sort_index",
        pid,
        tid,
        args([("sort_index", Num::Int(sort_index as u64))]),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_pid_packs_sort_index_and_device() {
        assert_eq!(Track::Cann.pid(HOST_PID, 5), (31 << 10) | (7 << 5) | 5);
        assert_eq!(Track::Qos.pid(HOST_PID, 1), (31 << 10) | (28 << 5) | 1);
    }

    #[test]
    fn complete_events_serialize_with_the_contract_key_order() {
        let event = TraceEvent::complete(
            "task".into(),
            7,
            9,
            "1.000".into(),
            2.5,
            args([("k", Num::Int(1))]),
        );
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"name":"task","pid":7,"tid":9,"ts":"1.000","dur":2.5,"ph":"X","args":{"k":1}}"#
        );
    }

    #[test]
    fn metadata_events_omit_ts_and_dur() {
        let event = TraceEvent::meta(
            "process_name",
            7,
            0,
            args([("name", Num::Str("worker".to_string()))]),
        );
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"name":"process_name","pid":7,"tid":0,"ph":"M","args":{"name":"worker"}}"#
        );
    }

    #[test]
    fn counter_rows_are_attached_to_the_requested_pid() {
        let mut events = Vec::new();
        {
            let mut collect = |event| {
                events.push(event);
                Ok(())
            };
            let mut sink = Sink::new(&mut collect);
            write_counter(
                &mut sink,
                42,
                CounterRow {
                    name: "counter".into(),
                    tid: 3,
                    ts: "1.000".into(),
                    args: smallvec::smallvec![("v", Num::Float(0.5))],
                },
            );
            assert_eq!(sink.finish().unwrap(), 1);
        }
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].pid, 42);
        assert_eq!(events[0].tid, 3);
        assert_eq!(events[0].ph, "C");
        assert_eq!(
            serde_json::to_string(&events[0]).unwrap(),
            r#"{"name":"counter","pid":42,"tid":3,"ts":"1.000","ph":"C","args":{"v":0.5}}"#
        );
    }

    #[test]
    fn argument_order_is_preserved() {
        let value = serde_json::to_string(&args([
            ("first", Num::Int(1)),
            ("second", Num::Str("two".to_string())),
            ("third", Num::Float(3.0)),
            ("fourth", Num::Int(4)),
            ("fifth", Num::Int(5)),
        ]))
        .unwrap();
        assert_eq!(
            value,
            r#"{"first":1,"second":"two","third":3.0,"fourth":4,"fifth":5}"#
        );
    }
}

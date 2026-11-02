//! EXP-001 record envelope: **one whole applied operation = one open-wal record**.
//!
//! open-wal only guarantees atomicity of a single record (a commit batch is not a
//! transaction, it can be split at a segment roll). The envelope therefore carries
//! everything the SQLite writer spreads over several tables in one transaction:
//! the operation payload (same canonical `encode_op` bytes as VS-006), the
//! [`TimeEnvelope`], the source timestamps, every event with its `event_seq` and origin,
//! the event-less alarm status updates, the operator action, the source recovery updates
//! (with gaps), the time anomaly, the boot generation and the timeline id.
//!
//! Layout (little endian), version 1:
//!
//! ```text
//! header (16 B): magic "MSOW" | version u16 | flags u16 (0) | body_len u32 | body_crc32c u32
//! body:          timeline_id [16] | operation_seq u64 | boot_generation u64 | time | reconciliation u8
//!                | op_len u32 | op payload (codec::encode_op) | source_times opt (len u32 + codec bytes)
//!                | events u32 × 42 B | origins u32 × (opt i64, u8, u8) | alarm_updates u32 × 5 B
//!                | action opt | recovery u32 × ... | anomaly opt
//! ```
//!
//! open-wal already protects each framed record with CRC-32C; the inner CRC is defence in
//! depth (and protects the envelope if it is ever copied out of the log, e.g. shipped to
//! a projector or a replica). The decoder is strict and bounded: every count is checked
//! against the remaining bytes before anything is allocated, trailing bytes and unknown
//! codes are rejected, a decode error is corruption (fail-closed).

use scada_core::{AlarmEvent, AlarmFlags, AlarmId};

use crate::codec::{CodecError, decode_op, decode_source_times, encode_op, encode_source_times, event_kind_of};
use crate::{
    EventOrigin, GapRecord, MAX_CLOCK_ID_BYTES, MAX_NOTE_BYTES, OpRecord, OperatorAction, SourceRecoveryUpdate,
    SourceTimes, TimeAnomaly, TimeEnvelope,
};

pub const MAGIC: &[u8; 4] = b"MSOW";
pub const VERSION: u16 = 1;
pub const HEADER_BYTES: usize = 16;
/// Encoded bytes of one event (fixed).
pub const EVENT_BYTES: usize = 8 + 8 + 4 + 4 + 8 + 8 + 1 + 1 + 4;
/// Encoded bytes of one origin (worst case).
pub const ORIGIN_BYTES: usize = 9 + 2;
/// Fixed part of the body (without op payload, source times, lists, action).
pub const BODY_FIXED_BYTES: usize = 16 + 8 + 8 + (8 + 8 + 8 + 1 + 1 + 9) + 1 + 4 + 1 + 4 + 4 + 4 + 1 + 4 + 1;

/// What a decoded record carries (an [`OpRecord`] without receipt, reservation is
/// recomputed) plus the envelope identity.
#[derive(Debug)]
pub struct Decoded {
    pub timeline_id: [u8; 16],
    pub boot_generation: u64,
    pub record: OpRecord,
}

/// Worst-case encoded size of a record, computable **before** `Kernel::apply` from the
/// admission inputs (EXP-001 §2.4, T05): observations of the batch, maximum fan-out of
/// the operation (events + event-less updates), source timestamps, note, recovery
/// updates. Used to refuse an operation that cannot fit `max_record_size` with a typed
/// error and no mutation.
pub fn worst_case_len(
    observations: usize,
    max_events: usize,
    source_time_obs: usize,
    note_bytes: usize,
    recovery: usize,
) -> usize {
    HEADER_BYTES
        + BODY_FIXED_BYTES
        // op payload: version, kind, batch header, observations (u32 + type + i64)
        + 2 + 4 + 8 + 1 + 8 + 4 + observations * 13 + 32
        // source times: version, opt acquired, quality, id, observations
        + 1 + 9 + 1 + 4 + MAX_CLOCK_ID_BYTES + 4 + source_time_obs * 12
        + max_events * (EVENT_BYTES + ORIGIN_BYTES + 5)
        + (16 + 4 + 8 + 4 + 2 + note_bytes.min(MAX_NOTE_BYTES) + 1 + 9 + 8)
        + recovery * (4 + 9 + 1 + 1 + 9 + 8 + 1 + 9 + 9 + 9 + 1 + 1)
        + 9
}

// ---- writer ----

struct W<'a>(&'a mut Vec<u8>);

impl W<'_> {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn len(&mut self, n: usize) {
        self.u32(u32::try_from(n).unwrap_or(u32::MAX));
    }
    fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.u64(x);
            }
            None => self.u8(0),
        }
    }
    fn opt_i64(&mut self, v: Option<i64>) {
        match v {
            Some(x) => {
                self.u8(1);
                self.i64(x);
            }
            None => self.u8(0),
        }
    }
}

/// Encode `r` into `out` (cleared first). `scratch` is reused for the op payload.
pub fn encode(r: &OpRecord, timeline_id: &[u8; 16], boot_generation: u64, out: &mut Vec<u8>, scratch: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&[0u8; 8]); // body_len, crc: patched below
    let mut w = W(out);
    w.0.extend_from_slice(timeline_id);
    w.u64(r.operation_seq);
    w.u64(boot_generation);
    let t = &r.time;
    w.i64(t.sequenced_at_utc_ns);
    w.u64(t.boottime_ns);
    w.i64(t.logical_time_ns);
    w.u8(t.clock_quality);
    w.u8(t.clock_flags);
    w.opt_i64(t.received_at_utc_ns);
    w.u8(u8::from(r.reconciliation));
    encode_op(&r.operation, scratch);
    w.len(scratch.len());
    w.0.extend_from_slice(scratch);
    match r.source_times.as_deref() {
        Some(s) => {
            let b = encode_source_times(s);
            w.u8(1);
            w.len(b.len());
            w.0.extend_from_slice(&b);
        }
        None => w.u8(0),
    }
    w.len(r.events.len());
    for e in &r.events {
        w.u64(e.event_seq);
        w.u64(e.operation_seq);
        w.u32(e.ordinal_in_operation);
        w.u32(e.alarm_id.0);
        w.u64(e.occurrence_id);
        w.u64(e.activation_token);
        w.u8(e.kind.code());
        w.u8(e.status_after.0);
        w.u32(e.actor_uid);
    }
    w.len(r.origins.len());
    for o in &r.origins {
        w.opt_i64(o.utc_ns);
        w.u8(o.source);
        w.u8(o.clock_quality);
    }
    w.len(r.alarm_updates.len());
    for &(a, f) in &r.alarm_updates {
        w.u32(a);
        w.u8(f);
    }
    match &r.action {
        Some(a) => {
            w.u8(1);
            w.0.extend_from_slice(&a.action_id);
            w.u32(a.alarm_id);
            w.u64(a.requested_token);
            w.u32(a.actor_uid);
            let note = a.note.as_bytes();
            w.u16(u16::try_from(note.len()).unwrap_or(u16::MAX));
            w.0.extend_from_slice(note);
            w.u8(a.outcome);
            w.opt_u64(a.event_seq);
            w.u64(a.occurrence_id);
        }
        None => w.u8(0),
    }
    w.len(r.recovery.len());
    for u in &r.recovery {
        w.u32(u.source_id);
        w.opt_u64(u.source_epoch);
        w.u8(u.live);
        w.u8(u.history);
        w.opt_u64(u.last_source_sequence);
        w.u64(u.verified_at_operation_seq);
        match &u.gap {
            Some(g) => {
                w.u8(1);
                w.opt_u64(g.source_epoch);
                w.opt_u64(g.from_seq);
                w.opt_u64(g.to_seq);
                w.u8(u8::from(g.bounds_unknown));
                w.u8(g.reason);
            }
            None => w.u8(0),
        }
    }
    match &r.anomaly {
        Some(a) => {
            w.u8(1);
            w.u8(a.kind);
            w.i64(a.delta_ns);
        }
        None => w.u8(0),
    }
    let body_len = out.len() - HEADER_BYTES;
    let crc = open_wal::crc32c(&out[HEADER_BYTES..]);
    out[8..12].copy_from_slice(&u32::try_from(body_len).unwrap_or(u32::MAX).to_le_bytes());
    out[12..16].copy_from_slice(&crc.to_le_bytes());
}

// ---- reader ----

struct R<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> R<'a> {
    fn err<T>(&self, reason: &'static str) -> Result<T, CodecError> {
        Err(CodecError { at: self.pos, reason })
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.pos.checked_add(n).ok_or(CodecError { at: self.pos, reason: "length overflow" })?;
        match self.b.get(self.pos..end) {
            Some(s) => {
                self.pos = end;
                Ok(s)
            }
            None => self.err("truncated"),
        }
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let s = self.take(N)?;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.arr::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_le_bytes(self.arr()?))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.arr()?))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.arr()?))
    }
    fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_le_bytes(self.arr()?))
    }
    fn flag(&mut self) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => self.err("invalid flag"),
        }
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, CodecError> {
        Ok(if self.flag()? { Some(self.u64()?) } else { None })
    }
    fn opt_i64(&mut self) -> Result<Option<i64>, CodecError> {
        Ok(if self.flag()? { Some(self.i64()?) } else { None })
    }
    /// A count whose elements take at least `min_elem` bytes each: refused before any
    /// allocation if the remaining bytes cannot hold it (bounded allocation on hostile
    /// input).
    fn count(&mut self, min_elem: usize) -> Result<usize, CodecError> {
        let n = self.u32()? as usize;
        let left = self.b.len() - self.pos;
        if n.checked_mul(min_elem.max(1)).is_none_or(|need| need > left) {
            return self.err("count exceeds record");
        }
        Ok(n)
    }
}

/// Validate header + CRC and decode a record (strict, bounded).
pub fn decode(b: &[u8]) -> Result<Decoded, CodecError> {
    if b.len() < HEADER_BYTES {
        return Err(CodecError { at: 0, reason: "record shorter than the envelope header" });
    }
    if &b[0..4] != MAGIC {
        return Err(CodecError { at: 0, reason: "bad envelope magic" });
    }
    let version = u16::from_le_bytes([b[4], b[5]]);
    if version != VERSION {
        return Err(CodecError { at: 4, reason: "unknown envelope version" });
    }
    if b[6] != 0 || b[7] != 0 {
        return Err(CodecError { at: 6, reason: "unknown envelope flags" });
    }
    let body_len = u32::from_le_bytes([b[8], b[9], b[10], b[11]]) as usize;
    if body_len != b.len() - HEADER_BYTES {
        return Err(CodecError { at: 8, reason: "envelope length mismatch" });
    }
    let crc = u32::from_le_bytes([b[12], b[13], b[14], b[15]]);
    if open_wal::crc32c(&b[HEADER_BYTES..]) != crc {
        return Err(CodecError { at: 12, reason: "envelope checksum mismatch" });
    }
    let mut r = R { b, pos: HEADER_BYTES };
    let timeline_id = r.arr::<16>()?;
    let operation_seq = r.u64()?;
    let boot_generation = r.u64()?;
    let time = TimeEnvelope {
        sequenced_at_utc_ns: r.i64()?,
        boottime_ns: r.u64()?,
        logical_time_ns: r.i64()?,
        clock_quality: r.u8()?,
        clock_flags: r.u8()?,
        received_at_utc_ns: r.opt_i64()?,
    };
    let reconciliation = r.flag()?;
    let n = r.count(1)?;
    let operation = decode_op(r.take(n)?).map_err(|e| CodecError { at: r.pos, reason: e.reason })?;
    let source_times: Option<Box<SourceTimes>> = if r.flag()? {
        let n = r.count(1)?;
        Some(Box::new(decode_source_times(r.take(n)?).map_err(|e| CodecError { at: r.pos, reason: e.reason })?))
    } else {
        None
    };
    let n = r.count(EVENT_BYTES)?;
    let mut events = Vec::with_capacity(n);
    for i in 0..n {
        let event_seq = r.u64()?;
        let op = r.u64()?;
        let ordinal = r.u32()?;
        let alarm_id = r.u32()?;
        let occurrence_id = r.u64()?;
        let activation_token = r.u64()?;
        let kind = event_kind_of(r.u8()?).ok_or(CodecError { at: r.pos, reason: "unknown event kind" })?;
        let status_after = AlarmFlags(r.u8()?);
        let actor_uid = r.u32()?;
        if op != operation_seq {
            return r.err("event of another operation");
        }
        if ordinal as usize != i {
            return r.err("event ordinal out of order");
        }
        if let Some(prev) = events.last().map(|e: &AlarmEvent| e.event_seq)
            && prev.checked_add(1) != Some(event_seq)
        {
            return r.err("events not contiguous");
        }
        events.push(AlarmEvent {
            event_seq,
            operation_seq: op,
            occurrence_id,
            activation_token,
            alarm_id: AlarmId(alarm_id),
            ordinal_in_operation: ordinal,
            actor_uid,
            kind,
            status_after,
        });
    }
    let n = r.count(3)?;
    if n != 0 && n != events.len() {
        return r.err("origins do not match events");
    }
    let mut origins = Vec::with_capacity(n);
    for _ in 0..n {
        origins.push(EventOrigin { utc_ns: r.opt_i64()?, source: r.u8()?, clock_quality: r.u8()? });
    }
    let n = r.count(5)?;
    let mut alarm_updates = Vec::with_capacity(n);
    for _ in 0..n {
        alarm_updates.push((r.u32()?, r.u8()?));
    }
    let action = if r.flag()? {
        let action_id = r.arr::<16>()?;
        let alarm_id = r.u32()?;
        let requested_token = r.u64()?;
        let actor_uid = r.u32()?;
        let n = r.u16()? as usize;
        if n > MAX_NOTE_BYTES {
            return r.err("note too long");
        }
        let note = match std::str::from_utf8(r.take(n)?) {
            Ok(s) => s.to_owned(),
            Err(_) => return r.err("note not UTF-8"),
        };
        Some(OperatorAction {
            action_id,
            alarm_id,
            requested_token,
            actor_uid,
            note,
            outcome: r.u8()?,
            event_seq: r.opt_u64()?,
            occurrence_id: r.u64()?,
        })
    } else {
        None
    };
    let n = r.count(16)?;
    let mut recovery = Vec::with_capacity(n);
    for _ in 0..n {
        let source_id = r.u32()?;
        let source_epoch = r.opt_u64()?;
        let live = r.u8()?;
        let history = r.u8()?;
        let last_source_sequence = r.opt_u64()?;
        let verified_at_operation_seq = r.u64()?;
        let gap = if r.flag()? {
            Some(GapRecord {
                source_epoch: r.opt_u64()?,
                from_seq: r.opt_u64()?,
                to_seq: r.opt_u64()?,
                bounds_unknown: r.flag()?,
                reason: r.u8()?,
            })
        } else {
            None
        };
        recovery.push(SourceRecoveryUpdate {
            source_id,
            source_epoch,
            live,
            history,
            last_source_sequence,
            verified_at_operation_seq,
            gap,
        });
    }
    let anomaly = if r.flag()? { Some(TimeAnomaly { kind: r.u8()?, delta_ns: r.i64()? }) } else { None };
    if r.pos != b.len() {
        return r.err("trailing bytes");
    }
    let mut record = OpRecord {
        operation_seq,
        operation,
        time,
        source_times,
        events,
        origins,
        reconciliation,
        action,
        recovery,
        alarm_updates,
        anomaly,
        reserved_bytes: 0,
        receipt: None,
    };
    record.reserved_bytes = record.queued_bytes();
    Ok(Decoded { timeline_id, boot_generation, record })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use scada_core::{AlarmEventKind, BatchMode, ChannelId, Observation, ObservationBatch, Operation, SourceId, Value};

    fn full_record(n_events: usize) -> OpRecord {
        let seq = 42;
        let events: Vec<AlarmEvent> = (0..n_events)
            .map(|i| AlarmEvent {
                event_seq: 100 + i as u64,
                operation_seq: seq,
                occurrence_id: 7 + i as u64,
                activation_token: 9 + i as u64,
                alarm_id: AlarmId(1 + i as u32),
                ordinal_in_operation: i as u32,
                actor_uid: 0,
                kind: AlarmEventKind::Raised,
                status_after: AlarmFlags(3),
            })
            .collect();
        let origins =
            (0..n_events).map(|i| EventOrigin { utc_ns: Some(i as i64 - 5), source: 1, clock_quality: 2 }).collect();
        let mut r = OpRecord {
            operation_seq: seq,
            operation: Operation::ObservationBatch(ObservationBatch {
                source_id: SourceId(3),
                link_generation: 2,
                mode: BatchMode::FullState,
                source_sequence: 99,
                observations: vec![Observation { channel_id: ChannelId(1), value: Value::I64(-4) }],
            }),
            time: TimeEnvelope {
                sequenced_at_utc_ns: -1,
                boottime_ns: 5,
                logical_time_ns: 7,
                clock_quality: 3,
                clock_flags: 6,
                received_at_utc_ns: Some(i64::MIN),
            },
            source_times: Some(Box::new(SourceTimes {
                acquired_at_utc_ns: Some(1),
                clock_quality: 2,
                clock_id: "ptp0".into(),
                observations: vec![(1, 123)],
            })),
            events,
            origins,
            reconciliation: true,
            action: Some(OperatorAction {
                action_id: [5; 16],
                alarm_id: 1,
                requested_token: 9,
                actor_uid: 1000,
                note: "nota è".into(),
                outcome: 1,
                event_seq: Some(100),
                occurrence_id: 7,
            }),
            recovery: vec![SourceRecoveryUpdate {
                source_id: 3,
                source_epoch: Some(4),
                live: 4,
                history: 3,
                last_source_sequence: None,
                verified_at_operation_seq: 42,
                gap: Some(GapRecord {
                    source_epoch: None,
                    from_seq: Some(1),
                    to_seq: None,
                    bounds_unknown: true,
                    reason: 2,
                }),
            }],
            alarm_updates: vec![(2, 0x11)],
            anomaly: Some(TimeAnomaly { kind: 3, delta_ns: -3_600_000_000_000 }),
            reserved_bytes: 0,
            receipt: None,
        };
        r.reserved_bytes = r.queued_bytes();
        r
    }

    fn same(a: &OpRecord, b: &OpRecord) {
        assert_eq!(a.operation_seq, b.operation_seq);
        assert_eq!(a.operation, b.operation);
        assert_eq!(a.time, b.time);
        assert_eq!(a.source_times, b.source_times);
        assert_eq!(a.events, b.events);
        assert_eq!(a.origins, b.origins);
        assert_eq!(a.reconciliation, b.reconciliation);
        assert_eq!(a.action, b.action);
        assert_eq!(a.recovery, b.recovery);
        assert_eq!(a.alarm_updates, b.alarm_updates);
        assert_eq!(a.anomaly, b.anomaly);
        assert_eq!(a.reserved_bytes, b.reserved_bytes);
    }

    #[test]
    fn roundtrip_keeps_every_field() {
        for n in [0, 1, 10, 1000] {
            let r = full_record(n);
            let (mut out, mut scratch) = (Vec::new(), Vec::new());
            encode(&r, &[9; 16], 77, &mut out, &mut scratch);
            assert!(out.len() <= worst_case_len(1, n, 1, 64, 1), "worst case bound holds for {n} events");
            let d = decode(&out).unwrap();
            assert_eq!(d.timeline_id, [9; 16]);
            assert_eq!(d.boot_generation, 77);
            same(&r, &d.record);
        }
    }

    #[test]
    fn every_truncation_and_single_bit_flip_is_rejected() {
        let r = full_record(3);
        let (mut out, mut scratch) = (Vec::new(), Vec::new());
        encode(&r, &[1; 16], 1, &mut out, &mut scratch);
        for cut in 0..out.len() {
            assert!(decode(&out[..cut]).is_err(), "truncated at {cut}");
        }
        for i in 0..out.len() {
            for bit in 0..8 {
                let mut b = out.clone();
                b[i] ^= 1 << bit;
                assert!(decode(&b).is_err(), "flip byte {i} bit {bit} accepted");
            }
        }
    }

    proptest! {
        #[test]
        fn decoder_never_panics_on_garbage(b in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = decode(&b);
        }

        /// Hostile body with a valid header and CRC: the decoder must stay bounded (no
        /// huge allocation from a forged count) and never panic.
        #[test]
        fn forged_crc_body_is_bounded(body in proptest::collection::vec(any::<u8>(), 0..512)) {
            let mut b = Vec::new();
            b.extend_from_slice(MAGIC);
            b.extend_from_slice(&VERSION.to_le_bytes());
            b.extend_from_slice(&0u16.to_le_bytes());
            b.extend_from_slice(&(body.len() as u32).to_le_bytes());
            b.extend_from_slice(&open_wal::crc32c(&body).to_le_bytes());
            b.extend_from_slice(&body);
            if let Ok(d) = decode(&b) {
                // Canonical: whatever decodes re-encodes to the same bytes.
                let (mut out, mut scratch) = (Vec::new(), Vec::new());
                encode(&d.record, &d.timeline_id, d.boot_generation, &mut out, &mut scratch);
                prop_assert_eq!(out, b);
            }
        }

        #[test]
        fn roundtrip_random_sizes(n in 0usize..64, note in "[a-z]{0,256}", seq in 1u64..u64::MAX / 2) {
            let mut r = full_record(n);
            r.operation_seq = seq;
            for e in &mut r.events { e.operation_seq = seq; }
            if let Some(a) = &mut r.action { a.note = note; }
            r.reserved_bytes = r.queued_bytes();
            let (mut out, mut scratch) = (Vec::new(), Vec::new());
            encode(&r, &[2; 16], 3, &mut out, &mut scratch);
            let d = decode(&out).unwrap();
            same(&r, &d.record);
        }
    }
}

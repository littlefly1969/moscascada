//! Versioned binary encodings of the journal (ADR 0013 D6/D7).
//!
//! Every decoder is strict: explicit lengths, bounded counts, no trailing bytes, unknown
//! codes rejected. A decode error during recovery is a corrupt store (fail-closed).

use scada_core::{
    AlarmCheckpoint, AlarmEvent, AlarmEventKind, AlarmFlags, AlarmId, AlarmStateRecord, BatchMode, ChannelId,
    CheckpointSource, CheckpointTag, Continuity, KernelCheckpoint, LinkState, Observation, ObservationBatch, Operation,
    Quality, SourceId, UncertainReason, Value,
};

use crate::SourceTimes;

/// Version of the operation payload encoding.
pub const OP_PAYLOAD_VERSION: u8 = 1;
/// Checkpoint blob magic and version.
pub const CHECKPOINT_MAGIC: &[u8; 4] = b"MSCK";
pub const CHECKPOINT_VERSION: u16 = 1;
/// Upper bound of any decoded count (defence against corrupt length fields).
const MAX_COUNT: u32 = 50_000_000;

/// Decode error (position and reason).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    pub at: usize,
    pub reason: &'static str,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at byte {}", self.reason, self.at)
    }
}

/// Operation kind codes (column `journal_operations.kind`).
pub mod kind {
    pub const CONNECTED: u8 = 1;
    pub const DISCONNECTED: u8 = 2;
    pub const BATCH: u8 = 3;
    pub const SILENCE: u8 = 4;
    pub const LIVENESS: u8 = 5;
    pub const FRESHNESS: u8 = 6;
    pub const ACK: u8 = 7;
    pub const WATERMARK: u8 = 8;
    pub const FENCE: u8 = 9;
}

pub fn op_kind(op: &Operation) -> u8 {
    match op {
        Operation::SourceConnected { .. } => kind::CONNECTED,
        Operation::SourceDisconnected { .. } => kind::DISCONNECTED,
        Operation::ObservationBatch(_) => kind::BATCH,
        Operation::SourceSilenceExpired { .. } => kind::SILENCE,
        Operation::SourceLivenessRestored { .. } => kind::LIVENESS,
        Operation::FreshnessExpired { .. } => kind::FRESHNESS,
        Operation::AlarmAcknowledge { .. } => kind::ACK,
        Operation::AdvanceDurableWatermark { .. } => kind::WATERMARK,
        Operation::RecoveryFence { .. } => kind::FENCE,
    }
}

// ---- primitive writer / reader ----

#[derive(Debug, Default)]
pub struct Wr(pub Vec<u8>);

impl Wr {
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
    fn bytes(&mut self, b: &[u8]) {
        self.0.extend_from_slice(b);
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
    fn value(&mut self, v: Value) {
        match v {
            Value::Bool(b) => {
                self.u8(1);
                self.i64(i64::from(b));
            }
            Value::I64(i) => {
                self.u8(2);
                self.i64(i);
            }
        }
    }
}

#[derive(Debug)]
pub struct Rd<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Rd<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Rd { b, pos: 0 }
    }
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
    fn bool(&mut self) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => self.err("invalid bool"),
        }
    }
    fn count(&mut self) -> Result<usize, CodecError> {
        let n = self.u32()?;
        if n > MAX_COUNT {
            return self.err("count too large");
        }
        Ok(n as usize)
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, CodecError> {
        Ok(if self.bool()? { Some(self.u64()?) } else { None })
    }
    fn value(&mut self) -> Result<Value, CodecError> {
        match self.u8()? {
            1 => match self.i64()? {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => self.err("invalid bool value"),
            },
            2 => Ok(Value::I64(self.i64()?)),
            _ => self.err("invalid value type"),
        }
    }
    fn end(&self) -> Result<(), CodecError> {
        if self.pos == self.b.len() { Ok(()) } else { self.err("trailing bytes") }
    }
}

fn mode_code(m: BatchMode) -> u8 {
    match m {
        BatchMode::FullState => 1,
        BatchMode::Delta => 2,
    }
}

fn mode_of(c: u8, r: &Rd<'_>) -> Result<BatchMode, CodecError> {
    match c {
        1 => Ok(BatchMode::FullState),
        2 => Ok(BatchMode::Delta),
        _ => r.err("invalid batch mode"),
    }
}

// ---- operations ----

/// Canonical payload of an operation (version byte first).
pub fn encode_op(op: &Operation, out: &mut Vec<u8>) {
    let mut w = Wr(std::mem::take(out));
    w.0.clear();
    w.u8(OP_PAYLOAD_VERSION);
    w.u8(op_kind(op));
    match op {
        Operation::SourceConnected { source_id, link_generation, source_epoch } => {
            w.u32(source_id.0);
            w.u64(*link_generation);
            w.u64(*source_epoch);
        }
        Operation::SourceDisconnected { source_id, link_generation }
        | Operation::SourceSilenceExpired { source_id, link_generation }
        | Operation::SourceLivenessRestored { source_id, link_generation } => {
            w.u32(source_id.0);
            w.u64(*link_generation);
        }
        Operation::ObservationBatch(b) => {
            w.u32(b.source_id.0);
            w.u64(b.link_generation);
            w.u8(mode_code(b.mode));
            w.u64(b.source_sequence);
            w.len(b.observations.len());
            for o in &b.observations {
                w.u32(o.channel_id.0);
                match o.value {
                    Value::Bool(x) => {
                        w.u8(1);
                        w.i64(i64::from(x));
                    }
                    Value::I64(x) => {
                        w.u8(2);
                        w.i64(x);
                    }
                }
            }
        }
        Operation::FreshnessExpired { source_id, link_generation, observed_up_to } => {
            w.u32(source_id.0);
            w.u64(*link_generation);
            w.u64(*observed_up_to);
        }
        Operation::AlarmAcknowledge { alarm_id, expected_activation_token, actor_uid } => {
            w.u32(alarm_id.0);
            w.u64(*expected_activation_token);
            w.u32(*actor_uid);
        }
        Operation::AdvanceDurableWatermark { up_to_event_seq } => w.u64(*up_to_event_seq),
        Operation::RecoveryFence { boot_generation } => w.u64(*boot_generation),
    }
    *out = w.0;
}

pub fn decode_op(b: &[u8]) -> Result<Operation, CodecError> {
    let mut r = Rd::new(b);
    if r.u8()? != OP_PAYLOAD_VERSION {
        return r.err("unknown payload version");
    }
    let k = r.u8()?;
    let op = match k {
        kind::CONNECTED => Operation::SourceConnected {
            source_id: SourceId(r.u32()?),
            link_generation: r.u64()?,
            source_epoch: r.u64()?,
        },
        kind::DISCONNECTED => {
            Operation::SourceDisconnected { source_id: SourceId(r.u32()?), link_generation: r.u64()? }
        }
        kind::SILENCE => Operation::SourceSilenceExpired { source_id: SourceId(r.u32()?), link_generation: r.u64()? },
        kind::LIVENESS => {
            Operation::SourceLivenessRestored { source_id: SourceId(r.u32()?), link_generation: r.u64()? }
        }
        kind::BATCH => {
            let source_id = SourceId(r.u32()?);
            let link_generation = r.u64()?;
            let m = r.u8()?;
            let mode = mode_of(m, &r)?;
            let source_sequence = r.u64()?;
            let n = r.count()?;
            let mut observations = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                let channel_id = ChannelId(r.u32()?);
                let value = r.value()?;
                observations.push(Observation { channel_id, value });
            }
            Operation::ObservationBatch(ObservationBatch {
                source_id,
                link_generation,
                mode,
                source_sequence,
                observations,
            })
        }
        kind::FRESHNESS => Operation::FreshnessExpired {
            source_id: SourceId(r.u32()?),
            link_generation: r.u64()?,
            observed_up_to: r.u64()?,
        },
        kind::ACK => Operation::AlarmAcknowledge {
            alarm_id: AlarmId(r.u32()?),
            expected_activation_token: r.u64()?,
            actor_uid: r.u32()?,
        },
        kind::WATERMARK => Operation::AdvanceDurableWatermark { up_to_event_seq: r.u64()? },
        kind::FENCE => Operation::RecoveryFence { boot_generation: r.u64()? },
        _ => return r.err("unknown operation kind"),
    };
    r.end()?;
    Ok(op)
}

/// First 8 bytes of BLAKE3 of the payload, as a signed integer column.
pub fn payload_digest(payload: &[u8]) -> i64 {
    let h = blake3::hash(payload);
    let mut a = [0u8; 8];
    a.copy_from_slice(&h.as_bytes()[..8]);
    i64::from_le_bytes(a)
}

// ---- source times sidecar ----

pub fn encode_source_times(s: &SourceTimes) -> Vec<u8> {
    let mut w = Wr::default();
    w.u8(1);
    match s.acquired_at_utc_ns {
        Some(t) => {
            w.u8(1);
            w.i64(t);
        }
        None => w.u8(0),
    }
    w.u8(s.clock_quality);
    w.len(s.clock_id.len());
    w.bytes(s.clock_id.as_bytes());
    w.len(s.observations.len());
    for (c, t) in &s.observations {
        w.u32(*c);
        w.i64(*t);
    }
    w.0
}

pub fn decode_source_times(b: &[u8]) -> Result<SourceTimes, CodecError> {
    let mut r = Rd::new(b);
    if r.u8()? != 1 {
        return r.err("unknown source times version");
    }
    let acquired_at_utc_ns = if r.bool()? { Some(r.i64()?) } else { None };
    let clock_quality = r.u8()?;
    let n = r.count()?;
    if n > crate::MAX_CLOCK_ID_BYTES {
        return r.err("clock id too long");
    }
    let clock_id = match std::str::from_utf8(r.take(n)?) {
        Ok(s) => s.to_owned(),
        Err(_) => return r.err("clock id not UTF-8"),
    };
    let n = r.count()?;
    let mut observations = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        observations.push((r.u32()?, r.i64()?));
    }
    r.end()?;
    Ok(SourceTimes { acquired_at_utc_ns, clock_quality, clock_id, observations })
}

// ---- events ----

pub fn event_kind_of(code: u8) -> Option<AlarmEventKind> {
    match code {
        1 => Some(AlarmEventKind::Raised),
        2 => Some(AlarmEventKind::Returned),
        3 => Some(AlarmEventKind::Retriggered),
        4 => Some(AlarmEventKind::Acknowledged),
        _ => None,
    }
}

/// Event row fields (as stored) back to the 48-byte record.
#[allow(clippy::too_many_arguments)]
pub fn event_from_row(
    event_seq: u64,
    operation_seq: u64,
    ordinal: u32,
    alarm_id: u32,
    occurrence_id: u64,
    activation_token: u64,
    kind: u8,
    status_after: u8,
    actor_uid: u32,
) -> Option<AlarmEvent> {
    Some(AlarmEvent {
        event_seq,
        operation_seq,
        occurrence_id,
        activation_token,
        alarm_id: AlarmId(alarm_id),
        ordinal_in_operation: ordinal,
        actor_uid,
        kind: event_kind_of(kind)?,
        status_after: AlarmFlags(status_after),
    })
}

// ---- checkpoint ----

fn quality_code(q: Quality) -> u8 {
    match q {
        Quality::Unknown => 0,
        Quality::Good => 1,
        Quality::Uncertain(UncertainReason::SourceUnavailable) => 11,
        Quality::Uncertain(UncertainReason::SourceSilent) => 12,
        Quality::Uncertain(UncertainReason::StaleData) => 13,
        Quality::Uncertain(UncertainReason::RecoveredUnverified) => 14,
    }
}

fn quality_of(c: u8, r: &Rd<'_>) -> Result<Quality, CodecError> {
    Ok(match c {
        0 => Quality::Unknown,
        1 => Quality::Good,
        11 => Quality::Uncertain(UncertainReason::SourceUnavailable),
        12 => Quality::Uncertain(UncertainReason::SourceSilent),
        13 => Quality::Uncertain(UncertainReason::StaleData),
        14 => Quality::Uncertain(UncertainReason::RecoveredUnverified),
        _ => return r.err("invalid quality"),
    })
}

fn continuity_code(c: Option<Continuity>) -> u8 {
    match c {
        None => 0,
        Some(Continuity::Continuous) => 1,
        Some(Continuity::BaselineEstablished) => 2,
        Some(Continuity::Resynchronized) => 3,
    }
}

fn continuity_of(c: u8, r: &Rd<'_>) -> Result<Option<Continuity>, CodecError> {
    Ok(match c {
        0 => None,
        1 => Some(Continuity::Continuous),
        2 => Some(Continuity::BaselineEstablished),
        3 => Some(Continuity::Resynchronized),
        _ => return r.err("invalid continuity"),
    })
}

/// Versioned checkpoint blob (`MSCK`, version, explicit lengths).
pub fn encode_checkpoint(cp: &KernelCheckpoint) -> Vec<u8> {
    let mut w = Wr::default();
    w.bytes(CHECKPOINT_MAGIC);
    w.u16(CHECKPOINT_VERSION);
    w.bytes(&cp.config_fingerprint);
    w.u64(cp.last_operation_sequence);
    w.u8(u8::from(cp.recoverable));
    w.u64(cp.fence_boot_generation);
    w.len(cp.tags.len());
    for t in &cp.tags {
        match t.value {
            None => {
                w.u8(0);
                w.i64(0);
            }
            Some(v) => w.value(v),
        }
        w.u8(quality_code(t.quality));
        w.u64(t.revision);
        w.u64(t.observed);
    }
    w.len(cp.sources.len());
    for s in &cp.sources {
        match s.link {
            LinkState::NeverConnected => {
                w.u8(0);
                w.u64(0);
            }
            LinkState::Connected { generation } => {
                w.u8(1);
                w.u64(generation);
            }
            LinkState::Disconnected { generation } => {
                w.u8(2);
                w.u64(generation);
            }
        }
        w.opt_u64(s.epoch);
        w.opt_u64(s.last_accepted);
        w.u8(continuity_code(s.continuity));
        match &s.last_batch {
            None => w.u8(0),
            Some((mode, seq, entries)) => {
                w.u8(1);
                w.u8(mode_code(*mode));
                w.u64(*seq);
                w.len(entries.len());
                for (t, v) in entries {
                    w.u32(*t);
                    w.value(*v);
                }
            }
        }
        w.u8(u8::from(s.silent));
        w.u64(s.last_batch_operation);
        w.u8(u8::from(s.unverified));
    }
    match &cp.alarms {
        None => w.u8(0),
        Some(a) => {
            w.u8(1);
            w.len(a.states.len());
            for s in &a.states {
                w.u64(s.token);
                w.u64(s.occurrence);
                w.u64(s.last_event_seq);
                w.u8(s.flags);
            }
            w.u64(a.next_event_seq);
            w.u64(a.durable_watermark);
            w.u64(a.high_water);
            w.u64(a.last_alarm_change_operation);
        }
    }
    w.0
}

pub fn decode_checkpoint(b: &[u8]) -> Result<KernelCheckpoint, CodecError> {
    let mut r = Rd::new(b);
    if r.take(4)? != CHECKPOINT_MAGIC {
        return r.err("bad checkpoint magic");
    }
    if r.u16()? != CHECKPOINT_VERSION {
        return r.err("unknown checkpoint version");
    }
    let config_fingerprint = r.arr::<32>()?;
    let last_operation_sequence = r.u64()?;
    let recoverable = r.bool()?;
    let fence_boot_generation = r.u64()?;
    let n = r.count()?;
    let mut tags = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let value = match r.u8()? {
            0 => {
                if r.i64()? != 0 {
                    return r.err("invalid empty value");
                }
                None
            }
            1 => match r.i64()? {
                0 => Some(Value::Bool(false)),
                1 => Some(Value::Bool(true)),
                _ => return r.err("invalid bool value"),
            },
            2 => Some(Value::I64(r.i64()?)),
            _ => return r.err("invalid value type"),
        };
        let qc = r.u8()?;
        let quality = quality_of(qc, &r)?;
        tags.push(CheckpointTag { value, quality, revision: r.u64()?, observed: r.u64()? });
    }
    let n = r.count()?;
    let mut sources = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        let lk = r.u8()?;
        let g = r.u64()?;
        let link = match lk {
            0 if g == 0 => LinkState::NeverConnected,
            1 => LinkState::Connected { generation: g },
            2 => LinkState::Disconnected { generation: g },
            _ => return r.err("invalid link state"),
        };
        let epoch = r.opt_u64()?;
        let last_accepted = r.opt_u64()?;
        let cc = r.u8()?;
        let continuity = continuity_of(cc, &r)?;
        let last_batch = if r.bool()? {
            let m = r.u8()?;
            let mode = mode_of(m, &r)?;
            let seq = r.u64()?;
            let k = r.count()?;
            let mut entries = Vec::with_capacity(k.min(1 << 16));
            for _ in 0..k {
                entries.push((r.u32()?, r.value()?));
            }
            Some((mode, seq, entries))
        } else {
            None
        };
        sources.push(CheckpointSource {
            link,
            epoch,
            last_accepted,
            continuity,
            last_batch,
            silent: r.bool()?,
            last_batch_operation: r.u64()?,
            unverified: r.bool()?,
        });
    }
    let alarms = if r.bool()? {
        let k = r.count()?;
        let mut states = Vec::with_capacity(k.min(1 << 20));
        for _ in 0..k {
            states.push(AlarmStateRecord {
                token: r.u64()?,
                occurrence: r.u64()?,
                last_event_seq: r.u64()?,
                flags: r.u8()?,
            });
        }
        Some(AlarmCheckpoint {
            states,
            next_event_seq: r.u64()?,
            durable_watermark: r.u64()?,
            high_water: r.u64()?,
            last_alarm_change_operation: r.u64()?,
        })
    } else {
        None
    };
    r.end()?;
    Ok(KernelCheckpoint {
        config_fingerprint,
        last_operation_sequence,
        recoverable,
        fence_boot_generation,
        tags,
        sources,
        alarms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_value() -> impl Strategy<Value = Value> {
        prop_oneof![any::<bool>().prop_map(Value::Bool), any::<i64>().prop_map(Value::I64)]
    }

    fn arb_op() -> impl Strategy<Value = Operation> {
        prop_oneof![
            (any::<u32>(), any::<u64>(), any::<u64>()).prop_map(|(s, g, e)| Operation::SourceConnected {
                source_id: SourceId(s),
                link_generation: g,
                source_epoch: e
            }),
            (any::<u32>(), any::<u64>())
                .prop_map(|(s, g)| Operation::SourceDisconnected { source_id: SourceId(s), link_generation: g }),
            (
                any::<u32>(),
                any::<u64>(),
                any::<bool>(),
                any::<u64>(),
                proptest::collection::vec((any::<u32>(), arb_value()), 0..20)
            )
                .prop_map(|(s, g, full, seq, obs)| Operation::ObservationBatch(ObservationBatch {
                    source_id: SourceId(s),
                    link_generation: g,
                    mode: if full { BatchMode::FullState } else { BatchMode::Delta },
                    source_sequence: seq,
                    observations: obs
                        .into_iter()
                        .map(|(c, value)| Observation { channel_id: ChannelId(c), value })
                        .collect(),
                })),
            (any::<u32>(), any::<u64>(), any::<u64>()).prop_map(|(s, g, o)| Operation::FreshnessExpired {
                source_id: SourceId(s),
                link_generation: g,
                observed_up_to: o
            }),
            (any::<u32>(), any::<u64>(), any::<u32>()).prop_map(|(a, t, u)| Operation::AlarmAcknowledge {
                alarm_id: AlarmId(a),
                expected_activation_token: t,
                actor_uid: u
            }),
            any::<u64>().prop_map(|u| Operation::AdvanceDurableWatermark { up_to_event_seq: u }),
            any::<u64>().prop_map(|b| Operation::RecoveryFence { boot_generation: b }),
            (any::<u32>(), any::<u64>())
                .prop_map(|(s, g)| Operation::SourceSilenceExpired { source_id: SourceId(s), link_generation: g }),
            (any::<u32>(), any::<u64>())
                .prop_map(|(s, g)| Operation::SourceLivenessRestored { source_id: SourceId(s), link_generation: g }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        /// T29: every operation round-trips; any truncation or extension is rejected.
        #[test]
        fn op_roundtrip_and_strict(op in arb_op(), cut in 0usize..64, extra in any::<u8>()) {
            let mut b = Vec::new();
            encode_op(&op, &mut b);
            let d = decode_op(&b);
            prop_assert_eq!(d, Ok(op.clone()));
            if cut < b.len() {
                prop_assert!(decode_op(&b[..cut]).is_err());
            }
            let mut longer = b.clone();
            longer.push(extra);
            prop_assert!(decode_op(&longer).is_err());
        }

        /// T29: arbitrary bytes never panic the decoders.
        #[test]
        fn decoders_never_panic(b in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode_op(&b);
            let _ = decode_checkpoint(&b);
            let _ = decode_source_times(&b);
        }

        #[test]
        fn source_times_roundtrip(acq in proptest::option::of(any::<i64>()), q in 0u8..5, id in "[a-z0-9]{0,64}",
                                  obs in proptest::collection::vec((any::<u32>(), any::<i64>()), 0..10)) {
            let s = SourceTimes { acquired_at_utc_ns: acq, clock_quality: q, clock_id: id, observations: obs };
            prop_assert_eq!(decode_source_times(&encode_source_times(&s)), Ok(s));
        }
    }

    #[test]
    fn checkpoint_roundtrip() {
        let cp = KernelCheckpoint {
            config_fingerprint: [7; 32],
            last_operation_sequence: 42,
            recoverable: true,
            fence_boot_generation: 3,
            tags: vec![
                CheckpointTag { value: None, quality: Quality::Unknown, revision: 0, observed: 0 },
                CheckpointTag {
                    value: Some(Value::I64(-5)),
                    quality: Quality::Uncertain(UncertainReason::RecoveredUnverified),
                    revision: 9,
                    observed: 40,
                },
            ],
            sources: vec![CheckpointSource {
                link: LinkState::Disconnected { generation: 4 },
                epoch: Some(1),
                last_accepted: Some(17),
                continuity: Some(Continuity::Resynchronized),
                last_batch: Some((BatchMode::FullState, 17, vec![(0, Value::Bool(true)), (1, Value::I64(3))])),
                silent: false,
                last_batch_operation: 40,
                unverified: true,
            }],
            alarms: Some(AlarmCheckpoint {
                states: vec![AlarmStateRecord { token: 4, occurrence: 1, last_event_seq: 2, flags: 3 }],
                next_event_seq: 3,
                durable_watermark: 2,
                high_water: 2,
                last_alarm_change_operation: 41,
            }),
        };
        let b = encode_checkpoint(&cp);
        assert_eq!(decode_checkpoint(&b), Ok(cp));
        for cut in 0..b.len() {
            assert!(decode_checkpoint(&b[..cut]).is_err(), "truncated at {cut} accepted");
        }
    }
}

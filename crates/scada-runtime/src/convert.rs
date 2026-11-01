//! Protocol <-> core conversions. The core never sees protobuf types.

use scada_core::{ApplyError, BatchMode, ChannelId, Continuity, Observation, ObservationBatch, SourceId, Value};
use scada_protocol::Message;
use scada_protocol::driver_v1::{self as pb, ErrorCode, driver_message};

/// Result of [`decode_driver_message`].
#[derive(Debug)]
pub enum Decoded<'a> {
    /// The frame carried exactly one `ObservationBatch`, decoded into the reusable scratch.
    Batch(&'a pb::ObservationBatch),
    /// Anything else (Hello, empty body, unusual encodings), decoded generically.
    Other(pb::DriverMessage),
    /// A well-formed batch with more observations than the limit. Nothing was
    /// materialized: only its header fields and the count are known (review P1).
    Oversized { mode: i32, source_sequence: u64, observations: u64 },
    /// A well-formed frame whose body is not a batch, recognized without decoding it
    /// (it held more observation fields than the limit before a later `hello` replaced
    /// the batch body).
    NotBatch,
    /// VS-004: a well-formed frame whose final body is a `heartbeat`, recognized without
    /// materializing the oversized batch fields that preceded it. (A small heartbeat
    /// frame takes the generic decoder and arrives as [`Decoded::Other`].)
    Heartbeat { sequence: u64 },
}

/// The frame is not a valid `DriverMessage` (the generic decoder would fail too).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Malformed;

impl From<prost::DecodeError> for Malformed {
    fn from(_: prost::DecodeError) -> Self {
        Malformed
    }
}

/// `DriverMessage` field 2 (`batch`), wire type 2 (length-delimited).
const BATCH_KEY: u8 = (2 << 3) | 2;
/// Smallest encoding of one `Observation` inside a batch: key byte + zero length.
const MIN_OBSERVATION_WIRE_BYTES: usize = 2;

/// Decode one data-phase frame (C3) without ever *retaining* memory for more than
/// `max_observations` observations (review P1).
///
/// Fast path: a frame that is exactly one `batch` field — what every conforming driver
/// sends — is merged into `scratch` after resetting every field to its default, so the
/// observation vector keeps its capacity across frames instead of being regrown from zero
/// for every batch. Proto3 merging into a default message is decoding, so the result is
/// the same `ObservationBatch` that `DriverMessage::decode` would produce. Only keys and
/// length varints are parsed by hand, with prost's own helpers, all bounds-checked; there
/// is no unsafe code.
///
/// Limit (review P1): a frame within `max_frame_bytes` can encode up to
/// `max_frame_bytes / 2` observations. On the fast path the merge runs at full speed and
/// the count is checked right after it: a batch over the limit (or a frame that fails to
/// decode) is reported as [`Decoded::Oversized`] (or an error) and, if it made the vector
/// grow past [`retained_capacity_bound`], the vector is dropped at once — before the
/// caller can await anything. The transient peak is therefore bounded by
/// [`transient_decode_bound_bytes`] per thread that decodes at that moment (decoding is
/// synchronous), not per connection; the memory bound counts it per runtime worker.
/// (A limit checked inside the merge was measured: 13–16 % less throughput and +30 %
/// rare-source p99 in `current_thread`, `docs/benchmarks/vs002.md` §12.)
///
/// Every other shape (other bodies, repeated or unknown top-level fields, trailing
/// bytes) takes the generic decoder, whose result can be held across awaits: there a
/// cheap pass first counts the observation fields (an upper bound of the decoded length;
/// frames too short to exceed the limit skip it), and past the limit an exact,
/// allocation-free validating scan decides between a malformed frame, a non-batch body
/// and an oversized batch, so nothing over the limit is materialized at all.
pub fn decode_driver_message<'a>(
    bytes: &[u8],
    scratch: &'a mut pb::ObservationBatch,
    max_observations: u32,
) -> Result<Decoded<'a>, Malformed> {
    let max = u64::from(max_observations);
    if let Some((&BATCH_KEY, mut rest)) = bytes.split_first()
        && let Ok(len) = prost::encoding::decode_varint(&mut rest)
        && usize::try_from(len).is_ok_and(|l| l == rest.len())
    {
        // `clear` resets every field (including any added later) and keeps the
        // observation vector's capacity.
        scratch.clear();
        let merged = scratch.merge(rest);
        let n = scratch.observations.len() as u64;
        if merged.is_err() || n > max {
            // Never retain what an oversized or broken frame made the vector grow to.
            if scratch.observations.capacity() > retained_capacity_bound(max_observations) {
                scratch.observations = Vec::new();
            }
            merged?;
            return Ok(Decoded::Oversized {
                mode: scratch.mode,
                source_sequence: scratch.source_sequence,
                observations: n,
            });
        }
        return Ok(Decoded::Batch(scratch));
    }
    // Any other shape: a cheap count bounds what the generic decoder would allocate.
    if bytes.len() / MIN_OBSERVATION_WIRE_BYTES > max_observations as usize && count_upper_bound(bytes) > max {
        match scan_exact(bytes)? {
            Body::Batch { mode, source_sequence, observations } if observations > max => {
                return Ok(Decoded::Oversized { mode, source_sequence, observations });
            }
            Body::Hello => return Ok(Decoded::NotBatch),
            Body::Heartbeat(sequence) => return Ok(Decoded::Heartbeat { sequence }),
            // Within the limit after all: the upper bound also counted `batch` fields that
            // a later `hello` discarded. A generic decode would still materialize those, so
            // merge only the `batch` fields after the last `hello`, as prost would.
            Body::Batch { .. } => {
                merge_final_batch(bytes, scratch)?;
                return Ok(Decoded::Batch(scratch));
            }
            Body::None => {}
        }
    }
    let msg = pb::DriverMessage::decode(bytes)?;
    Ok(Decoded::Other(msg))
}

/// Number of `observations` fields in every `batch` field of the frame: an upper bound
/// of the decoded batch length. Stops at the first undecodable byte (what was counted
/// so far is still an upper bound of what a decoder would allocate before failing).
#[cold]
#[inline(never)]
fn count_upper_bound(bytes: &[u8]) -> u64 {
    use prost::encoding::{WireType, decode_key, decode_varint, skip_field};
    let mut n = 0u64;
    let mut top = bytes;
    while !top.is_empty() {
        let Ok((tag, wt)) = decode_key(&mut top) else { break };
        if tag == 2 && wt == WireType::LengthDelimited {
            let Some(mut inner) = take_len(&mut top) else { break };
            while !inner.is_empty() {
                let Ok((t, w)) = decode_key(&mut inner) else { break };
                if t == 3 && w == WireType::LengthDelimited {
                    let Ok(len) = decode_varint(&mut inner) else { break };
                    let Some(rest) = usize::try_from(len).ok().and_then(|l| inner.get(l..)) else { break };
                    inner = rest;
                    n = n.saturating_add(1);
                } else if skip_field(w, t, &mut inner, prost::encoding::DecodeContext::default()).is_err() {
                    break;
                }
            }
        } else if skip_field(wt, tag, &mut top, prost::encoding::DecodeContext::default()).is_err() {
            break;
        }
    }
    n
}

/// Split a length-delimited payload off the front of `buf`.
fn take_len<'b>(buf: &mut &'b [u8]) -> Option<&'b [u8]> {
    let len = usize::try_from(prost::encoding::decode_varint(buf).ok()?).ok()?;
    let b: &'b [u8] = buf;
    let (payload, rest) = (b.get(..len)?, b.get(len..)?);
    *buf = rest;
    Some(payload)
}

/// Final body of a frame as prost would decode it (oneof: last field wins; repeated
/// `batch` fields merge, concatenating observations).
enum Body {
    None,
    Hello,
    Batch {
        mode: i32,
        source_sequence: u64,
        observations: u64,
    },
    /// VS-004: `heartbeat` (field 3) with its merged `sequence`.
    Heartbeat(u64),
}

/// Exact, allocation-free mirror of the derived `DriverMessage::merge` for frames whose
/// observation count exceeds the limit: validates every field the way prost does
/// (wire types, varints, nested `Observation`s, unknown fields skipped) and returns the
/// resulting body, counting observations instead of storing them. Only reached for
/// oversized or hostile frames, never on the hot path of a conforming driver.
#[cold]
#[inline(never)]
fn scan_exact(bytes: &[u8]) -> Result<Body, Malformed> {
    use prost::encoding::{DecodeContext, WireType, check_wire_type, decode_key, int32, skip_field, uint64};
    let mut top = bytes;
    let mut body = Body::None;
    while !top.is_empty() {
        let (tag, wt) = decode_key(&mut top)?;
        match tag {
            1 => {
                check_wire_type(WireType::LengthDelimited, wt)?;
                let sub = take_len(&mut top).ok_or(Malformed)?;
                // Validates; allocates at most the strings of this field (bounded by the
                // frame) and frees them at once.
                pb::Hello::decode(sub)?;
                body = Body::Hello;
            }
            3 => {
                // VS-004: a repeated `heartbeat` field merges into the previous one (as
                // prost does for a oneof message field); any other body is replaced.
                check_wire_type(WireType::LengthDelimited, wt)?;
                let sub = take_len(&mut top).ok_or(Malformed)?;
                let mut hb = pb::Heartbeat { sequence: if let Body::Heartbeat(s) = body { s } else { 0 } };
                hb.merge(sub)?; // no heap fields: no allocation
                body = Body::Heartbeat(hb.sequence);
            }
            2 => {
                check_wire_type(WireType::LengthDelimited, wt)?;
                let mut inner = take_len(&mut top).ok_or(Malformed)?;
                let (mut mode, mut seq, mut n) = match body {
                    Body::Batch { mode, source_sequence, observations } => (mode, source_sequence, observations),
                    _ => (0, 0, 0),
                };
                while !inner.is_empty() {
                    let (t, w) = decode_key(&mut inner)?;
                    match t {
                        1 => int32::merge(w, &mut mode, &mut inner, DecodeContext::default())?,
                        2 => uint64::merge(w, &mut seq, &mut inner, DecodeContext::default())?,
                        3 => {
                            check_wire_type(WireType::LengthDelimited, w)?;
                            let o = take_len(&mut inner).ok_or(Malformed)?;
                            pb::Observation::decode(o)?; // no heap fields: no allocation
                            n = n.saturating_add(1);
                        }
                        _ => skip_field(w, t, &mut inner, DecodeContext::default())?,
                    }
                }
                body = Body::Batch { mode, source_sequence: seq, observations: n };
            }
            _ => skip_field(wt, tag, &mut top, DecodeContext::default())?,
        }
    }
    Ok(body)
}

/// Merge into `scratch` the `batch` fields that follow the last `hello` field: the body
/// prost's oneof decoding ends with. Only called after [`scan_exact`] validated the frame.
#[cold]
#[inline(never)]
fn merge_final_batch(bytes: &[u8], scratch: &mut pb::ObservationBatch) -> Result<(), Malformed> {
    use prost::encoding::{DecodeContext, decode_key, skip_field};
    // Pass 1: where the body after the last `hello` (or VS-004 `heartbeat`) starts.
    let mut top = bytes;
    let mut start = bytes;
    while !top.is_empty() {
        let (tag, wt) = decode_key(&mut top)?;
        match tag {
            1 | 3 => {
                take_len(&mut top).ok_or(Malformed)?;
                start = top;
            }
            2 => {
                take_len(&mut top).ok_or(Malformed)?;
            }
            _ => skip_field(wt, tag, &mut top, DecodeContext::default())?,
        }
    }
    // Pass 2: merge every `batch` field from there on (no hello / heartbeat is left in
    // `start`).
    scratch.clear();
    let mut top = start;
    while !top.is_empty() {
        let (tag, wt) = decode_key(&mut top)?;
        if tag == 2 {
            scratch.merge(take_len(&mut top).ok_or(Malformed)?)?;
        } else {
            skip_field(wt, tag, &mut top, DecodeContext::default())?;
        }
    }
    Ok(())
}

/// Largest observation capacity the reused scratch can retain with limit `max`: it only
/// grows while holding at most `max` observations, and `Vec` growth at most doubles the
/// previous capacity (minimum 4). Used by the memory bound (`budget.rs`).
/// Upper bound, in bytes, of the observation vector while one fast-path frame of at most
/// `max_frame_bytes` is merged, before an oversized result is dropped: at most
/// `max_frame_bytes / 2` observations; during the last reallocation the old and the new
/// buffer coexist (capacity c and at most 2c, with c ≤ that count), hence the factor 3.
pub const fn transient_decode_bound_bytes(max_frame_bytes: u32) -> u128 {
    3 * (max_frame_bytes as u128 / MIN_OBSERVATION_WIRE_BYTES as u128 + 1)
        * std::mem::size_of::<pb::Observation>() as u128
}

pub const fn retained_capacity_bound(max: u32) -> usize {
    let twice = (max as usize).saturating_mul(2);
    if twice < 4 { 4 } else { twice }
}

/// Header checks shared by [`batch_from_wire`] and oversized batches: the same error,
/// in the same order, whether or not the observations were materialized.
pub fn check_batch_header(mode: i32, observations: u64, max_observations: u32) -> Result<BatchMode, ErrorCode> {
    let mode = match pb::BatchMode::try_from(mode) {
        Ok(pb::BatchMode::FullState) => BatchMode::FullState,
        Ok(pb::BatchMode::Delta) => BatchMode::Delta,
        Ok(pb::BatchMode::Unspecified) | Err(_) => return Err(ErrorCode::InvalidMode),
    };
    if observations > u64::from(max_observations) {
        return Err(ErrorCode::BatchTooLarge);
    }
    Ok(mode)
}

/// Unwrap a generically decoded message that turned out to be a batch.
pub fn as_batch(msg: &pb::DriverMessage) -> Option<&pb::ObservationBatch> {
    match &msg.body {
        Some(driver_message::Body::Batch(b)) => Some(b),
        _ => None,
    }
}

/// Convert a decoded wire batch into a core batch. Structural problems that the core
/// type system cannot even represent (unknown mode, missing value) are rejected here,
/// before the sequencer, and therefore also without any state change.
pub fn batch_from_wire(
    source_id: SourceId,
    link_generation: u64,
    b: &pb::ObservationBatch,
    max_observations: u32,
) -> Result<ObservationBatch, ErrorCode> {
    // Size checked again by the core; done here to avoid converting oversized vectors.
    let mode = check_batch_header(b.mode, b.observations.len() as u64, max_observations)?;
    let mut observations = Vec::with_capacity(b.observations.len());
    for o in &b.observations {
        let value = match o.value {
            Some(pb::observation::Value::BoolValue(v)) => Value::Bool(v),
            Some(pb::observation::Value::I64Value(v)) => Value::I64(v),
            None => return Err(ErrorCode::MissingValue),
        };
        observations.push(Observation { channel_id: ChannelId(o.channel_id), value });
    }
    Ok(ObservationBatch { source_id, link_generation, mode, source_sequence: b.source_sequence, observations })
}

/// Error code reported to the driver for a core rejection.
pub fn error_code(e: &ApplyError) -> ErrorCode {
    match e {
        ApplyError::OperationSequenceMismatch { .. } => ErrorCode::Internal,
        ApplyError::OperationSequenceExhausted => ErrorCode::OperationSequenceExhausted,
        ApplyError::UnknownSource(_) => ErrorCode::UnknownSource,
        ApplyError::InvalidEpoch => ErrorCode::InvalidHello,
        ApplyError::NonMonotonicLinkGeneration { .. } | ApplyError::UnknownLinkGeneration { .. } => ErrorCode::Internal,
        ApplyError::LinkNotActive { .. } => ErrorCode::LinkSuperseded,
        ApplyError::EmptyBatch => ErrorCode::EmptyBatch,
        ApplyError::BatchTooLarge { .. } => ErrorCode::BatchTooLarge,
        ApplyError::UnknownChannel(_) => ErrorCode::UnknownChannel,
        ApplyError::DuplicateChannel(_) => ErrorCode::DuplicateChannel,
        ApplyError::TypeMismatch { .. } => ErrorCode::TypeMismatch,
        ApplyError::IncompleteFullState { .. } => ErrorCode::IncompleteFullState,
        ApplyError::InvalidSequence => ErrorCode::InvalidSequence,
        ApplyError::ResyncRequired => ErrorCode::ResyncRequired,
        ApplyError::SequenceGap { .. } => ErrorCode::SequenceGap,
        ApplyError::StaleSequence { .. } => ErrorCode::StaleSequence,
        ApplyError::SequenceConflict { .. } => ErrorCode::SequenceConflict,
        ApplyError::SequenceOverflow => ErrorCode::SequenceOverflow,
        ApplyError::RevisionOverflow(_) => ErrorCode::RevisionOverflow,
        // VS-004: produced only by the runtime watchdog, never by a driver message.
        ApplyError::SupervisionNotConfigured(_) | ApplyError::InvalidFreshnessToken { .. } => ErrorCode::Internal,
    }
}

pub fn continuity(c: Continuity) -> pb::Continuity {
    match c {
        Continuity::Continuous => pb::Continuity::Continuous,
        Continuity::BaselineEstablished => pb::Continuity::BaselineEstablished,
        Continuity::Resynchronized => pb::Continuity::Resynchronized,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn obs(c: u32, v: Option<pb::observation::Value>) -> pb::Observation {
        pb::Observation { channel_id: c, value: v }
    }

    fn reference(bytes: &[u8]) -> Result<pb::DriverMessage, ()> {
        pb::DriverMessage::decode(bytes).map_err(|_| ())
    }

    fn fast(bytes: &[u8], scratch: &mut pb::ObservationBatch) -> Result<pb::DriverMessage, ()> {
        match decode_driver_message(bytes, scratch, u32::MAX) {
            Ok(Decoded::Batch(b)) => Ok(pb::DriverMessage { body: Some(driver_message::Body::Batch(b.clone())) }),
            Ok(Decoded::Other(m)) => Ok(m),
            Ok(d @ (Decoded::Oversized { .. } | Decoded::NotBatch | Decoded::Heartbeat { .. })) => {
                panic!("no limit, got {d:?}")
            }
            Err(_) => Err(()),
        }
    }

    fn arb_value() -> impl Strategy<Value = Option<pb::observation::Value>> {
        prop_oneof![
            Just(None),
            any::<bool>().prop_map(|b| Some(pb::observation::Value::BoolValue(b))),
            any::<i64>().prop_map(|i| Some(pb::observation::Value::I64Value(i))),
        ]
    }

    fn arb_batch() -> impl Strategy<Value = pb::ObservationBatch> {
        (
            -2i32..4,
            prop_oneof![Just(0u64), Just(1), Just(u64::MAX), any::<u64>()],
            prop::collection::vec((0u32..300, arb_value()), 0..40),
        )
            .prop_map(|(mode, source_sequence, o)| pb::ObservationBatch {
                mode,
                source_sequence,
                observations: o.into_iter().map(|(c, v)| obs(c, v)).collect(),
            })
    }

    /// Encodings a fuzzer would not easily reach: concatenated messages (proto merge
    /// semantics), trailing garbage, other bodies, truncation.
    fn arb_frame() -> impl Strategy<Value = Vec<u8>> {
        (prop::collection::vec(arb_batch(), 1..4), 0u8..6, prop::collection::vec(any::<u8>(), 0..8)).prop_map(
            |(batches, shape, junk)| {
                let enc = |b: &pb::ObservationBatch| {
                    pb::DriverMessage { body: Some(driver_message::Body::Batch(b.clone())) }.encode_to_vec()
                };
                match shape {
                    0 => enc(&batches[0]),
                    1 => batches.iter().flat_map(enc).collect(), // repeated batch field
                    2 => {
                        let mut v = enc(&batches[0]);
                        v.extend_from_slice(&junk);
                        v
                    }
                    3 => {
                        let mut v = enc(&batches[0]);
                        v.truncate(v.len().saturating_sub(1 + junk.len() % 4));
                        v
                    }
                    4 => scada_protocol::driver_v1::DriverMessage {
                        body: Some(driver_message::Body::Hello(pb::Hello {
                            driver_id: "x".into(),
                            source_id: 1,
                            source_epoch: 2,
                            supported_protocol_majors: vec![1],
                            protocol_minor: 0,
                        })),
                    }
                    .encode_to_vec(),
                    _ => junk,
                }
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

        /// C3 equivalence: same result as the generic decoder for any frame, whatever the
        /// scratch buffer held before (stale fields must never leak into the next batch).
        #[test]
        fn fast_decode_equals_generic(prev in arb_batch(), frame in arb_frame()) {
            let mut scratch = prev;
            prop_assert_eq!(fast(&frame, &mut scratch), reference(&frame));
        }

        #[test]
        fn fast_decode_equals_generic_on_raw_bytes(prev in arb_batch(), frame in prop::collection::vec(any::<u8>(), 0..64)) {
            let mut scratch = prev;
            let mut f = frame.clone();
            prop_assert_eq!(fast(&f, &mut scratch), reference(&f));
            // Force the fast-path prefix onto random bytes.
            f.insert(0, BATCH_KEY);
            f.insert(1, (frame.len() & 0x7f) as u8);
            prop_assert_eq!(fast(&f, &mut scratch), reference(&f));
        }
    }

    /// Review P1: the same agreement with a small observation limit, so that the
    /// count, the exact scan and the final-batch merge are all exercised.
    fn check_limited(frame: &[u8], limit: u32, scratch: &mut pb::ObservationBatch) -> Result<(), TestCaseError> {
        let generic = pb::DriverMessage::decode(frame);
        match (&generic, decode_driver_message(frame, scratch, limit)) {
            (Err(_), Err(_)) => {}
            (Ok(g), Ok(Decoded::Batch(b))) => {
                prop_assert!(b.observations.len() <= limit as usize);
                prop_assert_eq!(g, &pb::DriverMessage { body: Some(driver_message::Body::Batch(b.clone())) });
            }
            (Ok(g), Ok(Decoded::Other(m))) => prop_assert_eq!(g, &m),
            (Ok(g), Ok(Decoded::Oversized { mode, source_sequence, observations })) => match &g.body {
                Some(driver_message::Body::Batch(b)) => {
                    prop_assert!(observations > u64::from(limit));
                    prop_assert_eq!(
                        (b.mode, b.source_sequence, b.observations.len() as u64),
                        (mode, source_sequence, observations)
                    );
                }
                other => prop_assert!(false, "Oversized, generic {:?}", other),
            },
            (Ok(g), Ok(Decoded::NotBatch)) => {
                prop_assert!(matches!(g.body, Some(driver_message::Body::Hello(_))), "NotBatch, generic {:?}", g)
            }
            (Ok(g), Ok(Decoded::Heartbeat { sequence })) => {
                prop_assert_eq!(&g.body, &Some(driver_message::Body::Heartbeat(pb::Heartbeat { sequence })))
            }
            (g, f) => prop_assert!(false, "disagree: {:?} vs {:?}", g, f),
        }
        prop_assert!(scratch.observations.capacity() <= retained_capacity_bound(limit));
        Ok(())
    }

    fn arb_mixed_frame() -> impl Strategy<Value = Vec<u8>> {
        // Sequences of batch / hello / unknown fields: oneof replacement and merging.
        let field = prop_oneof![
            arb_batch().prop_map(|b| pb::DriverMessage { body: Some(driver_message::Body::Batch(b)) }.encode_to_vec()),
            Just(
                pb::DriverMessage {
                    body: Some(driver_message::Body::Hello(pb::Hello { driver_id: "h".into(), ..Default::default() }))
                }
                .encode_to_vec()
            ),
            Just(vec![(9 << 3), 1]), // unknown varint field 9
            // VS-004: heartbeat bodies, also empty ones (merging keeps the sequence).
            any::<u64>().prop_map(|sequence| pb::DriverMessage {
                body: Some(driver_message::Body::Heartbeat(pb::Heartbeat { sequence }))
            }
            .encode_to_vec()),
            Just(vec![(3 << 3) | 2, 0]),
        ];
        prop::collection::vec(field, 1..5).prop_map(|v| v.concat())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 3000, ..ProptestConfig::default() })]

        #[test]
        fn limited_decode_agrees_with_generic(prev in arb_batch(), frame in arb_frame(), limit in 0u32..8) {
            let mut scratch = pb::ObservationBatch::default();
            // A scratch already within the limit (the decoder never leaves it larger).
            scratch.observations.extend(prev.observations.into_iter().take(limit as usize));
            check_limited(&frame, limit, &mut scratch)?;
        }

        #[test]
        fn limited_decode_agrees_on_mixed_bodies(frame in arb_mixed_frame(), limit in 0u32..8) {
            let mut scratch = pb::ObservationBatch::default();
            check_limited(&frame, limit, &mut scratch)?;
        }
    }

    #[test]
    fn scratch_capacity_is_reused() {
        let big = pb::DriverMessage {
            body: Some(driver_message::Body::Batch(pb::ObservationBatch {
                mode: 2,
                source_sequence: 9,
                observations: (1..=128).map(|c| obs(c, Some(pb::observation::Value::I64Value(1)))).collect(),
            })),
        }
        .encode_to_vec();
        let mut scratch = pb::ObservationBatch::default();
        assert!(matches!(decode_driver_message(&big, &mut scratch, 128), Ok(Decoded::Batch(_))));
        let cap = scratch.observations.capacity();
        for _ in 0..100 {
            assert!(matches!(decode_driver_message(&big, &mut scratch, 128), Ok(Decoded::Batch(_))));
        }
        assert_eq!(scratch.observations.capacity(), cap);
    }
}

#[cfg(test)]
mod adversarial {
    //! P1 (review of PR #2): a frame within `max_frame_bytes` can carry far more
    //! observations than `max_observations_per_batch`, each as small as 2 bytes (an empty
    //! `Observation`). The limit must be enforced before they are materialized, and the
    //! reused scratch must never keep more than the limit.
    use super::*;

    /// `DriverMessage{batch{mode, seq, n × empty Observation}}`, hand-encoded.
    pub(crate) fn many_empty_observations(n: usize, mode: i32, seq: u64) -> Vec<u8> {
        let mut inner = Vec::new();
        prost::encoding::int32::encode(1, &mode, &mut inner);
        prost::encoding::uint64::encode(2, &seq, &mut inner);
        for _ in 0..n {
            inner.extend_from_slice(&[(3 << 3) | 2, 0]);
        }
        let mut out = vec![BATCH_KEY];
        prost::encoding::encode_varint(inner.len() as u64, &mut out);
        out.extend_from_slice(&inner);
        out
    }

    #[test]
    fn oversized_batch_is_not_retained() {
        // 64 KiB frame: ~32 000 observations, limit 128.
        let frame = many_empty_observations(32_000, 2, 7);
        assert!(frame.len() <= 65_536);
        let mut scratch = pb::ObservationBatch::default();
        match decode_driver_message(&frame, &mut scratch, 128) {
            Ok(Decoded::Oversized { mode, source_sequence, observations }) => {
                assert_eq!((mode, source_sequence, observations), (2, 7, 32_000));
            }
            other => panic!("expected Oversized, got {other:?}"),
        }
        assert!(scratch.observations.capacity() <= 128, "scratch grew to {}", scratch.observations.capacity());
    }

    #[test]
    fn oversized_with_invalid_mode_reports_invalid_mode_first() {
        // Same error order as a materialized batch: mode, then size.
        assert_eq!(check_batch_header(7, 1_000, 128), Err(ErrorCode::InvalidMode));
        assert_eq!(check_batch_header(2, 1_000, 128), Err(ErrorCode::BatchTooLarge));
        assert_eq!(check_batch_header(2, 128, 128), Ok(BatchMode::Delta));
    }

    #[test]
    fn oversized_then_corrupt_is_malformed_without_retention() {
        let mut frame = many_empty_observations(10_000, 2, 1);
        frame.push(0xff); // trailing garbage: an invalid key
        let mut scratch = pb::ObservationBatch::default();
        assert_eq!(decode_driver_message(&frame, &mut scratch, 128).unwrap_err(), Malformed);
        assert!(pb::DriverMessage::decode(frame.as_slice()).is_err());
        assert_eq!(scratch.observations.capacity(), 0);
    }

    #[test]
    fn oversized_batch_replaced_by_hello_is_not_a_batch() {
        let mut frame = many_empty_observations(10_000, 2, 1);
        frame.extend(
            pb::DriverMessage { body: Some(driver_message::Body::Hello(pb::Hello::default())) }.encode_to_vec(),
        );
        let mut scratch = pb::ObservationBatch::default();
        assert!(matches!(decode_driver_message(&frame, &mut scratch, 128), Ok(Decoded::NotBatch)));
        assert_eq!(scratch.observations.capacity(), 0);
    }

    #[test]
    fn small_batch_after_discarded_oversized_one_is_decoded_within_the_limit() {
        // batch(10 000 obs), hello, batch(3 obs): prost ends with the 3-observation batch.
        let mut frame = many_empty_observations(10_000, 2, 1);
        frame.extend(
            pb::DriverMessage { body: Some(driver_message::Body::Hello(pb::Hello::default())) }.encode_to_vec(),
        );
        frame.extend(many_empty_observations(3, 1, 9));
        let mut scratch = pb::ObservationBatch::default();
        let generic = pb::DriverMessage::decode(frame.as_slice()).unwrap();
        match decode_driver_message(&frame, &mut scratch, 128) {
            Ok(Decoded::Batch(b)) => {
                assert_eq!(generic, pb::DriverMessage { body: Some(driver_message::Body::Batch(b.clone())) });
                assert_eq!(b.observations.len(), 3);
            }
            other => panic!("{other:?}"),
        }
        assert!(scratch.observations.capacity() <= 4);
    }
}

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
}

/// `DriverMessage` field 2 (`batch`), wire type 2 (length-delimited).
const BATCH_KEY: u8 = (2 << 3) | 2;

/// Decode one data-phase frame (C3).
///
/// Fast path: a frame that is exactly one `batch` field — what every conforming driver
/// sends — is merged into `scratch` after resetting every field to its default, so the
/// observation vector keeps its capacity across frames instead of being regrown from zero
/// for every batch. Proto3 merging into a default message is decoding, so the result is
/// the same `ObservationBatch` that `DriverMessage::decode` would produce. Every other
/// shape (other bodies, repeated or unknown top-level fields, trailing bytes) takes the
/// generic decoder. Only the key byte and one length varint are parsed by hand, both
/// bounds-checked; there is no unsafe code and no custom parsing of observations.
pub fn decode_driver_message<'a>(
    bytes: &[u8],
    scratch: &'a mut pb::ObservationBatch,
) -> Result<Decoded<'a>, prost::DecodeError> {
    if let Some((&BATCH_KEY, mut rest)) = bytes.split_first()
        && let Ok(len) = prost::encoding::decode_varint(&mut rest)
        && usize::try_from(len).is_ok_and(|l| l == rest.len())
    {
        // `clear` resets every field (including any added later) and keeps the
        // observation vector's capacity.
        scratch.clear();
        scratch.merge(rest)?;
        return Ok(Decoded::Batch(scratch));
    }
    let msg = pb::DriverMessage::decode(bytes)?;
    Ok(Decoded::Other(msg))
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
    let mode = match pb::BatchMode::try_from(b.mode) {
        Ok(pb::BatchMode::FullState) => BatchMode::FullState,
        Ok(pb::BatchMode::Delta) => BatchMode::Delta,
        Ok(pb::BatchMode::Unspecified) | Err(_) => return Err(ErrorCode::InvalidMode),
    };
    // Checked again by the core; done here to avoid converting oversized vectors.
    if b.observations.len() > max_observations as usize {
        return Err(ErrorCode::BatchTooLarge);
    }
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
        match decode_driver_message(bytes, scratch) {
            Ok(Decoded::Batch(b)) => Ok(pb::DriverMessage { body: Some(driver_message::Body::Batch(b.clone())) }),
            Ok(Decoded::Other(m)) => Ok(m),
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
        assert!(matches!(decode_driver_message(&big, &mut scratch), Ok(Decoded::Batch(_))));
        let cap = scratch.observations.capacity();
        for _ in 0..100 {
            assert!(matches!(decode_driver_message(&big, &mut scratch), Ok(Decoded::Batch(_))));
        }
        assert_eq!(scratch.observations.capacity(), cap);
    }
}

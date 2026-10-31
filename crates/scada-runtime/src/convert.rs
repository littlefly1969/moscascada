//! Protocol <-> core conversions. The core never sees protobuf types.

use scada_core::{ApplyError, BatchMode, ChannelId, Continuity, Observation, ObservationBatch, SourceId, Value};
use scada_protocol::driver_v1::{self as pb, ErrorCode};

/// Convert a decoded wire batch into a core batch. Structural problems that the core
/// type system cannot even represent (unknown mode, missing value) are rejected here,
/// before the sequencer, and therefore also without any state change.
pub fn batch_from_wire(
    source_id: SourceId,
    link_generation: u64,
    b: pb::ObservationBatch,
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
    for o in b.observations {
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

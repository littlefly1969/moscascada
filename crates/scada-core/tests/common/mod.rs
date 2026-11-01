#![allow(dead_code)]
//! Shared fixtures: the demo configuration of brief §6 and input builders.

use std::sync::Arc;

use scada_core::*;

pub const SRC: SourceId = SourceId(1001);
pub const DOOR: TagId = TagId(101);
pub const TAMPER: TagId = TagId(102);
pub const TEMP: TagId = TagId(103);

pub fn demo_spec() -> ConfigSpec {
    ConfigSpec {
        limits: CoreLimits::default(),
        tags: vec![
            TagSpec { tag_id: DOOR, name: "door_contact".into(), value_type: ValueType::Bool },
            TagSpec { tag_id: TAMPER, name: "tamper_switch".into(), value_type: ValueType::Bool },
            TagSpec { tag_id: TEMP, name: "case_temperature_centi_c".into(), value_type: ValueType::I64 },
        ],
        sources: vec![SourceSpec {
            source_id: SRC,
            name: "sim-enclosure-1".into(),
            channels: vec![
                ChannelSpec { channel_id: ChannelId(1), tag_id: DOOR, value_type: ValueType::Bool },
                ChannelSpec { channel_id: ChannelId(2), tag_id: TAMPER, value_type: ValueType::Bool },
                ChannelSpec { channel_id: ChannelId(3), tag_id: TEMP, value_type: ValueType::I64 },
            ],
            supervision: Supervision::NONE,
        }],
    }
}

pub fn demo_kernel() -> Kernel {
    Kernel::new(Arc::new(ValidatedConfig::validate(demo_spec()).expect("demo config valid")))
}

pub fn b(ch: u32, v: bool) -> Observation {
    Observation { channel_id: ChannelId(ch), value: Value::Bool(v) }
}

pub fn i(ch: u32, v: i64) -> Observation {
    Observation { channel_id: ChannelId(ch), value: Value::I64(v) }
}

pub fn connect(generation: u64, epoch: u64) -> Operation {
    Operation::SourceConnected { source_id: SRC, link_generation: generation, source_epoch: epoch }
}

pub fn disconnect(generation: u64) -> Operation {
    Operation::SourceDisconnected { source_id: SRC, link_generation: generation }
}

pub fn batch(generation: u64, mode: BatchMode, seq: u64, obs: Vec<Observation>) -> Operation {
    Operation::ObservationBatch(ObservationBatch {
        source_id: SRC,
        link_generation: generation,
        mode,
        source_sequence: seq,
        observations: obs,
    })
}

pub fn full(generation: u64, seq: u64, door: bool, tamper: bool, temp: i64) -> Operation {
    batch(generation, BatchMode::FullState, seq, vec![b(1, door), b(2, tamper), i(3, temp)])
}

/// Applies `op` with the next operation sequence.
pub fn step(k: &mut Kernel, op: Operation) -> Result<TransitionOutcome, ApplyError> {
    let operation_sequence = k.last_operation_sequence() + 1;
    k.apply(SequencedInput { operation_sequence, operation: op })
}

/// Asserts that `op` is rejected with `err` and that state and digest are unchanged.
pub fn assert_rejected_unchanged(k: &mut Kernel, op: Operation, err: ApplyError) {
    let snap = k.logical_snapshot();
    let digest = k.state_digest();
    assert_eq!(step(k, op), Err(err));
    assert_eq!(snap, k.logical_snapshot());
    assert_eq!(digest, k.state_digest());
}

pub fn tag(k: &Kernel, t: TagId) -> TagSnapshot {
    k.tag(t).expect("tag exists")
}

pub const UNAVAILABLE: Quality = Quality::Uncertain(UncertainReason::SourceUnavailable);

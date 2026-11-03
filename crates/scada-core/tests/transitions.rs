//! DoD 2-8 and 10: transition rules, atomicity, sequencing, quality.

mod common;

use common::*;
use scada_core::*;

fn connected() -> Kernel {
    let mut k = demo_kernel();
    step(&mut k, connect(1, 0xE1)).unwrap();
    k
}

fn baseline() -> Kernel {
    let mut k = connected();
    step(&mut k, full(1, 1, false, false, 2150)).unwrap();
    k
}

#[test]
fn initial_state_is_unknown() {
    let k = demo_kernel();
    for t in [DOOR, TAMPER, TEMP] {
        let s = tag(&k, t);
        assert_eq!((s.value, s.quality, s.revision), (None, Quality::Unknown, 0));
    }
    assert_eq!(k.last_operation_sequence(), 0);
}

// DoD 2
#[test]
fn value_quality_revision_and_identical_reacquisition() {
    let mut k = connected();
    let out = step(&mut k, full(1, 1, false, true, 2150)).unwrap();
    assert_eq!(
        out,
        TransitionOutcome::Applied {
            operation_sequence: 2,
            kind: AppliedKind::Batch {
                continuity: Continuity::BaselineEstablished,
                changed_tags: 3,
                reconciled: false
            }
        }
    );
    let t = tag(&k, TAMPER);
    assert_eq!((t.value, t.quality, t.revision), (Some(Value::Bool(true)), Quality::Good, 1));
    let t = tag(&k, TEMP);
    assert_eq!((t.value, t.quality, t.revision), (Some(Value::I64(2150)), Quality::Good, 1));

    // Same value again: applied (new operation, new source sequence) but no revision change.
    let out = step(&mut k, batch(1, BatchMode::Delta, 2, vec![i(3, 2150)])).unwrap();
    assert_eq!(
        out,
        TransitionOutcome::Applied {
            operation_sequence: 3,
            kind: AppliedKind::Batch { continuity: Continuity::Continuous, changed_tags: 0, reconciled: false }
        }
    );
    assert_eq!(tag(&k, TEMP).revision, 1);
    assert_eq!(k.source_status(SRC).unwrap().last_accepted_source_sequence, Some(2));

    // Different value: revision +1.
    step(&mut k, batch(1, BatchMode::Delta, 3, vec![i(3, 2175)])).unwrap();
    assert_eq!((tag(&k, TEMP).value, tag(&k, TEMP).revision), (Some(Value::I64(2175)), 2));
    // A physical fault value received correctly is Good data.
    step(&mut k, batch(1, BatchMode::Delta, 4, vec![b(2, true)])).unwrap();
    assert_eq!(tag(&k, TAMPER).quality, Quality::Good);
}

// DoD 3
#[test]
fn batch_with_unknown_channel_changes_nothing() {
    let mut k = baseline();
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 2, vec![b(1, true), i(3, 3000), b(99, true)]),
        ApplyError::UnknownChannel(ChannelId(99)),
    );
    assert_eq!(k.last_operation_sequence(), 2);
    // The same sequence number is still usable afterwards.
    step(&mut k, batch(1, BatchMode::Delta, 2, vec![b(1, true), i(3, 3000)])).unwrap();
}

#[test]
fn other_validation_errors_change_nothing() {
    let mut k = baseline();
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 2, vec![b(1, true), b(1, false)]),
        ApplyError::DuplicateChannel(ChannelId(1)),
    );
    assert_rejected_unchanged(&mut k, batch(1, BatchMode::Delta, 2, vec![]), ApplyError::EmptyBatch);
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::FullState, 2, vec![b(1, true), b(2, true)]),
        ApplyError::IncompleteFullState { expected: 3, got: 2 },
    );
    let many: Vec<_> = (0..129).map(|n| b(1000 + n, true)).collect();
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 2, many),
        ApplyError::BatchTooLarge { len: 129, max: 128 },
    );
    assert_rejected_unchanged(&mut k, batch(1, BatchMode::Delta, 0, vec![b(1, true)]), ApplyError::InvalidSequence);
    let mut other = batch(1, BatchMode::Delta, 2, vec![b(1, true)]);
    if let Operation::ObservationBatch(x) = &mut other {
        x.source_id = SourceId(5);
    }
    assert_rejected_unchanged(&mut k, other, ApplyError::UnknownSource(SourceId(5)));
}

// DoD 4
#[test]
fn identical_duplicate_of_last_batch() {
    let mut k = baseline();
    let op = batch(1, BatchMode::Delta, 2, vec![i(3, 2200), b(1, true)]);
    step(&mut k, op).unwrap();
    let (snap, digest, last) = (k.logical_snapshot(), k.state_digest(), k.last_operation_sequence());
    // Same content in a different observation order is the same canonical payload.
    let dup = batch(1, BatchMode::Delta, 2, vec![b(1, true), i(3, 2200)]);
    assert_eq!(step(&mut k, dup), Ok(TransitionOutcome::Duplicate { source_sequence: 2 }));
    assert_eq!((snap, digest, last), (k.logical_snapshot(), k.state_digest(), k.last_operation_sequence()));
    // Full state duplicates are detected too.
    let mut k = baseline();
    let d = k.state_digest();
    assert_eq!(step(&mut k, full(1, 1, false, false, 2150)), Ok(TransitionOutcome::Duplicate { source_sequence: 1 }));
    assert_eq!(d, k.state_digest());
}

// DoD 5
#[test]
fn same_sequence_different_payload_is_a_conflict() {
    let mut k = baseline();
    step(&mut k, batch(1, BatchMode::Delta, 2, vec![i(3, 2200)])).unwrap();
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 2, vec![i(3, 2201)]),
        ApplyError::SequenceConflict { source_sequence: 2 },
    );
    // An older sequence is stale whatever its content.
    assert_rejected_unchanged(
        &mut k,
        full(1, 1, false, false, 2150),
        ApplyError::StaleSequence { last_accepted: 2, got: 1 },
    );
}

// DoD 6
#[test]
fn gap_stale_type_and_resync() {
    let mut k = baseline();
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 5, vec![b(1, true)]),
        ApplyError::SequenceGap { expected: 2, got: 5 },
    );
    step(&mut k, batch(1, BatchMode::Delta, 2, vec![b(1, true)])).unwrap();
    step(&mut k, batch(1, BatchMode::Delta, 3, vec![b(1, false)])).unwrap();
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 2, vec![b(1, true)]),
        ApplyError::StaleSequence { last_accepted: 3, got: 2 },
    );
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 4, vec![i(1, 1)]),
        ApplyError::TypeMismatch { channel_id: ChannelId(1), expected: ValueType::Bool, got: ValueType::I64 },
    );
    // A full state after a gap resynchronizes and says continuity is not proven.
    assert_eq!(
        step(&mut k, full(1, 10, true, true, 1)),
        Ok(TransitionOutcome::Applied {
            operation_sequence: 5,
            kind: AppliedKind::Batch { continuity: Continuity::Resynchronized, changed_tags: 3, reconciled: false }
        })
    );
    // Delta without baseline in a new epoch.
    step(&mut k, disconnect(1)).unwrap();
    step(&mut k, connect(2, 0xE2)).unwrap();
    assert_rejected_unchanged(&mut k, batch(2, BatchMode::Delta, 11, vec![b(1, true)]), ApplyError::ResyncRequired);
}

#[test]
fn first_full_state_may_start_at_any_positive_sequence() {
    let mut k = connected();
    assert_eq!(
        step(&mut k, full(1, 4242, false, false, 0)),
        Ok(TransitionOutcome::Applied {
            operation_sequence: 2,
            kind: AppliedKind::Batch {
                continuity: Continuity::BaselineEstablished,
                changed_tags: 3,
                reconciled: false
            }
        })
    );
}

// DoD 7
#[test]
fn disconnect_preserves_value_reconnect_stays_uncertain() {
    let mut k = baseline();
    step(&mut k, batch(1, BatchMode::Delta, 2, vec![i(3, 2300)])).unwrap();
    assert_eq!(
        step(&mut k, disconnect(1)).unwrap(),
        TransitionOutcome::Applied { operation_sequence: 4, kind: AppliedKind::Disconnected { degraded_tags: 3 } }
    );
    let t = tag(&k, TEMP);
    assert_eq!((t.value, t.quality, t.revision), (Some(Value::I64(2300)), UNAVAILABLE, 3));
    // Batches on the closed link are refused.
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 3, vec![b(1, true)]),
        ApplyError::LinkNotActive { got: 1 },
    );

    // Reconnect without producer restart (same epoch): still uncertain, sequence continues.
    let out = step(&mut k, connect(2, 0xE1)).unwrap();
    assert_eq!(
        out,
        TransitionOutcome::Applied {
            operation_sequence: 5,
            kind: AppliedKind::Connected {
                full_state_required: false,
                last_accepted_source_sequence: Some(2),
                degraded_tags: 0
            }
        }
    );
    for t in [DOOR, TAMPER, TEMP] {
        assert_eq!(tag(&k, t).quality, UNAVAILABLE);
    }
    // Retransmitting the last acknowledged batch is a safe duplicate.
    assert_eq!(
        step(&mut k, batch(2, BatchMode::Delta, 2, vec![i(3, 2300)])),
        Ok(TransitionOutcome::Duplicate { source_sequence: 2 })
    );
    assert_eq!(tag(&k, TEMP).quality, UNAVAILABLE);
    // Only a valid observation restores Good, and only for the tags it carries.
    step(&mut k, batch(2, BatchMode::Delta, 3, vec![i(3, 2300)])).unwrap();
    let t = tag(&k, TEMP);
    assert_eq!((t.value, t.quality, t.revision), (Some(Value::I64(2300)), Quality::Good, 4));
    assert_eq!(tag(&k, DOOR).quality, UNAVAILABLE);
}

#[test]
fn never_observed_tags_stay_unknown_on_disconnect() {
    let mut k = connected();
    step(&mut k, disconnect(1)).unwrap();
    assert_eq!(tag(&k, DOOR).quality, Quality::Unknown);
    assert_eq!(tag(&k, DOOR).revision, 0);
}

#[test]
fn new_epoch_requires_full_state() {
    let mut k = baseline();
    step(&mut k, disconnect(1)).unwrap();
    let out = step(&mut k, connect(2, 0xE2)).unwrap();
    assert!(matches!(
        out,
        TransitionOutcome::Applied {
            kind: AppliedKind::Connected { full_state_required: true, last_accepted_source_sequence: None, .. },
            ..
        }
    ));
    // Old sequence numbers are not remembered across epochs.
    step(&mut k, full(2, 1, true, false, 1)).unwrap();
}

// DoD 8
#[test]
fn late_disconnect_of_old_generation_is_ignored() {
    let mut k = baseline();
    // Takeover: generation 2 connects while generation 1 is still marked connected.
    step(&mut k, connect(2, 0xE1)).unwrap();
    step(&mut k, batch(2, BatchMode::Delta, 2, vec![i(3, 1)])).unwrap();
    let (snap, digest) = (k.logical_snapshot(), k.state_digest());
    assert_eq!(step(&mut k, disconnect(1)), Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration)));
    assert_eq!((snap.clone(), digest), (k.logical_snapshot(), k.state_digest()));
    assert_eq!(tag(&k, TEMP).quality, Quality::Good);
    // Messages of the superseded link cannot overwrite the new generation's state.
    assert_rejected_unchanged(
        &mut k,
        batch(1, BatchMode::Delta, 3, vec![i(3, 99)]),
        ApplyError::LinkNotActive { got: 1 },
    );
    // Generations never go backwards, unknown future generations are refused.
    assert_rejected_unchanged(&mut k, connect(2, 0xE1), ApplyError::NonMonotonicLinkGeneration { current: 2, got: 2 });
    assert_rejected_unchanged(&mut k, disconnect(7), ApplyError::UnknownLinkGeneration { got: 7 });
    // A second disconnect of the same generation is a no-op.
    step(&mut k, disconnect(2)).unwrap();
    let d = k.state_digest();
    assert_eq!(step(&mut k, disconnect(2)), Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration)));
    assert_eq!(d, k.state_digest());
}

#[test]
fn invalid_connect_inputs() {
    let mut k = demo_kernel();
    assert_rejected_unchanged(&mut k, connect(1, 0), ApplyError::InvalidEpoch);
    assert_rejected_unchanged(
        &mut k,
        Operation::SourceConnected { source_id: SourceId(9), link_generation: 1, source_epoch: 1 },
        ApplyError::UnknownSource(SourceId(9)),
    );
    assert_rejected_unchanged(&mut k, disconnect(1), ApplyError::UnknownLinkGeneration { got: 1 });
    assert_rejected_unchanged(&mut k, full(1, 1, true, true, 1), ApplyError::LinkNotActive { got: 1 });
}

// DoD 10
#[test]
fn out_of_order_operation_sequence_is_rejected() {
    let mut k = baseline();
    let (snap, digest) = (k.logical_snapshot(), k.state_digest());
    for bad in [0, 1, 2, 4, 1000, u64::MAX] {
        let r = k.apply(SequencedInput {
            operation_sequence: bad,
            operation: batch(1, BatchMode::Delta, 2, vec![b(1, true)]),
        });
        assert_eq!(r, Err(ApplyError::OperationSequenceMismatch { expected: 3, got: bad }));
    }
    assert_eq!((snap, digest), (k.logical_snapshot(), k.state_digest()));
}

#[test]
fn rejected_and_duplicate_inputs_do_not_consume_operation_sequence() {
    let mut k = baseline();
    assert_eq!(k.last_operation_sequence(), 2);
    let _ = step(&mut k, batch(1, BatchMode::Delta, 9, vec![b(1, true)]));
    let _ = step(&mut k, full(1, 1, false, false, 2150));
    assert_eq!(k.last_operation_sequence(), 2);
    assert_eq!(
        step(&mut k, batch(1, BatchMode::Delta, 2, vec![b(1, true)])).unwrap(),
        TransitionOutcome::Applied {
            operation_sequence: 3,
            kind: AppliedKind::Batch { continuity: Continuity::Continuous, changed_tags: 1, reconciled: false }
        }
    );
}

#[test]
fn digest_excludes_nothing_logical() {
    // Two states that differ only in dedup metadata must have different digests.
    let mut a = baseline();
    let mut b2 = baseline();
    step(&mut a, batch(1, BatchMode::Delta, 2, vec![i(3, 2150)])).unwrap();
    step(&mut b2, batch(1, BatchMode::Delta, 2, vec![b(1, false)])).unwrap();
    assert_eq!(a.logical_snapshot().tags, b2.logical_snapshot().tags, "tag cells identical (no value changed)");
    assert_ne!(a.state_digest(), b2.state_digest(), "last batch differs");
}

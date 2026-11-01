//! VS-006 (ADR 0013 D7, D9): checkpoint export/restore and the recovery fence, driven
//! through `Kernel::apply`. T11 (restart semantics), T25 (determinism) and T29 (property
//! tests) at the core level.

use std::sync::Arc;

use proptest::prelude::*;
use scada_core::*;

const S1: SourceId = SourceId(1);
const S2: SourceId = SourceId(2);
const RECOVERED: Quality = Quality::Uncertain(UncertainReason::RecoveredUnverified);

fn spec() -> ConfigSpec {
    let t = |id: u32, ty: ValueType| TagSpec { tag_id: TagId(id), name: format!("t{id}"), value_type: ty };
    let c =
        |ch: u32, id: u32, ty: ValueType| ChannelSpec { channel_id: ChannelId(ch), tag_id: TagId(id), value_type: ty };
    use ValueType::{Bool, I64};
    ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: 8 },
        tags: vec![t(11, I64), t(12, Bool), t(21, I64)],
        sources: vec![
            SourceSpec {
                source_id: S1,
                name: "s1".into(),
                channels: vec![c(1, 11, I64), c(2, 12, Bool)],
                supervision: Supervision { liveness: true, freshness: true },
            },
            SourceSpec {
                source_id: S2,
                name: "s2".into(),
                channels: vec![c(1, 21, I64)],
                supervision: Supervision::NONE,
            },
        ],
    }
}

fn alarms(capacity: u64) -> AlarmSpec {
    use AlarmCondition::*;
    let r = |id: u32, tag: u32, condition: AlarmCondition| AlarmRuleSpec {
        alarm_id: AlarmId(id),
        name: format!("a{id}"),
        tag_id: TagId(tag),
        condition,
        severity: Severity::Major,
    };
    AlarmSpec {
        max_rules: 16,
        outbox_capacity_events: capacity,
        rules: vec![
            r(1, 11, High { trip: 100, reset: 90 }),
            r(2, 11, BadQuality),
            r(3, 12, BoolEquals { expected: true }),
            r(4, 21, Low { trip: 0, reset: 5 }),
            r(5, 21, BadQuality),
        ],
    }
}

fn config(capacity: u64) -> Arc<ValidatedConfig> {
    Arc::new(ValidatedConfig::validate_with_alarms(spec(), Some(alarms(capacity))).unwrap())
}

fn step(k: &mut Kernel, op: &Operation) -> Result<TransitionOutcome, ApplyError> {
    let operation_sequence = k.last_operation_sequence() + 1;
    k.apply_ref(&SequencedInput { operation_sequence, operation: op.clone() })
}

fn ok(k: &mut Kernel, op: Operation) -> AppliedKind {
    match step(k, &op) {
        Ok(TransitionOutcome::Applied { kind, .. }) => kind,
        other => panic!("{op:?}: expected Applied, got {other:?}"),
    }
}

fn batch(s: SourceId, g: u64, mode: BatchMode, seq: u64, obs: &[(u32, Value)]) -> Operation {
    Operation::ObservationBatch(ObservationBatch {
        source_id: s,
        link_generation: g,
        mode,
        source_sequence: seq,
        observations: obs.iter().map(|&(c, value)| Observation { channel_id: ChannelId(c), value }).collect(),
    })
}

fn ring(k: &Kernel, cp: &KernelCheckpoint) -> Vec<AlarmEvent> {
    match Kernel::checkpoint_ring_window(k.config(), cp) {
        Some((a, b)) => (a..=b).map(|s| k.alarm_event(s).expect("ring event")).collect(),
        None => Vec::new(),
    }
}

fn restore(k: &Kernel) -> Kernel {
    let cp = k.export_checkpoint();
    let r = Kernel::from_checkpoint(k.config().clone(), &cp, &ring(k, &cp)).expect("restore");
    assert_eq!(r.state_digest(), k.state_digest(), "digest after restore");
    assert_eq!(r.logical_snapshot(), k.logical_snapshot(), "snapshot after restore");
    r
}

/// J10: a non-recoverable kernel refuses the fence; its digest version is unchanged.
#[test]
fn fence_needs_a_recoverable_kernel() {
    let mut k = Kernel::new(config(16));
    assert_eq!(k.state_digest_version(), STATE_DIGEST_VERSION_ALARMS);
    assert_eq!(step(&mut k, &Operation::RecoveryFence { boot_generation: 1 }), Err(ApplyError::RecoveryNotEnabled));
    let r = Kernel::new_recoverable(config(16));
    assert_eq!(r.state_digest_version(), STATE_DIGEST_VERSION_RECOVERABLE);
}

/// T11 (core): Good values and latched alarms survive a restore; the fence marks every
/// value `RecoveredUnverified`, closes links, clears `input_valid` and emits **no** event
/// (no false Returned / Raised / ACK, J5, J6). A Delta is refused until a FullState.
#[test]
fn fence_semantics_no_false_events() {
    let mut k = Kernel::new_recoverable(config(16));
    ok(&mut k, Operation::SourceConnected { source_id: S1, link_generation: 1, source_epoch: 7 });
    ok(&mut k, batch(S1, 1, BatchMode::FullState, 1, &[(1, Value::I64(150)), (2, Value::Bool(true))]));
    let before = k.outbox_status().unwrap();
    assert!(k.alarm(AlarmId(1)).unwrap().flags.active());
    let mut r = restore(&k);
    match ok(&mut r, Operation::RecoveryFence { boot_generation: 2 }) {
        AppliedKind::RecoveryFenced { links_closed, unverified_sources, tags, .. } => {
            assert_eq!((links_closed, unverified_sources, tags), (1, 1, 2));
        }
        other => panic!("{other:?}"),
    }
    let after = r.outbox_status().unwrap();
    assert_eq!(after.next_event_seq, before.next_event_seq, "the fence emits no event");
    assert_eq!(r.tag(TagId(11)).unwrap().quality, RECOVERED);
    assert_eq!(r.tag(TagId(11)).unwrap().value, Some(Value::I64(150)), "last known value kept");
    let a1 = r.alarm(AlarmId(1)).unwrap();
    assert!(a1.flags.active() && !a1.flags.input_valid(), "latched, not re-evaluated");
    assert!(r.source_status(S1).unwrap().unverified);
    // New link: full state required; Delta refused.
    match ok(&mut r, Operation::SourceConnected { source_id: S1, link_generation: 2, source_epoch: 7 }) {
        AppliedKind::Connected { full_state_required, .. } => assert!(full_state_required),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        step(&mut r, &batch(S1, 2, BatchMode::Delta, 2, &[(1, Value::I64(10))])),
        Err(ApplyError::ResyncRequired)
    );
    // The first FullState re-verifies: the return to normal is a reconciliation event.
    let ev0 = r.outbox_status().unwrap().next_event_seq;
    match ok(&mut r, batch(S1, 2, BatchMode::FullState, 2, &[(1, Value::I64(10)), (2, Value::Bool(true))])) {
        AppliedKind::Batch { reconciled, .. } => assert!(reconciled),
        other => panic!("{other:?}"),
    }
    assert!(!r.source_status(S1).unwrap().unverified);
    let e = r.alarm_event(ev0).unwrap();
    assert_eq!((e.alarm_id, e.kind), (AlarmId(1), AlarmEventKind::Returned));
    // Next batch is not a reconciliation any more.
    match ok(&mut r, batch(S1, 2, BatchMode::Delta, 3, &[(1, Value::I64(11))])) {
        AppliedKind::Batch { reconciled, .. } => assert!(!reconciled),
        other => panic!("{other:?}"),
    }
}

/// `RecoveredUnverified` outranks every other degradation until the full state.
#[test]
fn recovered_unverified_has_top_precedence() {
    let mut k = Kernel::new_recoverable(config(16));
    ok(&mut k, Operation::SourceConnected { source_id: S2, link_generation: 1, source_epoch: 1 });
    ok(&mut k, batch(S2, 1, BatchMode::FullState, 1, &[(1, Value::I64(3))]));
    ok(&mut k, Operation::RecoveryFence { boot_generation: 1 });
    ok(&mut k, Operation::SourceConnected { source_id: S2, link_generation: 2, source_epoch: 1 });
    let ev = k.outbox_status().unwrap().next_event_seq;
    ok(&mut k, Operation::SourceDisconnected { source_id: S2, link_generation: 2 });
    assert_eq!(k.tag(TagId(21)).unwrap().quality, RECOVERED);
    assert_eq!(k.outbox_status().unwrap().next_event_seq, ev, "no BadQuality evaluated on an unverified value");
}

#[derive(Debug, Clone)]
enum Step {
    Connect(bool),
    Disconnect(bool),
    Full(bool, i64, bool),
    Delta(bool, i64),
    Ack(u32),
    Watermark,
    Fence,
}

fn arb_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        1 => any::<bool>().prop_map(Step::Connect),
        1 => any::<bool>().prop_map(Step::Disconnect),
        1 => (any::<bool>(), -20i64..200, any::<bool>()).prop_map(|(s, v, b)| Step::Full(s, v, b)),
        3 => (any::<bool>(), -20i64..200).prop_map(|(s, v)| Step::Delta(s, v)),
        1 => (1u32..6).prop_map(Step::Ack),
        1 => Just(Step::Watermark),
        1 => Just(Step::Fence),
    ]
}

/// Translate a step into an operation for the current kernel state (best effort; invalid
/// inputs are simply rejected by both kernels identically).
fn op_of(k: &Kernel, s: &Step, gens: &mut [u64; 2], seqs: &mut [u64; 2], boot: &mut u64) -> Operation {
    let src = |b: bool| if b { (S1, 0usize) } else { (S2, 1usize) };
    match s {
        Step::Connect(b) => {
            let (id, i) = src(*b);
            gens[i] += 1;
            Operation::SourceConnected {
                source_id: id,
                link_generation: gens[i] + 2 * i as u64 * 1000,
                source_epoch: 1,
            }
        }
        Step::Disconnect(b) => {
            let (id, i) = src(*b);
            Operation::SourceDisconnected { source_id: id, link_generation: gens[i] + 2 * i as u64 * 1000 }
        }
        Step::Full(b, v, x) => {
            let (id, i) = src(*b);
            seqs[i] += 1;
            let obs: Vec<(u32, Value)> =
                if *b { vec![(1, Value::I64(*v)), (2, Value::Bool(*x))] } else { vec![(1, Value::I64(*v))] };
            batch(id, gens[i] + 2 * i as u64 * 1000, BatchMode::FullState, seqs[i], &obs)
        }
        Step::Delta(b, v) => {
            let (id, i) = src(*b);
            seqs[i] += 1;
            batch(id, gens[i] + 2 * i as u64 * 1000, BatchMode::Delta, seqs[i], &[(1, Value::I64(*v))])
        }
        Step::Ack(a) => {
            let token = k.alarm(AlarmId(*a)).map_or(0, |v| v.activation_token);
            Operation::AlarmAcknowledge { alarm_id: AlarmId(*a), expected_activation_token: token, actor_uid: 1000 }
        }
        Step::Watermark => {
            Operation::AdvanceDurableWatermark { up_to_event_seq: k.outbox_status().unwrap().next_event_seq - 1 }
        }
        Step::Fence => {
            *boot += 1;
            Operation::RecoveryFence { boot_generation: *boot }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// T29/T25: a kernel restored from a checkpoint taken at any point, fed the same
    /// remaining inputs, gives the same outcomes, events and digests as the original.
    #[test]
    fn checkpoint_restore_is_transparent(steps in proptest::collection::vec(arb_step(), 1..120), at in 0usize..120, cap in 4u64..24) {
        let mut a = Kernel::new_recoverable(config(cap));
        let mut gens = [0u64; 2];
        let mut seqs = [0u64; 2];
        let mut boot = 0u64;
        let mut b: Option<Kernel> = None;
        for (i, s) in steps.iter().enumerate() {
            if i == at.min(steps.len() - 1) {
                b = Some(restore(&a));
            }
            let op = op_of(&a, s, &mut gens, &mut seqs, &mut boot);
            let ra = step(&mut a, &op);
            if let Some(b) = b.as_mut() {
                let rb = step(b, &op);
                prop_assert_eq!(ra, rb);
                prop_assert_eq!(a.state_digest(), b.state_digest());
                prop_assert_eq!(a.alarm_events_digest(), b.alarm_events_digest());
            }
        }
    }
}

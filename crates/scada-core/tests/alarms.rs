//! VS-005 (ADR 0012): alarm engine and bounded outbox, driven directly through
//! `Kernel::apply`. Covers the core part of the test matrix: T02 (config), T03 (thresholds),
//! T04 (quality), T05 (episodes and ACK), T06 (multi-alarm ordering), T07 (atomicity),
//! T08 (outbox), T11 (replay) and T12 (model-based property test with outbox pressure).

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;
use scada_core::*;

const S1: SourceId = SourceId(1); // liveness + freshness supervised
const S2: SourceId = SourceId(2); // unsupervised

const SILENT: Quality = Quality::Uncertain(UncertainReason::SourceSilent);
const STALE: Quality = Quality::Uncertain(UncertainReason::StaleData);
const UNAVAILABLE: Quality = Quality::Uncertain(UncertainReason::SourceUnavailable);

fn tags_and_sources() -> ConfigSpec {
    let t = |id: u32, ty: ValueType| TagSpec { tag_id: TagId(id), name: format!("t{id}"), value_type: ty };
    let c =
        |ch: u32, id: u32, ty: ValueType| ChannelSpec { channel_id: ChannelId(ch), tag_id: TagId(id), value_type: ty };
    use ValueType::{Bool, I64};
    ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: 8 },
        tags: vec![t(11, I64), t(12, I64), t(13, Bool), t(14, I64), t(21, I64), t(22, Bool)],
        sources: vec![
            SourceSpec {
                source_id: S1,
                name: "s1".into(),
                channels: vec![c(1, 11, I64), c(2, 12, I64), c(3, 13, Bool), c(4, 14, I64)],
                supervision: Supervision { liveness: true, freshness: true },
            },
            SourceSpec {
                source_id: S2,
                name: "s2".into(),
                channels: vec![c(1, 21, I64), c(2, 22, Bool)],
                supervision: Supervision::NONE,
            },
        ],
    }
}

fn rule(id: u32, tag: u32, condition: AlarmCondition, severity: Severity) -> AlarmRuleSpec {
    AlarmRuleSpec { alarm_id: AlarmId(id), name: format!("a{id}"), tag_id: TagId(tag), condition, severity }
}

fn rules() -> Vec<AlarmRuleSpec> {
    use AlarmCondition::*;
    vec![
        // Deliberately not in id order: the table sorts.
        rule(2003, 21, BadQuality, Severity::Warning),
        rule(1001, 11, High { trip: 100, reset: 90 }, Severity::Major),
        rule(1002, 11, Low { trip: -100, reset: -90 }, Severity::Major),
        rule(1003, 11, BadQuality, Severity::Warning),
        rule(1004, 13, BoolEquals { expected: true }, Severity::Critical),
        rule(1005, 12, High { trip: 0, reset: -1 }, Severity::Info),
        rule(1006, 13, BadQuality, Severity::Warning),
        rule(2001, 21, High { trip: 50, reset: 40 }, Severity::Major),
        rule(2002, 22, BoolEquals { expected: false }, Severity::Critical),
    ]
}

fn alarm_spec(capacity: u64) -> AlarmSpec {
    AlarmSpec { max_rules: 100, outbox_capacity_events: capacity, rules: rules() }
}

fn kernel(capacity: u64) -> Kernel {
    Kernel::new(Arc::new(
        ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(alarm_spec(capacity))).unwrap(),
    ))
}

fn step(k: &mut Kernel, operation: Operation) -> Result<TransitionOutcome, ApplyError> {
    let operation_sequence = k.last_operation_sequence() + 1;
    k.apply(SequencedInput { operation_sequence, operation })
}

fn ok(k: &mut Kernel, op: Operation) -> u64 {
    match step(k, op.clone()) {
        Ok(TransitionOutcome::Applied { operation_sequence, .. }) => operation_sequence,
        other => panic!("{op:?}: expected Applied, got {other:?}"),
    }
}

fn connect(s: SourceId, g: u64) -> Operation {
    Operation::SourceConnected { source_id: s, link_generation: g, source_epoch: 7 }
}

fn disconnect(s: SourceId, g: u64) -> Operation {
    Operation::SourceDisconnected { source_id: s, link_generation: g }
}

fn obs(ch: u32, v: Value) -> Observation {
    Observation { channel_id: ChannelId(ch), value: v }
}

fn batch(s: SourceId, g: u64, mode: BatchMode, seq: u64, o: Vec<Observation>) -> Operation {
    Operation::ObservationBatch(ObservationBatch {
        source_id: s,
        link_generation: g,
        mode,
        source_sequence: seq,
        observations: o,
    })
}

/// S1 full state: tag 11, 12 (i64), 13 (bool), 14 (i64).
fn full1(g: u64, seq: u64, a: i64, b: i64, c: bool, d: i64) -> Operation {
    batch(
        S1,
        g,
        BatchMode::FullState,
        seq,
        vec![obs(1, Value::I64(a)), obs(2, Value::I64(b)), obs(3, Value::Bool(c)), obs(4, Value::I64(d))],
    )
}

fn delta1(g: u64, seq: u64, a: i64) -> Operation {
    batch(S1, g, BatchMode::Delta, seq, vec![obs(1, Value::I64(a))])
}

fn ack(id: u32, token: u64, uid: u32) -> Operation {
    Operation::AlarmAcknowledge { alarm_id: AlarmId(id), expected_activation_token: token, actor_uid: uid }
}

fn wm(up_to: u64) -> Operation {
    Operation::AdvanceDurableWatermark { up_to_event_seq: up_to }
}

fn events(k: &Kernel, after: u64) -> Vec<AlarmEvent> {
    let mut out = Vec::with_capacity(1 << 16);
    k.read_alarm_events(after, usize::MAX, &mut out).unwrap();
    out
}

fn new_events(k: &Kernel, before: &OutboxStatus) -> Vec<(u32, AlarmEventKind)> {
    events(k, before.next_event_seq - 1).iter().map(|e| (e.alarm_id.0, e.kind)).collect()
}

fn st(k: &Kernel, id: u32) -> AlarmView {
    k.alarm(AlarmId(id)).unwrap()
}

fn episode(k: &Kernel, id: u32) -> EpisodeState {
    st(k, id).flags.episode()
}

/// Full observable state: snapshot (RTDB + alarms + outbox), digest, event contents.
fn observe(k: &Kernel) -> (LogicalSnapshot, [u8; 32], Option<[u8; 32]>) {
    (k.logical_snapshot(), k.state_digest(), k.alarm_events_digest())
}

fn assert_refused_unchanged(k: &mut Kernel, op: Operation, err: ApplyError) {
    let before = observe(k);
    assert_eq!(step(k, op), Err(err));
    assert_eq!(before, observe(k));
}

// ------------------------------------------------------------------------------- T01/T02

#[test]
fn t01_no_alarms_keeps_v1_v2_encodings() {
    let mut spec = tags_and_sources();
    let sup = ValidatedConfig::validate(spec.clone()).unwrap();
    assert_eq!(sup.fingerprint_version(), 2);
    assert_eq!(ValidatedConfig::validate_with_alarms(spec.clone(), None).unwrap(), sup);
    for s in &mut spec.sources {
        s.supervision = Supervision::NONE;
    }
    let plain = ValidatedConfig::validate(spec.clone()).unwrap();
    assert_eq!(plain.fingerprint_version(), 1);
    let k = Kernel::new(Arc::new(plain));
    assert_eq!(k.state_digest_version(), 1);
    assert!(!k.alarms_enabled() && k.outbox_status().is_none() && k.logical_snapshot().alarms.is_none());
    assert_eq!(Kernel::alarm_storage_bytes(k.config()), 0);
    // Alarm operations are refused without effect.
    let mut k = k;
    assert_refused_unchanged(&mut k, ack(1, 1, 0), ApplyError::AlarmsNotConfigured);
    assert_refused_unchanged(&mut k, wm(0), ApplyError::AlarmsNotConfigured);
    // With alarms: v3, also without supervision.
    let with = ValidatedConfig::validate_with_alarms(spec, Some(alarm_spec(64))).unwrap();
    assert_eq!(with.fingerprint_version(), 3);
    assert_eq!(Kernel::new(Arc::new(with)).state_digest_version(), 3);
}

#[test]
fn t02_config_validation_fails_fast() {
    use AlarmCondition::*;
    let err = |a: AlarmSpec| ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(a)).unwrap_err();
    let with = |r: Vec<AlarmRuleSpec>| AlarmSpec { max_rules: 100, outbox_capacity_events: 64, rules: r };
    assert_eq!(err(with(vec![])), ConfigError::NoAlarmRules);
    assert_eq!(err(with(vec![rule(0, 11, BadQuality, Severity::Info)])), ConfigError::ZeroAlarmId);
    assert_eq!(
        err(with(vec![rule(5, 11, BadQuality, Severity::Info), rule(5, 12, BadQuality, Severity::Info)])),
        ConfigError::DuplicateAlarmId(AlarmId(5))
    );
    let mut dup = vec![rule(5, 11, BadQuality, Severity::Info), rule(6, 12, BadQuality, Severity::Info)];
    dup[1].name = "a5".into();
    assert_eq!(err(with(dup)), ConfigError::DuplicateAlarmName("a5".into()));
    let mut bad = rule(5, 11, BadQuality, Severity::Info);
    bad.name = "has space".into();
    assert!(matches!(err(with(vec![bad])), ConfigError::InvalidName { .. }));
    assert_eq!(
        err(with(vec![rule(5, 99, BadQuality, Severity::Info)])),
        ConfigError::AlarmUnknownTag { alarm_id: AlarmId(5), tag_id: TagId(99) }
    );
    assert!(matches!(
        err(with(vec![rule(5, 13, High { trip: 1, reset: 0 }, Severity::Info)])),
        ConfigError::AlarmTypeMismatch { .. }
    ));
    assert!(matches!(
        err(with(vec![rule(5, 11, BoolEquals { expected: true }, Severity::Info)])),
        ConfigError::AlarmTypeMismatch { .. }
    ));
    for (t, r) in [(10, 10), (10, 11), (i64::MIN, i64::MIN)] {
        assert!(matches!(
            err(with(vec![rule(5, 11, High { trip: t, reset: r }, Severity::Info)])),
            ConfigError::AlarmInvalidThresholds { .. }
        ));
    }
    for (t, r) in [(10, 10), (10, 9), (i64::MAX, i64::MAX)] {
        assert!(matches!(
            err(with(vec![rule(5, 11, Low { trip: t, reset: r }, Severity::Info)])),
            ConfigError::AlarmInvalidThresholds { .. }
        ));
    }
    // Extreme but valid thresholds.
    let ok = |r| ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(with(vec![r]))).is_ok();
    assert!(ok(rule(5, 11, High { trip: i64::MAX, reset: i64::MIN }, Severity::Info)));
    assert!(ok(rule(5, 11, Low { trip: i64::MIN, reset: i64::MAX }, Severity::Info)));
    // Limits.
    let mut a = alarm_spec(64);
    a.max_rules = 8;
    assert_eq!(err(a), ConfigError::TooManyAlarmRules { count: 9, max: 8 });
    let mut a = alarm_spec(64);
    a.max_rules = MAX_ALARM_RULES_CEILING + 1;
    assert!(matches!(err(a), ConfigError::InvalidLimit { name: "alarms.max_rules", .. }));
    assert!(matches!(err(alarm_spec(0)), ConfigError::InvalidLimit { name: "alarms.outbox_capacity_events", .. }));
    assert!(matches!(err(alarm_spec(MAX_OUTBOX_CAPACITY_CEILING + 1)), ConfigError::InvalidLimit { .. }));
    // Outbox smaller than the worst single-operation fanout (S1: 6 rules).
    assert_eq!(
        err(alarm_spec(5)),
        ConfigError::OutboxTooSmallForFanout { capacity: 5, worst_fanout: 6, source_id: S1 }
    );
    let t = ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(alarm_spec(6))).unwrap();
    assert_eq!(t.alarm_table().unwrap().worst_fanout(), (6, Some(S1)));
}

#[test]
fn t02_fingerprint_is_order_independent_and_semantic() {
    let a = ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(alarm_spec(64))).unwrap();
    let mut rev = alarm_spec(64);
    rev.rules.reverse();
    let b = ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(rev)).unwrap();
    assert_eq!(a.fingerprint(), b.fingerprint());
    // Capacity is operational, not semantic: same fingerprint (it is in the state digest).
    let c = ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(alarm_spec(1024))).unwrap();
    assert_eq!(a.fingerprint(), c.fingerprint());
    assert_ne!(Kernel::new(Arc::new(a.clone())).state_digest(), Kernel::new(Arc::new(c)).state_digest());
    // A threshold or a severity changes it.
    let mut d = alarm_spec(64);
    d.rules[1].condition = AlarmCondition::High { trip: 101, reset: 90 };
    assert_ne!(
        a.fingerprint(),
        ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(d)).unwrap().fingerprint()
    );
    let mut e = alarm_spec(64);
    e.rules[1].severity = Severity::Critical;
    assert_ne!(
        a.fingerprint(),
        ValidatedConfig::validate_with_alarms(tags_and_sources(), Some(e)).unwrap().fingerprint()
    );
}

// ------------------------------------------------------------------------------- T03

#[test]
fn t03_high_low_hysteresis_and_boundaries() {
    let mut k = kernel(1024);
    ok(&mut k, connect(S1, 1));
    ok(&mut k, full1(1, 1, 99, -1, false, 0));
    assert_eq!(episode(&k, 1001), EpisodeState::Normal);
    let mut seq = 2;
    let mut d = |k: &mut Kernel, v: i64| {
        let before = k.outbox_status().unwrap();
        ok(k, delta1(1, seq, v));
        seq += 1;
        new_events(k, &before)
    };
    assert_eq!(d(&mut k, 100), vec![(1001, AlarmEventKind::Raised)]); // trip is inclusive
    assert_eq!(d(&mut k, 91), vec![]); // in the band: stays active
    assert_eq!(d(&mut k, 120), vec![]); // repeated trip while active: nothing
    assert_eq!(d(&mut k, 90), vec![(1001, AlarmEventKind::Returned)]); // reset inclusive
    assert_eq!(d(&mut k, 99), vec![]); // band from below: stays inactive
    assert_eq!(d(&mut k, -100), vec![(1002, AlarmEventKind::Raised)]); // Low trip inclusive
    assert_eq!(d(&mut k, -91), vec![]);
    assert_eq!(d(&mut k, -90), vec![(1002, AlarmEventKind::Returned)]);
    assert_eq!(d(&mut k, i64::MAX), vec![(1001, AlarmEventKind::Retriggered)]); // returned-unacked + trip
    assert_eq!(d(&mut k, i64::MIN), vec![(1001, AlarmEventKind::Returned), (1002, AlarmEventKind::Retriggered)]);
    // Tag 12: High trip 0 reset -1 (negative and zero boundaries).
    let before = k.outbox_status().unwrap();
    ok(&mut k, batch(S1, 1, BatchMode::Delta, seq, vec![obs(2, Value::I64(0))]));
    assert_eq!(new_events(&k, &before), vec![(1005, AlarmEventKind::Raised)]);
    // Bool: tag 13 rule 1004 expects true.
    let before = k.outbox_status().unwrap();
    ok(&mut k, batch(S1, 1, BatchMode::Delta, seq + 1, vec![obs(3, Value::Bool(true)), obs(2, Value::I64(-1))]));
    assert_eq!(new_events(&k, &before), vec![(1004, AlarmEventKind::Raised), (1005, AlarmEventKind::Returned)]);
    let before = k.outbox_status().unwrap();
    ok(&mut k, batch(S1, 1, BatchMode::Delta, seq + 2, vec![obs(3, Value::Bool(false))]));
    assert_eq!(new_events(&k, &before), vec![(1004, AlarmEventKind::Returned)]);
}

// ------------------------------------------------------------------------------- T04

#[test]
fn t04_quality_is_orthogonal_and_never_returns_to_normal() {
    let mut k = kernel(1024);
    // Startup: everything Unknown, no storm (BadQuality is not armed yet).
    ok(&mut k, connect(S1, 1));
    assert_eq!(k.outbox_status().unwrap().next_event_seq, 1);
    ok(&mut k, disconnect(S1, 1));
    ok(&mut k, connect(S1, 2));
    assert_eq!(k.outbox_status().unwrap().next_event_seq, 1, "no Good yet: BadQuality unarmed");
    ok(&mut k, full1(2, 1, 150, -5, false, 0)); // 1001 Raised
    assert_eq!(episode(&k, 1001), EpisodeState::ActiveUnacked);
    assert!(st(&k, 1001).flags.input_valid() && st(&k, 1003).flags.armed());
    // Silence: process alarm stays active (indeterminate), BadQuality raises.
    let before = k.outbox_status().unwrap();
    ok(&mut k, Operation::SourceSilenceExpired { source_id: S1, link_generation: 2 });
    assert_eq!(k.tag(TagId(11)).unwrap().quality, SILENT);
    assert_eq!(new_events(&k, &before), vec![(1003, AlarmEventKind::Raised), (1006, AlarmEventKind::Raised)]);
    assert_eq!(episode(&k, 1001), EpisodeState::ActiveUnacked);
    assert!(!st(&k, 1001).flags.input_valid());
    // Heartbeat restoration: link alive, tags not Good, no alarm change at all.
    let before = observe(&k);
    ok(&mut k, Operation::SourceLivenessRestored { source_id: S1, link_generation: 2 });
    assert_eq!(before.0.alarms, k.logical_snapshot().alarms);
    // Disconnect: higher-precedence reason, still nothing for process alarms; 1003 already active.
    let before = k.outbox_status().unwrap();
    ok(&mut k, disconnect(S1, 2));
    assert_eq!(k.tag(TagId(11)).unwrap().quality, UNAVAILABLE);
    assert_eq!(new_events(&k, &before), vec![]);
    // Back to Good with a value in the band: the latch is evaluated with hysteresis (stays
    // active), BadQuality returns.
    ok(&mut k, connect(S1, 3));
    let before = k.outbox_status().unwrap();
    ok(&mut k, full1(3, 2, 95, -5, false, 0));
    assert_eq!(new_events(&k, &before), vec![(1003, AlarmEventKind::Returned), (1006, AlarmEventKind::Returned)]);
    assert_eq!(episode(&k, 1001), EpisodeState::ActiveUnacked);
    // Freshness: Good -> StaleData raises BadQuality only.
    let up_to = k.last_operation_sequence();
    let before = k.outbox_status().unwrap();
    ok(&mut k, Operation::FreshnessExpired { source_id: S1, link_generation: 3, observed_up_to: up_to });
    assert_eq!(k.tag(TagId(11)).unwrap().quality, STALE);
    // Their previous episode returned but was never acknowledged: same occurrence reopened.
    assert_eq!(new_events(&k, &before), vec![(1003, AlarmEventKind::Retriggered), (1006, AlarmEventKind::Retriggered)]);
    assert_eq!(st(&k, 1003).occurrence_id, 1);
    // A value below reset while stale is never seen: the alarm cannot return without a
    // Good measurement; the next Good measurement decides.
    let before = k.outbox_status().unwrap();
    ok(&mut k, delta1(3, 3, 10));
    assert_eq!(new_events(&k, &before), vec![(1001, AlarmEventKind::Returned), (1003, AlarmEventKind::Returned)]);
    assert_eq!(episode(&k, 1001), EpisodeState::ReturnedUnacked);
}

#[test]
fn t04_bad_quality_arms_only_after_first_good() {
    let mut k = kernel(1024);
    ok(&mut k, connect(S2, 1));
    ok(&mut k, disconnect(S2, 1));
    ok(&mut k, connect(S2, 2));
    assert!(!st(&k, 2003).flags.armed());
    ok(&mut k, batch(S2, 2, BatchMode::FullState, 1, vec![obs(1, Value::I64(0)), obs(2, Value::Bool(true))]));
    assert!(st(&k, 2003).flags.armed());
    let before = k.outbox_status().unwrap();
    ok(&mut k, disconnect(S2, 2));
    assert_eq!(new_events(&k, &before), vec![(2003, AlarmEventKind::Raised)]);
    // Not armed again by a reconnect: stays armed for the instance.
    ok(&mut k, connect(S2, 3));
    assert!(st(&k, 2003).flags.armed() && st(&k, 2003).flags.active());
}

// ------------------------------------------------------------------------------- T05

#[test]
fn t05_episode_automaton_tokens_and_stale_acks() {
    let mut k = kernel(1024);
    ok(&mut k, connect(S1, 1));
    let op_raise = ok(&mut k, full1(1, 1, 150, -5, false, 0));
    let v = st(&k, 1001);
    assert_eq!((v.occurrence_id, v.activation_token), (1, op_raise));
    ok(&mut k, delta1(1, 2, 50)); // returned unacked
    assert_eq!(episode(&k, 1001), EpisodeState::ReturnedUnacked);
    let op_retrig = ok(&mut k, delta1(1, 3, 150)); // retrigger: same occurrence, new token
    let v = st(&k, 1001);
    assert_eq!((v.occurrence_id, v.activation_token, v.flags.episode()), (1, op_retrig, EpisodeState::ActiveUnacked));
    // The old token is stale even though the alarm is still active and unacknowledged.
    assert_refused_unchanged(
        &mut k,
        ack(1001, op_raise, 1000),
        ApplyError::StaleActivationToken { alarm_id: AlarmId(1001), current: op_retrig },
    );
    assert_refused_unchanged(&mut k, ack(9999, op_retrig, 1000), ApplyError::UnknownAlarm(AlarmId(9999)));
    assert_refused_unchanged(&mut k, ack(1002, 0, 1000), ApplyError::NotAcknowledgeable(AlarmId(1002)));
    let before = k.outbox_status().unwrap();
    ok(&mut k, ack(1001, op_retrig, 1000));
    assert_eq!(episode(&k, 1001), EpisodeState::ActiveAcked);
    let e = events(&k, before.next_event_seq - 1);
    assert_eq!(e.len(), 1);
    assert_eq!(
        (e[0].kind, e[0].actor_uid, e[0].occurrence_id, e[0].activation_token),
        (AlarmEventKind::Acknowledged, 1000, 1, op_retrig)
    );
    // Repeated ACK: no-op, no sequence consumed.
    let before = observe(&k);
    assert_eq!(
        step(&mut k, ack(1001, op_retrig, 1000)),
        Ok(TransitionOutcome::Ignored(IgnoredReason::AlreadyAcknowledged))
    );
    assert_eq!(before, observe(&k));
    // ActiveAcked -> Normal on clear; next trip is a new occurrence.
    ok(&mut k, delta1(1, 4, 0));
    assert_eq!(episode(&k, 1001), EpisodeState::Normal);
    let op2 = ok(&mut k, delta1(1, 5, 100));
    let v = st(&k, 1001);
    assert_eq!((v.occurrence_id, v.activation_token), (2, op2));
    // ReturnedUnacked -> Normal on ACK.
    ok(&mut k, delta1(1, 6, 0));
    ok(&mut k, ack(1001, op2, 1));
    assert_eq!(episode(&k, 1001), EpisodeState::Normal);
    // Reconnection / takeover never acknowledges.
    let op3 = ok(&mut k, delta1(1, 7, 100));
    ok(&mut k, connect(S1, 2));
    assert_eq!(episode(&k, 1001), EpisodeState::ActiveUnacked);
    assert_eq!(st(&k, 1001).activation_token, op3);
    assert_eq!(k.tag(TagId(11)).unwrap().quality, UNAVAILABLE);
    // ACK of an indeterminate (input invalid) alarm is allowed: it acknowledges the episode.
    ok(&mut k, ack(1001, op3, 5));
    assert_eq!(episode(&k, 1001), EpisodeState::ActiveAcked);
}

// ------------------------------------------------------------------------------- T06

#[test]
fn t06_events_of_one_operation_are_ordered_by_alarm_id_whatever_the_input_order() {
    let run = |order: &[usize]| {
        let mut k = kernel(1024);
        ok(&mut k, connect(S1, 1));
        ok(&mut k, full1(1, 1, 0, -5, false, 0));
        let o = [obs(3, Value::Bool(true)), obs(1, Value::I64(150)), obs(2, Value::I64(5))];
        let before = k.outbox_status().unwrap();
        let op = ok(&mut k, batch(S1, 1, BatchMode::Delta, 2, order.iter().map(|&i| o[i]).collect()));
        let e = events(&k, before.next_event_seq - 1);
        for (n, ev) in e.iter().enumerate() {
            assert_eq!(
                (ev.operation_seq, ev.ordinal_in_operation, ev.event_seq),
                (op, n as u32, before.next_event_seq + n as u64)
            );
        }
        (e, k.state_digest())
    };
    let (a, da) = run(&[0, 1, 2]);
    assert_eq!(a.iter().map(|e| e.alarm_id.0).collect::<Vec<_>>(), vec![1001, 1004, 1005]);
    for order in [[2, 1, 0], [1, 0, 2], [2, 0, 1]] {
        assert_eq!(run(&order), (a.clone(), da));
    }
}

// ------------------------------------------------------------------------------- T07/T08

#[test]
fn t07_outbox_full_refuses_whole_operation_without_any_change() {
    let mut k = kernel(6); // = worst fanout
    ok(&mut k, connect(S1, 1));
    ok(&mut k, connect(S2, 2));
    ok(&mut k, full1(1, 1, 150, 5, true, 0)); // 1001, 1004, 1005 raised: 3 events
    assert_eq!(k.outbox_status().unwrap().free, 3);
    ok(&mut k, batch(S2, 2, BatchMode::FullState, 1, vec![obs(1, Value::I64(60)), obs(2, Value::Bool(false))])); // 2001, 2002
    assert_eq!(k.outbox_status().unwrap().free, 1);
    // Disconnect of S1 would raise 1003 and 1006: 2 > 1.
    assert_refused_unchanged(&mut k, disconnect(S1, 1), ApplyError::OutboxCapacityExceeded { required: 2, free: 1 });
    // A batch with zero events is admitted even when the outbox is nearly full.
    ok(&mut k, batch(S1, 1, BatchMode::Delta, 2, vec![obs(4, Value::I64(1))]));
    // One event fits.
    ok(&mut k, batch(S1, 1, BatchMode::Delta, 3, vec![obs(2, Value::I64(-1))]));
    assert_eq!(k.outbox_status().unwrap().free, 0);
    // Full: even a single ACK is refused, and its sequence is not consumed.
    let token = st(&k, 1001).activation_token;
    assert_refused_unchanged(&mut k, ack(1001, token, 1), ApplyError::OutboxCapacityExceeded { required: 1, free: 0 });
    // A rejected batch leaves the source sequence unconsumed: the driver resends it.
    assert_refused_unchanged(&mut k, delta1(1, 4, 0), ApplyError::OutboxCapacityExceeded { required: 1, free: 0 });
    assert_eq!(k.source_status(S1).unwrap().last_accepted_source_sequence, Some(3));
    // The watermark is the only way to free slots; then the very same inputs apply.
    let used = k.outbox_status().unwrap();
    assert_eq!((used.used, used.high_water), (6, 6));
    ok(&mut k, wm(2));
    ok(&mut k, delta1(1, 4, 0));
    ok(&mut k, ack(1001, token, 1));
    assert_eq!(k.outbox_status().unwrap().free, 0);
}

#[test]
fn t07_errors_ignored_and_duplicates_change_nothing() {
    let mut k = kernel(64);
    ok(&mut k, connect(S1, 1));
    ok(&mut k, full1(1, 1, 150, 5, true, 0));
    let b = full1(1, 1, 150, 5, true, 0);
    let before = observe(&k);
    assert!(matches!(step(&mut k, b), Ok(TransitionOutcome::Duplicate { .. })));
    assert_eq!(before, observe(&k));
    for (op, e) in [
        (delta1(1, 9, 0), ApplyError::SequenceGap { expected: 2, got: 9 }),
        (delta1(2, 2, 0), ApplyError::LinkNotActive { got: 2 }),
        (
            batch(S1, 1, BatchMode::Delta, 2, vec![obs(1, Value::Bool(true))]),
            ApplyError::TypeMismatch { channel_id: ChannelId(1), expected: ValueType::I64, got: ValueType::Bool },
        ),
        (wm(99), ApplyError::WatermarkBeyondCommitted { up_to: 99, last_committed: 3 }),
    ] {
        assert_refused_unchanged(&mut k, op, e);
    }
    ok(&mut k, wm(2));
    assert_refused_unchanged(&mut k, wm(1), ApplyError::WatermarkRegression { current: 2, got: 1 });
    let before = observe(&k);
    assert_eq!(step(&mut k, wm(2)), Ok(TransitionOutcome::Ignored(IgnoredReason::WatermarkUnchanged)));
    assert_eq!(before, observe(&k));
    // Operation sequence mismatch.
    let before = observe(&k);
    assert!(k.apply(SequencedInput { operation_sequence: 99, operation: wm(3) }).is_err());
    assert_eq!(before, observe(&k));
}

/// One source, 4000 tags, 5 rules per tag: one disconnection produces 20 000 events in
/// one operation, all of them or none (brief I2).
#[test]
fn t07_twenty_thousand_events_in_one_operation_all_or_nothing() {
    let n = 4000u32;
    let spec = ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: 4096 },
        tags: (1..=n)
            .map(|i| TagSpec { tag_id: TagId(i), name: format!("t{i}"), value_type: ValueType::I64 })
            .collect(),
        sources: vec![SourceSpec {
            source_id: S1,
            name: "big".into(),
            channels: (1..=n)
                .map(|i| ChannelSpec { channel_id: ChannelId(i), tag_id: TagId(i), value_type: ValueType::I64 })
                .collect(),
            supervision: Supervision::NONE,
        }],
    };
    let rules: Vec<_> = (1..=n)
        .flat_map(|t| {
            (0..5).map(move |j| {
                rule(
                    t * 10 + j,
                    t,
                    if j == 0 {
                        AlarmCondition::BadQuality
                    } else {
                        AlarmCondition::High { trip: 1000 + j as i64, reset: 0 }
                    },
                    Severity::Major,
                )
            })
        })
        .collect();
    let mk = |cap: u64| {
        Kernel::new(Arc::new(
            ValidatedConfig::validate_with_alarms(
                spec.clone(),
                Some(AlarmSpec { max_rules: 20_000, outbox_capacity_events: cap, rules: rules.clone() }),
            )
            .unwrap(),
        ))
    };
    let full = |g: u64, seq: u64, v: i64| {
        batch(S1, g, BatchMode::FullState, seq, (1..=n).map(|c| obs(c, Value::I64(v))).collect())
    };
    // Capacity exactly 20 000: the full state at 5000 raises 16 000 High alarms, the
    // disconnection raises 4 000 BadQuality.
    let mut k = mk(20_000);
    ok(&mut k, connect(S1, 1));
    ok(&mut k, full(1, 1, 5000));
    assert_eq!(k.outbox_status().unwrap().used, 16_000);
    let op = ok(&mut k, disconnect(S1, 1));
    let s = k.outbox_status().unwrap();
    assert_eq!((s.used, s.free, s.active_or_unacked), (20_000, 0, 20_000));
    let ev = events(&k, 16_000);
    assert_eq!(ev.len(), 4000);
    assert!(ev.iter().all(|e| e.operation_seq == op && e.kind == AlarmEventKind::Raised));
    // One slot short (an ACK took one slot): nothing at all.
    let mut k = mk(20_000);
    ok(&mut k, connect(S1, 1));
    ok(&mut k, full(1, 1, 5000));
    let t = k.alarm(AlarmId(11)).unwrap().activation_token;
    ok(&mut k, ack(11, t, 1));
    assert_refused_unchanged(
        &mut k,
        disconnect(S1, 1),
        ApplyError::OutboxCapacityExceeded { required: 4000, free: 3999 },
    );
    assert_eq!(k.tag(TagId(1)).unwrap().quality, Quality::Good);
    // An outbox smaller than the worst fanout of one operation is refused at startup.
    let e = ValidatedConfig::validate_with_alarms(
        spec,
        Some(AlarmSpec { max_rules: 20_000, outbox_capacity_events: 19_999, rules }),
    );
    assert_eq!(
        e.unwrap_err(),
        ConfigError::OutboxTooSmallForFanout { capacity: 19_999, worst_fanout: 20_000, source_id: S1 }
    );
}

#[test]
fn t08_ring_wraps_physically_never_logically_and_never_overwrites_undurable_events() {
    let mut k = kernel(8);
    ok(&mut k, connect(S1, 1));
    ok(&mut k, full1(1, 1, 0, -5, false, 0));
    let mut seq = 2;
    let mut log: Vec<AlarmEvent> = Vec::new();
    let mut toggle = |k: &mut Kernel, v: i64| -> Result<TransitionOutcome, ApplyError> {
        let r = step(k, delta1(1, seq, v));
        if r.is_ok() {
            seq += 1;
        }
        r
    };
    for round in 0..100 {
        let v = if round % 2 == 0 { 150 } else { 0 };
        match toggle(&mut k, v) {
            Ok(_) => {}
            Err(ApplyError::OutboxCapacityExceeded { required: 1, free: 0 }) => {
                // Consumer: read everything not yet logged, then confirm half of it.
                let s = k.outbox_status().unwrap();
                let mut page = Vec::with_capacity(3);
                let mut cursor = log.last().map_or(0, |e| e.event_seq);
                loop {
                    let p = k.read_alarm_events(cursor, 3, &mut page).unwrap();
                    assert!(!p.gap_detected, "an undurable event is always readable");
                    log.extend_from_slice(&page);
                    cursor = p.next_cursor;
                    if !p.has_more {
                        break;
                    }
                }
                assert_eq!(cursor, s.next_event_seq - 1);
                ok(&mut k, wm(s.durable_watermark + 4));
                toggle(&mut k, v).unwrap();
            }
            Err(e) => panic!("{e:?}"),
        }
        let s = k.outbox_status().unwrap();
        assert_eq!(s.used + s.free, s.capacity);
        assert!(s.used <= 8);
        // Every logged event that is not durable yet is still in the ring, identical.
        for e in log.iter().filter(|e| e.event_seq > s.durable_watermark) {
            assert_eq!(k.alarm_event(e.event_seq), Some(*e));
        }
    }
    let s = k.outbox_status().unwrap();
    assert!(s.next_event_seq > 50, "wrapped several times: {s:?}");
    // A cursor far behind reports the gap (only durable events were reclaimed).
    let mut page = Vec::with_capacity(4);
    let p = k.read_alarm_events(0, 4, &mut page).unwrap();
    assert!(p.gap_detected && page[0].event_seq == s.oldest_visible && s.oldest_visible <= s.durable_watermark + 1);
    // A cursor ahead of the instance is flagged.
    let p = k.read_alarm_events(s.next_event_seq + 10, 4, &mut page).unwrap();
    assert!(p.cursor_ahead && page.is_empty() && !p.has_more);
    // Reading changed nothing.
    let before = observe(&k);
    let _ = k.read_alarm_events(0, 4, &mut page);
    assert_eq!(before, observe(&k));
}

#[test]
fn t08_snapshot_pages_list_active_or_unacked_in_id_order() {
    let mut k = kernel(64);
    ok(&mut k, connect(S1, 1));
    ok(&mut k, connect(S2, 2));
    ok(&mut k, full1(1, 1, 150, 5, true, 0));
    ok(&mut k, batch(S2, 2, BatchMode::FullState, 1, vec![obs(1, Value::I64(60)), obs(2, Value::Bool(false))]));
    let mut out = Vec::with_capacity(2);
    let mut after = AlarmId(0);
    let mut ids = Vec::new();
    loop {
        let p = k.alarm_snapshot_page(after, 2, &mut out).unwrap();
        ids.extend(out.iter().map(|v| v.alarm_id.0));
        assert_eq!(p.total_listed, 5);
        after = p.next_after;
        if !p.has_more {
            break;
        }
    }
    assert_eq!(ids, vec![1001, 1004, 1005, 2001, 2002]);
    // Return + ACK removes it from the listing; return alone keeps it.
    let t = st(&k, 1001).activation_token;
    ok(&mut k, delta1(1, 2, 0));
    let mut all = Vec::with_capacity(16);
    k.alarm_snapshot_page(AlarmId(0), 16, &mut all).unwrap();
    assert!(all.iter().any(|v| v.alarm_id.0 == 1001));
    let p0 = k.alarm_snapshot_page(AlarmId(0), 16, &mut all).unwrap().as_of_change_operation;
    ok(&mut k, ack(1001, t, 3));
    let p = k.alarm_snapshot_page(AlarmId(0), 16, &mut all).unwrap();
    assert!(!all.iter().any(|v| v.alarm_id.0 == 1001));
    assert!(p.as_of_change_operation > p0, "consistency token moved");
}

// ------------------------------------------------------------------------------- T11/T12

/// Independent model of the alarm semantics (ADR 0012 §3), fed with the tag (value,
/// quality) transitions observed on the kernel.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct M {
    active: bool,
    unacked: bool,
    armed: bool,
    valid: bool,
    occurrence: u64,
    token: u64,
}

fn model_eval(cond: AlarmCondition, m: &mut M, v: Option<Value>, q: Quality, op: u64) -> Option<AlarmEventKind> {
    let good = q == Quality::Good;
    m.valid = good;
    let c = match cond {
        AlarmCondition::BadQuality => {
            if good {
                m.armed = true;
            }
            Some(m.armed && !good)
        }
        _ if !good => None,
        AlarmCondition::BoolEquals { expected } => Some(v == Some(Value::Bool(expected))),
        AlarmCondition::High { trip, reset } => match v {
            Some(Value::I64(x)) => Some(if m.active { x > reset } else { x >= trip }),
            _ => None,
        },
        AlarmCondition::Low { trip, reset } => match v {
            Some(Value::I64(x)) => Some(if m.active { x < reset } else { x <= trip }),
            _ => None,
        },
    };
    match c {
        Some(true) if !m.active => {
            m.active = true;
            m.token = op;
            if m.unacked {
                Some(AlarmEventKind::Retriggered)
            } else {
                m.unacked = true;
                m.occurrence += 1;
                Some(AlarmEventKind::Raised)
            }
        }
        Some(false) if m.active => {
            m.active = false;
            Some(AlarmEventKind::Returned)
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
enum Act {
    Connect(u8, bool),
    Disconnect(u8, bool),
    Full(u8, Vec<u8>),
    Delta(u8, Vec<(u8, u8)>, u8),
    Silence,
    Restore,
    Fresh(u8),
    Ack(u8, u8),
    Wm(u8),
}

const VALS: [i64; 22] =
    [-200, -101, -100, -95, -90, -89, -1, 0, 1, 39, 40, 41, 49, 50, 51, 89, 90, 91, 99, 100, i64::MIN, i64::MAX];

fn act() -> impl Strategy<Value = Act> {
    prop_oneof![
        2 => (0u8..2, any::<bool>()).prop_map(|(s, e)| Act::Connect(s, e)),
        1 => (0u8..2, any::<bool>()).prop_map(|(s, st)| Act::Disconnect(s, st)),
        3 => (0u8..2, prop::collection::vec(any::<u8>(), 4)).prop_map(|(s, v)| Act::Full(s, v)),
        8 => (0u8..2, prop::collection::vec((any::<u8>(), any::<u8>()), 1..4), 0u8..12).prop_map(|(s, v, o)| Act::Delta(s, v, o)),
        1 => Just(Act::Silence),
        1 => Just(Act::Restore),
        1 => any::<u8>().prop_map(Act::Fresh),
        3 => (any::<u8>(), 0u8..8).prop_map(|(r, m)| Act::Ack(r, m)),
        2 => any::<u8>().prop_map(Act::Wm),
    ]
}

struct Driver {
    next_gen: u64,
}

fn concretize(k: &Kernel, d: &mut Driver, a: &Act) -> Operation {
    let sid = |s: u8| if s == 0 { S1 } else { S2 };
    let cur = |s: SourceId| match k.source_status(s).unwrap().link {
        LinkState::Connected { generation } | LinkState::Disconnected { generation } => generation,
        LinkState::NeverConnected => 0,
    };
    let chans = |s: SourceId| {
        if s == S1 { vec![(1u32, false), (2, false), (3, true), (4, false)] } else { vec![(1, false), (2, true)] }
    };
    let val = |is_bool: bool, x: u8| {
        if is_bool { Value::Bool(x % 2 == 0) } else { Value::I64(VALS[x as usize % VALS.len()]) }
    };
    match a {
        Act::Connect(s, new_epoch) => {
            d.next_gen += 1;
            Operation::SourceConnected {
                source_id: sid(*s),
                link_generation: d.next_gen,
                source_epoch: if *new_epoch { d.next_gen + 100 } else { 7 },
            }
        }
        Act::Disconnect(s, stale) => {
            let g = cur(sid(*s));
            Operation::SourceDisconnected {
                source_id: sid(*s),
                link_generation: if *stale { g.saturating_sub(1) } else { g },
            }
        }
        Act::Full(s, v) => {
            let s = sid(*s);
            let seq = k.source_status(s).unwrap().last_accepted_source_sequence.map_or(1, |x| x + 1);
            let o = chans(s).iter().zip(v).map(|(&(c, b), &x)| obs(c, val(b, x))).collect();
            batch(s, cur(s), BatchMode::FullState, seq, o)
        }
        Act::Delta(s, v, off) => {
            let s = sid(*s);
            let ch = chans(s);
            let last = k.source_status(s).unwrap().last_accepted_source_sequence.unwrap_or(0);
            // Mostly the next sequence; sometimes a duplicate / gap / stale one.
            let seq = match off {
                0 => last,
                1 => last + 2,
                _ => last + 1,
            };
            let mut seen = Vec::new();
            let o = v
                .iter()
                .filter_map(|&(c, x)| {
                    let (c, b) = ch[c as usize % ch.len()];
                    if seen.contains(&c) {
                        return None;
                    }
                    seen.push(c);
                    Some(obs(c, val(b, x)))
                })
                .collect();
            batch(s, cur(s), BatchMode::Delta, seq, o)
        }
        Act::Silence => Operation::SourceSilenceExpired { source_id: S1, link_generation: cur(S1) },
        Act::Restore => Operation::SourceLivenessRestored { source_id: S1, link_generation: cur(S1) },
        Act::Fresh(back) => Operation::FreshnessExpired {
            source_id: S1,
            link_generation: cur(S1),
            observed_up_to: k.last_operation_sequence().saturating_sub(u64::from(*back % 6)).max(1),
        },
        Act::Ack(r, mode) => {
            let ids = k.config().alarm_table().unwrap().ids().to_vec();
            let id = ids[*r as usize % ids.len()];
            let tok = k.alarm(id).unwrap().activation_token;
            let token = match mode {
                0 => tok.saturating_sub(1),
                1 => 0,
                _ => tok,
            };
            Operation::AlarmAcknowledge { alarm_id: id, expected_activation_token: token, actor_uid: u32::from(*r) }
        }
        Act::Wm(frac) => {
            let s = k.outbox_status().unwrap();
            let last = s.next_event_seq - 1;
            let up =
                if *frac == 255 { last + 1 } else { s.durable_watermark + (s.used * u64::from(*frac % 101)) / 100 };
            Operation::AdvanceDurableWatermark { up_to_event_seq: up }
        }
    }
}

/// Runs `acts` on a kernel with outbox `cap` (pressure) and on a twin with a huge outbox
/// (no pressure), checking at every step: the model's events, atomicity of every refusal,
/// the outbox accounting and that no undurable event is ever overwritten. Returns the
/// applied input stream of the pressured kernel.
fn run_model(acts: &[Act], cap: u64) -> Vec<SequencedInput> {
    let mut main = kernel(cap);
    let mut twin = kernel(1 << 16);
    let table = main.config().alarm_table().unwrap().clone();
    let conds: BTreeMap<u32, (AlarmCondition, TagId)> =
        (0..table.len()).map(|i| table.rule(i).unwrap()).map(|r| (r.alarm_id.0, (r.condition, r.tag_id))).collect();
    let mut model: BTreeMap<u32, M> = conds.keys().map(|&id| (id, M::default())).collect();
    let mut log: Vec<AlarmEvent> = Vec::new();
    let mut applied = Vec::new();
    let mut d = Driver { next_gen: 0 };
    for a in acts {
        let op = concretize(&main, &mut d, a);
        let input = SequencedInput { operation_sequence: main.last_operation_sequence() + 1, operation: op.clone() };
        let twin_before = twin.clone();
        let tags_before = twin.logical_snapshot().tags;
        let tw_status = twin.outbox_status().unwrap();
        let r_twin = twin.apply(input.clone());
        let before = observe(&main);
        let m_status = main.outbox_status().unwrap();
        let r_main = main.apply(input.clone());
        match (&r_twin, &r_main) {
            (Ok(TransitionOutcome::Applied { .. }), Ok(TransitionOutcome::Applied { .. })) => {}
            (Ok(TransitionOutcome::Applied { .. }), Err(ApplyError::OutboxCapacityExceeded { required, free })) => {
                let produced = twin.outbox_status().unwrap().next_event_seq - tw_status.next_event_seq;
                assert_eq!((*required, *free), (produced, m_status.free));
                assert!(produced > m_status.free);
                assert_eq!(before, observe(&main), "refused operation changed state");
                twin = twin_before;
                continue;
            }
            (Ok(TransitionOutcome::Applied { .. }), other) => panic!("{op:?}: twin applied, main {other:?}"),
            (t, m) => {
                // Watermark ops see different outbox states; everything else must agree.
                if !matches!(op, Operation::AdvanceDurableWatermark { .. }) {
                    assert_eq!(t, m, "{op:?}");
                }
                assert!(
                    !matches!(m, Ok(TransitionOutcome::Applied { .. }))
                        || matches!(op, Operation::AdvanceDurableWatermark { .. })
                );
                if !matches!(m, Ok(TransitionOutcome::Applied { .. })) {
                    assert_eq!(before, observe(&main), "{op:?} -> {m:?} changed state");
                }
                if matches!(m, Ok(TransitionOutcome::Applied { .. })) {
                    applied.push(input);
                }
                if !matches!(t, Ok(TransitionOutcome::Applied { .. })) {
                    assert_eq!(twin.state_digest(), twin_before.state_digest());
                }
                continue;
            }
        }
        applied.push(input.clone());
        let op_seq = input.operation_sequence;
        // Expected events from the model.
        let tags_after = twin.logical_snapshot().tags;
        let mut expected: Vec<(u32, AlarmEventKind, u64, u64, u32)> = Vec::new();
        if let Operation::AlarmAcknowledge { alarm_id, actor_uid, .. } = op {
            let m = model.get_mut(&alarm_id.0).unwrap();
            assert!(m.unacked);
            m.unacked = false;
            expected.push((alarm_id.0, AlarmEventKind::Acknowledged, m.occurrence, m.token, actor_uid));
        }
        for (b, a) in tags_before.iter().zip(&tags_after) {
            if (b.value, b.quality) == (a.value, a.quality) {
                continue;
            }
            for (&id, &(cond, tag)) in &conds {
                if tag != a.tag_id {
                    continue;
                }
                let m = model.get_mut(&id).unwrap();
                if let Some(kind) = model_eval(cond, m, a.value, a.quality, op_seq) {
                    expected.push((id, kind, m.occurrence, m.token, 0));
                }
            }
        }
        expected.sort_by_key(|e| e.0);
        for k in [&twin, &main] {
            let got: Vec<_> = events(k, k.outbox_status().unwrap().next_event_seq - 1 - expected.len() as u64)
                .iter()
                .map(|e| {
                    assert_eq!(e.operation_seq, op_seq);
                    (e.alarm_id.0, e.kind, e.occurrence_id, e.activation_token, e.actor_uid)
                })
                .collect();
            assert_eq!(got, expected, "{op:?}");
        }
        log.extend(events(&main, m_status.next_event_seq - 1));
        // Model states equal kernel states; listed index consistent.
        let mut listed = 0;
        for (&id, m) in &model {
            let v = main.alarm(AlarmId(id)).unwrap();
            assert_eq!(
                (v.flags.active(), !v.flags.acknowledged(), v.flags.input_valid(), v.occurrence_id, v.activation_token),
                (m.active, m.unacked, m.valid, m.occurrence, m.token),
                "alarm {id}"
            );
            listed += u64::from(m.active || m.unacked);
        }
        let s = main.outbox_status().unwrap();
        assert_eq!(s.active_or_unacked, listed);
        assert_eq!(s.used + s.free, cap);
        assert!(s.high_water <= cap);
        for e in log.iter().filter(|e| e.event_seq > s.durable_watermark) {
            assert_eq!(main.alarm_event(e.event_seq), Some(*e), "undurable event overwritten");
        }
    }
    applied
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 384, ..ProptestConfig::default() })]

    /// T12 + T11: random interleavings with outbox pressure; then a replay of the applied
    /// stream reproduces RTDB, alarms, events and digests exactly.
    #[test]
    fn t12_model_atomicity_and_replay(acts in prop::collection::vec(act(), 1..120), cap in prop_oneof![Just(6u64), Just(7), Just(11), Just(64)]) {
        let applied = run_model(&acts, cap);
        let replay = |stream: &[SequencedInput]| {
            let mut k = kernel(cap);
            for i in stream {
                assert!(matches!(k.apply(i.clone()), Ok(TransitionOutcome::Applied { .. })), "{i:?}");
            }
            k
        };
        let a = replay(&applied);
        let b = replay(&applied);
        prop_assert_eq!(observe(&a), observe(&b));
        let mut main = kernel(cap);
        let mut d = Driver { next_gen: 0 };
        for act in &acts {
            let op = concretize(&main, &mut d, act);
            let _ = step(&mut main, op);
        }
        prop_assert_eq!(observe(&main), observe(&a));
    }
}

#[test]
fn t11_replay_reproduces_events_and_digest() {
    let mut k = kernel(16);
    let mut stream = Vec::new();
    let mut go = |k: &mut Kernel, op: Operation| {
        let i = SequencedInput { operation_sequence: k.last_operation_sequence() + 1, operation: op };
        if matches!(k.apply(i.clone()), Ok(TransitionOutcome::Applied { .. })) {
            stream.push(i);
        }
    };
    go(&mut k, connect(S1, 1));
    go(&mut k, full1(1, 1, 150, 5, true, 0));
    let t = k.alarm(AlarmId(1001)).unwrap().activation_token;
    go(&mut k, ack(1001, t, 42));
    go(&mut k, wm(2));
    go(&mut k, Operation::SourceSilenceExpired { source_id: S1, link_generation: 1 });
    go(&mut k, disconnect(S1, 1));
    let mut r = kernel(16);
    for i in &stream {
        r.apply(i.clone()).unwrap();
    }
    assert_eq!(observe(&k), observe(&r));
    assert_eq!(events(&k, 0), events(&r, 0));
}

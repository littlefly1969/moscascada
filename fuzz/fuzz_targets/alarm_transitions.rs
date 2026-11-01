//! VS-005 (brief §10, fuzz): arbitrary interleavings of batches (values crossing the
//! thresholds), duplicates, reconnects, silences, freshness expiries, ACKs (fresh, stale,
//! unknown tokens and alarms) and watermark advances on a supervised kernel with alarm
//! rules of every kind and a SMALL outbox (refusals are frequent). Checked after every input:
//! - refused / ignored / duplicate inputs change nothing: state, digest, outbox status;
//! - an applied input commits its events contiguously, ordered by alarm id, all with its
//!   operation sequence, numbered from the previous next_event_seq;
//! - no overwrite: every committed event above the watermark is still readable, unchanged;
//! - used <= capacity, and used == next_event_seq - 1 - watermark;
//! - a process rule never returns while its input is not Good (indeterminate, I5);
//! - replaying the same inputs gives the same digest and the same events digest.
#![no_main]

use std::collections::BTreeMap;
use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use scada_core::*;

fn config(capacity: u64) -> Arc<ValidatedConfig> {
    let mut tags = Vec::new();
    let mut sources = Vec::new();
    let mut rules = Vec::new();
    let mut id = 0u32;
    for (sid, n, liveness, freshness) in [(1u32, 3u32, true, true), (2, 2, false, true), (3, 1, true, false)] {
        let mut channels = Vec::new();
        for c in 1..=n {
            let tag_id = TagId(sid * 10 + c);
            tags.push(TagSpec { tag_id, name: format!("t{}", tag_id.0), value_type: ValueType::I64 });
            channels.push(ChannelSpec { channel_id: ChannelId(c), tag_id, value_type: ValueType::I64 });
            for condition in [
                AlarmCondition::High { trip: 2, reset: 1 },
                AlarmCondition::Low { trip: -1, reset: 0 },
                AlarmCondition::BadQuality,
            ] {
                id += 1;
                rules.push(AlarmRuleSpec {
                    alarm_id: AlarmId(id),
                    name: format!("a{id}"),
                    tag_id,
                    condition,
                    severity: Severity::Warning,
                });
            }
        }
        sources.push(SourceSpec {
            source_id: SourceId(sid),
            name: format!("s{sid}"),
            channels,
            supervision: Supervision { liveness, freshness },
        });
    }
    let spec = ConfigSpec { limits: CoreLimits { max_observations_per_batch: 4 }, tags, sources };
    // Worst fan-out: source 1 (3 tags x 3 rules = 9).
    let alarms = AlarmSpec { max_rules: 32, outbox_capacity_events: capacity.max(9), rules };
    Arc::new(ValidatedConfig::validate_with_alarms(spec, Some(alarms)).unwrap())
}

fn op(k: &Kernel, b: &[u8; 4], generation: &mut u64) -> Operation {
    let sid = SourceId(1 + u32::from(b[1] % 4)); // 4 = unknown source
    let st = k.source_status(sid);
    let active = st.map_or(0, |s| match s.link {
        LinkState::Connected { generation } | LinkState::Disconnected { generation } => generation,
        LinkState::NeverConnected => 0,
    });
    let g = if b[2] % 5 == 4 { active.saturating_sub(1) } else { active };
    let next = st.and_then(|s| s.last_accepted_source_sequence).map_or(1, |l| l + 1);
    let seq = if b[3] % 7 == 0 { next.saturating_sub(1) } else { next };
    let last = k.last_operation_sequence();
    let val = |x: u8| i64::from(x % 4) - 1; // -1..=2 crosses every threshold
    match b[0] % 11 {
        0 => {
            *generation += 1;
            Operation::SourceConnected { source_id: sid, link_generation: *generation, source_epoch: 1 }
        }
        1 => Operation::SourceDisconnected { source_id: sid, link_generation: g },
        2 => Operation::ObservationBatch(ObservationBatch {
            source_id: sid,
            link_generation: g,
            mode: BatchMode::FullState,
            source_sequence: seq,
            observations: (1..=k.config().source(sid).map_or(1, |m| m.channel_count() as u32))
                .map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(val(b[3].rotate_left(c))) })
                .collect(),
        }),
        3 | 4 => Operation::ObservationBatch(ObservationBatch {
            source_id: sid,
            link_generation: g,
            mode: BatchMode::Delta,
            source_sequence: seq,
            observations: vec![Observation { channel_id: ChannelId(1 + u32::from(b[2] % 3)), value: Value::I64(val(b[3])) }],
        }),
        5 => Operation::SourceSilenceExpired { source_id: sid, link_generation: g },
        6 => Operation::SourceLivenessRestored { source_id: sid, link_generation: g },
        7 => Operation::FreshnessExpired { source_id: sid, link_generation: g, observed_up_to: last },
        8 | 9 => {
            let alarm_id = AlarmId(1 + u32::from(b[1] % 22)); // 22 = unknown
            let token = k.alarm(alarm_id).map_or(1, |a| a.activation_token);
            let expected_activation_token = match b[2] % 4 {
                0 => token.wrapping_add(1),
                _ => token,
            };
            Operation::AlarmAcknowledge { alarm_id, expected_activation_token, actor_uid: u32::from(b[3]) }
        }
        _ => {
            let s = k.outbox_status().unwrap();
            let committed = s.next_event_seq - 1;
            let up_to = match b[2] % 4 {
                0 => s.durable_watermark.saturating_sub(1), // regression
                1 => committed + 1,                         // beyond
                2 => s.durable_watermark,                   // unchanged
                _ => s.durable_watermark + (committed - s.durable_watermark).min(u64::from(b[3] % 8)),
            };
            Operation::AdvanceDurableWatermark { up_to_event_seq: up_to }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&cap, data)) = data.split_first() else { return };
    let cfg = config(u64::from(cap % 32));
    let table = cfg.alarm_table().unwrap().clone();
    let mut k = Kernel::new(cfg.clone());
    let mut generation = 0;
    let mut inputs = Vec::new();
    let mut committed: BTreeMap<u64, AlarmEvent> = BTreeMap::new();
    for chunk in data.chunks_exact(4).take(512) {
        let b: &[u8; 4] = chunk.try_into().unwrap();
        let operation = op(&k, b, &mut generation);
        let before = k.logical_snapshot();
        let digest = k.state_digest();
        let st0 = k.outbox_status().unwrap();
        let input = SequencedInput { operation_sequence: k.last_operation_sequence() + 1, operation: operation.clone() };
        inputs.push(input.clone());
        let r = k.apply(input);
        let after = k.logical_snapshot();
        let st1 = k.outbox_status().unwrap();
        match &r {
            Ok(TransitionOutcome::Applied { operation_sequence, .. }) => {
                assert_eq!(*operation_sequence, before.last_operation_sequence + 1);
                for seq in st0.next_event_seq..st1.next_event_seq {
                    let e = k.alarm_event(seq).expect("new event readable");
                    assert_eq!(e.operation_seq, *operation_sequence);
                    assert_eq!(u64::from(e.ordinal_in_operation), seq - st0.next_event_seq);
                    if let Some(prev) = committed.range(..seq).next_back().map(|(_, e)| *e) {
                        if prev.operation_seq == e.operation_seq {
                            assert!(prev.alarm_id < e.alarm_id, "ordered by alarm id");
                        }
                    }
                    // I5: a process rule never returns while its input is not Good.
                    if e.kind == AlarmEventKind::Returned {
                        let rule = table.rule_index(e.alarm_id).unwrap();
                        let meta = table.rule(rule).unwrap();
                        if !matches!(meta.condition, AlarmCondition::BadQuality) {
                            let t = after.tags.iter().find(|t| t.tag_id == meta.tag_id).unwrap();
                            assert_eq!(t.quality, Quality::Good, "{e:?} returned on a non-Good input");
                        }
                    }
                    committed.insert(seq, e);
                }
            }
            _ => {
                assert_eq!(before, after, "{operation:?} -> {r:?} changed the state");
                assert_eq!(digest, k.state_digest());
                assert_eq!((st0.next_event_seq, st0.used, st0.durable_watermark), (st1.next_event_seq, st1.used, st1.durable_watermark));
            }
        }
        assert!(st1.used <= st1.capacity);
        assert_eq!(st1.used, st1.next_event_seq - 1 - st1.durable_watermark);
        // No overwrite of non-durable events.
        for (seq, e) in committed.range(st1.durable_watermark + 1..) {
            assert_eq!(k.alarm_event(*seq).as_ref(), Some(e), "event {seq} lost before it was durable");
        }
    }
    let mut replay = Kernel::new(cfg);
    for i in inputs {
        let _ = replay.apply(i);
    }
    assert_eq!(replay.state_digest(), k.state_digest());
    assert_eq!(replay.alarm_events_digest(), k.alarm_events_digest());
});

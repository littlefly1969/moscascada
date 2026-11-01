//! VS-004 (brief §7.11): arbitrary interleavings of batches, duplicates, reconnects,
//! silences, restorations and freshness expiries (with fresh, stale, invalid tokens and
//! current / old / unknown link generations) on a supervised kernel. Checked after every
//! input:
//! - refused / ignored / duplicate inputs change nothing and consume no operation sequence;
//! - an applied input consumes exactly the next operation sequence;
//! - revisions never decrease and change exactly when (value, quality) changes;
//! - only an applied batch makes a tag `Good`; a restoration changes no tag; a freshness
//!   expiry only moves `Good` tags with a marker `<=` its token to `StaleData`;
//! - a silent source has an active link;
//! - the freshness list (review P1) agrees with a full scan of every source, for any token;
//! - replaying the same input stream gives the same digest (no hidden time input).
#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use scada_core::*;

fn config() -> Arc<ValidatedConfig> {
    let mut tags = Vec::new();
    let mut sources = Vec::new();
    for (sid, n, liveness, freshness) in [(1u32, 3u32, true, true), (2, 2, false, true), (3, 1, true, false), (4, 1, false, false)] {
        let mut channels = Vec::new();
        for c in 1..=n {
            let tag_id = TagId(sid * 10 + c);
            tags.push(TagSpec { tag_id, name: format!("t{}", tag_id.0), value_type: ValueType::I64 });
            channels.push(ChannelSpec { channel_id: ChannelId(c), tag_id, value_type: ValueType::I64 });
        }
        sources.push(SourceSpec {
            source_id: SourceId(sid),
            name: format!("s{sid}"),
            channels,
            supervision: Supervision { liveness, freshness },
        });
    }
    Arc::new(ValidatedConfig::validate(ConfigSpec { limits: CoreLimits { max_observations_per_batch: 4 }, tags, sources }).unwrap())
}

fn op(k: &Kernel, b: &[u8; 4], generation: &mut u64) -> Operation {
    let sid = SourceId(1 + u32::from(b[1] % 5)); // 5 = unknown source
    let st = k.source_status(sid);
    let active = st.map_or(0, |s| match s.link {
        LinkState::Connected { generation } | LinkState::Disconnected { generation } => generation,
        LinkState::NeverConnected => 0,
    });
    let g = match b[2] % 4 {
        0 | 1 => active,
        2 => active.saturating_sub(1),
        _ => active + 1 + u64::from(b[3] % 3),
    };
    let next = st.and_then(|s| s.last_accepted_source_sequence).map_or(1, |l| l + 1);
    let seq = match b[3] % 5 {
        0 => next.saturating_sub(1),
        4 => next + 1,
        _ => next,
    };
    let last = k.last_operation_sequence();
    let obs = |n: u8| -> Vec<Observation> {
        (1..=u32::from(n % 4).max(1))
            .map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(i64::from(b[3] % 3)) })
            .collect()
    };
    match b[0] % 9 {
        0 => {
            *generation += 1;
            Operation::SourceConnected { source_id: sid, link_generation: *generation, source_epoch: 1 + u64::from(b[3] % 2) }
        }
        1 => Operation::SourceDisconnected { source_id: sid, link_generation: g },
        2 => Operation::ObservationBatch(ObservationBatch {
            source_id: sid,
            link_generation: g,
            mode: BatchMode::FullState,
            source_sequence: seq,
            observations: (1..=k.config().source(sid).map_or(1, |m| m.channel_count() as u32))
                .map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(i64::from(b[3] % 3)) })
                .collect(),
        }),
        3 | 4 => Operation::ObservationBatch(ObservationBatch {
            source_id: sid,
            link_generation: g,
            mode: BatchMode::Delta,
            source_sequence: seq,
            observations: obs(b[2]),
        }),
        5 => Operation::SourceSilenceExpired { source_id: sid, link_generation: g },
        6 => Operation::SourceLivenessRestored { source_id: sid, link_generation: g },
        _ => Operation::FreshnessExpired {
            source_id: sid,
            link_generation: g,
            observed_up_to: match b[3] % 4 {
                0 => 0,
                1 => last + 1, // invalid: not applied yet
                _ => last.saturating_sub(u64::from(b[3] / 4) % (last + 1)),
            },
        },
    }
}

fuzz_target!(|data: &[u8]| {
    let cfg = config();
    let mut k = Kernel::new(cfg.clone());
    let mut generation = 0;
    let mut inputs = Vec::new();
    for chunk in data.chunks_exact(4).take(512) {
        let b: &[u8; 4] = chunk.try_into().unwrap();
        let operation = op(&k, b, &mut generation);
        let before = k.logical_snapshot();
        let digest = k.state_digest();
        let input = SequencedInput { operation_sequence: k.last_operation_sequence() + 1, operation: operation.clone() };
        inputs.push(input.clone());
        let r = k.apply(input);
        let after = k.logical_snapshot();
        match r {
            Ok(TransitionOutcome::Applied { operation_sequence, .. }) => {
                assert_eq!(operation_sequence, before.last_operation_sequence + 1);
                assert_eq!(after.last_operation_sequence, operation_sequence);
            }
            _ => {
                assert_eq!(before, after, "{operation:?} -> {r:?} changed the state");
                assert_eq!(digest, k.state_digest());
            }
        }
        for (x, y) in before.tags.iter().zip(&after.tags) {
            assert!(y.revision >= x.revision);
            assert_eq!(y.revision != x.revision, (y.value, y.quality) != (x.value, x.quality));
            if y.quality == Quality::Good && x.quality != Quality::Good {
                assert!(matches!(operation, Operation::ObservationBatch(_)), "{operation:?} made a tag Good");
            }
            match &operation {
                Operation::SourceLivenessRestored { .. } => assert_eq!(x, y),
                Operation::FreshnessExpired { observed_up_to, .. } if x != y => {
                    assert_eq!((x.quality, y.quality), (Quality::Good, Quality::Uncertain(UncertainReason::StaleData)));
                    assert!(x.last_observed_operation != 0 && x.last_observed_operation <= *observed_up_to);
                }
                _ => {}
            }
        }
        // Review P1: the O(eligible) freshness list agrees with a full scan.
        let last = after.last_operation_sequence;
        for meta in cfg.sources() {
            for up_to in [1, last / 2, last] {
                let oracle = if meta.supervision.freshness {
                    meta.channels()
                        .iter()
                        .filter(|&&(_, t)| {
                            let x = &after.tags[t as usize];
                            x.quality == Quality::Good && x.last_observed_operation != 0 && x.last_observed_operation <= up_to
                        })
                        .count() as u32
                } else {
                    0
                };
                assert_eq!(k.freshness_candidates(meta.source_id, up_to), oracle);
            }
        }
        for s in &after.sources {
            if s.silent {
                assert!(matches!(s.link, LinkState::Connected { .. }) && s.supervision.liveness);
            }
        }
    }
    let mut replay = Kernel::new(cfg);
    for i in inputs {
        let _ = replay.apply(i);
    }
    assert_eq!(replay.state_digest(), k.state_digest());
});

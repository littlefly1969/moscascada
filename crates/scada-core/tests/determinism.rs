//! DoD 9 (+ invariants I1, I3, I4, I6): determinism with reproducible random streams.

mod common;

use std::sync::Arc;

use common::*;
use proptest::prelude::*;
use scada_core::*;

/// Small multi-source configuration so random streams hit every rule.
fn multi_config() -> Arc<ValidatedConfig> {
    let mut tags = Vec::new();
    let mut sources = Vec::new();
    for s in 0..3u32 {
        let mut channels = Vec::new();
        for c in 0..4u32 {
            let tag_id = TagId(100 * (s + 1) + c);
            let value_type = if c % 2 == 0 { ValueType::Bool } else { ValueType::I64 };
            tags.push(TagSpec { tag_id, name: format!("t{}", tag_id.0), value_type });
            channels.push(ChannelSpec { channel_id: ChannelId(c + 1), tag_id, value_type });
        }
        sources.push(SourceSpec { source_id: SourceId(10 + s), name: format!("s{s}"), channels });
    }
    Arc::new(
        ValidatedConfig::validate(ConfigSpec { limits: CoreLimits { max_observations_per_batch: 6 }, tags, sources })
            .unwrap(),
    )
}

/// Deterministic PRNG (SplitMix64); the seed is printed on failure by the caller.
struct SplitMix(u64);
impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Generates a plausible-but-often-invalid operation. Generation counters are kept
/// per stream so that valid connects exist, but stale/unknown ones appear too.
fn random_op(r: &mut SplitMix, generation: &mut u64, k: &Kernel) -> Operation {
    let source_id = SourceId(10 + r.below(4) as u32); // 13 is unknown
    // Steer towards valid inputs using the (read-only) current state, so that every rule is
    // exercised; the stream is still a pure function of the seed and the previous stream.
    let status = k.source_status(source_id);
    let active = match status.map(|s| s.link) {
        Some(LinkState::Connected { generation: g }) => Some(g),
        _ => None,
    };
    let last = status.and_then(|s| s.last_accepted_source_sequence);
    match r.below(10) {
        0 => {
            *generation += 1;
            Operation::SourceConnected {
                source_id,
                link_generation: if r.below(8) == 0 { r.below(*generation + 1) } else { *generation },
                source_epoch: r.below(3),
            }
        }
        1 => Operation::SourceDisconnected {
            source_id,
            link_generation: match active {
                Some(g) if r.below(2) == 0 => g,
                _ => r.below(*generation + 2),
            },
        },
        _ => {
            let full_state = r.below(3) == 0;
            let n = if full_state && r.below(4) != 0 { 4 } else { r.below(8) as usize }; // up to 7 > max 6
            let mut observations: Vec<Observation> = (0..n)
                .map(|idx| {
                    // Channels 1 and 3 are bool, 2 and 4 are i64, 5 is unknown.
                    let ch =
                        if full_state && n == 4 && r.below(8) != 0 { idx as u32 + 1 } else { 1 + r.below(5) as u32 };
                    let typed_bool = ch % 2 == 1;
                    let as_bool = if r.below(10) == 0 { !typed_bool } else { typed_bool };
                    let value = if as_bool { Value::Bool(r.below(2) == 0) } else { Value::I64(r.below(4) as i64 - 1) };
                    Observation { channel_id: ChannelId(ch), value }
                })
                .collect();
            if observations.len() > 1 && r.below(2) == 0 {
                let x = r.below(observations.len() as u64) as usize;
                observations.swap(0, x);
            }
            Operation::ObservationBatch(ObservationBatch {
                source_id,
                link_generation: match active {
                    Some(g) if r.below(6) != 0 => g,
                    _ => r.below(*generation + 1),
                },
                mode: if full_state { BatchMode::FullState } else { BatchMode::Delta },
                source_sequence: match last {
                    Some(l) if r.below(5) != 0 => l.saturating_sub(1) + r.below(4),
                    _ => r.below(4),
                },
                observations,
            })
        }
    }
}

/// Run one stream on two independent kernels and check invariants at every step.
fn run_stream(seed: u64, steps: usize) -> (usize, usize, std::collections::BTreeSet<String>) {
    let cfg = multi_config();
    // Built independently (separate Arc-ed copies of an equal config).
    let mut a = Kernel::new(cfg.clone());
    let mut b = Kernel::new(Arc::new((*multi_config()).clone()));
    let mut r = SplitMix(seed);
    let mut generation = 0;
    let (mut applied, mut rejected) = (0, 0);
    let mut kinds = std::collections::BTreeSet::new();
    for n in 0..steps {
        let op = random_op(&mut r, &mut generation, &a);
        // Mostly the right operation sequence, sometimes a wrong one (DoD 10).
        let next = a.last_operation_sequence() + 1;
        let operation_sequence = if r.below(20) == 0 { next + 1 + r.below(3) } else { next };
        let input = SequencedInput { operation_sequence, operation: op };
        let before = a.logical_snapshot();
        let before_digest = a.state_digest();
        let ra = a.apply(input.clone());
        let rb = b.apply(input);
        kinds.insert(outcome_kind(&ra));
        assert_eq!(ra, rb, "seed {seed} step {n}: outcomes diverge");
        let after = a.logical_snapshot();
        assert_eq!(after, b.logical_snapshot(), "seed {seed} step {n}: snapshots diverge");
        assert_eq!(a.state_digest(), b.state_digest(), "seed {seed} step {n}: digests diverge");
        match ra {
            Err(_) | Ok(TransitionOutcome::Duplicate { .. }) | Ok(TransitionOutcome::Ignored(_)) => {
                // I1: no effect at all.
                assert_eq!(before, after, "seed {seed} step {n}");
                assert_eq!(before_digest, a.state_digest(), "seed {seed} step {n}");
                rejected += 1;
            }
            Ok(TransitionOutcome::Applied { operation_sequence, .. }) => {
                // I3
                assert_eq!(operation_sequence, before.last_operation_sequence + 1);
                assert_eq!(after.last_operation_sequence, operation_sequence);
                applied += 1;
            }
        }
        // I4: revisions never decrease and change iff (value, quality) changes.
        for (x, y) in before.tags.iter().zip(&after.tags) {
            assert!(y.revision >= x.revision);
            let changed = (x.value, x.quality) != (y.value, y.quality);
            assert_eq!(changed, y.revision == x.revision + 1, "seed {seed} step {n} tag {:?}", y.tag_id);
            assert!(y.revision - x.revision <= 1);
        }
    }
    (applied, rejected, kinds)
}

fn outcome_kind(r: &Result<TransitionOutcome, ApplyError>) -> String {
    let s = match r {
        Ok(TransitionOutcome::Applied { kind, .. }) => format!("{kind:?}"),
        Ok(o) => format!("{o:?}"),
        Err(e) => format!("{e:?}"),
    };
    let s = s.split(['{', '(']).next().unwrap_or("").trim().to_owned();
    match r {
        Ok(TransitionOutcome::Applied { kind: AppliedKind::Batch { continuity, .. }, .. }) => {
            format!("Batch/{continuity:?}")
        }
        _ => s,
    }
}

#[test]
fn seeded_random_streams_are_deterministic() {
    let mut total_applied = 0;
    let mut total_rejected = 0;
    let mut kinds = std::collections::BTreeSet::new();
    for seed in 0..200u64 {
        let (a, r, k) = run_stream(seed, 400);
        total_applied += a;
        total_rejected += r;
        kinds.extend(k);
    }
    // Every outcome class must be reached by the generator.
    for expected in [
        "Batch/Continuous",
        "Batch/BaselineEstablished",
        "Batch/Resynchronized",
        "Connected",
        "Disconnected",
        "Duplicate",
        "Ignored",
        "OperationSequenceMismatch",
        "UnknownSource",
        "InvalidEpoch",
        "NonMonotonicLinkGeneration",
        "UnknownLinkGeneration",
        "LinkNotActive",
        "EmptyBatch",
        "BatchTooLarge",
        "UnknownChannel",
        "DuplicateChannel",
        "TypeMismatch",
        "IncompleteFullState",
        "InvalidSequence",
        "ResyncRequired",
        "SequenceGap",
        "StaleSequence",
        "SequenceConflict",
    ] {
        assert!(kinds.contains(expected), "outcome {expected} never generated; got {kinds:?}");
    }
    // The generator must exercise both paths substantially.
    assert!(total_applied > 5_000, "applied {total_applied}");
    assert!(total_rejected > 5_000, "rejected {total_rejected}");
}

#[test]
fn replaying_the_same_stream_gives_the_same_final_digest() {
    let cfg = multi_config();
    let digest_of = |seed| {
        let mut k = Kernel::new(cfg.clone());
        let mut r = SplitMix(seed);
        let mut generation = 0;
        for _ in 0..2000 {
            let op = random_op(&mut r, &mut generation, &k);
            let operation_sequence = k.last_operation_sequence() + 1;
            let _ = k.apply(SequencedInput { operation_sequence, operation: op });
        }
        k.state_digest()
    };
    assert_eq!(digest_of(42), digest_of(42));
    assert_ne!(digest_of(42), digest_of(43));
}

/// Regression anchor: the digest of a fixed scenario on the demo config. If this changes,
/// the canonical encoding changed and STATE_DIGEST_VERSION must be bumped.
#[test]
fn digest_v1_golden_value() {
    let mut k = demo_kernel();
    step(&mut k, connect(1, 0xE1)).unwrap();
    step(&mut k, full(1, 1, false, true, 2150)).unwrap();
    step(&mut k, batch(1, BatchMode::Delta, 2, vec![i(3, -40)])).unwrap();
    step(&mut k, disconnect(1)).unwrap();
    let hex: String = k.state_digest().iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, GOLDEN, "canonical digest encoding v1 changed");
}

const GOLDEN: &str = "6b78000754e86ffcf1a68a4d4215d359f2410618a9d781271f79f013aff66241";

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// DoD 9 via proptest: arbitrary seeds and lengths.
    #[test]
    fn prop_two_kernels_same_stream(seed in any::<u64>(), steps in 1usize..300) {
        let _ = run_stream(seed, steps);
    }

    /// I6: the order of observations inside a batch does not matter.
    #[test]
    fn prop_observation_order_irrelevant(perm_seed in any::<u64>(), door: bool, tamper: bool, temp in any::<i64>()) {
        let mut obs = vec![b(1, door), b(2, tamper), i(3, temp)];
        let mut r = SplitMix(perm_seed);
        for k in (1..obs.len()).rev() {
            obs.swap(k, r.below(k as u64 + 1) as usize);
        }
        let mut x = demo_kernel();
        let mut y = demo_kernel();
        step(&mut x, connect(1, 7)).unwrap();
        step(&mut y, connect(1, 7)).unwrap();
        step(&mut x, full(1, 1, door, tamper, temp)).unwrap();
        step(&mut y, batch(1, BatchMode::FullState, 1, obs)).unwrap();
        prop_assert_eq!(x.state_digest(), y.state_digest());
        prop_assert_eq!(x.logical_snapshot(), y.logical_snapshot());
    }

    /// I1 for single-batch corruption: one invalid observation anywhere rejects all.
    #[test]
    fn prop_one_bad_observation_rejects_whole_batch(pos in 0usize..3, kind in 0u8..3) {
        let mut k = demo_kernel();
        step(&mut k, connect(1, 7)).unwrap();
        step(&mut k, full(1, 1, false, false, 0)).unwrap();
        let mut obs = vec![b(1, true), b(2, true), i(3, 5)];
        let bad = match kind {
            0 => Observation { channel_id: ChannelId(77), value: Value::Bool(true) },
            1 => Observation { channel_id: obs[(pos + 1) % 3].channel_id, value: obs[(pos + 1) % 3].value },
            _ => Observation { channel_id: obs[pos].channel_id, value: match obs[pos].value { Value::Bool(_) => Value::I64(1), Value::I64(_) => Value::Bool(true) } },
        };
        obs[pos] = bad;
        let before = (k.logical_snapshot(), k.state_digest());
        prop_assert!(step(&mut k, batch(1, BatchMode::Delta, 2, obs)).is_err());
        prop_assert_eq!(before, (k.logical_snapshot(), k.state_digest()));
    }
}

//! VS-004 (ADR 0011): liveness and per-tag freshness as explicit, sequenced operations.
//!
//! No clock anywhere: time exists only as the order of the operations the runtime
//! sequences. These tests drive the kernel directly and check the transitions, the
//! precedence of the uncertain reasons, the anti-stale tokens, atomicity and replay.

use std::sync::Arc;

use proptest::prelude::*;
use scada_core::*;

const BOTH: SourceId = SourceId(1); // liveness + freshness, 3 channels
const FRESH: SourceId = SourceId(2); // freshness only, 2 channels
const LIVE: SourceId = SourceId(3); // liveness only, 1 channel
const NONE: SourceId = SourceId(4); // unsupervised, 1 channel

const SILENT: Quality = Quality::Uncertain(UncertainReason::SourceSilent);
const STALE: Quality = Quality::Uncertain(UncertainReason::StaleData);
const UNAVAILABLE: Quality = Quality::Uncertain(UncertainReason::SourceUnavailable);

fn spec(supervised: bool) -> ConfigSpec {
    let layout: [(SourceId, u32, Supervision); 4] = [
        (BOTH, 3, Supervision { liveness: true, freshness: true }),
        (FRESH, 2, Supervision { liveness: false, freshness: true }),
        (LIVE, 1, Supervision { liveness: true, freshness: false }),
        (NONE, 1, Supervision::NONE),
    ];
    let mut tags = Vec::new();
    let mut sources = Vec::new();
    for (sid, n, sup) in layout {
        let mut channels = Vec::new();
        for c in 1..=n {
            let tag_id = TagId(sid.0 * 10 + c);
            tags.push(TagSpec { tag_id, name: format!("t{}", tag_id.0), value_type: ValueType::I64 });
            channels.push(ChannelSpec { channel_id: ChannelId(c), tag_id, value_type: ValueType::I64 });
        }
        sources.push(SourceSpec {
            source_id: sid,
            name: format!("s{}", sid.0),
            channels,
            supervision: if supervised { sup } else { Supervision::NONE },
        });
    }
    ConfigSpec { limits: CoreLimits { max_observations_per_batch: 8 }, tags, sources }
}

fn kernel(supervised: bool) -> Kernel {
    Kernel::new(Arc::new(ValidatedConfig::validate(spec(supervised)).unwrap()))
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

fn batch(s: SourceId, g: u64, mode: BatchMode, seq: u64, obs: &[(u32, i64)]) -> Operation {
    Operation::ObservationBatch(ObservationBatch {
        source_id: s,
        link_generation: g,
        mode,
        source_sequence: seq,
        observations: obs
            .iter()
            .map(|&(c, v)| Observation { channel_id: ChannelId(c), value: Value::I64(v) })
            .collect(),
    })
}

fn silence(s: SourceId, g: u64) -> Operation {
    Operation::SourceSilenceExpired { source_id: s, link_generation: g }
}

fn restore(s: SourceId, g: u64) -> Operation {
    Operation::SourceLivenessRestored { source_id: s, link_generation: g }
}

fn expire(s: SourceId, g: u64, up_to: u64) -> Operation {
    Operation::FreshnessExpired { source_id: s, link_generation: g, observed_up_to: up_to }
}

fn q(k: &Kernel, t: u32) -> Quality {
    k.tag(TagId(t)).unwrap().quality
}

fn rev(k: &Kernel, t: u32) -> u64 {
    k.tag(TagId(t)).unwrap().revision
}

fn assert_noop(k: &mut Kernel, op: Operation, expected: Result<TransitionOutcome, ApplyError>) {
    let (snap, digest, last) = (k.logical_snapshot(), k.state_digest(), k.last_operation_sequence());
    assert_eq!(step(k, op), expected);
    assert_eq!(snap, k.logical_snapshot());
    assert_eq!(digest, k.state_digest());
    assert_eq!(last, k.last_operation_sequence(), "a no-op never consumes an operation sequence");
}

/// Brief §7.1: without supervision the encodings stay v1 and the new operations are
/// refused without effect, so the VS-003 contract is unchanged.
#[test]
fn unsupervised_config_keeps_v1_and_refuses_supervision_operations() {
    let mut k = kernel(false);
    assert_eq!(k.config().fingerprint_version(), 1);
    assert_eq!(k.state_digest_version(), 1);
    ok(&mut k, connect(BOTH, 1));
    ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 1, &[(1, 1), (2, 2), (3, 3)]));
    for op in [silence(BOTH, 1), restore(BOTH, 1), expire(BOTH, 1, 2)] {
        assert_noop(&mut k, op, Err(ApplyError::SupervisionNotConfigured(BOTH)));
    }
    let snap = k.logical_snapshot();
    assert!(snap.tags.iter().all(|t| t.last_observed_operation == 0), "no marker without supervision");
    assert!(snap.sources.iter().all(|s| s.last_batch_operation == 0 && !s.silent));
    assert_eq!(kernel(true).config().fingerprint_version(), 2);
    assert_eq!(kernel(true).state_digest_version(), 2);
    assert_ne!(kernel(true).config().fingerprint(), kernel(false).config().fingerprint());
}

/// Brief §7.2: silence on an open link: tags with a value become `SourceSilent`, values
/// kept, the link stays `Connected`; a second expiry is a no-op (no double revision).
#[test]
fn silence_degrades_values_and_keeps_the_link() {
    let mut k = kernel(true);
    ok(&mut k, connect(BOTH, 1));
    ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 1, &[(1, 10), (2, 20), (3, 30)]));
    let r = rev(&k, 11);
    assert!(matches!(
        step(&mut k, silence(BOTH, 1)),
        Ok(TransitionOutcome::Applied { kind: AppliedKind::SilenceExpired { degraded_tags: 3 }, .. })
    ));
    let st = k.source_status(BOTH).unwrap();
    assert_eq!(st.link, LinkState::Connected { generation: 1 });
    assert!(st.silent);
    assert_eq!(q(&k, 11), SILENT);
    assert_eq!(k.tag(TagId(11)).unwrap().value, Some(Value::I64(10)));
    assert_eq!(rev(&k, 11), r + 1);
    assert_noop(&mut k, silence(BOTH, 1), Ok(TransitionOutcome::Ignored(IgnoredReason::AlreadySilent)));
    // A silent source that was never observed degrades nothing but is still silent.
    ok(&mut k, connect(LIVE, 2));
    assert!(matches!(
        step(&mut k, silence(LIVE, 2)),
        Ok(TransitionOutcome::Applied { kind: AppliedKind::SilenceExpired { degraded_tags: 0 }, .. })
    ));
    assert_eq!(q(&k, 31), Quality::Unknown);
}

/// Brief §7.3 and §5.1: a heartbeat after silence restores the communication state only;
/// no tag becomes `Good` and no revision changes. Only a new observation does that, for
/// its own tags.
#[test]
fn liveness_restoration_never_makes_a_tag_good() {
    let mut k = kernel(true);
    ok(&mut k, connect(BOTH, 1));
    ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 1, &[(1, 10), (2, 20), (3, 30)]));
    ok(&mut k, silence(BOTH, 1));
    let revs: Vec<u64> = (11..=13).map(|t| rev(&k, t)).collect();
    assert!(matches!(
        step(&mut k, restore(BOTH, 1)),
        Ok(TransitionOutcome::Applied { kind: AppliedKind::LivenessRestored, .. })
    ));
    assert!(!k.source_status(BOTH).unwrap().silent);
    for (t, r) in (11..=13).zip(&revs) {
        assert_eq!(q(&k, t), SILENT, "restoration leaves tag {t} uncertain");
        assert_eq!(rev(&k, t), *r);
    }
    assert_noop(&mut k, restore(BOTH, 1), Ok(TransitionOutcome::Ignored(IgnoredReason::NotSilent)));
    // A Delta refreshes only the tag it carries (and is itself liveness evidence).
    ok(&mut k, silence(BOTH, 1));
    ok(&mut k, batch(BOTH, 1, BatchMode::Delta, 2, &[(2, 21)]));
    assert!(!k.source_status(BOTH).unwrap().silent, "an applied batch is liveness evidence");
    assert_eq!((q(&k, 11), q(&k, 12), q(&k, 13)), (SILENT, Quality::Good, SILENT));
}

/// Brief §7.4: FullState then a partial Delta: only observed tags are refreshed; the
/// others expire; a never-observed tag stays `Unknown`.
#[test]
fn freshness_is_per_tag() {
    let mut k = kernel(true);
    ok(&mut k, connect(BOTH, 1));
    let full = ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 1, &[(1, 10), (2, 20), (3, 30)]));
    let delta = ok(&mut k, batch(BOTH, 1, BatchMode::Delta, 2, &[(1, 11)]));
    assert_eq!(k.tag(TagId(11)).unwrap().last_observed_operation, delta);
    assert_eq!(k.tag(TagId(12)).unwrap().last_observed_operation, full);
    assert_eq!(k.freshness_candidates(BOTH, full), 2);
    assert!(matches!(
        step(&mut k, expire(BOTH, 1, full)),
        Ok(TransitionOutcome::Applied { kind: AppliedKind::FreshnessExpired { expired_tags: 2 }, .. })
    ));
    assert_eq!((q(&k, 11), q(&k, 12), q(&k, 13)), (Quality::Good, STALE, STALE));
    assert_eq!(k.tag(TagId(12)).unwrap().value, Some(Value::I64(20)), "value kept");
    assert!(!k.source_status(BOTH).unwrap().silent, "freshness does not touch liveness");
    // FRESH was never observed: Unknown stays Unknown, nothing to expire.
    ok(&mut k, connect(FRESH, 2));
    assert_eq!(k.freshness_candidates(FRESH, delta), 0);
    assert_noop(&mut k, expire(FRESH, 2, delta), Ok(TransitionOutcome::Ignored(IgnoredReason::NothingExpired)));
    assert_eq!(q(&k, 21), Quality::Unknown);
    // A new observation brings its own tag back to Good.
    ok(&mut k, batch(BOTH, 1, BatchMode::Delta, 3, &[(3, 30)]));
    assert_eq!(q(&k, 13), Quality::Good);
}

/// Brief §7.5: an identical value with a new `source_sequence` renews the marker without a
/// revision change, so a token computed before it no longer expires the tag; a duplicate
/// renews nothing.
#[test]
fn identical_observation_renews_the_marker_and_defeats_an_older_token() {
    let mut k = kernel(true);
    ok(&mut k, connect(BOTH, 1));
    let full = ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 1, &[(1, 10), (2, 20), (3, 30)]));
    let r = rev(&k, 11);
    let again = ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 2, &[(1, 10), (2, 20), (3, 30)]));
    assert_eq!(rev(&k, 11), r, "same value, same quality: same revision");
    assert_eq!(k.tag(TagId(11)).unwrap().last_observed_operation, again);
    // The token `full` was computed before the renewal: stale, nothing expires.
    assert_noop(&mut k, expire(BOTH, 1, full), Ok(TransitionOutcome::Ignored(IgnoredReason::NothingExpired)));
    // A duplicate (same sequence, same payload) is not a new observation.
    let before = k.logical_snapshot();
    assert_eq!(
        step(&mut k, batch(BOTH, 1, BatchMode::FullState, 2, &[(1, 10), (2, 20), (3, 30)])),
        Ok(TransitionOutcome::Duplicate { source_sequence: 2 })
    );
    assert_eq!(before, k.logical_snapshot());
    // A rejected batch neither.
    assert!(step(&mut k, batch(BOTH, 1, BatchMode::Delta, 9, &[(1, 10)])).is_err());
    assert_eq!(before, k.logical_snapshot());
    ok(&mut k, expire(BOTH, 1, again));
    assert_eq!(q(&k, 11), STALE);
}

/// Precedence `SourceUnavailable` > `SourceSilent` > `StaleData`, a revision only on an
/// effective change, and stale / foreign link generations.
#[test]
fn precedence_and_link_generations() {
    let mut k = kernel(true);
    ok(&mut k, connect(BOTH, 1));
    let full = ok(&mut k, batch(BOTH, 1, BatchMode::FullState, 1, &[(1, 10), (2, 20), (3, 30)]));
    ok(&mut k, batch(BOTH, 1, BatchMode::Delta, 2, &[(1, 11)]));
    ok(&mut k, expire(BOTH, 1, full)); // 12, 13 stale
    let r12 = rev(&k, 12);
    // Silence upgrades every valued tag (Good and StaleData) to SourceSilent.
    assert!(matches!(
        step(&mut k, silence(BOTH, 1)),
        Ok(TransitionOutcome::Applied { kind: AppliedKind::SilenceExpired { degraded_tags: 3 }, .. })
    ));
    assert_eq!(rev(&k, 12), r12 + 1);
    // Freshness never downgrades SourceSilent.
    assert_noop(&mut k, expire(BOTH, 1, full + 1), Ok(TransitionOutcome::Ignored(IgnoredReason::NothingExpired)));
    // Disconnection: SourceUnavailable wins, silence ends with the link.
    ok(&mut k, Operation::SourceDisconnected { source_id: BOTH, link_generation: 1 });
    assert_eq!((q(&k, 11), q(&k, 12)), (UNAVAILABLE, UNAVAILABLE));
    assert!(!k.source_status(BOTH).unwrap().silent);
    // Late supervision operations of the closed link: no-ops.
    for op in [silence(BOTH, 1), restore(BOTH, 1), expire(BOTH, 1, full)] {
        assert_noop(&mut k, op, Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration)));
    }
    // New link: starts alive; the tags stay unavailable until observed (no elevation on
    // mere connection); an expiry of the old generation can never degrade it.
    ok(&mut k, connect(BOTH, 5));
    assert!(!k.source_status(BOTH).unwrap().silent);
    assert_eq!(q(&k, 11), UNAVAILABLE);
    ok(&mut k, batch(BOTH, 5, BatchMode::Delta, 3, &[(2, 22)]));
    assert_eq!(q(&k, 12), Quality::Good);
    for op in [silence(BOTH, 1), expire(BOTH, 1, k.last_operation_sequence())] {
        assert_noop(&mut k, op, Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration)));
    }
    assert_eq!(q(&k, 12), Quality::Good);
    // A generation never seen is an error, not a no-op.
    for op in [silence(BOTH, 9), expire(BOTH, 9, 1)] {
        assert_noop(&mut k, op, Err(ApplyError::UnknownLinkGeneration { got: 9 }));
    }
}

#[test]
fn supervision_kind_and_token_are_validated() {
    let mut k = kernel(true);
    ok(&mut k, connect(FRESH, 1));
    ok(&mut k, connect(LIVE, 2));
    ok(&mut k, connect(NONE, 3));
    assert_noop(&mut k, silence(FRESH, 1), Err(ApplyError::SupervisionNotConfigured(FRESH)));
    assert_noop(&mut k, expire(LIVE, 2, 1), Err(ApplyError::SupervisionNotConfigured(LIVE)));
    assert_noop(&mut k, restore(NONE, 3), Err(ApplyError::SupervisionNotConfigured(NONE)));
    assert_noop(&mut k, silence(SourceId(99), 1), Err(ApplyError::UnknownSource(SourceId(99))));
    let next = k.last_operation_sequence() + 1;
    assert_noop(&mut k, expire(FRESH, 1, 0), Err(ApplyError::InvalidFreshnessToken { observed_up_to: 0 }));
    assert_noop(&mut k, expire(FRESH, 1, next), Err(ApplyError::InvalidFreshnessToken { observed_up_to: next }));
    // Never connected: the generation is unknown.
    let mut k = kernel(true);
    assert_noop(&mut k, silence(BOTH, 1), Err(ApplyError::UnknownLinkGeneration { got: 1 }));
}

/// The unsupervised source of a supervised configuration behaves exactly as in VS-003.
#[test]
fn unsupervised_source_in_a_supervised_config_has_no_markers() {
    let mut k = kernel(true);
    ok(&mut k, connect(NONE, 1));
    ok(&mut k, batch(NONE, 1, BatchMode::FullState, 1, &[(1, 5)]));
    assert_eq!(k.tag(TagId(41)).unwrap().last_observed_operation, 0);
    assert_eq!(k.source_status(NONE).unwrap().last_batch_operation, 0);
    // LIVE has liveness only: batch marker yes, tag marker no.
    ok(&mut k, connect(LIVE, 2));
    let op = ok(&mut k, batch(LIVE, 2, BatchMode::FullState, 1, &[(1, 5)]));
    assert_eq!(k.source_status(LIVE).unwrap().last_batch_operation, op);
    assert_eq!(k.tag(TagId(31)).unwrap().last_observed_operation, 0);
}

// ---------------------------------------------------------------------------------------
// Property tests over interleaved streams (brief §7.6, §7.8, §7.11).

#[derive(Debug, Clone)]
enum Act {
    Connect(u8),
    Disconnect(u8, bool),
    Full(u8, i64),
    Delta(u8, u8, i64),
    SameAgain(u8),
    Duplicate(u8),
    Silence(u8, bool),
    Restore(u8, bool),
    Expire(u8, bool, u8),
}

fn arb_act() -> impl Strategy<Value = Act> {
    let s = 0u8..4;
    prop_oneof![
        1 => s.clone().prop_map(Act::Connect),
        1 => (s.clone(), any::<bool>()).prop_map(|(a, b)| Act::Disconnect(a, b)),
        2 => (s.clone(), -3i64..3).prop_map(|(a, v)| Act::Full(a, v)),
        4 => (s.clone(), 1u8..4, -3i64..3).prop_map(|(a, c, v)| Act::Delta(a, c, v)),
        1 => s.clone().prop_map(Act::SameAgain),
        1 => s.clone().prop_map(Act::Duplicate),
        2 => (s.clone(), any::<bool>()).prop_map(|(a, b)| Act::Silence(a, b)),
        2 => (s.clone(), any::<bool>()).prop_map(|(a, b)| Act::Restore(a, b)),
        3 => (s.clone(), any::<bool>(), any::<u8>()).prop_map(|(a, b, t)| Act::Expire(a, b, t)),
    ]
}

const SOURCES: [(SourceId, u32); 4] = [(BOTH, 3), (FRESH, 2), (LIVE, 1), (NONE, 1)];

/// Turn an abstract action into a concrete operation against the current state (mostly
/// valid, sometimes stale or invalid on purpose).
fn concretize(k: &Kernel, a: &Act, generation: &mut u64, last_batch: &mut [Option<Operation>; 4]) -> Operation {
    let s = match a {
        Act::Connect(s)
        | Act::Disconnect(s, _)
        | Act::Full(s, _)
        | Act::Delta(s, _, _)
        | Act::SameAgain(s)
        | Act::Duplicate(s)
        | Act::Silence(s, _)
        | Act::Restore(s, _)
        | Act::Expire(s, _, _) => *s,
    };
    let i = usize::from(s) % 4;
    let (sid, n) = SOURCES[i];
    let st = k.source_status(sid).unwrap();
    let active = match st.link {
        LinkState::Connected { generation } | LinkState::Disconnected { generation } => generation,
        LinkState::NeverConnected => 0,
    };
    let gen_of = |current: bool| if current { active } else { active.saturating_sub(1) };
    let next_seq = st.last_accepted_source_sequence.map_or(1, |l| l + 1);
    match a {
        Act::Connect(_) => {
            *generation += 1;
            connect(sid, *generation)
        }
        Act::Disconnect(_, cur) => Operation::SourceDisconnected { source_id: sid, link_generation: gen_of(*cur) },
        Act::Full(_, v) => {
            let obs: Vec<(u32, i64)> = (1..=n).map(|c| (c, *v + i64::from(c))).collect();
            let op = batch(sid, active, BatchMode::FullState, next_seq, &obs);
            last_batch[i] = Some(op.clone());
            op
        }
        Act::Delta(_, c, v) => {
            let op = batch(sid, active, BatchMode::Delta, next_seq, &[(u32::from(*c).min(n), *v)]);
            last_batch[i] = Some(op.clone());
            op
        }
        Act::SameAgain(_) => {
            // Identical payload, new sequence: refreshes markers without revisions.
            let snap = k.logical_snapshot();
            let src = snap.sources.iter().find(|s| s.source_id == sid).unwrap();
            match &src.last_batch {
                Some(lb) => {
                    let obs: Vec<(u32, i64)> = lb
                        .entries
                        .iter()
                        .map(|&(t, v)| {
                            (
                                t.0 % 10,
                                match v {
                                    Value::I64(x) => x,
                                    Value::Bool(_) => 0,
                                },
                            )
                        })
                        .collect();
                    batch(sid, active, lb.mode, next_seq, &obs)
                }
                None => batch(sid, active, BatchMode::Delta, next_seq, &[(1, 0)]),
            }
        }
        Act::Duplicate(_) => {
            last_batch[i].clone().unwrap_or_else(|| batch(sid, active, BatchMode::Delta, 1, &[(1, 0)]))
        }
        Act::Silence(_, cur) => silence(sid, gen_of(*cur)),
        Act::Restore(_, cur) => restore(sid, gen_of(*cur)),
        Act::Expire(_, cur, t) => {
            let last = k.last_operation_sequence();
            let up_to = if last == 0 { 1 } else { last.saturating_sub(u64::from(*t) % (last + 1)) };
            expire(sid, gen_of(*cur), up_to)
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// Invariants of every supervised transition, on random interleavings of batches,
    /// duplicates, identical observations, reconnects, silences, restorations and
    /// freshness expiries (fresh, stale and invalid tokens; current and old generations).
    #[test]
    fn prop_supervision_invariants(acts in prop::collection::vec(arb_act(), 1..120)) {
        let mut k = kernel(true);
        let mut generation = 0u64;
        let mut last_batch: [Option<Operation>; 4] = Default::default();
        let mut inputs = Vec::new();
        for a in &acts {
            let op = concretize(&k, a, &mut generation, &mut last_batch);
            let before = k.logical_snapshot();
            let digest = k.state_digest();
            let input = SequencedInput { operation_sequence: k.last_operation_sequence() + 1, operation: op.clone() };
            inputs.push(input.clone());
            let r = k.apply(input);
            let after = k.logical_snapshot();
            match &r {
                Err(_) | Ok(TransitionOutcome::Duplicate { .. }) | Ok(TransitionOutcome::Ignored(_)) => {
                    // I1 + I3: no effect, no operation sequence.
                    prop_assert_eq!(&before, &after);
                    prop_assert_eq!(digest, k.state_digest());
                }
                Ok(TransitionOutcome::Applied { operation_sequence, .. }) => {
                    prop_assert_eq!(*operation_sequence, before.last_operation_sequence + 1);
                    prop_assert_eq!(after.last_operation_sequence, *operation_sequence);
                }
            }
            for (b, t) in before.tags.iter().zip(&after.tags) {
                // I4: revisions never decrease and change only with (value, quality).
                prop_assert!(t.revision >= b.revision);
                prop_assert_eq!(t.revision != b.revision, (t.value, t.quality) != (b.value, b.quality));
                // Only an applied batch can make a tag Good (never a heartbeat/restoration).
                if t.quality == Quality::Good && b.quality != Quality::Good {
                    prop_assert!(matches!(op, Operation::ObservationBatch(_)), "{:?} made {:?} Good", op, t.tag_id);
                }
                // Values are never lost by supervision.
                if b.value.is_some() {
                    prop_assert!(t.value.is_some());
                }
                if t.value.is_none() {
                    prop_assert_eq!(t.quality, Quality::Unknown);
                }
                match &op {
                    Operation::SourceLivenessRestored { .. } => prop_assert_eq!(b, t),
                    Operation::FreshnessExpired { observed_up_to, .. } if b != t => {
                        prop_assert_eq!(b.quality, Quality::Good);
                        prop_assert_eq!(t.quality, STALE);
                        prop_assert!(b.last_observed_operation != 0 && b.last_observed_operation <= *observed_up_to);
                    }
                    Operation::SourceSilenceExpired { .. } if b != t => {
                        prop_assert!(matches!(b.quality, Quality::Good | Quality::Uncertain(UncertainReason::StaleData)), "bad silence source");
                        prop_assert_eq!(t.quality, SILENT);
                    }
                    _ => {}
                }
            }
            // Review P1: the O(eligible) freshness list agrees with a full scan, for every
            // source and for every token, and an applied expiry leaves no eligible tag.
            let cfg = k.config().clone();
            let last = after.last_operation_sequence;
            for meta in cfg.sources() {
                for up_to in [0, 1, last / 3, last / 2, last.saturating_sub(1), last] {
                    let oracle = if meta.supervision.freshness {
                        meta.channels().iter().filter(|&&(_, t)| {
                            let tag = &after.tags[t as usize];
                            tag.quality == Quality::Good && tag.last_observed_operation != 0 && tag.last_observed_operation <= up_to
                        }).count() as u32
                    } else { 0 };
                    prop_assert_eq!(k.freshness_candidates(meta.source_id, up_to), oracle, "source {:?} token {}", meta.source_id, up_to);
                }
            }
            if let (Operation::FreshnessExpired { source_id, observed_up_to, .. }, Ok(TransitionOutcome::Applied { .. })) = (&op, &r) {
                prop_assert_eq!(k.freshness_candidates(*source_id, *observed_up_to), 0);
            }
            for s in &after.sources {
                // Silent implies an active link, and only for liveness-supervised sources.
                if s.silent {
                    prop_assert!(matches!(s.link, LinkState::Connected { .. }), "silent without link");
                    prop_assert!(s.supervision.liveness);
                }
            }
        }
        // I2 / brief §7.8: replaying the exact input stream (with the ignored and refused
        // inputs) gives the same snapshot and digest, without any clock.
        let mut replay = kernel(true);
        for input in inputs {
            let _ = replay.apply(input);
        }
        prop_assert_eq!(replay.logical_snapshot(), k.logical_snapshot());
        prop_assert_eq!(replay.state_digest(), k.state_digest());
    }

    /// Digest v2 is injective on the logical snapshot over reachable states: equal
    /// snapshots give equal digests and different snapshots (markers included) different
    /// digests.
    #[test]
    fn prop_digest_v2_tracks_the_snapshot(a in prop::collection::vec(arb_act(), 1..40), b in prop::collection::vec(arb_act(), 1..40)) {
        let run = |acts: &[Act]| {
            let mut k = kernel(true);
            let mut g = 0;
            let mut lb: [Option<Operation>; 4] = Default::default();
            for x in acts {
                let op = concretize(&k, x, &mut g, &mut lb);
                let _ = step(&mut k, op);
            }
            (k.logical_snapshot(), k.state_digest())
        };
        let (sa, da) = run(&a);
        let (sb, db) = run(&b);
        prop_assert_eq!(sa == sb, da == db);
    }
}

/// The generator really exercises every supervised transition (otherwise the properties
/// above could pass vacuously).
#[test]
fn generator_covers_every_supervised_transition() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..300 {
        let acts = prop::collection::vec(arb_act(), 1..120).new_tree(&mut runner).unwrap().current();
        let mut k = kernel(true);
        let (mut g, mut lb) = (0, Default::default());
        for a in &acts {
            let op = concretize(&k, a, &mut g, &mut lb);
            let label = match step(&mut k, op) {
                Ok(TransitionOutcome::Applied { kind, .. }) => match kind {
                    AppliedKind::SilenceExpired { degraded_tags } if degraded_tags > 0 => "silence>0",
                    AppliedKind::SilenceExpired { .. } => "silence0",
                    AppliedKind::LivenessRestored => "restored",
                    AppliedKind::FreshnessExpired { .. } => "expired",
                    AppliedKind::Batch { changed_tags: 0, .. } => "batch-unchanged",
                    AppliedKind::Batch { .. } => "batch",
                    _ => "link",
                },
                Ok(TransitionOutcome::Ignored(r)) => match r {
                    IgnoredReason::AlreadySilent => "ign-silent",
                    IgnoredReason::NotSilent => "ign-notsilent",
                    IgnoredReason::NothingExpired => "ign-nothing",
                    IgnoredReason::StaleLinkGeneration => "ign-stale",
                    IgnoredReason::AlreadyAcknowledged | IgnoredReason::WatermarkUnchanged => "ign-alarm",
                },
                Ok(TransitionOutcome::Duplicate { .. }) => "duplicate",
                Err(ApplyError::SupervisionNotConfigured(_)) => "err-notconfigured",
                Err(_) => "err",
            };
            seen.insert(label);
        }
    }
    for l in [
        "silence>0",
        "restored",
        "expired",
        "batch-unchanged",
        "batch",
        "ign-silent",
        "ign-notsilent",
        "ign-nothing",
        "ign-stale",
        "duplicate",
        "err-notconfigured",
    ] {
        assert!(seen.contains(l), "{l} never generated: {seen:?}");
    }
}

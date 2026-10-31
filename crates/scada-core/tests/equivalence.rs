//! VS-002 gate: the optimized `Kernel::apply` is observably identical to the frozen VS-001
//! implementation (`reference/vs001_kernel.rs`) on the same sequenced input stream:
//! same outcome or error for every input, same logical snapshot after every step.
//! Streams cover sorted, reversed, shuffled and duplicated observations, sparse and dense
//! channel numbering, full states, gaps, stale and duplicate sequences, takeovers.

#[path = "reference/vs001_kernel.rs"]
mod reference;

use std::sync::Arc;

use proptest::prelude::*;
use reference::RefKernel;
use scada_core::*;

/// Source 1: dense channels 1..=16. Source 2: sparse channels (multiples of 1000).
/// Source 3: 1 channel. Mixed bool / i64.
fn config() -> Arc<ValidatedConfig> {
    let mut tags = Vec::new();
    let mut sources = Vec::new();
    let layouts: [(u32, Vec<u32>); 3] =
        [(1, (1..=16).collect()), (2, (1..=12).map(|c| c * 1000 + 7).collect()), (3, vec![5])];
    for (s, chans) in layouts.iter() {
        let mut channels = Vec::new();
        for (i, c) in chans.iter().enumerate() {
            let tag_id = TagId(s * 100 + i as u32 + 1);
            let value_type = if i % 3 == 0 { ValueType::Bool } else { ValueType::I64 };
            tags.push(TagSpec { tag_id, name: format!("t{}", tag_id.0), value_type });
            channels.push(ChannelSpec { channel_id: ChannelId(*c), tag_id, value_type });
        }
        sources.push(SourceSpec { source_id: SourceId(*s), name: format!("s{s}"), channels });
    }
    Arc::new(
        ValidatedConfig::validate(ConfigSpec { limits: CoreLimits { max_observations_per_batch: 16 }, tags, sources })
            .unwrap(),
    )
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn gen_op(r: &mut Rng, cfg: &ValidatedConfig, generation: &mut u64, k: &Kernel) -> Operation {
    let source_id = SourceId(1 + r.below(4) as u32); // 4 is unknown
    let status = k.source_status(source_id);
    let active = match status.map(|s| s.link) {
        Some(LinkState::Connected { generation: g }) => Some(g),
        _ => None,
    };
    let last = status.and_then(|s| s.last_accepted_source_sequence);
    match r.below(12) {
        0 => {
            *generation += 1;
            Operation::SourceConnected {
                source_id,
                link_generation: if r.below(10) == 0 { r.below(*generation + 1) } else { *generation },
                source_epoch: 1 + r.below(2),
            }
        }
        1 => Operation::SourceDisconnected {
            source_id,
            link_generation: match active {
                Some(g) if r.below(3) != 0 => g,
                _ => r.below(*generation + 2),
            },
        },
        _ => {
            let meta = cfg.source(source_id);
            let chans: Vec<(ChannelId, ValueType)> = meta.map_or(vec![(ChannelId(5), ValueType::I64)], |m| {
                m.channels().iter().map(|&(c, t)| (c, cfg.tags()[t as usize].value_type)).collect()
            });
            let full = r.below(3) == 0;
            let n = if full { chans.len() } else { 1 + r.below(chans.len() as u64) as usize };
            // Start from canonical order, pick a subset for deltas.
            let mut picked: Vec<(ChannelId, ValueType)> = chans.clone();
            if !full {
                for i in (1..picked.len()).rev() {
                    picked.swap(i, r.below(i as u64 + 1) as usize);
                }
                picked.truncate(n);
                picked.sort_by_key(|x| x.0);
            }
            // Order: 0 sorted, 1 reversed, 2 shuffled.
            match r.below(3) {
                0 => {}
                1 => picked.reverse(),
                _ => {
                    for i in (1..picked.len()).rev() {
                        picked.swap(i, r.below(i as u64 + 1) as usize);
                    }
                }
            }
            let mut observations: Vec<Observation> = picked
                .iter()
                .map(|&(c, t)| Observation {
                    channel_id: c,
                    value: match t {
                        ValueType::Bool => Value::Bool(r.below(2) == 0),
                        ValueType::I64 => Value::I64(r.below(4) as i64 - 1),
                    },
                })
                .collect();
            match r.below(14) {
                0 if !observations.is_empty() => {
                    // duplicate channel, adjacent or not
                    let i = r.below(observations.len() as u64) as usize;
                    let o = observations[i];
                    let j = r.below(observations.len() as u64 + 1) as usize;
                    observations.insert(j, o);
                }
                1 => observations.push(Observation { channel_id: ChannelId(999_999), value: Value::I64(0) }),
                2 if !observations.is_empty() => {
                    let i = r.below(observations.len() as u64) as usize;
                    observations[i].value = match observations[i].value {
                        Value::Bool(_) => Value::I64(1),
                        Value::I64(_) => Value::Bool(true),
                    };
                }
                3 => observations.clear(),
                4 => {
                    while observations.len() <= 16 {
                        observations.push(
                            observations
                                .first()
                                .copied()
                                .unwrap_or(Observation { channel_id: ChannelId(1), value: Value::Bool(true) }),
                        );
                    }
                }
                _ => {}
            }
            Operation::ObservationBatch(ObservationBatch {
                source_id,
                link_generation: match active {
                    Some(g) if r.below(8) != 0 => g,
                    _ => r.below(*generation + 2),
                },
                mode: if full { BatchMode::FullState } else { BatchMode::Delta },
                source_sequence: match last {
                    Some(l) if r.below(6) != 0 => (l + r.below(3)).saturating_sub(r.below(2)),
                    _ => r.below(4),
                },
                observations,
            })
        }
    }
}

fn run(seed: u64, steps: usize) -> usize {
    let cfg = config();
    let mut k = Kernel::new(cfg.clone());
    let mut reference = RefKernel::new(cfg.clone());
    let mut r = Rng(seed);
    let mut generation = 0;
    let mut applied = 0;
    for step in 0..steps {
        let op = gen_op(&mut r, &cfg, &mut generation, &k);
        let next = k.last_operation_sequence() + 1;
        let operation_sequence = if r.below(25) == 0 { next + 1 } else { next };
        let input = SequencedInput { operation_sequence, operation: op };
        let a = k.apply(input.clone());
        let b = reference.apply(input.clone());
        assert_eq!(a, b, "seed {seed} step {step}: outcome differs for {input:?}");
        assert_eq!(k.logical_snapshot(), reference.logical_snapshot(), "seed {seed} step {step}: state differs");
        applied += usize::from(matches!(a, Ok(TransitionOutcome::Applied { .. })));
    }
    applied
}

#[test]
fn optimized_kernel_matches_vs001_on_seeded_streams() {
    let mut applied = 0;
    for seed in 0..200 {
        applied += run(seed, 400);
    }
    // The generator must actually exercise successful transitions, not only rejections.
    assert!(applied > 10_000, "only {applied} applied transitions");
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]
    #[test]
    fn prop_optimized_kernel_matches_vs001(seed in any::<u64>(), steps in 1usize..300) {
        run(seed, steps);
    }
}

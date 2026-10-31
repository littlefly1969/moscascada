//! Criterion microbenchmarks for the VS-002 hot path (brief §3 A4, §5 C3/C4).
//!
//! `cargo bench -p scada-bench --bench micro [-- FILTER]`
//!
//! - `decode/fresh/*`: `DriverMessage::decode` of an ObservationBatch frame payload (VS-001).
//! - `decode/reuse/*`: `convert::decode_driver_message` with a reused scratch batch (C3),
//!   limit 128; `decode/reuse_nolimit/*`: same with no limit (review P1: the fast path
//!   only compares the count after the merge, so the two must match).
//! - `convert/*`: protocol batch -> core batch.
//! - `apply/{order}/{n}`: one Delta of n changed i64 observations on a source with 128
//!   channels, input in canonical (sorted), reversed or shuffled channel order.
//!
//! Inputs are built in criterion's untimed setup (`iter_batched`).
//!
//! A/B between commits uses criterion baselines on the SAME crate and code path
//! (`-- --save-baseline A` on the old commit, `-- --baseline A` on the new one).
//! Comparing two implementations compiled in different crates was tried and rejected:
//! identical code measured up to 50 % apart because of different inlining/layout.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use scada_core::{
    BatchMode, ChannelId, ChannelSpec, ConfigSpec, CoreLimits, Kernel, Observation, ObservationBatch, Operation,
    SequencedInput, SourceId, SourceSpec, TagId, TagSpec, ValidatedConfig, Value, ValueType,
};
use scada_protocol::Message;
use scada_protocol::driver_v1 as pb;

const CH: u32 = 128;
const SIZES: [u32; 3] = [1, 32, 128];

fn wire_batch(n: u32, seq: u64) -> Vec<u8> {
    pb::DriverMessage {
        body: Some(pb::driver_message::Body::Batch(pb::ObservationBatch {
            mode: pb::BatchMode::Delta as i32,
            source_sequence: seq,
            observations: (1..=n)
                .map(|c| pb::Observation {
                    channel_id: c,
                    value: Some(pb::observation::Value::I64Value(i64::from(c) * 1_000_003)),
                })
                .collect(),
        })),
    }
    .encode_to_vec()
}

fn bench_decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("decode");
    for n in SIZES {
        let bytes = wire_batch(n, 123_456);
        g.bench_with_input(BenchmarkId::new("fresh", n), &bytes, |b, bytes| {
            b.iter(|| black_box(pb::DriverMessage::decode(black_box(bytes.as_slice())).unwrap()))
        });
        let mut scratch = pb::ObservationBatch::default();
        // Limit = channels per source (128), as in the kernel (review P1).
        g.bench_with_input(BenchmarkId::new("reuse", n), &bytes, |b, bytes| {
            b.iter(|| {
                let d = scada_runtime::convert::decode_driver_message(black_box(bytes.as_slice()), &mut scratch, CH);
                black_box(matches!(d, Ok(scada_runtime::convert::Decoded::Batch(_))))
            })
        });
        // Same decoder with no limit: the cost of the P1 check on the fast path.
        g.bench_with_input(BenchmarkId::new("reuse_nolimit", n), &bytes, |b, bytes| {
            b.iter(|| {
                let d =
                    scada_runtime::convert::decode_driver_message(black_box(bytes.as_slice()), &mut scratch, u32::MAX);
                black_box(matches!(d, Ok(scada_runtime::convert::Decoded::Batch(_))))
            })
        });
    }
    g.finish();
}

fn bench_convert(c: &mut Criterion) {
    let mut g = c.benchmark_group("convert");
    for n in SIZES {
        let msg = pb::DriverMessage::decode(wire_batch(n, 1).as_slice()).unwrap();
        let Some(pb::driver_message::Body::Batch(b)) = msg.body else { unreachable!() };
        g.bench_with_input(BenchmarkId::new("batch_from_wire", n), &b, |bn, b| {
            bn.iter_batched(
                || b.clone(),
                |b| black_box(scada_runtime::convert::batch_from_wire(SourceId(1), 1, &b, CH)),
                BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

fn config() -> Arc<ValidatedConfig> {
    let tags =
        (1..=CH).map(|c| TagSpec { tag_id: TagId(c), name: format!("t{c}"), value_type: ValueType::I64 }).collect();
    let channels = (1..=CH)
        .map(|c| ChannelSpec { channel_id: ChannelId(c), tag_id: TagId(c), value_type: ValueType::I64 })
        .collect();
    Arc::new(
        ValidatedConfig::validate(ConfigSpec {
            limits: CoreLimits { max_observations_per_batch: CH },
            tags,
            sources: vec![SourceSpec { source_id: SourceId(1), name: "s".into(), channels }],
        })
        .unwrap(),
    )
}

#[derive(Clone, Copy)]
enum Order {
    Sorted,
    Reversed,
    Shuffled,
}

/// Channels of the i-th Delta: a sliding window of n distinct channels, in `order`.
fn channels(i: u64, n: u32, order: Order) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n).map(|j| 1 + ((i as u32).wrapping_mul(7).wrapping_add(j) % CH)).collect();
    v.sort_unstable();
    match order {
        Order::Sorted => {}
        Order::Reversed => v.reverse(),
        Order::Shuffled => {
            let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            for k in (1..v.len()).rev() {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                v.swap(k, (s % (k as u64 + 1)) as usize);
            }
        }
    }
    v
}

fn input(i: u64, n: u32, order: Order) -> SequencedInput {
    SequencedInput {
        operation_sequence: 2 + i,
        operation: Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(1),
            link_generation: 1,
            mode: BatchMode::Delta,
            source_sequence: 1 + i,
            observations: channels(i, n, order)
                .into_iter()
                .map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(i as i64) })
                .collect(),
        }),
    }
}

fn prime(apply: &mut dyn FnMut(SequencedInput)) {
    apply(SequencedInput {
        operation_sequence: 1,
        operation: Operation::SourceConnected { source_id: SourceId(1), link_generation: 1, source_epoch: 1 },
    });
    apply(SequencedInput {
        operation_sequence: 2,
        operation: Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(1),
            link_generation: 1,
            mode: BatchMode::FullState,
            source_sequence: 1,
            observations: (1..=CH).map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(-1) }).collect(),
        }),
    });
}

macro_rules! apply_bench {
    ($g:expr, $ty:ty, $order:expr, $oname:expr, $n:expr) => {{
        let mut k = <$ty>::new(config());
        prime(&mut |x| {
            k.apply(x).unwrap();
        });
        let mut next = 1u64;
        $g.bench_function(BenchmarkId::new($oname, $n), |b| {
            b.iter_batched(
                || {
                    next += 1;
                    input(next - 1, $n, $order)
                },
                |x| {
                    black_box(k.apply(x).unwrap());
                },
                BatchSize::SmallInput,
            )
        });
    }};
}

fn bench_apply(c: &mut Criterion) {
    let mut g = c.benchmark_group("apply");
    for n in SIZES {
        for (order, oname) in [(Order::Sorted, "sorted"), (Order::Reversed, "reversed"), (Order::Shuffled, "shuffled")]
        {
            if n == 1 && !matches!(order, Order::Sorted) {
                continue;
            }
            apply_bench!(g, Kernel, order, oname, n);
        }
    }
    g.finish();
}

criterion_group!(benches, bench_decode, bench_convert, bench_apply);
criterion_main!(benches);

//! Wire batch -> conversion -> core validation/apply on a kernel with an established baseline.
//! Properties: no panic; any non-applied outcome leaves state and digest unchanged
//! (all-or-nothing); applied outcomes advance the operation sequence by exactly one.
#![no_main]

use std::sync::{Arc, OnceLock};

use libfuzzer_sys::fuzz_target;
use scada_core::*;
use scada_protocol::Message;
use scada_protocol::driver_v1::ObservationBatch as WireBatch;

fn base() -> &'static Kernel {
    static K: OnceLock<Kernel> = OnceLock::new();
    K.get_or_init(|| {
        let ch = |c, t, v| ChannelSpec { channel_id: ChannelId(c), tag_id: TagId(t), value_type: v };
        let spec = ConfigSpec {
            limits: CoreLimits::default(),
            tags: vec![
                TagSpec { tag_id: TagId(101), name: "door_contact".into(), value_type: ValueType::Bool },
                TagSpec { tag_id: TagId(102), name: "tamper_switch".into(), value_type: ValueType::Bool },
                TagSpec { tag_id: TagId(103), name: "case_temperature_centi_c".into(), value_type: ValueType::I64 },
            ],
            sources: vec![SourceSpec {
                source_id: SourceId(1001),
                name: "sim".into(),
                channels: vec![ch(1, 101, ValueType::Bool), ch(2, 102, ValueType::Bool), ch(3, 103, ValueType::I64)],
                supervision: Supervision::NONE,
            }],
        };
        let mut k = Kernel::new(Arc::new(ValidatedConfig::validate(spec).unwrap()));
        let src = SourceId(1001);
        k.apply(SequencedInput {
            operation_sequence: 1,
            operation: Operation::SourceConnected { source_id: src, link_generation: 1, source_epoch: 9 },
        })
        .unwrap();
        let obs = vec![
            Observation { channel_id: ChannelId(1), value: Value::Bool(false) },
            Observation { channel_id: ChannelId(2), value: Value::Bool(false) },
            Observation { channel_id: ChannelId(3), value: Value::I64(2150) },
        ];
        k.apply(SequencedInput {
            operation_sequence: 2,
            operation: Operation::ObservationBatch(ObservationBatch {
                source_id: src,
                link_generation: 1,
                mode: BatchMode::FullState,
                source_sequence: 5,
                observations: obs,
            }),
        })
        .unwrap();
        k
    })
}

fuzz_target!(|data: &[u8]| {
    let Ok(wire) = WireBatch::decode(data) else { return };
    // VS-006: the source-time sidecar never reaches the core (ADR 0014 §6).
    let Ok((batch, _times)) = scada_runtime::convert::batch_from_wire(SourceId(1001), 1, &wire, 128) else { return };
    let mut k = base().clone();
    let before = (k.logical_snapshot(), k.state_digest());
    let r = k.apply(SequencedInput { operation_sequence: 3, operation: Operation::ObservationBatch(batch) });
    match r {
        Ok(TransitionOutcome::Applied { operation_sequence, .. }) => {
            assert_eq!(operation_sequence, 3);
            assert_eq!(k.last_operation_sequence(), 3);
        }
        _ => assert_eq!(before, (k.logical_snapshot(), k.state_digest())),
    }
});

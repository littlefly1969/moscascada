//! Writes the initial corpus: valid and malformed frames / batches.
use std::fs;

use scada_protocol::driver_v1::{
    BatchMode, DriverMessage, Hello, Observation, ObservationBatch, driver_message, observation,
};
use scada_protocol::{Message, frame};

fn obs(c: u32, v: observation::Value) -> Observation {
    Observation { channel_id: c, value: Some(v) }
}

fn main() {
    use observation::Value::{BoolValue as B, I64Value as I};
    let batches = vec![
        (
            "full_valid",
            ObservationBatch {
                mode: BatchMode::FullState as i32,
                source_sequence: 5,
                observations: vec![obs(1, B(true)), obs(2, B(false)), obs(3, I(2150))], ..Default::default() },
        ),
        (
            "delta_next",
            ObservationBatch { mode: BatchMode::Delta as i32, source_sequence: 6, observations: vec![obs(3, I(-40))], ..Default::default()  },
        ),
        (
            "delta_dup",
            ObservationBatch {
                mode: BatchMode::FullState as i32,
                source_sequence: 5,
                observations: vec![obs(3, I(2150)), obs(2, B(false)), obs(1, B(false))], ..Default::default() },
        ),
        (
            "delta_gap",
            ObservationBatch { mode: BatchMode::Delta as i32, source_sequence: 9, observations: vec![obs(1, B(true))], ..Default::default()  },
        ),
        (
            "unknown_channel",
            ObservationBatch {
                mode: BatchMode::Delta as i32,
                source_sequence: 6,
                observations: vec![obs(1, B(true)), obs(99, B(true))], ..Default::default() },
        ),
        (
            "dup_channel",
            ObservationBatch {
                mode: BatchMode::Delta as i32,
                source_sequence: 6,
                observations: vec![obs(1, B(true)), obs(1, B(false))], ..Default::default() },
        ),
        (
            "type_mismatch",
            ObservationBatch { mode: BatchMode::Delta as i32, source_sequence: 6, observations: vec![obs(3, B(true))], ..Default::default()  },
        ),
        (
            "missing_value",
            ObservationBatch {
                mode: BatchMode::Delta as i32,
                source_sequence: 6,
                observations: vec![Observation { channel_id: 1, value: None }], ..Default::default() },
        ),
        ("bad_mode", ObservationBatch { mode: 7, source_sequence: 6, observations: vec![obs(1, B(true))], ..Default::default()  }),
        (
            "incomplete_full",
            ObservationBatch {
                mode: BatchMode::FullState as i32,
                source_sequence: 6,
                observations: vec![obs(1, B(true))], ..Default::default() },
        ),
    ];
    fs::create_dir_all("seeds/batch_validation").unwrap();
    fs::create_dir_all("seeds/frame_decode").unwrap();
    for (name, b) in &batches {
        fs::write(format!("seeds/batch_validation/{name}"), b.encode_to_vec()).unwrap();
    }
    // Frames: first byte selects the limit (see the target).
    let hello = DriverMessage {
        body: Some(driver_message::Body::Hello(Hello {
            driver_id: "sim".into(),
            source_id: 1001,
            source_epoch: 9,
            supported_protocol_majors: vec![1],
            protocol_minor: 0, ..Default::default() })),
    };
    let mut stream = vec![255u8];
    let mut f = Vec::new();
    frame::encode_frame(&hello, 65_536, &mut f).unwrap();
    stream.extend_from_slice(&f);
    for (_, b) in &batches {
        let m = DriverMessage { body: Some(driver_message::Body::Batch(b.clone())) };
        frame::encode_frame(&m, 65_536, &mut f).unwrap();
        stream.extend_from_slice(&f);
    }
    fs::write("seeds/frame_decode/valid_session", &stream).unwrap();
    fs::write("seeds/frame_decode/zero_len", [255, 0, 0, 0, 0]).unwrap();
    fs::write("seeds/frame_decode/huge_len", [255, 0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    fs::write("seeds/frame_decode/truncated", &stream[..stream.len() - 3]).unwrap();
    fs::write("seeds/frame_decode/corrupt", [255, 0, 0, 0, 4, 0x12, 0x7F, 1, 2]).unwrap();
    println!("corpus written");
}

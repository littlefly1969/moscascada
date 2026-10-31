//! Diagnostic capture of the sequenced input stream (brief §6 D5).
//!
//! With `--capture-inputs PATH` the sequencer writes every `SequencedInput` it hands to
//! `Kernel::apply`, in apply order, one line each, before calling `apply`. Replaying the
//! file through a fresh kernel with the same configuration must reproduce the live state
//! and digest exactly: this is how tests compare kernels across ingestion settings
//! without assuming that two independent runs admit inputs in the same global order.
//!
//! Not a journal: no durability, no fsync, bounded to [`MAX_RECORDS`] lines, test and
//! diagnostics only. Format (text, stable within VS-002):
//!
//! ```text
//! C <op> <source> <generation> <epoch>
//! D <op> <source> <generation>
//! B <op> <source> <generation> <F|D> <source_sequence> <obs>   obs = '-' | ch:b:0|1,ch:i:N,...
//! ```

use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;

use scada_core::{BatchMode, ChannelId, Observation, ObservationBatch, Operation, SequencedInput, SourceId, Value};

/// Records after which capturing stops (one warning on stderr).
pub const MAX_RECORDS: u64 = 5_000_000;

/// Line-oriented writer owned by the sequencer thread.
#[derive(Debug)]
pub struct CaptureWriter {
    out: BufWriter<File>,
    line: String,
    records: u64,
    failed: bool,
}

impl CaptureWriter {
    pub fn create(path: &Path) -> io::Result<CaptureWriter> {
        let f = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(CaptureWriter { out: BufWriter::new(f), line: String::with_capacity(256), records: 0, failed: false })
    }

    /// Record one input. Errors stop the capture (reported once), never the sequencer.
    pub fn record(&mut self, input: &SequencedInput) {
        if self.failed {
            return;
        }
        if self.records >= MAX_RECORDS {
            eprintln!("capture: {MAX_RECORDS} records reached, capture stopped");
            self.failed = true;
            return;
        }
        self.line.clear();
        format_input(&mut self.line, input);
        self.line.push('\n');
        if let Err(e) = self.out.write_all(self.line.as_bytes()) {
            eprintln!("capture: write failed, capture stopped: {e}");
            self.failed = true;
        }
        self.records = self.records.saturating_add(1);
    }

    pub fn flush(&mut self) {
        if !self.failed && self.out.flush().is_err() {
            self.failed = true;
        }
    }
}

impl Drop for CaptureWriter {
    fn drop(&mut self) {
        self.flush();
    }
}

fn format_input(s: &mut String, input: &SequencedInput) {
    let op = input.operation_sequence;
    match &input.operation {
        Operation::SourceConnected { source_id, link_generation, source_epoch } => {
            let _ = write!(s, "C {op} {} {link_generation} {source_epoch}", source_id.0);
        }
        Operation::SourceDisconnected { source_id, link_generation } => {
            let _ = write!(s, "D {op} {} {link_generation}", source_id.0);
        }
        Operation::ObservationBatch(b) => {
            let mode = match b.mode {
                BatchMode::FullState => 'F',
                BatchMode::Delta => 'D',
            };
            let _ = write!(s, "B {op} {} {} {mode} {} ", b.source_id.0, b.link_generation, b.source_sequence);
            if b.observations.is_empty() {
                s.push('-');
            }
            for (i, o) in b.observations.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                let _ = match o.value {
                    Value::Bool(v) => write!(s, "{}:b:{}", o.channel_id.0, u8::from(v)),
                    Value::I64(v) => write!(s, "{}:i:{v}", o.channel_id.0),
                };
            }
        }
    }
}

/// Parse one capture line.
pub fn parse_line(line: &str) -> Option<SequencedInput> {
    let mut f = line.split_ascii_whitespace();
    let kind = f.next()?;
    let op: u64 = f.next()?.parse().ok()?;
    let source_id = SourceId(f.next()?.parse().ok()?);
    let link_generation: u64 = f.next()?.parse().ok()?;
    let operation = match kind {
        "C" => Operation::SourceConnected { source_id, link_generation, source_epoch: f.next()?.parse().ok()? },
        "D" => Operation::SourceDisconnected { source_id, link_generation },
        "B" => {
            let mode = match f.next()? {
                "F" => BatchMode::FullState,
                "D" => BatchMode::Delta,
                _ => return None,
            };
            let source_sequence: u64 = f.next()?.parse().ok()?;
            let obs = f.next()?;
            let mut observations = Vec::new();
            if obs != "-" {
                for o in obs.split(',') {
                    let mut p = o.split(':');
                    let channel_id = ChannelId(p.next()?.parse().ok()?);
                    let value = match (p.next()?, p.next()?) {
                        ("b", "0") => Value::Bool(false),
                        ("b", "1") => Value::Bool(true),
                        ("i", v) => Value::I64(v.parse().ok()?),
                        _ => return None,
                    };
                    observations.push(Observation { channel_id, value });
                }
            }
            Operation::ObservationBatch(ObservationBatch {
                source_id,
                link_generation,
                mode,
                source_sequence,
                observations,
            })
        }
        _ => return None,
    };
    Some(SequencedInput { operation_sequence: op, operation })
}

/// Read a whole capture file. Fails on the first malformed line.
pub fn read(path: &Path) -> io::Result<Vec<SequencedInput>> {
    let r = io::BufReader::new(File::open(path)?);
    let mut v = Vec::new();
    for (n, line) in r.lines().enumerate() {
        let line = line?;
        let input = parse_line(&line)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("line {}: {line:?}", n + 1)))?;
        v.push(input);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let inputs = [
            SequencedInput {
                operation_sequence: 1,
                operation: Operation::SourceConnected {
                    source_id: SourceId(7),
                    link_generation: 3,
                    source_epoch: u64::MAX,
                },
            },
            SequencedInput {
                operation_sequence: 2,
                operation: Operation::ObservationBatch(ObservationBatch {
                    source_id: SourceId(7),
                    link_generation: 3,
                    mode: BatchMode::FullState,
                    source_sequence: 1,
                    observations: vec![
                        Observation { channel_id: ChannelId(1), value: Value::Bool(true) },
                        Observation { channel_id: ChannelId(2), value: Value::I64(i64::MIN) },
                    ],
                }),
            },
            SequencedInput {
                operation_sequence: 3,
                operation: Operation::ObservationBatch(ObservationBatch {
                    source_id: SourceId(7),
                    link_generation: 3,
                    mode: BatchMode::Delta,
                    source_sequence: 2,
                    observations: vec![],
                }),
            },
            SequencedInput {
                operation_sequence: 3,
                operation: Operation::SourceDisconnected { source_id: SourceId(7), link_generation: 3 },
            },
        ];
        for i in &inputs {
            let mut s = String::new();
            format_input(&mut s, i);
            assert_eq!(parse_line(&s).as_ref(), Some(i), "{s}");
        }
        assert!(parse_line("X 1 2 3").is_none());
        assert!(parse_line("B 1 2 3 F 1 1:x:1").is_none());
    }
}

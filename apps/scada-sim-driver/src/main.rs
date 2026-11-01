//! `scada-sim-driver`: simulated driver process.
//!
//! ```text
//! scada-sim-driver --socket PATH [--source-id 1001] [--epoch N] <scenario>
//!   demo   FullState, Deltas, duplicate, gap, invalid batch, disconnect, reconnect
//!   hold   FullState then stay connected until killed (driver crash tests)
//!   stream [--count N] [--pipeline P]  continuous Deltas on channel 3
//!   heartbeat [--count N]  FullState, then N heartbeats only (VS-004)
//!   soak [--duration-ms D] [--cycle-ms C] [--pause-ms P]  VS-004 soak cycle:
//!          (re)connect + FullState, partial Deltas on channel 3 and heartbeats for C ms,
//!          then P ms of complete silence, one heartbeat, one Delta, disconnect; repeat
//!
//! Common options:
//!   --legacy-hello      announce protocol 1.0 (byte-identical to a VS-001..VS-003 driver)
//!   --heartbeat-ms N    heartbeat period for hold / stream / heartbeat / soak (VS-004);
//!                       heartbeats are sent only if the kernel negotiated minor >= 1
//! ```
//! Every kernel answer is printed as one line on stdout.

use std::path::PathBuf;
use std::process::ExitCode;

use scada_protocol::driver_v1::BatchMode;
use std::time::{Duration, Instant};

use scada_sim_driver::{ClientError, Session, Val, ack_line, batch, hello, hello_v10, welcome_line};

const DRIVER_ID: &str = "scada-sim-driver/0.1";

struct Args {
    socket: PathBuf,
    source_id: u32,
    epoch: u64,
    scenario: String,
    count: u64,
    pipeline: u64,
    legacy_hello: bool,
    heartbeat_ms: u64,
    duration_ms: u64,
    cycle_ms: u64,
    pause_ms: u64,
}

fn random_epoch() -> u64 {
    use std::io::Read;
    let mut b = [0u8; 8];
    let ok = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b)).is_ok();
    let e = u64::from_le_bytes(b);
    if ok && e != 0 { e } else { u64::from(std::process::id()).max(1) }
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        socket: PathBuf::new(),
        source_id: 1001,
        epoch: 0,
        scenario: String::new(),
        count: 1000,
        pipeline: 1,
        legacy_hello: false,
        heartbeat_ms: 0,
        duration_ms: 60_000,
        cycle_ms: 20_000,
        pause_ms: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(x) = it.next() {
        let mut v = |n: &str| it.next().ok_or_else(|| format!("{n} needs a value"));
        match x.as_str() {
            "--socket" => a.socket = v("--socket")?.into(),
            "--source-id" => a.source_id = v("--source-id")?.parse().map_err(|e| format!("{e}"))?,
            "--epoch" => a.epoch = v("--epoch")?.parse().map_err(|e| format!("{e}"))?,
            "--count" => a.count = v("--count")?.parse().map_err(|e| format!("{e}"))?,
            "--pipeline" => a.pipeline = v("--pipeline")?.parse().map_err(|e| format!("{e}"))?,
            "--legacy-hello" => a.legacy_hello = true,
            "--heartbeat-ms" => a.heartbeat_ms = v("--heartbeat-ms")?.parse().map_err(|e| format!("{e}"))?,
            "--duration-ms" => a.duration_ms = v("--duration-ms")?.parse().map_err(|e| format!("{e}"))?,
            "--cycle-ms" => a.cycle_ms = v("--cycle-ms")?.parse().map_err(|e| format!("{e}"))?,
            "--pause-ms" => a.pause_ms = v("--pause-ms")?.parse().map_err(|e| format!("{e}"))?,
            s if !s.starts_with('-') && a.scenario.is_empty() => a.scenario = s.to_owned(),
            s => return Err(format!("unknown argument {s}")),
        }
    }
    if a.socket.as_os_str().is_empty() || a.scenario.is_empty() {
        return Err("--socket and a scenario are required".into());
    }
    if a.epoch == 0 {
        a.epoch = random_epoch();
    }
    Ok(a)
}

fn main() -> ExitCode {
    let a = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!(
                "error: {e}\nusage: scada-sim-driver --socket PATH [--source-id N] [--epoch N] [--legacy-hello] [--heartbeat-ms N] demo|hold|stream|heartbeat|soak [--count N] [--pipeline P] [--duration-ms D] [--cycle-ms C] [--pause-ms P]"
            );
            return ExitCode::from(2);
        }
    };
    let r = match a.scenario.as_str() {
        "demo" => demo(&a),
        "hold" => hold(&a),
        "stream" => stream(&a),
        "heartbeat" => heartbeats(&a),
        "soak" => soak(&a),
        s => {
            eprintln!("unknown scenario {s}");
            return ExitCode::from(2);
        }
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("ERROR {e}");
            ExitCode::from(1)
        }
    }
}

fn open(a: &Args) -> Result<Session, ClientError> {
    let hello = if a.legacy_hello {
        hello_v10(DRIVER_ID, a.source_id, a.epoch)
    } else {
        hello(DRIVER_ID, a.source_id, a.epoch)
    };
    let s = Session::connect_with(&a.socket, &hello)?;
    println!("{}", welcome_line(&s.welcome));
    Ok(s)
}

/// VS-004: send a heartbeat if one is due (and negotiated). Returns whether one was sent.
fn beat(s: &mut Session, a: &Args, last: &mut Instant) -> Result<bool, ClientError> {
    if a.heartbeat_ms == 0 || last.elapsed() < Duration::from_millis(a.heartbeat_ms) {
        return Ok(false);
    }
    *last = Instant::now();
    s.heartbeat()
}

fn step(s: &mut Session, label: &str, msg: &scada_protocol::driver_v1::DriverMessage) -> Result<(), ClientError> {
    let ack = s.request(msg)?;
    println!("{label}: {}", ack_line(&ack));
    Ok(())
}

/// Brief §6 scenario.
fn demo(a: &Args) -> Result<(), ClientError> {
    println!("EPOCH {}", a.epoch);
    let mut s = open(a)?;
    step(
        &mut s,
        "full_state",
        &batch(BatchMode::FullState, 1, &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(2150))]),
    )?;
    step(&mut s, "delta", &batch(BatchMode::Delta, 2, &[(3, Val::I(2175))]))?;
    step(&mut s, "delta", &batch(BatchMode::Delta, 3, &[(1, Val::B(true))]))?;
    step(&mut s, "duplicate", &batch(BatchMode::Delta, 3, &[(1, Val::B(true))]))?;
    step(&mut s, "gap", &batch(BatchMode::Delta, 5, &[(3, Val::I(2200))]))?;
    step(&mut s, "invalid", &batch(BatchMode::Delta, 4, &[(2, Val::B(true)), (99, Val::B(true))]))?;
    step(&mut s, "delta", &batch(BatchMode::Delta, 4, &[(2, Val::B(true))]))?;
    drop(s);
    println!("DISCONNECTED");
    // Reconnect without restart: same epoch. Retransmit the last batch (as if its ACK had
    // been lost), then continue from the sequence the kernel reports.
    let mut s = open(a)?;
    step(&mut s, "retransmit", &batch(BatchMode::Delta, 4, &[(2, Val::B(true))]))?;
    let next = s.welcome.last_accepted_source_sequence.unwrap_or(0) + 1;
    step(&mut s, "delta", &batch(BatchMode::Delta, next, &[(3, Val::I(2200))]))?;
    println!("DONE");
    Ok(())
}

/// Connect, send a FullState, then block until killed.
fn hold(a: &Args) -> Result<(), ClientError> {
    let mut s = open(a)?;
    step(
        &mut s,
        "full_state",
        &batch(BatchMode::FullState, 1, &[(1, Val::B(true)), (2, Val::B(false)), (3, Val::I(1999))]),
    )?;
    println!("HOLDING heartbeats_negotiated={}", s.heartbeats_negotiated());
    if a.heartbeat_ms == 0 {
        loop {
            std::thread::park();
        }
    }
    loop {
        std::thread::sleep(Duration::from_millis(a.heartbeat_ms));
        s.heartbeat()?;
    }
}

/// VS-004: FullState, then `count` heartbeats every `heartbeat_ms` and no data at all.
fn heartbeats(a: &Args) -> Result<(), ClientError> {
    let mut s = open(a)?;
    let first = s.welcome.last_accepted_source_sequence.map_or(1, |l| l + 1);
    step(
        &mut s,
        "full_state",
        &batch(BatchMode::FullState, first, &[(1, Val::B(false)), (2, Val::B(true)), (3, Val::I(42))]),
    )?;
    let mut sent = 0u64;
    for _ in 0..a.count {
        std::thread::sleep(Duration::from_millis(a.heartbeat_ms.max(1)));
        if s.heartbeat()? {
            sent += 1;
        }
    }
    println!("HEARTBEATS sent={sent} negotiated=1.{}", s.welcome.negotiated_protocol_minor);
    Ok(())
}

/// VS-004 soak cycle (see the module docs). Reconnects keep the epoch (no restart) and
/// resume with a FullState at the next sequence.
fn soak(a: &Args) -> Result<(), ClientError> {
    let start = Instant::now();
    let deadline = Duration::from_millis(a.duration_ms);
    let (mut cycles, mut deltas, mut beats, mut next) = (0u64, 0u64, 0u64, None::<u64>);
    while start.elapsed() < deadline {
        let mut s = open(a)?;
        let mut seq = next.unwrap_or(1).max(s.welcome.last_accepted_source_sequence.map_or(1, |l| l + 1));
        let v = cycles as i64;
        s.request(&batch(BatchMode::FullState, seq, &[(1, Val::B(v % 2 == 0)), (2, Val::B(false)), (3, Val::I(v))]))?;
        seq += 1;
        let cycle_end = Instant::now() + Duration::from_millis(a.cycle_ms);
        let mut last_beat = Instant::now();
        let mut last_delta = Instant::now();
        while Instant::now() < cycle_end && start.elapsed() < deadline {
            std::thread::sleep(Duration::from_millis(20));
            if beat(&mut s, a, &mut last_beat)? {
                beats += 1;
            }
            if last_delta.elapsed() >= Duration::from_millis(500) {
                last_delta = Instant::now();
                // Partial Delta: channel 3 only; channels 1 and 2 go stale.
                s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))]))?;
                seq += 1;
                deltas += 1;
            }
        }
        if a.pause_ms > 0 {
            std::thread::sleep(Duration::from_millis(a.pause_ms)); // silence on an open link
            if s.heartbeat()? {
                beats += 1;
            }
            std::thread::sleep(Duration::from_millis(50));
            s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(-1))]))?;
            seq += 1;
            deltas += 1;
        }
        next = Some(seq);
        drop(s); // disconnect; the next cycle reconnects with the same epoch
        cycles += 1;
    }
    println!("SOAK cycles={cycles} deltas={deltas} heartbeats={beats}");
    Ok(())
}

/// FullState then `count` Deltas on channel 3, with up to `pipeline` batches in flight.
fn stream(a: &Args) -> Result<(), ClientError> {
    let mut s = open(a)?;
    let first = s.welcome.last_accepted_source_sequence.map_or(1, |l| l + 1);
    step(
        &mut s,
        "full_state",
        &batch(BatchMode::FullState, first, &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(0))]),
    )?;
    let pipeline = a.pipeline.max(1);
    let (mut sent, mut acked, mut applied) = (0u64, 0u64, 0u64);
    while acked < a.count {
        while sent < a.count && sent - acked < pipeline {
            sent += 1;
            s.send(&batch(BatchMode::Delta, first + sent, &[(3, Val::I(sent as i64))]))?;
        }
        let ack = s.recv_ack()?;
        acked += 1;
        if ack.status == scada_protocol::driver_v1::AckStatus::AppliedVolatile as i32 {
            applied += 1;
        }
    }
    println!("STREAM sent={sent} acked={acked} applied={applied}");
    Ok(())
}

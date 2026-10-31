//! `scada-sim-driver`: simulated driver process.
//!
//! ```text
//! scada-sim-driver --socket PATH [--source-id 1001] [--epoch N] <scenario>
//!   demo   FullState, Deltas, duplicate, gap, invalid batch, disconnect, reconnect
//!   hold   FullState then stay connected until killed (driver crash tests)
//!   stream [--count N] [--pipeline P]  continuous Deltas on channel 3
//! ```
//! Every kernel answer is printed as one line on stdout.

use std::path::PathBuf;
use std::process::ExitCode;

use scada_protocol::driver_v1::BatchMode;
use scada_sim_driver::{ClientError, Session, Val, ack_line, batch, welcome_line};

const DRIVER_ID: &str = "scada-sim-driver/0.1";

struct Args {
    socket: PathBuf,
    source_id: u32,
    epoch: u64,
    scenario: String,
    count: u64,
    pipeline: u64,
}

fn random_epoch() -> u64 {
    use std::io::Read;
    let mut b = [0u8; 8];
    let ok = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b)).is_ok();
    let e = u64::from_le_bytes(b);
    if ok && e != 0 { e } else { u64::from(std::process::id()).max(1) }
}

fn parse() -> Result<Args, String> {
    let mut a =
        Args { socket: PathBuf::new(), source_id: 1001, epoch: 0, scenario: String::new(), count: 1000, pipeline: 1 };
    let mut it = std::env::args().skip(1);
    while let Some(x) = it.next() {
        let mut v = |n: &str| it.next().ok_or_else(|| format!("{n} needs a value"));
        match x.as_str() {
            "--socket" => a.socket = v("--socket")?.into(),
            "--source-id" => a.source_id = v("--source-id")?.parse().map_err(|e| format!("{e}"))?,
            "--epoch" => a.epoch = v("--epoch")?.parse().map_err(|e| format!("{e}"))?,
            "--count" => a.count = v("--count")?.parse().map_err(|e| format!("{e}"))?,
            "--pipeline" => a.pipeline = v("--pipeline")?.parse().map_err(|e| format!("{e}"))?,
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
                "error: {e}\nusage: scada-sim-driver --socket PATH [--source-id N] [--epoch N] demo|hold|stream [--count N] [--pipeline P]"
            );
            return ExitCode::from(2);
        }
    };
    let r = match a.scenario.as_str() {
        "demo" => demo(&a),
        "hold" => hold(&a),
        "stream" => stream(&a),
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
    let s = Session::connect(&a.socket, DRIVER_ID, a.source_id, a.epoch)?;
    println!("{}", welcome_line(&s.welcome));
    Ok(s)
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
    println!("HOLDING");
    loop {
        std::thread::park();
    }
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

//! `scada-modbus-sim`: standalone simulator for soak runs and benchmarks (loopback by
//! default). The image is the one of the generic test profile
//! (`docs/driver-catalog/generic-io.v1.toml`); values change by a seeded random walk.
//!
//! Usage: scada-modbus-sim [--listen 127.0.0.1:1502] [--seed N] [--mutate-every N]
//!                         [--fault-per-million N] [--registers N] [--max-connections N]
//!
//! `--registers N` fills input registers 100..100+N (benchmark block); with
//! `--mutate-every K` all of them are incremented every K requests.
//! Prints `LISTENING <addr>` on stdout, then `STATS ...` every second.
use std::time::Duration;

use scada_modbus::Area;
use scada_modbus_sim::{Fault, SimConfig, Simulator, Trigger};

fn main() {
    let mut cfg = SimConfig::default();
    let mut mutate_every: u64 = 0;
    let mut fault_ppm: u32 = 0;
    let mut registers: u16 = 0;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = |n: &str| args.next().unwrap_or_else(|| die(&format!("{n} needs a value")));
        match a.as_str() {
            "--listen" => cfg.bind = val("--listen").parse().unwrap_or_else(|e| die(&format!("--listen: {e}"))),
            "--seed" => cfg.seed = val("--seed").parse().unwrap_or_else(|e| die(&format!("--seed: {e}"))),
            "--mutate-every" => {
                mutate_every = val("--mutate-every").parse().unwrap_or_else(|e| die(&format!("--mutate-every: {e}")))
            }
            "--fault-per-million" => {
                fault_ppm = val("--fault-per-million").parse().unwrap_or_else(|e| die(&format!("{e}")))
            }
            "--registers" => registers = val("--registers").parse().unwrap_or_else(|e| die(&format!("{e}"))),
            "--max-connections" => {
                cfg.max_connections = val("--max-connections").parse().unwrap_or_else(|e| die(&format!("{e}")))
            }
            "-h" | "--help" => {
                println!(
                    "scada-modbus-sim [--listen ADDR] [--seed N] [--mutate-every N] [--fault-per-million N] [--registers N] [--max-connections N]"
                );
                return;
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    let sim = Simulator::start(cfg).unwrap_or_else(|e| die(&format!("bind: {e}")));
    sim.with_memory(|m| {
        // Generic profile defaults (see the catalog): plausible temperatures and counters.
        m.input[0] = 2150;
        m.input[1] = 0;
        m.holding[0] = 1;
        for i in 0..registers {
            m.input[usize::from(100 + i)] = i;
        }
    });
    if mutate_every > 0 {
        sim.set_mutation(Some(Box::new(move |m, n, rng| {
            if n % mutate_every == 0 {
                // Benchmark block (--registers): every register changes, so a Delta carries
                // all the channels mapped on it.
                for i in 0..registers {
                    let r = &mut m.input[usize::from(100 + i)];
                    *r = r.wrapping_add(1);
                }
                let i = rng.below(4);
                match i {
                    0 => m.discrete_inputs[0] = !m.discrete_inputs[0],
                    1 => m.coils[0] = !m.coils[0],
                    2 => m.input[0] = 2000 + rng.below(600) as u16,
                    _ => {
                        let c = (u32::from(m.input[2]) << 16 | u32::from(m.input[3])).wrapping_add(1);
                        m.input[2] = (c >> 16) as u16;
                        m.input[3] = c as u16;
                    }
                }
            }
        })));
    }
    if fault_ppm > 0 {
        sim.inject(Fault::Close, Trigger::Random { per_million: fault_ppm }, None);
    }
    let _ = Area::Coil;
    println!("LISTENING {}", sim.addr());
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let c = sim.counters();
        println!(
            "STATS connections={} refused={} requests={} responses={} exceptions={} faults={}",
            c.connections_accepted, c.connections_refused, c.requests, c.responses, c.exceptions, c.faults_injected
        );
    }
}

fn die(msg: &str) -> ! {
    eprintln!("scada-modbus-sim: {msg}");
    std::process::exit(2)
}

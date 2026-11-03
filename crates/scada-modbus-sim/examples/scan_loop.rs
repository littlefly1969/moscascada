//! VS-007 raw Modbus benchmark: back-to-back FC03/FC04 reads (one request at a time per
//! connection, as the driver does) against a running `scada-modbus-sim`, no kernel.
//!
//!   cargo run --release -p scada-modbus-sim --example scan_loop -- ADDR AREA QTY SECS
//!
//! Prints one `kind=modbus_raw` line: reads/s, registers/s, request latency p50/p99/p99.9.
use std::time::{Duration, Instant};

use scada_modbus::Area;
use scada_modbus::client::{Client, connect_tcp};
use scada_modbus::pdu::ReadRequest;

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let [addr, area, qty, secs] = a.as_slice() else {
        eprintln!("usage: scan_loop ADDR(host:port) holding_register|input_register QTY SECS");
        std::process::exit(2);
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let area = Area::from_name(area).expect("area");
    let qty: u16 = qty.parse().expect("qty");
    let secs: f64 = secs.parse().expect("secs");
    let s = connect_tcp(host, port.parse().expect("port"), Duration::from_secs(2)).expect("connect");
    let mut c = Client::new(s, 1, Duration::from_secs(1));
    let req = ReadRequest::new(area, 100, qty).expect("request");
    let mut lat: Vec<u32> = Vec::with_capacity(1 << 20);
    let start = Instant::now();
    let end = start + Duration::from_secs_f64(secs);
    let mut errors = 0u64;
    while Instant::now() < end {
        let t = Instant::now();
        match c.read(&req) {
            Ok(_) => lat.push(t.elapsed().as_nanos().min(u128::from(u32::MAX)) as u32),
            Err(_) => errors += 1,
        }
    }
    let el = start.elapsed().as_secs_f64();
    lat.sort_unstable();
    let q = |p: f64| lat.get(((lat.len() as f64 * p).ceil() as usize).saturating_sub(1)).copied().unwrap_or(0) / 1000;
    let n = lat.len() as f64;
    println!(
        "kind=modbus_raw area={} qty={qty} secs={el:.2} reads={} errors={errors} reads_per_s={:.0} items_per_s={:.0} p50_us={} p99_us={} p999_us={}",
        area.name(),
        lat.len(),
        n / el,
        n * f64::from(qty) / el,
        q(0.5),
        q(0.99),
        q(0.999)
    );
}

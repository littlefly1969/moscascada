//! VS-007 T02/T03: the real client against the simulator over loopback TCP.

use std::time::{Duration, Instant};

use scada_modbus::client::{Client, ModbusError, connect_tcp};
use scada_modbus::pdu::{ExceptionCode, ReadData, ReadRequest, ResponseError};
use scada_modbus::plan::{self, Block, PointSpan};
use scada_modbus::scan;
use scada_modbus::{Area, mbap};
use scada_modbus_sim::{Fault, SimConfig, Simulator, Trigger};

const T: Duration = Duration::from_millis(300);

fn sim() -> Simulator {
    let s = Simulator::start(SimConfig { units: vec![7], ..SimConfig::default() }).unwrap();
    s.with_memory(|m| {
        m.coils[19] = true;
        m.coils[21] = true;
        m.discrete_inputs[0] = true;
        m.discrete_inputs[1999] = true;
        for i in 0..200u16 {
            m.holding[usize::from(i)] = 1000 + i;
            m.input[usize::from(i)] = 5000 + i;
        }
    });
    s
}

fn client(s: &Simulator) -> Client<std::net::TcpStream> {
    Client::new(connect_tcp("127.0.0.1", s.port(), T).unwrap(), 7, T)
}

fn req(a: Area, addr: u16, q: u16) -> ReadRequest {
    ReadRequest::new(a, addr, q).unwrap()
}

#[test]
fn t02_all_four_functions_exact_zero_based() {
    let s = sim();
    let mut c = client(&s);
    let ReadData::Bits(b) = c.read(&req(Area::Coil, 19, 3)).unwrap() else { panic!() };
    assert_eq!(b, vec![true, false, true]);
    let ReadData::Bits(b) = c.read(&req(Area::DiscreteInput, 0, 2000)).unwrap() else { panic!() };
    assert_eq!(b.len(), 2000);
    assert!(b[0] && b[1999] && !b[1]);
    // PDU address 0 is the first register ("40001" in some documentation): no offset.
    assert_eq!(c.read(&req(Area::HoldingRegister, 0, 2)).unwrap(), ReadData::Registers(vec![1000, 1001]));
    let ReadData::Registers(r) = c.read(&req(Area::InputRegister, 75, 125)).unwrap() else { panic!() };
    assert_eq!(r.len(), 125);
    assert_eq!((r[0], r[124]), (5075, 5199));
    assert_eq!(
        s.request_log(),
        vec![
            (Area::Coil, 19, 3),
            (Area::DiscreteInput, 0, 2000),
            (Area::HoldingRegister, 0, 2),
            (Area::InputRegister, 75, 125)
        ]
    );
    assert_eq!(c.counters.responses_ok, 4);
}

#[test]
fn t02_exceptions_keep_the_connection_byte_count_poisons_it() {
    let s = sim();
    let mut c = client(&s);
    s.inject(Fault::Exception(ExceptionCode::IllegalDataAddress), Trigger::Next, None);
    match c.read(&req(Area::HoldingRegister, 0, 1)) {
        Err(ModbusError::Response(ResponseError::Exception(ExceptionCode::IllegalDataAddress))) => {}
        other => panic!("{other:?}"),
    }
    assert!(!c.is_poisoned());
    assert_eq!(c.read(&req(Area::HoldingRegister, 0, 1)).unwrap(), ReadData::Registers(vec![1000]));
    s.inject(Fault::BadByteCount, Trigger::Next, None);
    match c.read(&req(Area::HoldingRegister, 0, 1)) {
        Err(ModbusError::Response(ResponseError::ByteCount { expected: 2, got: 3 })) => {}
        other => panic!("{other:?}"),
    }
    assert!(c.is_poisoned());
    assert!(matches!(c.read(&req(Area::HoldingRegister, 0, 1)), Err(ModbusError::Poisoned)));
    // A unit the gateway does not reach answers with an exception.
    let mut other = Client::new(connect_tcp("127.0.0.1", s.port(), T).unwrap(), 9, T);
    assert!(matches!(
        other.read(&req(Area::InputRegister, 0, 1)),
        Err(ModbusError::Response(ResponseError::Exception(ExceptionCode::GatewayTargetFailedToRespond)))
    ));
    s.inject(Fault::WrongFunction, Trigger::Next, None);
    let mut c = client(&s);
    assert!(matches!(
        c.read(&req(Area::HoldingRegister, 0, 1)),
        Err(ModbusError::Response(ResponseError::FunctionMismatch { expected: 3, got: 4 }))
    ));
}

#[test]
fn t03_fragmentation_is_transparent() {
    let s = sim();
    let mut c = client(&s);
    s.inject(Fault::Fragment(1), Trigger::Times(3), None);
    for _ in 0..3 {
        assert_eq!(c.read(&req(Area::HoldingRegister, 10, 3)).unwrap(), ReadData::Registers(vec![1010, 1011, 1012]));
    }
}

#[test]
fn t03_wrong_transaction_unit_protocol_never_accepted() {
    for (fault, kind) in [
        (Fault::WrongTransactionId, "transaction_mismatch"),
        (Fault::WrongUnitId, "unit_mismatch"),
        (Fault::WrongProtocolId, "bad_header"),
    ] {
        let s = sim();
        let mut c = client(&s);
        s.inject(fault.clone(), Trigger::Next, None);
        let e = c.read(&req(Area::InputRegister, 0, 1)).unwrap_err();
        assert_eq!(e.kind(), kind, "{fault:?}: {e}");
        assert!(c.is_poisoned());
    }
}

#[test]
fn t03_coalesced_duplicate_is_never_taken_as_the_next_answer() {
    let s = sim();
    let mut c = client(&s);
    s.inject(Fault::Duplicate, Trigger::Next, None);
    // First answer is correct; its stale copy stays in the socket.
    assert_eq!(c.read(&req(Area::InputRegister, 0, 1)).unwrap(), ReadData::Registers(vec![5000]));
    // The next request reads the stale copy first: its transaction id is the old one.
    let e = c.read(&req(Area::InputRegister, 1, 1)).unwrap_err();
    assert!(matches!(e, ModbusError::TransactionMismatch { expected: 2, got: 1 }), "{e}");
}

#[test]
fn t03_timeout_partial_frame_and_eof_are_distinct() {
    let s = sim();
    let mut c = client(&s);
    s.inject(Fault::NoResponse, Trigger::Next, None);
    let t0 = Instant::now();
    let e = c.read(&req(Area::InputRegister, 0, 1)).unwrap_err();
    assert_eq!(e.kind(), "timeout");
    assert!(t0.elapsed() >= T && t0.elapsed() < T * 4);

    let mut c = client(&s);
    s.inject(Fault::Truncate { keep: 9 }, Trigger::Next, None);
    let e = c.read(&req(Area::InputRegister, 0, 2)).unwrap_err();
    assert!(matches!(e, ModbusError::Timeout { partial_bytes: 9 }), "{e}");
    // Poisoned: the 9 bytes are never combined with a later frame.
    assert!(matches!(c.read(&req(Area::InputRegister, 0, 2)), Err(ModbusError::Poisoned)));

    let mut c = client(&s);
    s.inject(Fault::TruncateClose { keep: 4 }, Trigger::Next, None);
    let e = c.read(&req(Area::InputRegister, 0, 2)).unwrap_err();
    assert!(matches!(e, ModbusError::Closed { partial_bytes: 4 }), "{e}");

    let mut c = client(&s);
    s.inject(Fault::Close, Trigger::Next, None);
    let e = c.read(&req(Area::InputRegister, 0, 2)).unwrap_err();
    assert!(matches!(e, ModbusError::Closed { partial_bytes: 0 }), "{e}");

    // A fresh connection after all this reads correct data.
    let mut c = client(&s);
    assert_eq!(c.read(&req(Area::InputRegister, 0, 2)).unwrap(), ReadData::Registers(vec![5000, 5001]));
}

#[test]
fn t03_scan_fails_as_a_whole_and_reports_its_spread() {
    let s = sim();
    let blocks = [
        Block { area: Area::HoldingRegister, start: 0, count: 100, max_per_request: 10 },
        Block { area: Area::Coil, start: 0, count: 64, max_per_request: 2000 },
    ];
    let points: Vec<PointSpan> = (0..50)
        .map(|a| PointSpan { area: Area::HoldingRegister, address: a, width: 1 })
        .chain([PointSpan { area: Area::Coil, address: 21, width: 1 }])
        .collect();
    let p = plan::build(&blocks, &points, 64).unwrap();
    assert_eq!(p.requests.len(), 6);
    let mut c = client(&s);
    let img = scan::execute(&mut c, &p, Duration::ZERO).unwrap();
    assert!(img.acquired_at_utc_ns.is_some());
    assert!(img.spread() > Duration::ZERO);
    // A failure in the middle: no partial image.
    s.inject(Fault::Close, Trigger::Times(1), Some(Area::HoldingRegister));
    s.clear_log();
    let mut c = client(&s);
    let e = scan::execute(&mut c, &p, Duration::ZERO).unwrap_err();
    assert_eq!((e.request_index, e.completed), (1, 1)); // coil request first, then registers
    let _ = mbap::MAX_ADU_BYTES;
}

#[test]
fn simulator_refuses_and_drops_connections() {
    let s = sim();
    let mut c = client(&s);
    assert!(c.read(&req(Area::InputRegister, 0, 1)).is_ok());
    s.drop_connections();
    std::thread::sleep(Duration::from_millis(80));
    assert!(c.read(&req(Area::InputRegister, 0, 1)).is_err());
    s.set_accepting(false);
    let mut c = client(&s);
    assert!(c.read(&req(Area::InputRegister, 0, 1)).is_err());
    assert!(s.counters().connections_refused >= 1);
    s.set_accepting(true);
    let mut c = client(&s);
    assert!(c.read(&req(Area::InputRegister, 0, 1)).is_ok());
}

#[test]
fn seeded_random_faults_are_reproducible() {
    let run = |seed| {
        let s = Simulator::start(SimConfig { seed, ..SimConfig::default() }).unwrap();
        s.inject(Fault::Exception(ExceptionCode::ServerDeviceBusy), Trigger::Random { per_million: 300_000 }, None);
        let mut c = Client::new(connect_tcp("127.0.0.1", s.port(), T).unwrap(), 1, T);
        (0..50).map(|_| c.read(&req(Area::InputRegister, 0, 1)).is_ok()).collect::<Vec<_>>()
    };
    let a = run(42);
    assert_eq!(a, run(42));
    assert_ne!(a, run(43));
    assert!(a.iter().any(|x| !x) && a.iter().any(|x| *x));
}

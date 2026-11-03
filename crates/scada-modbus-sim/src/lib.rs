//! VS-007: Modbus TCP **server simulator** for tests, benchmarks and soak runs.
//!
//! Loopback only by default. Serves FC01..FC04 from an in-memory image (65536 items per
//! area) and lets a test inject, reproducibly, the faults the driver must survive:
//! exception responses, truncated frames, bad MBAP headers, wrong transaction or unit ids,
//! inconsistent byte counts, delays, silent requests, connection drops in the middle of a
//! scan, stale duplicate responses, fragmented writes, refused connections and value
//! mutations between two requests of the same scan. Random faults and mutations use a
//! seeded generator (splitmix64): the same seed gives the same sequence.
//!
//! Not a production server: no write functions, no security, one thread per connection.
#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use scada_modbus::Area;
use scada_modbus::mbap::{self, AduDecoder};
use scada_modbus::pdu::{self, ExceptionCode, PduDecodeError, ReadData, ReadRequest};
use scada_modbus::plan::Block;

/// Deterministic generator (splitmix64).
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (n > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

/// Device memory: 65536 items per area.
#[derive(Debug, Clone)]
pub struct Memory {
    pub coils: Vec<bool>,
    pub discrete_inputs: Vec<bool>,
    pub holding: Vec<u16>,
    pub input: Vec<u16>,
}

impl Default for Memory {
    fn default() -> Self {
        Memory {
            coils: vec![false; 65_536],
            discrete_inputs: vec![false; 65_536],
            holding: vec![0; 65_536],
            input: vec![0; 65_536],
        }
    }
}

impl Memory {
    fn read(&self, r: &ReadRequest) -> ReadData {
        let a = usize::from(r.address());
        let q = usize::from(r.quantity());
        match r.area() {
            Area::Coil => ReadData::Bits(self.coils[a..a + q].to_vec()),
            Area::DiscreteInput => ReadData::Bits(self.discrete_inputs[a..a + q].to_vec()),
            Area::HoldingRegister => ReadData::Registers(self.holding[a..a + q].to_vec()),
            Area::InputRegister => ReadData::Registers(self.input[a..a + q].to_vec()),
        }
    }

    pub fn set_bit(&mut self, area: Area, addr: u16, v: bool) {
        match area {
            Area::Coil => self.coils[usize::from(addr)] = v,
            Area::DiscreteInput => self.discrete_inputs[usize::from(addr)] = v,
            _ => panic!("not a bit area"),
        }
    }

    pub fn set_reg(&mut self, area: Area, addr: u16, v: u16) {
        match area {
            Area::HoldingRegister => self.holding[usize::from(addr)] = v,
            Area::InputRegister => self.input[usize::from(addr)] = v,
            _ => panic!("not a register area"),
        }
    }

    /// Two registers, high word first.
    pub fn set_u32(&mut self, area: Area, addr: u16, v: u32) {
        self.set_reg(area, addr, (v >> 16) as u16);
        self.set_reg(area, addr + 1, v as u16);
    }
}

/// One injected misbehaviour, applied to the response of one request.
#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    /// Exception response with this code.
    Exception(ExceptionCode),
    /// Send only the first `keep` bytes of the response, then keep the connection open and
    /// silent (the client must time out and must not reuse the bytes).
    Truncate { keep: usize },
    /// Send the first `keep` bytes, then close.
    TruncateClose { keep: usize },
    /// Response with transaction id + 1 (stale or foreign).
    WrongTransactionId,
    /// Response from another unit id.
    WrongUnitId,
    /// MBAP protocol id 1.
    WrongProtocolId,
    /// Byte count one larger than correct (and one extra byte of data).
    BadByteCount,
    /// Response with the function code of another read.
    WrongFunction,
    /// Answer after a delay.
    Delay(Duration),
    /// No answer at all (connection stays open).
    NoResponse,
    /// Close the connection without answering (device reboot in the middle of a scan).
    Close,
    /// Write the response in chunks of `n` bytes with a short pause (TCP segmentation).
    Fragment(usize),
    /// Send the correct response twice back to back (coalesced duplicate).
    Duplicate,
}

/// When a fault applies.
#[derive(Debug, Clone, PartialEq)]
pub enum Trigger {
    /// The next request (matching the filter), once.
    Next,
    /// The next `n` matching requests.
    Times(u32),
    /// Every matching request until cleared.
    Always,
    /// Each matching request with probability `per_million / 1e6` (seeded generator).
    Random { per_million: u32 },
}

#[derive(Debug, Clone)]
struct Rule {
    fault: Fault,
    trigger: Trigger,
    area: Option<Area>,
}

/// Hook called after each request is answered (mutates the image between two requests of
/// the same scan, like a real device whose inputs change while it is polled).
pub type MutationHook = Box<dyn FnMut(&mut Memory, u64, &mut Rng) + Send>;

/// Counters of the simulator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SimCounters {
    pub connections_accepted: u64,
    pub connections_refused: u64,
    pub requests: u64,
    pub responses: u64,
    pub exceptions: u64,
    pub faults_injected: u64,
    pub bad_requests: u64,
    pub reads_outside_readable: u64,
}

struct State {
    memory: Memory,
    rules: Vec<Rule>,
    rng: Rng,
    counters: SimCounters,
    accepting: bool,
    /// Incremented by `drop_connections`: every connection of an older generation closes.
    generation: u64,
    units: Vec<u8>,
    readable: Option<Vec<Block>>,
    mutation: Option<MutationHook>,
    /// Requests log (area, address, quantity), bounded.
    log: VecDeque<(Area, u16, u16)>,
}

/// Simulator configuration.
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub bind: SocketAddr,
    pub seed: u64,
    /// Unit ids answered; requests for other ids get GATEWAY TARGET FAILED (0x0B).
    /// Empty = answer every unit id.
    pub units: Vec<u8>,
    /// Readable ranges; reads outside get ILLEGAL DATA ADDRESS. `None` = everything.
    pub readable: Option<Vec<Block>>,
    pub max_connections: usize,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            seed: 1,
            units: Vec::new(),
            readable: None,
            max_connections: 16,
        }
    }
}

/// A running simulator. Stops (and closes every connection) on drop.
pub struct Simulator {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Simulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Simulator").field("addr", &self.addr).finish()
    }
}

const LOG_MAX: usize = 4096;

fn lock(s: &Mutex<State>) -> MutexGuard<'_, State> {
    s.lock().unwrap_or_else(|e| e.into_inner())
}

impl Simulator {
    pub fn start(cfg: SimConfig) -> io::Result<Simulator> {
        let listener = TcpListener::bind(cfg.bind)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let state = Arc::new(Mutex::new(State {
            memory: Memory::default(),
            rules: Vec::new(),
            rng: Rng::new(cfg.seed),
            counters: SimCounters::default(),
            accepting: true,
            generation: 0,
            units: cfg.units.clone(),
            readable: cfg.readable.clone(),
            mutation: None,
            log: VecDeque::new(),
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (st, sp) = (state.clone(), stop.clone());
        let max = cfg.max_connections;
        let accept = std::thread::Builder::new().name("modbus-sim-accept".into()).spawn(move || {
            let live = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            while !sp.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((s, _)) => {
                        let ok = lock(&st).accepting && live.load(Ordering::Relaxed) < max;
                        if !ok {
                            lock(&st).counters.connections_refused += 1;
                            drop(s);
                            continue;
                        }
                        lock(&st).counters.connections_accepted += 1;
                        let (st2, sp2, live2) = (st.clone(), sp.clone(), live.clone());
                        live2.fetch_add(1, Ordering::Relaxed);
                        let _ = std::thread::Builder::new().name("modbus-sim-conn".into()).spawn(move || {
                            let _ = serve(s, &st2, &sp2);
                            live2.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(2)),
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        })?;
        Ok(Simulator { addr, state, stop, accept: Some(accept) })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Change the image under the lock.
    pub fn with_memory<R>(&self, f: impl FnOnce(&mut Memory) -> R) -> R {
        f(&mut lock(&self.state).memory)
    }

    /// Inject a fault for any area (`None`) or one area.
    pub fn inject(&self, fault: Fault, trigger: Trigger, area: Option<Area>) {
        lock(&self.state).rules.push(Rule { fault, trigger, area });
    }

    pub fn clear_faults(&self) {
        lock(&self.state).rules.clear();
    }

    pub fn set_mutation(&self, hook: Option<MutationHook>) {
        lock(&self.state).mutation = hook;
    }

    /// Accept (true) or refuse (false: accept and close at once) new connections.
    pub fn set_accepting(&self, on: bool) {
        lock(&self.state).accepting = on;
    }

    /// Close every open connection (device reboot / cable cut).
    pub fn drop_connections(&self) {
        lock(&self.state).generation += 1;
    }

    pub fn counters(&self) -> SimCounters {
        lock(&self.state).counters
    }

    /// Requests served so far (area, address, quantity), last `LOG_MAX`.
    pub fn request_log(&self) -> Vec<(Area, u16, u16)> {
        lock(&self.state).log.iter().copied().collect()
    }

    pub fn clear_log(&self) {
        lock(&self.state).log.clear();
    }
}

impl Drop for Simulator {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
    }
}

/// Pick the fault (if any) for this request; consumes `Next`/`Times`.
fn pick_fault(st: &mut State, area: Option<Area>) -> Option<Fault> {
    let mut chosen = None;
    let mut remove = None;
    for (i, r) in st.rules.iter_mut().enumerate() {
        if r.area.is_some() && r.area != area {
            continue;
        }
        let fire = match &mut r.trigger {
            Trigger::Next => {
                remove = Some(i);
                true
            }
            Trigger::Times(n) => {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    remove = Some(i);
                }
                true
            }
            Trigger::Always => true,
            Trigger::Random { per_million } => st.rng.below(1_000_000) < u64::from(*per_million),
        };
        if fire {
            chosen = Some(r.fault.clone());
            break;
        }
    }
    if let Some(i) = remove {
        st.rules.remove(i);
    }
    if chosen.is_some() {
        st.counters.faults_injected += 1;
    }
    chosen
}

fn serve(mut s: TcpStream, st: &Mutex<State>, stop: &AtomicBool) -> io::Result<()> {
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_millis(20)))?;
    let generation = lock(st).generation;
    let mut dec = AduDecoder::new();
    let mut pdu_out = Vec::new();
    let mut adu_out = Vec::new();
    loop {
        if stop.load(Ordering::Relaxed) || lock(st).generation != generation {
            return Ok(());
        }
        let n = match s.read(dec.spare()) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => continue,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        dec.advance(n);
        let adu = match dec.next_frame() {
            Ok(Some(a)) => a,
            Ok(None) => continue,
            Err(_) => {
                lock(st).counters.bad_requests += 1;
                return Ok(()); // desynchronised: close
            }
        };
        let tid = adu.header.transaction_id;
        let unit = adu.header.unit_id;
        let (req, fault) = {
            let mut g = lock(st);
            g.counters.requests += 1;
            let req = ReadRequest::decode(&adu.pdu);
            let area = req.as_ref().ok().map(|r| r.area());
            let fault = pick_fault(&mut g, area);
            (req, fault)
        };
        let fc = adu.pdu.first().copied().unwrap_or(0);
        // Normal response PDU (or a protocol exception).
        {
            let mut g = lock(st);
            let st_ref = &mut *g;
            pdu_out.clear();
            if !st_ref.units.is_empty() && !st_ref.units.contains(&unit) {
                pdu_out.extend_from_slice(&pdu::encode_exception(fc, ExceptionCode::GatewayTargetFailedToRespond));
                st_ref.counters.exceptions += 1;
            } else {
                match &req {
                    Ok(r) => {
                        let inside = st_ref.readable.as_ref().is_none_or(|bs| {
                            bs.iter().any(|b| {
                                b.area == r.area()
                                    && r.address() >= b.start
                                    && u32::from(r.address()) + u32::from(r.quantity()) <= b.end()
                            })
                        });
                        if inside {
                            let data = st_ref.memory.read(r);
                            pdu::encode_response(r, &data, &mut pdu_out);
                        } else {
                            st_ref.counters.reads_outside_readable += 1;
                            st_ref.counters.exceptions += 1;
                            pdu_out.extend_from_slice(&pdu::encode_exception(fc, ExceptionCode::IllegalDataAddress));
                        }
                        if st_ref.log.len() >= LOG_MAX {
                            st_ref.log.pop_front();
                        }
                        st_ref.log.push_back((r.area(), r.address(), r.quantity()));
                    }
                    Err(PduDecodeError::Function(_)) | Err(PduDecodeError::Length(_)) => {
                        st_ref.counters.bad_requests += 1;
                        st_ref.counters.exceptions += 1;
                        pdu_out.extend_from_slice(&pdu::encode_exception(fc, ExceptionCode::IllegalFunction));
                    }
                    Err(PduDecodeError::Request(_)) => {
                        st_ref.counters.bad_requests += 1;
                        st_ref.counters.exceptions += 1;
                        pdu_out.extend_from_slice(&pdu::encode_exception(fc, ExceptionCode::IllegalDataValue));
                    }
                }
            }
        }
        let mut out_tid = tid;
        let mut out_unit = unit;
        let mut proto = 0u16;
        match &fault {
            Some(Fault::Exception(c)) => {
                pdu_out.clear();
                pdu_out.extend_from_slice(&pdu::encode_exception(fc, *c));
                lock(st).counters.exceptions += 1;
            }
            Some(Fault::WrongTransactionId) => out_tid = tid.wrapping_add(1),
            Some(Fault::WrongUnitId) => out_unit = unit.wrapping_add(1),
            Some(Fault::WrongProtocolId) => proto = 1,
            Some(Fault::BadByteCount) => {
                if let Some(c) = pdu_out.get_mut(1) {
                    *c = c.wrapping_add(1);
                }
                pdu_out.push(0);
            }
            Some(Fault::WrongFunction) => {
                if let Some(f) = pdu_out.first_mut() {
                    *f = if *f == 0x03 { 0x04 } else { 0x03 };
                }
            }
            Some(Fault::Delay(d)) => std::thread::sleep(*d),
            Some(Fault::NoResponse) => {
                after_request(st);
                continue;
            }
            Some(Fault::Close) => return Ok(()),
            _ => {}
        }
        mbap::encode_adu(out_tid, out_unit, &pdu_out, &mut adu_out);
        if proto != 0 {
            adu_out[2..4].copy_from_slice(&proto.to_be_bytes());
        }
        match &fault {
            Some(Fault::Truncate { keep }) => {
                let k = (*keep).min(adu_out.len().saturating_sub(1));
                s.write_all(&adu_out[..k])?;
                after_request(st);
                // Stay silent: the client must time out. Drain requests without answering.
                loop {
                    if stop.load(Ordering::Relaxed) || lock(st).generation != generation {
                        return Ok(());
                    }
                    let mut sink = [0u8; 512];
                    match s.read(&mut sink) {
                        Ok(0) => return Ok(()),
                        Ok(_) => {}
                        Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
                        Err(_) => return Ok(()),
                    }
                }
            }
            Some(Fault::TruncateClose { keep }) => {
                let k = (*keep).min(adu_out.len().saturating_sub(1));
                s.write_all(&adu_out[..k])?;
                after_request(st);
                return Ok(());
            }
            Some(Fault::Fragment(n)) => {
                for chunk in adu_out.chunks((*n).max(1)) {
                    s.write_all(chunk)?;
                    s.flush()?;
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Some(Fault::Duplicate) => {
                let mut two = adu_out.clone();
                two.extend_from_slice(&adu_out);
                s.write_all(&two)?;
            }
            _ => s.write_all(&adu_out)?,
        }
        lock(st).counters.responses += 1;
        after_request(st);
    }
}

fn after_request(st: &Mutex<State>) {
    let mut g = lock(st);
    let n = g.counters.requests;
    let State { memory, mutation, rng, .. } = &mut *g;
    if let Some(h) = mutation.as_mut() {
        h(memory, n, rng);
    }
}

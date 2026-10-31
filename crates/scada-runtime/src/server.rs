//! Unix socket ingress: bounded framing, handshake, per-connection pipelining with a
//! bounded number of in-flight batches, and conversion into sequencer requests.

use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use scada_core::SourceId;
use scada_protocol::driver_v1::{
    self as pb, AckStatus, DriverMessage, ErrorCode, KernelMessage, driver_message, kernel_message,
};
use scada_protocol::frame::{self, FrameError, HEADER_LEN};
use scada_protocol::introspect_v1 as ipb;
use scada_protocol::{KERNEL_INSTANCE_ID_LEN, MAX_DRIVER_ID_BYTES, Message, PROTOCOL_MAJOR, PROTOCOL_MINOR};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::timeout;

use crate::config::RuntimeConfig;
use crate::convert;
use crate::diag::{self, Diag, Stage};
use crate::sequencer::{BatchReply, ConnectReply, Counters, Request};

/// Shared, immutable state of the I/O side.
#[derive(Debug)]
pub struct Server {
    pub config: Arc<RuntimeConfig>,
    pub kernel_instance_id: [u8; KERNEL_INSTANCE_ID_LEN],
    pub counters: Arc<Counters>,
    pub requests: mpsc::Sender<Request>,
    /// Stage timings (`--diag`); `None` in production.
    pub diag: Option<Arc<Diag>>,
}

/// Generate the kernel instance id (16 random bytes from the OS).
pub fn new_instance_id() -> io::Result<[u8; KERNEL_INSTANCE_ID_LEN]> {
    use std::io::Read;
    let mut id = [0u8; KERNEL_INSTANCE_ID_LEN];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut id)?;
    Ok(id)
}

/// Effective uid of this process (owner of the files it creates), without libc.
fn effective_uid() -> io::Result<u32> {
    use std::os::unix::fs::MetadataExt;
    Ok(std::fs::metadata("/proc/self")?.uid())
}

/// E2: the directory that holds a socket must be a real directory (not a symlink), owned
/// by this process's user or by root, and not writable by group or others. Otherwise
/// another user could replace or pre-create the socket (or its directory) between our
/// checks and our bind, or delete it later.
pub fn check_socket_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::symlink_metadata(dir)?;
    let deny = |why: &str| {
        Err(io::Error::new(io::ErrorKind::PermissionDenied, format!("socket directory {}: {why}", dir.display())))
    };
    if m.file_type().is_symlink() {
        return deny("is a symbolic link");
    }
    if !m.is_dir() {
        return deny("is not a directory");
    }
    let me = effective_uid()?;
    if m.uid() != me && m.uid() != 0 {
        return deny(&format!("owned by uid {}, not by this user ({me}) or root", m.uid()));
    }
    if m.mode() & 0o022 != 0 {
        return deny(&format!("mode {:o} is writable by group or others; use a private directory", m.mode() & 0o7777));
    }
    Ok(())
}

/// Bind a Unix socket with restricted permissions. Refuses to replace a live socket, a
/// socket of another user or anything that is not a socket (E2).
pub fn bind_socket(path: &Path, mode: u32, group: Option<u32>) -> io::Result<(UnixListener, SocketGuard)> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    };
    if !dir.exists() {
        // Created with its final mode (no window with a umask-dependent mode).
        std::fs::DirBuilder::new().recursive(true).mode(0o750).create(&dir)?;
    }
    check_socket_dir(&dir)?;
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_socket() => {
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                return Err(io::Error::new(io::ErrorKind::AddrInUse, "a kernel is already listening on this socket"));
            }
            if m.uid() != effective_uid()? {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("stale socket {} belongs to uid {}; not removing it", path.display(), m.uid()),
                ));
            }
            std::fs::remove_file(path)?;
        }
        Ok(_) => return Err(io::Error::new(io::ErrorKind::AlreadyExists, "path exists and is not a socket")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let std_listener = std::os::unix::net::UnixListener::bind(path)?;
    let secure = || -> io::Result<()> {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
        if let Some(gid) = group {
            std::os::unix::fs::chown(path, None, Some(gid))?;
        }
        Ok(())
    };
    if let Err(e) = secure() {
        let _ = std::fs::remove_file(path);
        return Err(e);
    }
    // After chmod/chown: they change the ctime the guard records.
    let guard = SocketGuard::new(path.to_path_buf())?;
    std_listener.set_nonblocking(true)?;
    Ok((UnixListener::from_std(std_listener)?, guard))
}

/// Removes the socket file when dropped — only if it is still the very file this process
/// bound (a socket with the same device, inode and change time), never a file another
/// process put there later. The change time guards against inode reuse after the
/// original was deleted.
#[derive(Debug)]
pub struct SocketGuard {
    path: PathBuf,
    id: (u64, u64, i64, i64),
}

fn file_id(m: &std::fs::Metadata) -> (u64, u64, i64, i64) {
    use std::os::unix::fs::MetadataExt;
    (m.dev(), m.ino(), m.ctime(), m.ctime_nsec())
}

impl SocketGuard {
    fn new(path: PathBuf) -> io::Result<SocketGuard> {
        let m = std::fs::symlink_metadata(&path)?;
        Ok(SocketGuard { path, id: file_id(&m) })
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(m) = std::fs::symlink_metadata(&self.path)
            && m.file_type().is_socket()
            && file_id(&m) == self.id
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

enum ReadError {
    Frame(FrameError),
    Timeout,
}

impl ReadError {
    fn code(&self) -> Option<ErrorCode> {
        match self {
            ReadError::Timeout => Some(ErrorCode::FrameTimeout),
            ReadError::Frame(FrameError::ZeroLength) => Some(ErrorCode::InvalidFrameLength),
            ReadError::Frame(FrameError::TooLarge { .. }) => Some(ErrorCode::FrameTooLarge),
            ReadError::Frame(FrameError::Decode(_)) => Some(ErrorCode::MalformedMessage),
            ReadError::Frame(_) => None, // truncated stream / I/O: peer is gone
        }
    }
}

/// Read one frame: waits indefinitely for the first header byte, then requires the rest of
/// the frame within `frame_timeout`. The length is validated before `buf` is resized.
async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    max: u32,
    buf: &mut Vec<u8>,
    frame_timeout: Duration,
    diag: Option<&Diag>,
) -> Result<bool, ReadError> {
    let mut header = [0u8; HEADER_LEN];
    let n = r.read(&mut header).await.map_err(|e| ReadError::Frame(e.into()))?;
    if n == 0 {
        return Ok(false);
    }
    let t0 = diag::now(diag);
    let rest = async {
        if let Some(tail) = header.get_mut(n..) {
            r.read_exact(tail).await.map_err(|e| ReadError::Frame(e.into()))?;
        }
        let len = frame::check_length(header, max).map_err(ReadError::Frame)?;
        buf.clear();
        buf.resize(len, 0);
        r.read_exact(buf).await.map_err(|e| ReadError::Frame(e.into()))?;
        if let (Some(d), Some(t0)) = (diag, t0) {
            d.since(Stage::FrameRead, t0);
        }
        Ok(true)
    };
    timeout(frame_timeout, rest).await.map_err(|_| ReadError::Timeout)?
}

async fn write_msg(w: &mut OwnedWriteHalf, msg: &KernelMessage, max: u32, scratch: &mut Vec<u8>) -> io::Result<()> {
    frame::encode_frame(msg, max, scratch).map_err(|e| io::Error::other(e.to_string()))?;
    w.write_all(scratch).await
}

fn rejected(code: ErrorCode, id: &[u8], detail: &str) -> KernelMessage {
    KernelMessage {
        body: Some(kernel_message::Body::HandshakeRejected(pb::HandshakeRejected {
            error_code: code as i32,
            kernel_instance_id: id.to_vec(),
            detail: detail.to_owned(),
        })),
    }
}

fn protocol_error(code: ErrorCode, detail: &str) -> KernelMessage {
    KernelMessage {
        body: Some(kernel_message::Body::ProtocolError(pb::ProtocolError {
            error_code: code as i32,
            detail: detail.to_owned(),
        })),
    }
}

/// Submit a request to the bounded sequencer queue, recording when the queue was full.
/// Waiting here is the backpressure: the session stops reading its socket meanwhile.
async fn submit(server: &Server, req: Request) -> Result<(), ()> {
    match server.requests.try_send(req) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(req)) => {
            Counters::inc(&server.counters.input_queue_full_waits);
            let t0 = diag::now(server.diag.as_deref());
            let r = server.requests.send(req).await.map_err(|_| ());
            if let (Some(d), Some(t0)) = (server.diag.as_deref(), t0) {
                d.since(Stage::SubmitWait, t0);
            }
            r
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
    }
}

/// Accept loop for driver connections. Runs until the listener fails fatally.
pub async fn serve(server: Arc<Server>, listener: UnixListener) {
    let permits = Arc::new(Semaphore::new(server.config.limits.max_driver_connections as usize));
    let mut refusal = Vec::new();
    loop {
        let stream = match listener.accept().await {
            Ok((s, _)) => s,
            Err(e) => {
                // e.g. EMFILE: back off instead of spinning; the listener stays usable.
                eprintln!("accept: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        Counters::inc(&server.counters.connections_accepted);
        match permits.clone().try_acquire_owned() {
            Ok(permit) => {
                let server = server.clone();
                tokio::spawn(async move {
                    server.counters.active_connections.fetch_add(1, Ordering::Relaxed);
                    session(&server, stream).await;
                    server.counters.active_connections.fetch_sub(1, Ordering::Relaxed);
                    drop(permit);
                });
            }
            Err(_) => {
                // Refuse without spawning: one small non-blocking write into an empty socket
                // buffer, then close. No per-refusal task, so a flood cannot grow memory.
                Counters::inc(&server.counters.connections_refused_limit);
                let msg =
                    rejected(ErrorCode::TooManyConnections, &server.kernel_instance_id, "connection limit reached");
                if frame::encode_frame(&msg, server.config.limits.max_frame_bytes, &mut refusal).is_ok() {
                    let _ = stream.try_write(&refusal);
                }
            }
        }
    }
}

enum Pending {
    /// `frame_done`: when the frame was fully read (diagnostics only).
    Wait {
        source_sequence: u64,
        reply: oneshot::Receiver<BatchReply>,
        frame_done: Option<Instant>,
    },
    Ready {
        source_sequence: u64,
        reply: BatchReply,
    },
    Fatal(KernelMessage),
}

async fn session(server: &Server, stream: UnixStream) {
    let limits = server.config.limits;
    let max = limits.max_frame_bytes;
    let peer_uid = stream.peer_cred().ok().map(|c| c.uid());
    let (mut rd, mut wr) = stream.into_split();
    let mut buf = Vec::new();
    let mut scratch = Vec::new();
    let id = server.kernel_instance_id;

    // ---- Handshake ----
    let hs = timeout(
        Duration::from_millis(limits.handshake_timeout_ms),
        read_frame(&mut rd, max, &mut buf, Duration::from_millis(limits.frame_timeout_ms), None),
    )
    .await;
    let hello = match hs {
        Err(_) | Ok(Err(ReadError::Timeout)) => {
            reject(server, &mut wr, ErrorCode::HandshakeTimeout, "no Hello within handshake timeout", &mut scratch)
                .await;
            return;
        }
        Ok(Ok(false)) => return,
        Ok(Err(e)) => {
            if let Some(code) = e.code() {
                reject(server, &mut wr, code, "invalid handshake frame", &mut scratch).await;
            }
            return;
        }
        Ok(Ok(true)) => match DriverMessage::decode(buf.as_slice()) {
            Ok(DriverMessage { body: Some(driver_message::Body::Hello(h)) }) => h,
            Ok(_) => {
                reject(server, &mut wr, ErrorCode::UnexpectedMessage, "first message must be Hello", &mut scratch)
                    .await;
                return;
            }
            Err(_) => {
                reject(server, &mut wr, ErrorCode::InvalidHello, "undecodable Hello", &mut scratch).await;
                return;
            }
        },
    };
    if !hello.supported_protocol_majors.contains(&PROTOCOL_MAJOR) {
        reject(server, &mut wr, ErrorCode::UnsupportedVersion, "kernel speaks protocol major 1 only", &mut scratch)
            .await;
        return;
    }
    if hello.driver_id.len() > MAX_DRIVER_ID_BYTES || hello.source_epoch == 0 {
        reject(server, &mut wr, ErrorCode::InvalidHello, "driver_id too long or source_epoch = 0", &mut scratch).await;
        return;
    }
    let source_id = SourceId(hello.source_id);
    match server.config.authorize(source_id, peer_uid) {
        None => {
            reject(server, &mut wr, ErrorCode::UnknownSource, "source not configured", &mut scratch).await;
            return;
        }
        Some(false) => {
            reject(server, &mut wr, ErrorCode::Unauthorized, "peer uid not authorized for source", &mut scratch).await;
            return;
        }
        Some(true) => {}
    }
    let (tx, rx) = oneshot::channel();
    let (superseded_tx, superseded_rx) = oneshot::channel();
    let connect =
        Request::Connect { source_id, source_epoch: hello.source_epoch, reply: tx, superseded: superseded_tx };
    if submit(server, connect).await.is_err() {
        reject(server, &mut wr, ErrorCode::KernelShuttingDown, "sequencer stopped", &mut scratch).await;
        return;
    }
    let (link_generation, full_state_required, last_accepted) = match rx.await {
        Ok(ConnectReply::Welcome { link_generation, full_state_required, last_accepted }) => {
            (link_generation, full_state_required, last_accepted)
        }
        Ok(ConnectReply::Rejected(code)) => {
            reject(server, &mut wr, code, "connect rejected by kernel", &mut scratch).await;
            return;
        }
        Err(_) => {
            reject(server, &mut wr, ErrorCode::KernelShuttingDown, "sequencer stopped", &mut scratch).await;
            return;
        }
    };
    let welcome = KernelMessage {
        body: Some(kernel_message::Body::Welcome(pb::Welcome {
            kernel_instance_id: id.to_vec(),
            negotiated_protocol_major: PROTOCOL_MAJOR,
            negotiated_protocol_minor: PROTOCOL_MINOR,
            link_generation,
            limits: Some(pb::SessionLimits {
                max_frame_bytes: max,
                max_observations_per_batch: server.config.core.limits().max_observations_per_batch,
                max_inflight_batches: limits.max_inflight_per_connection,
            }),
            full_state_required,
            last_accepted_source_sequence: last_accepted,
        })),
    };
    let write_timeout = Duration::from_millis(limits.ack_write_timeout_ms);
    if matches!(timeout(write_timeout, write_msg(&mut wr, &welcome, max, &mut scratch)).await, Ok(Ok(()))) {
        // ---- Data phase ----
        data_phase(server, rd, wr, source_id, link_generation, buf, superseded_rx).await;
    }
    // Every Connected link gets exactly one Disconnected, queued after all of its batches.
    let _ = submit(server, Request::Disconnect { source_id, link_generation }).await;
}

async fn reject(server: &Server, wr: &mut OwnedWriteHalf, code: ErrorCode, detail: &str, scratch: &mut Vec<u8>) {
    Counters::inc(&server.counters.handshakes_rejected);
    let msg = rejected(code, &server.kernel_instance_id, detail);
    let _ = timeout(
        Duration::from_millis(server.config.limits.frame_timeout_ms),
        write_msg(wr, &msg, server.config.limits.max_frame_bytes, scratch),
    )
    .await;
}

async fn data_phase(
    server: &Server,
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    source_id: SourceId,
    link_generation: u64,
    mut buf: Vec<u8>,
    mut superseded: oneshot::Receiver<()>,
) {
    let limits = server.config.limits;
    let max = limits.max_frame_bytes;
    let max_obs = server.config.core.limits().max_observations_per_batch;
    let (pending_tx, pending_rx) = mpsc::channel::<Pending>(limits.max_inflight_per_connection as usize);
    let (closed_tx, mut closed_rx) = oneshot::channel::<()>();
    let diag = server.diag.as_deref();
    let probe = diag.is_some_and(|d| d.is_probe(source_id.0));
    let ws = WriterSettings {
        instance_id: server.kernel_instance_id,
        max_frame: max,
        write_timeout: Duration::from_millis(limits.ack_write_timeout_ms),
        coalesce_max: limits.ack_coalesce_max,
    };
    let counters = &server.counters;
    let writer = writer_task(ws, wr, pending_rx, closed_tx, diag, probe, counters);
    // C5: optional read buffer, so that one `recv` can deliver several pipelined frames.
    // 0 = read straight from the socket (VS-001). Frame limits are unchanged: the length
    // is still validated before the payload buffer is resized.
    let mut rd = match limits.read_buffer_bytes {
        0 => Reader::Plain(rd),
        n => Reader::Buffered(tokio::io::BufReader::with_capacity(n as usize, rd)),
    };
    let reader = async move {
        // C3: decoded batch reused across frames (observation buffer keeps its capacity).
        let mut wire = pb::ObservationBatch::default();
        loop {
            let frame = tokio::select! {
                r = read_frame(&mut rd, max, &mut buf, Duration::from_millis(limits.frame_timeout_ms), diag) => r,
                _ = &mut closed_rx => return, // writer stopped (peer gone or link superseded)
                r = &mut superseded => {
                    // Ok: a newer link of this source was welcomed (D3). Err: the
                    // sequencer is gone. Either way this link is finished.
                    if r.is_ok() {
                        Counters::inc(&server.counters.links_superseded);
                        let msg = protocol_error(ErrorCode::LinkSuperseded, "a newer link took over this source");
                        let _ = pending_tx.send(Pending::Fatal(msg)).await;
                    }
                    return;
                }
            };
            let frame_done = diag::now(diag);
            let fatal = match frame {
                Ok(false) => return,
                Ok(true) => match convert::decode_driver_message(buf.as_slice(), &mut wire) {
                    Ok(decoded) => {
                        let b = match &decoded {
                            convert::Decoded::Batch(b) => *b,
                            convert::Decoded::Other(m) => match convert::as_batch(m) {
                                Some(b) => b,
                                None => {
                                    Counters::inc(&server.counters.protocol_errors);
                                    let msg = protocol_error(
                                        ErrorCode::UnexpectedMessage,
                                        "only ObservationBatch after Welcome",
                                    );
                                    let _ = pending_tx.send(Pending::Fatal(msg)).await;
                                    return;
                                }
                            },
                        };
                        let t_conv = diag::now(diag);
                        if let (Some(d), Some(t)) = (diag, frame_done) {
                            d.since(Stage::Decode, t);
                        }
                        let source_sequence = b.source_sequence;
                        let converted = convert::batch_from_wire(source_id, link_generation, b, max_obs);
                        if let (Some(d), Some(t)) = (diag, t_conv) {
                            d.since(Stage::Convert, t);
                        }
                        let pending = match converted {
                            Ok(batch) => {
                                let (tx, rx) = oneshot::channel();
                                // Reserve the ACK slot first (bounded in-flight per connection),
                                // then enqueue for the sequencer (bounded global queue).
                                let t_slot = diag::now(diag);
                                let Ok(slot) = pending_tx.reserve().await else { return };
                                if let (Some(d), Some(t)) = (diag, t_slot) {
                                    d.since(Stage::SlotWait, t);
                                }
                                slot.send(Pending::Wait { source_sequence, reply: rx, frame_done });
                                let submitted = diag::now(diag);
                                if submit(server, Request::Batch { batch, reply: tx, submitted }).await.is_err() {
                                    return;
                                }
                                continue;
                            }
                            Err(code) => {
                                Counters::inc(&server.counters.batches_rejected);
                                Pending::Ready {
                                    source_sequence,
                                    reply: BatchReply {
                                        status: AckStatus::Rejected,
                                        operation_sequence: None,
                                        error_code: code,
                                        continuity: None,
                                        last_accepted: None,
                                        sent: None,
                                    },
                                }
                            }
                        };
                        if pending_tx.send(pending).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Err(_) => protocol_error(ErrorCode::MalformedMessage, "undecodable message"),
                },
                Err(e) => match e.code() {
                    Some(code) => protocol_error(code, "invalid frame"),
                    None => return,
                },
            };
            Counters::inc(&server.counters.protocol_errors);
            let _ = pending_tx.send(Pending::Fatal(fatal)).await;
            return;
        }
    };
    // When the reader ends, `pending_tx` is dropped and the writer drains the remaining
    // ACKs and stops. When the writer ends first, `closed_rx` stops the reader.
    tokio::join!(reader, writer);
}

/// Read half of a data-phase connection, with or without the C5 read buffer.
enum Reader {
    Plain(OwnedReadHalf),
    Buffered(tokio::io::BufReader<OwnedReadHalf>),
}

impl AsyncRead for Reader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            Reader::Plain(r) => std::pin::Pin::new(r).poll_read(cx, buf),
            Reader::Buffered(r) => std::pin::Pin::new(r).poll_read(cx, buf),
        }
    }
}

/// Settings of one connection's ACK writer.
#[derive(Debug, Clone, Copy)]
struct WriterSettings {
    instance_id: [u8; KERNEL_INSTANCE_ID_LEN],
    max_frame: u32,
    /// D2: every socket write must complete within this time, otherwise the connection is
    /// closed. A peer that stops reading cannot hold the session (and its slots) forever.
    write_timeout: Duration,
    /// C5: at most this many ACKs that are already available are encoded into one write.
    /// 1 = one write per ACK (VS-001).
    coalesce_max: u32,
}

/// What the writer does with one pending entry once it is resolved.
enum Next {
    Ack {
        source_sequence: u64,
        reply: BatchReply,
        frame_done: Option<Instant>,
    },
    Fatal(KernelMessage),
    /// The sequencer is gone: close without inventing an ACK.
    Stop,
}

async fn resolve(p: Pending, diag: Option<&Diag>) -> Next {
    match p {
        Pending::Wait { source_sequence, reply, frame_done } => match reply.await {
            Ok(r) => {
                if let (Some(d), Some(t)) = (diag, r.sent) {
                    d.since(Stage::ReplyWait, t);
                }
                Next::Ack { source_sequence, reply: r, frame_done }
            }
            Err(_) => Next::Stop,
        },
        Pending::Ready { source_sequence, reply } => Next::Ack { source_sequence, reply, frame_done: None },
        Pending::Fatal(msg) => Next::Fatal(msg),
    }
}

/// Non-blocking variant: `Err(p)` gives the entry back when its reply is not there yet.
fn try_resolve(p: Pending, diag: Option<&Diag>) -> Result<Next, Pending> {
    match p {
        Pending::Wait { source_sequence, mut reply, frame_done } => match reply.try_recv() {
            Ok(r) => {
                if let (Some(d), Some(t)) = (diag, r.sent) {
                    d.since(Stage::ReplyWait, t);
                }
                Ok(Next::Ack { source_sequence, reply: r, frame_done })
            }
            Err(oneshot::error::TryRecvError::Empty) => Err(Pending::Wait { source_sequence, reply, frame_done }),
            Err(oneshot::error::TryRecvError::Closed) => Ok(Next::Stop),
        },
        other => Ok(ready_now(other)),
    }
}

fn ready_now(p: Pending) -> Next {
    match p {
        Pending::Ready { source_sequence, reply } => Next::Ack { source_sequence, reply, frame_done: None },
        Pending::Fatal(msg) => Next::Fatal(msg),
        Pending::Wait { .. } => Next::Stop, // unreachable: handled by try_resolve
    }
}

async fn timed_write(wr: &mut OwnedWriteHalf, bytes: &[u8], t: Duration) -> io::Result<()> {
    match timeout(t, wr.write_all(bytes)).await {
        Ok(r) => r,
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "peer does not read")),
    }
}

/// Writes the ACKs of one connection in batch order. ACKs are written only after the
/// sequencer replied, i.e. after `apply` returned; nothing is ever acknowledged ahead.
async fn writer_task(
    ws: WriterSettings,
    mut wr: OwnedWriteHalf,
    mut pending: mpsc::Receiver<Pending>,
    closed: oneshot::Sender<()>,
    diag: Option<&Diag>,
    probe: bool,
    counters: &Counters,
) {
    let mut out: Vec<u8> = Vec::new();
    let mut frame = Vec::new();
    let mut ack = KernelMessage {
        body: Some(kernel_message::Body::Ack(pb::InputAck {
            kernel_instance_id: ws.instance_id.to_vec(),
            ..Default::default()
        })),
    };
    // In-kernel timestamps of the ACKs in `out` (diagnostics only; bounded by coalesce_max).
    let mut stamps: Vec<Instant> = Vec::new();
    let mut head: Option<Pending> = None;
    'conn: loop {
        let first = match head.take() {
            Some(p) => p,
            None => match pending.recv().await {
                Some(p) => p,
                None => break,
            },
        };
        let mut next = resolve(first, diag).await;
        let mut n = 0u32;
        let mut stop = false;
        out.clear();
        stamps.clear();
        loop {
            match next {
                Next::Ack { source_sequence, reply, frame_done } => {
                    // C2: one ACK message per connection, every per-batch field overwritten.
                    if let Some(kernel_message::Body::Ack(a)) = &mut ack.body {
                        set_ack(a, source_sequence, &reply);
                    }
                    if frame::encode_frame(&ack, ws.max_frame, &mut frame).is_err() {
                        stop = true;
                        break;
                    }
                    out.extend_from_slice(&frame);
                    if let (Some(_), Some(t)) = (diag, frame_done) {
                        stamps.push(t);
                    }
                    n += 1;
                    if reply.error_code == ErrorCode::LinkSuperseded {
                        stop = true; // a newer link owns the source; this one is closed
                        break;
                    }
                }
                Next::Fatal(msg) => {
                    if frame::encode_frame(&msg, ws.max_frame, &mut frame).is_ok() {
                        out.extend_from_slice(&frame);
                    }
                    stop = true;
                    break;
                }
                Next::Stop => {
                    stop = true;
                    break;
                }
            }
            if n >= ws.coalesce_max {
                break;
            }
            // C5: append ACKs that are already available, never wait for more.
            match pending.try_recv() {
                Ok(p) => match try_resolve(p, diag) {
                    Ok(x) => next = x,
                    Err(p) => {
                        head = Some(p);
                        break;
                    }
                },
                Err(_) => break,
            }
        }
        if !out.is_empty() {
            let t_w = diag::now(diag);
            if let Err(e) = timed_write(&mut wr, &out, ws.write_timeout).await {
                if e.kind() == io::ErrorKind::TimedOut {
                    Counters::inc(&counters.ack_write_timeouts);
                }
                break 'conn;
            }
            if let (Some(d), Some(t_w)) = (diag, t_w) {
                d.since(Stage::AckWrite, t_w);
                for t in &stamps {
                    d.since(Stage::InKernel, *t);
                    if probe {
                        d.since(Stage::ProbeInKernel, *t);
                    }
                }
            }
        }
        if stop {
            break;
        }
    }
    let _ = timeout(ws.write_timeout, wr.shutdown()).await;
    drop(closed);
}

/// Overwrite every per-batch field of a reused ACK (the instance id stays).
fn set_ack(a: &mut pb::InputAck, source_sequence: u64, reply: &BatchReply) {
    a.source_sequence = source_sequence;
    a.status = reply.status as i32;
    a.operation_sequence = reply.operation_sequence;
    a.error_code = reply.error_code as i32;
    a.continuity = reply.continuity.map_or(0, |c| c as i32);
    a.last_accepted_source_sequence = reply.last_accepted;
}

/// Read-only introspection endpoint (tests and benchmarks). Bounded to a few connections.
pub async fn serve_introspection(server: Arc<Server>, listener: UnixListener) {
    let permits = Arc::new(Semaphore::new(4));
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
        let server = server.clone();
        tokio::spawn(async move {
            introspection_session(&server, stream).await;
            drop(permit);
        });
    }
}

async fn introspection_session(server: &Server, stream: UnixStream) {
    let max = server.config.limits.max_frame_bytes;
    let ft = Duration::from_millis(server.config.limits.frame_timeout_ms);
    let (mut rd, mut wr) = stream.into_split();
    let (mut buf, mut scratch) = (Vec::new(), Vec::new());
    // E3: an idle introspection client is dropped after the handshake timeout, so the four
    // permits cannot be held forever; requests are stop-and-wait (at most 4 queued).
    let idle = Duration::from_millis(server.config.limits.handshake_timeout_ms);
    while let Ok(Ok(true)) = timeout(idle, read_frame(&mut rd, max, &mut buf, ft, None)).await {
        let Ok(req) = ipb::IntrospectRequest::decode(buf.as_slice()) else { return };
        if req.introspect_version != 1 {
            return;
        }
        let (tx, rx) = oneshot::channel();
        if submit(
            server,
            Request::Introspect { include_tags: req.include_tags, reset_diagnostics: req.reset_diagnostics, reply: tx },
        )
        .await
        .is_err()
        {
            return;
        }
        let Ok(resp) = rx.await else { return };
        // Responses can exceed the driver frame limit with many tags; use the ceiling.
        let wt = Duration::from_millis(server.config.limits.ack_write_timeout_ms);
        if frame::encode_frame(&resp, frame::MAX_FRAME_BYTES_CEILING, &mut scratch).is_err()
            || timed_write(&mut wr, &scratch, wt).await.is_err()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh(id: &[u8], seq: u64, r: &BatchReply) -> KernelMessage {
        KernelMessage {
            body: Some(kernel_message::Body::Ack(pb::InputAck {
                source_sequence: seq,
                status: r.status as i32,
                operation_sequence: r.operation_sequence,
                error_code: r.error_code as i32,
                kernel_instance_id: id.to_vec(),
                continuity: r.continuity.map_or(0, |c| c as i32),
                last_accepted_source_sequence: r.last_accepted,
            })),
        }
    }

    /// C2 equivalence: the reused ACK encodes to exactly the bytes of a freshly built one,
    /// also when optional fields go from Some to None between batches.
    #[test]
    fn reused_ack_encodes_identically() {
        let id = [7u8; KERNEL_INSTANCE_ID_LEN];
        let replies = [
            BatchReply {
                status: AckStatus::AppliedVolatile,
                operation_sequence: Some(42),
                error_code: ErrorCode::Unspecified,
                continuity: Some(pb::Continuity::Continuous),
                last_accepted: Some(9),
                sent: None,
            },
            BatchReply {
                status: AckStatus::Rejected,
                operation_sequence: None,
                error_code: ErrorCode::SequenceGap,
                continuity: None,
                last_accepted: None,
                sent: None,
            },
            BatchReply {
                status: AckStatus::Duplicate,
                operation_sequence: None,
                error_code: ErrorCode::Unspecified,
                continuity: None,
                last_accepted: Some(u64::MAX),
                sent: None,
            },
        ];
        let mut reused = KernelMessage {
            body: Some(kernel_message::Body::Ack(pb::InputAck {
                kernel_instance_id: id.to_vec(),
                ..Default::default()
            })),
        };
        for (i, r) in replies.iter().cycle().take(9).enumerate() {
            if let Some(kernel_message::Body::Ack(a)) = &mut reused.body {
                set_ack(a, i as u64 + 1, r);
            }
            assert_eq!(reused.encode_to_vec(), fresh(&id, i as u64 + 1, r).encode_to_vec());
        }
    }
}

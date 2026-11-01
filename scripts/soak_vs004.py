#!/usr/bin/env python3
"""VS-004 soak (brief §8): one supervised kernel + N `scada-sim-driver soak` processes.

  soak_vs004.py --out DIR [--minutes 30] [--drivers 4] [--sample-s 30] [--scheduler ct|mt2]

Each driver cycles: (re)connect + FullState, partial Deltas (channel 3 only) and heartbeats
for --cycle-ms, then --pause-ms of complete silence on the open link (> liveness timeout:
the source goes Silent), one heartbeat (restore), one Delta, disconnect. Channels 1 and 2
are never refreshed by the Deltas, so they expire (StaleData) every cycle.

Every --sample-s the kernel's RSS / threads (/proc) and the VS-004 counters (introspection)
are appended to DIR/soak-samples.txt as `key=value` lines; nothing is aggregated here.
Uses only the standard library: the introspection reply is parsed with a minimal protobuf
varint reader (field numbers from proto/scada/introspect/v1/introspect.proto).
"""
import argparse, os, socket, struct, subprocess, sys, tempfile, time

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--minutes", type=float, default=30)
ap.add_argument("--drivers", type=int, default=4)
ap.add_argument("--sample-s", type=float, default=30)
ap.add_argument("--scheduler", default="ct")
ap.add_argument("--cycle-ms", type=int, default=10000)
ap.add_argument("--pause-ms", type=int, default=2000)
ap.add_argument("--heartbeat-ms", type=int, default=250)
ap.add_argument("--bin", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "target", "release"))
a = ap.parse_args()

LIVE, FRESH, POLL = 1000, 3000, 100
COUNTERS = {5: "batches_applied", 6: "batches_duplicate", 7: "batches_rejected", 4: "protocol_errors",
            1: "connections_accepted", 10: "active_connections", 13: "heartbeats_valid", 14: "heartbeats_invalid",
            15: "heartbeats_coalesced", 16: "liveness_expiries", 17: "liveness_recoveries", 18: "freshness_expiries",
            19: "freshness_tags_expired", 20: "watchdog_ticks", 21: "watchdog_ticks_coalesced",
            22: "watchdog_ticks_late", 23: "watchdog_max_late_ns", 24: "liveness_recoveries_by_batch"}


def varint(b, i):
    v = s = 0
    while True:
        x = b[i]; i += 1
        v |= (x & 0x7F) << s; s += 7
        if x < 0x80:
            return v, i


def fields(b):
    i, out = 0, []
    while i < len(b):
        k, i = varint(b, i)
        f, t = k >> 3, k & 7
        if t == 0:
            v, i = varint(b, i)
        elif t == 1:
            v = b[i:i + 8]; i += 8
        elif t == 5:
            v = b[i:i + 4]; i += 4
        elif t == 2:
            n, i = varint(b, i); v = b[i:i + n]; i += n
        else:
            raise ValueError(f"wire type {t}")
        out.append((f, v))
    return out


def introspect(path):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect(path)
    req = bytes([0x10, 0x01])  # introspect_version = 1, include_tags = false
    s.sendall(struct.pack(">I", len(req)) + req)
    hdr = b""
    while len(hdr) < 4:
        hdr += s.recv(4 - len(hdr))
    n = struct.unpack(">I", hdr)[0]
    body = b""
    while len(body) < n:
        body += s.recv(n - len(body))
    s.close()
    r = {}
    for f, v in fields(body):
        if f == 3:
            r["last_operation_sequence"] = v
        elif f == 10:
            r["state_digest_version"] = v
        elif f == 7:
            for cf, cv in fields(v):
                if cf in COUNTERS:
                    r[COUNTERS[cf]] = cv
        elif f == 6:
            live = dict(fields(v)).get(6, 0)
            r[f"liveness_{live}"] = r.get(f"liveness_{live}", 0) + 1
    return r


def proc(pid, key):
    for l in open(f"/proc/{pid}/status"):
        if l.startswith(key):
            return int(l.split()[1])
    return 0


os.makedirs(a.out, exist_ok=True)
d = tempfile.mkdtemp(prefix="scada-soak-")
os.chmod(d, 0o700)
cfg = os.path.join(d, "soak.toml")
with open(cfg, "w") as f:
    f.write(f'[ipc]\nsocket_path = "{d}/driver.sock"\nmax_frame_bytes = 65536\nmax_driver_connections = {a.drivers + 4}\n'
            "input_queue_capacity = 64\nmax_inflight_per_connection = 32\nhandshake_timeout_ms = 2000\nframe_timeout_ms = 2000\n"
            f"\n[runtime]\nwatchdog_poll_ms = {POLL}\n\n[core]\nmax_observations_per_batch = 128\n")
    for n in range(a.drivers):
        for c, t in ((1, "bool"), (2, "bool"), (3, "i64")):
            f.write(f'\n[[tags]]\nid = {(n + 1) * 100 + c}\nname = "s{n}_c{c}"\ntype = "{t}"\n')
    for n in range(a.drivers):
        f.write(f'\n[[sources]]\nid = {1001 + n}\nname = "soak-{n}"\nliveness_timeout_ms = {LIVE}\nfreshness_timeout_ms = {FRESH}\n'
                "channels = [\n")
        for c, t in ((1, "bool"), (2, "bool"), (3, "i64")):
            f.write(f'  {{ channel = {c}, tag = {(n + 1) * 100 + c}, type = "{t}" }},\n')
        f.write("]\n")
sched = [] if a.scheduler == "ct" else ["--scheduler", "multi_thread", "--worker-threads", "2"]
intro = os.path.join(d, "intro.sock")
log = open(os.path.join(a.out, "soak-kernel-stderr.txt"), "w")
k = subprocess.Popen([os.path.join(a.bin, "scada-kernel"), "--config", cfg, "--introspection-socket", intro] + sched,
                     stdout=subprocess.PIPE, stderr=log, text=True)
ready = k.stdout.readline().strip()
ms = int(a.minutes * 60000)
drivers = [subprocess.Popen([os.path.join(a.bin, "scada-sim-driver"), "--socket", f"{d}/driver.sock", "--source-id", str(1001 + n),
                             "--heartbeat-ms", str(a.heartbeat_ms), "soak", "--duration-ms", str(ms), "--cycle-ms",
                             str(a.cycle_ms + 700 * n), "--pause-ms", str(a.pause_ms)],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for n in range(a.drivers)]
out = open(os.path.join(a.out, "soak-samples.txt"), "w")
out.write(f"# {ready}\n# drivers={a.drivers} minutes={a.minutes} liveness_ms={LIVE} freshness_ms={FRESH} poll_ms={POLL} "
          f"cycle_ms={a.cycle_ms}(+700/driver) pause_ms={a.pause_ms} heartbeat_ms={a.heartbeat_ms} scheduler={a.scheduler}\n")
t0 = time.time()
while any(p.poll() is None for p in drivers):
    r = introspect(intro)
    r["t_s"] = round(time.time() - t0)
    r["rss_kib"] = proc(k.pid, "VmRSS:")
    r["hwm_kib"] = proc(k.pid, "VmHWM:")
    r["threads"] = proc(k.pid, "Threads:")
    out.write(" ".join(f"{x}={y}" for x, y in r.items()) + "\n")
    out.flush()
    time.sleep(a.sample_s)
r = introspect(intro)
r["t_s"] = round(time.time() - t0)
r["rss_kib"] = proc(k.pid, "VmRSS:")
r["hwm_kib"] = proc(k.pid, "VmHWM:")
out.write("final " + " ".join(f"{x}={y}" for x, y in r.items()) + "\n")
for n, p in enumerate(drivers):
    out.write(f"# driver {1001 + n} exit={p.returncode} {p.stdout.read().strip().splitlines()[-1:]}\n")
k.terminate()
k.wait()
out.write(f"# kernel exit={k.returncode}\n")
print(open(os.path.join(a.out, "soak-samples.txt")).read().splitlines()[-a.drivers - 2:])

#!/usr/bin/env python3
"""VS-007 system benchmark: real kernel + Modbus driver process + Modbus simulator, loopback.

One cell = one run of the three processes for --secs seconds after --warmup seconds. The
raw result is ONE `kind=vs007` line (key=value), appended to --out. Analysis lives in
docs/benchmarks/vs007.md; this script does not interpret.

  python3 scripts/bench_vs007.py --out FILE --label L --devices 8 --qty 16 --scan-ms 10 \
      [--mutate-every 1] [--journal full|off] [--durable] [--ingress fifo|source_fair:4] \
      [--scheduler current_thread|multi_thread:2] [--secs 10] [--warmup 2]

Measured from the processes, never estimated:
  scans/s, registers/s (items read), observations/s (published), ACKs/s (volatile,
  durable), publish->ACK latency p50/p99/p99.9/max (bucket upper bounds, cumulative over
  warm-up + run), CPU % of each process (utime+stime over the run, 100 = one core), peak
  RSS (VmHWM) and final RSS (VmRSS), deferred scans, overruns, rejections, failures.
Binaries: target/release/{scada-kernel,scada-driver-modbus-tcp,scada-modbus-sim}.
"""
import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release")
TICK = os.sysconf("SC_CLK_TCK")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def cpu_ticks(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return int(parts[11]) + int(parts[12])  # utime + stime


def mem_kib(pid):
    out = {}
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith(("VmRSS:", "VmHWM:")):
                k, v = line.split(":")
                out[k] = int(v.split()[0])
    return out.get("VmRSS", 0), out.get("VmHWM", 0)


def profile(qty):
    pts = "".join(
        f'[[points]]\nname = "r{i}"\narea = "input_register"\naddress = {100 + i}\ntype = "u16"\n\n' for i in range(qty)
    )
    return (
        'schema = "moscada.driver-catalog/1"\n\n[profile]\n'
        f'id = "bench-q{qty}"\nversion = 1\ntitle = "benchmark block of {qty} input registers"\n'
        'manufacturer = "none (benchmark fixture)"\ntransport = "tcp"\nprotocol = "modbus"\n'
        'source_document = "scripts/bench_vs007.py"\ndescription = "benchmark"\nmax_requests_per_scan = 1\n\n'
        f'[[blocks]]\narea = "input_register"\nstart = 100\ncount = {qty}\n\n' + pts
    )


def kernel_toml(d, a):
    s = (
        f'[ipc]\nsocket_path = "{d}/driver.sock"\nmax_frame_bytes = 65536\nmax_driver_connections = {max(16, a.devices)}\n'
        f"input_queue_capacity = 256\nmax_inflight_per_connection = 32\nhandshake_timeout_ms = 2000\nframe_timeout_ms = 2000\n"
    )
    if a.ingress.startswith("source_fair"):
        s += f'ingress_policy = "source_fair"\nper_source_pending_limit = {a.ingress.split(":")[1]}\n'
    s += "\n[core]\nmax_observations_per_batch = 128\n\n"
    for dev in range(a.devices):
        src, tag0 = 1001 + dev, 10_000 + dev * 200
        for i in range(a.qty):
            s += f'[[tags]]\nid = {tag0 + i}\nname = "d{dev}_r{i}"\ntype = "i64"\n'
        s += f'[[sources]]\nid = {src}\nname = "modbus-{src}"\nchannels = [\n'
        s += "".join(f'  {{ channel = {i + 1}, tag = {tag0 + i}, type = "i64" }},\n' for i in range(a.qty))
        s += "]\n"
    if a.journal != "off":
        s += (
            f'\n[persistence]\npath = "{a.store}/kernel.db"\ndurability = "{a.journal}"\n'
            f'history_socket_path = "{d}/history.sock"\nhistory_authorized_uids = [{os.getuid()}]\n'
        )
    return s


def driver_toml(d, a, port):
    s = (
        f'schema = "moscada.driver.modbus-tcp/1"\n\n[driver]\ndriver_id = "bench"\nkernel_socket = "{d}/driver.sock"\n'
        f'catalog_dir = "{d}/catalog"\nrequest_durable_receipts = {"true" if a.durable else "false"}\n'
        f"max_pending_batches = 64\nmax_inflight_batches = 8\nstatus_interval_ms = 1000\nmax_devices = {max(64, a.devices)}\n\n"
    )
    for dev in range(a.devices):
        s += (
            f'[[devices]]\nname = "d{dev}"\nsource_id = {1001 + dev}\nhost = "127.0.0.1"\nport = {port}\nunit_id = 1\n'
            f'profile = "bench-q{a.qty}"\nprofile_version = 1\nscan_interval_ms = {a.scan_ms}\n'
            f"response_timeout_ms = 1000\nconnect_timeout_ms = 1000\ndevice_backoff_min_ms = 100\n"
            "channels = [" + ", ".join(f'{{ channel = {i + 1}, point = "r{i}" }}' for i in range(a.qty)) + "]\n\n"
        )
    return s


def parse_kv(line):
    out = {}
    for m in re.finditer(r'(\w+)=("(?:[^"\\]|\\.)*"|\S+)', line):
        out[m.group(1)] = m.group(2).strip('"')
    return out


def reader(proc, sink, lock):
    for line in proc.stdout:
        with lock:
            sink.append((time.monotonic(), line.rstrip("\n")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--devices", type=int, default=1)
    ap.add_argument("--qty", type=int, default=16)
    ap.add_argument("--scan-ms", type=int, default=10)
    ap.add_argument("--mutate-every", type=int, default=1, help="sim requests between value changes (0 = static)")
    ap.add_argument("--journal", default="off", choices=["off", "full", "normal"])
    ap.add_argument("--durable", action="store_true", help="driver requests durable receipts")
    ap.add_argument("--ingress", default="fifo")
    ap.add_argument("--scheduler", default="current_thread")
    ap.add_argument("--secs", type=float, default=10)
    ap.add_argument("--warmup", type=float, default=2)
    ap.add_argument("--store-dir", default=None, help="journal directory parent (default: a temp dir on /tmp)")
    a = ap.parse_args()

    d = tempfile.mkdtemp(prefix="vs007b-")
    a.store = tempfile.mkdtemp(prefix="vs007j-", dir=a.store_dir) if a.store_dir else os.path.join(d, "journal")
    os.makedirs(a.store, exist_ok=True)
    os.makedirs(f"{d}/catalog")
    with open(f"{d}/catalog/bench-q{a.qty}.v1.toml", "w") as f:
        f.write(profile(a.qty))
    with open(f"{d}/kernel.toml", "w") as f:
        f.write(kernel_toml(d, a))
    port = free_port()
    with open(f"{d}/driver.toml", "w") as f:
        f.write(driver_toml(d, a, port))

    procs = []
    lock = threading.Lock()
    try:
        kargs = [f"{BIN}/scada-kernel", "--config", f"{d}/kernel.toml"]
        if a.scheduler.startswith("multi_thread"):
            kargs += ["--scheduler", "multi_thread", "--worker-threads", a.scheduler.split(":")[1]]
        k = subprocess.Popen(kargs, stdout=subprocess.PIPE, stderr=open(f"{d}/kernel.err", "w"), text=True)
        procs.append(k)
        if not k.stdout.readline().startswith("READY"):
            raise SystemExit(f"kernel did not start: {open(d + '/kernel.err').read()}")
        sim = subprocess.Popen(
            [f"{BIN}/scada-modbus-sim", "--listen", f"127.0.0.1:{port}", "--seed", "7007",
             "--mutate-every", str(a.mutate_every), "--registers", str(a.qty), "--max-connections", str(a.devices + 4)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        procs.append(sim)
        if not sim.stdout.readline().startswith("LISTENING"):
            raise SystemExit("simulator did not start")
        threading.Thread(target=lambda: [None for _ in sim.stdout], daemon=True).start()
        lines = []
        drv = subprocess.Popen(
            [f"{BIN}/scada-driver-modbus-tcp", "--config", f"{d}/driver.toml", "--status-interval-ms", "500"],
            stdout=subprocess.PIPE, stderr=open(f"{d}/driver.err", "w"), text=True,
        )
        procs.append(drv)
        threading.Thread(target=reader, args=(drv, lines, lock), daemon=True).start()

        def snapshot():
            with lock:
                n = len(lines)
            # Wait for one full round of STATUS lines after `n`.
            deadline = time.time() + 5
            while time.time() < deadline:
                with lock:
                    st = [(t, parse_kv(x)) for t, x in lines[n:] if x.startswith("STATUS ")]
                by = {}
                for t, s in st:
                    by.setdefault(s["device"], (t, s))
                if len(by) == a.devices:
                    # One STATUS round is printed at once: its time is the latest line's.
                    return max(t for t, _ in by.values()), {k: v for k, (_, v) in by.items()}
                time.sleep(0.05)
            raise SystemExit(f"no STATUS round; driver stderr: {open(d + '/driver.err').read()[-2000:]}")

        time.sleep(a.warmup)
        ts0, s0 = snapshot()
        c0 = {p.pid: cpu_ticks(p.pid) for p in procs}
        t0 = time.time()
        time.sleep(a.secs)
        c1 = {p.pid: cpu_ticks(p.pid) for p in procs}
        t1 = time.time()
        ts1, s1 = snapshot()
        sel = ts1 - ts0  # interval between the two STATUS rounds: basis of the rates
        mem = {p.pid: mem_kib(p.pid) for p in procs}
        el = t1 - t0

        def tot(key, snap):
            return sum(int(s.get(key, "0").replace("-", "0") or 0) for s in snap.values())

        def rate(key):
            return (tot(key, s1) - tot(key, s0)) / sel

        def worst(key):
            vals = [int(s[key]) for s in s1.values() if s.get(key, "-") != "-"]
            return max(vals) if vals else "-"

        cpu = {p.pid: 100.0 * (c1[p.pid] - c0[p.pid]) / TICK / el for p in procs}
        failed = tot("scans_failed", s1)
        rec = {
            "kind": "vs007",
            "label": a.label,
            "devices": a.devices,
            "qty": a.qty,
            "scan_ms": a.scan_ms,
            "mutate_every": a.mutate_every,
            "journal": a.journal,
            "durable": int(a.durable),
            "ingress": a.ingress,
            "scheduler": a.scheduler,
            "secs": f"{sel:.2f}",
            "scans_per_s": f"{rate('scans_ok'):.1f}",
            "registers_per_s": f"{rate('scans_ok') * a.qty:.0f}",
            "observations_per_s": f"{rate('observations'):.0f}",
            "acks_volatile_per_s": f"{rate('acks_volatile'):.1f}",
            "acks_durable_per_s": f"{rate('acks_durable'):.1f}",
            "ack_p50_us_worst": worst("ack_p50_us"),
            "ack_p99_us_worst": worst("ack_p99_us"),
            "ack_p999_us_worst": worst("ack_p999_us"),
            "ack_max_us_worst": worst("ack_max_us"),
            "deferred": tot("deferred", s1) - tot("deferred", s0),
            "overruns": tot("overruns", s1) - tot("overruns", s0),
            "rejected_retryable": tot("rejected_retryable", s1),
            "rejected_fatal": tot("rejected_fatal", s1),
            "scans_failed": failed,
            "pending_high_water_max": max(int(s["pending_high_water"]) for s in s1.values()),
            "cpu_kernel_pct": f"{cpu[k.pid]:.1f}",
            "cpu_driver_pct": f"{cpu[drv.pid]:.1f}",
            "cpu_sim_pct": f"{cpu[sim.pid]:.1f}",
            "rss_kernel_kib": mem[k.pid][0],
            "hwm_kernel_kib": mem[k.pid][1],
            "rss_driver_kib": mem[drv.pid][0],
            "hwm_driver_kib": mem[drv.pid][1],
            "rss_sim_kib": mem[sim.pid][0],
        }
        line = " ".join(f"{k_}={v}" for k_, v in rec.items())
        print(line)
        with open(a.out, "a") as f:
            f.write(line + "\n")
    finally:
        for p in reversed(procs):
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        shutil.rmtree(d, ignore_errors=True)
        if a.store_dir:
            shutil.rmtree(a.store, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

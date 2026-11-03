#!/usr/bin/env python3
"""VS-007 T15 soak: real kernel with the VS-006 journal (FULL) + Modbus driver (durable
receipts) + Modbus simulator on loopback, with driven disconnections, for --minutes.

Disturbances, on a fixed schedule (seeded, reproducible):
  * every --sim-kill-s seconds the simulator is killed (SIGKILL: TCP reset mid-scan) and
    restarted --sim-down-s seconds later on the same port: devices go down, the driver
    closes their kernel sessions (SourceDisconnected), backs off, reconnects, FullState;
  * every --kernel-restart-s seconds the kernel is stopped (SIGTERM) and restarted on the
    same journal: recovery, links re-established, replay/FullState.
Every --sample-s seconds one `kind=soak_sample` line: RSS of each process, summed driver
counters, device states. At the end one `kind=soak_result` line with the verdict:
  PASS needs: driver alive for the whole run with ONE pid (never restarted), no `fatal`,
  every device `up` and `streaming` at the end, no fatal rejections, and the driver RSS
  of the last third of the run within +10 % of its median over the middle third (bounded).
Kernel RSS is reported, not judged here (it restarts by design).
"""
import argparse
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_vs007 as b  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--minutes", type=float, default=30)
    ap.add_argument("--devices", type=int, default=8)
    ap.add_argument("--qty", type=int, default=16)
    ap.add_argument("--scan-ms", type=int, default=100)
    ap.add_argument("--sim-kill-s", type=float, default=90)
    ap.add_argument("--sim-down-s", type=float, default=5)
    ap.add_argument("--kernel-restart-s", type=float, default=420)
    ap.add_argument("--sample-s", type=float, default=30)
    a = ap.parse_args()
    a.journal, a.durable, a.ingress, a.scheduler, a.mutate_every = "full", True, "fifo", "current_thread", 3

    d = tempfile.mkdtemp(prefix="vs007s-")
    a.store = os.path.join(d, "journal")
    os.makedirs(a.store)
    os.makedirs(f"{d}/catalog")
    with open(f"{d}/catalog/bench-q{a.qty}.v1.toml", "w") as f:
        f.write(b.profile(a.qty))
    with open(f"{d}/kernel.toml", "w") as f:
        f.write(b.kernel_toml(d, a))
    port = b.free_port()
    with open(f"{d}/driver.toml", "w") as f:
        f.write(b.driver_toml(d, a, port))
    out = open(a.out, "a")

    def emit(line):
        print(line, flush=True)
        out.write(line + "\n")
        out.flush()

    emit(f"# vs007 soak {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {vars(a)}")
    kerr = open(f"{d}/kernel.err", "a")

    def start_kernel():
        k = subprocess.Popen([f"{b.BIN}/scada-kernel", "--config", f"{d}/kernel.toml"],
                             stdout=subprocess.PIPE, stderr=kerr, text=True)
        line = k.stdout.readline()
        if not line.startswith("READY"):
            raise SystemExit(f"kernel did not start: {line}")
        threading.Thread(target=lambda: [None for _ in k.stdout], daemon=True).start()
        return k

    def start_sim():
        s = subprocess.Popen(
            [f"{b.BIN}/scada-modbus-sim", "--listen", f"127.0.0.1:{port}", "--seed", "15015", "--mutate-every", "3",
             "--registers", str(a.qty), "--max-connections", str(a.devices + 4)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if not s.stdout.readline().startswith("LISTENING"):
            raise SystemExit("simulator did not start")
        threading.Thread(target=lambda: [None for _ in s.stdout], daemon=True).start()
        return s

    k = start_kernel()
    sim = start_sim()
    lines, lock = [], threading.Lock()
    drv = subprocess.Popen([f"{b.BIN}/scada-driver-modbus-tcp", "--config", f"{d}/driver.toml",
                            "--status-interval-ms", "1000"],
                           stdout=subprocess.PIPE, stderr=open(f"{d}/driver.err", "w"), text=True)
    threading.Thread(target=b.reader, args=(drv, lines, lock), daemon=True).start()
    drv_pid = drv.pid
    t_start = time.monotonic()
    end = t_start + a.minutes * 60
    next_sim_kill = t_start + a.sim_kill_s
    sim_back_at = None
    next_kernel = t_start + a.kernel_restart_s
    next_sample = t_start + a.sample_s
    sim_kills = kernel_restarts = 0
    samples = []

    def latest():
        with lock:
            tail = lines[-4 * a.devices - 64:]
        by = {}
        for _, x in reversed(tail):
            if x.startswith("STATUS "):
                s = b.parse_kv(x)
                by.setdefault(s["device"], s)
        return by

    try:
        while time.monotonic() < end:
            now = time.monotonic()
            if drv.poll() is not None:
                emit(f"kind=soak_event t={now - t_start:.0f} event=driver_exited code={drv.returncode}")
                break
            if sim_back_at is None and now >= next_sim_kill:
                sim.send_signal(signal.SIGKILL)
                sim.wait()
                sim_kills += 1
                sim_back_at = now + a.sim_down_s
                emit(f"kind=soak_event t={now - t_start:.0f} event=sim_killed n={sim_kills}")
            if sim_back_at is not None and now >= sim_back_at:
                sim = start_sim()
                sim_back_at = None
                next_sim_kill = now + a.sim_kill_s
                emit(f"kind=soak_event t={now - t_start:.0f} event=sim_restarted")
            if now >= next_kernel:
                k.terminate()
                k.wait(timeout=30)
                k = start_kernel()
                kernel_restarts += 1
                next_kernel = now + a.kernel_restart_s
                emit(f"kind=soak_event t={now - t_start:.0f} event=kernel_restarted n={kernel_restarts}")
            if now >= next_sample:
                next_sample += a.sample_s
                st = latest()
                rk, _ = b.mem_kib(k.pid)
                rd, hd = b.mem_kib(drv.pid)
                rs = b.mem_kib(sim.pid)[0] if sim.poll() is None else 0
                tot = lambda key: sum(int(s.get(key, "0").replace("-", "0") or 0) for s in st.values())  # noqa: E731
                up = sum(1 for s in st.values() if s["device_state"] == "up")
                streaming = sum(1 for s in st.values() if s["link"] == "streaming")
                samples.append((now - t_start, rd))
                emit(
                    f"kind=soak_sample t={now - t_start:.0f} rss_kernel_kib={rk} rss_driver_kib={rd} hwm_driver_kib={hd} "
                    f"rss_sim_kib={rs} devices_up={up} streaming={streaming} scans_ok={tot('scans_ok')} "
                    f"scans_failed={tot('scans_failed')} acks_durable={tot('acks_durable')} acks_duplicate={tot('acks_duplicate')} "
                    f"full_states={tot('full_states')} deltas={tot('deltas')} resent={tot('resent')} "
                    f"lost_kernel_restart={tot('lost_kernel_restart')} rejected_retryable={tot('rejected_retryable')} "
                    f"rejected_fatal={tot('rejected_fatal')} pending_max={max((int(s['pending']) for s in st.values()), default=0)} "
                    f"deferred={tot('deferred')} connects={tot('connects')}"
                )
            time.sleep(0.2)
        # Settle: let every device come back after the last disturbance.
        if sim_back_at is not None:
            sim = start_sim()
        deadline = time.monotonic() + 30
        st = latest()
        while time.monotonic() < deadline:
            st = latest()
            if len(st) == a.devices and all(s["device_state"] == "up" and s["link"] == "streaming" for s in st.values()):
                break
            time.sleep(0.5)
        alive = drv.poll() is None and drv.pid == drv_pid
        fatal = [s["device"] for s in st.values() if s.get("fatal")]
        ok_end = len(st) == a.devices and all(s["device_state"] == "up" and s["link"] == "streaming" for s in st.values())
        rej_fatal = sum(int(s["rejected_fatal"]) for s in st.values())
        n = len(samples)
        mid = sorted(r for _, r in samples[n // 3: 2 * n // 3]) or [0]
        last = [r for _, r in samples[2 * n // 3:]] or [0]
        med = mid[len(mid) // 2]
        bounded = med > 0 and max(last) <= med * 1.10
        verdict = "PASS" if alive and not fatal and ok_end and rej_fatal == 0 and bounded else "FAIL"
        emit(
            f"kind=soak_result verdict={verdict} minutes={(time.monotonic() - t_start) / 60:.1f} driver_alive_same_pid={alive} "
            f"fatal_devices={','.join(fatal) or '-'} all_up_streaming_at_end={ok_end} rejected_fatal={rej_fatal} "
            f"sim_kills={sim_kills} kernel_restarts={kernel_restarts} driver_rss_mid_median_kib={med} "
            f"driver_rss_last_third_max_kib={max(last)} driver_rss_bounded={bounded} samples={n}"
        )
        with open(f"{d}/kernel.err") as f:
            errs = [x for x in f.read().splitlines() if "FATAL" in x or "panic" in x]
        emit(f"kind=soak_kernel_stderr fatal_or_panic_lines={len(errs)}")
        return 0 if verdict == "PASS" else 1
    finally:
        for p in (drv, sim, k):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

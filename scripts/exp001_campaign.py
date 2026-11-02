#!/usr/bin/env python3
"""EXP-001 benchmark campaign: SQLite (A) vs open-wal (B) vs open-wal + derived projection (C).

Every cell runs the three variants interleaved (a, b, c, a, b, c, ...) on the same
machine, filesystem and binary, so drift of the VM affects them alike. Raw output lines
are kept verbatim; a CSV and a Markdown summary (median, min-max, n) are derived.

    cargo build --release -p scada-bench --features experimental-open-wal
    python3 scripts/exp001_campaign.py --store /path/on/real/disk --out docs/benchmarks/results/exp001 \
        [--phases fsync,batch,delay,shape,read,latency,large,soak] [--reps 10] [--soak-min 30]
"""
import argparse
import csv
import os
import platform
import statistics
import subprocess
import sys
import time

BIN = "target/release/scada-bench"


def kv(line):
    out = {}
    for tok in line.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            out[k] = v
    return out


def run(args, raw):
    cmd = [BIN] + args
    t = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True)
    raw.write(f"# {' '.join(cmd)}\n# exit={p.returncode} wall_s={time.time() - t:.1f}\n")
    raw.write(p.stdout)
    if p.returncode != 0:
        raw.write("# STDERR\n" + p.stderr)
        raw.flush()
        print(p.stderr, file=sys.stderr)
        raise SystemExit(f"failed: {' '.join(cmd)}")
    raw.flush()
    return [kv(l) for l in p.stdout.splitlines() if l.startswith("kind=")]


def env_info(store):
    def sh(c):
        try:
            return subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()
        except Exception as e:  # noqa: BLE001
            return str(e)

    return {
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "uname": platform.uname()._asdict(),
        "cpu": sh("lscpu | grep -E 'Model name|^CPU\\(s\\)|Architecture|Hypervisor'"),
        "mem": sh("free -m | head -2"),
        "fs": sh(f"df -T {store} | tail -1"),
        "mount": sh(f"findmnt -T {store} -o SOURCE,FSTYPE,OPTIONS -n"),
        "block": sh("lsblk -d -o NAME,ROTA,SIZE,MODEL 2>/dev/null"),
        "rustc": sh("rustc --version"),
        "git": sh("git rev-parse HEAD"),
        "open_wal": sh("grep -A1 'name = \"open-wal\"' Cargo.lock | tail -1"),
        "rusqlite": sh("grep -A1 'name = \"rusqlite\"' Cargo.lock | tail -1"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--phases", default="fsync,batch,delay,shape,read,large")
    ap.add_argument("--reps", type=int, default=10)
    ap.add_argument("--soak-min", type=int, default=30)
    ap.add_argument("--soak-rate", type=int, default=5000)
    a = ap.parse_args()
    os.makedirs(os.path.join(a.out, "raw"), exist_ok=True)
    phases = a.phases.split(",")
    store = os.path.join(a.store, "exp001-run")
    with open(os.path.join(a.out, "environment.txt"), "a") as f:
        for k, v in env_info(a.store).items():
            f.write(f"{k}: {v}\n")
        f.write("\n")
    rows = []

    def cell(phase, name, extra, reps, variants="abc"):
        path = os.path.join(a.out, "raw", f"{phase}-{name}.txt")
        with open(path, "a") as raw:
            for rep in range(reps):
                for v in variants:
                    for r in run(["exp001", "--variant", v, "--store", store, "--label", f"{phase}:{name}",
                                  "--rep", str(rep)] + extra, raw):
                        r["phase"] = phase
                        r["cell"] = name
                        rows.append(r)
        print(f"{phase}:{name} done", flush=True)

    if "fsync" in phases:
        with open(os.path.join(a.out, "raw", "fsync-probe.txt"), "a") as raw:
            for size in (512, 4096, 65536):
                for r in run(["fsync-probe", "--store", a.store, "--n", "3000", "--size", str(size)], raw):
                    r["phase"] = "fsync"
                    rows.append(r)
    if "batch" in phases:
        # Journal-only input -> durable watermark; commit batch 1/16/128/1024, no delay.
        for cm, ops in ((1, 5000), (16, 50000), (128, 200000), (1024, 200000)):
            cell("batch", f"cm{cm}", ["--ops", str(ops), "--commit-max", str(cm), "--events-per-op", "1",
                                      "--checkpoint-every", "100000"], a.reps)
    if "delay" in phases:
        for cm, ops in ((1, 3000), (16, 30000), (128, 100000), (1024, 100000)):
            for d in (1000, 5000):
                cell("delay", f"cm{cm}-d{d}", ["--ops", str(ops), "--commit-max", str(cm), "--commit-delay-us", str(d),
                                               "--events-per-op", "1", "--checkpoint-every", "100000"], max(3, a.reps // 3))
    if "shape" in phases:
        # Sources x observations x fan-out, saturated, commit_max 1024.
        for name, s, o, e, ops in (("s1-o1-e0", 1, 1, 0, 200000), ("s1-o1-e16", 1, 1, 16, 100000),
                                   ("s1-o1-e100", 1, 1, 100, 30000), ("s8-o128-e1", 8, 128, 1, 50000),
                                   ("s16-o128-e1", 16, 128, 1, 50000), ("s16-o1-e16", 16, 1, 16, 100000)):
            cell("shape", name, ["--ops", str(ops), "--sources", str(s), "--obs", str(o), "--events-per-op", str(e),
                                 "--commit-max", "1024", "--checkpoint-every", "100000"], max(5, a.reps // 2))
    if "read" in phases:
        # Historian queried in parallel every 10 ms while writing (A: journal file, C: projection).
        cell("read", "s1-o1-e1-r10", ["--ops", "300000", "--events-per-op", "1", "--commit-max", "1024",
                                      "--read-interval-ms", "10", "--checkpoint-every", "100000"],
             max(5, a.reps // 2), variants="ac")
    if "latency" in phases:
        # Receipt latency below saturation (paced producer, 10 s per run): the saturated
        # cells above measure queueing, not the commit path.
        for rate in (1000, 5000, 20000):
            cell("latency", f"rate{rate}", ["--ops", str(rate * 10), "--rate", str(rate), "--events-per-op", "1",
                                            "--commit-max", "1024", "--checkpoint-every", "100000"], max(5, a.reps // 2))
    if "large" in phases:
        # 10 M events (1 M operations x fan-out 10), checkpoints, recovery and (C) full rebuild.
        cell("large", "1M-e10", ["--ops", "1000000", "--events-per-op", "10", "--commit-max", "1024",
                                 "--checkpoint-every", "100000", "--rebuild"], 3)
    if "soak" in phases:
        ops = a.soak_min * 60 * a.soak_rate
        cell("soak", f"{a.soak_min}min-{a.soak_rate}ops", ["--ops", str(ops), "--rate", str(a.soak_rate),
                                                            "--report-every-s", "60", "--events-per-op", "1",
                                                            "--obs", "8", "--commit-max", "1024",
                                                            "--read-interval-ms", "100", "--checkpoint-every", "100000"],
             1, variants="ac")

    keys = sorted({k for r in rows for k in r})
    first = ["phase", "cell", "kind", "variant", "rep"]
    keys = first + [k for k in keys if k not in first]
    csv_path = os.path.join(a.out, f"exp001-{'-'.join(phases)}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"wrote {csv_path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()

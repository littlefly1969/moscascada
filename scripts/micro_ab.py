#!/usr/bin/env python3
"""Interleaved A/B runs of Criterion benchmark binaries (VS-002).

Criterion's confidence interval only covers noise inside one run. On a shared VM the
drift between runs is larger, so each variant (a separately compiled bench binary) is
run RUNS times in a shuffled order and every run's median is recorded as one sample;
bench_analyze.py then compares variants across runs.

  micro_ab.py --out FILE --runs 7 --filter apply --variant NAME=BINARY ...
"""
import argparse, os, random, re, subprocess, sys, tempfile, time

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--runs", type=int, default=7)
ap.add_argument("--seed", type=int, default=3)
ap.add_argument("--filter", default="")
ap.add_argument("--measurement", default="2")
ap.add_argument("--variant", action="append", required=True)
a = ap.parse_args()
variants = [v.split("=", 1) for v in a.variant]
rng = random.Random(a.seed)
pat = re.compile(r"^(\S+)\s+time:\s+\[\S+ \S+ (\S+) (\S+) \S+ \S+\]")
units = {"ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}
with open(a.out, "a") as out:
    out.write(f"# micro_ab start {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} runs={a.runs} filter={a.filter}\n")
    for name, binary in variants:
        out.write(f"# variant {name} binary={binary}\n")
    for r in range(1, a.runs + 1):
        order = variants[:]
        rng.shuffle(order)
        for name, binary in order:
            with tempfile.TemporaryDirectory() as d:
                p = subprocess.run([os.path.abspath(binary), "--bench", a.filter, "--warm-up-time", "1",
                                    "--measurement-time", a.measurement, "--noplot"],
                                   capture_output=True, text=True, cwd=d)
            text = p.stdout.replace("\n                        time:", " time:")
            n = 0
            for line in text.splitlines():
                m = pat.match(line.strip())
                if m:
                    ns = float(m.group(2)) * units[m.group(3)]
                    out.write(f"round={r} variant={name} spec={m.group(1)} median_ns={ns:.2f}\n")
                    n += 1
            out.flush()
            print(f"round {r} {name}: {n} benches rc={p.returncode}", file=sys.stderr, flush=True)

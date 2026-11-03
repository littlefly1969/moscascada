#!/usr/bin/env python3
"""A/B benchmark campaign runner (VS-002, brief §9).

Runs every (variant, cell) pair RUNS times. In each round the order of all pairs is
shuffled with a fixed seed, so baseline and candidates alternate and slow drifts of the
machine (thermal, hypervisor neighbours) are spread over all variants instead of biasing
one of them. Every raw line printed by `scada-bench` is appended to OUT, prefixed with
`round=<r> variant=<name>`; nothing is aggregated here (see bench_analyze.py).

  bench_campaign.py --out FILE --runs 7 [--seed 1] [--secs 3] [--warmup 1]
      --variant NAME=KERNEL_PATH[,kernel-arg,...]   (repeatable; a kernel-arg of the form
                                                   @ipc:key=value becomes an extra [ipc]
                                                   config line, e.g. a VS-003 ingress policy;
                                                   @bench:--opt=value is passed to scada-bench,
                                                   e.g. VS-004 @bench:--supervise=5000:12000:250;
                                                   @flag:--durable-receipts is a bare flag, VS-006)
      --cell 'cell clients=8 size=128 pipeline=32'  (repeatable)
      --cell 'mixed hv=8 rare_interval_us=1000 queue=64'
"""
import argparse, os, random, shlex, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--runs", type=int, default=7)
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--secs", type=int, default=3)
ap.add_argument("--warmup", type=int, default=1)
ap.add_argument("--bench", default=os.path.join(os.path.dirname(__file__), "..", "target", "release", "scada-bench"))
ap.add_argument("--variant", action="append", required=True)
ap.add_argument("--cell", action="append", required=True)
a = ap.parse_args()

variants = []
for v in a.variant:
    name, rest = v.split("=", 1)
    parts = rest.split(",")
    variants.append((name, parts[0], [p for p in parts[1:] if p]))

def cell_cmd(spec):
    kind, *kv = spec.split()
    d = dict(x.split("=", 1) for x in kv)
    args = [kind, "--secs", str(a.secs), "--warmup-secs", str(a.warmup)]
    if "queue" in d:
        args += ["--queue", d.pop("queue")]
    if kind == "cell":
        args += ["--clients", d["clients"], "--size", d["size"], "--pipeline", d["pipeline"]]
    elif kind == "mixed":
        args += ["--hv", d.pop("hv"), "--rare-interval-us", d.pop("rare_interval_us", "1000")]
        # VS-003 harness options pass through: rare_mode=open -> --rare-mode open, ...
        for k, v in d.items():
            args += ["--" + k.replace("_", "-"), v]
    else:
        sys.exit(f"unknown cell kind {kind}")
    return args

pairs = [(v, c) for v in variants for c in a.cell]
rng = random.Random(a.seed)
total = a.runs * len(pairs)
done = 0
t_start = time.time()
with open(a.out, "a") as out:
    out.write(f"# campaign start {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} runs={a.runs} seed={a.seed} secs={a.secs} warmup={a.warmup}\n")
    for v in variants:
        out.write(f"# variant {v[0]} kernel={v[1]} args={' '.join(v[2])}\n")
    for r in range(1, a.runs + 1):
        order = pairs[:]
        rng.shuffle(order)
        for (name, kernel, kargs), spec in order:
            cmd = [a.bench] + cell_cmd(spec) + ["--kernel", kernel, "--label", name]
            for k in kargs:
                if k.startswith("@ipc:"):
                    key, val = k[5:].split("=", 1)
                    cmd += ["--ipc-line", f"{key} = {val}"]
                elif k.startswith("@flag:"):
                    # VS-006: a scada-bench flag without value, e.g. @flag:--durable-receipts
                    cmd += [k[6:]]
                elif k.startswith("@bench:"):
                    # VS-004: a scada-bench option, e.g. @bench:--supervise=5000:12000:250
                    opt, val = k[7:].split("=", 1)
                    cmd += [opt, val]
                else:
                    cmd += ["--kernel-arg", k]
            p = subprocess.run(cmd, capture_output=True, text=True)
            line = next((l for l in p.stdout.splitlines() if l.startswith("kind=")), None)
            if p.returncode != 0 or line is None:
                out.write(f"# FAILED round={r} variant={name} cell={spec!r} rc={p.returncode} stderr={p.stderr[-500:]!r}\n")
            else:
                out.write(f"round={r} variant={name} spec={spec.replace(' ', '/')} {line}\n")
            out.flush()
            done += 1
            el = time.time() - t_start
            print(f"[{done}/{total}] {el:.0f}s eta {el / done * (total - done):.0f}s {name} {spec}", file=sys.stderr, flush=True)
    out.write(f"# campaign end {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")

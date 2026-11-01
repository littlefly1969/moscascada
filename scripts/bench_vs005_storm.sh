#!/usr/bin/env bash
# VS-005 process-level alarm storms (brief §10 "storm per 60 s", §11.2/§11.4/§11.5).
# Each line of results/vs005/storm.txt is one 60 s run with its own PASS/FAIL:
#   storm60=PASS  the outbox absorbed the whole measured window with zero refusals
#   safety=PASS   used <= C, exact accounting, and (sink stopped) event 1 still readable:
#                 nothing committed was overwritten; refusals explicit (CRITICAL)
# The sink "drain" reads pages and releases them with the TEST-ONLY simulated durable
# commit (--test-simulated-durable-sink): it is NOT a journal. "stopped" = no consumer,
# i.e. the VS-005 production reality (mu = 0).
set -euo pipefail
cd "$(dirname "$0")/.."
B=${BENCH:-target/release/scada-bench}
OUT=${OUT:-docs/benchmarks/results/vs005/storm.txt}
SECS=${SECS:-60}
run() { echo "# $*" >> "$OUT"; "$B" alarm-storm --secs "$SECS" --warmup-secs 2 "$@" 2>/dev/null | grep '^kind=' >> "$OUT"; tail -1 "$OUT"; }
echo "# vs005 storm campaign $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(uname -srm) secs=$SECS" >> "$OUT"
MT=(--kernel-arg --scheduler --kernel-arg multi_thread --kernel-arg --worker-threads --kernel-arg 2)
# S1: unthrottled storm, every batch flips 128 alarms, sink draining: kernel sustained rate.
run --label S1_ct_drain   --alarms 1:1048576:storm --clients 4 --size 128 --sink drain
run --label S1_mt2_drain  --alarms 1:1048576:storm --clients 4 --size 128 --sink drain "${MT[@]}"
run --label S1_ct_drain_8 --alarms 1:1048576:storm --clients 8 --size 128 --sink drain
# S1b: 4 rules per tag (512 events per batch).
run --label S1b_ct_drain_r4 --alarms 4:1048576:storm --clients 4 --size 128 --sink drain
# S2: plant-like storm ~1000 events/s (10 alarms per batch, 100 batches/s), sink STOPPED,
#     C = 65 536 >= 60 s x 1000: must absorb 60 s.
run --label S2_ct_stopped_fit   --alarms 1:65536:storm --clients 1 --size 10 --interval-us 10000 --sink stopped
# S3: same storm, C = 32 768 < 60 000 required: controlled saturation after ~30 s.
run --label S3_ct_stopped_short --alarms 1:32768:storm --clients 1 --size 10 --interval-us 10000 --sink stopped
# S4: unthrottled storm, sink stopped, C = 1 Mi: saturates within a second, then refusals.
run --label S4_ct_stopped_max   --alarms 1:1048576:storm --clients 4 --size 128 --sink stopped
run --label S4_mt2_stopped_max  --alarms 1:1048576:storm --clients 4 --size 128 --sink stopped "${MT[@]}"
# S5: slow consumer (pages of 16) against the unthrottled storm.
run --label S5_ct_slow_sink     --alarms 1:1048576:storm --clients 4 --size 128 --sink drain --sink-page 16
# S6: unthrottled storm the drain sink keeps up with (fewer producers): sustained rate with
#     zero refusals over 60 s = the level-A input of scripts/alarm_capacity.py.
run --label S6_ct_drain_2 --alarms 1:1048576:storm --clients 2 --size 128 --sink drain
run --label S6_ct_drain_1 --alarms 1:1048576:storm --clients 1 --size 128 --sink drain

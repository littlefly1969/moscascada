## VS-005 alarm engine in-memory (`scada-bench alarms-core --ops 200000`)

Timing: `Instant::now()` around each single `apply` (inputs built outside the timed region).
ops/s and events/s = count / sum of measured `apply` durations. allocs/op = counting allocator inside `apply`.

### A. Normal operation without trips (102 400 tags, Delta x 128 rotating over 800 sources)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| quiet rules=0 | 200000 | 237404 | 0 | 3.40 | 5.55 | 11.65 | 52.41 | 3994.74 | 0.000 | alarm storage 0 B, table 0 B, validate 27.9 ms |
| quiet rules=100 | 200000 | 255285 | 0 | 3.19 | 4.43 | 10.66 | 53.83 | 4146.81 | 0.000 | alarm storage 4792552 B, table 420334 B, validate 23.5 ms |
| quiet rules=1000 | 200000 | 241606 | 0 | 3.38 | 4.88 | 10.80 | 59.14 | 3473.12 | 0.000 | alarm storage 4822920 B, table 517534 B, validate 28.1 ms |
| quiet rules=100000 | 200000 | 250626 | 0 | 3.25 | 4.86 | 9.18 | 49.30 | 3080.11 | 0.000 | alarm storage 8003488 B, table 11209534 B, validate 54.7 ms |

### B. Trip/clear/retrigger storms (FullState x 128 alternating 1/0, ideal sink)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| storm events/op=1 | 200000 | 325865 | 325865 | 2.70 | 3.66 | 6.09 | 20.76 | 647.86 | 0.000 | trip+clear alternate |
| storm events/op=10 | 20000 | 261059 | 2610594 | 3.94 | 4.17 | 7.57 | 29.28 | 106.45 | 0.000 | trip+clear alternate |
| storm events/op=100 | 2000 | 202052 | 20205222 | 4.71 | 5.99 | 9.52 | 28.71 | 31.87 | 0.000 | trip+clear alternate |
| storm events/op=1000 | 2000 | 25343 | 25343003 | 34.65 | 55.96 | 72.96 | 332.35 | 1885.89 | 0.000 | trip+clear alternate |

### C. Bad-quality fan-out on disconnection / reconnection

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| disconnect fanout=128 | 1562 | 261135 | 33425266 | 3.46 | 4.75 | 7.74 | 32.05 | 54.10 | 0.000 | raises fanout events |
| reconnect FullState fanout=128 | 1562 | 156697 | 20057195 | 5.74 | 7.88 | 19.08 | 69.95 | 133.31 | 0.000 | returns fanout events |
| disconnect fanout=1024 | 500 | 28516 | 29200241 | 32.58 | 44.19 | 83.09 | 152.05 | 152.05 | 0.000 | raises fanout events |
| reconnect FullState fanout=1024 | 500 | 25645 | 26260033 | 37.11 | 50.02 | 59.98 | 92.92 | 92.92 | 0.000 | returns fanout events |
| disconnect fanout=8192 | 500 | 2868 | 23491467 | 335.11 | 412.78 | 619.96 | 2542.16 | 2542.16 | 0.000 | raises fanout events |
| reconnect FullState fanout=8192 | 500 | 2684 | 21989802 | 353.48 | 443.40 | 554.45 | 2743.32 | 2743.32 | 0.000 | returns fanout events |

### D. Outbox capacity C / C+1 (128 rules, C = 256, stopped sink)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| accept at free == required (C) | 1 | 171292 | 21925317 | 5.84 | 5.84 | 5.84 | 5.84 | 5.84 | 0.000 | 128 events into the last 128 slots |
| refuse at C+1 (outbox full) | 20000 | 199957 | 0 | 4.96 | 5.38 | 10.93 | 43.19 | 335.25 | 0.000 | OutboxCapacityExceeded, state unchanged |

### E. Acknowledgement (1 event per operation)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ack | 10000 | 8702607 | 8702607 | 0.11 | 0.12 | 0.26 | 0.45 | 31.90 | 0.000 | 1 000 alarms x 10 rounds |

### F. Cold start and alarm memory

| rules | outbox events | validate+build ms | Kernel::new ms | alarm storage bytes | table heap bytes | bytes/rule (storage+table, outbox excluded) |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 65536 | 0.1 | 0.52 | 3156200 | 11246 | 217.2 |
| 1000 | 65536 | 0.6 | 0.36 | 3200904 | 112030 | 167.2 |
| 100000 | 65536 | 73.6 | 8.76 | 7966624 | 11200318 | 160.2 |
| 1000000 | 1048576 | 493.1 | 54.54 | 90860528 | 110097086 | 150.6 |

### G. Supervision coupling: silence / freshness fan-out, entirely stale plant

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SourceSilenceExpired fanout=128 | 1562 | 297973 | 38140585 | 3.19 | 3.70 | 4.79 | 28.42 | 64.88 | 0.000 | raises fanout bad-quality events |
| FreshnessExpired fanout=128 | 1562 | 264254 | 33824533 | 3.61 | 4.15 | 6.84 | 36.11 | 40.77 | 0.000 | raises fanout events |
| FreshnessExpired stale plant rules=128 | 1562 | 26471884 | 0 | 0.03 | 0.06 | 0.13 | 0.24 | 0.32 | 0.000 | no candidate: Ignored, O(1) |
| SourceSilenceExpired fanout=8192 | 200 | 2575 | 21091717 | 363.55 | 482.07 | 513.66 | 1331.35 | 1331.35 | 0.000 | raises fanout bad-quality events |
| FreshnessExpired fanout=8192 | 200 | 2605 | 21338338 | 363.86 | 491.60 | 510.10 | 532.31 | 532.31 | 0.000 | raises fanout events |
| FreshnessExpired stale plant rules=8192 | 200 | 5319573 | 0 | 0.19 | 0.30 | 0.41 | 0.55 | 0.55 | 0.000 | no candidate: Ignored, O(1) |

Layouts: [("AlarmState", 32), ("AlarmEvent", 48), ("CompiledRule", 24), ("StagedAlarm", 48)]

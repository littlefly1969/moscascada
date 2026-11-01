## VS-005 alarm engine in-memory (`scada-bench alarms-core --ops 200000`)

Timing: `Instant::now()` around each single `apply` (inputs built outside the timed region).
ops/s and events/s = count / sum of measured `apply` durations. allocs/op = counting allocator inside `apply`.

### A. Normal operation without trips (102 400 tags, Delta x 128 rotating over 800 sources)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| quiet rules=0 | 200000 | 273441 | 0 | 3.00 | 3.93 | 8.92 | 52.98 | 2705.24 | 0.000 | alarm storage 0 B, table 0 B, validate 31.6 ms |
| quiet rules=100 | 200000 | 224723 | 0 | 3.65 | 5.27 | 12.52 | 55.52 | 2601.54 | 0.000 | alarm storage 3153752 B, table 420334 B, validate 28.3 ms |
| quiet rules=1000 | 200000 | 242730 | 0 | 3.54 | 4.75 | 10.49 | 48.14 | 2614.69 | 0.000 | alarm storage 3184008 B, table 517534 B, validate 21.4 ms |
| quiet rules=100000 | 200000 | 204231 | 0 | 4.13 | 6.15 | 13.71 | 51.38 | 2929.19 | 0.000 | alarm storage 6364576 B, table 11209534 B, validate 51.3 ms |

### B. Trip/clear/retrigger storms (FullState x 128 alternating 1/0, ideal sink)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| storm events/op=1 | 200000 | 282788 | 282788 | 3.19 | 4.65 | 6.20 | 25.07 | 2464.87 | 0.000 | trip+clear alternate |
| storm events/op=10 | 20000 | 264556 | 2645564 | 3.32 | 4.63 | 7.04 | 40.00 | 812.51 | 0.000 | trip+clear alternate |
| storm events/op=100 | 2000 | 179345 | 17934546 | 4.70 | 6.83 | 11.72 | 34.67 | 522.97 | 0.000 | trip+clear alternate |
| storm events/op=1000 | 2000 | 27044 | 27044305 | 35.29 | 48.30 | 71.27 | 115.65 | 504.68 | 0.000 | trip+clear alternate |

### C. Bad-quality fan-out on disconnection / reconnection

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| disconnect fanout=128 | 1562 | 338411 | 43316591 | 2.79 | 2.87 | 5.05 | 22.13 | 92.70 | 0.000 | raises fanout events |
| reconnect FullState fanout=128 | 1562 | 191584 | 24522712 | 5.09 | 5.18 | 7.76 | 21.90 | 21.92 | 0.000 | returns fanout events |
| disconnect fanout=1024 | 500 | 30998 | 31742217 | 30.52 | 45.85 | 55.24 | 108.17 | 108.17 | 0.000 | raises fanout events |
| reconnect FullState fanout=1024 | 500 | 27535 | 28195475 | 32.87 | 43.75 | 65.03 | 741.96 | 741.96 | 0.000 | returns fanout events |
| disconnect fanout=8192 | 500 | 2867 | 23489918 | 326.37 | 480.53 | 611.78 | 1364.17 | 1364.17 | 0.000 | raises fanout events |
| reconnect FullState fanout=8192 | 500 | 2821 | 23106433 | 328.46 | 508.02 | 675.17 | 1674.70 | 1674.70 | 0.000 | returns fanout events |

### D. Outbox capacity C / C+1 (128 rules, C = 256, stopped sink)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| accept at free == required (C) | 1 | 146606 | 18765577 | 6.82 | 6.82 | 6.82 | 6.82 | 6.82 | 0.000 | 128 events into the last 128 slots |
| refuse at C+1 (outbox full) | 20000 | 282653 | 0 | 2.99 | 4.54 | 5.71 | 33.51 | 66.45 | 0.000 | OutboxCapacityExceeded, state unchanged |

### E. Acknowledgement (1 event per operation)

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ack | 10000 | 10223590 | 10223590 | 0.08 | 0.10 | 0.23 | 0.48 | 44.17 | 0.000 | 1 000 alarms x 10 rounds |

### F. Cold start and alarm memory

| rules | outbox events | validate+build ms | Kernel::new ms | alarm storage bytes | table heap bytes | bytes/rule (storage+table, outbox excluded) |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 65536 | 0.1 | 0.38 | 3153752 | 11246 | 192.7 |
| 1000 | 65536 | 0.5 | 0.32 | 3184008 | 112030 | 150.3 |
| 100000 | 65536 | 53.9 | 5.95 | 6364576 | 11200318 | 144.2 |
| 1000000 | 1048576 | 516.7 | 54.13 | 82470896 | 110097086 | 142.2 |

### G. Supervision coupling: silence / freshness fan-out, entirely stale plant

| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SourceSilenceExpired fanout=128 | 1562 | 347549 | 44486275 | 2.76 | 2.87 | 3.71 | 26.12 | 29.65 | 0.000 | raises fanout bad-quality events |
| FreshnessExpired fanout=128 | 1562 | 308082 | 39434517 | 3.14 | 3.17 | 3.94 | 21.95 | 31.07 | 0.000 | raises fanout events |
| FreshnessExpired stale plant rules=128 | 1562 | 24818075 | 0 | 0.03 | 0.07 | 0.10 | 0.23 | 0.37 | 0.000 | no candidate: Ignored, O(1) |
| SourceSilenceExpired fanout=8192 | 200 | 2888 | 23660598 | 327.75 | 515.79 | 634.61 | 848.49 | 848.49 | 0.000 | raises fanout bad-quality events |
| FreshnessExpired fanout=8192 | 200 | 2839 | 23256480 | 330.65 | 501.59 | 747.04 | 1047.32 | 1047.32 | 0.000 | raises fanout events |
| FreshnessExpired stale plant rules=8192 | 200 | 5564675 | 0 | 0.14 | 0.46 | 0.54 | 0.70 | 0.70 | 0.000 | no candidate: Ignored, O(1) |

Layouts: [("AlarmState", 32), ("AlarmEvent", 48), ("CompiledRule", 24), ("StagedAlarm", 48)]

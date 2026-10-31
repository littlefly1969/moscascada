## Saturation (`scada-bench saturation --secs 60 --clients 16`)

Kernel `/home/claude/bins/f2/scada-kernel` [] started with `--throttle-sequencer-us 200` (artificial 200 µs per request in the sequencer
thread) so that 16 pipelined producers (32 in flight each, 128-observation batches) are faster than the consumer.
Queue capacity 64. RSS sampled every second.

memory bound (application allocations, not RSS): total=3836171 (connections=2558208 [106592 each], sequencer_queue=204360, kernel=224579, introspection=849024, diagnostics=0)
| t (s) | kernel RSS KiB | batches applied (cum.) | queue-full waits (cum.) |
|---:|---:|---:|---:|
| 1 | 8032 | 3053 | 3077 |
| 2 | 8032 | 6185 | 6210 |
| 3 | 8036 | 9271 | 9297 |
| 4 | 8036 | 12403 | 12430 |
| 5 | 8036 | 15581 | 15609 |
| 10 | 8040 | 30790 | 30823 |
| 20 | 8040 | 61246 | 61288 |
| 30 | 8040 | 91398 | 91449 |
| 40 | 8040 | 118354 | 118407 |
| 50 | 8040 | 148337 | 148400 |
| 59 | 8040 | 175504 | 175572 |
| 60 | 8040 | 177018 | 177009 |

- RSS idle 7908 KiB; under load min 8032 KiB, max 8040 KiB; VmHWM 12476 KiB.
- RSS second half of the run: min 8040 KiB, max 8040 KiB.
- Batches applied 177018, duplicates 0, rejected 0; ACKs counted by producers inside the window 176506.
- Queue-full waits (producer blocked on the full input queue): 177009.
- Theoretical consumer ceiling with the throttle: 5000 requests/s; observed 2950 batches/s.

SIGTERM: shutting down (volatile state is discarded)

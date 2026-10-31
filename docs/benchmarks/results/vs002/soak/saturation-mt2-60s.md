## Saturation (`scada-bench saturation --secs 60 --clients 16`)

Kernel `/home/claude/bins/f2/scada-kernel` ["--scheduler", "multi_thread", "--worker-threads", "2"] started with `--throttle-sequencer-us 200` (artificial 200 µs per request in the sequencer
thread) so that 16 pipelined producers (32 in flight each, 128-observation batches) are faster than the consumer.
Queue capacity 64. RSS sampled every second.

memory bound (application allocations, not RSS): total=3836171 (connections=2558208 [106592 each], sequencer_queue=204360, kernel=224579, introspection=849024, diagnostics=0)
| t (s) | kernel RSS KiB | batches applied (cum.) | queue-full waits (cum.) |
|---:|---:|---:|---:|
| 1 | 9160 | 3154 | 3180 |
| 2 | 9160 | 6310 | 6337 |
| 3 | 9164 | 9535 | 9563 |
| 4 | 9164 | 12790 | 12819 |
| 5 | 9164 | 16046 | 16076 |
| 10 | 9168 | 32112 | 32118 |
| 20 | 9176 | 63672 | 63655 |
| 30 | 9176 | 95751 | 95672 |
| 40 | 9176 | 123907 | 123825 |
| 50 | 9180 | 155368 | 155255 |
| 59 | 9180 | 183756 | 183651 |
| 60 | 9180 | 185703 | 185522 |

- RSS idle 7968 KiB; under load min 9160 KiB, max 9180 KiB; VmHWM 12644 KiB.
- RSS second half of the run: min 9176 KiB, max 9180 KiB.
- Batches applied 185703, duplicates 0, rejected 0; ACKs counted by producers inside the window 185191.
- Queue-full waits (producer blocked on the full input queue): 185522.
- Theoretical consumer ceiling with the throttle: 5000 requests/s; observed 3095 batches/s.

SIGTERM: shutting down (volatile state is discarded)

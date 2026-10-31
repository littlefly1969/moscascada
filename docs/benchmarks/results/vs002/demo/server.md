## profile server
```
memory bound (application allocations, not RSS): total=7029860 (connections=6821888 [106592 each], sequencer_queue=204360, kernel=3612, introspection=0, diagnostics=0)
READY instance=1d96c9b27c3cb6b41540540ce17ccfb3 socket=/tmp/claude-0/demo-server/driver.sock scheduler=multi_thread(2)
SIGTERM: shutting down (volatile state is discarded)
--- driver
EPOCH 7036441275282452190
WELCOME instance=1d96c9b27c3cb6b41540540ce17ccfb3 version=1.0 link_generation=1 full_state_required=true last_accepted=-
full_state: ACK seq=1 status=ACK_STATUS_APPLIED_VOLATILE op=2 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_BASELINE_ESTABLISHED last_accepted=1
delta: ACK seq=2 status=ACK_STATUS_APPLIED_VOLATILE op=3 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=2
delta: ACK seq=3 status=ACK_STATUS_APPLIED_VOLATILE op=4 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=3
duplicate: ACK seq=3 status=ACK_STATUS_DUPLICATE op=- error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_UNSPECIFIED last_accepted=3
gap: ACK seq=5 status=ACK_STATUS_REJECTED op=- error=ERROR_CODE_SEQUENCE_GAP continuity=CONTINUITY_UNSPECIFIED last_accepted=3
invalid: ACK seq=4 status=ACK_STATUS_REJECTED op=- error=ERROR_CODE_UNKNOWN_CHANNEL continuity=CONTINUITY_UNSPECIFIED last_accepted=3
delta: ACK seq=4 status=ACK_STATUS_APPLIED_VOLATILE op=5 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=4
DISCONNECTED
WELCOME instance=1d96c9b27c3cb6b41540540ce17ccfb3 version=1.0 link_generation=2 full_state_required=false last_accepted=4
retransmit: ACK seq=4 status=ACK_STATUS_DUPLICATE op=- error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_UNSPECIFIED last_accepted=4
delta: ACK seq=5 status=ACK_STATUS_APPLIED_VOLATILE op=8 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=5
DONE
```

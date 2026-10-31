## profile micro
```
memory bound (application allocations, not RSS): total=885220 (connections=828160 [103520 each], sequencer_queue=53448, kernel=3612, introspection=0, diagnostics=0)
READY instance=a0b9201e61f3d769bb947a03ed66484e socket=/tmp/claude-0/demo-micro/driver.sock scheduler=current_thread
SIGTERM: shutting down (volatile state is discarded)
--- driver
EPOCH 18146940466302606961
WELCOME instance=a0b9201e61f3d769bb947a03ed66484e version=1.0 link_generation=1 full_state_required=true last_accepted=-
full_state: ACK seq=1 status=ACK_STATUS_APPLIED_VOLATILE op=2 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_BASELINE_ESTABLISHED last_accepted=1
delta: ACK seq=2 status=ACK_STATUS_APPLIED_VOLATILE op=3 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=2
delta: ACK seq=3 status=ACK_STATUS_APPLIED_VOLATILE op=4 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=3
duplicate: ACK seq=3 status=ACK_STATUS_DUPLICATE op=- error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_UNSPECIFIED last_accepted=3
gap: ACK seq=5 status=ACK_STATUS_REJECTED op=- error=ERROR_CODE_SEQUENCE_GAP continuity=CONTINUITY_UNSPECIFIED last_accepted=3
invalid: ACK seq=4 status=ACK_STATUS_REJECTED op=- error=ERROR_CODE_UNKNOWN_CHANNEL continuity=CONTINUITY_UNSPECIFIED last_accepted=3
delta: ACK seq=4 status=ACK_STATUS_APPLIED_VOLATILE op=5 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=4
DISCONNECTED
WELCOME instance=a0b9201e61f3d769bb947a03ed66484e version=1.0 link_generation=2 full_state_required=false last_accepted=4
retransmit: ACK seq=4 status=ACK_STATUS_DUPLICATE op=- error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_UNSPECIFIED last_accepted=4
delta: ACK seq=5 status=ACK_STATUS_APPLIED_VOLATILE op=8 error=ERROR_CODE_UNSPECIFIED continuity=CONTINUITY_CONTINUOUS last_accepted=5
DONE
```

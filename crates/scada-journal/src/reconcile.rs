//! History continuity of a source (ADR 0013 D10, J8).
//!
//! [`legacy_continuity`] is the rule applied to every legacy (state-based) driver at its
//! first full state after a recovery fence: contiguity of batch numbers never proves that
//! the field did not change while the SCADA was down, so the best possible verdict is
//! `Unknown`; a sequence hole is a `Gap` with known bounds; a new epoch is `Unknown` with
//! unknown bounds.
//!
//! [`BufferedContinuity`] is the verifier for sources that replay a **buffered event
//! stream** (alarm panels, fire, intrusion): `Complete` only with proof (contiguous ranges
//! in the same epoch from a known durable position, no overflow). It is exercised by
//! component tests; the wire ingest of native buffered events is deferred (ADR 0013 D10).

use crate::{GapRecord, continuity, gap_reason};

/// Verdict at the first full state of a recovered source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    pub continuity: u8,
    pub gap: Option<GapRecord>,
}

/// `before`: `(epoch, last_accepted)` durable before the restart; `now`: epoch of the new
/// link and sequence of the full state that re-verified the source.
pub fn legacy_continuity(before: Option<(Option<u64>, Option<u64>)>, now_epoch: Option<u64>, now_seq: u64) -> Verdict {
    let Some((Some(epoch), last)) = before else {
        // Never connected before the restart: nothing was recorded, nothing is claimed.
        return Verdict { continuity: continuity::UNKNOWN, gap: None };
    };
    if now_epoch != Some(epoch) {
        return Verdict {
            continuity: continuity::UNKNOWN,
            gap: Some(GapRecord {
                source_epoch: now_epoch,
                from_seq: None,
                to_seq: None,
                bounds_unknown: true,
                reason: gap_reason::DRIVER_RESTART,
            }),
        };
    }
    match last {
        Some(l) if now_seq > l.saturating_add(1) => Verdict {
            continuity: continuity::GAP,
            gap: Some(GapRecord {
                source_epoch: Some(epoch),
                from_seq: Some(l.saturating_add(1)),
                to_seq: Some(now_seq.saturating_sub(1)),
                bounds_unknown: false,
                reason: gap_reason::SEQUENCE_GAP,
            }),
        },
        // Contiguous numbering (or no previous batch): still not a proof that the field
        // did not change while the SCADA was down.
        _ => Verdict {
            continuity: continuity::UNKNOWN,
            gap: Some(GapRecord {
                source_epoch: Some(epoch),
                from_seq: None,
                to_seq: None,
                bounds_unknown: true,
                reason: gap_reason::SCADA_DOWN,
            }),
        },
    }
}

/// Result of offering a range of buffered events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOutcome {
    /// New events `first..=last` accepted (possibly a suffix of the offered range).
    Accepted { first: u64, last: u64 },
    /// Every event already seen: idempotent, nothing to apply.
    Duplicate,
    /// A hole or an overflow: recorded; the new events are still accepted.
    Gap { gap: GapRecord, first: u64, last: u64 },
}

/// Verifier of a buffered stream (component; see module doc).
#[derive(Debug, Clone, Default)]
pub struct BufferedContinuity {
    epoch: Option<u64>,
    /// Next expected event sequence in `epoch`.
    next: Option<u64>,
    proven: bool,
    gaps: Vec<GapRecord>,
    pub duplicates: u64,
}

impl BufferedContinuity {
    /// Without a baseline nothing is proven (`Unknown`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Start from the durable position `(epoch, last event sequence)` recorded before the
    /// restart: contiguous ranges from here prove completeness.
    pub fn with_baseline(epoch: u64, last_seq: u64) -> Self {
        BufferedContinuity {
            epoch: Some(epoch),
            next: Some(last_seq.saturating_add(1)),
            proven: true,
            ..Self::default()
        }
    }

    pub fn continuity(&self) -> u8 {
        if !self.gaps.is_empty() {
            continuity::GAP
        } else if self.proven {
            continuity::COMPLETE
        } else {
            continuity::UNKNOWN
        }
    }

    pub fn gaps(&self) -> &[GapRecord] {
        &self.gaps
    }

    /// Offer events `first..=last` of `epoch`; `overflow` = the source reports it lost
    /// events from its buffer.
    pub fn offer(&mut self, epoch: u64, first: u64, last: u64, overflow: bool) -> RangeOutcome {
        if first > last {
            return RangeOutcome::Duplicate;
        }
        if self.epoch != Some(epoch) {
            // New epoch: what happened between the epochs is unknown.
            let had_epoch = self.epoch.is_some();
            self.epoch = Some(epoch);
            self.next = Some(last.saturating_add(1));
            if had_epoch || overflow || first != 1 {
                let gap = GapRecord {
                    source_epoch: Some(epoch),
                    from_seq: None,
                    to_seq: if first > 1 { Some(first - 1) } else { None },
                    bounds_unknown: true,
                    reason: if overflow { gap_reason::BUFFER_GAP } else { gap_reason::DRIVER_RESTART },
                };
                self.gaps.push(gap);
                return RangeOutcome::Gap { gap, first, last };
            }
            // First epoch seen from its very first event, but no durable baseline: still
            // not proven complete (events before this process started are unknown).
            return RangeOutcome::Accepted { first, last };
        }
        let next = self.next.unwrap_or(1);
        if last < next {
            self.duplicates += 1;
            return RangeOutcome::Duplicate;
        }
        let start = first.max(next);
        self.next = Some(last.saturating_add(1));
        if first > next || overflow {
            let gap = GapRecord {
                source_epoch: Some(epoch),
                from_seq: Some(next),
                to_seq: if first > next { Some(first - 1) } else { None },
                bounds_unknown: overflow,
                reason: if overflow { gap_reason::BUFFER_GAP } else { gap_reason::SEQUENCE_GAP },
            };
            self.gaps.push(gap);
            return RangeOutcome::Gap { gap, first: start, last };
        }
        RangeOutcome::Accepted { first: start, last }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_rules() {
        // Same epoch, contiguous: Unknown (ScadaDown), never Complete.
        let v = legacy_continuity(Some((Some(1), Some(10))), Some(1), 11);
        assert_eq!(v.continuity, continuity::UNKNOWN);
        assert_eq!(v.gap.map(|g| g.reason), Some(gap_reason::SCADA_DOWN));
        // Same epoch, hole: Gap with bounds.
        let v = legacy_continuity(Some((Some(1), Some(10))), Some(1), 15);
        assert_eq!(v.continuity, continuity::GAP);
        let g = v.gap.unwrap();
        assert_eq!((g.from_seq, g.to_seq, g.bounds_unknown), (Some(11), Some(14), false));
        // New epoch: Unknown, bounds unknown.
        let v = legacy_continuity(Some((Some(1), Some(10))), Some(2), 1);
        assert_eq!(v.continuity, continuity::UNKNOWN);
        assert!(v.gap.unwrap().bounds_unknown);
        // Never connected before.
        assert_eq!(legacy_continuity(None, Some(1), 1).continuity, continuity::UNKNOWN);
    }

    /// T14: buffered replay continuity.
    #[test]
    fn buffered_complete_only_with_proof() {
        let mut b = BufferedContinuity::with_baseline(7, 100);
        assert_eq!(b.offer(7, 101, 150, false), RangeOutcome::Accepted { first: 101, last: 150 });
        // Overlapping retry: only the new suffix.
        assert_eq!(b.offer(7, 140, 160, false), RangeOutcome::Accepted { first: 151, last: 160 });
        // Pure duplicate: idempotent.
        assert_eq!(b.offer(7, 101, 160, false), RangeOutcome::Duplicate);
        assert_eq!(b.continuity(), continuity::COMPLETE);
        // Hole.
        assert!(matches!(b.offer(7, 170, 180, false), RangeOutcome::Gap { .. }));
        assert_eq!(b.continuity(), continuity::GAP);
        // A later contiguous range does not erase the gap.
        b.offer(7, 181, 190, false);
        assert_eq!(b.continuity(), continuity::GAP);
        assert_eq!(b.gaps().len(), 1);
    }

    #[test]
    fn buffered_without_baseline_is_unknown_and_overflow_is_gap() {
        let mut b = BufferedContinuity::new();
        b.offer(1, 1, 10, false);
        assert_eq!(b.continuity(), continuity::UNKNOWN);
        let mut b = BufferedContinuity::with_baseline(1, 10);
        assert!(matches!(b.offer(1, 11, 20, true), RangeOutcome::Gap { .. }));
        assert_eq!(b.continuity(), continuity::GAP);
        let mut b = BufferedContinuity::with_baseline(1, 10);
        assert!(matches!(b.offer(2, 1, 5, false), RangeOutcome::Gap { .. }));
        assert_eq!(b.continuity(), continuity::GAP);
    }
}

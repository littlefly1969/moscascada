//! VS-007 T12 (stable part): property tests of the codec, the planner and the value decoder.
//! The libFuzzer targets in `fuzz/` (nightly) cover arbitrary bytes; these run on every
//! `cargo test`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use proptest::prelude::*;
use scada_modbus::Area;
use scada_modbus::mbap::{self, AduDecoder};
use scada_modbus::pdu::{self, ReadData, ReadRequest};
use scada_modbus::plan::{self, Block, PointSpan};
use scada_modbus::value::{DecodeError, Rounding, Scale, scale_integer};

fn area() -> impl Strategy<Value = Area> {
    (0usize..4).prop_map(|i| Area::ALL[i])
}

fn request() -> impl Strategy<Value = ReadRequest> {
    (area(), any::<u16>(), any::<u16>()).prop_filter_map("valid request", |(a, addr, q)| {
        let q = q % a.max_quantity() + 1;
        ReadRequest::new(a, addr, q).ok()
    })
}

fn data_for(req: &ReadRequest, seed: &[u16]) -> ReadData {
    let n = usize::from(req.quantity());
    if req.area().is_bit() {
        ReadData::Bits((0..n).map(|i| seed[i % seed.len()] & (1 << (i % 16)) != 0).collect())
    } else {
        ReadData::Registers((0..n).map(|i| seed[i % seed.len()].wrapping_add(i as u16)).collect())
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// A response encoded for a request decodes back to exactly the same data.
    #[test]
    fn response_round_trip(req in request(), seed in prop::collection::vec(any::<u16>(), 1..8)) {
        let data = data_for(&req, &seed);
        let mut pdu_bytes = Vec::new();
        pdu::encode_response(&req, &data, &mut pdu_bytes);
        prop_assert!(pdu_bytes.len() <= scada_modbus::MAX_PDU_BYTES);
        prop_assert_eq!(pdu::decode_response(&req, &pdu_bytes), Ok(data));
        prop_assert_eq!(ReadRequest::decode(&req.encode()), Ok(req));
    }

    /// Any split of a stream of valid ADUs yields the same frames, in order.
    #[test]
    fn mbap_any_split(
        frames in prop::collection::vec((any::<u16>(), any::<u8>(), request()), 1..6),
        cuts in prop::collection::vec(1usize..40, 1..64),
    ) {
        let mut stream = Vec::new();
        let mut expected = Vec::new();
        for (tid, unit, req) in &frames {
            let data = data_for(req, &[0x5a5a, 0x1234]);
            let mut p = Vec::new();
            pdu::encode_response(req, &data, &mut p);
            let mut adu = Vec::new();
            mbap::encode_adu(*tid, *unit, &p, &mut adu); // clears its output
            stream.extend_from_slice(&adu);
            expected.push((*tid, *unit, p));
        }
        let mut d = AduDecoder::new();
        let mut got = Vec::new();
        let mut rest = stream.as_slice();
        let mut k = 0;
        while !rest.is_empty() {
            let take = cuts[k % cuts.len()].min(rest.len());
            k += 1;
            let mut chunk = &rest[..take];
            rest = &rest[take..];
            while !chunk.is_empty() {
                let n = d.push(chunk);
                chunk = &chunk[n..];
                prop_assert!(d.buffered() <= mbap::MAX_ADU_BYTES);
                while let Some(f) = d.next_frame().unwrap() {
                    got.push((f.header.transaction_id, f.header.unit_id, f.pdu));
                }
            }
        }
        prop_assert_eq!(got, expected);
        prop_assert_eq!(d.buffered(), 0);
    }

    /// The plan never reads outside the declared blocks, respects the per-request maximum,
    /// and every point is located on its own items.
    #[test]
    fn plan_reads_only_declared_blocks(
        blk in prop::collection::vec((area(), 0u16..2000, 1u16..400, 1u16..130), 1..5),
        pts in prop::collection::vec((0usize..5, 0u16..400, 1u16..3), 1..40),
    ) {
        // Lay blocks out without overlap: each block starts after the previous one.
        let mut blocks = Vec::new();
        let mut next = 0u32;
        for (a, gap, count, maxr) in blk {
            let start = next + u32::from(gap);
            if start + u32::from(count) > 65_536 { break; }
            // >= 2 for registers: a 32-bit point is never split across two requests.
            let maxr = maxr.min(a.max_quantity()).max(if a.is_bit() { 1 } else { 2 });
            blocks.push(Block { area: a, start: start as u16, count, max_per_request: maxr });
            next = start + u32::from(count);
        }
        prop_assume!(!blocks.is_empty());
        let spans: Vec<PointSpan> = pts
            .iter()
            .filter_map(|&(bi, off, w)| {
                let b = blocks[bi % blocks.len()];
                let w = if b.area.is_bit() { 1 } else { w };
                (off + w <= b.count).then(|| PointSpan { area: b.area, address: b.start + off, width: w })
            })
            .collect();
        prop_assume!(!spans.is_empty());
        let p = plan::build(&blocks, &spans, usize::MAX).unwrap();
        prop_assert_eq!(p.locations.len(), spans.len());
        for r in &p.requests {
            let inside = blocks.iter().any(|b| {
                b.area == r.area()
                    && r.address() >= b.start
                    && u32::from(r.address()) + u32::from(r.quantity()) <= b.end()
                    && r.quantity() <= b.max_per_request
            });
            prop_assert!(inside, "request {:?} outside the declared blocks", r);
        }
        for (s, l) in spans.iter().zip(&p.locations) {
            let r = &p.requests[l.request];
            prop_assert_eq!(r.area(), s.area);
            prop_assert_eq!(usize::from(r.address()) + l.offset, usize::from(s.address));
            prop_assert!(l.offset + usize::from(s.width) <= usize::from(r.quantity()));
        }
        // A limit below the need is refused, never truncated.
        if p.requests.len() > 1 {
            let lim = p.requests.len() - 1;
            prop_assert!(
                matches!(plan::build(&blocks, &spans, lim), Err(plan::PlanError::TooManyRequests { .. })),
                "a limit below the need must be refused"
            );
        }
    }

    /// Integer scaling: compared with the exact rational value raw*num/den.
    #[test]
    fn scale_integer_matches_rational(
        raw in -(1i64 << 40)..(1i64 << 40),
        num in -100_000i64..100_000,
        den in 1i64..100_000,
        off in -1_000_000i64..1_000_000,
        rmode in 0u8..3,
    ) {
        let rounding = [Rounding::Exact, Rounding::HalfEven, Rounding::Truncate][usize::from(rmode)];
        let s = Scale { numerator: num, denominator: den, offset: off, rounding };
        let n = i128::from(raw) * i128::from(num);
        let d = i128::from(den);
        match scale_integer(raw, &s) {
            Ok(v) => {
                let q = i128::from(v) - i128::from(off);
                let err = q * d - n; // d * (q - n/d)
                match rounding {
                    Rounding::Exact => prop_assert_eq!(err, 0),
                    Rounding::Truncate => {
                        prop_assert!(err.abs() < d);
                        prop_assert!(q.abs() * d <= n.abs(), "truncate goes towards zero");
                    }
                    Rounding::HalfEven => {
                        prop_assert!(2 * err.abs() <= d);
                        if 2 * err.abs() == d { prop_assert_eq!(q % 2, 0, "ties go to even"); }
                    }
                }
            }
            Err(DecodeError::Inexact) => {
                prop_assert_eq!(rounding, Rounding::Exact);
                prop_assert_ne!(n % d, 0);
            }
            Err(e) => prop_assert!(false, "unexpected {e:?} for bounded inputs"),
        }
    }
}

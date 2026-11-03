//! Read planning: turn the points of a product profile into a bounded, deterministic list
//! of read requests that stay inside the **declared readable blocks** of the device.
//!
//! A block is a range the product documentation declares readable in one area. The planner
//! never reads outside a block and never merges two blocks (a gap between blocks may be an
//! unreadable range that answers ILLEGAL DATA ADDRESS). Inside a block it reads the range
//! covering the used points and splits it into requests of at most
//! `min(block.max_per_request, 125 | 2000)` items. The order of the requests is
//! `(area, address)` and does not depend on hashing or on the order of the input.

use std::fmt;

use crate::Area;
use crate::pdu::{ReadRequest, RequestError};

/// A readable range declared by a product profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Block {
    pub area: Area,
    pub start: u16,
    /// Items (bits or registers), >= 1.
    pub count: u16,
    /// Largest quantity per request for this block (<= the area maximum). Some devices
    /// accept less than the protocol maximum.
    pub max_per_request: u16,
}

impl Block {
    /// Exclusive end address (as u32: may be 65536).
    pub fn end(&self) -> u32 {
        u32::from(self.start) + u32::from(self.count)
    }

    fn contains(&self, area: Area, address: u16, width: u16) -> bool {
        self.area == area
            && u32::from(address) >= u32::from(self.start)
            && u32::from(address) + u32::from(width) <= self.end()
    }
}

/// One item range a point needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointSpan {
    pub area: Area,
    pub address: u16,
    /// 1 or 2 items.
    pub width: u16,
}

/// Where a point's items are in the scan results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// Index into [`Plan::requests`].
    pub request: usize,
    /// Offset of the first item inside that request's data.
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub requests: Vec<ReadRequest>,
    /// One location per input point, in input order.
    pub locations: Vec<Location>,
}

impl Plan {
    /// Total items read per scan.
    pub fn items(&self) -> u64 {
        self.requests.iter().map(|r| u64::from(r.quantity())).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    BlockEmpty(usize),
    BlockOverflow(usize),
    BlockMaxPerRequest(usize),
    /// Two blocks of the same area overlap (ambiguous documentation).
    BlocksOverlap(usize, usize),
    /// A point is not entirely inside one declared block.
    PointOutsideBlocks(usize),
    PointWidth(usize),
    /// More requests than the configured limit.
    TooManyRequests {
        needed: usize,
        max: usize,
    },
    Request(RequestError),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::BlockEmpty(i) => write!(f, "block #{i}: count must be >= 1"),
            PlanError::BlockOverflow(i) => write!(f, "block #{i}: start + count exceeds 65536"),
            PlanError::BlockMaxPerRequest(i) => {
                write!(f, "block #{i}: max_per_request must be 1..=125 (registers) or 1..=2000 (bits)")
            }
            PlanError::BlocksOverlap(a, b) => write!(f, "blocks #{a} and #{b} overlap"),
            PlanError::PointOutsideBlocks(i) => write!(f, "point #{i} is not inside a declared readable block"),
            PlanError::PointWidth(i) => write!(f, "point #{i}: width must be 1 or 2"),
            PlanError::TooManyRequests { needed, max } => {
                write!(f, "scan needs {needed} requests, limit {max}")
            }
            PlanError::Request(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// Validate the blocks of a profile (independently of any point).
pub fn validate_blocks(blocks: &[Block]) -> Result<(), PlanError> {
    for (i, b) in blocks.iter().enumerate() {
        if b.count == 0 {
            return Err(PlanError::BlockEmpty(i));
        }
        if b.end() > 65_536 {
            return Err(PlanError::BlockOverflow(i));
        }
        if b.max_per_request == 0 || b.max_per_request > b.area.max_quantity() {
            return Err(PlanError::BlockMaxPerRequest(i));
        }
        for (j, c) in blocks.iter().enumerate().skip(i + 1) {
            if b.area == c.area && u32::from(b.start) < c.end() && u32::from(c.start) < b.end() {
                return Err(PlanError::BlocksOverlap(i, j));
            }
        }
    }
    Ok(())
}

/// Build the plan. `max_requests` bounds the work of one scan.
pub fn build(blocks: &[Block], points: &[PointSpan], max_requests: usize) -> Result<Plan, PlanError> {
    validate_blocks(blocks)?;
    // Block of each point.
    let mut owner = Vec::with_capacity(points.len());
    for (i, p) in points.iter().enumerate() {
        if p.width == 0 || p.width > 2 {
            return Err(PlanError::PointWidth(i));
        }
        let b = blocks
            .iter()
            .position(|b| b.contains(p.area, p.address, p.width))
            .ok_or(PlanError::PointOutsideBlocks(i))?;
        owner.push(b);
    }
    // Blocks in deterministic (area, start) order; only blocks with points are read.
    let mut order: Vec<usize> = (0..blocks.len()).filter(|b| owner.contains(b)).collect();
    order.sort_by_key(|&b| blocks.get(b).map(|x| (x.area, x.start)));
    let mut requests = Vec::new();
    // (block, first address, quantity, request index) for locating points.
    let mut spans: Vec<(usize, u32, u32, usize)> = Vec::new();
    for b in order {
        let Some(block) = blocks.get(b) else { continue };
        let used: Vec<(u32, u32)> = points
            .iter()
            .zip(&owner)
            .filter(|(_, o)| **o == b)
            .map(|(p, _)| (u32::from(p.address), u32::from(p.address) + u32::from(p.width)))
            .collect();
        let step = u32::from(block.max_per_request);
        // Each request starts at a used item and ends at the last used item that fits in
        // the window: unused tails are not read, gaps inside a window are (the block is
        // declared readable), a 32-bit point is never split.
        let mut next = used.iter().map(|u| u.0).min();
        while let Some(a) = next {
            let window_end = (a + step).min(block.end());
            let end = used.iter().filter(|(s, e)| *s >= a && *e <= window_end).map(|u| u.1).max();
            let Some(end) = end else {
                // Only possible with max_per_request == 1 and a 2-item point.
                return Err(PlanError::BlockMaxPerRequest(b));
            };
            let addr = u16::try_from(a).map_err(|_| PlanError::BlockOverflow(b))?;
            let qty = u16::try_from(end - a).map_err(|_| PlanError::BlockMaxPerRequest(b))?;
            let req = ReadRequest::new(block.area, addr, qty).map_err(PlanError::Request)?;
            spans.push((b, a, end - a, requests.len()));
            requests.push(req);
            if requests.len() > max_requests {
                return Err(PlanError::TooManyRequests { needed: requests.len(), max: max_requests });
            }
            next = used.iter().map(|u| u.0).filter(|s| *s >= end).min();
        }
    }
    let mut locations = Vec::with_capacity(points.len());
    for (i, (p, o)) in points.iter().zip(&owner).enumerate() {
        let addr = u32::from(p.address);
        let (_, first, _, req) = spans
            .iter()
            .find(|(b, first, q, _)| b == o && addr >= *first && addr + u32::from(p.width) <= first + q)
            .copied()
            .ok_or(PlanError::PointOutsideBlocks(i))?;
        locations.push(Location { request: req, offset: (addr - first) as usize });
    }
    Ok(Plan { requests, locations })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blk(area: Area, start: u16, count: u16, max: u16) -> Block {
        Block { area, start, count, max_per_request: max }
    }

    fn pt(area: Area, address: u16, width: u16) -> PointSpan {
        PointSpan { area, address, width }
    }

    #[test]
    fn groups_contiguous_points_inside_blocks_only() {
        let blocks = [blk(Area::HoldingRegister, 0, 10, 125), blk(Area::HoldingRegister, 20, 10, 125)];
        let points =
            [pt(Area::HoldingRegister, 2, 1), pt(Area::HoldingRegister, 5, 2), pt(Area::HoldingRegister, 21, 1)];
        let p = build(&blocks, &points, 16).unwrap();
        // Never one request 2..22 across the gap 10..20.
        assert_eq!(
            p.requests,
            vec![
                ReadRequest::new(Area::HoldingRegister, 2, 5).unwrap(),
                ReadRequest::new(Area::HoldingRegister, 21, 1).unwrap()
            ]
        );
        assert_eq!(
            p.locations,
            vec![
                Location { request: 0, offset: 0 },
                Location { request: 0, offset: 3 },
                Location { request: 1, offset: 0 }
            ]
        );
    }

    #[test]
    fn splits_at_limit_without_cutting_32bit_points_or_reading_unused_tails() {
        let blocks = [blk(Area::InputRegister, 0, 300, 125)];
        let points = [pt(Area::InputRegister, 0, 1), pt(Area::InputRegister, 124, 2), pt(Area::InputRegister, 299, 1)];
        let p = build(&blocks, &points, 16).unwrap();
        // 0..1 (the 32-bit point at 124 does not fit in the 0..125 window), 124..126, 299.
        assert_eq!(p.requests[0], ReadRequest::new(Area::InputRegister, 0, 1).unwrap());
        assert_eq!(p.requests[1], ReadRequest::new(Area::InputRegister, 124, 2).unwrap());
        assert_eq!(p.requests[2], ReadRequest::new(Area::InputRegister, 299, 1).unwrap());
        assert_eq!(p.locations[1], Location { request: 1, offset: 0 });
        assert_eq!(p.locations[2], Location { request: 2, offset: 0 });
        assert_eq!(p.items(), 4);
        // A dense block is read in maximal requests.
        let dense: Vec<_> = (0..300).map(|a| pt(Area::InputRegister, a, 1)).collect();
        let p = build(&blocks, &dense, 16).unwrap();
        let q: Vec<u16> = p.requests.iter().map(|r| r.quantity()).collect();
        assert_eq!(q, vec![125, 125, 50]);
        assert_eq!(p.locations[299], Location { request: 2, offset: 49 });
    }

    #[test]
    fn deterministic_order_independent_of_input_order() {
        let blocks = [blk(Area::InputRegister, 0, 4, 125), blk(Area::Coil, 0, 16, 2000)];
        let a = [pt(Area::InputRegister, 1, 1), pt(Area::Coil, 3, 1)];
        let b = [pt(Area::Coil, 3, 1), pt(Area::InputRegister, 1, 1)];
        let pa = build(&blocks, &a, 4).unwrap();
        let pb = build(&blocks, &b, 4).unwrap();
        assert_eq!(pa.requests, pb.requests);
        assert_eq!(pa.requests[0].area(), Area::Coil);
    }

    #[test]
    fn rejects_bad_profiles() {
        assert_eq!(validate_blocks(&[blk(Area::Coil, 0, 0, 1)]), Err(PlanError::BlockEmpty(0)));
        assert_eq!(validate_blocks(&[blk(Area::Coil, 65_535, 2, 1)]), Err(PlanError::BlockOverflow(0)));
        assert_eq!(validate_blocks(&[blk(Area::HoldingRegister, 0, 1, 126)]), Err(PlanError::BlockMaxPerRequest(0)));
        assert_eq!(
            validate_blocks(&[blk(Area::HoldingRegister, 0, 10, 10), blk(Area::HoldingRegister, 9, 2, 2)]),
            Err(PlanError::BlocksOverlap(0, 1))
        );
        // Same addresses in different areas do not overlap.
        assert!(validate_blocks(&[blk(Area::HoldingRegister, 0, 10, 10), blk(Area::InputRegister, 0, 10, 10)]).is_ok());
        let blocks = [blk(Area::HoldingRegister, 0, 10, 125)];
        assert_eq!(build(&blocks, &[pt(Area::HoldingRegister, 9, 2)], 4), Err(PlanError::PointOutsideBlocks(0)));
        assert_eq!(build(&blocks, &[pt(Area::InputRegister, 0, 1)], 4), Err(PlanError::PointOutsideBlocks(0)));
        let many = [blk(Area::HoldingRegister, 0, 100, 1)];
        let pts: Vec<_> = (0..10).map(|a| pt(Area::HoldingRegister, a * 10, 1)).collect();
        assert_eq!(build(&many, &pts, 4), Err(PlanError::TooManyRequests { needed: 5, max: 4 }));
        assert_eq!(build(&many, &[pt(Area::HoldingRegister, 0, 2)], 4), Err(PlanError::BlockMaxPerRequest(0)));
    }
}

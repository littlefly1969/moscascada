//! VS-007 T12: product profile and driver configuration parsing on arbitrary text.
//! Properties: no panic; an accepted profile only has points inside its blocks and a read
//! plan within its request limit.
#![no_main]

use std::path::Path;

use libfuzzer_sys::fuzz_target;
use scada_driver_modbus_tcp::catalog;
use scada_modbus::plan::{self, PointSpan};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    if let Ok(p) = catalog::parse_profile(text, Path::new("fuzz.toml")) {
        let spans: Vec<PointSpan> = p
            .points
            .values()
            .map(|x| PointSpan { area: x.area, address: x.address, width: x.decoding.raw.width() })
            .collect();
        for s in &spans {
            plan::build(&p.blocks, std::slice::from_ref(s), usize::MAX).expect("accepted point outside blocks");
        }
        if let Ok(pl) = plan::build(&p.blocks, &spans, p.max_requests_per_scan as usize) {
            assert!(pl.requests.len() <= p.max_requests_per_scan as usize);
            assert_eq!(pl.locations.len(), spans.len());
        }
    }
    let _ = scada_driver_modbus_tcp::config::parse(text, Path::new("."));
});

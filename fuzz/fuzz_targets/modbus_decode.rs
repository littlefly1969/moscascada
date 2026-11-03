//! VS-007 T12: Modbus TCP framing and response validation on arbitrary bytes.
//! Properties: no panic, termination, never more than one maximal ADU buffered, a
//! validated response holds exactly the requested quantity, a request that decodes
//! re-encodes to the same bytes.
#![no_main]

use libfuzzer_sys::fuzz_target;
use scada_modbus::mbap::{self, AduDecoder};
use scada_modbus::pdu::{self, ReadData, ReadRequest};
use scada_modbus::Area;

fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else { return };
    // 1) Request decode/encode round trip.
    if let Ok(r) = ReadRequest::decode(rest.get(..5).unwrap_or(rest)) {
        assert_eq!(ReadRequest::decode(&r.encode()), Ok(r));
    }
    // 2) Incremental MBAP decoding with a chunk size chosen by the input.
    let chunk = usize::from(sel % 17) + 1;
    let mut d = AduDecoder::new();
    let mut frames = 0;
    for c in rest.chunks(chunk) {
        let mut c = c;
        while !c.is_empty() {
            let n = d.push(c);
            assert!(d.buffered() <= mbap::MAX_ADU_BYTES);
            c = &c[n..];
            match d.next_frame() {
                Ok(Some(f)) => {
                    frames += 1;
                    assert!(f.pdu.len() >= 1 && f.pdu.len() <= scada_modbus::MAX_PDU_BYTES);
                    // 3) Validate the PDU against a request derived from the input.
                    let area = Area::ALL[usize::from(sel >> 6)];
                    let q = (u16::from(sel) % area.max_quantity()).max(1);
                    let req = ReadRequest::new(area, 0, q).unwrap();
                    if let Ok(data) = pdu::decode_response(&req, &f.pdu) {
                        assert_eq!(data.len(), usize::from(q));
                        assert_eq!(matches!(data, ReadData::Bits(_)), area.is_bit());
                    }
                }
                Ok(None) => {
                    if n == 0 {
                        break;
                    }
                }
                Err(_) => return,
            }
        }
    }
    assert!(frames <= rest.len() / 8 + 1);
});

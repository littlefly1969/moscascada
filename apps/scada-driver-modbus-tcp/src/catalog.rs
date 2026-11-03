//! Product profile catalog (schema `moscada.driver-catalog/1`, ADR 0015 D11).
//!
//! A profile describes **one product version** for one transport and protocol: the readable
//! blocks of the device and its points (area, zero-based PDU address, raw type, scale). It
//! holds no MoScada identity: `source_id` and `channel_id` belong to the driver
//! configuration. Adding a product = adding a file; nothing is recompiled.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use scada_modbus::Area;
use scada_modbus::plan::{self, Block};
use scada_modbus::value::{Decoding, RawType, Rounding, Scale, WordOrder};
use serde::Deserialize;

pub const CATALOG_SCHEMA: &str = "moscada.driver-catalog/1";
/// Upper bound of `max_requests_per_scan`.
pub const MAX_REQUESTS_PER_SCAN_CEILING: u32 = 256;
pub const MAX_POINTS_PER_PROFILE: usize = 4096;
pub const MAX_BLOCKS_PER_PROFILE: usize = 256;
/// Largest catalog file read (bounded parsing).
pub const MAX_CATALOG_FILE_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogError(pub String);

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CatalogError {}

fn err<T>(s: impl Into<String>) -> Result<T, CatalogError> {
    Err(CatalogError(s.into()))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileProfile {
    schema: String,
    profile: FileMeta,
    #[serde(default)]
    blocks: Vec<FileBlock>,
    #[serde(default)]
    points: Vec<FilePoint>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileMeta {
    id: String,
    version: u32,
    title: String,
    manufacturer: String,
    transport: String,
    protocol: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    max_requests_per_scan: Option<u32>,
    /// Free-form provenance of the register map (document, revision). Required so that a
    /// map is never presented without saying where it comes from.
    source_document: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileBlock {
    area: String,
    start: u16,
    count: u16,
    #[serde(default)]
    max_per_request: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePoint {
    name: String,
    area: String,
    address: u16,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    bit: Option<u8>,
    #[serde(default)]
    word_order: Option<String>,
    #[serde(default)]
    byte_swap: Option<bool>,
    #[serde(default)]
    scale_numerator: Option<i64>,
    #[serde(default)]
    scale_denominator: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
    #[serde(default)]
    rounding: Option<String>,
    #[serde(default)]
    valid_min: Option<i64>,
    #[serde(default)]
    valid_max: Option<i64>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// One validated point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Point {
    pub name: String,
    pub area: Area,
    pub address: u16,
    pub decoding: Decoding,
    pub unit: Option<String>,
}

impl Point {
    /// MoScada type of the decoded value.
    pub fn is_bool(&self) -> bool {
        self.decoding.raw.is_bool()
    }
}

/// One validated product profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub id: String,
    pub version: u32,
    pub title: String,
    pub manufacturer: String,
    pub source_document: String,
    pub blocks: Vec<Block>,
    /// Sorted by name.
    pub points: BTreeMap<String, Point>,
    pub max_requests_per_scan: u32,
    pub file: PathBuf,
}

/// Every profile, keyed by `(id, version)`.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub profiles: BTreeMap<(String, u32), Profile>,
}

pub fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        && s.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
}

fn area(s: &str, ctx: &str) -> Result<Area, CatalogError> {
    Area::from_name(s).ok_or_else(|| {
        CatalogError(format!("{ctx}: area \"{s}\" (coil | discrete_input | holding_register | input_register)"))
    })
}

/// Parse and validate one profile document.
pub fn parse_profile(text: &str, file: &Path) -> Result<Profile, CatalogError> {
    let ctx = file.display().to_string();
    let f: FileProfile = toml::from_str(text).map_err(|e| CatalogError(format!("{ctx}: {e}")))?;
    if f.schema != CATALOG_SCHEMA {
        return err(format!("{ctx}: schema \"{}\" (expected \"{CATALOG_SCHEMA}\")", f.schema));
    }
    let m = &f.profile;
    if !valid_name(&m.id) {
        return err(format!("{ctx}: profile id \"{}\" must match [a-z][a-z0-9_-]{{0,63}}", m.id));
    }
    if m.version == 0 {
        return err(format!("{ctx}: profile version must be >= 1"));
    }
    if m.transport != "tcp" {
        return err(format!("{ctx}: transport \"{}\" (only \"tcp\" in VS-007)", m.transport));
    }
    if m.protocol != "modbus" {
        return err(format!("{ctx}: protocol \"{}\" (only \"modbus\" in VS-007)", m.protocol));
    }
    for (k, v) in [("title", &m.title), ("manufacturer", &m.manufacturer), ("source_document", &m.source_document)] {
        if v.trim().is_empty() || v.len() > 256 {
            return err(format!("{ctx}: profile.{k} must be 1..=256 bytes"));
        }
    }
    let _ = &m.description;
    let max_req = m.max_requests_per_scan.unwrap_or(32);
    if max_req == 0 || max_req > MAX_REQUESTS_PER_SCAN_CEILING {
        return err(format!("{ctx}: max_requests_per_scan must be 1..={MAX_REQUESTS_PER_SCAN_CEILING}"));
    }
    if f.blocks.is_empty() || f.blocks.len() > MAX_BLOCKS_PER_PROFILE {
        return err(format!("{ctx}: 1..={MAX_BLOCKS_PER_PROFILE} blocks required"));
    }
    if f.points.is_empty() || f.points.len() > MAX_POINTS_PER_PROFILE {
        return err(format!("{ctx}: 1..={MAX_POINTS_PER_PROFILE} points required"));
    }
    let mut blocks = Vec::new();
    for (i, b) in f.blocks.iter().enumerate() {
        let a = area(&b.area, &format!("{ctx}: block #{i}"))?;
        blocks.push(Block {
            area: a,
            start: b.start,
            count: b.count,
            max_per_request: b.max_per_request.unwrap_or(a.max_quantity()),
        });
    }
    plan::validate_blocks(&blocks).map_err(|e| CatalogError(format!("{ctx}: {e}")))?;
    let mut points = BTreeMap::new();
    for p in &f.points {
        let pctx = format!("{ctx}: point \"{}\"", p.name);
        if !valid_name(&p.name) {
            return err(format!("{pctx}: name must match [a-z][a-z0-9_-]{{0,63}}"));
        }
        let a = area(&p.area, &pctx)?;
        let raw = match p.ty.as_str() {
            "bit" => RawType::Bit,
            "register_bit" => {
                RawType::RegisterBit(p.bit.ok_or_else(|| CatalogError(format!("{pctx}: register_bit needs `bit`")))?)
            }
            "u16" => RawType::U16,
            "i16" => RawType::I16,
            "u32" => RawType::U32,
            "i32" => RawType::I32,
            "f32" => RawType::F32,
            other => {
                return err(format!("{pctx}: type \"{other}\" (bit | register_bit | u16 | i16 | u32 | i32 | f32)"));
            }
        };
        if p.bit.is_some() && !matches!(raw, RawType::RegisterBit(_)) {
            return err(format!("{pctx}: `bit` only for register_bit"));
        }
        if raw.needs_bit_area() != a.is_bit() {
            return err(format!(
                "{pctx}: type {} is incompatible with area {a} (bit = coils/discrete inputs only, other types = registers only)",
                p.ty
            ));
        }
        if raw.width() == 1 && (p.word_order.is_some()) {
            return err(format!("{pctx}: word_order only for 32-bit types"));
        }
        if raw.is_bool() && p.byte_swap.is_some() && raw == RawType::Bit {
            return err(format!("{pctx}: byte_swap is meaningless for a bit"));
        }
        let word_order = match p.word_order.as_deref() {
            None => {
                if raw.width() == 2 {
                    return err(format!("{pctx}: 32-bit types need an explicit word_order (high_first | low_first)"));
                }
                WordOrder::HighFirst
            }
            Some("high_first") => WordOrder::HighFirst,
            Some("low_first") => WordOrder::LowFirst,
            Some(o) => return err(format!("{pctx}: word_order \"{o}\" (high_first | low_first)")),
        };
        let rounding = match p.rounding.as_deref() {
            None | Some("exact") => Rounding::Exact,
            Some("half_even") => Rounding::HalfEven,
            Some("truncate") => Rounding::Truncate,
            Some(o) => return err(format!("{pctx}: rounding \"{o}\" (exact | half_even | truncate)")),
        };
        let scale = Scale {
            numerator: p.scale_numerator.unwrap_or(1),
            denominator: p.scale_denominator.unwrap_or(1),
            offset: p.offset.unwrap_or(0),
            rounding,
        };
        let valid_range = match (p.valid_min, p.valid_max) {
            (None, None) => None,
            (lo, hi) => Some((lo.unwrap_or(i64::MIN), hi.unwrap_or(i64::MAX))),
        };
        let decoding = Decoding { raw, word_order, byte_swap: p.byte_swap.unwrap_or(false), scale, valid_range };
        decoding.validate().map_err(|e| CatalogError(format!("{pctx}: {e}")))?;
        // Inside one declared block.
        let span = plan::PointSpan { area: a, address: p.address, width: raw.width() };
        plan::build(&blocks, &[span], usize::MAX).map_err(|e| CatalogError(format!("{pctx}: {e}")))?;
        let _ = &p.description;
        if let Some(u) = &p.unit
            && u.len() > 32
        {
            return err(format!("{pctx}: unit longer than 32 bytes"));
        }
        let pt = Point { name: p.name.clone(), area: a, address: p.address, decoding, unit: p.unit.clone() };
        if points.insert(p.name.clone(), pt).is_some() {
            return err(format!("{pctx}: duplicate point name"));
        }
    }
    // Two points on the same item with different decodings are allowed (e.g. a register and
    // one of its bits, or a 16-bit view of one word of a 32-bit value); an identical
    // duplicate is ambiguous, and so are two 32-bit values that overlap with an offset
    // (the word alignment of the device would be undefined).
    let all: Vec<&Point> = points.values().collect();
    for (i, a) in all.iter().enumerate() {
        for b in all.iter().skip(i + 1) {
            if a.area == b.area && a.address == b.address && a.decoding == b.decoding {
                return err(format!(
                    "{ctx}: points \"{}\" and \"{}\" are identical (ambiguous mapping)",
                    a.name, b.name
                ));
            }
            let (aw, bw) = (u32::from(a.decoding.raw.width()), u32::from(b.decoding.raw.width()));
            let overlap = a.area == b.area
                && u32::from(a.address) < u32::from(b.address) + bw
                && u32::from(b.address) < u32::from(a.address) + aw;
            if overlap && aw == 2 && bw == 2 && a.address != b.address {
                return err(format!(
                    "{ctx}: points \"{}\" and \"{}\" overlap partially (ambiguous 32-bit alignment)",
                    a.name, b.name
                ));
            }
        }
    }
    Ok(Profile {
        id: m.id.clone(),
        version: m.version,
        title: m.title.clone(),
        manufacturer: m.manufacturer.clone(),
        source_document: m.source_document.clone(),
        blocks,
        points,
        max_requests_per_scan: max_req,
        file: file.to_path_buf(),
    })
}

fn read_bounded(path: &Path) -> Result<String, CatalogError> {
    let meta = std::fs::metadata(path).map_err(|e| CatalogError(format!("{}: {e}", path.display())))?;
    if meta.len() > MAX_CATALOG_FILE_BYTES {
        return err(format!("{}: larger than {MAX_CATALOG_FILE_BYTES} bytes", path.display()));
    }
    std::fs::read_to_string(path).map_err(|e| CatalogError(format!("{}: {e}", path.display())))
}

impl Catalog {
    /// Load every `*.toml` of `dir` (sorted by file name) plus `files`.
    pub fn load(dir: Option<&Path>, files: &[PathBuf]) -> Result<Catalog, CatalogError> {
        let mut paths: Vec<PathBuf> = Vec::new();
        if let Some(d) = dir {
            let rd = std::fs::read_dir(d).map_err(|e| CatalogError(format!("{}: {e}", d.display())))?;
            let mut v: Vec<PathBuf> = rd
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "toml"))
                .collect();
            v.sort();
            paths.extend(v);
        }
        paths.extend(files.iter().cloned());
        if paths.is_empty() {
            return err("catalog: no profile file");
        }
        let mut c = Catalog::default();
        for p in paths {
            let prof = parse_profile(&read_bounded(&p)?, &p)?;
            let key = (prof.id.clone(), prof.version);
            if let Some(old) = c.profiles.get(&key) {
                return err(format!(
                    "catalog: profile {} v{} defined twice ({} and {})",
                    key.0,
                    key.1,
                    old.file.display(),
                    p.display()
                ));
            }
            c.profiles.insert(key, prof);
        }
        Ok(c)
    }

    pub fn get(&self, id: &str, version: u32) -> Option<&Profile> {
        self.profiles.get(&(id.to_owned(), version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: &str = r#"schema = "moscada.driver-catalog/1"
[profile]
id = "t"
version = 1
title = "test"
manufacturer = "none"
transport = "tcp"
protocol = "modbus"
source_document = "unit test"
[[blocks]]
area = "holding_register"
start = 0
count = 10
[[blocks]]
area = "coil"
start = 0
count = 8
"#;

    fn parse(points: &str) -> Result<Profile, CatalogError> {
        parse_profile(&format!("{HEAD}{points}"), Path::new("t.toml"))
    }

    #[test]
    fn accepts_a_valid_profile() {
        let p = parse(
            r#"
[[points]]
name = "a"
area = "holding_register"
address = 0
type = "u32"
word_order = "low_first"
[[points]]
name = "a_bit3"
area = "holding_register"
address = 0
type = "register_bit"
bit = 3
[[points]]
name = "c"
area = "coil"
address = 7
type = "bit"
"#,
        )
        .unwrap();
        assert_eq!(p.points.len(), 3);
        assert_eq!(p.points["a"].decoding.word_order, WordOrder::LowFirst);
    }

    #[test]
    fn rejects_invalid_profiles() {
        let cases = [
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 9\ntype = \"u32\"\nword_order = \"high_first\"\n",
                "not inside",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u32\"\n",
                "explicit word_order",
            ),
            ("[[points]]\nname = \"a\"\narea = \"coil\"\naddress = 0\ntype = \"u16\"\n", "incompatible"),
            ("[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"bit\"\n", "incompatible"),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"f32\"\nword_order = \"high_first\"\n",
                "f32 needs",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\nscale_denominator = 0\n",
                "denominator",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u32\"\nword_order = \"high_first\"\nscale_numerator = 9223372036854775807\n",
                "overflow",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\n[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 1\ntype = \"u16\"\n",
                "duplicate point",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\n[[points]]\nname = \"b\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\n",
                "identical",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u32\"\nword_order = \"high_first\"\n[[points]]\nname = \"b\"\narea = \"holding_register\"\naddress = 1\ntype = \"i32\"\nword_order = \"high_first\"\n",
                "overlap partially",
            ),
            ("[[points]]\nname = \"A\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\n", "name must match"),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\nfoo = 1\n",
                "unknown field",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\nbit = 1\n",
                "only for register_bit",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"register_bit\"\nbit = 16\n",
                "outside 0..=15",
            ),
            (
                "[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\nvalid_min = 5\nvalid_max = 1\n",
                "min > max",
            ),
        ];
        for (pts, want) in cases {
            let e = parse(pts).unwrap_err();
            assert!(e.0.contains(want), "expected `{want}` in `{e}` for\n{pts}");
        }
        assert!(
            parse_profile(&HEAD.replace("catalog/1", "catalog/2"), Path::new("x")).unwrap_err().0.contains("schema")
        );
        assert!(parse_profile(&HEAD.replace("\"modbus\"", "\"spabus\""), Path::new("x")).is_err());
        let overlap =
            HEAD.replace("area = \"coil\"\nstart = 0\ncount = 8", "area = \"holding_register\"\nstart = 5\ncount = 8");
        assert!(
            parse_profile(
                &format!(
                    "{overlap}[[points]]\nname = \"a\"\narea = \"holding_register\"\naddress = 0\ntype = \"u16\"\n"
                ),
                Path::new("x")
            )
            .unwrap_err()
            .0
            .contains("overlap")
        );
    }
}

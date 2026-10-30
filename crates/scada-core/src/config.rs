//! Static configuration: specification as parsed by the host, and the validated,
//! immutable form the kernel runs on.

use std::fmt;

use crate::CONFIG_FINGERPRINT_VERSION;
use crate::digest::CanonicalHasher;
use crate::types::{ChannelId, SourceId, TagId, ValueType};

/// Upper bound for `max_observations_per_batch` (protects the preallocation).
pub const MAX_OBSERVATIONS_PER_BATCH_CEILING: u32 = 4096;
/// Upper bound for the number of tags in one kernel.
pub const MAX_TAGS_CEILING: usize = 1 << 20;
/// Upper bound for the number of sources in one kernel.
pub const MAX_SOURCES_CEILING: usize = 4096;
/// Maximum length of a tag or source name, in bytes.
pub const MAX_NAME_BYTES: usize = 64;

/// Limits that the core itself enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreLimits {
    /// Maximum observations in one batch. Also the maximum number of channels of a
    /// source, because a `FullState` must carry all of them in one batch (VS-001 has
    /// no chunked snapshot protocol).
    pub max_observations_per_batch: u32,
}

impl Default for CoreLimits {
    fn default() -> Self {
        CoreLimits { max_observations_per_batch: 128 }
    }
}

/// Tag declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSpec {
    pub tag_id: TagId,
    pub name: String,
    pub value_type: ValueType,
}

/// Binding of a source channel to a tag. `value_type` restates the expected type so that
/// a binding to a tag of another type is rejected at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelSpec {
    pub channel_id: ChannelId,
    pub tag_id: TagId,
    pub value_type: ValueType,
}

/// Authorised source and its channel bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpec {
    pub source_id: SourceId,
    pub name: String,
    pub channels: Vec<ChannelSpec>,
}

/// Configuration as written by the operator (order of entries is irrelevant).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigSpec {
    pub limits: CoreLimits,
    pub tags: Vec<TagSpec>,
    pub sources: Vec<SourceSpec>,
}

/// Reason why a configuration is refused. The kernel does not start with an invalid one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    InvalidLimit { name: &'static str, value: u64 },
    NoTags,
    NoSources,
    TooManyTags { count: usize, max: usize },
    TooManySources { count: usize, max: usize },
    ZeroTagId,
    ZeroSourceId,
    ZeroChannelId { source_id: SourceId },
    DuplicateTagId(TagId),
    DuplicateTagName(String),
    InvalidName { name: String },
    DuplicateSourceId(SourceId),
    DuplicateSourceName(String),
    SourceWithoutChannels(SourceId),
    TooManyChannels { source_id: SourceId, count: usize, max: u32 },
    DuplicateChannel { source_id: SourceId, channel_id: ChannelId },
    UnknownTag { source_id: SourceId, channel_id: ChannelId, tag_id: TagId },
    TypeMismatch { source_id: SourceId, channel_id: ChannelId, tag_id: TagId, binding: ValueType, tag: ValueType },
    TagBoundTwice(TagId),
    TagWithoutBinding(TagId),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::InvalidLimit { name, value } => write!(f, "invalid limit {name}={value}"),
            ConfigError::NoTags => f.write_str("no tags configured"),
            ConfigError::NoSources => f.write_str("no sources configured"),
            ConfigError::TooManyTags { count, max } => write!(f, "{count} tags exceed the maximum {max}"),
            ConfigError::TooManySources { count, max } => write!(f, "{count} sources exceed the maximum {max}"),
            ConfigError::ZeroTagId => f.write_str("tag id 0 is reserved"),
            ConfigError::ZeroSourceId => f.write_str("source id 0 is reserved"),
            ConfigError::ZeroChannelId { source_id } => write!(f, "source {source_id}: channel id 0 is reserved"),
            ConfigError::DuplicateTagId(t) => write!(f, "duplicate tag id {t}"),
            ConfigError::DuplicateTagName(n) => write!(f, "duplicate tag name {n:?}"),
            ConfigError::InvalidName { name } => {
                write!(f, "invalid name {name:?} (1..={MAX_NAME_BYTES} bytes of [A-Za-z0-9_.-])")
            }
            ConfigError::DuplicateSourceId(s) => write!(f, "duplicate source id {s}"),
            ConfigError::DuplicateSourceName(n) => write!(f, "duplicate source name {n:?}"),
            ConfigError::SourceWithoutChannels(s) => write!(f, "source {s} has no channels"),
            ConfigError::TooManyChannels { source_id, count, max } => {
                write!(f, "source {source_id} has {count} channels but a FullState is limited to {max} observations")
            }
            ConfigError::DuplicateChannel { source_id, channel_id } => {
                write!(f, "source {source_id}: duplicate channel {channel_id}")
            }
            ConfigError::UnknownTag { source_id, channel_id, tag_id } => {
                write!(f, "source {source_id} channel {channel_id}: unknown tag {tag_id}")
            }
            ConfigError::TypeMismatch { source_id, channel_id, tag_id, binding, tag } => write!(
                f,
                "source {source_id} channel {channel_id}: binding type {binding} incompatible with tag {tag_id} of type {tag}"
            ),
            ConfigError::TagBoundTwice(t) => write!(f, "tag {t} is bound to more than one channel"),
            ConfigError::TagWithoutBinding(t) => write!(f, "tag {t} is not bound to any channel"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Immutable tag metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagMeta {
    pub tag_id: TagId,
    pub name: String,
    pub value_type: ValueType,
}

/// Immutable source metadata: channels sorted by `ChannelId`, each mapped to the index
/// of its tag in [`ValidatedConfig::tags`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMeta {
    pub source_id: SourceId,
    pub name: String,
    channels: Vec<(ChannelId, u32)>,
}

impl SourceMeta {
    /// Tag index bound to `channel_id`, if any (binary search, no hashing).
    pub fn tag_index(&self, channel_id: ChannelId) -> Option<usize> {
        self.channels
            .binary_search_by_key(&channel_id, |&(c, _)| c)
            .ok()
            .and_then(|i| self.channels.get(i))
            .map(|&(_, t)| t as usize)
    }

    /// Channels and tag indices, sorted by channel id.
    pub fn channels(&self) -> &[(ChannelId, u32)] {
        &self.channels
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

/// Validated configuration. Tags are sorted by `TagId`, sources by `SourceId`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedConfig {
    limits: CoreLimits,
    tags: Vec<TagMeta>,
    sources: Vec<SourceMeta>,
    fingerprint: [u8; 32],
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
}

impl ValidatedConfig {
    /// Validate a specification. Any error refuses the whole configuration.
    pub fn validate(spec: ConfigSpec) -> Result<ValidatedConfig, ConfigError> {
        let limits = spec.limits;
        if limits.max_observations_per_batch == 0
            || limits.max_observations_per_batch > MAX_OBSERVATIONS_PER_BATCH_CEILING
        {
            return Err(ConfigError::InvalidLimit {
                name: "max_observations_per_batch",
                value: u64::from(limits.max_observations_per_batch),
            });
        }
        if spec.tags.is_empty() {
            return Err(ConfigError::NoTags);
        }
        if spec.sources.is_empty() {
            return Err(ConfigError::NoSources);
        }
        if spec.tags.len() > MAX_TAGS_CEILING {
            return Err(ConfigError::TooManyTags { count: spec.tags.len(), max: MAX_TAGS_CEILING });
        }
        if spec.sources.len() > MAX_SOURCES_CEILING {
            return Err(ConfigError::TooManySources { count: spec.sources.len(), max: MAX_SOURCES_CEILING });
        }

        // Tags: sorted by id, unique ids and names.
        let mut tags: Vec<TagMeta> = spec
            .tags
            .into_iter()
            .map(|t| TagMeta { tag_id: t.tag_id, name: t.name, value_type: t.value_type })
            .collect();
        tags.sort_by_key(|t| t.tag_id);
        for t in &tags {
            if t.tag_id.0 == 0 {
                return Err(ConfigError::ZeroTagId);
            }
            if !valid_name(&t.name) {
                return Err(ConfigError::InvalidName { name: t.name.clone() });
            }
        }
        for w in tags.windows(2) {
            if let [a, b] = w
                && a.tag_id == b.tag_id
            {
                return Err(ConfigError::DuplicateTagId(a.tag_id));
            }
        }
        let mut names: Vec<&str> = tags.iter().map(|t| t.name.as_str()).collect();
        names.sort_unstable();
        for w in names.windows(2) {
            if let [a, b] = w
                && a == b
            {
                return Err(ConfigError::DuplicateTagName((*a).to_owned()));
            }
        }

        // Sources.
        let mut specs = spec.sources;
        specs.sort_by_key(|s| s.source_id);
        for s in &specs {
            if s.source_id.0 == 0 {
                return Err(ConfigError::ZeroSourceId);
            }
            if !valid_name(&s.name) {
                return Err(ConfigError::InvalidName { name: s.name.clone() });
            }
        }
        for w in specs.windows(2) {
            if let [a, b] = w
                && a.source_id == b.source_id
            {
                return Err(ConfigError::DuplicateSourceId(a.source_id));
            }
        }
        let mut snames: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        snames.sort_unstable();
        for w in snames.windows(2) {
            if let [a, b] = w
                && a == b
            {
                return Err(ConfigError::DuplicateSourceName((*a).to_owned()));
            }
        }

        let mut bound = vec![false; tags.len()];
        let mut sources = Vec::with_capacity(specs.len());
        for s in specs {
            if s.channels.is_empty() {
                return Err(ConfigError::SourceWithoutChannels(s.source_id));
            }
            if s.channels.len() > limits.max_observations_per_batch as usize {
                return Err(ConfigError::TooManyChannels {
                    source_id: s.source_id,
                    count: s.channels.len(),
                    max: limits.max_observations_per_batch,
                });
            }
            let mut chans: Vec<ChannelSpec> = s.channels;
            chans.sort_by_key(|c| c.channel_id);
            let mut channels = Vec::with_capacity(chans.len());
            let mut prev: Option<ChannelId> = None;
            for c in chans {
                if c.channel_id.0 == 0 {
                    return Err(ConfigError::ZeroChannelId { source_id: s.source_id });
                }
                if prev == Some(c.channel_id) {
                    return Err(ConfigError::DuplicateChannel { source_id: s.source_id, channel_id: c.channel_id });
                }
                prev = Some(c.channel_id);
                let Ok(idx) = tags.binary_search_by_key(&c.tag_id, |t| t.tag_id) else {
                    return Err(ConfigError::UnknownTag {
                        source_id: s.source_id,
                        channel_id: c.channel_id,
                        tag_id: c.tag_id,
                    });
                };
                let tag_type = tags.get(idx).map(|t| t.value_type);
                if tag_type != Some(c.value_type) {
                    return Err(ConfigError::TypeMismatch {
                        source_id: s.source_id,
                        channel_id: c.channel_id,
                        tag_id: c.tag_id,
                        binding: c.value_type,
                        tag: tag_type.unwrap_or(c.value_type),
                    });
                }
                match bound.get_mut(idx) {
                    Some(b) if *b => return Err(ConfigError::TagBoundTwice(c.tag_id)),
                    Some(b) => *b = true,
                    None => {
                        return Err(ConfigError::UnknownTag {
                            source_id: s.source_id,
                            channel_id: c.channel_id,
                            tag_id: c.tag_id,
                        });
                    }
                }
                // idx < MAX_TAGS_CEILING (2^20) always fits in u32.
                let idx32 =
                    u32::try_from(idx).map_err(|_| ConfigError::TooManyTags { count: idx, max: MAX_TAGS_CEILING })?;
                channels.push((c.channel_id, idx32));
            }
            sources.push(SourceMeta { source_id: s.source_id, name: s.name, channels });
        }
        if let Some(pos) = bound.iter().position(|b| !*b) {
            let tag_id = tags.get(pos).map_or(TagId(0), |t| t.tag_id);
            return Err(ConfigError::TagWithoutBinding(tag_id));
        }

        let mut cfg = ValidatedConfig { limits, tags, sources, fingerprint: [0; 32] };
        cfg.fingerprint = cfg.compute_fingerprint();
        Ok(cfg)
    }

    fn compute_fingerprint(&self) -> [u8; 32] {
        let mut h = CanonicalHasher::new(b"scada.kernel.config", CONFIG_FINGERPRINT_VERSION);
        h.u32(self.limits.max_observations_per_batch);
        h.len(self.tags.len());
        for t in &self.tags {
            h.u32(t.tag_id.0);
            h.u8(t.value_type.code());
            h.bytes(t.name.as_bytes());
        }
        h.len(self.sources.len());
        for s in &self.sources {
            h.u32(s.source_id.0);
            h.bytes(s.name.as_bytes());
            h.len(s.channels.len());
            for &(c, t) in &s.channels {
                h.u32(c.0);
                h.u32(self.tags.get(t as usize).map_or(0, |m| m.tag_id.0));
            }
        }
        h.finish()
    }

    pub fn limits(&self) -> CoreLimits {
        self.limits
    }

    /// Tags sorted by `TagId`.
    pub fn tags(&self) -> &[TagMeta] {
        &self.tags
    }

    /// Sources sorted by `SourceId`.
    pub fn sources(&self) -> &[SourceMeta] {
        &self.sources
    }

    /// BLAKE3 of the canonical, versioned encoding of this configuration.
    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    pub fn source_index(&self, source_id: SourceId) -> Option<usize> {
        self.sources.binary_search_by_key(&source_id, |s| s.source_id).ok()
    }

    pub fn tag_index(&self, tag_id: TagId) -> Option<usize> {
        self.tags.binary_search_by_key(&tag_id, |t| t.tag_id).ok()
    }

    pub fn source(&self, source_id: SourceId) -> Option<&SourceMeta> {
        self.source_index(source_id).and_then(|i| self.sources.get(i))
    }
}

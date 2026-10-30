//! DoD 1: an invalid configuration refuses startup.

mod common;

use common::*;
use scada_core::*;

fn err(spec: ConfigSpec) -> ConfigError {
    ValidatedConfig::validate(spec).expect_err("must be refused")
}

#[test]
fn demo_config_is_valid_and_sorted() {
    let mut spec = demo_spec();
    spec.tags.reverse();
    spec.sources[0].channels.reverse();
    let a = ValidatedConfig::validate(spec).unwrap();
    let b = ValidatedConfig::validate(demo_spec()).unwrap();
    assert_eq!(a.fingerprint(), b.fingerprint(), "fingerprint independent of declaration order");
    assert_eq!(a.tags().iter().map(|t| t.tag_id.0).collect::<Vec<_>>(), vec![101, 102, 103]);
}

#[test]
fn duplicate_tag_id() {
    let mut s = demo_spec();
    s.tags[1].tag_id = DOOR;
    assert_eq!(err(s), ConfigError::DuplicateTagId(DOOR));
}

#[test]
fn duplicate_tag_name() {
    let mut s = demo_spec();
    s.tags[1].name = "door_contact".into();
    assert_eq!(err(s), ConfigError::DuplicateTagName("door_contact".into()));
}

#[test]
fn duplicate_source_id() {
    let mut s = demo_spec();
    let mut other = s.sources[0].clone();
    other.name = "other".into();
    s.sources.push(other);
    assert_eq!(err(s), ConfigError::DuplicateSourceId(SRC));
}

#[test]
fn duplicate_channel() {
    let mut s = demo_spec();
    s.sources[0].channels[1].channel_id = ChannelId(1);
    assert_eq!(err(s), ConfigError::DuplicateChannel { source_id: SRC, channel_id: ChannelId(1) });
}

#[test]
fn channel_bound_to_unknown_tag() {
    let mut s = demo_spec();
    s.sources[0].channels[2].tag_id = TagId(999);
    assert_eq!(err(s), ConfigError::UnknownTag { source_id: SRC, channel_id: ChannelId(3), tag_id: TagId(999) });
}

#[test]
fn tag_without_binding() {
    let mut s = demo_spec();
    s.sources[0].channels.pop();
    assert_eq!(err(s), ConfigError::TagWithoutBinding(TEMP));
}

#[test]
fn tag_bound_twice() {
    let mut s = demo_spec();
    s.tags.pop();
    s.sources[0].channels[2] = ChannelSpec { channel_id: ChannelId(3), tag_id: DOOR, value_type: ValueType::Bool };
    assert_eq!(err(s), ConfigError::TagBoundTwice(DOOR));
}

#[test]
fn incompatible_types() {
    let mut s = demo_spec();
    s.sources[0].channels[2].value_type = ValueType::Bool;
    assert_eq!(
        err(s),
        ConfigError::TypeMismatch {
            source_id: SRC,
            channel_id: ChannelId(3),
            tag_id: TEMP,
            binding: ValueType::Bool,
            tag: ValueType::I64
        }
    );
}

#[test]
fn source_with_more_channels_than_a_full_state_can_carry() {
    let mut s = demo_spec();
    s.limits.max_observations_per_batch = 2;
    assert_eq!(err(s), ConfigError::TooManyChannels { source_id: SRC, count: 3, max: 2 });
}

#[test]
fn reserved_zero_ids_and_empty_sets() {
    let mut s = demo_spec();
    s.tags[0].tag_id = TagId(0);
    assert_eq!(err(s), ConfigError::ZeroTagId);
    let mut s = demo_spec();
    s.sources[0].source_id = SourceId(0);
    assert_eq!(err(s), ConfigError::ZeroSourceId);
    let mut s = demo_spec();
    s.sources[0].channels[0].channel_id = ChannelId(0);
    assert_eq!(err(s), ConfigError::ZeroChannelId { source_id: SRC });
    let mut s = demo_spec();
    s.sources.clear();
    assert_eq!(err(s), ConfigError::NoSources);
    let mut s = demo_spec();
    s.sources[0].channels.clear();
    assert_eq!(err(s), ConfigError::SourceWithoutChannels(SRC));
    let mut s = demo_spec();
    s.limits.max_observations_per_batch = 0;
    assert!(matches!(err(s), ConfigError::InvalidLimit { .. }));
    let mut s = demo_spec();
    s.tags[0].name = "bad name!".into();
    assert!(matches!(err(s), ConfigError::InvalidName { .. }));
}

//! DoD 1 at process level: the kernel refuses to start with an invalid configuration.

use std::process::Command;

use scada_runtime::config::{self, LoadError};

const DEMO: &str = include_str!("../../../config/demo.toml");

#[test]
fn shipped_demo_config_is_valid() {
    let c = config::parse(DEMO).unwrap();
    assert_eq!(c.limits.max_frame_bytes, 65_536);
    assert_eq!(c.limits.max_driver_connections, 16);
    assert_eq!(c.limits.input_queue_capacity, 64);
    assert_eq!(c.core.limits().max_observations_per_batch, 128);
    assert_eq!(c.core.tags().len(), 3);
}

#[test]
fn invalid_files_are_refused() {
    let cases: &[(&str, &str)] = &[
        ("unknown field", "frame_size = 3"),
        ("bad type", r#"type = "f64""#),
        ("duplicate tag", "id = 101\nname = \"door_contact_2\"\ntype = \"bool\"\n"),
        ("frame too small", "max_frame_bytes = 1024"),
        ("zero queue", "input_queue_capacity = 0"),
        ("empty uid list", "authorized_uids = []"),
    ];
    for (what, patch) in cases {
        let text = match *what {
            "unknown field" => DEMO.replace("[ipc]\n", &format!("[ipc]\n{patch}\n")),
            "bad type" => DEMO.replacen(r#"type = "i64""#, patch, 1),
            "duplicate tag" => DEMO.replacen("[[tags]]\n", &format!("[[tags]]\n{patch}\n[[tags]]\n"), 1),
            "frame too small" => DEMO.replace("max_frame_bytes = 65536", patch),
            "zero queue" => DEMO.replace("input_queue_capacity = 64", patch),
            "empty uid list" => DEMO.replace("# authorized_uids = [1000]", patch),
            _ => unreachable!(),
        };
        assert_ne!(text, DEMO, "{what}: patch did not apply");
        let r = config::parse(&text);
        assert!(r.is_err(), "{what} accepted");
        if *what == "duplicate tag" {
            assert!(matches!(r, Err(LoadError::Core(scada_core::ConfigError::DuplicateTagId(_)))), "{r:?}");
        }
    }
}

#[test]
fn kernel_binary_exits_with_error_on_invalid_config() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.toml");
    std::fs::write(&bad, DEMO.replace("tag = 103", "tag = 999")).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scada-kernel")).arg("--config").arg(&bad).output().unwrap();
    assert_eq!(out.status.code(), Some(3));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("unknown tag 999"), "{err}");
    let good = dir.path().join("good.toml");
    std::fs::write(&good, DEMO).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scada-kernel"))
        .arg("--config")
        .arg(&good)
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(out.status.success());
}

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

#[test]
fn runtime_section_selects_scheduler_explicitly() {
    use scada_runtime::config::Scheduler;
    assert_eq!(config::parse(DEMO).unwrap().runtime.scheduler, Scheduler::CurrentThread);
    let with = |rt: &str| config::parse(&format!("{DEMO}\n[runtime]\n{rt}\n"));
    assert_eq!(
        with("scheduler = \"multi_thread\"\nworker_threads = 2").unwrap().runtime.scheduler,
        Scheduler::MultiThread { worker_threads: 2 }
    );
    assert_eq!(with("scheduler = \"current_thread\"").unwrap().runtime.scheduler, Scheduler::CurrentThread);
    // No implicit expansion to all cores: multi_thread needs an explicit, bounded count.
    for bad in [
        "scheduler = \"multi_thread\"",
        "scheduler = \"multi_thread\"\nworker_threads = 0",
        "scheduler = \"multi_thread\"\nworker_threads = 65",
        "scheduler = \"current_thread\"\nworker_threads = 2",
        "scheduler = \"auto\"",
        "workers = 2",
    ] {
        assert!(with(bad).is_err(), "{bad} accepted");
    }
}

#[test]
fn scheduler_override_on_command_line_is_validated() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good.toml");
    std::fs::write(&good, DEMO).unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_scada-kernel"))
            .arg("--config")
            .arg(&good)
            .args(args)
            .arg("--check-config")
            .output()
            .unwrap()
    };
    let out = run(&["--scheduler", "multi_thread", "--worker-threads", "4"]);
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("scheduler=multi_thread(4)"));
    assert_eq!(run(&["--scheduler", "multi_thread"]).status.code(), Some(2));
    assert_eq!(run(&["--worker-threads", "2"]).status.code(), Some(2));
}

// VS-002 §10: both shipped profiles are valid, select the declared scheduler and fit
// their explicit memory budget.
#[test]
fn shipped_profiles_are_valid_and_fit_their_budget() {
    use scada_runtime::{budget, config::Scheduler};
    for (text, sched) in [
        (include_str!("../../../config/profiles/micro.toml"), Scheduler::CurrentThread),
        (include_str!("../../../config/profiles/server.toml"), Scheduler::MultiThread { worker_threads: 2 }),
    ] {
        let c = config::parse(text).unwrap();
        assert_eq!(c.runtime.scheduler, sched);
        assert!(c.runtime.memory_budget_bytes.is_some());
        let b = budget::compute(&c, false, false);
        budget::check(&c, &b).unwrap();
    }
}

// VS-003: both shipped profiles still fit their memory budget with the optional
// per-source ingress policies enabled (ADR 0010 §6), and default to `fifo`.
#[test]
fn shipped_profiles_fit_their_budget_with_the_vs003_ingress_policies() {
    use scada_runtime::{budget, config::IngressPolicy};
    for text in [include_str!("../../../config/profiles/micro.toml"), include_str!("../../../config/profiles/server.toml")]
    {
        assert_eq!(config::parse(text).unwrap().limits.ingress, IngressPolicy::Fifo);
        for policy in ["source_capped", "source_fair"] {
            let t = text.replace(
                "# ingress_policy = \"fifo\"\n# per_source_pending_limit = 4",
                &format!("ingress_policy = \"{policy}\"\nper_source_pending_limit = 4"),
            );
            assert_ne!(t, text, "profile lost its commented VS-003 keys");
            let c = config::parse(&t).unwrap();
            assert!(c.limits.ingress.per_source() == Some(4));
            let b = budget::compute(&c, true, false);
            budget::check(&c, &b).unwrap_or_else(|e| panic!("{policy}: {e}"));
        }
    }
}

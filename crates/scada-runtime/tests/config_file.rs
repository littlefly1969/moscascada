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
    for text in
        [include_str!("../../../config/profiles/micro.toml"), include_str!("../../../config/profiles/server.toml")]
    {
        assert_eq!(config::parse(text).unwrap().limits.ingress, IngressPolicy::Fifo);
        let policy = "source_fair";
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

// VS-004: both shipped profiles keep supervision off (commented keys only) and still fit
// their memory budget with liveness and freshness enabled on their source.
#[test]
fn shipped_profiles_fit_their_budget_with_vs004_supervision() {
    use scada_runtime::budget;
    for text in
        [include_str!("../../../config/profiles/micro.toml"), include_str!("../../../config/profiles/server.toml")]
    {
        let c = config::parse(text).unwrap();
        assert!(c.supervision.is_none(), "supervision must stay off by default");
        assert_eq!(budget::compute(&c, false, false).supervision, 0);
        let t = text
            .replace("# watchdog_poll_ms = 250", "watchdog_poll_ms = 250")
            .replace("# liveness_timeout_ms = 5000", "liveness_timeout_ms = 5000")
            .replace("# freshness_timeout_ms = 12000", "freshness_timeout_ms = 12000");
        for k in ["\nwatchdog_poll_ms = 250", "\nliveness_timeout_ms = 5000", "\nfreshness_timeout_ms = 12000"] {
            assert!(t.contains(k), "profile lost its commented VS-004 key {k:?}");
        }
        let c = config::parse(&t).unwrap();
        let sup = c.supervision.as_ref().unwrap();
        assert_eq!((sup.watchdog_poll_ms, sup.sources.len()), (250, 1));
        let b = budget::compute(&c, true, false);
        assert!(b.supervision > 0);
        budget::check(&c, &b).unwrap();
    }
}

// VS-004: incoherent supervision settings refuse startup with a clear error (fail fast).
#[test]
fn invalid_supervision_settings_are_refused() {
    let cfg = |runtime: &str, src: &str| {
        format!(
            "[ipc]\nsocket_path = \"/run/x/d.sock\"\n[runtime]\n{runtime}\n[[tags]]\nid = 1\nname = \"t\"\ntype = \"i64\"\n\
             [[sources]]\nid = 1\nname = \"s\"\n{src}\nchannels = [{{ channel = 1, tag = 1, type = \"i64\" }}]\n"
        )
    };
    let err = |r: &str, s: &str| config::parse(&cfg(r, s)).err().map(|e| e.to_string());
    assert!(err("watchdog_poll_ms = 100", "").unwrap().contains("only valid when a source"));
    assert!(err("", "liveness_timeout_ms = 749").unwrap().contains("at least 3 x watchdog_poll_ms=250"));
    assert!(err("watchdog_poll_ms = 5", "liveness_timeout_ms = 1000").unwrap().contains("watchdog_poll_ms=5 outside"));
    assert!(err("watchdog_poll_ms = 60001", "liveness_timeout_ms = 200000").is_some());
    assert!(err("", "freshness_timeout_ms = 86400001").unwrap().contains("outside"));
    assert!(err("", "freshness_timeout_ms = 1024251").unwrap().contains("bounded sample ring"));
    assert!(err("", "liveness_timeout_ms = 0").is_some());
    assert!(err("", "liveness_timeout_ms = -1").is_some(), "negative is a parse error");
    assert!(err("", "heartbeat_timeout_ms = 1000").is_some(), "unknown keys are refused");
    // Valid: each kind alone, both, none.
    for s in [
        "",
        "liveness_timeout_ms = 750",
        "freshness_timeout_ms = 1024000",
        "liveness_timeout_ms = 5000\nfreshness_timeout_ms = 12000",
    ] {
        assert!(err("", s).is_none(), "{s}: {:?}", err("", s));
    }
    let c = config::parse(&cfg("watchdog_poll_ms = 100", "liveness_timeout_ms = 300")).unwrap();
    let p = c.supervision.unwrap().sources[0];
    assert_eq!((p.liveness_timeout_ms, p.freshness_timeout_ms), (Some(300), None));
    assert_eq!(c.core.sources()[0].supervision, scada_core::Supervision { liveness: true, freshness: false });
}

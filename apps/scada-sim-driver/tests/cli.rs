//! CLI contract of the simulator (also makes `cargo test` build the binary that the
//! multi-process tests in `scada-runtime` spawn).

use std::process::Command;

#[test]
fn missing_arguments_exit_with_usage() {
    let out = Command::new(env!("CARGO_BIN_EXE_scada-sim-driver")).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
}

#[test]
fn unreachable_socket_is_reported_not_panicked() {
    let out = Command::new(env!("CARGO_BIN_EXE_scada-sim-driver"))
        .args(["--socket", "/nonexistent/x.sock", "demo"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).lines().any(|l| l.starts_with("ERROR")));
}

//! VS-002 E2: socket directory and file hardening (in-process, no kernel binary needed).

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use scada_runtime::server::{bind_socket, check_socket_dir};

fn chmod(p: &Path, mode: u32) {
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn private_dir() -> tempfile::TempDir {
    let d = tempfile::Builder::new().prefix("scada-sec").tempdir().unwrap();
    chmod(d.path(), 0o700);
    d
}

#[tokio::test(flavor = "current_thread")]
async fn group_or_world_writable_directory_is_refused() {
    for mode in [0o777, 0o770, 0o1777, 0o702] {
        let d = private_dir();
        chmod(d.path(), mode);
        let e = bind_socket(&d.path().join("k.sock"), 0o660, None).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "mode {mode:o}: {e}");
        assert!(!d.path().join("k.sock").exists());
    }
    let d = private_dir();
    chmod(d.path(), 0o750);
    assert!(bind_socket(&d.path().join("k.sock"), 0o660, None).is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn symlinked_directory_is_refused() {
    let d = private_dir();
    let real = d.path().join("real");
    std::fs::create_dir(&real).unwrap();
    chmod(&real, 0o700);
    let link = d.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let e = bind_socket(&link.join("k.sock"), 0o660, None).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
}

#[tokio::test(flavor = "current_thread")]
async fn missing_directory_is_created_private() {
    let d = private_dir();
    let p = d.path().join("a/b/k.sock");
    let (_l, _g) = bind_socket(&p, 0o660, None).unwrap();
    let m = std::fs::metadata(p.parent().unwrap()).unwrap();
    assert_eq!(m.mode() & 0o777, 0o750);
    assert_eq!(std::fs::symlink_metadata(&p).unwrap().mode() & 0o777, 0o660);
    check_socket_dir(p.parent().unwrap()).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn non_socket_and_live_socket_are_not_replaced() {
    let d = private_dir();
    let p = d.path().join("k.sock");
    std::fs::write(&p, b"precious").unwrap();
    assert_eq!(bind_socket(&p, 0o660, None).unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&p).unwrap(), b"precious");
    std::fs::remove_file(&p).unwrap();
    let (_l, _g) = bind_socket(&p, 0o660, None).unwrap();
    assert_eq!(bind_socket(&p, 0o660, None).unwrap_err().kind(), std::io::ErrorKind::AddrInUse);
}

#[tokio::test(flavor = "current_thread")]
async fn own_stale_socket_is_replaced() {
    let d = private_dir();
    let p = d.path().join("k.sock");
    drop(std::os::unix::net::UnixListener::bind(&p).unwrap()); // leaves a dead socket file
    assert!(p.exists());
    let (_l, _g) = bind_socket(&p, 0o660, None).unwrap();
    assert!(std::os::unix::net::UnixStream::connect(&p).is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn stale_socket_of_another_user_is_not_removed() {
    let d = private_dir();
    let p = d.path().join("k.sock");
    drop(std::os::unix::net::UnixListener::bind(&p).unwrap());
    // Only root can give a file away; elsewhere this case cannot be built and is skipped.
    if std::os::unix::fs::chown(&p, Some(4242), None).is_err() {
        eprintln!("not root: skipping foreign-owner case");
        return;
    }
    let e = bind_socket(&p, 0o660, None).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
    assert_eq!(std::fs::symlink_metadata(&p).unwrap().uid(), 4242);
}

#[tokio::test(flavor = "current_thread")]
async fn guard_removes_only_its_own_socket() {
    let d = private_dir();
    let p = d.path().join("k.sock");
    let (l, g) = bind_socket(&p, 0o660, None).unwrap();
    drop(l);
    // Someone replaced the path after we bound it: the guard must not delete theirs.
    std::fs::remove_file(&p).unwrap();
    std::fs::write(&p, b"other").unwrap();
    drop(g);
    assert_eq!(std::fs::read(&p).unwrap(), b"other");
    std::fs::remove_file(&p).unwrap();
    let (_l, g) = bind_socket(&p, 0o660, None).unwrap();
    drop(g);
    assert!(!p.exists(), "own socket not removed");
}

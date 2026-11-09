//! End-to-end tests of the `quorid` binary.

mod common;

use std::path::Path;
use std::process::Output;

use common::{Server, quorid};

fn run(args: &[&str]) -> Output {
    quorid().args(args).output().expect("failed to run quorid")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

#[test]
fn encode_defaults_to_mania() {
    let out = run(&["encode", "1"]);
    assert!(out.status.success());
    assert_eq!(stdout(&out), "🤬\n");

    let out = run(&["encode", "61", "-f", "base62"]);
    assert_eq!(stdout(&out), "z\n");

    let out = run(&["encode", "0", "--format", "base32", "--padded"]);
    assert_eq!(stdout(&out), format!("{}\n", "0".repeat(26)));
}

#[test]
fn decode_prints_every_format() {
    let out = run(&["decode", "🤬"]);
    assert!(out.status.success());
    assert_eq!(
        stdout(&out),
        " decimal  1\n  base32  1\n  base62  1\n  fruity  🍇\n   mania  🤬\n"
    );
}

#[test]
fn errors_exit_non_zero() {
    let out = run(&["decode", "!!", "-f", "base62"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.starts_with("error: invalid symbol '!' at position 0"),
        "{stderr}"
    );
}

#[test]
fn serve_fails_on_unusable_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"").unwrap();

    let out = run(&["serve", "--data-dir", file.to_str().unwrap()]);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.starts_with("error: storage error"), "{stderr}");
}

#[test]
fn serve_fails_on_busy_port() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap().to_string();

    let out = run(&["serve", "--backend", "memory", "--listen", &addr]);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.starts_with("error: "), "{stderr}");
}

#[test]
fn serve_raft_needs_a_node_id() {
    let out = run(&["serve", "--backend", "raft", "--voters", "1"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.starts_with("error: the raft backend needs --node-id and --voters"),
        "{stderr}"
    );
}

#[test]
fn serve_raft_needs_this_node_among_the_voters() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().to_str().unwrap();
    for voters in [[].as_slice(), ["--voters", "2,3"].as_slice()] {
        let out = quorid()
            .args([
                "serve",
                "--backend",
                "raft",
                "--node-id",
                "1",
                "--data-dir",
                data,
            ])
            .args(voters)
            .output()
            .unwrap();
        assert!(!out.status.success());
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(
            stderr.starts_with("error: invalid request: this node is not one of the voters"),
            "{stderr}"
        );
    }
}

#[test]
fn serve_raft_rejects_a_malformed_peer() {
    let out = run(&["serve", "--backend", "raft", "--peer", "2"]);
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains("expected ID=RAFT_ADDR[@CLIENT_ADDR], got \"2\""),
        "{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn serve_memory_backend() {
    let server = Server::start(&["--backend", "memory"]);
    let response = server.post("/v1/ns/users/ids?count=2");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.ends_with(r#"{"namespace":"users","format":"mania","ids":["🤬","🤣"]}"#),
        "{response}"
    );
    assert!(server.interrupt().contains("shutting down"));
}

#[cfg(unix)]
#[test]
fn serve_file_backend_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let args = [
        "--data-dir",
        data.to_str().unwrap(),
        "--format",
        "decimal",
        "--block-size",
        "100",
    ];

    let server = Server::start(&args);
    assert!(server.post("/v1/ns/users/ids").ends_with(r#""ids":["1"]}"#));
    server.interrupt();
    assert!(Path::new(&data).join("state.json").exists());

    // The first block was leased durably, so the restart starts after it.
    let server = Server::start(&args);
    assert!(
        server
            .post("/v1/ns/users/ids")
            .ends_with(r#""ids":["101"]}"#)
    );
    server.interrupt();
}

#[cfg(unix)]
#[test]
fn serve_raft_backend_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let args = [
        "--backend",
        "raft",
        "--node-id",
        "1",
        "--voters",
        "1",
        "--raft-listen",
        "127.0.0.1:0",
        "--data-dir",
        data.to_str().unwrap(),
        "--format",
        "decimal",
        "--block-size",
        "100",
    ];

    let server = Server::start(&args);
    let response = server.post_when_ready("/v1/ns/users/ids");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with(r#""ids":["1"]}"#), "{response}");
    server.interrupt();
    assert!(data.join("raft").join("raft.state").exists());

    // The first block was committed durably, so the restart starts after it.
    let server = Server::start(&args);
    let response = server.post_when_ready("/v1/ns/users/ids");
    assert!(response.ends_with(r#""ids":["101"]}"#), "{response}");
    server.interrupt();
}

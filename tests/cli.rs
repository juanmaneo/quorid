//! End-to-end tests of the `quorid` binary.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Output, Stdio};

fn quorid() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_quorid"));
    // Plain logs at the default level, whatever the developer's environment says.
    cmd.env("NO_COLOR", "1").env_remove("RUST_LOG");
    for var in ["LISTEN", "BACKEND", "DATA_DIR", "BLOCK_SIZE", "FORMAT"] {
        cmd.env_remove(format!("QUORID_{var}"));
    }
    cmd
}

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

/// A running `quorid serve`, killed on drop if a test fails midway.
struct Server {
    child: Child,
    logs: BufReader<ChildStdout>,
    addr: String,
}

impl Server {
    fn start(args: &[&str]) -> Server {
        let mut child = quorid()
            .arg("serve")
            .args(["--listen", "127.0.0.1:0"])
            .args(args)
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed to start quorid");
        let mut logs = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            let read = logs.read_line(&mut line).unwrap();
            assert!(read > 0, "quorid exited before listening");
            if line.contains("quorid listening") {
                break;
            }
        }
        let addr = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("addr="))
            .expect("listening log has an addr")
            .to_string();
        Server { child, logs, addr }
    }

    fn post(&self, path: &str) -> String {
        let mut stream = TcpStream::connect(&self.addr).unwrap();
        write!(
            stream,
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            self.addr
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    /// Stops the server like Ctrl-C would and returns its remaining logs.
    #[cfg(unix)]
    fn interrupt(mut self) -> String {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill").args(["-INT", &pid]).status().unwrap();
        assert!(sent.success());
        assert!(self.child.wait().unwrap().success());
        let mut rest = String::new();
        self.logs.read_to_string(&mut rest).unwrap();
        rest
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

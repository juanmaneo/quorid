//! Helpers of the end-to-end tests, which run the real `quorid` binary.

// Each test crate uses its own part of this module.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::Duration;

pub fn quorid() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_quorid"));
    // Plain logs at the default level, whatever the developer's environment says.
    cmd.env("NO_COLOR", "1").env_remove("RUST_LOG");
    for var in [
        "LISTEN",
        "BACKEND",
        "DATA_DIR",
        "BLOCK_SIZE",
        "FORMAT",
        "NODE_ID",
        "VOTERS",
        "RAFT_LISTEN",
        "PEERS",
    ] {
        cmd.env_remove(format!("QUORID_{var}"));
    }
    cmd
}

/// Sends one request and returns the whole raw response.
pub fn http(addr: &str, method: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

/// The status code and the body of a raw response.
pub fn split(response: &str) -> (u16, &str) {
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("not an HTTP response: {response:?}"));
    (
        status,
        response.split_once("\r\n\r\n").map_or("", |(_, body)| body),
    )
}

/// A running `quorid serve`, killed on drop if a test fails midway.
pub struct Server {
    child: Child,
    logs: BufReader<ChildStdout>,
    pub addr: String,
}

impl Server {
    /// Starts a server on a port picked by the system.
    pub fn start(args: &[&str]) -> Server {
        Server::start_on("127.0.0.1:0", args)
    }

    pub fn start_on(listen: &str, args: &[&str]) -> Server {
        let mut child = quorid()
            .arg("serve")
            .args(["--listen", listen])
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

    pub fn request(&self, method: &str, path: &str) -> String {
        http(&self.addr, method, path)
    }

    pub fn post(&self, path: &str) -> String {
        self.request("POST", path)
    }

    /// Posts until the node accepts writes: a Raft node first has to find or
    /// become the leader, and answers `421 not_leader` meanwhile.
    pub fn post_when_ready(&self, path: &str) -> String {
        let start = std::time::Instant::now();
        loop {
            let response = self.post(path);
            if !response.starts_with("HTTP/1.1 421") {
                return response;
            }
            assert!(start.elapsed().as_secs() < 30, "no leader: {response}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Stops the server like Ctrl-C would and returns its remaining logs.
    #[cfg(unix)]
    pub fn interrupt(mut self) -> String {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill").args(["-INT", &pid]).status().unwrap();
        assert!(sent.success());
        assert!(self.child.wait().unwrap().success());
        let mut rest = String::new();
        self.logs.read_to_string(&mut rest).unwrap();
        rest
    }

    /// Stops the server without letting it clean up, like a crash.
    pub fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

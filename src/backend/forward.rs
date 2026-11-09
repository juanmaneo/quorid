//! Hands a command to the leader, for the followers of a replicated backend.
//!
//! The command travels as a `/blocks` request to the leader's HTTP API,
//! marked with [`FORWARDED_HEADER`] so that the leader does not forward it
//! again if it has stopped leading in the meantime. The client is just enough
//! HTTP/1.1 for that: one request per connection, and the response read until
//! the leader closes it.

use std::fmt::Display;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::command::{Command, Outcome};
use crate::error::{Error, Result};
use crate::id::IdRange;
use crate::server::FORWARDED_HEADER;

/// More than any answer of a leader, which is a few hundred bytes.
const MAX_RESPONSE: u64 = 64 * 1024;

/// What the leader says when it refuses.
#[derive(Default, Deserialize)]
struct Refusal {
    #[serde(default)]
    message: String,
    leader: Option<String>,
}

/// Executes `command` on the leader, whose client API is at `addr`
/// (`host:port`). It must all be done within `timeout`.
pub async fn execute(addr: &str, command: &Command, timeout: Duration) -> Result<Outcome> {
    let Command::Reserve { namespace, count } = command;
    let request = format!(
        "POST /v1/ns/{namespace}/blocks?size={count} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         {FORWARDED_HEADER}: 1\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\r\n"
    );
    let raw = match tokio::time::timeout(timeout, exchange(addr, &request)).await {
        Ok(Ok(raw)) => raw,
        Ok(Err(e)) => return Err(cannot_reach(addr, e)),
        Err(_) => return Err(cannot_reach(addr, "timed out")),
    };

    let response = String::from_utf8_lossy(&raw);
    let (head, body) = response.split_once("\r\n\r\n").unwrap_or((&response, ""));
    let status_line = head.lines().next().unwrap_or("no answer");
    let code = status_line.split_whitespace().nth(1).unwrap_or_default();
    if code == "200"
        && let Ok(range) = serde_json::from_str::<IdRange>(body)
    {
        return Ok(Outcome::Reserved(range));
    }

    let refusal: Refusal = serde_json::from_str(body).unwrap_or_default();
    Err(match code {
        "421" => Error::NotLeader {
            leader: refusal.leader,
        },
        "409" => Error::Exhausted(namespace.clone()),
        "400" => Error::InvalidRequest(
            refusal
                .message
                .strip_prefix("invalid request: ")
                .unwrap_or(&refusal.message)
                .to_string(),
        ),
        _ => Error::Unavailable(format!(
            "the leader at {addr} did not give a block: {}: {}",
            status_line.chars().take(100).collect::<String>(),
            refusal.message
        )),
    })
}

fn cannot_reach(addr: &str, why: impl Display) -> Error {
    Error::Unavailable(format!("cannot reach the leader at {addr}: {why}"))
}

/// Sends the request and reads the response to its end.
async fn exchange(addr: &str, request: &str) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await?;
    // Past this point a failure leaves nothing that parses as an answer,
    // which the caller reports as one.
    let _ = stream.write_all(request.as_bytes()).await;
    let mut response = Vec::new();
    let _ = stream.take(MAX_RESPONSE).read_to_end(&mut response).await;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::net::TcpListener;

    use super::*;
    use crate::id::Namespace;

    const SHORT: Duration = Duration::from_millis(300);

    fn reserve() -> Command {
        Command::Reserve {
            namespace: Namespace::new("users").unwrap(),
            count: 25,
        }
    }

    /// Answers one connection with `response`, then closes it, and hands back
    /// the request it got. `None` leaves the connection open and silent.
    async fn leader(
        response: Option<&'static str>,
    ) -> (SocketAddr, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let n = stream.read(&mut buf).await.unwrap();
                assert!(n > 0, "the request ended early");
                request.extend_from_slice(&buf[..n]);
            }
            match response {
                Some(response) => stream.write_all(response.as_bytes()).await.unwrap(),
                None => tokio::time::sleep(Duration::from_secs(60)).await,
            }
            String::from_utf8(request).unwrap()
        });
        (addr, task)
    }

    #[tokio::test]
    async fn a_block_from_the_leader() {
        let (addr, task) = leader(Some(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 70\r\n\r\n\
             {\"namespace\":\"users\",\"start\":\"11\",\"end\":\"36\",\"count\":\"25\"}",
        ))
        .await;
        let result = execute(&addr.to_string(), &reserve(), SHORT).await;
        let request = task.await.unwrap();
        assert_eq!(result.unwrap(), Outcome::Reserved(IdRange::new(11, 36)));
        assert!(
            request.starts_with("POST /v1/ns/users/blocks?size=25 HTTP/1.1\r\n"),
            "{request}"
        );
        assert!(
            request.contains("\r\nx-quorid-forwarded: 1\r\n"),
            "{request}"
        );
        assert!(request.contains("\r\nConnection: close\r\n"), "{request}");
    }

    #[tokio::test]
    async fn refusals_become_errors() {
        let cases: [(&str, &str); 7] = [
            (
                "HTTP/1.1 421 Misdirected Request\r\n\r\n\
                 {\"error\":\"not_leader\",\"message\":\"this node is not the leader\",\"leader\":\"10.0.0.3:7070\"}",
                "NotLeader { leader: Some(\"10.0.0.3:7070\") }",
            ),
            (
                "HTTP/1.1 421 Misdirected Request\r\n\r\n{\"error\":\"not_leader\"}",
                "NotLeader { leader: None }",
            ),
            (
                "HTTP/1.1 409 Conflict\r\n\r\n{\"error\":\"exhausted\"}",
                "Exhausted(Namespace(\"users\"))",
            ),
            (
                "HTTP/1.1 400 Bad Request\r\n\r\n\
                 {\"error\":\"invalid_request\",\"message\":\"invalid request: count must be at least 1\"}",
                "InvalidRequest(\"count must be at least 1\")",
            ),
            (
                "HTTP/1.1 503 Service Unavailable\r\n\r\n\
                 {\"error\":\"unavailable\",\"message\":\"backend unavailable: no quorum\"}",
                "Unavailable(\"the leader at ADDR did not give a block: HTTP/1.1 503 Service Unavailable: backend unavailable: no quorum\")",
            ),
            // Not what a leader says.
            (
                "HTTP/1.1 200 OK\r\n\r\nnot json",
                "Unavailable(\"the leader at ADDR did not give a block: HTTP/1.1 200 OK: \")",
            ),
            (
                "",
                "Unavailable(\"the leader at ADDR did not give a block: no answer: \")",
            ),
        ];
        for (response, expected) in cases {
            let (addr, task) = leader(Some(response)).await;
            let err = execute(&addr.to_string(), &reserve(), SHORT)
                .await
                .unwrap_err();
            task.await.unwrap();
            assert_eq!(
                format!("{err:?}"),
                expected.replace("ADDR", &addr.to_string())
            );
        }
    }

    #[tokio::test]
    async fn a_leader_that_does_not_answer() {
        let (addr, task) = leader(None).await;
        let err = execute(&addr.to_string(), &reserve(), SHORT)
            .await
            .unwrap_err();
        task.abort();
        assert_eq!(
            err.to_string(),
            format!("backend unavailable: cannot reach the leader at {addr}: timed out")
        );
    }

    #[tokio::test]
    async fn a_leader_that_is_down() {
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let err = execute(&addr.to_string(), &reserve(), SHORT)
            .await
            .unwrap_err();
        assert!(
            err.to_string().starts_with(&format!(
                "backend unavailable: cannot reach the leader at {addr}: "
            )),
            "{err}"
        );
    }
}

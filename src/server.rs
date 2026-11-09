//! HTTP/JSON API.
//!
//! | method | path                          | description                              |
//! |--------|-------------------------------|------------------------------------------|
//! | POST   | `/v1/ns/{namespace}/ids`      | `?count=1&format=mania&padded=false`     |
//! | POST   | `/v1/ns/{namespace}/blocks`   | lease a raw range, `?size=<block_size>`  |
//! | GET    | `/v1/decode/{value}`          | `?format=mania`, shows every encoding    |
//! | GET    | `/v1/status`                  | backend and allocator info               |
//! | GET    | `/healthz`                    | liveness                                 |
//! | POST   | `/v1/cluster/voters`          | add a node as a voter (JSON body)        |
//! | DELETE | `/v1/cluster/voters/{id}`     | remove a voter                           |
//!
//! IDs are always JSON strings: JSON numbers cannot carry 128-bit integers.
//!
//! A node that forwards a write to the leader does it with a `/blocks` request
//! that carries [`FORWARDED_HEADER`], which the receiving node serves without
//! forwarding it any further.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::allocator::{Allocator, AllocatorConfig};
use crate::backend::{Backend, BackendStatus, Members, Peer};
use crate::codec::{DecodeError, Format};
use crate::error::Error;
use crate::id::{IdRange, Namespace};

/// Upper bound on IDs returned by one `/ids` call (use `/blocks` for more).
pub const MAX_IDS_PER_REQUEST: u64 = 10_000;

/// Set on the `/blocks` requests that one node sends another to forward a
/// write: the receiver must not forward them again.
pub const FORWARDED_HEADER: &str = "x-quorid-forwarded";

pub struct AppState<B> {
    allocator: Arc<Allocator<B>>,
    default_format: Format,
}

impl<B> Clone for AppState<B> {
    fn clone(&self) -> Self {
        AppState {
            allocator: self.allocator.clone(),
            default_format: self.default_format,
        }
    }
}

pub fn router<B: Backend>(allocator: Arc<Allocator<B>>, default_format: Format) -> Router {
    Router::new()
        .route("/v1/ns/{namespace}/ids", post(allocate_ids::<B>))
        .route("/v1/ns/{namespace}/blocks", post(lease_block::<B>))
        .route("/v1/decode/{value}", get(decode::<B>))
        .route("/v1/cluster/voters", post(add_voter::<B>))
        .route("/v1/cluster/voters/{id}", delete(remove_voter::<B>))
        .route("/v1/status", get(status::<B>))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(AppState {
            allocator,
            default_format,
        })
}

#[derive(Deserialize)]
struct IdsQuery {
    count: Option<u64>,
    format: Option<Format>,
    #[serde(default)]
    padded: bool,
}

#[derive(Serialize)]
struct IdsResponse {
    namespace: Namespace,
    format: Format,
    ids: Vec<String>,
}

async fn allocate_ids<B: Backend>(
    State(state): State<AppState<B>>,
    Path(namespace): Path<String>,
    Query(query): Query<IdsQuery>,
) -> Result<Json<IdsResponse>, ApiError> {
    let namespace = Namespace::new(namespace)?;
    let count = query.count.unwrap_or(1);
    if !(1..=MAX_IDS_PER_REQUEST).contains(&count) {
        return Err(Error::InvalidRequest(format!(
            "count must be between 1 and {MAX_IDS_PER_REQUEST}, use /blocks for larger ranges"
        ))
        .into());
    }
    let format = query.format.unwrap_or(state.default_format);
    let ranges = state.allocator.allocate(&namespace, count).await?;
    let ids = ranges
        .iter()
        .flat_map(IdRange::iter)
        .map(|id| format.encode(id, query.padded))
        .collect();
    Ok(Json(IdsResponse {
        namespace,
        format,
        ids,
    }))
}

#[derive(Deserialize)]
struct BlockQuery {
    size: Option<u64>,
}

#[derive(Serialize)]
struct BlockResponse {
    namespace: Namespace,
    #[serde(flatten)]
    range: IdRange,
    count: String,
}

async fn lease_block<B: Backend>(
    State(state): State<AppState<B>>,
    Path(namespace): Path<String>,
    Query(query): Query<BlockQuery>,
    headers: HeaderMap,
) -> Result<Json<BlockResponse>, ApiError> {
    let namespace = Namespace::new(namespace)?;
    let size = query.size.unwrap_or(state.allocator.config().block_size);
    let range = if headers.contains_key(FORWARDED_HEADER) {
        state.allocator.lease_here(&namespace, size).await?
    } else {
        state.allocator.lease(&namespace, size).await?
    };
    Ok(Json(BlockResponse {
        namespace,
        count: range.len().to_string(),
        range,
    }))
}

#[derive(Deserialize)]
struct DecodeQuery {
    format: Option<Format>,
}

#[derive(Serialize)]
struct DecodeResponse {
    decimal: String,
    encodings: BTreeMap<Format, String>,
}

async fn decode<B: Backend>(
    State(state): State<AppState<B>>,
    Path(value): Path<String>,
    Query(query): Query<DecodeQuery>,
) -> Result<Json<DecodeResponse>, ApiError> {
    let id = query
        .format
        .unwrap_or(state.default_format)
        .decode(&value)?;
    Ok(Json(DecodeResponse {
        decimal: id.to_string(),
        encodings: Format::ALL
            .iter()
            .map(|&f| (f, f.encode(id, false)))
            .collect(),
    }))
}

/// Adds the node described by the JSON body, `{"id": 4, "raft_addr":
/// "10.0.0.4:7171", "client_addr": "10.0.0.4:7070"}`: the same as `--peer`
/// takes. Only the leader does it; the others answer 421 with the leader.
async fn add_voter<B: Backend>(
    State(state): State<AppState<B>>,
    body: Bytes,
) -> Result<Json<Members>, ApiError> {
    let peer: Peer = serde_json::from_slice(&body)
        .map_err(|e| Error::InvalidRequest(format!("the body is not a node: {e}")))?;
    Ok(Json(state.allocator.backend().add_voter(peer).await?))
}

async fn remove_voter<B: Backend>(
    State(state): State<AppState<B>>,
    Path(id): Path<String>,
) -> Result<Json<Members>, ApiError> {
    let id = id
        .parse()
        .map_err(|_| Error::InvalidRequest(format!("{id:?} is not a node ID")))?;
    Ok(Json(state.allocator.backend().remove_voter(id).await?))
}

#[derive(Serialize)]
struct StatusResponse {
    name: &'static str,
    version: &'static str,
    default_format: Format,
    backend: BackendStatus,
    allocator: AllocatorConfig,
}

async fn status<B: Backend>(State(state): State<AppState<B>>) -> Json<StatusResponse> {
    Json(StatusResponse {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        default_format: state.default_format,
        backend: state.allocator.backend().status(),
        allocator: state.allocator.config().clone(),
    })
}

pub enum ApiError {
    Core(Error),
    Decode(DecodeError),
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError::Core(e)
    }
}

impl From<crate::id::InvalidNamespace> for ApiError {
    fn from(e: crate::id::InvalidNamespace) -> Self {
        ApiError::Core(e.into())
    }
}

impl From<DecodeError> for ApiError {
    fn from(e: DecodeError) -> Self {
        ApiError::Decode(e)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    leader: Option<String>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error, leader) = match &self {
            ApiError::Decode(_) => (StatusCode::BAD_REQUEST, "invalid_id", None),
            ApiError::Core(e) => match e {
                Error::InvalidNamespace(_) => (StatusCode::BAD_REQUEST, "invalid_namespace", None),
                Error::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request", None),
                Error::Exhausted(_) => (StatusCode::CONFLICT, "exhausted", None),
                Error::NotLeader { leader } => (
                    StatusCode::MISDIRECTED_REQUEST,
                    "not_leader",
                    leader.clone(),
                ),
                Error::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", None),
                Error::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage", None),
            },
        };
        let message = match self {
            ApiError::Core(e) => e.to_string(),
            ApiError::Decode(e) => e.to_string(),
        };
        if status.is_server_error() {
            tracing::error!(%message, "request failed");
        }
        (
            status,
            Json(ErrorBody {
                error,
                message,
                leader,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Method, Request};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;
    use crate::backend::{Backend, BackendStatus, MemoryBackend};
    use crate::command::{Command, Outcome};
    use crate::state::IdState;

    fn app() -> Router {
        app_with(MemoryBackend::new())
    }

    fn app_with(backend: MemoryBackend) -> Router {
        let allocator = Allocator::new(Arc::new(backend), AllocatorConfig::with_block_size(100));
        router(Arc::new(allocator), Format::Mania)
    }

    async fn call(app: &Router, method: Method, uri: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn allocate_ids_in_formats() {
        let app = app();
        let (status, body) = call(&app, Method::POST, "/v1/ns/users/ids?count=3").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ids"], serde_json::json!(["🤬", "🤣", "🤗"]));
        assert_eq!(body["format"], "mania");

        let (_, body) = call(
            &app,
            Method::POST,
            "/v1/ns/users/ids?format=decimal&padded=true",
        )
        .await;
        assert_eq!(body["ids"][0], format!("{:039}", 4));

        let (_, body) = call(&app, Method::POST, "/v1/ns/users/ids?format=fruity").await;
        assert_eq!(body["ids"][0], "🍋"); // id 5
    }

    #[tokio::test]
    async fn blocks_and_validation() {
        let app = app();
        let (status, body) = call(&app, Method::POST, "/v1/ns/users/blocks?size=5000").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["start"], "1");
        assert_eq!(body["end"], "5001");
        assert_eq!(body["count"], "5000");

        for path in ["/v1/ns/Bad%20Name/ids", "/v1/ns/Bad%20Name/blocks"] {
            let (status, body) = call(&app, Method::POST, path).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(body["error"], "invalid_namespace", "{path}");
        }

        let (status, _) = call(&app, Method::POST, "/v1/ns/users/ids?count=0").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = call(&app, Method::POST, "/v1/ns/users/ids?count=10001").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// A node that is not the leader: it forwards what it is asked to, and
    /// answers what was forwarded to it with a pointer to the leader.
    struct Follower;

    impl Backend for Follower {
        async fn execute(&self, _: Command) -> crate::error::Result<Outcome> {
            Ok(Outcome::Reserved(IdRange::new(7, 9)))
        }

        async fn execute_here(&self, _: Command) -> crate::error::Result<Outcome> {
            Err(Error::NotLeader {
                leader: Some("leader:7070".into()),
            })
        }

        /// Says what it was asked, for the test to see it arrive.
        async fn add_voter(&self, peer: Peer) -> crate::error::Result<Members> {
            Ok(Members {
                voters: vec![peer.id],
                learners: peer.client_addr.iter().map(|_| 0).collect(),
                config_pending: false,
            })
        }

        async fn remove_voter(&self, id: u64) -> crate::error::Result<Members> {
            Ok(Members {
                voters: Vec::new(),
                learners: vec![id],
                config_pending: true,
            })
        }

        fn status(&self) -> BackendStatus {
            MemoryBackend::new().status()
        }
    }

    async fn send(app: &Router, method: Method, uri: &str, body: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn the_cluster_is_changed_through_the_backend() {
        let allocator = Allocator::new(Arc::new(Follower), AllocatorConfig::with_block_size(100));
        let app = router(Arc::new(allocator), Format::Mania);
        let voters = "/v1/cluster/voters";

        let node = r#"{"id":4,"raft_addr":"10.0.0.4:7171","client_addr":"10.0.0.4:7070"}"#;
        let (status, body) = send(&app, Method::POST, voters, node).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["voters"], serde_json::json!([4]));
        assert_eq!(body["learners"], serde_json::json!([0]));
        // The client address is optional.
        let node = r#"{"id":5,"raft_addr":"10.0.0.5:7171"}"#;
        let (status, body) = send(&app, Method::POST, voters, node).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["voters"], serde_json::json!([5]));
        assert_eq!(body["learners"], serde_json::json!([]));

        let (status, body) = send(&app, Method::POST, voters, "nope").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_request");
        let message = body["message"].as_str().unwrap();
        assert!(message.starts_with("invalid request: the body is not a node: "));

        let (status, body) = send(&app, Method::DELETE, "/v1/cluster/voters/3", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["learners"], serde_json::json!([3]));
        assert_eq!(body["config_pending"], true);
        let (status, body) = send(&app, Method::DELETE, "/v1/cluster/voters/x", "").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["message"], "invalid request: \"x\" is not a node ID");
    }

    #[tokio::test]
    async fn backends_without_a_cluster_refuse_to_change_it() {
        let app = app();
        let node = r#"{"id":4,"raft_addr":"10.0.0.4:7171"}"#;
        let (status, body) = send(&app, Method::POST, "/v1/cluster/voters", node).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["message"],
            "invalid request: this backend has no cluster to change"
        );
        let (status, _) = send(&app, Method::DELETE, "/v1/cluster/voters/1", "").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // What is wrong with the request is said first.
        let (_, body) = send(&app, Method::POST, "/v1/cluster/voters", "{}").await;
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("the body is not a node")
        );
        let (_, body) = send(&app, Method::DELETE, "/v1/cluster/voters/x", "").await;
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("is not a node ID")
        );
    }

    async fn forwarded(app: &Router, uri: &str) -> (StatusCode, Value) {
        let request = Request::post(uri)
            .header(FORWARDED_HEADER, "1")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn forwarded_blocks_are_not_forwarded_again() {
        // A backend that never forwards serves them like any other.
        let (status, body) = forwarded(&app(), "/v1/ns/users/blocks?size=3").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["start"], "1");
        assert_eq!(body["end"], "4");

        let allocator = Allocator::new(Arc::new(Follower), AllocatorConfig::with_block_size(100));
        let app = router(Arc::new(allocator), Format::Mania);
        let (status, body) = call(&app, Method::POST, "/v1/ns/users/blocks").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["start"], "7");

        let (status, body) = forwarded(&app, "/v1/ns/users/blocks").await;
        assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(body["leader"], "leader:7070");

        let (status, _) = call(&app, Method::GET, "/v1/status").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn decode_and_status() {
        let app = app();
        let (status, body) = call(&app, Method::GET, "/v1/decode/10?format=base62").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["decimal"], "62");
        assert_eq!(body["encodings"]["base32"], "1Y");

        // The default format is mania; %F0%9F%A4%AC is 🤬, i.e. ID 1.
        let (status, body) = call(&app, Method::GET, "/v1/decode/%F0%9F%A4%AC").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["decimal"], "1");

        let (status, body) = call(&app, Method::GET, "/v1/decode/!!").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_id");

        let response = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");

        let (status, body) = call(&app, Method::GET, "/v1/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["backend"]["kind"], "memory");
        assert!(body["backend"].get("members").is_none());
        assert_eq!(body["default_format"], "mania");
        assert_eq!(body["allocator"]["block_size"], 100);
    }

    #[tokio::test]
    async fn exhausted_namespace_is_a_conflict() {
        let nearly_full = format!(r#"{{"version":1,"next":{{"users":"{}"}}}}"#, u128::MAX - 5);
        let state = IdState::restore(nearly_full.as_bytes()).unwrap();
        let app = app_with(MemoryBackend::with_state(state));

        let (status, body) = call(&app, Method::POST, "/v1/ns/users/blocks?size=10").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "exhausted");
        let (status, body) = forwarded(&app, "/v1/ns/users/blocks?size=10").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "exhausted");

        // Fewer than a block (100) is left, so /ids cannot lease one either.
        let (status, body) = call(&app, Method::POST, "/v1/ns/users/ids").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "exhausted");

        let (status, body) = call(&app, Method::POST, "/v1/ns/users/blocks?size=5").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["end"], u128::MAX.to_string());
    }

    async fn render(error: Error) -> (StatusCode, Value) {
        let response = ApiError::from(error).into_response();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// Errors only replicated backends produce, mapped to their HTTP form.
    #[tokio::test]
    async fn backend_errors_map_to_statuses() {
        let (status, body) = render(Error::NotLeader {
            leader: Some("10.0.0.2:7070".into()),
        })
        .await;
        assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(body["error"], "not_leader");
        assert_eq!(body["leader"], "10.0.0.2:7070");

        let (_, body) = render(Error::NotLeader { leader: None }).await;
        assert!(body.get("leader").is_none());

        let (status, body) = render(Error::Unavailable("no quorum".into())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "unavailable");
        assert_eq!(body["message"], "backend unavailable: no quorum");

        let (status, body) = render(Error::Storage("disk full".into())).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "storage");
    }
}

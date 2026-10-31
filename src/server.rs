//! HTTP/JSON API.
//!
//! | method | path                          | description                              |
//! |--------|-------------------------------|------------------------------------------|
//! | POST   | `/v1/ns/{namespace}/ids`      | `?count=1&format=mania&padded=false`     |
//! | POST   | `/v1/ns/{namespace}/blocks`   | lease a raw range, `?size=<block_size>`  |
//! | GET    | `/v1/decode/{value}`          | `?format=mania`, shows every encoding    |
//! | GET    | `/v1/status`                  | backend and allocator info               |
//! | GET    | `/healthz`                    | liveness                                 |
//!
//! IDs are always JSON strings: JSON numbers cannot carry 128-bit integers.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::allocator::{Allocator, AllocatorConfig};
use crate::backend::{Backend, BackendStatus};
use crate::codec::{DecodeError, Format};
use crate::error::Error;
use crate::id::{IdRange, Namespace};

/// Upper bound on IDs returned by one `/ids` call (use `/blocks` for more).
pub const MAX_IDS_PER_REQUEST: u64 = 10_000;

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
) -> Result<Json<BlockResponse>, ApiError> {
    let namespace = Namespace::new(namespace)?;
    let size = query.size.unwrap_or(state.allocator.config().block_size);
    let range = state.allocator.lease(&namespace, size).await?;
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
    use crate::backend::MemoryBackend;
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

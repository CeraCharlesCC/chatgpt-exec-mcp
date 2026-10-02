use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::activity::{ActivityEvent, ActivityHub};
use crate::agent_pool::{AdminMember, AdminMessage, AgentIdentity, AgentPoolStore};
use crate::agent_pool_context::AgentPoolContext;

struct WebError {
    status: StatusCode,
    message: String,
}

impl WebError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

#[derive(Clone)]
pub struct WebUiState {
    pool: Option<AgentPoolContext>,
    activity: Arc<ActivityHub>,
    started: Instant,
}

impl WebUiState {
    /// Construct a dashboard for process activity without agent-pool controls.
    pub fn without_pool(activity: Arc<ActivityHub>) -> Self {
        Self::new(None, activity)
    }

    /// Construct a dashboard scoped to one authenticated account.
    pub fn for_principal(
        store: Arc<AgentPoolStore>,
        principal: String,
        activity: Arc<ActivityHub>,
    ) -> Result<Self, rmcp::ErrorData> {
        // Validate the account before exposing any handlers.
        store.identities_for_session(&principal, None)?;
        Ok(Self::new(
            Some(AgentPoolContext::new(store, principal)),
            activity,
        ))
    }

    pub(crate) fn new(pool: Option<AgentPoolContext>, activity: Arc<ActivityHub>) -> Self {
        Self {
            pool,
            activity,
            started: Instant::now(),
        }
    }

    fn pool_context(&self) -> Result<&AgentPoolContext, WebError> {
        match self.pool.as_ref() {
            Some(context) => Ok(context),
            None => Err(WebError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "agent pool is not configured",
            )),
        }
    }
}

#[derive(Serialize)]
struct PoolSnapshot {
    name: String,
    agents: Vec<AdminMember>,
}

#[derive(Serialize)]
struct SnapshotResponse {
    version: &'static str,
    generated_ms: i64,
    uptime_seconds: u64,
    pools: Vec<PoolSnapshot>,
    admin_messages: Vec<AdminMessage>,
    events: Vec<ActivityEvent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendRequest {
    pool: String,
    target: String,
    message: String,
    #[serde(default)]
    in_reply_to: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminateRequest {
    pool: String,
    agent: String,
}

#[derive(Serialize)]
struct TerminateResponse {
    terminated: bool,
}

pub fn router(state: WebUiState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/_admin/api/snapshot", get(snapshot))
        .route("/_admin/api/send", post(send))
        .route("/_admin/api/terminate", post(terminate))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("webui.html"))
}

async fn snapshot(State(state): State<WebUiState>) -> Result<Response, WebError> {
    let (members, admin_messages) = match state.pool_context() {
        Ok(context) => (
            context
                .store()
                .admin_members(context.principal())
                .map_err(pool_error)?,
            context
                .store()
                .admin_messages(context.principal(), 200)
                .map_err(pool_error)?,
        ),
        Err(_) => (Vec::new(), Vec::new()),
    };

    let mut grouped = BTreeMap::<String, Vec<AdminMember>>::new();
    for member in members {
        grouped.entry(member.pool.clone()).or_default().push(member);
    }
    let pools = grouped
        .into_iter()
        .map(|(name, agents)| PoolSnapshot { name, agents })
        .collect();

    json_response(
        StatusCode::OK,
        &SnapshotResponse {
            version: env!("CARGO_PKG_VERSION"),
            generated_ms: now_ms(),
            uptime_seconds: state.started.elapsed().as_secs(),
            pools,
            admin_messages,
            events: state.activity.snapshot(400),
        },
    )
}

async fn send(State(state): State<WebUiState>, body: Bytes) -> Result<Response, WebError> {
    let request: SendRequest = serde_json::from_slice(&body)
        .map_err(|error| WebError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    let context = state.pool_context()?;
    let result = context
        .store()
        .admin_send(
            context.principal(),
            &request.pool,
            &request.target,
            &request.message,
            request.in_reply_to.as_deref(),
        )
        .map_err(pool_error)?;
    let agents = result
        .recipients
        .iter()
        .map(|agent| AgentIdentity {
            pool: request.pool.clone(),
            agent: agent.clone(),
        })
        .collect();
    state.activity.emit(
        "admin.message.send",
        "info",
        None,
        agents,
        json!({
            "pool": request.pool,
            "target": request.target,
            "message": request.message,
            "in_reply_to": request.in_reply_to,
            "message_id": result.message_id.clone(),
            "delivery_count": result.recipients.len(),
        }),
    );
    json_response(StatusCode::OK, &result)
}

async fn terminate(State(state): State<WebUiState>, body: Bytes) -> Result<Response, WebError> {
    let request: TerminateRequest = serde_json::from_slice(&body)
        .map_err(|error| WebError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    let context = state.pool_context()?;
    let terminated = context
        .store()
        .admin_terminate(context.principal(), &request.pool, &request.agent)
        .map_err(pool_error)?;
    state.activity.emit(
        "admin.membership.terminate",
        if terminated { "warn" } else { "info" },
        None,
        vec![AgentIdentity {
            pool: request.pool.clone(),
            agent: request.agent.clone(),
        }],
        json!({"pool": request.pool, "agent": request.agent, "terminated": terminated}),
    );
    json_response(StatusCode::OK, &TerminateResponse { terminated })
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Result<Response, WebError> {
    let body = serde_json::to_string(value).map_err(|_| {
        WebError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "JSON serialization failed",
        )
    })?;
    Ok((status, [(header::CONTENT_TYPE, "application/json")], body).into_response())
}

fn pool_error(error: rmcp::ErrorData) -> WebError {
    WebError::new(StatusCode::BAD_REQUEST, error.message.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

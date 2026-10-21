use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header, uri::Authority};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::activity::{ActivityEvent, ActivityHub, ActivityPage};
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
    csrf_token: String,
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
            csrf_token: Uuid::new_v4().to_string(),
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
    csrf_token: String,
    generated_ms: i64,
    uptime_seconds: u64,
    pools: Vec<PoolSnapshot>,
    admin_messages: Vec<AdminMessage>,
    admin_messages_next_cursor: Option<String>,
    events: Vec<ActivityEvent>,
    correlated_events: Vec<ActivityEvent>,
    events_next_before_id: Option<u64>,
    events_has_more: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotQuery {
    cursor: Option<String>,
    #[serde(default = "default_all")]
    all: bool,
}

fn default_all() -> bool {
    true
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsQuery {
    before_id: Option<u64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct EventsResponse {
    events: Vec<ActivityEvent>,
    correlated_events: Vec<ActivityEvent>,
    next_before_id: Option<u64>,
    has_more: bool,
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
struct AckRequest {
    message_id: String,
}

#[derive(Serialize)]
struct AckResponse {
    acknowledged: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClearRequest {}

#[derive(Serialize)]
struct ClearResponse {
    cleared_events: usize,
    cleared_admin_messages: usize,
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
        .route("/_admin/assets/{*path}", get(asset))
        .route("/_admin/api/snapshot", get(snapshot))
        .route("/_admin/api/events", get(events))
        .route("/_admin/api/send", post(send))
        .route("/_admin/api/ack", post(ack))
        .route("/_admin/api/clear", post(clear))
        .route("/_admin/api/terminate", post(terminate))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("webui.html"))
}

async fn asset(Path(path): Path<String>) -> Response {
    // Explicitly register each browser asset so a standalone binary never has
    // to read front-end files from the working directory at runtime.
    let (content_type, contents) = match path.trim_start_matches('/') {
        "styles.css" => ("text/css; charset=utf-8", include_str!("webui/styles.css")),
        "app.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/app.js"),
        ),
        "state.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/state.js"),
        ),
        "api.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/api.js"),
        ),
        "dom.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/dom.js"),
        ),
        "model/events.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/model/events.js"),
        ),
        "model/messages.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/model/messages.js"),
        ),
        "controllers/activity.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/controllers/activity.js"),
        ),
        "controllers/inbox.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/controllers/inbox.js"),
        ),
        "views/activity.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/views/activity.js"),
        ),
        "views/pools.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/views/pools.js"),
        ),
        "views/inbox.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/views/inbox.js"),
        ),
        "views/menu.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/views/menu.js"),
        ),
        "views/tool-output.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/views/tool-output.js"),
        ),
        "vendor/prism.js" => (
            "text/javascript; charset=utf-8",
            include_str!("webui/vendor/prism.js"),
        ),
        // Add future native ES modules to this allowlist when they are extracted.
        _ => return StatusCode::NOT_FOUND.into_response(),
    };

    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        contents,
    )
        .into_response()
}

async fn snapshot(
    State(state): State<WebUiState>,
    headers: HeaderMap,
    Query(query): Query<SnapshotQuery>,
) -> Result<Response, WebError> {
    validate_admin_host(&headers)?;
    if query.all && query.cursor.is_some() {
        return Err(WebError::new(
            StatusCode::BAD_REQUEST,
            "all cannot be combined with cursor",
        ));
    }
    let limit = if query.all { 500 } else { 200 };
    let (members, mut page) = match state.pool_context() {
        Ok(context) => (
            context
                .store()
                .admin_members_async(context.principal())
                .await
                .map_err(pool_error)?,
            context
                .store()
                .admin_messages_page_async(context.principal(), limit, query.cursor.as_deref())
                .await
                .map_err(pool_error)?,
        ),
        Err(_) => (
            Vec::new(),
            crate::agent_pool::AdminMessagesPage {
                messages: Vec::new(),
                next_cursor: None,
            },
        ),
    };

    let mut grouped = BTreeMap::<String, Vec<AdminMember>>::new();
    for member in members {
        grouped.entry(member.pool.clone()).or_default().push(member);
    }
    let pools = grouped
        .into_iter()
        .map(|(name, agents)| PoolSnapshot { name, agents })
        .collect();

    if query.all
        && let Some(context) = &state.pool
    {
        while let Some(cursor) = page.next_cursor.take() {
            let next = context
                .store()
                .admin_messages_page_async(context.principal(), 500, Some(&cursor))
                .await
                .map_err(pool_error)?;
            page.messages.extend(next.messages);
            page.next_cursor = next.next_cursor;
        }
    }

    let build_response = move || {
        let activity_page = if query.all {
            ActivityPage {
                events: state.activity.snapshot_all().map_err(|_| {
                    WebError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "could not load all events",
                    )
                })?,
                correlated_events: Vec::new(),
                next_before_id: None,
                has_more: false,
            }
        } else {
            // Normal polling only touches RAM, including correlation metadata.
            state.activity.snapshot_page(200)
        };
        json_response(
            StatusCode::OK,
            &SnapshotResponse {
                version: env!("CARGO_PKG_VERSION"),
                csrf_token: state.csrf_token.clone(),
                generated_ms: now_ms(),
                uptime_seconds: state.started.elapsed().as_secs(),
                pools,
                admin_messages: page.messages,
                admin_messages_next_cursor: page.next_cursor,
                events: activity_page.events,
                correlated_events: activity_page.correlated_events,
                events_next_before_id: activity_page.next_before_id,
                events_has_more: activity_page.has_more,
            },
        )
    };
    if query.all {
        // Both decompression and potentially large JSON serialization run off-thread.
        tokio::task::spawn_blocking(build_response)
            .await
            .map_err(|_| {
                WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "could not build snapshot",
                )
            })?
    } else {
        build_response()
    }
}

async fn events(
    State(state): State<WebUiState>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Result<Response, WebError> {
    validate_admin_host(&headers)?;
    if query.before_id == Some(0) {
        return Err(WebError::new(
            StatusCode::BAD_REQUEST,
            "before_id must be positive",
        ));
    }
    let limit = query.limit.unwrap_or(200);
    if !(1..=200).contains(&limit) {
        return Err(WebError::new(
            StatusCode::BAD_REQUEST,
            "limit must be between 1 and 200",
        ));
    }
    let hub = state.activity.clone();
    // A history page may decompress a chunk or touch disk. Do not block a
    // Tokio worker while it loads.
    let page = tokio::task::spawn_blocking(move || hub.page(query.before_id, limit))
        .await
        .map_err(|_| WebError::new(StatusCode::INTERNAL_SERVER_ERROR, "could not load events"))?;
    json_response(
        StatusCode::OK,
        &EventsResponse {
            events: page.events,
            correlated_events: page.correlated_events,
            next_before_id: page.next_before_id,
            has_more: page.has_more,
        },
    )
}

async fn send(
    State(state): State<WebUiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, WebError> {
    validate_admin_post(&state, &headers)?;
    let request: SendRequest = serde_json::from_slice(&body)
        .map_err(|error| WebError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    let context = state.pool_context()?;
    let result = context
        .store()
        .admin_send_async(
            context.principal(),
            &request.pool,
            &request.target,
            &request.message,
            request.in_reply_to.as_deref(),
        )
        .await
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

async fn ack(
    State(state): State<WebUiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, WebError> {
    validate_admin_post(&state, &headers)?;
    let request: AckRequest = serde_json::from_slice(&body)
        .map_err(|error| WebError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    let context = state.pool_context()?;
    let acknowledged = context
        .store()
        .admin_ack_async(context.principal(), &request.message_id)
        .await
        .map_err(pool_error)?;
    json_response(StatusCode::OK, &AckResponse { acknowledged })
}

async fn clear(
    State(state): State<WebUiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, WebError> {
    validate_admin_post(&state, &headers)?;
    let _: ClearRequest = serde_json::from_slice(&body)
        .map_err(|error| WebError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    let cleared_admin_messages = match state.pool.as_ref() {
        Some(context) => context
            .store()
            .admin_clear_messages_async(context.principal())
            .await
            .map_err(pool_error)?,
        None => 0,
    };
    let cleared_events = state.activity.clear();
    json_response(
        StatusCode::OK,
        &ClearResponse {
            cleared_events,
            cleared_admin_messages,
        },
    )
}

async fn terminate(
    State(state): State<WebUiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, WebError> {
    validate_admin_post(&state, &headers)?;
    let request: TerminateRequest = serde_json::from_slice(&body)
        .map_err(|error| WebError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    let context = state.pool_context()?;
    let terminated = context
        .store()
        .admin_terminate_async(context.principal(), &request.pool, &request.agent)
        .await
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
    Ok((
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response())
}

/// The dashboard is exposed on loopback only. Reject other Host values even
/// on GET: otherwise DNS rebinding could disclose the CSRF token in snapshots.
fn validate_admin_host(headers: &HeaderMap) -> Result<&str, WebError> {
    let forbidden = || WebError::new(StatusCode::FORBIDDEN, "invalid WebUI host");
    let host = headers
        .get(header::HOST)
        .ok_or_else(forbidden)?
        .to_str()
        .map_err(|_| forbidden())?;
    let authority: Authority = host.parse().map_err(|_| forbidden())?;
    if host.contains('@')
        || !matches!(
            authority.host().to_ascii_lowercase().as_str(),
            "localhost" | "127.0.0.1" | "[::1]" | "::1"
        )
    {
        return Err(forbidden());
    }
    Ok(host)
}

fn validate_admin_post(state: &WebUiState, headers: &HeaderMap) -> Result<(), WebError> {
    let host = validate_admin_host(headers)?;
    let forbidden = || WebError::new(StatusCode::FORBIDDEN, "invalid WebUI origin or CSRF token");
    let origin = headers
        .get(header::ORIGIN)
        .ok_or_else(forbidden)?
        .to_str()
        .map_err(|_| forbidden())?;
    if ![format!("http://{host}"), format!("https://{host}")]
        .iter()
        .any(|expected| origin.eq_ignore_ascii_case(expected))
    {
        return Err(forbidden());
    }
    if headers
        .get("x-webui-csrf-token")
        .is_none_or(|token| token.as_bytes() != state.csrf_token.as_bytes())
    {
        return Err(forbidden());
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(WebError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        ));
    }
    Ok(())
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

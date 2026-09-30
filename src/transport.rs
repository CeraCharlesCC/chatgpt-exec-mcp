//! Stateless MCP over a private Unix socket. rmcp owns protocol dispatch.

use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use axum::body::Body;
use axum::http::{Method, StatusCode, header::CONTENT_LENGTH};
use axum::middleware::Next;
use axum::response::Response;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

use crate::ExecMcpServer;

const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

pub async fn serve_unix(
    server: ExecMcpServer,
    path: &Path,
    shutdown: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) -> anyhow::Result<()> {
    prepare_socket_path(path)?;
    let listener = tokio::net::UnixListener::bind(path).context("bind MCP Unix socket failed")?;
    let socket = SocketGuard::new(path)?;
    let server = server.with_http_protocol();
    let events_enabled = server.events_enabled();
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.stateless_protocol_metadata_required = true;
    config.json_response = true;
    config.max_request_body_bytes = MAX_BODY_BYTES;
    let cancellation = config.cancellation_token.clone();
    let service: StreamableHttpService<ExecMcpServer, LocalSessionManager> =
        StreamableHttpService::new(move || Ok(server.clone()), Default::default(), config);
    let router = axum::Router::new()
        .route("/readyz", axum::routing::get(|| async { "ready" }))
        .nest_service("/mcp", service)
        .layer(axum::middleware::from_fn_with_state(
            events_enabled,
            discovery_events,
        ));
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown.await;
            cancellation.cancel();
        })
        .await
        .context("streamable HTTP server failed");
    drop(socket);
    result
}

/// The SDK does not yet model OpenAI's Events discovery capability. Augment
/// only a successful server/discover response; keep negotiation in rmcp.
async fn discovery_events(
    axum::extract::State(events_enabled): axum::extract::State<bool>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !events_enabled || request.method() != Method::POST || request.uri().path() != "/mcp" {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
    };
    let discover = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .is_some_and(|value| value["method"] == "server/discover");
    let response = next
        .run(axum::http::Request::from_parts(parts, Body::from(bytes)))
        .await;
    if !discover || !response.status().is_success() {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return self::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "discovery response too large",
            );
        }
    };
    let mut value = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(value) => value,
        Err(_) => return Response::from_parts(parts, Body::from(bytes)),
    };
    if let Some(capabilities) = value
        .get_mut("result")
        .and_then(|result| result.get_mut("capabilities"))
        .and_then(serde_json::Value::as_object_mut)
    {
        capabilities.insert("events".to_owned(), serde_json::json!({}));
        parts.headers.remove(CONTENT_LENGTH);
        return Response::from_parts(parts, Body::from(value.to_string()));
    }
    Response::from_parts(parts, Body::from(bytes))
}

fn response(status: StatusCode, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(body))
        .expect("static response")
}

fn prepare_socket_path(path: &Path) -> anyhow::Result<()> {
    use std::io::ErrorKind;
    use std::os::unix::fs::FileTypeExt as _;
    anyhow::ensure!(
        path.is_absolute(),
        "listen Unix socket path must be absolute"
    );
    let parent = path.parent().context("listen Unix socket has no parent")?;
    anyhow::ensure!(parent.is_dir(), "listen Unix socket parent must exist");
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "listen Unix socket path exists and is not a socket"
            );
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => anyhow::bail!("listen Unix socket is already active"),
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::ConnectionRefused | ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path).context("remove stale Unix socket failed")?;
                }
                Err(error) => return Err(error).context("probe existing Unix socket failed"),
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect Unix socket failed"),
    }
    Ok(())
}

/// Remove only the socket this process bound, never a replacement path.
struct SocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl SocketGuard {
    fn new(path: &Path) -> anyhow::Result<Self> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let metadata = std::fs::symlink_metadata(path)?;
        let guard = Self {
            path: path.to_owned(),
            dev: metadata.dev(),
            ino: metadata.ino(),
        };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        Ok(guard)
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket()
                && metadata.dev() == self.dev
                && metadata.ino() == self.ino
        }) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    async fn events_call(
        client: &reqwest::Client,
        method: &str,
        mut params: serde_json::Value,
        session: Option<&str>,
    ) -> serde_json::Value {
        use serde_json::json;
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "custom-meta-test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
            "principal": "forged-owner"
        });
        if let Some(session) = session {
            params["_meta"]["openai/session"] = json!(session);
        }
        let mut request = client
            .post("http://localhost/mcp")
            .header("MCP-Protocol-Version", "2026-07-28")
            .header("Mcp-Method", method)
            .header("Accept", "application/json, text/event-stream");
        if method == "tools/call" {
            request = request.header("Mcp-Name", params["name"].as_str().unwrap());
        }
        let response = request
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        assert!(!response.headers().contains_key("Mcp-Session-Id"));
        let body = response.text().await.unwrap();
        assert_eq!(status, 200, "{method}: {body}");
        serde_json::from_str(&body).unwrap()
    }

    #[tokio::test]
    async fn custom_request_metadata_binds_sender_over_strict_http() {
        use crate::events::{EventStore, PoolMembersArgs};
        use crate::{Config, ProcessManager};
        use serde_json::json;
        use std::os::unix::fs::PermissionsExt;
        use std::sync::Arc;
        use std::time::Duration;

        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let config_path = directory.path().join("config.json");
        std::fs::write(
            &config_path,
            json!({
                "version":1, "workspace":".", "shell":"/bin/bash", "output_store_dir":"outputs",
                "child_env":{"inherit":[], "rules":[]}
            })
            .to_string(),
        )
        .unwrap();
        let manager = ProcessManager::new(Config::load(&config_path).unwrap()).unwrap();
        let store = Arc::new(EventStore::open(&directory.path().join("events.sqlite3")).unwrap());
        let owner = "configured-owner";
        let callback = "https://example.com/verified-callback";
        // Callback verification is covered by webhook tests. Cache it only in
        // this unit fixture; no worker is started, so no outbound request occurs.
        store.cache_verified_for_test(owner, callback);
        let server =
            ExecMcpServer::new(Arc::clone(&manager)).with_events(Arc::clone(&store), owner.into());
        let socket = directory.path().join("mcp.sock");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            serve_unix(server, &socket, async {
                stopped.await?;
                Ok(())
            })
            .await
        });
        let client = reqwest::Client::builder()
            .unix_socket(directory.path().join("mcp.sock"))
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if client
                    .get("http://localhost/readyz")
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                assert!(!task.is_finished(), "HTTP fixture exited during startup");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let subscription = |agent| {
            json!({"name":"multiagent.message",
            "arguments":{"pool":"project", "agent":agent},
            "delivery":{"mode":"webhook", "url":callback,
                "secret":"whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}})
        };
        let alice = events_call(
            &client,
            "events/subscribe",
            subscription("alice"),
            Some("chat-a"),
        )
        .await;
        assert!(alice["result"]["id"].is_string(), "{alice}");
        let bob = events_call(
            &client,
            "events/subscribe",
            subscription("bob"),
            Some("chat-b"),
        )
        .await;
        assert!(bob["result"]["id"].is_string(), "{bob}");
        let refresh = events_call(&client, "events/subscribe", subscription("alice"), None).await;
        assert_eq!(refresh["result"]["id"], alice["result"]["id"]);
        let send = events_call(
            &client,
            "tools/call",
            json!({"name":"pool_send",
            "arguments":{"pool":"project", "agent":"global", "message":"metadata survived"}}),
            Some("chat-a"),
        )
        .await;
        assert_eq!(
            send["result"]["structuredContent"]["recipients"],
            json!(["bob"]),
            "{send}"
        );
        let override_sender = events_call(&client, "tools/call", json!({"name":"pool_send",
            "arguments":{"pool":"project", "agent":"alice", "message":"forged", "from_agent":"bob"}}), Some("chat-a")).await;
        assert_eq!(override_sender["result"]["isError"], true);
        assert!(
            store
                .members(
                    "forged-owner",
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents
                .is_empty()
        );
        let unsubscribe = events_call(
            &client,
            "events/unsubscribe",
            json!({"name":"multiagent.message",
            "arguments":{"pool":"project", "agent":"bob"},
            "delivery":{"mode":"webhook", "url":callback}}),
            Some("chat-b"),
        )
        .await;
        assert_eq!(unsubscribe["result"], json!({}));
        assert_eq!(
            store
                .members(
                    owner,
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents,
            ["alice"]
        );

        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        manager.shutdown_all().await;
    }

    #[test]
    fn refuses_active_sockets_regular_files_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.sock");
        let listener = UnixListener::bind(&path).unwrap();
        assert!(prepare_socket_path(&path).is_err());
        assert!(path.exists());
        drop(listener);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "keep").unwrap();
        assert!(prepare_socket_path(&path).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep");
        let link = dir.path().join("link.sock");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(prepare_socket_path(&link).is_err());
        assert!(link.is_symlink());
    }

    #[test]
    fn stale_socket_is_reclaimed_and_cleanup_preserves_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.sock");
        drop(UnixListener::bind(&path).unwrap());
        prepare_socket_path(&path).unwrap();
        assert!(!path.exists());
        let listener = UnixListener::bind(&path).unwrap();
        let guard = SocketGuard::new(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();
        drop(guard);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "replacement");
        drop(listener);
    }
}

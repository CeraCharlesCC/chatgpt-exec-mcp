use std::sync::Arc;
use std::time::Duration;

use chatgpt_exec_mcp::activity::ActivityHub;
use chatgpt_exec_mcp::agent_pool::{AgentPoolStore, PoolSendArgs};
use chatgpt_exec_mcp::webui::{WebUiState, router};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::oneshot;

struct Dashboard {
    client: Client,
    url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Dashboard {
    async fn start(state: WebUiState) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stopped) = oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, router(state))
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            url,
            shutdown: Some(shutdown),
        }
    }

    async fn snapshot(&self) -> Value {
        let response = self
            .client
            .get(format!("{}/_admin/api/snapshot", self.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        response.json().await.unwrap()
    }

    async fn post(&self, endpoint: &str, body: Value, status: StatusCode) -> Value {
        let response = self
            .client
            .post(format!("{}/_admin/api/{endpoint}", self.url))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let text = response.text().await.unwrap();
        if status == StatusCode::OK {
            serde_json::from_str(&text).unwrap()
        } else {
            json!(text)
        }
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn send_args(target: Option<&str>, message: Option<&str>) -> PoolSendArgs {
    PoolSendArgs {
        pool: "team".into(),
        target: target.map(str::to_owned),
        message: message.map(str::to_owned),
        in_reply_to: None,
        action: None,
    }
}

#[tokio::test]
async fn disabled_pool_snapshot_keeps_activity_and_rejects_controls() {
    let activity = Arc::new(ActivityHub::new(10));
    activity.emit(
        "tool.finish",
        "info",
        Some("session"),
        vec![],
        json!({"output": "ready"}),
    );
    let server = Dashboard::start(WebUiState::without_pool(activity)).await;
    let snapshot = server.snapshot().await;
    assert_eq!(snapshot["version"], env!("CARGO_PKG_VERSION"));
    assert!(snapshot["generated_ms"].as_i64().unwrap() > 0);
    assert!(snapshot["uptime_seconds"].is_u64());
    assert_eq!(snapshot["pools"], json!([]));
    assert_eq!(snapshot["admin_messages"], json!([]));
    assert_eq!(snapshot["events"][0]["detail"]["output"], "ready");
    for (endpoint, body) in [
        (
            "send",
            json!({"pool":"team", "target":"Alice", "message":"hello"}),
        ),
        ("terminate", json!({"pool":"team", "agent":"Alice"})),
    ] {
        assert_eq!(
            server
                .post(endpoint, body, StatusCode::SERVICE_UNAVAILABLE)
                .await,
            "agent pool is not configured"
        );
    }
}

#[tokio::test]
async fn handlers_scope_snapshot_send_and_termination_to_principal() {
    let workspace = TempDir::new().unwrap();
    let store = Arc::new(
        AgentPoolStore::open_with_names(
            &workspace.path().join("private/pool.sqlite"),
            Duration::from_secs(60),
            vec!["Alice".into(), "Bob".into()],
        )
        .unwrap(),
    );
    for principal in ["account-a", "account-b"] {
        let sent = store
            .send(
                principal,
                send_args(Some("admin"), Some(principal)),
                Some("session"),
            )
            .unwrap();
        assert_eq!(sent.assigned_agent.as_deref(), Some("Alice"));
    }
    let server = Dashboard::start(
        WebUiState::for_principal(
            store.clone(),
            "account-a".into(),
            Arc::new(ActivityHub::new(10)),
        )
        .unwrap(),
    )
    .await;
    let snapshot = server.snapshot().await;
    assert_eq!(snapshot["pools"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["pools"][0]["agents"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["pools"][0]["agents"][0]["agent"], "Alice");
    assert_eq!(snapshot["admin_messages"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["admin_messages"][0]["message"], "account-a");

    server
        .post(
            "send",
            json!({"pool":"team", "target":"Alice", "message":"hello", "principal":"account-b"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    server
        .post(
            "terminate",
            json!({"pool":"team", "agent":"Alice", "principal":"account-b"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    let sent = server
        .post(
            "send",
            json!({"pool":"team", "target":"Alice", "message":"hello"}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(sent["recipients"], json!(["Alice"]));
    let a_turn = store
        .start_tool("account-a", Some("session"))
        .await
        .unwrap();
    let b_turn = store
        .start_tool("account-b", Some("session"))
        .await
        .unwrap();
    let a_messages = store
        .finish_tool_turn("account-a", Some("session"), a_turn.as_ref())
        .await
        .unwrap();
    let b_messages = store
        .finish_tool_turn("account-b", Some("session"), b_turn.as_ref())
        .await
        .unwrap();
    assert_eq!(a_messages.len(), 1);
    assert_eq!(a_messages[0].message, "hello");
    assert!(b_messages.is_empty());

    assert_eq!(
        server
            .post(
                "terminate",
                json!({"pool":"team", "agent":"Alice"}),
                StatusCode::OK
            )
            .await,
        json!({"terminated":true})
    );
    assert_eq!(server.snapshot().await["pools"], json!([]));
    assert_eq!(store.admin_members("account-b").unwrap().len(), 1);
    assert_eq!(
        server
            .post(
                "terminate",
                json!({"pool":"team", "agent":"Alice"}),
                StatusCode::OK
            )
            .await,
        json!({"terminated":false})
    );
    server
        .post(
            "send",
            json!({"pool":"team", "target":"Alice", "message":"after termination"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    let events = server.snapshot().await["events"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        events
            .iter()
            .map(|event| event["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "admin.message.send",
            "admin.membership.terminate",
            "admin.membership.terminate"
        ]
    );
}

#[test]
fn public_dashboard_constructor_rejects_invalid_principals() {
    let workspace = TempDir::new().unwrap();
    let store =
        Arc::new(AgentPoolStore::open(&workspace.path().join("private/pool.sqlite")).unwrap());
    assert!(WebUiState::for_principal(store, "".into(), Arc::new(ActivityHub::new(10))).is_err());
}

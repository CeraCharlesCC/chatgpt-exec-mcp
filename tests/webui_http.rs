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
    let server = Dashboard::start(WebUiState::without_pool(activity.clone())).await;
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
        ("ack", json!({"message_id":"msg_example"})),
    ] {
        assert_eq!(
            server
                .post(endpoint, body, StatusCode::SERVICE_UNAVAILABLE)
                .await,
            "agent pool is not configured"
        );
    }
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await,
        json!({"cleared_events":1, "cleared_admin_messages":0})
    );
    assert_eq!(server.snapshot().await["events"], json!([]));
    let next_id = activity.emit("tool.start", "info", None, vec![], json!({}));
    assert_eq!(next_id, 2, "clearing must not reuse event IDs");
}

#[tokio::test]
async fn clear_removes_all_history_and_only_the_current_accounts_admin_inbox() {
    let workspace = TempDir::new().unwrap();
    let path = workspace.path().join("private/pool.sqlite");
    let store = Arc::new(AgentPoolStore::open(&path).unwrap());
    // Include messages beyond the dashboard's 200-row snapshot, in two pools.
    for i in 0..205 {
        let mut args = send_args(Some("admin"), Some("history"));
        if i % 2 == 1 {
            args.pool = "second-pool".into();
        }
        let sent = store.send("account-a", args, Some("session")).unwrap();
        if i == 0 {
            store
                .admin_ack("account-a", sent.message_id.as_deref().unwrap())
                .unwrap();
        }
    }
    store
        .send(
            "account-b",
            send_args(Some("admin"), Some("keep")),
            Some("session"),
        )
        .unwrap();
    let members = store.admin_members("account-a").unwrap();
    let agent = &members.iter().find(|m| m.pool == "team").unwrap().agent;
    store
        .admin_send("account-a", "team", agent, "pending delivery", None)
        .unwrap();
    let activity = Arc::new(ActivityHub::new(500));
    for _ in 0..450 {
        activity.emit(
            "tool.finish",
            "info",
            None,
            vec![],
            json!({"output":"history"}),
        );
    }
    let server = Dashboard::start(
        WebUiState::for_principal(store.clone(), "account-a".into(), activity.clone()).unwrap(),
    )
    .await;
    server
        .post(
            "clear",
            json!({"principal":"account-b"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    let before = server.snapshot().await;
    assert_eq!(before["events"].as_array().unwrap().len(), 400);
    assert_eq!(before["admin_messages"].as_array().unwrap().len(), 200);
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await,
        json!({"cleared_events":450, "cleared_admin_messages":205})
    );
    for _ in 0..2 {
        let after = server.snapshot().await;
        assert_eq!(after["events"], json!([]));
        assert_eq!(after["admin_messages"], json!([]));
        assert_eq!(after["pools"], before["pools"]);
        assert_eq!(
            store.admin_messages("account-b", 10).unwrap()[0].message,
            "keep"
        );
    }
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await,
        json!({"cleared_events":0, "cleared_admin_messages":0})
    );
    let turn = store
        .start_tool("account-a", Some("session"))
        .await
        .unwrap();
    let offered = store
        .finish_tool_turn("account-a", Some("session"), turn.as_ref())
        .await
        .unwrap();
    assert_eq!(offered.len(), 1);
    assert_eq!(offered[0].message, "pending delivery");
    let id = activity.emit("tool.start", "info", None, vec![], json!({}));
    assert_eq!(id, 451);
    assert_eq!(server.snapshot().await["events"][0]["id"], id);
    drop(server);
    drop(store);
    let reopened = AgentPoolStore::open(&path).unwrap();
    assert!(
        reopened
            .admin_messages("account-a", 500)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened.admin_messages("account-b", 10).unwrap()[0].message,
        "keep"
    );
}

#[tokio::test]
async fn handlers_scope_snapshot_send_ack_and_termination_to_principal() {
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
    assert_eq!(snapshot["admin_messages"][0]["acknowledged"], false);

    let message_id = snapshot["admin_messages"][0]["message_id"].clone();
    let other_message = store.admin_messages("account-b", 10).unwrap().remove(0);
    for body in [
        json!({"message_id":message_id, "principal":"account-b"}),
        json!({"message_id":""}),
        json!({}),
    ] {
        server.post("ack", body, StatusCode::BAD_REQUEST).await;
    }
    for id in [&other_message.message_id, "msg_missing"] {
        assert_eq!(
            server
                .post("ack", json!({"message_id":id}), StatusCode::OK)
                .await,
            json!({"acknowledged":false})
        );
    }
    // Retrying an ACK is safe, and snapshots retain the message and its status.
    for _ in 0..2 {
        assert_eq!(
            server
                .post("ack", json!({"message_id":message_id}), StatusCode::OK)
                .await,
            json!({"acknowledged":true})
        );
        let messages = server.snapshot().await["admin_messages"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["message_id"], message_id);
        assert_eq!(messages[0]["message"], "account-a");
        assert_eq!(messages[0]["acknowledged"], true);
        assert!(!store.admin_messages("account-b", 10).unwrap()[0].acknowledged);
    }

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

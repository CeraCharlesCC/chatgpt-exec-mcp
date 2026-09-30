#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

const PRINCIPAL: &str = "account-a";

struct Server {
    child: Option<Child>,
    workspace: TempDir,
    database: PathBuf,
    client: reqwest::Client,
}

impl Server {
    async fn new() -> Self {
        let workspace = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let database = workspace.path().join("agent-pool.sqlite3");
        let socket = workspace.path().join("mcp.sock");
        std::fs::write(
            workspace.path().join("config.json"),
            json!({
                "version": 1,
                "workspace": ".",
                "shell": "/bin/bash",
                "output_store_dir": "outputs",
                "child_env": {"inherit": [], "rules": []},
                "agent_pool": {"database_path": database, "principal": PRINCIPAL}
            })
            .to_string(),
        )
        .unwrap();
        let mut server = Self {
            child: None,
            workspace,
            database,
            client: reqwest::Client::builder()
                .unix_socket(socket)
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        };
        server.start().await;
        server
    }

    async fn start(&mut self) {
        let socket = self.workspace.path().join("mcp.sock");
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
                .arg("--config")
                .arg(self.workspace.path().join("config.json"))
                .arg("--listen-unix")
                .arg(socket)
                .current_dir(self.workspace.path())
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "core exited during startup"
            );
            if self
                .client
                .get("http://localhost/readyz")
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("core readiness timed out");
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success());
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = child.kill();
            let _ = child.wait();
            assert!(std::thread::panicking(), "core shutdown timed out");
        }
    }

    async fn call(&self, method: &str, mut params: Value, session: Option<&str>) -> Value {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "agent-pool-http-test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
            "principal": "forged-account"
        });
        if let Some(session) = session {
            params["_meta"]["openai/session"] = json!(session);
        }
        let mut request = self
            .client
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
        assert!(!response.headers().contains_key("Mcp-Session-Id"));
        let status = response.status();
        let body = response.text().await.unwrap();
        let value: Value = serde_json::from_str(&body).unwrap();
        let expected = match value["error"]["code"].as_i64() {
            Some(-32602) => 400,
            Some(-32601) => 404,
            _ => 200,
        };
        assert_eq!(status.as_u16(), expected, "{method}: {body}");
        value
    }

    async fn tool(&self, name: &str, arguments: Value, session: Option<&str>) -> Value {
        self.call(
            "tools/call",
            json!({"name": name, "arguments": arguments}),
            session,
        )
        .await
    }

    async fn send(
        &self,
        session: &str,
        register_as: Option<&str>,
        target: &str,
        message: &str,
    ) -> Value {
        let mut arguments = json!({
            "pool": "Imperator",
            "target": target,
            "message": message
        });
        if let Some(register_as) = register_as {
            arguments["register_as"] = json!(register_as);
        }
        self.tool("pool_send", arguments, Some(session)).await
    }

    async fn exec(&self, session: &str) -> Value {
        self.tool("exec_command", json!({"cmd":"true"}), Some(session))
            .await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

fn structured(value: &Value) -> &Value {
    &value["result"]["structuredContent"]
}

#[tokio::test]
async fn piggyback_agent_pool_acceptance_path() {
    let mut server = Server::new().await;

    let discovery = server.call("server/discover", json!({}), None).await;
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2026-07-28"])
    );
    assert!(discovery["result"]["capabilities"].get("events").is_none());
    for method in ["events/list", "events/subscribe", "events/unsubscribe"] {
        assert_eq!(
            server.call(method, json!({}), None).await["error"]["code"],
            -32601
        );
    }

    let tools = server.call("tools/list", json!({}), None).await;
    let tools = tools["result"]["tools"].as_array().unwrap();
    for name in [
        "exec_command",
        "start_session",
        "write_stdin",
        "wait_for_exit",
        "pool_members",
        "pool_send",
    ] {
        assert!(
            tools.iter().any(|tool| tool["name"] == name),
            "missing {name}"
        );
    }
    for forbidden in ["pool_join", "pool_leave", "pool_poll", "pool_ack"] {
        assert!(!tools.iter().any(|tool| tool["name"] == forbidden));
    }
    let send_schema = tools
        .iter()
        .find(|tool| tool["name"] == "pool_send")
        .unwrap();
    let required: std::collections::BTreeSet<_> = send_schema["inputSchema"]["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(required, ["pool", "target", "message"].into());
    assert_eq!(
        required.len(),
        send_schema["inputSchema"]["required"]
            .as_array()
            .unwrap()
            .len()
    );
    assert!(
        send_schema["inputSchema"]["properties"]
            .get("register_as")
            .is_some()
    );
    for property in [
        "sender",
        "message_id",
        "delivery_count",
        "membership_created",
    ] {
        assert!(
            send_schema["outputSchema"]["properties"]
                .get(property)
                .is_some(),
            "missing pool_send output property {property}"
        );
    }
    let exec_schema = tools
        .iter()
        .find(|tool| tool["name"] == "exec_command")
        .unwrap();
    assert!(
        exec_schema["outputSchema"]["properties"]
            .get("peer_messages")
            .is_some()
    );

    let augustus = server
        .send("session-a", Some("Augustus"), "global", "Augustus joined")
        .await;
    assert_eq!(structured(&augustus)["sender"], "Augustus");
    assert_eq!(structured(&augustus)["recipients"], json!([]));
    assert_eq!(structured(&augustus)["delivery_count"], 0);
    assert_eq!(structured(&augustus)["membership_created"], true);
    assert_eq!(structured(&augustus)["message_id"], Value::Null);

    let empty_again = server
        .send("session-a", None, "global", "still alone")
        .await;
    assert_eq!(structured(&empty_again)["sender"], "Augustus");
    assert_eq!(structured(&empty_again)["recipients"], json!([]));
    assert_eq!(structured(&empty_again)["delivery_count"], 0);
    assert_eq!(structured(&empty_again)["membership_created"], false);
    assert_eq!(structured(&empty_again)["message_id"], Value::Null);

    let tiberius = server
        .send("session-b", Some("Tiberius"), "global", "Tiberius joined")
        .await;
    assert_eq!(structured(&tiberius)["sender"], "Tiberius");
    assert_eq!(structured(&tiberius)["recipients"], json!(["Augustus"]));
    assert_eq!(structured(&tiberius)["delivery_count"], 1);
    assert_eq!(structured(&tiberius)["membership_created"], true);
    assert!(structured(&tiberius)["message_id"].as_str().is_some());

    let join_offer = server.exec("session-a").await;
    assert_eq!(
        structured(&join_offer)["peer_messages"][0]["from"],
        "Tiberius"
    );
    assert_eq!(structured(&join_offer)["peer_messages"][0]["to"], "global");
    assert!(
        join_offer["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Tiberius joined")
    );

    let redundant_registration = server
        .send(
            "session-b",
            Some("Tiberius"),
            "Augustus",
            "redundant registration",
        )
        .await;
    assert_eq!(redundant_registration["result"]["isError"], true);
    assert!(
        redundant_registration["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("register_as is only valid")
    );

    let targeted = server
        .send("session-b", None, "Augustus", "API side is updated")
        .await;
    assert_eq!(structured(&targeted)["sender"], "Tiberius");
    assert_eq!(structured(&targeted)["recipients"], json!(["Augustus"]));
    assert_eq!(structured(&targeted)["delivery_count"], 1);
    assert_eq!(structured(&targeted)["membership_created"], false);
    assert!(structured(&targeted)["message_id"].as_str().is_some());
    let offered = server.exec("session-a").await;
    assert_eq!(
        structured(&offered)["peer_messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        structured(&offered)["peer_messages"][0]["message"],
        "API side is updated"
    );
    assert_eq!(structured(&offered)["peer_messages"][0]["to"], "Augustus");
    let acknowledged = server.exec("session-a").await;
    assert!(structured(&acknowledged).get("peer_messages").is_none());

    // Ordinary tool-error results still surface unread peer messages.
    server
        .send("session-b", None, "Augustus", "error-path")
        .await;
    let errored = server
        .tool("exec_command", json!({"cmd":""}), Some("session-a"))
        .await;
    assert_eq!(errored["result"]["isError"], true);
    assert_eq!(
        structured(&errored)["peer_messages"][0]["message"],
        "error-path"
    );
    assert!(
        errored["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("error-path")
    );
    let error_ack = server.exec("session-a").await;
    assert!(structured(&error_ack).get("peer_messages").is_none());

    let conflict = server
        .send("session-c", Some("Augustus"), "global", "collision")
        .await;
    assert_eq!(conflict["result"]["isError"], true);
    assert!(
        conflict["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("already active")
    );

    let livia = server
        .send("session-c", Some("Livia"), "global", "Livia joined")
        .await;
    assert_eq!(
        structured(&livia)["recipients"],
        json!(["Augustus", "Tiberius"])
    );
    let fanout = server.send("session-b", None, "global", "broadcast").await;
    assert_eq!(
        structured(&fanout)["recipients"],
        json!(["Augustus", "Livia"])
    );
    assert_eq!(
        structured(&fanout)["peer_messages"][0]["from"],
        "Livia",
        "pool_send must piggyback the sender's unread inbox"
    );

    // Clear Augustus's outstanding offers, then queue one unread message and restart.
    let _ = server.exec("session-a").await;
    let _ = server.exec("session-a").await;
    server
        .send("session-b", None, "Augustus", "survives restart")
        .await;
    server.stop();
    server.start().await;
    let durable = server.exec("session-a").await;
    assert_eq!(
        structured(&durable)["peer_messages"][0]["message"],
        "survives restart"
    );
    let _ = server.exec("session-a").await;

    // Batching is bounded; the remainder stays queued for later tool calls.
    for index in 0..40 {
        server
            .send("session-b", None, "Augustus", &format!("batch-{index:02}"))
            .await;
    }
    let first_batch = server.exec("session-a").await;
    assert_eq!(
        structured(&first_batch)["peer_messages"]
            .as_array()
            .unwrap()
            .len(),
        32
    );
    let second_batch = server.exec("session-a").await;
    assert_eq!(
        structured(&second_batch)["peer_messages"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
    let empty = server.exec("session-a").await;
    assert!(structured(&empty).get("peer_messages").is_none());

    // Expiry removes the member and cascades any undelivered inbox rows.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    Connection::open(&server.database)
        .unwrap()
        .execute(
            "UPDATE members SET expires=?1 WHERE principal=?2 AND pool='Imperator' AND agent='Livia'",
            rusqlite::params![now - 1, PRINCIPAL],
        )
        .unwrap();
    let members = server
        .tool(
            "pool_members",
            json!({"pool":"Imperator"}),
            Some("session-a"),
        )
        .await;
    assert_eq!(
        structured(&members)["agents"],
        json!(["Augustus", "Tiberius"])
    );

    // Request metadata cannot forge the configured account scope.
    let forged = Connection::open(&server.database)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM members WHERE principal='forged-account'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(forged, 0);
}

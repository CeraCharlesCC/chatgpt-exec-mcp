#![cfg(unix)]

use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

const PRINCIPAL: &str = "account-a";

struct Server {
    child: Option<Child>,
    workspace: TempDir,
    client: reqwest::Client,
    webui_client: reqwest::Client,
    webui_address: SocketAddr,
}

impl Server {
    async fn new() -> Self {
        let workspace = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let database = workspace.path().join("agent-pool.sqlite3");
        let socket = workspace.path().join("mcp.sock");
        let webui_probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let webui_address = webui_probe.local_addr().unwrap();
        drop(webui_probe);
        std::fs::write(
            workspace.path().join("config.json"),
            json!({
                "version": 1,
                "workspace": ".",
                "shell": "/bin/bash",
                "output_store_dir": "outputs",
                "child_env": {"inherit": [], "rules": []},
                "agent_pool": {
                    "database_path": database,
                    "principal": PRINCIPAL,
                    "agent_name_dictionary": ["Augustus", "Tiberius", "Livia", "Debug"]
                }
            })
            .to_string(),
        )
        .unwrap();
        let mut server = Self {
            child: None,
            workspace,
            client: reqwest::Client::builder()
                .unix_socket(socket)
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            webui_client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            webui_address,
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
                .arg("--webui-listen")
                .arg(self.webui_address.to_string())
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
            let core_ready = self
                .client
                .get("http://localhost/readyz")
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            let webui_ready = self
                .webui_client
                .get(format!("http://{}/_admin/api/snapshot", self.webui_address))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            if core_ready && webui_ready {
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

    async fn send(&self, session: &str, target: &str, message: &str) -> Value {
        let arguments = json!({
            "pool": "Imperator",
            "target": target,
            "message": message
        });
        self.tool("pool_send", arguments, Some(session)).await
    }

    async fn leave(&self, session: &str) -> Value {
        self.tool(
            "pool_send",
            json!({"pool":"Imperator", "action":"leave"}),
            Some(session),
        )
        .await
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

    let send_schema = tools
        .iter()
        .find(|tool| tool["name"] == "pool_send")
        .unwrap();
    assert_eq!(send_schema["inputSchema"]["type"], "object");
    assert_eq!(send_schema["inputSchema"]["additionalProperties"], false);
    assert!(send_schema["inputSchema"].get("oneOf").is_none());
    assert_eq!(send_schema["inputSchema"]["required"], json!(["pool"]));
    assert!(
        send_schema["inputSchema"]["properties"]
            .get("register_as")
            .is_none()
    );
    assert_eq!(
        send_schema["inputSchema"]["$defs"]["PoolAction"]["enum"],
        json!(["leave"])
    );
    assert!(
        send_schema["inputSchema"]["properties"]["message"]
            .get("maxLength")
            .is_none(),
        "runtime enforces the UTF-8 byte limit; JSON Schema maxLength counts characters"
    );
    for removed in ["sender", "delivery_count", "membership_created"] {
        assert!(
            send_schema["outputSchema"]["properties"]
                .get(removed)
                .is_none()
        );
    }
    for kept in [
        "assigned_agent",
        "message_id",
        "recipients",
        "peer_messages",
    ] {
        assert!(
            send_schema["outputSchema"]["properties"]
                .get(kept)
                .is_some()
        );
    }

    let exec_schema = tools
        .iter()
        .find(|tool| tool["name"] == "exec_command")
        .unwrap();
    let exec_required = exec_schema["outputSchema"]["required"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        exec_schema["outputSchema"]["properties"]
            .get("call_wall_time_seconds")
            .is_none()
    );
    for sparse in ["output", "output_truncated", "output_encoding_loss"] {
        assert!(
            exec_schema["outputSchema"]["properties"]
                .get(sparse)
                .is_some()
        );
        assert!(!exec_required.iter().any(|value| value == sparse));
    }

    let members_schema = tools
        .iter()
        .find(|tool| tool["name"] == "pool_members")
        .unwrap();
    assert_eq!(members_schema["annotations"]["readOnlyHint"], true);
    assert!(
        members_schema["outputSchema"]["properties"]
            .get("self_agent")
            .is_some()
    );

    for arguments in [
        json!({"pool":"Imperator","target":null,"message":"x"}),
        json!({"pool":"Imperator","target":"global","message":"x","action":null}),
    ] {
        let invalid = server
            .tool("pool_send", arguments, Some("invalid-null"))
            .await;
        assert_eq!(invalid["result"]["isError"], true);
    }

    // WebUI administration continues to use the automatically assigned identity.
    let debug_to_admin = server
        .tool(
            "pool_send",
            json!({"pool":"WebUI","target":"admin","message":"need human input"}),
            Some("session-debug"),
        )
        .await;
    let debug_agent = structured(&debug_to_admin)["assigned_agent"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(structured(&debug_to_admin)["recipients"], json!(["admin"]));

    let snapshot: Value = server
        .webui_client
        .get(format!(
            "http://{}/_admin/api/snapshot",
            server.webui_address
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(snapshot["pools"].as_array().unwrap().iter().any(|pool| {
        pool["name"] == "WebUI"
            && pool["agents"]
                .as_array()
                .unwrap()
                .iter()
                .any(|agent| agent["agent"] == debug_agent)
    }));
    assert_eq!(snapshot["admin_messages"][0]["from"], debug_agent);

    let admin_send: Value = server
        .webui_client
        .post(format!("http://{}/_admin/api/send", server.webui_address))
        .json(&json!({"pool":"WebUI","target":debug_agent,"message":"human reply"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(admin_send["recipients"], json!([debug_agent.clone()]));

    let debug_receive = server
        .tool(
            "pool_members",
            json!({"pool":"WebUI"}),
            Some("session-debug"),
        )
        .await;
    assert_eq!(structured(&debug_receive)["self_agent"], debug_agent);
    assert_eq!(
        structured(&debug_receive)["peer_messages"][0]["message"],
        "human reply"
    );

    // First send allocates a name; later sends infer it without another field.
    let augustus = server.send("session-a", "global", "joined").await;
    assert_eq!(structured(&augustus)["assigned_agent"], "Augustus");
    assert!(structured(&augustus).get("recipients").is_none());
    assert!(structured(&augustus).get("message_id").is_none());

    let again = server.send("session-a", "global", "still alone").await;
    assert!(structured(&again).get("assigned_agent").is_none());

    let tiberius = server.send("session-b", "global", "joined").await;
    assert_eq!(structured(&tiberius)["assigned_agent"], "Tiberius");
    assert_eq!(structured(&tiberius)["recipients"], json!(["Augustus"]));

    let join_offer = server.exec("session-a").await;
    assert_eq!(
        structured(&join_offer)["peer_messages"][0]["from"],
        "Tiberius"
    );
    assert!(
        join_offer["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("joined"),
        "text fallback should continue carrying peer messages"
    );

    let targeted = server
        .send("session-b", "Augustus", "API side is updated")
        .await;
    assert_eq!(structured(&targeted)["recipients"], json!(["Augustus"]));
    let offered = server.exec("session-a").await;
    assert_eq!(
        structured(&offered)["peer_messages"][0]["message"],
        "API side is updated"
    );
    let acknowledged = server.exec("session-a").await;
    assert!(structured(&acknowledged).get("peer_messages").is_none());

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
    assert_eq!(structured(&members)["self_agent"], "Augustus");

    // Explicit leave is idempotent and ordinary tool activity must not resurrect membership.
    let left = server.leave("session-a").await;
    assert_eq!(structured(&left), &json!({}));
    let _ = server.exec("session-a").await;
    let after_leave = server
        .tool(
            "pool_members",
            json!({"pool":"Imperator"}),
            Some("session-b"),
        )
        .await;
    assert_eq!(structured(&after_leave)["agents"], json!(["Tiberius"]));
    assert_eq!(structured(&after_leave)["self_agent"], "Tiberius");

    let mixed_leave = server
        .tool(
            "pool_send",
            json!({"pool":"Imperator","action":"leave","target":"global","message":"no"}),
            Some("session-b"),
        )
        .await;
    assert_eq!(mixed_leave["result"]["isError"], true);

    let rejoined = server.send("session-a", "global", "back").await;
    assert_eq!(structured(&rejoined)["assigned_agent"], "Augustus");
    let _ = server.exec("session-b").await; // acknowledge the rejoin broadcast

    // Error results still surface unread peer messages.
    server.send("session-b", "Augustus", "error-path").await;
    let errored = server
        .tool("exec_command", json!({"cmd":""}), Some("session-a"))
        .await;
    assert_eq!(errored["result"]["isError"], true);
    assert_eq!(
        structured(&errored)["peer_messages"][0]["message"],
        "error-path"
    );

    // Pending inbox rows remain durable across a core restart.
    let _ = server.exec("session-a").await;
    server
        .send("session-b", "Augustus", "survives restart")
        .await;
    server.stop();
    server.start().await;
    let durable = server.exec("session-a").await;
    assert_eq!(
        structured(&durable)["peer_messages"][0]["message"],
        "survives restart"
    );
}

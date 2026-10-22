#![cfg(unix)]

use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

const PRINCIPAL: &str = "account-a";

#[path = "mcp_agent_pool_http/view_image.rs"]
mod view_image;

struct Server {
    child: Option<Child>,
    workspace: TempDir,
    client: reqwest::Client,
    webui_client: reqwest::Client,
    webui_address: SocketAddr,
}

impl Server {
    async fn new() -> Self {
        Self::with_name_allocation(None).await
    }

    async fn with_name_allocation(allocation: Option<&str>) -> Self {
        let workspace = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let database = workspace.path().join("agent-pool.sqlite3");
        let socket = workspace.path().join("mcp.sock");
        let webui_probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let webui_address = webui_probe.local_addr().unwrap();
        drop(webui_probe);
        let mut config = json!({
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
        });
        if let Some(allocation) = allocation {
            config["agent_pool"]["agent_name_allocation"] = json!(allocation);
        }
        std::fs::write(workspace.path().join("config.json"), config.to_string()).unwrap();
        let mut server = Self {
            child: None,
            workspace,
            client: reqwest::Client::builder()
                .unix_socket(socket)
                .no_proxy()
                .timeout(Duration::from_secs(55))
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
        if method == "tools/call" {
            assert_simplified_metadata(&value["result"]["structuredContent"]);
            for content in value["result"]["content"].as_array().into_iter().flatten() {
                let text = content["text"].as_str().unwrap_or_default();
                for removed in [
                    "peer_messages",
                    "in_reply_to",
                    "sent_at_ms",
                    "offered_at_ms",
                ] {
                    assert!(
                        !text.contains(removed),
                        "removed metadata in tool text: {text}"
                    );
                }
            }
        }
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

fn assert_simplified_metadata(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !matches!(
                        key.as_str(),
                        "peer_messages" | "in_reply_to" | "sent_at_ms" | "offered_at_ms"
                    ),
                    "removed metadata {key}: {object:?}"
                );
                assert_simplified_metadata(value);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_simplified_metadata),
        _ => {}
    }
}

#[tokio::test]
async fn piggyback_agent_pool_acceptance_path() {
    agent_pool_acceptance_path(None).await;
}

#[tokio::test]
async fn piggyback_agent_pool_round_robin_acceptance_path() {
    agent_pool_acceptance_path(Some("round_robin")).await;
}

async fn agent_pool_acceptance_path(allocation: Option<&str>) {
    let mut server = Server::with_name_allocation(allocation).await;
    let rejoined_agent = "Augustus";

    let empty = server.exec("unjoined").await;
    assert_eq!(structured(&empty), &json!({"exit_code": 0}));
    assert_eq!(empty["result"]["content"][0]["text"], "exit_code=0");

    let discovery = server.call("server/discover", json!({}), None).await;
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2026-07-28"])
    );
    assert!(discovery["result"]["capabilities"].get("events").is_none());

    let tools = server.call("tools/list", json!({}), None).await;
    assert_simplified_metadata(&tools);
    let tools = tools["result"]["tools"].as_array().unwrap();
    for name in [
        "exec_command",
        "pool_wait",
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
    for kept in ["assigned_agent", "message_id", "recipients", "messages"] {
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
    for sparse in [
        "output",
        "output_truncated",
        "output_encoding_loss",
        "capture_error",
        "output_ref",
    ] {
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
    assert_eq!(members_schema["inputSchema"]["required"], json!(["pool"]));
    assert!(
        members_schema["inputSchema"]["properties"]
            .get("wait_seconds")
            .is_none()
    );
    assert!(!tools.iter().any(|tool| tool["name"] == "start_session"));
    let wait_schema = tools
        .iter()
        .find(|tool| tool["name"] == "pool_wait")
        .unwrap();
    assert_eq!(wait_schema["inputSchema"]["required"], json!(["pool"]));
    assert_eq!(
        wait_schema["inputSchema"]["properties"]["wait_seconds"]["minimum"],
        5
    );
    assert_eq!(
        wait_schema["inputSchema"]["properties"]["wait_seconds"]["maximum"],
        45
    );
    assert_eq!(
        wait_schema["inputSchema"]["properties"]["wait_seconds"]["default"],
        35
    );
    for required in ["reason", "messages", "membership_changed"] {
        assert!(
            wait_schema["outputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!(required))
        );
    }
    assert_eq!(
        wait_schema["outputSchema"]["$defs"]["PoolWaitReason"]["enum"],
        json!(["message", "membership_changed", "timeout"])
    );
    for tool in tools {
        assert_eq!(
            tool["outputSchema"]["$defs"]["PeerMessage"],
            send_schema["outputSchema"]["$defs"]["PeerMessage"],
            "all tools must share one incoming message schema: {}",
            tool["name"]
        );
        assert!(tool["outputSchema"]["properties"].get("messages").is_some());
        assert!(
            tool["outputSchema"]["properties"]
                .get("delivery_receipts")
                .is_some()
        );
    }
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
    let exec_event = snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["kind"] == "tool.finish" && event["session"] == "unjoined")
        .unwrap();
    let detail = &exec_event["detail"]["result"];
    assert_eq!(detail["exit_code"], 0);
    assert_eq!(detail["output_bytes"], 0);
    assert!(detail["call_wall_time_seconds"].as_f64().is_some());
    for omitted in [
        "session_id",
        "output",
        "output_truncated",
        "output_encoding_loss",
        "capture_error",
        "output_ref",
        "messages",
    ] {
        assert!(
            detail.get(omitted).is_none(),
            "unexpected {omitted}: {detail}"
        );
    }
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
        .header("origin", format!("http://{}", server.webui_address))
        .header(
            "x-webui-csrf-token",
            snapshot["csrf_token"].as_str().unwrap(),
        )
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
        structured(&debug_receive)["messages"][0]["message"],
        "human reply"
    );

    // First send allocates a name; later sends infer it without another field.
    let augustus = server.send("session-a", "global", "joined").await;
    assert_eq!(structured(&augustus)["assigned_agent"], "Augustus");
    assert_eq!(structured(&augustus)["recipients"], json!([]));
    assert!(structured(&augustus).get("message_id").is_none());

    let again = server.send("session-a", "global", "still alone").await;
    assert!(structured(&again).get("assigned_agent").is_none());

    let tiberius = server.send("session-b", "global", "joined").await;
    assert_eq!(structured(&tiberius)["assigned_agent"], "Tiberius");
    assert_eq!(structured(&tiberius)["recipients"], json!(["Augustus"]));

    let join_offer = server.exec("session-a").await;
    assert_eq!(structured(&join_offer)["messages"][0]["from"], "Tiberius");
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
        structured(&offered)["messages"][0]["message"],
        "API side is updated"
    );
    let acknowledged = server.exec("session-a").await;
    assert!(structured(&acknowledged).get("messages").is_none());

    let receipt_wait_started = Instant::now();
    let receipt = server
        .tool(
            "pool_wait",
            json!({"pool":"Imperator", "wait_seconds":5}),
            Some("session-b"),
        )
        .await;
    assert_eq!(structured(&receipt)["reason"], "timeout");
    assert!(receipt_wait_started.elapsed() >= Duration::from_millis(4500));
    assert_eq!(structured(&receipt)["messages"], json!([]));
    assert_eq!(
        structured(&receipt)["delivery_receipts"][0]["status"],
        "offered"
    );
    assert_eq!(
        structured(&receipt)["delivery_receipts"][0]["message_id"],
        structured(&targeted)["message_id"]
    );

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
    assert_eq!(structured(&left), &json!({"recipients":[]}));
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
    assert_eq!(structured(&rejoined)["assigned_agent"], rejoined_agent);
    let resumed = server
        .tool(
            "pool_members",
            json!({"pool":"Imperator"}),
            Some("session-a"),
        )
        .await;
    assert_eq!(structured(&resumed)["self_agent"], rejoined_agent);
    let _ = server.exec("session-b").await; // acknowledge the rejoin broadcast

    // Error results still surface unread peer messages.
    server.send("session-b", rejoined_agent, "error-path").await;
    let errored = server
        .tool("exec_command", json!({"cmd":""}), Some("session-a"))
        .await;
    assert_eq!(errored["result"]["isError"], true);
    assert_eq!(structured(&errored)["messages"][0]["message"], "error-path");

    // Pending inbox rows remain durable across a core restart.
    let _ = server.exec("session-a").await;
    server
        .send("session-b", rejoined_agent, "survives restart")
        .await;
    server.stop();
    server.start().await;
    let durable = server.exec("session-a").await;
    assert_eq!(
        structured(&durable)["messages"][0]["message"],
        "survives restart"
    );
    // A resumed session keeps its membership under either allocation policy.
    for (session, agent) in [("session-a", rejoined_agent), ("session-b", "Tiberius")] {
        let members = server
            .tool("pool_members", json!({"pool":"Imperator"}), Some(session))
            .await;
        assert_eq!(structured(&members)["self_agent"], agent);
        let continued = server.send(session, "global", "resumed").await;
        assert!(structured(&continued).get("assigned_agent").is_none());
    }
}

#[tokio::test]
async fn pool_wait_wakes_and_pool_text_contains_each_body_once() {
    let server = Server::new().await;
    server.send("session-a", "global", "join-a").await;
    server.send("session-b", "global", "join-b").await;
    server.exec("session-a").await;
    // A new non-overlapping wait retires the previous join offer.
    let wait = server.tool(
        "pool_wait",
        json!({"pool":"Imperator", "wait_seconds":35}),
        Some("session-a"),
    );
    let send = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        server
            .send("session-b", "Augustus", "unique-wait-body")
            .await
    };
    let (received, sent) =
        tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(wait, send) })
            .await
            .unwrap();
    assert_eq!(structured(&sent)["recipients"], json!(["Augustus"]));
    assert_eq!(structured(&received)["reason"], "message");
    assert_eq!(structured(&received)["membership_changed"], false);
    let peers = structured(&received)["messages"].as_array().unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0]["to"], "Augustus");
    assert_eq!(peers[0]["target"], "Augustus");
    assert!(peers[0].get("sent_at_ms").is_none());
    let text = received["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(text.matches("unique-wait-body").count(), 1);
    let acknowledged = server
        .tool(
            "pool_members",
            json!({"pool":"Imperator"}),
            Some("session-a"),
        )
        .await;
    assert!(structured(&acknowledged).get("messages").is_none());
    for value in [json!(0), json!(4), json!(46), json!(null), json!(10.5)] {
        let invalid = server
            .tool(
                "pool_wait",
                json!({"pool":"Imperator", "wait_seconds":value}),
                Some("session-a"),
            )
            .await;
        assert_eq!(invalid["result"]["isError"], true, "{invalid}");
    }
    let removed_wait = server
        .tool(
            "pool_members",
            json!({"pool":"Imperator", "wait_seconds":35}),
            Some("session-a"),
        )
        .await;
    assert_eq!(removed_wait["result"]["isError"], true);
    // The concurrent send above may already include its receipt. Complete a
    // separate send before offering it so only the later error can collect it.
    let receipt_source = server
        .send("session-b", "Augustus", "error-receipt-body")
        .await;
    let offered = server.exec("session-a").await;
    assert_eq!(
        structured(&offered)["messages"][0]["message_id"],
        structured(&receipt_source)["message_id"]
    );
    // The offered receipt survives both structured and text error-result paths.
    let error_receipt = server
        .tool("exec_command", json!({"cmd":""}), Some("session-b"))
        .await;
    assert_eq!(error_receipt["result"]["isError"], true);
    assert_eq!(
        structured(&error_receipt)["delivery_receipts"][0]["status"],
        "offered"
    );
    assert_eq!(
        structured(&error_receipt)["delivery_receipts"][0]["message_id"],
        structured(&receipt_source)["message_id"]
    );
    assert!(
        error_receipt["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("offered to Augustus")
    );
    assert!(
        !error_receipt["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("offered to Augustus at ")
    );

    let wait = server.tool("pool_wait", json!({"pool":"Imperator"}), Some("session-a"));
    let leave = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        server.leave("session-b").await
    };
    let (changed, _) =
        tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(wait, leave) })
            .await
            .unwrap();
    assert_eq!(structured(&changed)["reason"], "membership_changed");
    assert_eq!(structured(&changed)["messages"], json!([]));
    assert_eq!(structured(&changed)["membership_changed"], true);
    let no_session = server
        .tool("pool_wait", json!({"pool":"Imperator"}), None)
        .await;
    assert_eq!(no_session["result"]["isError"], true);
}

#[tokio::test]
async fn receipt_during_wait_stays_pending_and_accompanies_the_next_message() {
    let server = Server::new().await;
    server.send("session-a", "global", "join-a").await;
    server.send("session-b", "global", "join-b").await;
    server.exec("session-a").await;
    let sent = server.send("session-a", "Tiberius", "please check").await;

    let mut wait = Box::pin(server.tool(
        "pool_wait",
        json!({"pool":"Imperator", "wait_seconds":5}),
        Some("session-a"),
    ));
    let offer = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let received = server.exec("session-b").await;
        assert_eq!(
            structured(&received)["messages"][0]["message"],
            "please check"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    tokio::select! {
        response = wait.as_mut() => panic!("receipt alone completed pool_wait: {response}"),
        _ = offer => {}
    }
    let reply = server.send("session-b", "Augustus", "checked").await;
    let received = tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .unwrap();
    assert_eq!(structured(&received)["reason"], "message");
    assert_eq!(
        structured(&received)["messages"][0]["message_id"],
        structured(&reply)["message_id"]
    );
    assert_eq!(
        structured(&received)["delivery_receipts"][0]["message_id"],
        structured(&sent)["message_id"]
    );
    let acknowledged = server.exec("session-a").await;
    assert!(structured(&acknowledged).get("messages").is_none());
    assert!(structured(&acknowledged).get("delivery_receipts").is_none());
}

#[tokio::test]
async fn ordinary_tools_share_the_incoming_message_contract() {
    let server = Server::new().await;
    server.send("session-a", "global", "join-a").await;
    server.send("session-b", "global", "join-b").await;
    server.exec("session-a").await;
    for (tool, arguments) in [
        ("pool_members", json!({"pool":"Imperator"})),
        (
            "pool_send",
            json!({"pool":"Imperator", "target":"global", "message":"outgoing"}),
        ),
        ("exec_command", json!({"cmd":"true"})),
    ] {
        let body = format!("incoming on {tool}");
        let sent = server.send("session-b", "Augustus", &body).await;
        let received = server.tool(tool, arguments, Some("session-a")).await;
        assert_ne!(received["result"]["isError"], true, "{received}");
        assert_eq!(
            structured(&received)["messages"],
            json!([{
                "message_id":structured(&sent)["message_id"], "pool":"Imperator",
                "from":"Tiberius", "to":"Augustus", "target":"Augustus", "message":body,
            }])
        );
        let text = received["result"]["content"][0]["text"].as_str().unwrap();
        assert_eq!(text.matches(&body).count(), 1);
    }
    for (tool, command) in [
        ("write_stdin", "read -r line; printf '%s' \"$line\""),
        ("wait_for_exit", "sleep 1"),
    ] {
        let process = server
            .tool(
                "exec_command",
                json!({
                    "cmd":command, "yield_time_ms":10,
                }),
                Some("session-a"),
            )
            .await;
        let process_id = structured(&process)["session_id"].as_str().unwrap();
        let arguments = if tool == "write_stdin" {
            json!({"session_id":process_id, "chars":"done\n"})
        } else {
            json!({"session_id":process_id})
        };
        let body = format!("incoming on {tool}");
        let sent = server.send("session-b", "Augustus", &body).await;
        let received = server.tool(tool, arguments, Some("session-a")).await;
        assert_ne!(received["result"]["isError"], true, "{received}");
        assert_eq!(
            structured(&received)["messages"],
            json!([{
                "message_id":structured(&sent)["message_id"], "pool":"Imperator",
                "from":"Tiberius", "to":"Augustus", "target":"Augustus", "message":body,
            }])
        );
        if tool == "wait_for_exit" {
            assert_eq!(structured(&received)["exit_code"], 0);
        }
    }
}

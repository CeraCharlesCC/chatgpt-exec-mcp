#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chatgpt_exec_mcp::events::EventStore;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const OWNER: &str = "account-a";
const EVENT: &str = "multiagent.message";
const SECRET: &str = "whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

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
        let database = workspace.path().join("events.sqlite3");
        drop(EventStore::open(&database).unwrap());
        // Inspect durability independently of the worker's delivery timing. This
        // fixture-only trigger holds new rows pending across the subprocess restart.
        Connection::open(&database)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER test_defer_deliveries AFTER INSERT ON deliveries BEGIN
             UPDATE deliveries SET next_attempt=NEW.created+3600000
             WHERE id=NEW.id AND subscription_id=NEW.subscription_id; END;",
            )
            .unwrap();
        let socket = workspace.path().join("mcp.sock");
        std::fs::write(
            workspace.path().join("config.json"),
            json!({
                "version": 1, "workspace": ".", "shell": "/bin/bash",
                "output_store_dir": "outputs", "child_env": {"inherit": [], "rules": []},
                "events": {"database_path": database, "principal": OWNER}
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
            "io.modelcontextprotocol/clientInfo": {"name": "events-http-test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
            // Metadata is not an authenticated account identity.
            "principal": "account-b"
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
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        assert!(!response.headers().contains_key("Mcp-Session-Id"));
        let body = response.text().await.unwrap();
        let value: Value = serde_json::from_str(&body).unwrap();
        // rmcp maps protocol errors to their HTTP statuses in stateless mode.
        let expected_status = match value["error"]["code"].as_i64() {
            Some(-32602) => 400,
            Some(-32601) => 404,
            _ => 200,
        };
        assert_eq!(status, expected_status, "{method}: {body}");
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

    fn seed_verified_member(
        &self,
        owner: &str,
        pool: &str,
        agent: &str,
        session: Option<&str>,
        expired: bool,
    ) -> String {
        // Fixtures enter below callback verification. These intentionally blocked
        // destinations ensure the background worker cannot contact any network.
        let callback = format!("https://127.0.0.1/callback/{owner}/{agent}");
        let identity = json!([owner, callback, EVENT, {"pool": pool, "agent": agent}]);
        let id = format!("sub_{:x}", Sha256::digest(identity.to_string().as_bytes()));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        Connection::open(&self.database).unwrap().execute(
            "INSERT INTO subscriptions(id,owner,pool,agent,callback,secret,expires,session,generation) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'fixture')",
            params![id, owner, pool, agent, callback, SECRET, now + if expired {-1000} else {86_400_000}, session]
        ).unwrap();
        callback
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test]
async fn custom_events_dispatch_and_pool_schemas_over_strict_http() {
    let server = Server::new().await;
    let catalog = server.call("events/list", json!({}), None).await;
    let definition = &catalog["result"]["events"][0];
    assert_eq!(definition["name"], EVENT);
    assert_eq!(definition["delivery"], json!(["webhook"]));
    assert_eq!(
        definition["inputSchema"]["required"],
        json!(["pool", "agent"])
    );
    let tools = server.call("tools/list", json!({}), None).await;
    let tools = tools["result"]["tools"].as_array().unwrap();
    for name in ["pool_members", "pool_send"] {
        let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }
    assert!(!tools.iter().any(|tool| tool["name"] == "pool_broadcast"));
    let send = tools
        .iter()
        .find(|tool| tool["name"] == "pool_send")
        .unwrap();
    assert_eq!(
        send["inputSchema"]["required"],
        json!(["pool", "agent", "message"])
    );
    assert!(
        send["inputSchema"]["properties"]
            .get("from_agent")
            .is_some()
    );
    for (method, args) in [
        ("events/list", json!({"cursor": "unsupported"})),
        ("events/subscribe", json!({"name": EVENT})),
        ("events/unsubscribe", json!({"id": "unknown"})),
    ] {
        assert_eq!(
            server.call(method, args, None).await["error"]["code"],
            -32602
        );
    }
    let subscribe = json!({"name": EVENT, "arguments": {"pool": "project", "agent": "alice"},
        "delivery": {"mode": "webhook", "url": "https://127.0.0.1/callback", "secret": SECRET}});
    let blocked = server
        .call("events/subscribe", subscribe.clone(), Some("session-a"))
        .await;
    assert_eq!(blocked["error"]["code"], -32015);
    assert_eq!(blocked["error"]["data"]["reason"], "non_public_address");
    let mut global = subscribe;
    global["arguments"]["agent"] = json!("global");
    assert_eq!(
        server.call("events/subscribe", global, None).await["error"]["code"],
        -32602
    );
    let unknown = server.call("events/unknown", json!({}), None).await;
    assert_eq!(unknown["error"]["code"], -32601);
}

#[tokio::test]
async fn pool_identity_isolation_fanout_unsubscribe_and_restart_are_durable() {
    let mut server = Server::new().await;
    server.seed_verified_member(OWNER, "project", "alice", Some("session-a"), false);
    let bob_callback =
        server.seed_verified_member(OWNER, "project", "bob", Some("session-b"), false);
    server.seed_verified_member(OWNER, "project", "carol", None, false);
    server.seed_verified_member(OWNER, "project", "expired", None, true);
    server.seed_verified_member("account-b", "project", "outsider", None, false);
    server.seed_verified_member(OWNER, "other-project", "other-pool", None, false);
    let members = server
        .tool("pool_members", json!({"pool": "project"}), None)
        .await;
    assert_eq!(
        members["result"]["structuredContent"]["agents"],
        json!(["alice", "bob", "carol"])
    );
    let conflict = server.call("events/subscribe", json!({"name": EVENT,
        "arguments": {"pool": "project", "agent": "alice"},
        "delivery": {"mode": "webhook", "url": "https://127.0.0.1/another-callback", "secret": SECRET}
    }), Some("different-session")).await;
    assert_eq!(conflict["error"]["data"]["reason"], "agent_conflict");
    let targeted = server
        .tool(
            "pool_send",
            json!({"pool": "project", "agent": "bob", "message": "hello"}),
            Some("session-a"),
        )
        .await;
    assert_eq!(
        targeted["result"]["structuredContent"]["recipients"],
        json!(["bob"])
    );
    let broadcast = server
        .tool(
            "pool_send",
            json!({"pool": "project", "agent": "global", "message": "team"}),
            Some("session-a"),
        )
        .await;
    assert_eq!(
        broadcast["result"]["structuredContent"]["recipients"],
        json!(["bob", "carol"])
    );
    let broadcast_id = broadcast["result"]["structuredContent"]["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let override_sender = server
        .tool(
            "pool_send",
            json!({"pool": "project", "agent": "bob", "message": "forged", "from_agent": "carol"}),
            Some("session-a"),
        )
        .await;
    assert_eq!(override_sender["result"]["isError"], true);
    let fallback = server.tool("pool_send", json!({"pool": "project", "agent": "alice", "message": "fallback", "from_agent": "carol"}), None).await;
    assert_eq!(
        fallback["result"]["structuredContent"]["recipients"],
        json!(["alice"])
    );
    let wrong_session = server.tool("pool_send", json!({"pool": "project", "agent": "carol", "message": "forged", "from_agent": "alice"}), Some("unbound-session")).await;
    assert_eq!(wrong_session["result"]["isError"], true);

    server.stop();
    server.start().await;
    let members = server
        .tool("pool_members", json!({"pool": "project"}), None)
        .await;
    assert_eq!(
        members["result"]["structuredContent"]["agents"],
        json!(["alice", "bob", "carol"])
    );
    let connection = Connection::open(&server.database).unwrap();
    let bodies: Vec<String> = connection
        .prepare("SELECT body FROM deliveries WHERE id=?1")
        .unwrap()
        .query_map([broadcast_id.clone()], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(bodies.len(), 2);
    for body in bodies {
        let event: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(event["eventId"], broadcast_id);
        assert_eq!(event["name"], EVENT);
        assert_eq!(
            event["data"],
            json!({"message_id": broadcast_id, "pool": "project", "from": "alice", "to": "global", "message": "team"})
        );
    }
    let unsubscribe = json!({"name": EVENT, "arguments": {"pool": "project", "agent": "bob"},
        "delivery": {"mode": "webhook", "url": bob_callback}});
    for _ in 0..2 {
        assert_eq!(
            server
                .call("events/unsubscribe", unsubscribe.clone(), None)
                .await["result"],
            json!({})
        );
    }
    let members = server
        .tool("pool_members", json!({"pool": "project"}), None)
        .await;
    assert_eq!(
        members["result"]["structuredContent"]["agents"],
        json!(["alice", "carol"])
    );
}

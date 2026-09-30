#![cfg(unix)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

struct Response {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn write_config(workspace: &TempDir) -> std::path::PathBuf {
    let path = workspace.path().join("config.json");
    std::fs::write(
        &path,
        json!({
            "version": 1,
            "workspace": ".",
            "shell": "/bin/bash",
            "output_store_dir": "outputs",
            "child_env": { "inherit": [], "rules": [] },
            "events": {"database_path":"events.sqlite3", "principal":"http-test-account"}
        })
        .to_string(),
    )
    .unwrap();
    path
}

fn wait_for_socket(child: &mut Child, path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "server exited before socket creation"
        );
        if UnixStream::connect(path).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("streamable HTTP socket did not become ready");
}

fn decode_chunked(mut body: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::new();
    loop {
        let line_end = body.windows(2).position(|w| w == b"\r\n").unwrap();
        let size = usize::from_str_radix(
            std::str::from_utf8(&body[..line_end])
                .unwrap()
                .split(';')
                .next()
                .unwrap(),
            16,
        )
        .unwrap();
        body = &body[line_end + 2..];
        if size == 0 {
            break;
        }
        decoded.extend_from_slice(&body[..size]);
        body = &body[size + 2..];
    }
    decoded
}

fn request(
    path: &std::path::Path,
    method: &str,
    session: Option<&str>,
    protocol_version: Option<&str>,
    body: Option<Value>,
) -> Response {
    let mut stream = UnixStream::connect(path).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let rpc_method = body
        .as_ref()
        .and_then(|value| value["method"].as_str())
        .map(str::to_owned);
    let rpc_name = body
        .as_ref()
        .filter(|value| value["method"] == "tools/call")
        .and_then(|value| value["params"]["name"].as_str())
        .map(str::to_owned);
    let payload = body.map(|value| value.to_string()).unwrap_or_default();
    let mut request = format!(
        "{method} /mcp HTTP/1.1\r\nHost: localhost\r\nAccept: application/json, text/event-stream\r\nConnection: close\r\n"
    );
    if !payload.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    }
    if let Some(rpc_method) = rpc_method {
        request.push_str(&format!("Mcp-Method: {rpc_method}\r\n"));
    }
    if let Some(rpc_name) = rpc_name {
        request.push_str(&format!("Mcp-Name: {rpc_name}\r\n"));
    }
    if let Some(session) = session {
        request.push_str(&format!("Mcp-Session-Id: {session}\r\n"));
    }
    if let Some(protocol_version) = protocol_version {
        request.push_str(&format!("MCP-Protocol-Version: {protocol_version}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(&payload);
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("invalid HTTP response: {:?}", String::from_utf8_lossy(&raw)));
    let header_text = std::str::from_utf8(&raw[..split]).unwrap();
    let mut lines = header_text.lines();
    let status = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: HashMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let mut body = raw[split + 4..].to_vec();
    if headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        body = decode_chunked(&body);
    }
    Response {
        status,
        headers,
        body,
    }
}

struct Server {
    child: Child,
    workspace: TempDir,
    socket: std::path::PathBuf,
}
impl Server {
    fn new() -> Self {
        let workspace = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let socket = workspace.path().join("mcp.sock");
        let mut child = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
            .arg("--config")
            .arg(write_config(&workspace))
            .arg("--listen-unix")
            .arg(&socket)
            .current_dir(workspace.path())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        wait_for_socket(&mut child, &socket);
        Self {
            child,
            workspace,
            socket,
        }
    }
    fn call(&self, id: i64, method: &str, mut params: Value) -> Response {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name":"http-test","version":"1"},
            "io.modelcontextprotocol/clientCapabilities": {}
        });
        request(
            &self.socket,
            "POST",
            None,
            Some("2026-07-28"),
            Some(json!({
                "jsonrpc":"2.0","id":id,"method":method,"params":params
            })),
        )
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn body(response: &Response) -> Value {
    serde_json::from_slice(&response.body).unwrap_or_else(|_| {
        panic!(
            "non JSON response: {}",
            String::from_utf8_lossy(&response.body)
        )
    })
}

#[test]
fn strict_stateless_http_discovery_and_shared_pty_survive_reconnection() {
    let mut server = Server::new();
    assert_eq!(
        std::fs::metadata(&server.socket)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let no_stream = request(&server.socket, "GET", None, None, None);
    assert_eq!(no_stream.status, 405);
    let discover = server.call(1, "server/discover", json!({}));
    assert_eq!(
        discover.status,
        200,
        "{}",
        String::from_utf8_lossy(&discover.body)
    );
    assert_eq!(
        discover.headers.get("content-type").unwrap(),
        "application/json"
    );
    assert!(!discover.headers.contains_key("mcp-session-id"));
    let discovery = body(&discover);
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2026-07-28"])
    );
    assert!(discovery["result"]["capabilities"]["events"].is_object());
    let list = server.call(2, "tools/list", json!({}));
    assert_eq!(list.status, 200);
    assert!(!list.headers.contains_key("mcp-session-id"));
    assert!(
        body(&list)["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "exec_command")
    );
    let started = server.call(
        3,
        "tools/call",
        json!({"name":"start_session","arguments":{"cmd":"/bin/bash"}}),
    );
    assert_eq!(started.status, 200);
    let value = body(&started);
    let session = value["result"]["structuredContent"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // Each request opens and closes its own connection, as a restarting tunnel does.
    let written=server.call(4,"tools/call",json!({"name":"write_stdin","arguments":{"session_id":session,"chars":"printf 'still-alive\\n'\n"}}));
    assert_eq!(written.status, 200);
    let mut output = body(&written)["result"]["structuredContent"]["output"]
        .as_str()
        .unwrap()
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !output.contains("still-alive") && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        let polled = server.call(
            40,
            "tools/call",
            json!({"name":"write_stdin","arguments":{"session_id":session,"chars":""}}),
        );
        output.push_str(
            body(&polled)["result"]["structuredContent"]["output"]
                .as_str()
                .unwrap(),
        );
    }
    assert!(output.contains("still-alive"));
    let stopped = server.call(
        5,
        "tools/call",
        json!({"name":"write_stdin","arguments":{"session_id":session,"chars":"exit\n"}}),
    );
    assert_eq!(stopped.status, 200);
    unsafe { libc::kill(server.child.id() as i32, libc::SIGTERM) };
    assert!(server.child.wait().unwrap().success());
    assert!(!server.socket.exists());
    assert!(server.workspace.path().exists());
}

#[test]
fn missing_metadata_and_old_versions_are_rejected_without_transport_sessions() {
    let server = Server::new();
    for (header, params) in [
        (None, json!({})),
        (Some("2026-07-28"), json!({})),
        (
            Some("2025-11-25"),
            json!({"_meta":{
                "io.modelcontextprotocol/protocolVersion":"2025-11-25",
                "io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},
                "io.modelcontextprotocol/clientCapabilities":{}
            }}),
        ),
    ] {
        let response = request(
            &server.socket,
            "POST",
            None,
            header,
            Some(json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":params})),
        );
        assert!(
            response.status >= 400 || body(&response).get("error").is_some(),
            "accepted invalid protocol request"
        );
        assert!(!response.headers.contains_key("mcp-session-id"));
    }
    let delete = request(
        &server.socket,
        "DELETE",
        Some("legacy"),
        Some("2026-07-28"),
        None,
    );
    assert_eq!(delete.status, 405);
}

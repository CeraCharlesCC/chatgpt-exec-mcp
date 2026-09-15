#![cfg(unix)]

use std::collections::HashMap;
use std::io::{Read, Write};
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
            "child_env": { "inherit": [], "rules": [] }
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
    let payload = body.map(|value| value.to_string()).unwrap_or_default();
    let mut request = format!(
        "{method} /mcp HTTP/1.1\r\nHost: localhost\r\nAccept: application/json, text/event-stream\r\nConnection: close\r\n"
    );
    if !payload.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", payload.len()));
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

fn sse_json(response: &Response) -> Value {
    let text = std::str::from_utf8(&response.body).unwrap();
    text.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(str::trim)
        .find(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str(line).unwrap())
        .expect("missing JSON SSE event")
}

#[test]
fn unix_streamable_http_sessions_are_distinct_and_visible_to_probe() {
    let workspace = TempDir::new().unwrap();
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

    let initialize = |id| {
        request(
            &socket,
            "POST",
            None,
            Some("2026-07-28"),
            Some(json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2026-07-28",
                    "capabilities": {},
                    "clientInfo": { "name": "http-test", "version": "1" }
                }
            })),
        )
    };
    let first = initialize(1);
    assert_eq!(first.status, 200);
    assert_eq!(sse_json(&first)["result"]["protocolVersion"], "2025-11-25");
    let first_session = first.headers.get("mcp-session-id").unwrap().clone();

    let initialized = request(
        &socket,
        "POST",
        Some(&first_session),
        Some("2025-11-25"),
        Some(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })),
    );
    assert_eq!(initialized.status, 202);

    let probe = request(
        &socket,
        "POST",
        Some(&first_session),
        Some("2025-11-25"),
        Some(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "name": "session_probe", "arguments": {} }
        })),
    );
    let probe = sse_json(&probe);
    let probe = &probe["result"]["structuredContent"];
    assert_eq!(probe["request_transport"], "streamable_http");
    assert_eq!(probe["mcp_session_id"], first_session);
    assert_eq!(probe["protocol_version"], "2025-11-25");
    assert_eq!(probe["client_name"], "http-test");

    let second = initialize(3);
    assert_eq!(second.status, 200);
    let second_session = second.headers.get("mcp-session-id").unwrap();
    assert_ne!(second_session, &first_session);

    let terminated = request(
        &socket,
        "DELETE",
        Some(&first_session),
        Some("2025-11-25"),
        None,
    );
    assert_eq!(terminated.status, 202);

    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = child.wait().unwrap();
    let diagnostics = child.stderr.take().unwrap();
    let diagnostics = std::io::read_to_string(diagnostics).unwrap();
    assert!(status.success(), "server shutdown failed: {diagnostics}");
    assert!(!socket.exists(), "server left unix socket behind");
}

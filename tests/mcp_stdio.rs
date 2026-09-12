use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::Command;

async fn request(
    stdin: &mut tokio::process::ChildStdin,
    stdout: &mut BufReader<tokio::process::ChildStdout>,
    message: Value,
) -> Value {
    stdin
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
    stdin.flush().await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), stdout.read_line(&mut line))
        .await
        .expect("MCP response timed out")
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn stdio_initialize_list_and_stateful_tool_calls() {
    let workspace = TempDir::new().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
        .args(["--workspace", workspace.path().to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = child.stderr.take().unwrap();

    let initialized = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "integration-test", "version": "1" }
            }
        }),
    )
    .await;
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        "chatgpt-exec-mcp"
    );
    let instructions = initialized["result"]["instructions"].as_str().unwrap();
    assert!(instructions.contains("prefer wait_for_exit"));
    assert!(instructions.contains("ordinary stdout/stderr does not wake"));
    assert!(!instructions.contains("build-summary"));
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();

    let listed = request(
        &mut stdin,
        &mut stdout,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }),
    )
    .await;
    let names: Vec<_> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "exec_command",
            "start_session",
            "write_stdin",
            "wait_for_exit"
        ]
    );
    for tool in listed["result"]["tools"].as_array().unwrap() {
        let properties = tool["outputSchema"]["properties"].as_object().unwrap();
        assert!(properties.contains_key("call_wall_time_seconds"));
        assert!(!properties.contains_key("wall_time_seconds"));
        assert!(!properties.contains_key("chunk_id"));
        assert!(properties.contains_key("output_encoding_loss"));
        assert!(properties.contains_key("capture_error"));
        let required = tool["outputSchema"]["required"].as_array().unwrap();
        for field in [
            "call_wall_time_seconds",
            "output",
            "output_truncated",
            "output_encoding_loss",
        ] {
            assert!(required.iter().any(|value| value == field));
        }
        let output_ref = &tool["outputSchema"]["properties"]["output_ref"];
        assert_eq!(output_ref["type"], "object");
        assert_eq!(output_ref["additionalProperties"], false);
        assert_eq!(
            output_ref["properties"]["capture_status"]["enum"],
            json!(["open", "complete", "incomplete"])
        );
        assert_eq!(
            output_ref["required"],
            json!([
                "path",
                "range_start",
                "range_end",
                "stored_bytes",
                "capture_status"
            ])
        );
    }
    let wait_tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "wait_for_exit")
        .unwrap();
    assert!(
        wait_tool["description"]
            .as_str()
            .unwrap()
            .contains("does not wake")
    );
    assert_eq!(wait_tool["inputSchema"]["additionalProperties"], false);
    assert_eq!(wait_tool["inputSchema"]["required"], json!(["session_id"]));
    assert_eq!(
        wait_tool["inputSchema"]["properties"]["wait_seconds"]["minimum"],
        15
    );
    assert_eq!(
        wait_tool["inputSchema"]["properties"]["wait_seconds"]["maximum"],
        100
    );
    assert_eq!(
        wait_tool["inputSchema"]["properties"]["wait_seconds"]["default"],
        35
    );
    assert!(
        wait_tool["inputSchema"]["properties"]["max_output_tokens"]
            .get("default")
            .is_none()
    );
    let start_tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "start_session")
        .unwrap();
    assert!(
        start_tool["inputSchema"]["properties"]["cmd"]
            .get("default")
            .is_none()
    );
    assert_eq!(
        start_tool["inputSchema"]["properties"]["cmd"]["minLength"],
        1
    );
    assert!(
        start_tool["inputSchema"]["properties"]["cmd"]
            .get("pattern")
            .is_none()
    );
    assert!(
        start_tool["inputSchema"]["properties"]
            .get("yield_time_ms")
            .is_none()
    );
    let write_tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "write_stdin")
        .unwrap();
    assert!(
        write_tool["inputSchema"]["properties"]
            .get("yield_time_ms")
            .is_none()
    );
    let exec_tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "exec_command")
        .unwrap();
    assert!(
        exec_tool["inputSchema"]["properties"]["cmd"]["description"]
            .as_str()
            .unwrap()
            .contains("server-configured shell")
    );
    assert_eq!(
        exec_tool["inputSchema"]["properties"]["cmd"]["minLength"],
        1
    );
    assert!(
        exec_tool["inputSchema"]["properties"]["cmd"]
            .get("pattern")
            .is_none()
    );
    assert_eq!(
        exec_tool["inputSchema"]["properties"]["yield_time_ms"]["minimum"],
        10
    );
    assert_eq!(
        exec_tool["inputSchema"]["properties"]["yield_time_ms"]["maximum"],
        120000
    );

    for (id, name, arguments, expected) in [
        (
            20,
            "exec_command",
            json!({ "cmd": "true", "yield_time_ms": 9 }),
            "between",
        ),
        (
            21,
            "exec_command",
            json!({ "cmd": "true", "yield_time_ms": 120001 }),
            "between",
        ),
        (
            22,
            "exec_command",
            json!({ "cmd": "true", "max_output_tokens": 0 }),
            "at least 1",
        ),
        (
            23,
            "start_session",
            json!({ "yield_time_ms": 250 }),
            "unknown field",
        ),
        (
            24,
            "write_stdin",
            json!({ "session_id": "maple-comet", "yield_time_ms": 250 }),
            "unknown field",
        ),
        (
            25,
            "wait_for_exit",
            json!({ "session_id": "maple-comet", "wait_seconds": 14 }),
            "between",
        ),
        (
            26,
            "wait_for_exit",
            json!({ "session_id": "maple-comet", "wait_seconds": 101 }),
            "between",
        ),
    ] {
        let invalid = request(
            &mut stdin,
            &mut stdout,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": { "name": name, "arguments": arguments }
            }),
        )
        .await;
        assert_eq!(invalid["error"]["code"], -32602, "{invalid}");
        let message = invalid["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(expected),
            "expected {expected:?} in {message:?}"
        );
    }

    let whitespace_only = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 27,
            "method": "tools/call",
            "params": { "name": "exec_command", "arguments": { "cmd": "   " } }
        }),
    )
    .await;
    assert_eq!(whitespace_only["result"]["isError"], true);
    assert!(
        whitespace_only["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("cmd must not be empty")
    );

    let one_shot = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": { "name": "exec_command", "arguments": { "cmd": "printf mcp-ok" } }
        }),
    )
    .await;
    assert_eq!(one_shot["result"]["structuredContent"]["exit_code"], 0);
    assert_eq!(one_shot["result"]["structuredContent"]["output"], "mcp-ok");
    assert!(
        one_shot["result"]["structuredContent"]["call_wall_time_seconds"]
            .as_f64()
            .unwrap()
            >= 0.0
    );
    assert_eq!(
        one_shot["result"]["content"][0]["text"],
        "exit_code=0; output_bytes=6"
    );

    let started = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "start_session",
                "arguments": { "cmd": "exec /bin/bash --noprofile --norc" }
            }
        }),
    )
    .await;
    let session_id = started["result"]["structuredContent"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(session_id.split('-').count(), 2);

    let state = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "write_stdin",
                "arguments": {
                    "session_id": session_id,
                    "chars": "export MCP_STATE=kept\nprintf 'STATE:%s\\n' \"$MCP_STATE\"\n"
                }
            }
        }),
    )
    .await;
    assert!(
        state["result"]["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .contains("STATE:kept")
    );

    let exited = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "write_stdin",
                "arguments": { "session_id": session_id, "chars": "exit\n" }
            }
        }),
    )
    .await;
    assert_eq!(exited["result"]["structuredContent"]["exit_code"], 0);

    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("server did not stop after stdio EOF")
        .unwrap();
    assert!(status.success());
    let mut diagnostics = String::new();
    stderr.read_to_string(&mut diagnostics).await.unwrap();
    assert!(diagnostics.is_empty(), "unexpected stderr: {diagnostics}");

    let mut extra_stdout = String::new();
    stdout.read_to_string(&mut extra_stdout).await.unwrap();
    assert!(extra_stdout.is_empty(), "unexpected stdout: {extra_stdout}");
}

#[tokio::test]
async fn cancelling_wait_for_exit_keeps_process_running_and_output_pending() {
    let workspace = TempDir::new().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
        .args(["--workspace", workspace.path().to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = child.stderr.take().unwrap();

    request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "cancellation-test", "version": "1" }
            }
        }),
    )
    .await;
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();

    let started = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "exec_command",
                "arguments": {
                    "cmd": "sleep 0.02; printf after-cancel; sleep 0.30",
                    "yield_time_ms": 10
                }
            }
        }),
    )
    .await;
    let session_id = started["result"]["structuredContent"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();

    stdin
        .write_all(
            format!(
                "{}\n",
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": "wait_for_exit",
                        "arguments": { "session_id": session_id, "wait_seconds": 15 }
                    }
                })
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stdin.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    stdin
        .write_all(
            format!(
                "{}\n",
                json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": { "requestId": 3, "reason": "test cancellation" }
                })
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stdin.flush().await.unwrap();

    let still_running = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "write_stdin",
                "arguments": { "session_id": session_id }
            }
        }),
    )
    .await;
    assert_eq!(still_running["id"], 4);
    assert_eq!(
        still_running["result"]["structuredContent"]["session_id"],
        session_id
    );
    assert!(still_running["result"]["structuredContent"]["exit_code"].is_null());
    assert_eq!(
        still_running["result"]["structuredContent"]["output"],
        "after-cancel"
    );

    let finished = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "wait_for_exit",
                "arguments": { "session_id": session_id, "wait_seconds": 15 }
            }
        }),
    )
    .await;
    assert_eq!(finished["id"], 5);
    assert_eq!(finished["result"]["structuredContent"]["exit_code"], 0);
    assert_eq!(finished["result"]["structuredContent"]["output"], "");

    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("server did not stop after stdio EOF")
        .unwrap();
    assert!(status.success());
    let mut diagnostics = String::new();
    stderr.read_to_string(&mut diagnostics).await.unwrap();
    assert!(diagnostics.is_empty(), "unexpected stderr: {diagnostics}");
}

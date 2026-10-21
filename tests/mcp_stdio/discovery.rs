use std::process::Stdio;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use serde_json::Value;
use std::collections::BTreeSet;

use super::{request, write_config};

fn string_set(value: &Value) -> BTreeSet<&str> {
    let values = value.as_array().unwrap();
    let strings: BTreeSet<_> = values.iter().map(|value| value.as_str().unwrap()).collect();
    assert_eq!(strings.len(), values.len(), "duplicate schema values");
    strings
}

#[tokio::test]
async fn stdio_initialize_list_and_stateful_tool_calls() {
    let workspace = TempDir::new().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
        .arg("--config")
        .arg({
            write_config(&workspace, false);
            "config.json"
        })
        .current_dir(workspace.path())
        .env_clear()
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
    assert!(instructions.contains("Test workspace instructions."));
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
    let names: BTreeSet<_> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names.len(),
        listed["result"]["tools"].as_array().unwrap().len()
    );
    for required in ["exec_command", "wait_for_exit", "write_stdin"] {
        assert!(
            names.contains(required),
            "missing execution tool {required}"
        );
    }
    for agent_pool_tool in ["pool_members", "pool_send", "pool_wait"] {
        assert!(
            !names.contains(agent_pool_tool),
            "agent-pool tools must stay disabled without agent_pool configuration"
        );
    }
    assert!(!names.contains("start_session"));
    for tool in listed["result"]["tools"].as_array().unwrap() {
        let properties = tool["outputSchema"]["properties"].as_object().unwrap();
        assert!(!properties.contains_key("call_wall_time_seconds"));
        assert!(properties.contains_key("output_encoding_loss"));
        assert!(properties.contains_key("capture_error"));
        let required: BTreeSet<_> = tool["outputSchema"]
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|value| value.as_str().unwrap())
            .collect();
        for sparse in ["output", "output_truncated", "output_encoding_loss"] {
            assert!(!required.contains(sparse));
        }
        let output_ref = &tool["outputSchema"]["properties"]["output_ref"];
        assert_eq!(output_ref["type"], "object");
        assert_eq!(output_ref["additionalProperties"], false);
        assert_eq!(
            string_set(&output_ref["properties"]["capture_status"]["enum"]),
            ["open", "complete", "incomplete"].into()
        );
        assert_eq!(
            string_set(&output_ref["required"]),
            [
                "path",
                "range_start",
                "range_end",
                "stored_bytes",
                "capture_status"
            ]
            .into()
        );
    }
    let wait_tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "wait_for_exit")
        .unwrap();
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
        .find(|tool| tool["name"] == "exec_command")
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
            .is_none(),
        "cmd schema must not use a regex that connector validators may treat as full-match"
    );
    assert_eq!(
        start_tool["inputSchema"]["properties"]["workdir"]["minLength"],
        1
    );
    assert!(
        start_tool["inputSchema"]["properties"]
            .get("yield_time_ms")
            .is_some()
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
    assert_eq!(
        exec_tool["inputSchema"]["properties"]["cmd"]["minLength"],
        1
    );
    assert!(
        exec_tool["inputSchema"]["properties"]["cmd"]
            .get("pattern")
            .is_none(),
        "cmd schema must not use a regex that connector validators may treat as full-match"
    );
    assert_eq!(
        exec_tool["inputSchema"]["properties"]["workdir"]["minLength"],
        1
    );
    assert_eq!(
        exec_tool["outputSchema"]["properties"]["session_id"]["pattern"],
        "^[a-z]+-[a-z]+$"
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
            "exec_command",
            json!({ "cmd": "true", "unexpected": 250 }),
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
        // rmcp reports argument deserialization failures as tool errors.
        assert_eq!(invalid["result"]["isError"], true, "{invalid}");
        let message = invalid["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            message.contains(expected),
            "expected {expected:?} in {message:?}"
        );
    }

    let unknown = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0", "id": 27, "method": "tools/call",
            "params": { "name": "unknown_tool", "arguments": {} }
        }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32602);

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

    let whitespace_only_session = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 30,
            "method": "tools/call",
            "params": { "name": "exec_command", "arguments": { "cmd": "   " } }
        }),
    )
    .await;
    assert_eq!(whitespace_only_session["result"]["isError"], true);
    assert!(
        whitespace_only_session["result"]["content"][0]["text"]
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
        one_shot["result"]["structuredContent"]
            .get("call_wall_time_seconds")
            .is_none()
    );

    // Every new process now requires an explicit command.
    let missing_command = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0", "id": 28, "method": "tools/call",
            "params": { "name": "exec_command" }
        }),
    )
    .await;
    assert!(missing_command.get("error").is_some() || missing_command["result"]["isError"] == true);

    let started = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "exec_command",
                "arguments": { "cmd": "exec /bin/bash --noprofile --norc", "tty": true, "yield_time_ms": 250 }
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

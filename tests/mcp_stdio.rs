use std::collections::BTreeSet;
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

const PRIVATE_USER: &str = "MCP_TEST_SOURCE_USER";
const PRIVATE_TOKEN: &str = "MCP_TEST_SOURCE_TOKEN";

fn string_set(value: &Value) -> BTreeSet<&str> {
    let values = value.as_array().unwrap();
    let strings: BTreeSet<_> = values.iter().map(|value| value.as_str().unwrap()).collect();
    assert_eq!(strings.len(), values.len(), "duplicate schema values");
    strings
}

fn write_config(workspace: &TempDir, scoped: bool) -> std::path::PathBuf {
    let path = workspace.path().join("config.json");
    let rules = if scoped {
        json!([{
            "tool": "exec_command", "workdir_under": "project-root",
            "set_from_env": { "BUILD_TEST_USER": PRIVATE_USER, "BUILD_TEST_TOKEN": PRIVATE_TOKEN }
        }])
    } else {
        json!([])
    };
    std::fs::write(
        workspace.path().join("instructions.txt"),
        "Test workspace instructions.",
    )
    .unwrap();
    std::fs::write(
        &path,
        json!({
            "version": 1, "workspace": ".", "shell": "/bin/bash", "output_store_dir": "outputs",
            "instructions_file": "instructions.txt",
            "child_env": { "inherit": [], "rules": rules }
        })
        .to_string(),
    )
    .unwrap();
    path
}

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
    assert!(instructions.ends_with("Test workspace instructions."));
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
    assert_eq!(
        names,
        [
            "exec_command",
            "start_session",
            "wait_for_exit",
            "write_stdin"
        ]
        .into()
    );
    for tool in listed["result"]["tools"].as_array().unwrap() {
        let properties = tool["outputSchema"]["properties"].as_object().unwrap();
        assert!(properties.contains_key("call_wall_time_seconds"));
        assert!(!properties.contains_key("wall_time_seconds"));
        assert!(!properties.contains_key("chunk_id"));
        assert!(properties.contains_key("output_encoding_loss"));
        assert!(properties.contains_key("capture_error"));
        let required = string_set(&tool["outputSchema"]["required"]);
        assert_eq!(
            required,
            [
                "call_wall_time_seconds",
                "output",
                "output_truncated",
                "output_encoding_loss"
            ]
            .into()
        );
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
            "params": { "name": "start_session", "arguments": { "cmd": "   " } }
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
        one_shot["result"]["structuredContent"]["call_wall_time_seconds"]
            .as_f64()
            .unwrap()
            >= 0.0
    );

    // start_session is the tool whose complete argument object may be omitted.
    let default_shell = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0", "id": 28, "method": "tools/call",
            "params": { "name": "start_session" }
        }),
    )
    .await;
    let default_id = default_shell["result"]["structuredContent"]["session_id"]
        .as_str()
        .unwrap();
    let stopped = request(&mut stdin, &mut stdout, json!({
        "jsonrpc": "2.0", "id": 29, "method": "tools/call",
        "params": { "name": "write_stdin", "arguments": { "session_id": default_id, "chars": "exit 0\n" } }
    })).await;
    assert_eq!(stopped["result"]["structuredContent"]["exit_code"], 0);

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
        .arg("--config")
        .arg(write_config(&workspace, false))
        .env_clear()
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

#[cfg(unix)]
#[tokio::test]
async fn conditional_environment_follow_canonical_exec_workdir_only() {
    let workspace = TempDir::new().unwrap();
    let auth_root = workspace.path().join("project-root");
    std::fs::create_dir_all(workspace.path().join("project-root-neighbor")).unwrap();
    let child_dir = auth_root.join("project");
    let outside = workspace.path().join("outside");
    std::fs::create_dir_all(&child_dir).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, auth_root.join("outside-link")).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
        .arg("--config")
        .arg(write_config(&workspace, true))
        .env_clear()
        .env("UNLISTED_TEST_VALUE", "must-not-leak")
        .env(PRIVATE_USER, "sentinel-user")
        .env(PRIVATE_TOKEN, "sentinel-token")
        .env("BUILD_TEST_USER", "inherited-user")
        .env("BUILD_TEST_TOKEN", "inherited-token")
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
                "clientInfo": { "name": "scoped-auth-test", "version": "1" }
            }
        }),
    )
    .await;
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();

    let command = concat!(
        "test -z \"${UNLISTED_TEST_VALUE:-}\" || exit 91; ",
        "printf '%s|%s|%s|%s' ",
        "\"${BUILD_TEST_USER:-}\" ",
        "\"${BUILD_TEST_TOKEN:-}\" ",
        "\"${MCP_TEST_SOURCE_USER:-}\" ",
        "\"${MCP_TEST_SOURCE_TOKEN:-}\""
    );
    for tty in [false, true] {
        for (id, workdir, expected) in [
            (2, "project-root", "sentinel-user|sentinel-token||"),
            (3, "project-root/project", "sentinel-user|sentinel-token||"),
            (4, "outside", "|||"),
            (5, "project-root/../outside", "|||"),
            (6, "project-root/outside-link", "|||"),
            (10, "project-root-neighbor", "|||"),
        ] {
            let response = request(
                &mut stdin,
                &mut stdout,
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "tools/call",
                    "params": {
                        "name": "exec_command",
                        "arguments": { "cmd": command, "workdir": workdir, "tty": tty }
                    }
                }),
            )
            .await;
            assert_eq!(
                response["result"]["structuredContent"]["output"], expected,
                "unexpected environment for {workdir}: {response}"
            );
        }
    }

    let absolute = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc":"2.0", "id":11, "method":"tools/call", "params":{
                "name":"exec_command", "arguments":{"cmd":command, "workdir":child_dir}
            }
        }),
    )
    .await;
    assert_eq!(
        absolute["result"]["structuredContent"]["output"],
        "sentinel-user|sentinel-token||"
    );

    let changed_inside_command = request(
        &mut stdin,
        &mut stdout,
        json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "exec_command",
                "arguments": {
                    "cmd": format!("cd project-root/project && {command}"),
                    "workdir": "."
                }
            }
        }),
    )
    .await;
    assert_eq!(
        changed_inside_command["result"]["structuredContent"]["output"],
        "|||"
    );
    let leaves_scope = request(&mut stdin, &mut stdout, json!({
        "jsonrpc":"2.0", "id":12, "method":"tools/call", "params":{
            "name":"exec_command", "arguments":{"cmd":format!("cd ../outside; {command}"), "workdir":"project-root"}
        }
    })).await;
    assert_eq!(
        leaves_scope["result"]["structuredContent"]["output"],
        "sentinel-user|sentinel-token||"
    );

    for tty in [false, true] {
        let explicit = request(
            &mut stdin,
            &mut stdout,
            json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "start_session",
                    "arguments": {
                        "cmd": command,
                        "workdir": "project-root/project",
                        "tty": tty
                    }
                }
            }),
        )
        .await;
        let mut explicit_output = explicit["result"]["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .to_owned();
        if let Some(session_id) = explicit["result"]["structuredContent"]["session_id"].as_str() {
            let finished = request(
                &mut stdin,
                &mut stdout,
                json!({
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/call",
                    "params": {
                        "name": "wait_for_exit",
                        "arguments": { "session_id": session_id }
                    }
                }),
            )
            .await;
            assert_eq!(finished["result"]["structuredContent"]["exit_code"], 0);
            explicit_output.push_str(
                finished["result"]["structuredContent"]["output"]
                    .as_str()
                    .unwrap(),
            );
        }
        assert_eq!(explicit_output, "|||");
    }

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

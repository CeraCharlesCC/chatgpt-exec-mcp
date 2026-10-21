use std::process::Stdio;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use super::{PRIVATE_TOKEN, PRIVATE_USER, request, write_config};

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
                    "name": "exec_command",
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
        assert_eq!(explicit_output, "sentinel-user|sentinel-token||");
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

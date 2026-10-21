use std::process::Stdio;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use super::{request, write_config};

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
                    "cmd": ": > ready; while ! test -f emit; do /bin/sleep 0.01; done; printf after-cancel; : > produced; while ! test -f finish; do /bin/sleep 0.01; done",
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
    // A completed independent request is a dispatch barrier for the earlier
    // waiter. The child stays gated until the server is handling requests.
    let barrier = request(
        &mut stdin,
        &mut stdout,
        json!({"jsonrpc": "2.0", "id": 30, "method": "tools/list", "params": {}}),
    )
    .await;
    assert_eq!(barrier["id"], 30);
    std::fs::write(workspace.path().join("emit"), "").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let captured = std::fs::read_dir(workspace.path().join("outputs"))
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| {
                    std::fs::read(entry.path().join("raw.log"))
                        .is_ok_and(|bytes| bytes == b"after-cancel")
                });
            if workspace.path().join("produced").exists() && captured {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("child output readiness timed out");
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

    std::fs::write(workspace.path().join("finish"), "").unwrap();
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
    assert!(
        finished["result"]["structuredContent"]
            .get("output")
            .is_none()
    );

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

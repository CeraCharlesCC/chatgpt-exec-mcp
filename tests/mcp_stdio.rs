use std::time::Duration;

use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

const PRIVATE_USER: &str = "MCP_TEST_SOURCE_USER";
const PRIVATE_TOKEN: &str = "MCP_TEST_SOURCE_TOKEN";

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

#[path = "mcp_stdio/cancellation.rs"]
mod cancellation;
#[path = "mcp_stdio/discovery.rs"]
mod discovery;
#[cfg(unix)]
#[path = "mcp_stdio/environment.rs"]
mod environment;

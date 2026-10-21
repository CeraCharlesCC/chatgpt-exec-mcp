use std::time::Duration;

use chatgpt_exec_mcp::Config;
use tempfile::TempDir;

pub fn test_config(workspace: &TempDir) -> Config {
    let path = workspace.path().join("config.json");
    std::fs::write(
        &path,
        serde_json::json!({
            "version": 1, "workspace": ".", "shell": "/bin/bash", "output_store_dir": "outputs",
            "child_env": { "inherit": [], "rules": [] },
            "session_idle_timeout": 60,
            "output_store_retention": 60,
            "output_store_max_bytes": 67108864
        })
        .to_string(),
    )
    .unwrap();
    Config::load(&path).unwrap()
}

// Observe the spool itself to establish that the expected bytes are captured;
// a wall-clock delay or response text alone is insufficient.
pub async fn wait_for_captured_output(workspace: &TempDir, expected: &[u8], timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            let captured = std::fs::read_dir(workspace.path().join("outputs"))
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| {
                    std::fs::read(entry.path().join("raw.log")).is_ok_and(|bytes| bytes == expected)
                });
            if captured {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("output capture readiness timed out");
}

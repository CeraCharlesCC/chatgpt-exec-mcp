#![cfg(unix)]

use std::time::Duration;

use chatgpt_exec_mcp::tools::{ExecCommandArgs, ExecResponse, WriteStdinArgs};
use chatgpt_exec_mcp::{Config, ProcessManager};
use tempfile::TempDir;

fn gate(workspace: &TempDir, name: &str) {
    use std::os::unix::ffi::OsStrExt;

    let path = workspace.path().join(name);
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // A FIFO lets the fixture block until the test releases it without polling
    // timeouts, and its writer also confirms that the shell reached the gate.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
}

async fn release(workspace: &TempDir, name: &str) {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let path = workspace.path().join(name);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&path)
            {
                Ok(mut fifo) => {
                    fifo.write_all(b"release\n").unwrap();
                    return;
                }
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("failed to open fixture gate: {error}"),
            }
        }
    })
    .await
    .expect("shell did not reach its gate");
}

fn manager(workspace: &TempDir) -> std::sync::Arc<ProcessManager> {
    let path = workspace.path().join("config.json");
    std::fs::write(
        &path,
        serde_json::json!({
            "version": 1, "workspace": ".", "shell": "/bin/bash", "output_store_dir": "outputs",
            "child_env": { "inherit": [], "rules": [] },
            "explicit_session_idle_timeout": 60, "exec_continuation_idle_timeout": 60,
            "output_store_retention": 60, "output_store_max_bytes": 67108864
        })
        .to_string(),
    )
    .unwrap();
    ProcessManager::new(Config::load(&path).unwrap()).unwrap()
}

fn args(cmd: &str, tokens: usize) -> ExecCommandArgs {
    ExecCommandArgs {
        cmd: cmd.into(),
        workdir: None,
        tty: false,
        yield_time_ms: Some(250),
        max_output_tokens: Some(tokens),
    }
}

async fn poll(manager: &ProcessManager, id: &str, chars: &str) -> ExecResponse {
    manager
        .write_stdin(WriteStdinArgs {
            session_id: id.into(),
            chars: chars.into(),
            max_output_tokens: Some(10_000),
        })
        .await
        .unwrap()
}

async fn terminal(
    manager: &ProcessManager,
    mut response: ExecResponse,
    tokens: usize,
) -> ExecResponse {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(id) = response.session_id.clone() {
            assert!(response.output.is_empty(), "unexpected intermediate output");
            response = manager
                .write_stdin(WriteStdinArgs {
                    session_id: id,
                    chars: String::new(),
                    max_output_tokens: Some(tokens),
                })
                .await
                .unwrap();
        }
        response
    })
    .await
    .expect("command did not finish")
}

// Observe the spool itself to establish that the incomplete bytes are captured;
// a wall-clock delay or a missing replacement character alone is insufficient.
async fn captured_bytes(workspace: &TempDir, expected: &[u8]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let found = std::fs::read_dir(workspace.path().join("outputs"))
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| {
                    std::fs::read(entry.path().join("raw.log")).is_ok_and(|bytes| bytes == expected)
                });
            if found {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expected raw bytes were not captured");
}

#[tokio::test]
async fn split_scalar_defers_incomplete_bytes_then_preserves_malformed_raw_output() {
    let workspace = TempDir::new().unwrap();
    gate(&workspace, "scalar-gate");
    let manager = manager(&workspace);
    let first = manager
        .exec_command(args(
            "printf 'prefix\\342'; read -r gate < scalar-gate; printf '\\202\\254suffix\\377'; read -r gate",
            10_000,
        ))
        .await
        .unwrap();
    let id = first.session_id.as_ref().unwrap().clone();
    let mut delivered = first.output.clone();
    captured_bytes(&workspace, b"prefix\xe2").await;
    let remaining = poll(&manager, &id, "").await;
    delivered.push_str(&remaining.output);
    assert_eq!(delivered, "prefix");
    for response in [&first, &remaining] {
        assert!(!response.output_encoding_loss);
        assert!(!response.output_truncated);
        assert!(response.output_ref.is_none());
    }
    let deferred = poll(&manager, &id, "").await;
    assert!(deferred.output.is_empty());
    assert!(!deferred.output_encoding_loss);

    let raw = b"prefix\xe2\x82\xacsuffix\xff";
    release(&workspace, "scalar-gate").await;
    captured_bytes(&workspace, raw).await;
    let second = poll(&manager, &id, "").await;
    assert_eq!(second.output, "€suffix�");
    assert!(second.output_encoding_loss);
    assert!(!second.output_truncated);
    assert!(second.capture_error.is_none());
    let reference = second.output_ref.unwrap();
    assert_eq!(reference.range_start, b"prefix".len() as u64);
    assert_eq!(reference.range_end, raw.len() as u64);
    assert_eq!(reference.stored_bytes, raw.len() as u64);
    assert_eq!(reference.capture_status, "open");
    assert_eq!(std::fs::read(&reference.path).unwrap(), raw);

    let ended = poll(&manager, &id, "finish\n").await;
    let ended = terminal(&manager, ended, 10_000).await;
    assert_eq!(ended.exit_code, Some(0));
    assert!(ended.output.is_empty());
    assert!(!ended.output_encoding_loss);
    let final_ref = ended.output_ref.unwrap();
    assert_eq!(final_ref.path, reference.path);
    assert_eq!(final_ref.range_start, raw.len() as u64);
    assert_eq!(final_ref.range_end, raw.len() as u64);
    assert_eq!(final_ref.capture_status, "complete");
    assert_eq!(std::fs::read(&final_ref.path).unwrap(), raw);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn incomplete_terminal_scalar_reports_encoding_loss_and_retains_exact_bytes() {
    let workspace = TempDir::new().unwrap();
    let manager = manager(&workspace);
    let response = manager
        .exec_command(args("printf '\\342\\202'", 10_000))
        .await
        .unwrap();
    let response = terminal(&manager, response, 10_000).await;
    assert_eq!(response.exit_code, Some(0));
    assert_eq!(response.output, "�");
    assert!(response.output_encoding_loss);
    assert!(!response.output_truncated);
    assert!(response.capture_error.is_none());
    let reference = response.output_ref.unwrap();
    assert_eq!(reference.range_start, 0);
    assert_eq!(reference.range_end, 2);
    assert_eq!(reference.stored_bytes, 2);
    assert_eq!(reference.capture_status, "complete");
    assert_eq!(std::fs::read(&reference.path).unwrap(), b"\xe2\x82");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn boundary_truncation_keeps_valid_scalars_and_exact_retained_raw_bytes() {
    let workspace = TempDir::new().unwrap();
    gate(&workspace, "output-gate");
    let manager = manager(&workspace);
    let raw = "あ".repeat(100);
    let response = manager
        .exec_command(args(
            &format!("read -r gate < output-gate; printf '{raw}'; read -r gate"),
            16,
        ))
        .await
        .unwrap();
    assert!(response.output.is_empty());
    let id = response.session_id.unwrap();
    release(&workspace, "output-gate").await;
    captured_bytes(&workspace, raw.as_bytes()).await;
    let response = manager
        .write_stdin(WriteStdinArgs {
            session_id: id.clone(),
            chars: String::new(),
            max_output_tokens: Some(16),
        })
        .await
        .unwrap();
    assert!(response.output_truncated);
    assert!(!response.output_encoding_loss);
    assert!(response.output.len() <= 64);
    assert!(response.output.starts_with('あ'));
    assert!(response.output.ends_with('あ'));
    assert!(!response.output.contains('�'));
    let reference = response.output_ref.unwrap();
    assert_eq!(reference.range_start, 0);
    assert_eq!(reference.range_end, raw.len() as u64);
    assert_eq!(reference.stored_bytes, raw.len() as u64);
    assert_eq!(std::fs::read(&reference.path).unwrap(), raw.as_bytes());
    let ended = poll(&manager, &id, "finish\n").await;
    let ended = terminal(&manager, ended, 16).await;
    assert_eq!(ended.exit_code, Some(0));
    assert_eq!(ended.output_ref.unwrap().capture_status, "complete");
    manager.shutdown_all().await;
}

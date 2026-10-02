use std::sync::Arc;
use std::time::Duration;
use std::{future::Future, pin::Pin, task::Poll};

use chatgpt_exec_mcp::Config;
use chatgpt_exec_mcp::ProcessManager;
use chatgpt_exec_mcp::tools::ExecCommandArgs;
use chatgpt_exec_mcp::tools::ExecResponse;
use chatgpt_exec_mcp::tools::StartSessionArgs;
use chatgpt_exec_mcp::tools::WaitForExitArgs;
use chatgpt_exec_mcp::tools::WriteStdinArgs;
use tempfile::TempDir;

fn test_config(workspace: &TempDir) -> Config {
    let path = workspace.path().join("config.json");
    std::fs::write(
        &path,
        serde_json::json!({
            "version": 1, "workspace": ".", "shell": "/bin/bash", "output_store_dir": "outputs",
            "child_env": { "inherit": [], "rules": [] },
            "explicit_session_idle_timeout": 60, "exec_continuation_idle_timeout": 60,
            "output_store_retention": 60,
            "output_store_max_bytes": 67108864
        })
        .to_string(),
    )
    .unwrap();
    Config::load(&path).unwrap()
}

fn exec_args(cmd: &str) -> ExecCommandArgs {
    ExecCommandArgs {
        cmd: cmd.into(),
        workdir: None,
        tty: false,
        yield_time_ms: Some(1000),
        max_output_tokens: Some(10_000),
    }
}

fn write_args(session_id: &str, chars: &str) -> WriteStdinArgs {
    WriteStdinArgs {
        session_id: session_id.into(),
        chars: chars.into(),
        max_output_tokens: Some(10_000),
    }
}

fn wait_args(session_id: &str, wait_seconds: u64) -> WaitForExitArgs {
    WaitForExitArgs {
        session_id: session_id.into(),
        wait_seconds,
        max_output_tokens: Some(10_000),
    }
}

async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn assert_response_artifact_consistent(response: &ExecResponse) {
    assert!(
        response.capture_error.is_none(),
        "unexpected capture error: {:?}",
        response.capture_error
    );
    if let Some(output_ref) = &response.output_ref {
        assert!(
            std::path::Path::new(&output_ref.path).exists(),
            "response published missing output_ref: {}",
            output_ref.path
        );
    }
}

async fn finish_session(manager: &Arc<ProcessManager>, session_id: &str) -> i32 {
    let mut response = manager
        .write_stdin(write_args(session_id, "exit\n"))
        .await
        .unwrap();
    for _ in 0..10 {
        if let Some(exit_code) = response.exit_code {
            return exit_code;
        }
        response = manager
            .write_stdin(write_args(session_id, ""))
            .await
            .unwrap();
    }
    panic!("session did not exit");
}

#[tokio::test]
async fn one_shot_returns_output_exit_code_and_workdir() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();

    let response = manager
        .exec_command(exec_args("printf hello; printf err >&2; exit 7"))
        .await
        .unwrap();
    assert_eq!(response.exit_code, Some(7));
    assert!(response.session_id.is_none());
    assert!(response.output.contains("hello"));
    assert!(response.output.contains("err"));

    let pwd = manager.exec_command(exec_args("pwd")).await.unwrap();
    assert_eq!(pwd.output.trim(), workspace.path().to_string_lossy());
    manager.exec_command(exec_args("cd /tmp")).await.unwrap();
    let still_stateless = manager.exec_command(exec_args("pwd")).await.unwrap();
    assert_eq!(
        still_stateless.output.trim(),
        workspace.path().to_string_lossy()
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn long_running_pipe_can_be_polled_and_interrupted() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let mut args = exec_args("echo ready; sleep 60");
    args.yield_time_ms = Some(1000);

    let response = manager.exec_command(args).await.unwrap();
    assert!(
        response.output.contains("ready"),
        "initial output was {:?}",
        response.output
    );
    let session_id = response.session_id.unwrap();

    let interrupted = manager
        .write_stdin(write_args(&session_id, "\u{3}"))
        .await
        .unwrap();
    assert_eq!(interrupted.exit_code, Some(130));
    assert!(interrupted.session_id.is_none());
    assert!(
        manager
            .write_stdin(write_args(&session_id, ""))
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown or expired")
    );
    manager.shutdown_all().await;
}

async fn wait_for_ready_file(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("child readiness signal timed out");
}

async fn wait_for_captured_output(workspace: &TempDir, expected: &[u8]) {
    tokio::time::timeout(Duration::from_secs(5), async {
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

#[tokio::test]
async fn wait_for_exit_returns_on_exit_and_does_not_wake_on_output() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some(": > ready; while ! test -f emit-first; do /bin/sleep 0.01; done; printf first; : > first-produced; while ! test -f finish; do /bin/sleep 0.01; done; printf second".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();
    wait_for_ready_file(&workspace.path().join("ready")).await;
    assert_eq!(started.output, "");

    let waiter = manager.wait_for_exit(wait_args(&session_id, 15));
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;
    std::fs::write(workspace.path().join("emit-first"), "").unwrap();
    wait_for_ready_file(&workspace.path().join("first-produced")).await;
    wait_for_captured_output(&workspace, b"first").await;
    // Ordinary output must leave the completion waiter pending, even after
    // capture has visibly completed. Only the explicit exit gate releases it.
    poll_pending(waiter.as_mut()).await;
    std::fs::write(workspace.path().join("finish"), "").unwrap();
    let response = waiter.await.unwrap();

    assert_eq!(response.exit_code, Some(0));
    assert!(response.session_id.is_none());
    assert_eq!(response.output, "firstsecond");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_returns_output_pending_before_and_produced_during_wait() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some(": > ready; while ! test -f emit-before; do /bin/sleep 0.01; done; printf before; : > before-produced; while ! test -f finish; do /bin/sleep 0.01; done; printf after".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();
    wait_for_ready_file(&workspace.path().join("ready")).await;
    assert_eq!(started.output, "");

    std::fs::write(workspace.path().join("emit-before"), "").unwrap();
    wait_for_ready_file(&workspace.path().join("before-produced")).await;
    wait_for_captured_output(&workspace, b"before").await;
    let waiter = manager.wait_for_exit(wait_args(&session_id, 15));
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;
    std::fs::write(workspace.path().join("finish"), "").unwrap();
    let response = waiter.await.unwrap();

    assert_eq!(response.exit_code, Some(0));
    assert!(response.session_id.is_none());
    assert_eq!(response.output, "beforeafter");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_does_not_block_concurrent_ctrl_c() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 60".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    let waiter = manager.wait_for_exit(wait_args(&session_id, 15));
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;

    let (waited, interrupted) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            waiter,
            manager.write_stdin(write_args(&session_id, "\u{3}"))
        )
    })
    .await
    .expect("Ctrl-C was blocked by wait_for_exit");
    let interrupted = interrupted.unwrap();
    assert_response_artifact_consistent(&interrupted);

    let waited = waited.unwrap();
    assert_eq!(waited.exit_code, Some(130));
    assert_response_artifact_consistent(&waited);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_does_not_block_concurrent_stdin() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("read line; printf 'got:%s\\n' \"$line\"".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    let waiter = manager.wait_for_exit(wait_args(&session_id, 15));
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;

    let (waited, written) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            waiter,
            manager.write_stdin(write_args(&session_id, "hello\n"))
        )
    })
    .await
    .expect("stdin write was blocked by wait_for_exit");
    let written = written.unwrap();
    assert_response_artifact_consistent(&written);

    let waited = waited.unwrap();
    assert_eq!(waited.exit_code, Some(0));
    assert_eq!(
        format!("{}{}", written.output, waited.output),
        "got:hello\n"
    );
    assert_response_artifact_consistent(&waited);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn process_exit_racing_with_concurrent_write_delivers_output_once() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("while [ ! -e finish ]; do sleep 0.01; done; printf race-output".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    let waiter = manager.wait_for_exit(wait_args(&session_id, 15));
    let writer = manager.write_stdin(write_args(&session_id, ""));
    tokio::pin!(waiter, writer);
    poll_pending(waiter.as_mut()).await;
    poll_pending(writer.as_mut()).await;
    std::fs::write(workspace.path().join("finish"), "").unwrap();

    let (waited, written) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(waiter, writer)
    })
    .await
    .expect("exit/write race stalled");
    let waited = waited.unwrap();
    let written = written.unwrap();
    assert_eq!(waited.exit_code, Some(0));
    // An activity poll may return output before process exit is observed.
    if let Some(exit_code) = written.exit_code {
        assert_eq!(exit_code, 0);
    } else {
        assert_eq!(written.session_id.as_deref(), Some(session_id.as_str()));
    }
    assert_response_artifact_consistent(&waited);
    assert_response_artifact_consistent(&written);
    assert_eq!(
        format!("{}{}", waited.output, written.output)
            .matches("race-output")
            .count(),
        1,
        "concurrent response commits duplicated or lost output"
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn concurrent_wait_for_exit_delivers_terminal_output_once_without_stale_ref() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("while [ ! -e finish ]; do sleep 0.01; done; printf race-output".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    let first = manager.wait_for_exit(wait_args(&session_id, 15));
    let second = manager.wait_for_exit(wait_args(&session_id, 15));
    tokio::pin!(first, second);
    poll_pending(first.as_mut()).await;
    poll_pending(second.as_mut()).await;
    std::fs::write(workspace.path().join("finish"), "").unwrap();

    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first, second)
    })
    .await
    .expect("concurrent waits stalled");
    let first = first.unwrap();
    let second = second.unwrap();

    for response in [&first, &second] {
        assert_eq!(response.exit_code, Some(0));
        assert_response_artifact_consistent(response);
    }
    assert_eq!(
        format!("{}{}", first.output, second.output)
            .matches("race-output")
            .count(),
        1,
        "concurrent waits duplicated or lost terminal output"
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_uses_existing_output_truncation_and_recovery() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 0.35; head -c 1000 /dev/zero | tr '\\0' x".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();
    let mut args = wait_args(&session_id, 15);
    args.max_output_tokens = Some(10);

    let response = manager.wait_for_exit(args).await.unwrap();
    assert_eq!(response.exit_code, Some(0));
    assert!(response.output_truncated);
    assert!(response.output.contains("bytes omitted"));
    let output_ref = response
        .output_ref
        .expect("truncated output needs recovery ref");
    assert_eq!(output_ref.range_start, 0);
    assert_eq!(output_ref.range_end, 1000);
    assert_eq!(output_ref.capture_status, "complete");
    assert_eq!(std::fs::read(output_ref.path).unwrap(), vec![b'x'; 1000]);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_rejects_invalid_and_expired_session_ids() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let invalid = manager.wait_for_exit(wait_args("not-a-valid-id", 15)).await;
    assert!(invalid.unwrap_err().to_string().contains("session_id"));

    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 60".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();
    manager
        .write_stdin(write_args(&session_id, "\u{3}"))
        .await
        .unwrap();
    let expired = manager.wait_for_exit(wait_args(&session_id, 15)).await;
    assert!(
        expired
            .unwrap_err()
            .to_string()
            .contains("unknown or expired")
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn interactive_bash_preserves_cwd_and_environment() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("exec /bin/bash --noprofile --norc".into()),
            workdir: None,
            tty: Some(true),
            max_output_tokens: Some(10_000),
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    manager
        .write_stdin(write_args(
            &session_id,
            "cd /tmp\nexport EXEC_MCP_TEST=abc\n",
        ))
        .await
        .unwrap();
    let state = manager
        .write_stdin(write_args(
            &session_id,
            "printf 'STATE:%s:%s\\n' \"$PWD\" \"$EXEC_MCP_TEST\"\n",
        ))
        .await
        .unwrap();
    assert!(state.output.contains("STATE:/tmp:abc"), "{}", state.output);
    assert_eq!(finish_session(&manager, &session_id).await, 0);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn python_repl_preserves_variables() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("exec python3 -q".into()),
            workdir: None,
            tty: Some(true),
            max_output_tokens: Some(10_000),
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    manager
        .write_stdin(write_args(&session_id, "x = 41\n"))
        .await
        .unwrap();
    let evaluated = manager
        .write_stdin(write_args(&session_id, "print(x + 1)\n"))
        .await
        .unwrap();
    assert!(evaluated.output.contains("42"), "{}", evaluated.output);
    let exited = manager
        .write_stdin(write_args(&session_id, "exit()\n"))
        .await
        .unwrap();
    assert_eq!(exited.exit_code, Some(0));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn output_is_bounded_and_marks_omitted_middle() {
    let workspace = TempDir::new().unwrap();
    let mut config = test_config(&workspace);
    config.output_cap_bytes = 4096;
    let manager = ProcessManager::new(config).unwrap();
    let response = manager
        .exec_command(exec_args("head -c 200000 /dev/zero | tr '\\0' x"))
        .await
        .unwrap();
    assert_eq!(response.exit_code, Some(0));
    assert!(response.output.len() <= 4096);
    assert!(response.output.contains("bytes omitted"));
    assert!(response.output.starts_with('x'));
    assert!(response.output.ends_with('x'));
    manager.shutdown_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn build_summary_is_opt_in_and_raw_recovery_survives_later_polls() {
    use std::os::unix::fs::PermissionsExt;

    let workspace = TempDir::new().unwrap();
    let mut config = test_config(&workspace);
    config.output_cap_bytes = 1024;
    let manager = ProcessManager::new(config).unwrap();
    let raw = format!(
        "head\n{}BUILD FAILED in 1s\n{}tail\n",
        "progress\n".repeat(1000),
        "cleanup\n".repeat(1000)
    );
    std::fs::write(workspace.path().join("build.log"), &raw).unwrap();
    let script = workspace.path().join("gradlew");
    std::fs::write(
        &script,
        "#!/bin/bash\nset -eu\ncat build.log\nfor attempt in {1..250}; do [ -f finish ] && break; sleep 0.02; done\nprintf 'after-poll\\n'\nexit 1\n",
    ).unwrap();
    std::fs::set_permissions(script, std::fs::Permissions::from_mode(0o700)).unwrap();

    // Reading a build log is a source-reading operation, not a build. Its
    // content must not opt it into summary extraction.
    let read = manager
        .exec_command(exec_args("cat build.log"))
        .await
        .unwrap();
    assert!(read.output_truncated);
    assert!(!read.output.contains("build summary excerpt"));
    assert!(!read.output.contains("BUILD FAILED"));
    assert_eq!(
        std::fs::read(&read.output_ref.unwrap().path).unwrap(),
        raw.as_bytes()
    );

    let started = manager
        .exec_command(exec_args("./gradlew build"))
        .await
        .unwrap();
    assert!(started.output.len() <= 1024);
    assert!(started.output.starts_with("head\n"));
    assert!(started.output.ends_with("tail\n"));
    assert!(started.output.contains("[build summary excerpt; bytes "));
    assert!(started.output.contains("BUILD FAILED in 1s\n"));
    assert!(started.exit_code.is_none()); // The excerpt is not an exit status.
    let reference = started.output_ref.unwrap();
    assert_eq!(reference.capture_status, "open");
    assert_eq!(reference.range_start, 0);
    assert_eq!(reference.range_end, raw.len() as u64);
    assert_eq!(std::fs::read(&reference.path).unwrap(), raw.as_bytes());

    std::fs::write(workspace.path().join("finish"), "").unwrap();
    let session_id = started.session_id.unwrap();
    let mut cursor = reference.range_end;
    let mut output = String::new();
    let mut ended = None;
    for _ in 0..10 {
        let response = manager
            .write_stdin(write_args(&session_id, ""))
            .await
            .unwrap();
        output.push_str(&response.output);
        let info = response.output_ref.as_ref().unwrap();
        assert_eq!(info.path, reference.path);
        assert_eq!(info.range_start, cursor);
        cursor = info.range_end;
        if response.exit_code.is_some() {
            ended = Some(response);
            break;
        }
    }
    let ended = ended.expect("fixture did not finish");
    assert_eq!(ended.exit_code, Some(1));
    assert_eq!(output, "after-poll\n");
    let final_ref = ended.output_ref.unwrap();
    assert_eq!(final_ref.path, reference.path);
    assert_eq!(final_ref.capture_status, "complete");
    assert_eq!(
        std::fs::read(final_ref.path).unwrap(),
        format!("{raw}after-poll\n").as_bytes()
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn idle_reaper_expires_and_kills_session() {
    let workspace = TempDir::new().unwrap();
    let mut config = test_config(&workspace);
    config.explicit_session_idle_timeout = Duration::from_secs(1);
    config.reaper_interval = Duration::from_secs(1);
    let manager = ProcessManager::new(config).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 60".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();
    let reaper = manager.spawn_reaper();

    tokio::time::sleep(Duration::from_millis(2200)).await;
    let error = manager
        .write_stdin(write_args(&session_id, ""))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown or expired"));

    // No reference was returned, so an expired session leaves no retained log.
    assert!(
        std::fs::read_dir(workspace.path().join("outputs"))
            .unwrap()
            .all(|entry| !entry.unwrap().file_type().unwrap().is_dir())
    );
    manager.shutdown_all().await;
    reaper.abort();
}

#[tokio::test]
async fn explicit_and_continuation_quotas_are_separate() {
    let workspace = TempDir::new().unwrap();
    let mut config = test_config(&workspace);
    config.max_explicit_sessions = 1;
    config.max_exec_continuations = 1;
    let manager = ProcessManager::new(config).unwrap();

    let explicit = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 60".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut continuation_args = exec_args("sleep 60");
    continuation_args.yield_time_ms = Some(10);
    let continuation = manager.exec_command(continuation_args).await.unwrap();
    assert!(explicit.session_id.is_some());
    assert!(continuation.session_id.is_some());

    let second_explicit = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 60".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(second_explicit.to_string().contains("quota reached"));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn invalid_workdir_never_spawns_a_child() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    for workdir in ["", "missing", "config.json"] {
        for tty in [false, true] {
            let mut args = exec_args("touch child-was-spawned");
            args.workdir = Some(workdir.into());
            args.tty = tty;
            assert!(manager.exec_command(args).await.is_err());
            assert!(
                manager
                    .start_session(StartSessionArgs {
                        cmd: Some("touch child-was-spawned".into()),
                        workdir: Some(workdir.into()),
                        tty: Some(tty),
                        ..Default::default()
                    })
                    .await
                    .is_err()
            );
            assert!(!workspace.path().join("child-was-spawned").exists());
        }
    }
    let good = manager
        .exec_command(exec_args("printf valid"))
        .await
        .unwrap();
    assert_eq!(good.output, "valid");
    manager.shutdown_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn default_session_handles_an_executable_shell_path_with_spaces_and_quotes() {
    let workspace = TempDir::new().unwrap();
    let shell = workspace.path().join("shell with 'quotes'");
    std::fs::copy("/bin/bash", &shell).unwrap();
    let mut config = test_config(&workspace);
    config.shell = shell.canonicalize().unwrap();
    let manager = ProcessManager::new(config).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let id = started.session_id.unwrap();
    let result = manager
        .write_stdin(write_args(&id, "printf quoted-shell; exit\n"))
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.output, "quoted-shell");
    manager.shutdown_all().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn process_cleanup_captures_late_output_and_preserves_peer_session() {
    // Fixtures stop on a private file, including on assertion failure. The alarm
    // also bounds their lifetime if the test process itself is killed.
    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::write(self.0.join("stop"), b"");
        }
    }
    fn running(directory: &std::path::Path) -> bool {
        let identity = std::fs::read_to_string(directory.join("child")).unwrap();
        let (pid, birth) = identity.split_once(' ').unwrap();
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let fields: Vec<_> = stat
            .rsplit_once(')')
            .unwrap()
            .1
            .split_whitespace()
            .collect();
        fields[19] == birth && !matches!(fields[0], "Z" | "X")
    }
    let workspace = TempDir::new().unwrap();
    std::fs::write(
        workspace.path().join("fixture.py"),
        r#"
import os, pathlib, signal, sys, time
mode = sys.argv[1]
directory = pathlib.Path(mode)
signal.alarm(15)
pid = os.fork()
if pid == 0:
    signal.alarm(15)
    if mode == 'detached':
        os.setsid()
    fields = pathlib.Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    (directory / 'child.tmp').write_text(f'{os.getpid()} {fields[19]}')
    (directory / 'child.tmp').replace(directory / 'child')
    print('child-ready', flush=True)
    if mode == 'delayed':
        time.sleep(0.15)
        print('delayed-output', flush=True)
    else:
        while not (directory / 'stop').exists():
            time.sleep(0.02)
    os._exit(0)
while not (directory / 'child').exists():
    time.sleep(0.005)
print('root-ready', flush=True)
if mode == 'ordinary':
    os.waitpid(pid, 0)
"#,
    )
    .unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let peer = manager
        .start_session(StartSessionArgs {
            cmd: Some("exec /bin/bash --noprofile --norc".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap()
        .session_id
        .unwrap();
    for mode in ["ordinary", "delayed", "held-pipe", "detached"] {
        let directory = workspace.path().join(mode);
        std::fs::create_dir(&directory).unwrap();
        let _fixture = Fixture(directory.clone());
        let started = std::time::Instant::now();
        let mut args = exec_args(&format!("exec /usr/bin/python3 fixture.py {mode}"));
        args.yield_time_ms = Some(300);
        let mut response = manager.exec_command(args).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !directory.join("child").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fixture did not start");
        let mut output = response.output.clone();
        if mode == "ordinary" {
            response = manager
                .write_stdin(write_args(response.session_id.as_ref().unwrap(), "\u{3}"))
                .await
                .unwrap();
            output.push_str(&response.output);
        }
        if let Some(id) = response.session_id.as_deref() {
            response = tokio::time::timeout(
                Duration::from_secs(6),
                manager.wait_for_exit(wait_args(id, 15)),
            )
            .await
            .expect("cleanup did not finish")
            .unwrap();
            output.push_str(&response.output);
        }
        assert!(started.elapsed() < Duration::from_secs(8), "{mode}");
        assert!(response.session_id.is_none(), "{mode}: {response:?}");
        assert_eq!(
            response.exit_code,
            Some(if mode == "ordinary" { 130 } else { 0 })
        );
        match mode {
            "delayed" => {
                assert!(output.contains("delayed-output"), "{output}");
                assert!(response.capture_error.is_none(), "{response:?}");
                assert!(
                    response
                        .output_ref
                        .as_ref()
                        .is_none_or(|r| r.capture_status == "complete")
                );
            }
            "held-pipe" | "detached" => {
                let reference = response.output_ref.as_ref().unwrap();
                assert_eq!(reference.capture_status, "incomplete");
                assert!(
                    reference
                        .incomplete_reason
                        .as_ref()
                        .unwrap()
                        .contains("drain deadline")
                );
                assert!(
                    std::fs::read_to_string(&reference.path)
                        .unwrap()
                        .contains("child-ready")
                );
            }
            _ => {}
        }
        // The detached child may survive group termination, but ordinary and
        // held-pipe descendants must have stopped before fixture cleanup.
        if mode != "detached" {
            tokio::time::timeout(Duration::from_secs(2), async {
                while running(&directory) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("descendant survived cleanup");
        }
        let check = manager
            .write_stdin(write_args(&peer, &format!("printf 'peer-{mode}\\n'\n")))
            .await
            .unwrap();
        assert!(check.output.contains(&format!("peer-{mode}")), "{check:?}");
        assert_eq!(check.session_id.as_deref(), Some(peer.as_str()));
        std::fs::write(directory.join("stop"), b"").unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while running(&directory) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("fixture did not stop");
    }
    manager.shutdown_all().await;
}

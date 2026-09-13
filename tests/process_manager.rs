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
            "output_store_retention": 60, "output_store_min_retention": 1,
            "output_store_max_bytes": 67108864, "output_store_headroom_bytes": 1048576,
            "output_store_max_files": 128
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

#[tokio::test]
async fn wait_for_exit_returns_on_exit_and_does_not_wake_on_output() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 0.35; printf first; sleep 0.20; printf second".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();

    let began = std::time::Instant::now();
    let response = manager
        .wait_for_exit(wait_args(&session_id, 15))
        .await
        .unwrap();

    assert_eq!(response.exit_code, Some(0));
    assert!(response.session_id.is_none());
    assert_eq!(response.output, "firstsecond");
    assert!(
        began.elapsed() >= Duration::from_millis(250),
        "ordinary output woke wait_for_exit early after {:?}",
        began.elapsed()
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_returns_output_pending_before_and_produced_during_wait() {
    let workspace = TempDir::new().unwrap();
    let manager = ProcessManager::new(test_config(&workspace)).unwrap();
    let started = manager
        .start_session(StartSessionArgs {
            cmd: Some("sleep 0.30; printf before; sleep 0.30; printf after".into()),
            tty: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let session_id = started.session_id.unwrap();
    assert_eq!(started.output, "");

    // start_session uses a fixed 250 ms settle window. Waiting another 150 ms
    // makes `before` pending before wait_for_exit starts, while `after` is still
    // produced during the completion wait.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let response = manager
        .wait_for_exit(wait_args(&session_id, 15))
        .await
        .unwrap();

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

    let artifact_dir = std::fs::read_dir(workspace.path().join("outputs"))
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .expect("expired session artifact must be retained")
        .path();
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(artifact_dir.join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["state"], "incomplete");
    assert_eq!(
        meta["incomplete_reason"],
        "session expired after idle timeout"
    );
    assert!(meta["retain_until_unix_millis"].is_number());
    assert!(meta["expires_at_unix_millis"].is_number());
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

use super::*;
use std::{future::Future, pin::Pin, task::Poll};
use tempfile::TempDir;

fn test_manager(workspace: &TempDir) -> Arc<ProcessManager> {
    let path = workspace.path().join("config.json");
    std::fs::write(
        &path,
        serde_json::json!({
            "version": 1, "workspace": ".", "shell": "/bin/bash", "output_store_dir": "outputs",
            "child_env": { "inherit": [], "rules": [] }
        })
        .to_string(),
    )
    .unwrap();
    let config = Config::load(&path).unwrap();
    ProcessManager::new(config).unwrap()
}

#[test]
fn additional_instructions_precede_default_instructions() {
    let workspace = TempDir::new().unwrap();
    let instructions_path = workspace.path().join("instructions.md");
    std::fs::write(&instructions_path, "Read README first.\n").unwrap();
    let config_path = workspace.path().join("config.json");
    std::fs::write(
        &config_path,
        serde_json::json!({
            "version": 1,
            "workspace": ".",
            "shell": "/bin/bash",
            "output_store_dir": "outputs",
            "instructions_file": "instructions.md",
            "child_env": { "inherit": [], "rules": [] }
        })
        .to_string(),
    )
    .unwrap();

    let manager = ProcessManager::new(Config::load(&config_path).unwrap()).unwrap();
    let instructions = manager.instructions();
    let additional = instructions.find("Read README first.").unwrap();
    let defaults = instructions.find("Use exec_command").unwrap();
    assert!(additional < defaults);
}

async fn test_session(manager: &Arc<ProcessManager>, command: &str) -> String {
    manager
        .exec_command(ExecCommandArgs {
            cmd: command.into(),
            tty: false,
            workdir: None,
            max_output_tokens: None,
            yield_time_ms: Some(250),
        })
        .await
        .unwrap()
        .session_id
        .expect("test command should still be running after session startup")
}

fn test_wait_args(session_id: &str) -> WaitForExitArgs {
    WaitForExitArgs {
        session_id: session_id.into(),
        wait_seconds: crate::tools::MIN_WAIT_SECONDS,
        max_output_tokens: None,
    }
}

async fn pinned_session(manager: &ProcessManager, session_id: &str) -> Arc<Session> {
    let id = SessionId::parse(session_id).unwrap();
    Arc::clone(manager.state.lock().await.sessions.get(&id).unwrap())
}

async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

async fn wait_for_pending_output(session: &Session) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !session.has_pending_output().await.unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fixture output was not captured");
}

#[tokio::test]
async fn concurrent_terminal_responses_preserve_capture_failure_and_recovery() {
    assert_concurrent_terminal_metadata(true).await;
}

#[tokio::test]
async fn concurrent_terminal_responses_preserve_truncated_output_recovery() {
    assert_concurrent_terminal_metadata(false).await;
}

async fn assert_concurrent_terminal_metadata(incomplete: bool) {
    let workspace = TempDir::new().unwrap();
    let manager = test_manager(&workspace);
    let session_id = test_session(&manager, "read line; printf captured-output").await;
    let session = pinned_session(&manager, &session_id).await;
    let mut args = test_wait_args(&session_id);
    if !incomplete {
        args.max_output_tokens = Some(1);
    }
    let first = manager.wait_for_exit(args.clone());
    let second = manager.wait_for_exit(args);
    tokio::pin!(first, second);
    poll_pending(first.as_mut()).await;
    poll_pending(second.as_mut()).await;

    // Drive the fixture directly so neither request consumes its output
    // before both completion waits have pinned the session.
    session.send_input(b"finish\n".to_vec()).await.unwrap();
    session
        .wait_until_exit_or_timeout(Duration::from_secs(5))
        .await;
    assert!(session.is_capture_complete());
    if incomplete {
        session.mark_forced_incomplete("fixture capture failure");
    }

    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        first.output.is_empty() as u8 + second.output.is_empty() as u8,
        1
    );
    for response in [&first, &second] {
        assert_eq!(response.exit_code, Some(0));
        assert!(response.session_id.is_none());
        assert_eq!(
            response.capture_error.as_deref(),
            incomplete.then_some("fixture capture failure")
        );
        let output_ref = response.output_ref.as_ref().unwrap();
        assert_eq!(
            output_ref.capture_status,
            if incomplete { "incomplete" } else { "complete" }
        );
        assert_eq!(std::fs::read(&output_ref.path).unwrap(), b"captured-output");
        if response.output.is_empty() {
            assert_eq!(output_ref.range_start, output_ref.range_end);
        }
    }
    assert_eq!(
        first.output_ref.unwrap().path,
        second.output_ref.unwrap().path
    );
    assert_eq!(manager.active_session_count().await, 0);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn cleanup_failure_retains_output_and_publishes_recovery_ref() {
    let workspace = TempDir::new().unwrap();
    let manager = test_manager(&workspace);
    let session_id = test_session(&manager, "read line; printf delivered").await;
    let session = pinned_session(&manager, &session_id).await;
    let raw_path = session.prepare_output(100).await.unwrap().snapshot.path;
    let artifact_dir = raw_path.parent().unwrap();
    std::fs::write(artifact_dir.join("unexpected"), b"foreign").unwrap();

    session.send_input(b"finish\n".to_vec()).await.unwrap();
    session
        .wait_until_exit_or_timeout(Duration::from_secs(5))
        .await;
    assert!(session.is_capture_complete());

    let response = manager
        .build_response(&session.id, &session, Instant::now(), 100)
        .await
        .unwrap();

    assert_eq!(response.exit_code, Some(0));
    assert_eq!(response.output, "delivered");
    assert!(
        response
            .capture_error
            .as_deref()
            .unwrap()
            .contains("failed to remove delivered output artifact")
    );
    let output_ref = response.output_ref.as_ref().unwrap();
    assert_eq!(output_ref.path, raw_path.display().to_string());
    assert_eq!(output_ref.range_start, 0);
    assert_eq!(output_ref.range_end, 9);
    assert_eq!(output_ref.stored_bytes, 9);
    assert_eq!(output_ref.capture_status, "complete");
    assert!(output_ref.expires_at_unix_seconds.is_some());

    let delivery = session.terminal_delivery().unwrap();
    assert_eq!(delivery.capture_error, response.capture_error);
    let delivery_ref = delivery.output_ref.unwrap();
    assert_eq!(delivery_ref.info.path, PathBuf::from(&output_ref.path));
    assert_eq!(delivery_ref.range_start, 9);
    assert_eq!(delivery_ref.range_end, 9);
    assert_eq!(std::fs::read(raw_path).unwrap(), b"delivered");
    assert_eq!(manager.active_session_count().await, 0);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn queued_input_is_rejected_after_terminal_delivery() {
    let workspace = TempDir::new().unwrap();
    let manager = test_manager(&workspace);
    for chars in ["unsent input\n", "\u{3}"] {
        let session_id = test_session(&manager, "read line; printf delivered").await;
        let session = pinned_session(&manager, &session_id).await;
        let interaction = session.interaction_lock.lock().await;
        let writer = manager.write_stdin(WriteStdinArgs {
            session_id: session_id.clone(),
            chars: chars.into(),
            max_output_tokens: None,
        });
        let poll = manager.write_stdin(WriteStdinArgs {
            session_id: session_id.clone(),
            chars: String::new(),
            max_output_tokens: None,
        });
        tokio::pin!(writer, poll);
        poll_pending(writer.as_mut()).await;
        poll_pending(poll.as_mut()).await;

        session.send_input(b"finish\n".to_vec()).await.unwrap();
        session
            .wait_until_exit_or_timeout(Duration::from_secs(5))
            .await;
        assert!(session.is_capture_complete());
        let delivered = manager
            .build_response(&session.id, &session, Instant::now(), 100)
            .await
            .unwrap();
        assert_eq!(delivered.output, "delivered");
        drop(interaction);

        let error = writer.await.unwrap_err().to_string();
        assert!(error.contains("input was not sent"), "{error}");
        let polled = poll.await.unwrap();
        assert_eq!(polled.exit_code, Some(0));
        assert!(polled.output.is_empty());
        assert!(polled.output_ref.is_none());
    }
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_timeout_keeps_session_and_returns_pending_output() {
    let workspace = TempDir::new().unwrap();
    let manager = test_manager(&workspace);
    let session_id = test_session(&manager, "read line; printf pending; read line").await;
    let session = pinned_session(&manager, &session_id).await;
    session.send_input(b"produce\n".to_vec()).await.unwrap();
    wait_for_pending_output(&session).await;

    // Only pause after the OS process's output is committed to the store.
    // The process then remains blocked on stdin while Tokio time advances.
    tokio::time::pause();
    let waiter = manager.wait_for_exit(test_wait_args(&session_id));
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;
    tokio::time::advance(Duration::from_secs(MIN_WAIT_SECONDS)).await;
    let response = waiter.await.unwrap();
    tokio::time::resume();

    assert_eq!(response.session_id.as_deref(), Some(session_id.as_str()));
    assert!(response.exit_code.is_none());
    assert_eq!(response.output, "pending");
    assert_eq!(manager.active_session_count().await, 1);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn reaper_does_not_terminate_a_session_during_an_active_wait() {
    let workspace = TempDir::new().unwrap();
    let mut manager = test_manager(&workspace);
    let config = &mut Arc::get_mut(&mut manager).unwrap().config;
    config.session_idle_timeout = Duration::from_secs(1);
    config.reaper_interval = Duration::from_secs(1);

    let session_id = test_session(&manager, "sleep 4; printf FINISHED").await;
    let reaper = manager.spawn_reaper();
    let response = manager
        .wait_for_exit(test_wait_args(&session_id))
        .await
        .unwrap();
    assert_eq!(response.exit_code, Some(0));
    assert!(response.output.contains("FINISHED"));
    assert_eq!(manager.active_session_count().await, 0);
    manager.shutdown_all().await;
    reaper.await.unwrap();
}

#[test]
fn implicit_output_budgets_match_policy_and_explicit_override_wins() {
    assert_eq!(output_tokens(None, OutputPostprocessing::default()), 8_000);
    assert_eq!(
        output_tokens(None, OutputPostprocessing::for_command("cargo test")),
        2_000
    );
    assert_eq!(
        output_tokens(Some(321), OutputPostprocessing::default()),
        321
    );
    assert_eq!(
        output_tokens(
            Some(321),
            OutputPostprocessing::for_command("./gradlew build")
        ),
        321
    );
}

#[tokio::test]
async fn wait_for_exit_cancellation_before_commit_preserves_session_and_cursor() {
    let workspace = TempDir::new().unwrap();
    let manager = test_manager(&workspace);
    let session_id = test_session(&manager, "sleep 60").await;
    let session = pinned_session(&manager, &session_id).await;
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

    let waiter = manager.wait_for_exit_cancellable(test_wait_args(&session_id), async move {
        let _ = cancel_rx.await;
    });
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;
    assert!(session.has_active_operations());
    cancel_tx.send(()).unwrap();
    assert!(waiter.await.unwrap().is_none());
    assert!(!session.has_active_operations());
    assert_eq!(manager.active_session_count().await, 1);

    let interrupted = manager
        .write_stdin(WriteStdinArgs {
            session_id,
            chars: "\u{3}".into(),
            max_output_tokens: None,
        })
        .await
        .unwrap();
    assert_eq!(interrupted.exit_code, Some(130));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn wait_for_exit_cancellation_while_terminal_commit_is_blocked_rolls_back_delivery() {
    let workspace = TempDir::new().unwrap();
    let manager = test_manager(&workspace);
    let session_id = test_session(&manager, "read line; printf commit-output").await;
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

    let session = pinned_session(&manager, &session_id).await;
    let mut args = test_wait_args(&session_id);
    args.max_output_tokens = Some(1);
    let waiter = manager.wait_for_exit_cancellable(args, async move {
        let _ = cancel_rx.await;
    });
    tokio::pin!(waiter);
    poll_pending(waiter.as_mut()).await;
    let state_guard = manager.state.lock().await;
    session.send_input(b"finish\n".to_vec()).await.unwrap();
    session
        .wait_until_exit_or_timeout(Duration::from_secs(5))
        .await;
    assert!(session.is_capture_complete());
    // Drive preparation to completion, observing the interaction lock, then
    // block at the registry commit boundary without an elapsed-time guess.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            poll_pending(waiter.as_mut()).await;
            if session.interaction_lock.try_lock().is_err()
                && session.output_ref_info().await.unwrap().capture_status == "complete"
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel_tx.send(()).unwrap();
    assert!(waiter.await.unwrap().is_none());
    assert!(!session.recovery_path_published().await.unwrap());
    drop(state_guard);

    assert_eq!(manager.active_session_count().await, 1);
    let delivered = manager
        .wait_for_exit(test_wait_args(&session_id))
        .await
        .unwrap();
    assert_eq!(delivered.exit_code, Some(0));
    assert_eq!(delivered.output, "commit-output");
    assert!(delivered.output_ref.is_none());
    assert_eq!(manager.active_session_count().await, 0);
    manager.shutdown_all().await;
}

async fn startup(
    manager: &ProcessManager,
    command: &str,
    tty: bool,
) -> anyhow::Result<ExecResponse> {
    manager
        .exec_command(ExecCommandArgs {
            cmd: command.into(),
            tty,
            yield_time_ms: Some(MIN_YIELD_MS),
            workdir: None,
            max_output_tokens: None,
        })
        .await
}

async fn completed_startup(manager: &ProcessManager, command: &str) -> ExecResponse {
    let initial = startup(manager, command, false).await.unwrap();
    if let Some(id) = initial.session_id.as_ref() {
        let mut completed = manager.wait_for_exit(test_wait_args(id)).await.unwrap();
        completed.output.insert_str(0, &initial.output);
        completed
    } else {
        initial
    }
}

fn quota_one_manager(workspace: &TempDir) -> Arc<ProcessManager> {
    let mut manager = test_manager(workspace);
    let config = &mut Arc::get_mut(&mut manager).unwrap().config;
    config.max_sessions = 1;
    manager
}

async fn no_artifacts(manager: &ProcessManager) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let has_artifact = std::fs::read_dir(&manager.config.output_store_dir)
                .unwrap()
                .any(|entry| entry.unwrap().file_type().unwrap().is_dir());
            if !has_artifact {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unregistered startup artifact was not cleaned up");
}

#[test]
fn dropped_startup_during_artifact_creation_reuses_quota_and_cleans_files() {
    // Occupy the only blocking worker to position cancellation before
    // artifact creation finishes, without a filesystem speed assumption.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        for tty in [false, true] {
            let workspace = TempDir::new().unwrap();
            let manager = quota_one_manager(&workspace);
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();
            let mut cancelled = Box::pin(startup(&manager, "touch cancelled-child", tty));
            poll_pending(cancelled.as_mut()).await;
            let error = startup(&manager, "true", false)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("quota reached"), "{error}");
            drop(cancelled);
            let mut replacement = Box::pin(completed_startup(&manager, "printf quota-reused"));
            poll_pending(replacement.as_mut()).await;
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            let response = replacement.await;
            assert_eq!(response.output, "quota-reused");
            assert_eq!(response.exit_code, Some(0));
            assert!(!workspace.path().join("cancelled-child").exists());
            no_artifacts(&manager).await;
            manager.shutdown_all().await;
        }
    });
}

#[tokio::test]
async fn dropped_reservation_releases_quota_while_registry_is_locked() {
    let workspace = TempDir::new().unwrap();
    let manager = quota_one_manager(&workspace);
    for _ in 0..2 {
        let reservation = manager.reserve().await.unwrap();
        let state = manager.state.lock().await;
        drop(reservation);
        drop(state);
        // Do not yield to the deferred map-removal task before reuse.
        let replacement = manager.reserve().await.unwrap();
        drop(replacement);
    }
    manager.shutdown_all().await;
}

#[cfg(unix)]
async fn child_pid(workspace: &TempDir) -> i32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(pid) = std::fs::read_to_string(workspace.path().join("child.pid"))
                && let Ok(pid) = pid.trim().parse()
            {
                return pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child did not reach its stdin gate")
}

#[cfg(unix)]
async fn child_terminated(pid: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled startup left a live child");
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_in_flight_spawn_finishes_ownership_and_terminates_child() {
    for tty in [false, true] {
        let workspace = TempDir::new().unwrap();
        let manager = quota_one_manager(&workspace);
        let reservation = manager.reserve().await.unwrap();
        let id = reservation.id.clone();
        let cwd = workspace.path().to_path_buf();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let worker_manager = Arc::clone(&manager);
        let mut spawning = Box::pin(complete_spawn(async move {
            let spawned = worker_manager
                .spawn(&id, "echo $$ > child.pid; read line", &cwd, tty)
                .await?;
            ready_tx.send(()).unwrap();
            let _ = finish_rx.await;
            Ok(spawned)
        }));
        poll_pending(spawning.as_mut()).await;
        ready_rx.await.unwrap();
        let pid = child_pid(&workspace).await;
        drop(spawning);
        drop(reservation);
        let response = completed_startup(&manager, "printf reused").await;
        assert_eq!(response.output, "reused");
        finish_tx.send(()).unwrap();
        child_terminated(pid).await;
        no_artifacts(&manager).await;
        assert_eq!(manager.active_session_count().await, 0);
        manager.shutdown_all().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_registration_drops_child_and_artifact_before_session_tasks_start() {
    for tty in [false, true] {
        let workspace = TempDir::new().unwrap();
        let manager = quota_one_manager(&workspace);
        let reservation = manager.reserve().await.unwrap();
        let store = manager.create_output_store().await.unwrap();
        let raw = store.snapshot(0).path;
        let spawned = manager
            .spawn(
                &reservation.id,
                "echo $$ > child.pid; read line",
                workspace.path(),
                tty,
            )
            .await
            .unwrap();
        let pid = child_pid(&workspace).await;
        let state = manager.state.lock().await;
        let mut registering = Box::pin(manager.commit_reservation(
            reservation,
            spawned,
            store,
            OutputPostprocessing::default(),
        ));
        poll_pending(registering.as_mut()).await;
        drop(registering);
        assert!(!raw.exists());
        drop(state);
        child_terminated(pid).await;
        assert_eq!(manager.active_session_count().await, 0);
        let response = completed_startup(&manager, "printf reused").await;
        assert_eq!(response.output, "reused");
        no_artifacts(&manager).await;
        manager.shutdown_all().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn spawn_error_and_shutdown_before_registration_clean_startup_resources() {
    use std::os::unix::fs::PermissionsExt;
    let workspace = TempDir::new().unwrap();
    let mut manager = quota_one_manager(&workspace);
    let invalid_shell = workspace.path().join("invalid-shell");
    std::fs::write(&invalid_shell, "not executable").unwrap();
    std::fs::set_permissions(&invalid_shell, std::fs::Permissions::from_mode(0o600)).unwrap();
    Arc::get_mut(&mut manager).unwrap().config.shell = invalid_shell;
    for _ in 0..2 {
        let error = startup(&manager, "true", false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("failed to spawn"), "{error}");
        no_artifacts(&manager).await;
    }
    Arc::get_mut(&mut manager).unwrap().config.shell = PathBuf::from("/bin/bash");
    let reservation = manager.reserve().await.unwrap();
    let store = manager.create_output_store().await.unwrap();
    let spawned = manager
        .spawn(
            &reservation.id,
            "echo $$ > child.pid; read line",
            workspace.path(),
            false,
        )
        .await
        .unwrap();
    let pid = child_pid(&workspace).await;
    manager.shutdown_all().await;
    let result = manager
        .commit_reservation(reservation, spawned, store, OutputPostprocessing::default())
        .await;
    assert!(result.is_err());
    child_terminated(pid).await;
    no_artifacts(&manager).await;
    assert_eq!(manager.active_session_count().await, 0);
}

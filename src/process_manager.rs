use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::bail;
use codex_utils_pty::TerminalSize;
use codex_utils_pty::spawn_pipe_process;
use codex_utils_pty::spawn_pty_process;
use tokio::sync::Mutex;
use tokio::sync::Notify;

use crate::config::Config;
use crate::output_store::OutputStore;
use crate::output_store::OutputStoreManager;
use crate::output_summary::BuildOutput;
use crate::response_delivery::{self, PreparedExecResponse};
use crate::session::Session;
use crate::session::SessionOrigin;
use crate::session_id::SessionId;
use crate::tools::ExecCommandArgs;
use crate::tools::ExecResponse;
use crate::tools::StartSessionArgs;
use crate::tools::WaitForExitArgs;
use crate::tools::WriteStdinArgs;
use crate::tools::{DEFAULT_POLL_YIELD_MS, DEFAULT_SESSION_YIELD_MS, DEFAULT_WRITE_YIELD_MS};
use crate::tools::{MAX_WAIT_SECONDS, MAX_YIELD_MS, MIN_WAIT_SECONDS, MIN_YIELD_MS};

#[derive(Default)]
struct StoreState {
    sessions: HashMap<SessionId, Arc<Session>>,
    reserved: HashMap<SessionId, ReservedSession>,
}

struct ReservedSession {
    origin: SessionOrigin,
    lease: Weak<()>,
}

/// Owns quota until the process and artifact enter the registered session map.
/// A dropped startup releases quota immediately, even if the registry is busy.
struct StartupReservation {
    id: SessionId,
    lease: Arc<()>,
    state: Arc<Mutex<StoreState>>,
}

impl Drop for StartupReservation {
    fn drop(&mut self) {
        let id = self.id.clone();
        let lease = Arc::downgrade(&self.lease);
        let remove = move |state: &mut StoreState| {
            if state
                .reserved
                .get(&id)
                .is_some_and(|entry| Weak::ptr_eq(&entry.lease, &lease))
            {
                state.reserved.remove(&id);
            }
        };
        if let Ok(mut state) = self.state.try_lock() {
            remove(&mut state);
        } else if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let state = Arc::clone(&self.state);
            runtime.spawn(async move {
                remove(&mut *state.lock().await);
            });
        }
        // The weak lease expires when this owner is dropped. reserve() ignores
        // expired leases, so deferred map cleanup never prevents quota reuse.
    }
}

struct StartAndWaitArgs {
    origin: SessionOrigin,
    command: String,
    workdir: Option<String>,
    tty: bool,
    yield_ms: u64,
    max_output_tokens: usize,
    build_output: Option<BuildOutput>,
}

pub struct ProcessManager {
    config: Config,
    output_store_manager: OutputStoreManager,
    state: Arc<Mutex<StoreState>>,
    shutting_down: AtomicBool,
    shutdown_notify: Notify,
}

impl ProcessManager {
    pub fn new(mut config: Config) -> anyhow::Result<Arc<Self>> {
        config.validate_limits()?;
        validate_directory(&config.workspace).context("invalid workspace")?;
        config.workspace =
            std::fs::canonicalize(&config.workspace).context("workspace canonicalize failed")?;
        if !config.shell.is_file() {
            bail!("shell must be a regular executable file");
        }
        let output_store_manager = OutputStoreManager::open(
            &config.output_store_dir,
            config.output_store_retention,
            config.output_store_max_bytes,
        )
        .context("output_store_dir initialization failed")?;
        Ok(Arc::new(Self {
            config,
            output_store_manager,
            state: Arc::new(Mutex::new(StoreState::default())),
            shutting_down: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
        }))
    }

    pub(crate) fn instructions(&self) -> String {
        let default_instructions = concat!(
            "Use exec_command for commands; start_session only for persistent state. ",
            "Prefer wait_for_exit for completion; ordinary output does not wake it. ",
            "Use write_stdin for input, Ctrl-C, or output polling. ",
            "Truncated output includes output_ref for the raw log.",
        );
        match &self.config.additional_instructions {
            Some(extra) => format!(
                "{}\n\n{default_instructions}",
                extra.trim_end_matches(['\r', '\n'])
            ),
            None => default_instructions.to_owned(),
        }
    }

    pub fn spawn_reaper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(manager.config.reaper_interval) => {
                        manager.reap_expired().await;
                    }
                    _ = manager.shutdown_notify.notified() => break,
                }
            }
        })
    }

    pub async fn exec_command(&self, args: ExecCommandArgs) -> anyhow::Result<ExecResponse> {
        if args.cmd.trim().is_empty() {
            bail!("cmd must not be empty");
        }
        let yield_ms = args.yield_ms();
        validate_yield_ms(yield_ms)?;
        validate_output_tokens(args.max_output_tokens)?;
        let build_output = BuildOutput::for_command(&args.cmd);
        let output_tokens = output_tokens(args.max_output_tokens, build_output);
        self.start_and_wait(StartAndWaitArgs {
            origin: SessionOrigin::ExecContinuation,
            command: args.cmd,
            workdir: args.workdir,
            tty: args.tty,
            yield_ms,
            max_output_tokens: output_tokens,
            build_output,
        })
        .await
    }

    pub async fn start_session(&self, args: StartSessionArgs) -> anyhow::Result<ExecResponse> {
        validate_output_tokens(args.max_output_tokens)?;
        let command = args.cmd.unwrap_or_else(|| {
            format!(
                "exec '{}'",
                self.config.shell.to_string_lossy().replace('\'', "'\\''")
            )
        });
        if command.trim().is_empty() {
            bail!("cmd must not be empty");
        }
        let build_output = BuildOutput::for_command(&command);
        let output_tokens = output_tokens(args.max_output_tokens, build_output);
        self.start_and_wait(StartAndWaitArgs {
            origin: SessionOrigin::ExplicitSession,
            command,
            workdir: args.workdir,
            tty: args.tty.unwrap_or(true),
            yield_ms: DEFAULT_SESSION_YIELD_MS,
            max_output_tokens: output_tokens,
            build_output,
        })
        .await
    }

    async fn start_and_wait(&self, args: StartAndWaitArgs) -> anyhow::Result<ExecResponse> {
        let StartAndWaitArgs {
            origin,
            command,
            workdir,
            tty,
            yield_ms,
            max_output_tokens,
            build_output,
        } = args;
        let started = Instant::now();
        let cwd = self.resolve_workdir(workdir.as_deref())?;
        let reservation = self.reserve(origin).await?;
        let id = reservation.id.clone();
        // Blocking artifact creation owns its result; a cancelled waiter drops
        // the unpublished store and removes its files when creation finishes.
        let output_store = self.create_output_store().await?;
        let spawned = self.spawn(&id, origin, &command, &cwd, tty).await?;
        // Registration transfers ownership to the manager. A cancelled initial
        // wait then follows the existing registered-session lifetime policy.
        let session = self
            .commit_reservation(reservation, origin, spawned, output_store, build_output)
            .await?;
        session
            .wait_until_exit_or_timeout(Duration::from_millis(yield_ms))
            .await;
        self.build_response(&id, &session, started, max_output_tokens)
            .await
    }

    pub async fn write_stdin(&self, args: WriteStdinArgs) -> anyhow::Result<ExecResponse> {
        validate_output_tokens(args.max_output_tokens)?;
        let started = Instant::now();
        let id = SessionId::parse(&args.session_id)?;
        let session = {
            let state = self.state.lock().await;
            state.sessions.get(&id).cloned()
        }
        .ok_or_else(|| anyhow::anyhow!("unknown or expired session_id `{id}`"))?;

        let _interaction = session.interaction_lock.lock().await;
        session.touch();
        if let Some(delivery) = session.terminal_delivery() {
            if !args.chars.is_empty() {
                bail!("process has already finished; input was not sent");
            }
            return Ok(response_delivery::delivered_terminal_response(
                &session, delivery, started,
            ));
        }
        let baseline = session.output_sequence();
        let had_pending = session.has_pending_output().await?;

        if args.chars == "\u{3}" {
            session.interrupt()?;
        } else if !args.chars.is_empty() {
            session.send_input(args.chars.as_bytes().to_vec()).await?;
        }

        if !args.chars.is_empty() {
            session
                .wait_until_exit_or_timeout(Duration::from_millis(DEFAULT_WRITE_YIELD_MS))
                .await;
        } else if !had_pending {
            session
                .wait_for_activity(baseline, Duration::from_millis(DEFAULT_POLL_YIELD_MS))
                .await;
        }

        let max_output_tokens = output_tokens(args.max_output_tokens, session.build_output());
        self.build_response(&id, &session, started, max_output_tokens)
            .await
    }

    pub async fn wait_for_exit(&self, args: WaitForExitArgs) -> anyhow::Result<ExecResponse> {
        self.wait_for_exit_cancellable(args, std::future::pending())
            .await?
            .ok_or_else(|| anyhow::anyhow!("wait_for_exit was unexpectedly cancelled"))
    }

    pub(crate) async fn wait_for_exit_cancellable<C>(
        &self,
        args: WaitForExitArgs,
        cancelled: C,
    ) -> anyhow::Result<Option<ExecResponse>>
    where
        C: std::future::Future<Output = ()>,
    {
        validate_wait_seconds(args.wait_seconds)?;
        validate_output_tokens(args.max_output_tokens)?;
        let started = Instant::now();
        let id = SessionId::parse(&args.session_id)?;
        let session = {
            let state = self.state.lock().await;
            state.sessions.get(&id).cloned()
        }
        .ok_or_else(|| anyhow::anyhow!("unknown or expired session_id `{id}`"))?;
        session.touch();

        tokio::pin!(cancelled);
        let wait = session.wait_until_exit_or_timeout(Duration::from_secs(args.wait_seconds));
        tokio::pin!(wait);
        tokio::select! {
            biased;
            _ = &mut cancelled => return Ok(None),
            _ = &mut wait => {}
        }

        let interaction = session.interaction_lock.lock();
        tokio::pin!(interaction);
        let _interaction = tokio::select! {
            biased;
            _ = &mut cancelled => return Ok(None),
            guard = &mut interaction => guard,
        };
        session.touch();

        if let Some(delivery) = session.terminal_delivery() {
            return Ok(Some(response_delivery::delivered_terminal_response(
                &session, delivery, started,
            )));
        }

        // Re-snapshot under the interaction lock. Terminal state and output may
        // have changed while this passive wait was pending or while a concurrent
        // write_stdin owned the lock.
        let max_output_tokens = output_tokens(args.max_output_tokens, session.build_output());
        let prepare = response_delivery::prepare_response(
            &session,
            started,
            self.response_budget(max_output_tokens),
        );
        tokio::pin!(prepare);
        let prepared = tokio::select! {
            biased;
            _ = &mut cancelled => return Ok(None),
            result = &mut prepare => result?,
        };

        // For terminal responses, acquire the manager state lock before the
        // delivery commit so cancellation can still leave both the output cursor
        // and session map untouched while lock acquisition is pending.
        let mut state = if prepared.terminal {
            let state = self.state.lock();
            tokio::pin!(state);
            Some(tokio::select! {
                biased;
                _ = &mut cancelled => return Ok(None),
                guard = &mut state => guard,
            })
        } else {
            None
        };

        // This is the delivery commit point. If cancellation is already visible,
        // it wins. Once this check succeeds, cursor/session state is committed
        // synchronously and a later cancellation is considered too late to roll
        // the response back.
        tokio::select! {
            biased;
            _ = &mut cancelled => return Ok(None),
            _ = std::future::ready(()) => {}
        }
        self.commit_prepared_response(&id, &session, &prepared, state.as_deref_mut())?;
        drop(state);

        response_delivery::finish_prepared_response(&session, prepared, started)
            .await
            .map(Some)
    }

    async fn build_response(
        &self,
        id: &SessionId,
        session: &Arc<Session>,
        started: Instant,
        max_output_tokens: usize,
    ) -> anyhow::Result<ExecResponse> {
        if let Some(delivery) = session.terminal_delivery() {
            return Ok(response_delivery::delivered_terminal_response(
                session, delivery, started,
            ));
        }
        let prepared = response_delivery::prepare_response(
            session,
            started,
            self.response_budget(max_output_tokens),
        )
        .await?;
        let mut state = if prepared.terminal {
            Some(self.state.lock().await)
        } else {
            None
        };
        self.commit_prepared_response(id, session, &prepared, state.as_deref_mut())?;
        drop(state);
        response_delivery::finish_prepared_response(session, prepared, started).await
    }

    fn commit_prepared_response(
        &self,
        id: &SessionId,
        session: &Arc<Session>,
        prepared: &PreparedExecResponse,
        state: Option<&mut StoreState>,
    ) -> anyhow::Result<()> {
        if let Some(end) = prepared.commit_end {
            session.try_commit_output(end)?;
        }
        if prepared.response.output_ref.is_some() {
            session.publish_output_ref()?;
        }
        if prepared.terminal {
            let state = state.expect("terminal response commit requires manager state lock");
            Self::remove_if_same_locked(state, id, session);
            session.record_terminal_delivery(prepared.terminal_delivery());
        }
        Ok(())
    }

    async fn create_output_store(&self) -> io::Result<OutputStore> {
        let manager = self.output_store_manager.clone();
        tokio::task::spawn_blocking(move || manager.create_artifact())
            .await
            .map_err(|error| io::Error::other(format!("output store task failed: {error}")))?
    }

    async fn spawn(
        &self,
        id: &SessionId,
        origin: SessionOrigin,
        command: &str,
        cwd: &Path,
        tty: bool,
    ) -> anyhow::Result<codex_utils_pty::SpawnedProcess> {
        let cwd = std::fs::canonicalize(cwd).context("workdir canonicalize failed before spawn")?;
        validate_directory(&cwd).context("invalid spawn workdir")?;
        let mut environment = self.config.child_env.for_spawn(origin, &cwd);
        environment.insert("CHATGPT_EXEC_SESSION".into(), id.to_string());
        let program = self.config.shell.to_string_lossy().into_owned();
        let arguments = vec!["-c".to_owned(), command.to_owned()];
        // Do not cancel the backend mid-spawn: the task owns any in-flight
        // child until it returns a ProcessHandle. A dropped waiter detaches the
        // task, whose undelivered handle terminates the child on drop.
        complete_spawn(async move {
            let arg0 = None;
            let result = if tty {
                spawn_pty_process(
                    &program,
                    &arguments,
                    &cwd,
                    &environment,
                    &arg0,
                    TerminalSize::default(),
                    &[],
                )
                .await
            } else {
                spawn_pipe_process(&program, &arguments, &cwd, &environment, &arg0, &[]).await
            };
            result.with_context(|| format!("failed to spawn command in `{}`", cwd.display()))
        })
        .await
    }

    fn resolve_workdir(&self, workdir: Option<&str>) -> anyhow::Result<PathBuf> {
        let path = match workdir {
            None => self.config.workspace.clone(),
            Some("") => bail!("workdir must not be empty"),
            Some(value) => {
                let supplied = PathBuf::from(value);
                if supplied.is_absolute() {
                    supplied
                } else {
                    self.config.workspace.join(supplied)
                }
            }
        };
        validate_directory(&path)
            .with_context(|| format!("invalid workdir `{}`", path.display()))?;
        std::fs::canonicalize(&path).context("workdir canonicalize failed")
    }

    async fn reserve(&self, origin: SessionOrigin) -> anyhow::Result<StartupReservation> {
        if self.shutting_down.load(Ordering::SeqCst) {
            bail!("server is shutting down");
        }
        let mut state = self.state.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            bail!("server is shutting down");
        }
        state
            .reserved
            .retain(|_, reservation| reservation.lease.strong_count() > 0);
        let active = state
            .sessions
            .values()
            .filter(|session| session.origin == origin)
            .count()
            + state
                .reserved
                .values()
                .filter(|reservation| reservation.origin == origin)
                .count();
        let limit = match origin {
            SessionOrigin::ExecContinuation => self.config.max_exec_continuations,
            SessionOrigin::ExplicitSession => self.config.max_explicit_sessions,
        };
        if active >= limit {
            bail!(
                "{} session quota reached ({limit})",
                match origin {
                    SessionOrigin::ExecContinuation => "exec continuation",
                    SessionOrigin::ExplicitSession => "explicit",
                }
            );
        }

        let id = SessionId::generate_unique(|candidate| {
            state.sessions.contains_key(candidate) || state.reserved.contains_key(candidate)
        })?;
        let lease = Arc::new(());
        state.reserved.insert(
            id.clone(),
            ReservedSession {
                origin,
                lease: Arc::downgrade(&lease),
            },
        );
        Ok(StartupReservation {
            id,
            lease,
            state: Arc::clone(&self.state),
        })
    }

    async fn commit_reservation(
        &self,
        reservation: StartupReservation,
        origin: SessionOrigin,
        spawned: codex_utils_pty::SpawnedProcess,
        output_store: OutputStore,
        build_output: Option<BuildOutput>,
    ) -> anyhow::Result<Arc<Session>> {
        let mut state = self.state.lock().await;
        if self.shutting_down.load(Ordering::SeqCst)
            || state.reserved.remove(&reservation.id).is_none()
        {
            // Both resources are still directly owned here: dropping the
            // process kills it, and dropping the unpublished artifact deletes it.
            bail!("session reservation disappeared before commit");
        }
        // Session capture tasks start only after acquiring the registry lock.
        // No await separates task ownership from registration.
        let session = Session::new(
            reservation.id.clone(),
            origin,
            spawned,
            output_store,
            build_output,
        );
        state
            .sessions
            .insert(reservation.id.clone(), Arc::clone(&session));
        drop(state);
        Ok(session)
    }

    fn remove_if_same_locked(state: &mut StoreState, id: &SessionId, expected: &Arc<Session>) {
        if state
            .sessions
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            state.sessions.remove(id);
        }
    }

    fn response_budget(&self, max_output_tokens: usize) -> usize {
        max_output_tokens
            .saturating_mul(4)
            .min(self.config.output_cap_bytes)
            .max(1)
    }

    async fn reap_expired(&self) {
        let expired = {
            let mut state = self.state.lock().await;
            let ids: Vec<_> = state
                .sessions
                .iter()
                .filter_map(|(id, session)| {
                    let timeout = match session.origin {
                        SessionOrigin::ExecContinuation => {
                            self.config.exec_continuation_idle_timeout
                        }
                        SessionOrigin::ExplicitSession => self.config.explicit_session_idle_timeout,
                    };
                    (session.idle_for() >= timeout).then(|| id.clone())
                })
                .collect();
            ids.into_iter()
                .filter_map(|id| state.sessions.remove(&id))
                .collect::<Vec<_>>()
        };
        for session in expired {
            let _ = session
                .terminate_and_seal_incomplete("session expired after idle timeout")
                .await;
        }
    }

    pub async fn shutdown_all(&self) {
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }
        self.shutdown_notify.notify_waiters();
        let sessions = {
            let mut state = self.state.lock().await;
            state.reserved.clear();
            state
                .sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        for session in sessions {
            let _ = session
                .terminate_and_seal_incomplete("server shutdown interrupted session")
                .await;
        }
    }

    #[cfg(test)]
    pub async fn active_session_count(&self) -> usize {
        self.state.lock().await.sessions.len()
    }
}

/// Let an in-flight backend finish constructing its owning process handle even
/// when its caller is cancelled. Undelivered task results are dropped by Tokio.
async fn complete_spawn<F>(spawn: F) -> anyhow::Result<codex_utils_pty::SpawnedProcess>
where
    F: std::future::Future<Output = anyhow::Result<codex_utils_pty::SpawnedProcess>>
        + Send
        + 'static,
{
    tokio::spawn(spawn)
        .await
        .context("process startup task failed")?
}

fn validate_directory(path: &Path) -> anyhow::Result<()> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_dir() {
        bail!("path is not a directory");
    }
    Ok(())
}

fn output_tokens(requested: Option<usize>, build_output: Option<BuildOutput>) -> usize {
    requested.unwrap_or_else(|| {
        if build_output.is_some() {
            BuildOutput::DEFAULT_MAX_OUTPUT_TOKENS
        } else {
            crate::tools::DEFAULT_MAX_OUTPUT_TOKENS
        }
    })
}

fn validate_yield_ms(value: u64) -> anyhow::Result<()> {
    if !(MIN_YIELD_MS..=MAX_YIELD_MS).contains(&value) {
        bail!("yield_time_ms must be between {MIN_YIELD_MS} and {MAX_YIELD_MS}");
    }
    Ok(())
}

fn validate_wait_seconds(value: u64) -> anyhow::Result<()> {
    if !(MIN_WAIT_SECONDS..=MAX_WAIT_SECONDS).contains(&value) {
        bail!("wait_seconds must be between {MIN_WAIT_SECONDS} and {MAX_WAIT_SECONDS}");
    }
    Ok(())
}

fn validate_output_tokens(value: Option<usize>) -> anyhow::Result<()> {
    if value == Some(0) {
        bail!("max_output_tokens must be at least 1");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
            .start_session(StartSessionArgs {
                cmd: Some(command.into()),
                tty: Some(false),
                ..Default::default()
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

    #[test]
    fn implicit_output_budgets_match_policy_and_explicit_override_wins() {
        assert_eq!(output_tokens(None, None), 8_000);
        assert_eq!(
            output_tokens(None, Some(BuildOutput::Cargo)),
            BuildOutput::DEFAULT_MAX_OUTPUT_TOKENS
        );
        assert_eq!(output_tokens(Some(321), None), 321);
        assert_eq!(output_tokens(Some(321), Some(BuildOutput::Gradle)), 321);
    }

    #[tokio::test]
    async fn wait_for_exit_cancellation_before_commit_preserves_session_and_cursor() {
        let workspace = TempDir::new().unwrap();
        let manager = test_manager(&workspace);
        let session_id = test_session(&manager, "sleep 60").await;
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

        let waiter = manager.wait_for_exit_cancellable(test_wait_args(&session_id), async move {
            let _ = cancel_rx.await;
        });
        tokio::pin!(waiter);
        poll_pending(waiter.as_mut()).await;
        cancel_tx.send(()).unwrap();
        assert!(waiter.await.unwrap().is_none());
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
        origin: SessionOrigin,
        command: &str,
        tty: bool,
    ) -> anyhow::Result<ExecResponse> {
        match origin {
            SessionOrigin::ExecContinuation => {
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
            SessionOrigin::ExplicitSession => {
                manager
                    .start_session(StartSessionArgs {
                        cmd: Some(command.into()),
                        tty: Some(tty),
                        ..Default::default()
                    })
                    .await
            }
        }
    }

    async fn completed_startup(
        manager: &ProcessManager,
        origin: SessionOrigin,
        command: &str,
    ) -> ExecResponse {
        let initial = startup(manager, origin, command, false).await.unwrap();
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
        config.max_exec_continuations = 1;
        config.max_explicit_sessions = 1;
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
            for origin in [
                SessionOrigin::ExecContinuation,
                SessionOrigin::ExplicitSession,
            ] {
                let workspace = TempDir::new().unwrap();
                let manager = quota_one_manager(&workspace);
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                });
                started_rx.await.unwrap();
                let mut cancelled =
                    Box::pin(startup(&manager, origin, "touch cancelled-child", false));
                poll_pending(cancelled.as_mut()).await;
                let error = startup(&manager, origin, "true", false)
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(error.contains("quota reached"), "{error}");
                drop(cancelled);
                let mut replacement =
                    Box::pin(completed_startup(&manager, origin, "printf quota-reused"));
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
        for origin in [
            SessionOrigin::ExecContinuation,
            SessionOrigin::ExplicitSession,
        ] {
            let reservation = manager.reserve(origin).await.unwrap();
            let state = manager.state.lock().await;
            drop(reservation);
            drop(state);
            // Do not yield to the deferred map-removal task before reuse.
            let replacement = manager.reserve(origin).await.unwrap();
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
            let reservation = manager
                .reserve(SessionOrigin::ExecContinuation)
                .await
                .unwrap();
            let id = reservation.id.clone();
            let cwd = workspace.path().to_path_buf();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
            let worker_manager = Arc::clone(&manager);
            let mut spawning = Box::pin(complete_spawn(async move {
                let spawned = worker_manager
                    .spawn(
                        &id,
                        SessionOrigin::ExecContinuation,
                        "echo $$ > child.pid; read line",
                        &cwd,
                        tty,
                    )
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
            let response =
                completed_startup(&manager, SessionOrigin::ExecContinuation, "printf reused").await;
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
            let reservation = manager
                .reserve(SessionOrigin::ExecContinuation)
                .await
                .unwrap();
            let store = manager.create_output_store().await.unwrap();
            let raw = store.snapshot(0).path;
            let spawned = manager
                .spawn(
                    &reservation.id,
                    SessionOrigin::ExecContinuation,
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
                SessionOrigin::ExecContinuation,
                spawned,
                store,
                None,
            ));
            poll_pending(registering.as_mut()).await;
            drop(registering);
            assert!(!raw.exists());
            drop(state);
            child_terminated(pid).await;
            assert_eq!(manager.active_session_count().await, 0);
            let response =
                completed_startup(&manager, SessionOrigin::ExecContinuation, "printf reused").await;
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
            let error = startup(&manager, SessionOrigin::ExecContinuation, "true", false)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("failed to spawn"), "{error}");
            no_artifacts(&manager).await;
        }
        Arc::get_mut(&mut manager).unwrap().config.shell = PathBuf::from("/bin/bash");
        let reservation = manager
            .reserve(SessionOrigin::ExecContinuation)
            .await
            .unwrap();
        let store = manager.create_output_store().await.unwrap();
        let spawned = manager
            .spawn(
                &reservation.id,
                SessionOrigin::ExecContinuation,
                "echo $$ > child.pid; read line",
                workspace.path(),
                false,
            )
            .await
            .unwrap();
        let pid = child_pid(&workspace).await;
        manager.shutdown_all().await;
        let result = manager
            .commit_reservation(
                reservation,
                SessionOrigin::ExecContinuation,
                spawned,
                store,
                None,
            )
            .await;
        assert!(result.is_err());
        child_terminated(pid).await;
        no_artifacts(&manager).await;
        assert_eq!(manager.active_session_count().await, 0);
    }
}

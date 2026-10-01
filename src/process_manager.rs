use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
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
use crate::session::Session;
use crate::session::SessionOrigin;
use crate::session::TerminalDelivery;
use crate::session_id::SessionId;
use crate::tools::ExecCommandArgs;
use crate::tools::ExecResponse;
use crate::tools::OutputRef;
use crate::tools::StartSessionArgs;
use crate::tools::WaitForExitArgs;
use crate::tools::WriteStdinArgs;
use crate::tools::{DEFAULT_POLL_YIELD_MS, DEFAULT_SESSION_YIELD_MS, DEFAULT_WRITE_YIELD_MS};
use crate::tools::{MAX_WAIT_SECONDS, MAX_YIELD_MS, MIN_WAIT_SECONDS, MIN_YIELD_MS};

#[derive(Default)]
struct StoreState {
    sessions: HashMap<SessionId, Arc<Session>>,
    reserved: HashMap<SessionId, SessionOrigin>,
}

struct PreparedExecResponse {
    response: ExecResponse,
    commit_end: Option<u64>,
    range_start: u64,
    range_end: u64,
    terminal: bool,
    delete_output: bool,
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
    state: Mutex<StoreState>,
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
            state: Mutex::new(StoreState::default()),
            shutting_down: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
        }))
    }

    pub(crate) fn instructions(&self) -> String {
        let default_instructions = concat!(
            "Use exec_command for ordinary stateless commands and start_session only when state must persist. When a returned session only needs time to finish, prefer wait_for_exit; ordinary stdout/stderr does not wake that wait. Use write_stdin when input, interruption, or immediate output polling is needed. ",
            "Oversized output uses head/tail and an output_ref raw-log path.",
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
        let id = self.reserve(origin).await?;
        let output_store = match self.create_output_store().await {
            Ok(store) => store,
            Err(error) => {
                self.release_reservation(&id).await;
                return Err(error.into());
            }
        };
        let spawned = match self.spawn(&id, origin, &command, &cwd, tty).await {
            Ok(spawned) => spawned,
            Err(error) => {
                self.release_reservation(&id).await;
                let _ = delete_unstarted_output(output_store).await;
                return Err(error);
            }
        };
        let session = Session::new(id.clone(), origin, spawned, output_store, build_output);
        self.commit_reservation(id.clone(), Arc::clone(&session))
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
            return Ok(Self::delivered_terminal_response(
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
            return Ok(Some(Self::delivered_terminal_response(
                &session, delivery, started,
            )));
        }

        // Re-snapshot under the interaction lock. Terminal state and output may
        // have changed while this passive wait was pending or while a concurrent
        // write_stdin owned the lock.
        let max_output_tokens = output_tokens(args.max_output_tokens, session.build_output());
        let prepare = self.prepare_response(&id, &session, started, max_output_tokens);
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

        self.finish_prepared_response(&session, prepared, started)
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
            return Ok(Self::delivered_terminal_response(
                session, delivery, started,
            ));
        }
        let prepared = self
            .prepare_response(id, session, started, max_output_tokens)
            .await?;
        let mut state = if prepared.terminal {
            Some(self.state.lock().await)
        } else {
            None
        };
        self.commit_prepared_response(id, session, &prepared, state.as_deref_mut())?;
        drop(state);
        self.finish_prepared_response(session, prepared, started)
            .await
    }

    async fn prepare_response(
        &self,
        _id: &SessionId,
        session: &Arc<Session>,
        started: Instant,
        max_output_tokens: usize,
    ) -> anyhow::Result<PreparedExecResponse> {
        let mut prepared = session
            .prepare_output(self.response_budget(max_output_tokens))
            .await?;
        let mut capture_error = session.terminal_reason();

        if let Some(read_error) = prepared.read_error.as_deref() {
            let reason = format!("output projection failed: {read_error}");
            merge_error(&mut capture_error, reason.clone());
            session.mark_forced_incomplete(&reason);
            session.terminate().await;
        }

        // Latch terminal state before committing the response cursor. If capture
        // completed after the first snapshot, refresh from the same cursor so the
        // final response includes every committed byte. If it completes later,
        // keep the session for one more poll instead of deleting unseen output.
        let terminal = session.is_terminal();
        if terminal && prepared.read_error.is_none() {
            prepared = session
                .prepare_output(self.response_budget(max_output_tokens))
                .await?;
            if let Some(read_error) = prepared.read_error.as_deref() {
                let reason = format!("output projection failed: {read_error}");
                merge_error(&mut capture_error, reason.clone());
                session.mark_forced_incomplete(&reason);
                session.terminate().await;
            }
        }

        if terminal {
            if let Some(reason) = session.terminal_reason().or_else(|| capture_error.clone()) {
                if let Err(error) = session.seal_incomplete(&reason).await {
                    merge_error(
                        &mut capture_error,
                        format!("failed to seal incomplete output artifact: {error}"),
                    );
                }
            } else if let Err(error) = session.seal_complete().await {
                let reason = format!("failed to seal output artifact: {error}");
                merge_error(&mut capture_error, reason.clone());
                session.mark_forced_incomplete(&reason);
                let _ = session.seal_incomplete(&reason).await;
            }
        }

        if let Some(reason) = session.terminal_reason() {
            merge_error(&mut capture_error, reason);
        }

        let recovery_published = session.recovery_path_published().await?;
        let need_ref = recovery_published
            || prepared.truncated
            || prepared.encoding_loss
            || capture_error.is_some();
        let output_ref = if need_ref {
            Some(
                Self::output_ref_for_range(session, prepared.snapshot.start, prepared.snapshot.end)
                    .await?,
            )
        } else {
            None
        };

        Ok(PreparedExecResponse {
            response: ExecResponse {
                call_wall_time_seconds: started.elapsed().as_secs_f64(),
                exit_code: terminal.then(|| session.known_exit_code()).flatten(),
                session_id: (!terminal).then(|| session.id.to_string()),
                output: prepared.output,
                output_truncated: prepared.truncated,
                output_encoding_loss: prepared.encoding_loss,
                capture_error,
                output_ref,
                peer_messages: None,
            },
            commit_end: prepared
                .read_error
                .is_none()
                .then_some(prepared.snapshot.end),
            range_start: prepared.snapshot.start,
            range_end: prepared.snapshot.end,
            terminal,
            delete_output: terminal && !need_ref,
        })
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
            session.record_terminal_delivery(&prepared.response);
        }
        Ok(())
    }

    fn delivered_terminal_response(
        session: &Session,
        delivery: TerminalDelivery,
        started: Instant,
    ) -> ExecResponse {
        ExecResponse {
            call_wall_time_seconds: started.elapsed().as_secs_f64(),
            exit_code: session.known_exit_code(),
            session_id: None,
            output: String::new(),
            output_truncated: false,
            output_encoding_loss: false,
            capture_error: delivery.capture_error,
            output_ref: delivery.output_ref,
            peer_messages: None,
        }
    }

    async fn finish_prepared_response(
        &self,
        session: &Arc<Session>,
        mut prepared: PreparedExecResponse,
        started: Instant,
    ) -> anyhow::Result<ExecResponse> {
        if prepared.delete_output
            && let Err(error) = session.delete_output().await
        {
            let cleanup_error = format!("failed to remove delivered output artifact: {error}");
            merge_error(&mut prepared.response.capture_error, cleanup_error);
            // The response has already committed, so preserve the artifact as a
            // recovery target instead of leaving an unreachable complete spool.
            match Self::output_ref_for_range(session, prepared.range_start, prepared.range_end)
                .await
            {
                Ok(output_ref) => {
                    session.publish_output_ref()?;
                    prepared.response.output_ref = Some(output_ref);
                }
                Err(error) => merge_error(
                    &mut prepared.response.capture_error,
                    format!("failed to publish output recovery reference: {error}"),
                ),
            }
        }

        prepared.response.call_wall_time_seconds = started.elapsed().as_secs_f64();
        if prepared.terminal {
            // Include post-commit cleanup errors before releasing the interaction
            // lock to callers already pinned to this terminal session.
            session.record_terminal_delivery(&prepared.response);
        }
        Ok(prepared.response)
    }

    async fn output_ref_for_range(
        session: &Session,
        range_start: u64,
        range_end: u64,
    ) -> io::Result<OutputRef> {
        let info = session.output_ref_info().await?;
        Ok(OutputRef {
            path: info.path.display().to_string(),
            range_start,
            range_end,
            stored_bytes: info.stored_bytes,
            capture_status: info.capture_status.into(),
            expires_at_unix_seconds: info.expires_at_unix_seconds,
            incomplete_reason: info.incomplete_reason,
        })
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
        let program = self.config.shell.to_string_lossy();
        let arguments = vec!["-c".to_owned(), command.to_owned()];
        let arg0 = None;
        if tty {
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
        }
        .with_context(|| format!("failed to spawn command in `{}`", cwd.display()))
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

    async fn reserve(&self, origin: SessionOrigin) -> anyhow::Result<SessionId> {
        if self.shutting_down.load(Ordering::SeqCst) {
            bail!("server is shutting down");
        }
        let mut state = self.state.lock().await;
        let active = state
            .sessions
            .values()
            .filter(|session| session.origin == origin)
            .count()
            + state
                .reserved
                .values()
                .filter(|reserved_origin| **reserved_origin == origin)
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
        state.reserved.insert(id.clone(), origin);
        Ok(id)
    }

    async fn release_reservation(&self, id: &SessionId) {
        self.state.lock().await.reserved.remove(id);
    }

    async fn commit_reservation(&self, id: SessionId, session: Arc<Session>) -> anyhow::Result<()> {
        let committed = {
            let mut state = self.state.lock().await;
            if state.reserved.remove(&id).is_some() {
                state.sessions.insert(id, Arc::clone(&session));
                true
            } else {
                false
            }
        };
        if !committed {
            session.terminate().await;
            bail!("session reservation disappeared before commit");
        }
        Ok(())
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

fn validate_directory(path: &Path) -> anyhow::Result<()> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_dir() {
        bail!("path is not a directory");
    }
    Ok(())
}

fn merge_error(target: &mut Option<String>, message: String) {
    if target.is_none() {
        *target = Some(message);
    }
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

async fn delete_unstarted_output(mut store: OutputStore) -> io::Result<()> {
    tokio::task::spawn_blocking(move || store.delete())
        .await
        .map_err(|error| io::Error::other(format!("output cleanup task failed: {error}")))?
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
        assert!(
            manager
                .instructions()
                .starts_with("Read README first.\n\nUse exec_command")
        );
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
        assert_eq!(delivery_ref.path, output_ref.path);
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

        let wait_manager = Arc::clone(&manager);
        let wait_id = session_id.clone();
        let waiter = tokio::spawn(async move {
            wait_manager
                .wait_for_exit_cancellable(test_wait_args(&wait_id), async move {
                    let _ = cancel_rx.await;
                })
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        cancel_tx.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
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
        let session_id = test_session(&manager, "sleep 0.40; printf commit-output").await;
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

        let wait_manager = Arc::clone(&manager);
        let wait_id = session_id.clone();
        let session = pinned_session(&manager, &session_id).await;
        let waiter = tokio::spawn(async move {
            let mut args = test_wait_args(&wait_id);
            args.max_output_tokens = Some(1);
            wait_manager
                .wait_for_exit_cancellable(args, async move {
                    let _ = cancel_rx.await;
                })
                .await
                .unwrap()
        });

        // Give the waiter time to pin the session, then block the terminal map
        // removal step. Cancellation while this lock is held must leave both the
        // output cursor and session map unchanged.
        tokio::time::sleep(Duration::from_millis(40)).await;
        let state_guard = manager.state.lock().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        cancel_tx.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
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
}

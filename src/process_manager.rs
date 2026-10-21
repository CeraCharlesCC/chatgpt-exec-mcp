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
use crate::output_postprocessing::OutputPostprocessing;
use crate::output_store::OutputStore;
use crate::output_store::OutputStoreManager;
use crate::response_delivery::{self, PreparedExecResponse};
use crate::session::{Session, SessionLease};
use crate::session_id::SessionId;
use crate::tools::ExecCommandArgs;
use crate::tools::ExecResponse;
use crate::tools::WaitForExitArgs;
use crate::tools::WriteStdinArgs;
use crate::tools::{DEFAULT_POLL_YIELD_MS, DEFAULT_WRITE_YIELD_MS};
use crate::tools::{MAX_WAIT_SECONDS, MAX_YIELD_MS, MIN_WAIT_SECONDS, MIN_YIELD_MS};

#[derive(Default)]
struct StoreState {
    sessions: HashMap<SessionId, Arc<Session>>,
    reserved: HashMap<SessionId, ReservedSession>,
}

struct ReservedSession {
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
    command: String,
    workdir: Option<String>,
    tty: bool,
    yield_ms: u64,
    max_output_tokens: usize,
    postprocessing: OutputPostprocessing,
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
            "Use exec_command to start commands, shells, and REPLs; tty only selects a PTY. Reuse session_id with write_stdin to preserve process state. ",
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
        let postprocessing = OutputPostprocessing::for_command(&args.cmd);
        let output_tokens = output_tokens(args.max_output_tokens, postprocessing);
        self.start_and_wait(StartAndWaitArgs {
            command: args.cmd,
            workdir: args.workdir,
            tty: args.tty,
            yield_ms,
            max_output_tokens: output_tokens,
            postprocessing,
        })
        .await
    }

    async fn start_and_wait(&self, args: StartAndWaitArgs) -> anyhow::Result<ExecResponse> {
        let StartAndWaitArgs {
            command,
            workdir,
            tty,
            yield_ms,
            max_output_tokens,
            postprocessing,
        } = args;
        let started = Instant::now();
        let cwd = self.resolve_workdir(workdir.as_deref())?;
        let reservation = self.reserve().await?;
        let id = reservation.id.clone();
        // Blocking artifact creation owns its result; a cancelled waiter drops
        // the unpublished store and removes its files when creation finishes.
        let output_store = self.create_output_store().await?;
        let spawned = self.spawn(&id, &command, &cwd, tty).await?;
        // Registration transfers ownership to the manager. A cancelled initial
        // wait then follows the existing registered-session lifetime policy.
        let (session, _lease) = self
            .commit_reservation(reservation, spawned, output_store, postprocessing)
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
        let (session, _lease) = {
            let state = self.state.lock().await;
            state
                .sessions
                .get(&id)
                .map(|session| (Arc::clone(session), session.lease()))
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

        let max_output_tokens = output_tokens(args.max_output_tokens, session.postprocessing());
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
        let (session, _lease) = {
            let state = self.state.lock().await;
            state
                .sessions
                .get(&id)
                .map(|session| (Arc::clone(session), session.lease()))
        }
        .ok_or_else(|| anyhow::anyhow!("unknown or expired session_id `{id}`"))?;

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
        let max_output_tokens = output_tokens(args.max_output_tokens, session.postprocessing());
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
        command: &str,
        cwd: &Path,
        tty: bool,
    ) -> anyhow::Result<codex_utils_pty::SpawnedProcess> {
        let cwd = std::fs::canonicalize(cwd).context("workdir canonicalize failed before spawn")?;
        validate_directory(&cwd).context("invalid spawn workdir")?;
        let mut environment = self.config.child_env.for_spawn(&cwd);
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

    async fn reserve(&self) -> anyhow::Result<StartupReservation> {
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
        let active = state.sessions.len() + state.reserved.len();
        let limit = self.config.max_sessions;
        if active >= limit {
            bail!("session quota reached ({limit})");
        }

        let id = SessionId::generate_unique(|candidate| {
            state.sessions.contains_key(candidate) || state.reserved.contains_key(candidate)
        })?;
        let lease = Arc::new(());
        state.reserved.insert(
            id.clone(),
            ReservedSession {
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
        spawned: codex_utils_pty::SpawnedProcess,
        output_store: OutputStore,
        postprocessing: OutputPostprocessing,
    ) -> anyhow::Result<(Arc<Session>, SessionLease)> {
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
            spawned,
            output_store,
            postprocessing,
        );
        let lease = session.lease();
        state
            .sessions
            .insert(reservation.id.clone(), Arc::clone(&session));
        drop(state);
        Ok((session, lease))
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
                    let timeout = self.config.session_idle_timeout;
                    (!session.has_active_operations() && session.idle_for() >= timeout)
                        .then(|| id.clone())
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

fn output_tokens(requested: Option<usize>, postprocessing: OutputPostprocessing) -> usize {
    requested.unwrap_or_else(|| postprocessing.default_max_output_tokens())
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
mod tests;

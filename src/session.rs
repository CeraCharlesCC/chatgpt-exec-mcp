use std::io;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use codex_utils_pty::ProcessHandle;
use codex_utils_pty::SpawnedProcess;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::output_store::OutputRefInfo;
use crate::output_store::OutputStore;
use crate::output_store::Snapshot;
use crate::output_summary::{BuildOutput, add_build_summary};
use crate::session_id::SessionId;
use crate::tools::{ExecResponse, OutputRef};

const CAPTURE_QUEUE_CHUNKS: usize = 128;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const FORCE_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOrigin {
    ExecContinuation,
    ExplicitSession,
}

#[derive(Clone, Debug)]
pub struct PreparedOutput {
    pub snapshot: Snapshot,
    pub output: String,
    pub truncated: bool,
    pub encoding_loss: bool,
    pub read_error: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum CaptureStream {
    Stdout,
    Stderr,
}

struct CaptureChunk {
    bytes: Vec<u8>,
    stream: CaptureStream,
}

#[derive(Clone, Debug)]
pub(crate) struct TerminalDelivery {
    pub capture_error: Option<String>,
    pub output_ref: Option<OutputRef>,
}

pub struct Session {
    pub id: SessionId,
    pub origin: SessionOrigin,
    process: ProcessHandle,
    output_store: Arc<StdMutex<OutputStore>>,
    build_output: Option<BuildOutput>,
    output_cursor: Mutex<u64>,
    output_sequence: AtomicU64,
    output_notify: Notify,
    streams_open: AtomicUsize,
    capture_writer_done: AtomicBool,
    capture_done_notify: Notify,
    exited: AtomicBool,
    exit_code: StdMutex<Option<i32>>,
    exit_notify: Notify,
    capture_error: StdMutex<Option<String>>,
    forced_incomplete_reason: StdMutex<Option<String>>,
    capture_failure_notify: Notify,
    stop_capture: AtomicBool,
    terminal_delivery: StdMutex<Option<TerminalDelivery>>,
    pub interaction_lock: Mutex<()>,
    last_used: StdMutex<Instant>,
}

impl Session {
    pub(crate) fn new(
        id: SessionId,
        origin: SessionOrigin,
        spawned: SpawnedProcess,
        output_store: OutputStore,
        build_output: Option<BuildOutput>,
    ) -> Arc<Self> {
        let SpawnedProcess {
            session: process,
            stdout_rx,
            stderr_rx,
            exit_rx,
        } = spawned;
        let now = Instant::now();
        let session = Arc::new(Self {
            id,
            origin,
            process,
            output_store: Arc::new(StdMutex::new(output_store)),
            build_output,
            output_cursor: Mutex::new(0),
            output_sequence: AtomicU64::new(0),
            output_notify: Notify::new(),
            streams_open: AtomicUsize::new(2),
            capture_writer_done: AtomicBool::new(false),
            capture_done_notify: Notify::new(),
            exited: AtomicBool::new(false),
            exit_code: StdMutex::new(None),
            exit_notify: Notify::new(),
            capture_error: StdMutex::new(None),
            forced_incomplete_reason: StdMutex::new(None),
            capture_failure_notify: Notify::new(),
            stop_capture: AtomicBool::new(false),
            terminal_delivery: StdMutex::new(None),
            interaction_lock: Mutex::new(()),
            last_used: StdMutex::new(now),
        });

        let (capture_tx, capture_rx) = mpsc::channel(CAPTURE_QUEUE_CHUNKS);
        Self::spawn_capture_writer(Arc::clone(&session), capture_rx);
        Self::spawn_output_reader(
            Arc::clone(&session),
            stdout_rx,
            capture_tx.clone(),
            CaptureStream::Stdout,
        );
        Self::spawn_output_reader(
            Arc::clone(&session),
            stderr_rx,
            capture_tx,
            CaptureStream::Stderr,
        );
        Self::spawn_exit_reader(Arc::clone(&session), exit_rx);
        session
    }

    fn spawn_output_reader(
        session: Arc<Self>,
        mut receiver: mpsc::Receiver<Vec<u8>>,
        capture_tx: mpsc::Sender<CaptureChunk>,
        stream: CaptureStream,
    ) {
        tokio::spawn(async move {
            while let Some(bytes) = receiver.recv().await {
                if session.stop_capture.load(Ordering::SeqCst) {
                    break;
                }
                if capture_tx
                    .send(CaptureChunk { bytes, stream })
                    .await
                    .is_err()
                {
                    break;
                }
            }

            if !session.stop_capture.load(Ordering::SeqCst)
                && let Some(error) = session.process.output_error()
            {
                session.record_capture_error(format!("output transport failed: {error}"), false);
            }
            session.streams_open.fetch_sub(1, Ordering::SeqCst);
            session.capture_done_notify.notify_waiters();
            session.output_notify.notify_waiters();
        });
    }

    fn spawn_capture_writer(session: Arc<Self>, mut capture_rx: mpsc::Receiver<CaptureChunk>) {
        tokio::spawn(async move {
            while let Some(chunk) = capture_rx.recv().await {
                let stream = chunk.stream;
                let store = Arc::clone(&session.output_store);
                let append_result = tokio::task::spawn_blocking(move || {
                    let mut store = lock_output_store(&store)?;
                    store.append(&chunk.bytes)
                })
                .await
                .map_err(|error| io::Error::other(format!("output writer task failed: {error}")))
                .and_then(|result| result);

                match append_result {
                    Ok(_) => {
                        session.output_sequence.fetch_add(1, Ordering::SeqCst);
                        session.output_notify.notify_waiters();
                    }
                    Err(error) => {
                        let message = format!("output capture failed on {stream:?}: {error}");
                        session.record_capture_error(message, true);
                        break;
                    }
                }
            }
            session.capture_writer_done.store(true, Ordering::SeqCst);
            session.capture_done_notify.notify_waiters();
            session.output_notify.notify_waiters();
        });
    }

    fn record_capture_error(&self, message: String, stop_capture: bool) {
        let mut changed = false;
        if let Ok(mut stored) = self.capture_error.lock()
            && stored.is_none()
        {
            *stored = Some(message);
            changed = true;
        }
        if stop_capture {
            self.stop_capture.store(true, Ordering::SeqCst);
        }
        self.process.request_terminate();
        if changed {
            self.capture_failure_notify.notify_waiters();
            self.output_notify.notify_waiters();
            self.capture_done_notify.notify_waiters();
        }
    }

    pub(crate) fn mark_forced_incomplete(&self, reason: &str) {
        let mut changed = false;
        if let Ok(mut stored) = self.forced_incomplete_reason.lock()
            && stored.is_none()
        {
            *stored = Some(reason.to_owned());
            changed = true;
        }
        self.stop_capture.store(true, Ordering::SeqCst);
        if changed {
            self.capture_failure_notify.notify_waiters();
            self.output_notify.notify_waiters();
            self.capture_done_notify.notify_waiters();
        }
    }

    fn spawn_exit_reader(session: Arc<Self>, receiver: oneshot::Receiver<i32>) {
        tokio::spawn(async move {
            let code = receiver.await.unwrap_or(-1);
            if let Ok(mut stored) = session.exit_code.lock() {
                *stored = Some(code);
            }
            session.exited.store(true, Ordering::SeqCst);
            session.exit_notify.notify_waiters();
            session.output_notify.notify_waiters();
            session.capture_done_notify.notify_waiters();
        });
    }

    async fn with_store<T, F>(&self, operation: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut OutputStore) -> io::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.output_store);
        tokio::task::spawn_blocking(move || {
            let mut store = lock_output_store(&store)?;
            operation(&mut store)
        })
        .await
        .map_err(|error| io::Error::other(format!("output store task failed: {error}")))?
    }

    pub fn touch(&self) {
        if let Ok(mut last_used) = self.last_used.lock() {
            *last_used = Instant::now();
        }
    }

    pub fn idle_for(&self) -> Duration {
        self.last_used
            .lock()
            .map(|last_used| last_used.elapsed())
            .unwrap_or_default()
    }

    pub fn is_exited(&self) -> bool {
        self.exited.load(Ordering::SeqCst) || self.process.has_exited()
    }

    pub fn known_exit_code(&self) -> Option<i32> {
        self.exit_code
            .lock()
            .ok()
            .and_then(|stored| *stored)
            .or_else(|| self.process.exit_code())
    }

    pub fn capture_error(&self) -> Option<String> {
        self.capture_error
            .lock()
            .ok()
            .and_then(|stored| stored.clone())
    }

    pub fn terminal_reason(&self) -> Option<String> {
        self.capture_error().or_else(|| {
            self.forced_incomplete_reason
                .lock()
                .ok()
                .and_then(|stored| stored.clone())
        })
    }

    fn capture_tasks_done(&self) -> bool {
        self.streams_open.load(Ordering::SeqCst) == 0
            && self.capture_writer_done.load(Ordering::SeqCst)
    }

    pub fn is_capture_complete(&self) -> bool {
        self.terminal_reason().is_none() && self.is_exited() && self.capture_tasks_done()
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal_reason().is_some() || self.is_capture_complete()
    }

    pub(crate) fn terminal_delivery(&self) -> Option<TerminalDelivery> {
        self.terminal_delivery
            .lock()
            .expect("terminal delivery lock poisoned")
            .clone()
    }

    pub(crate) fn record_terminal_delivery(&self, response: &ExecResponse) {
        let mut output_ref = response.output_ref.clone();
        if let Some(output_ref) = &mut output_ref {
            // A pinned concurrent caller receives no new bytes, but can still
            // recover the retained log and inspect its final capture status.
            output_ref.range_start = output_ref.range_end;
        }
        *self
            .terminal_delivery
            .lock()
            .expect("terminal delivery lock poisoned") = Some(TerminalDelivery {
            capture_error: response.capture_error.clone(),
            output_ref,
        });
    }

    pub async fn has_pending_output(&self) -> io::Result<bool> {
        let cursor = *self.output_cursor.lock().await;
        let committed = self.with_store(|store| Ok(store.committed_bytes())).await?;
        Ok(committed > cursor)
    }

    pub fn output_sequence(&self) -> u64 {
        self.output_sequence.load(Ordering::SeqCst)
    }

    pub(crate) fn build_output(&self) -> Option<BuildOutput> {
        self.build_output
    }

    pub async fn send_input(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        if let Some(reason) = self.terminal_reason() {
            anyhow::bail!("session output capture is incomplete: {reason}");
        }
        if self.is_exited() {
            anyhow::bail!("process has already exited; poll until output capture is finalized");
        }
        self.process
            .writer_sender()
            .send(bytes)
            .await
            .map_err(|_| anyhow::anyhow!("process stdin is closed"))
    }

    pub fn interrupt(&self) -> anyhow::Result<()> {
        self.process
            .signal(codex_utils_pty::ProcessSignal::Interrupt)
            .map_err(Into::into)
    }

    pub async fn wait_until_exit_or_timeout(&self, duration: Duration) {
        if !self.is_exited() && self.capture_error().is_none() {
            let exit = self.exit_notify.notified();
            let capture_failure = self.capture_failure_notify.notified();
            // `notify_waiters` does not retain a permit for futures created after
            // the notification. Re-check after creating both futures so an exit or
            // capture failure racing the initial state check cannot be missed.
            if !self.is_exited() && self.capture_error().is_none() {
                let _ = tokio::time::timeout(duration, async {
                    tokio::select! {
                        _ = exit => {},
                        _ = capture_failure => {},
                    }
                })
                .await;
            }
        }
        self.settle_after_wait().await;
    }

    pub async fn wait_for_activity(&self, baseline: u64, duration: Duration) {
        if self.output_sequence() == baseline && !self.is_exited() && self.capture_error().is_none()
        {
            let deadline = tokio::time::Instant::now() + duration;
            loop {
                let output = self.output_notify.notified();
                let exit = self.exit_notify.notified();
                let capture_failure = self.capture_failure_notify.notified();
                if self.is_exited()
                    || self.output_sequence() != baseline
                    || self.capture_error().is_some()
                {
                    break;
                }
                if tokio::time::timeout_at(deadline, async {
                    tokio::select! {
                        _ = output => {},
                        _ = exit => {},
                        _ = capture_failure => {},
                    }
                })
                .await
                .is_err()
                {
                    break;
                }
            }
        }
        self.settle_after_wait().await;
    }

    async fn settle_after_wait(&self) {
        if self.capture_error().is_some() {
            self.wait_for_exit(FORCE_DRAIN_TIMEOUT).await;
            if !self.is_exited() {
                self.process.terminate();
                self.wait_for_exit(FORCE_DRAIN_TIMEOUT).await;
            }
            let _ = self.wait_for_capture_tasks(FORCE_DRAIN_TIMEOUT).await;
            return;
        }

        if self.is_exited()
            && !self.capture_tasks_done()
            && !self.wait_for_capture_tasks(DRAIN_TIMEOUT).await
        {
            let reason = "output drain deadline exceeded after root process exit".to_owned();
            self.mark_forced_incomplete(&reason);
            self.process.terminate();
            let _ = self.wait_for_capture_tasks(FORCE_DRAIN_TIMEOUT).await;
        }
    }

    async fn wait_for_exit(&self, duration: Duration) -> bool {
        if self.is_exited() {
            return true;
        }
        let notified = self.exit_notify.notified();
        if self.is_exited() {
            return true;
        }
        let _ = tokio::time::timeout(duration, notified).await;
        self.is_exited()
    }

    async fn wait_for_capture_tasks(&self, duration: Duration) -> bool {
        if self.capture_tasks_done() {
            return true;
        }
        let deadline = tokio::time::Instant::now() + duration;
        while !self.capture_tasks_done() {
            let notified = self.capture_done_notify.notified();
            if self.capture_tasks_done() {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }
        self.capture_tasks_done()
    }

    pub async fn prepare_output(&self, max_bytes: usize) -> io::Result<PreparedOutput> {
        let cursor = *self.output_cursor.lock().await;
        let snapshot = self
            .with_store(move |store| Ok(store.snapshot(cursor)))
            .await?;
        let projection_snapshot = snapshot.clone();
        let hold_incomplete_utf8 = !self.is_terminal();
        let build_output = self.build_output;
        match self
            .with_store(move |store| {
                let projection =
                    store.project(projection_snapshot, max_bytes, hold_incomplete_utf8)?;
                Ok(add_build_summary(projection, build_output, max_bytes))
            })
            .await
        {
            Ok(projection) => Ok(PreparedOutput {
                snapshot: projection.snapshot,
                output: projection.output,
                truncated: projection.truncated,
                encoding_loss: projection.encoding_loss,
                read_error: None,
            }),
            Err(error) => Ok(PreparedOutput {
                snapshot,
                output: String::new(),
                truncated: false,
                encoding_loss: false,
                read_error: Some(error.to_string()),
            }),
        }
    }

    pub(crate) fn try_commit_output(&self, end: u64) -> io::Result<()> {
        let mut cursor = self.output_cursor.try_lock().map_err(|error| {
            io::Error::other(format!(
                "output cursor was unexpectedly busy during response commit: {error}"
            ))
        })?;
        if end >= *cursor {
            *cursor = end;
        }
        Ok(())
    }

    pub async fn seal_complete(&self) -> io::Result<()> {
        self.with_store(|store| store.finish(None)).await
    }

    pub async fn seal_incomplete(&self, reason: &str) -> io::Result<()> {
        let reason = reason.to_owned();
        self.with_store(move |store| store.finish(Some(reason)))
            .await
    }

    pub async fn recovery_path_published(&self) -> io::Result<bool> {
        self.with_store(|store| Ok(store.recovery_path_published()))
            .await
    }

    pub async fn output_ref_info(&self) -> io::Result<OutputRefInfo> {
        self.with_store(|store| Ok(store.ref_info())).await
    }

    pub fn publish_output_ref(&self) -> io::Result<()> {
        lock_output_store(&self.output_store)?.publish();
        Ok(())
    }

    pub async fn delete_output(&self) -> io::Result<()> {
        self.with_store(OutputStore::delete).await
    }

    pub async fn terminate(&self) {
        self.process.request_terminate();
        self.wait_for_exit(FORCE_DRAIN_TIMEOUT).await;
        if !self.is_exited() || !self.capture_tasks_done() {
            self.process.terminate();
            self.wait_for_exit(FORCE_DRAIN_TIMEOUT).await;
            let _ = self.wait_for_capture_tasks(FORCE_DRAIN_TIMEOUT).await;
        }
    }

    pub async fn terminate_and_seal_incomplete(&self, default_reason: &str) -> io::Result<()> {
        self.terminate().await;
        let reason = self
            .terminal_reason()
            .unwrap_or_else(|| default_reason.to_owned());
        self.seal_incomplete(&reason).await
    }
}

fn lock_output_store(
    store: &Arc<StdMutex<OutputStore>>,
) -> io::Result<std::sync::MutexGuard<'_, OutputStore>> {
    store
        .lock()
        .map_err(|_| io::Error::other("output store lock poisoned"))
}

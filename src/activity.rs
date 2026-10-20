//! In-memory recent activity with a bounded, temporary compressed history.
//!
//! The dashboard's normal snapshots touch only the recent queue. Older pages
//! read zstd chunks on demand; serialization and file I/O run on one background
//! worker and never hold the activity mutex.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Barrier;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;

use crate::agent_pool::{AgentIdentity, PoolSendArgs};

const RECENT_THRESHOLD: usize = 500;
const SPILL_BATCH: usize = 250;
const MAX_ARCHIVE_CHUNKS: usize = 8;
const MAX_PENDING_CHUNKS: usize = 4;
const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CHUNK_BYTES: u64 = MAX_ARCHIVE_BYTES / MAX_ARCHIVE_CHUNKS as u64;
const MAX_SERIALIZED_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RETAINED_EVENTS: usize = 2_500;
const MAX_PAGE_SIZE: usize = 200;

/// New activity events carry an explicit schema discriminator; older archive
/// records deserialize as version zero and keep their original `kind`/`detail`.
const ACTIVITY_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLifecycleType {
    Tool,
    MessageSend,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityPhase {
    Start,
    Finish,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityMessageStatus {
    Sending,
    Queued,
    Failed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ActivityMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipients: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ActivityMessageStatus>,
}

impl ActivityMessage {
    /// Capture only when the diagnostic sink is enabled, without forcing
    /// another JSON serialization or borrowing args after pool_send consumes them.
    pub(crate) fn from_pool_send_args(args: &PoolSendArgs) -> Option<Self> {
        if args.action.is_some() {
            return None;
        }
        Some(Self {
            pool: Some(args.pool.clone()),
            target: Some(args.target.as_deref().unwrap_or("global").to_owned()),
            body: Some(args.message.as_ref()?.clone()),
            in_reply_to: args.in_reply_to.clone(),
            ..Self::default()
        })
    }

    fn from_start_detail(detail: &Value) -> Option<Self> {
        let arguments = detail.get("arguments")?;
        if arguments
            .get("action")
            .is_some_and(|action| !action.is_null())
        {
            return None;
        }
        let body = arguments.get("message")?.as_str()?.to_owned();
        Some(Self {
            pool: arguments
                .get("pool")
                .and_then(Value::as_str)
                .map(str::to_owned),
            target: Some(
                arguments
                    .get("target")
                    .and_then(Value::as_str)
                    .unwrap_or("global")
                    .to_owned(),
            ),
            body: Some(body),
            in_reply_to: arguments
                .get("in_reply_to")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ..Self::default()
        })
    }
}

/// Captured from tool arguments only for an opted-in activity consumer.
/// The completion can then be rendered independently of the start row.
#[derive(Default)]
pub(crate) struct ActivityContext {
    pub pool: Option<String>,
    pub message: Option<ActivityMessage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityLifecycle {
    #[serde(rename = "type")]
    pub event_type: ActivityLifecycleType,
    pub phase: ActivityPhase,
    pub tool: String,
    /// Pool identity even when the matching start event is on another page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_event_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<ActivityMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peer_messages: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delivery_receipts: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityEvent {
    pub id: u64,
    pub timestamp_ms: i64,
    #[serde(default)]
    pub schema_version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<ActivityLifecycle>,
    pub kind: String,
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    pub agents: Vec<AgentIdentity>,
    pub detail: Value,
}

/// An activity-history page in chronological (ascending event ID) order.
#[derive(Debug, Serialize)]
pub struct ActivityPage {
    pub events: Vec<ActivityEvent>,
    /// Counterparts absent from `events`, bounded by this page's size.
    pub correlated_events: Vec<ActivityEvent>,
    /// Exclusive cursor for the next older page, if any.
    pub next_before_id: Option<u64>,
    pub has_more: bool,
}

/// An immutable chunk stays readable while a page is in flight, even if CLEAR
/// or retention simultaneously removes the chunk from the hub.
#[derive(Debug)]
struct ArchiveFile {
    path: PathBuf,
    bytes: u64,
}

impl Drop for ArchiveFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Clone, Debug)]
struct ArchiveChunk {
    first_id: u64,
    last_id: u64,
    count: usize,
    file: Arc<ArchiveFile>,
}

#[derive(Clone)]
struct PendingChunk {
    generation: u64,
    first_id: u64,
    last_id: u64,
    events: Arc<Vec<ActivityEvent>>,
}

#[derive(Default)]
struct CorrelationPair {
    start: Option<ActivityEvent>,
    completion: Option<ActivityEvent>,
}

#[derive(Default)]
struct ActivityState {
    next_id: u64,
    generation: u64,
    recent: VecDeque<ActivityEvent>,
    pending: VecDeque<PendingChunk>,
    archived: VecDeque<ArchiveChunk>,
    /// At most the bounded retained suffix, keyed by the start event ID.
    /// Keeping these in RAM makes snapshot correlation independent of zstd I/O.
    correlations: BTreeMap<u64, CorrelationPair>,
    archive_bytes: u64,
    shutdown: bool,
}

impl ActivityState {
    fn oldest_retained_id(&self) -> Option<u64> {
        self.archived
            .front()
            .map(|chunk| chunk.first_id)
            .or_else(|| self.pending.front().map(|chunk| chunk.first_id))
            .or_else(|| self.recent.front().map(|event| event.id))
    }

    fn trim_correlations(&mut self) {
        let oldest = self.oldest_retained_id().unwrap_or(u64::MAX);
        while self
            .correlations
            .first_key_value()
            .is_some_and(|(&id, _)| id < oldest)
            || self.correlations.len() > MAX_RETAINED_EVENTS
        {
            self.correlations.pop_first();
        }
    }

    fn correlated_events(&self, events: &[ActivityEvent], limit: usize) -> Vec<ActivityEvent> {
        let visible: HashSet<_> = events.iter().map(|event| event.id).collect();
        let oldest = self.oldest_retained_id().unwrap_or(u64::MAX);
        let mut seen = HashSet::new();
        let mut matches = Vec::new();
        for event in events {
            let (start_id, wants_completion) = match event.kind.as_str() {
                "tool.start" => (Some(event.id), true),
                "tool.finish" | "tool.error" => (
                    event
                        .lifecycle
                        .as_ref()
                        .and_then(|lifecycle| lifecycle.start_event_id)
                        .or_else(|| event.detail.get("start_event_id").and_then(Value::as_u64)),
                    false,
                ),
                _ => (None, false),
            };
            let counterpart = start_id
                .and_then(|id| self.correlations.get(&id))
                .and_then(|pair| {
                    if wants_completion {
                        pair.completion.as_ref()
                    } else {
                        pair.start.as_ref()
                    }
                });
            if let Some(counterpart) = counterpart
                && counterpart.id >= oldest
                && !visible.contains(&counterpart.id)
                && seen.insert(counterpart.id)
            {
                matches.push(counterpart.clone());
                if matches.len() >= limit {
                    break;
                }
            }
        }
        matches.sort_by_key(|event| event.id);
        matches
    }

    fn retained_count(&self) -> usize {
        self.recent.len()
            + self
                .pending
                .iter()
                .map(|chunk| chunk.events.len())
                .sum::<usize>()
            + self.archived.iter().map(|chunk| chunk.count).sum::<usize>()
    }

    /// Return evictions to the caller: dropping files must happen outside the lock.
    fn trim_archives(&mut self) -> Vec<ArchiveChunk> {
        let mut evicted = Vec::new();
        while self.archived.len() > MAX_ARCHIVE_CHUNKS
            || self.archive_bytes > MAX_ARCHIVE_BYTES
            || self.retained_count() > MAX_RETAINED_EVENTS
        {
            let Some(oldest) = self.archived.pop_front() else {
                break;
            };
            self.archive_bytes = self.archive_bytes.saturating_sub(oldest.file.bytes);
            evicted.push(oldest);
        }
        self.trim_correlations();
        evicted
    }
}

/// `kind` and `detail` remain the legacy wire contract. New producers also
/// record a typed lifecycle so consumers need not infer message state by
/// examining adjacent events or digging through untyped tool result JSON.
fn lifecycle_for_event(
    kind: &str,
    detail: &Value,
    agents: &[AgentIdentity],
    state: &ActivityState,
    context: ActivityContext,
) -> Option<ActivityLifecycle> {
    let phase = match kind {
        "tool.start" => ActivityPhase::Start,
        "tool.finish" => ActivityPhase::Finish,
        "tool.error" => ActivityPhase::Error,
        _ => return None,
    };
    let tool = detail.get("tool")?.as_str()?.to_owned();
    let start_event_id = detail.get("start_event_id").and_then(Value::as_u64);
    let ActivityContext {
        pool: pool_context,
        message: message_context,
    } = context;
    let pool = pool_context
        .or_else(|| {
            message_context
                .as_ref()
                .and_then(|message| message.pool.clone())
        })
        .or_else(|| {
            detail
                .get("arguments")
                .and_then(|arguments| arguments.get("pool"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            start_event_id
                .and_then(|id| state.correlations.get(&id))
                .and_then(|pair| pair.start.as_ref())
                .and_then(|event| event.lifecycle.as_ref())
                .and_then(|lifecycle| lifecycle.pool.clone())
        });

    let mut message = if tool == "pool_send" {
        message_context.or_else(|| {
            if phase == ActivityPhase::Start {
                ActivityMessage::from_start_detail(detail)
            } else {
                start_event_id
                    .and_then(|id| state.correlations.get(&id))
                    .and_then(|pair| pair.start.as_ref())
                    .and_then(|event| event.lifecycle.as_ref())
                    .and_then(|lifecycle| lifecycle.message.clone())
            }
        })
    } else {
        None
    };

    if let Some(message) = message.as_mut() {
        let result = detail.get("result");
        let value = result.and_then(|result| result.get("value"));
        // A successful first join may assign the sender's name in the result;
        // otherwise use an already known member identity for this session.
        if let Some(from) = value
            .and_then(|value| value.get("assigned_agent"))
            .and_then(Value::as_str)
            .or_else(|| {
                agents
                    .iter()
                    .find(|agent| message.pool.as_deref() == Some(agent.pool.as_str()))
                    .map(|agent| agent.agent.as_str())
            })
        {
            message.from = Some(from.to_owned());
        }
        if phase != ActivityPhase::Start {
            message.message_id = value
                .and_then(|value| value.get("message_id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            message.recipients = value
                .and_then(|value| value.get("recipients"))
                .and_then(Value::as_array)
                .map(|recipients| {
                    recipients
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                });
            if phase == ActivityPhase::Error {
                message.error = result
                    .and_then(|result| result.get("error"))
                    .and_then(Value::as_str)
                    .or_else(|| {
                        value
                            .and_then(|value| value.get("error"))
                            .and_then(Value::as_str)
                    })
                    .map(str::to_owned)
                    .or_else(|| Some("Send failed".to_owned()));
            }
        }
        message.status = Some(match phase {
            ActivityPhase::Start => ActivityMessageStatus::Sending,
            ActivityPhase::Finish => ActivityMessageStatus::Queued,
            ActivityPhase::Error => ActivityMessageStatus::Failed,
        });
    }

    let result = detail.get("result");
    let result_value = result.and_then(|result| result.get("value"));
    let array = |key: &str| {
        result
            .and_then(|result| result.get(key))
            .or_else(|| result_value.and_then(|value| value.get(key)))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    let mut peer_messages = array("peer_messages");
    if peer_messages.is_empty() && tool == "pool_wait" {
        peer_messages = array("messages");
    }
    Some(ActivityLifecycle {
        event_type: if message.is_some() {
            ActivityLifecycleType::MessageSend
        } else {
            ActivityLifecycleType::Tool
        },
        phase,
        tool,
        pool,
        start_event_id,
        message,
        peer_messages,
        delivery_receipts: array("delivery_receipts"),
    })
}

#[derive(Default)]
struct Shared {
    state: Mutex<ActivityState>,
    wake: Condvar,
    #[cfg(test)]
    worker_hook: Mutex<Option<Arc<WorkerTestHook>>>,
}

#[cfg(test)]
struct WorkerTestHook {
    entered: Barrier,
    resume: Barrier,
    finished: Barrier,
}

fn lock_state(shared: &Shared) -> MutexGuard<'_, ActivityState> {
    shared
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub struct ActivityHub {
    enabled: bool,
    shared: Arc<Shared>,
    capacity: usize,
    archive_dir: Option<TempDir>,
    worker: OnceLock<Option<JoinHandle<()>>>,
}

impl ActivityHub {
    /// A no-op diagnostic sink for runs with no activity consumer. Callers
    /// should check `is_enabled` before building JSON or fetching identities;
    /// `emit` itself is also inert in case a call site does not check.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            shared: Arc::new(Shared::default()),
            capacity: 0,
            archive_dir: None,
            worker: OnceLock::new(),
        }
    }

    /// Whether diagnostics are being collected. This is immutable for a hub's
    /// lifetime, so callers can cheaply skip diagnostic-only work.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.clamp(1, RECENT_THRESHOLD);
        // Small hubs retain their historical fixed-size ring-buffer behavior.
        // The production hub (capacity 500) enables tiered retention.
        let archive_dir = if capacity == RECENT_THRESHOLD {
            match tempfile::Builder::new()
                .prefix("chatgpt-exec-activity-")
                .tempdir()
            {
                Ok(dir) => Some(dir),
                Err(error) => {
                    eprintln!("activity archive disabled: cannot create temp directory: {error}");
                    None
                }
            }
        } else {
            None
        };
        Self {
            enabled: true,
            shared: Arc::new(Shared::default()),
            capacity,
            archive_dir,
            worker: OnceLock::new(),
        }
    }

    pub fn emit(
        &self,
        kind: impl Into<String>,
        level: impl Into<String>,
        session: Option<&str>,
        agents: Vec<AgentIdentity>,
        detail: Value,
    ) -> u64 {
        self.emit_with_context(
            kind,
            level,
            session,
            agents,
            detail,
            ActivityContext::default(),
        )
    }

    /// In-flight pool calls carry their pool/message context to completion
    /// without depending on a still-retained start event.
    pub(crate) fn emit_with_context(
        &self,
        kind: impl Into<String>,
        level: impl Into<String>,
        session: Option<&str>,
        agents: Vec<AgentIdentity>,
        detail: Value,
        context: ActivityContext,
    ) -> u64 {
        if !self.enabled {
            // Zero is never a real event ID; no IDs, strings, or event state
            // are allocated for a disabled sink.
            return 0;
        }
        let mut state = lock_state(&self.shared);
        // Allocation and insertion share the same lock so concurrent emitters
        // cannot put event IDs in a different order from their insertion order.
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("activity event ID overflow");
        let id = state.next_id;
        let kind = kind.into();
        let lifecycle = lifecycle_for_event(&kind, &detail, &agents, &state, context);
        let event = ActivityEvent {
            id,
            timestamp_ms: now_ms(),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            lifecycle,
            kind,
            level: level.into(),
            session: session.map(str::to_owned),
            agents,
            detail,
        };
        match event.kind.as_str() {
            "tool.start"
                if event.lifecycle.as_ref().is_some_and(|lifecycle| {
                    lifecycle.event_type == ActivityLifecycleType::MessageSend
                }) =>
            {
                state.correlations.entry(id).or_default().start = Some(event.clone());
            }
            "tool.finish" | "tool.error" => {
                if let Some(start_id) = event
                    .lifecycle
                    .as_ref()
                    .filter(|lifecycle| lifecycle.event_type == ActivityLifecycleType::MessageSend)
                    .and_then(|lifecycle| lifecycle.start_event_id)
                    && let Some(pair) = state.correlations.get_mut(&start_id)
                {
                    pair.completion = Some(event.clone());
                }
            }
            _ => {}
        }
        state.recent.push_back(event);

        let mut spill = false;
        let mut evicted_pending = Vec::new();
        let mut evicted_archives = Vec::new();
        if self.archive_dir.is_some() && state.recent.len() >= RECENT_THRESHOLD {
            let older: Vec<_> = state.recent.drain(..SPILL_BATCH).collect();
            let chunk = PendingChunk {
                generation: state.generation,
                first_id: older[0].id,
                last_id: older[SPILL_BATCH - 1].id,
                events: Arc::new(older),
            };
            state.pending.push_back(chunk);
            // Under a sustained burst or a blocked disk, drop the oldest
            // uncompressed chunks instead of growing without bound or holding
            // up tool calls. A worker finishing an evicted chunk cannot restore it.
            while state.pending.len() > MAX_PENDING_CHUNKS {
                if let Some(chunk) = state.pending.pop_front() {
                    evicted_pending.push(chunk);
                }
            }
            if !evicted_pending.is_empty() {
                // Every older archive precedes the discarded pending chunk.
                // Retain a contiguous newest suffix, never a prefix separated
                // from the current page by a missing middle chunk.
                evicted_archives.extend(state.archived.drain(..));
                state.archive_bytes = 0;
            }
            spill = true;
        } else {
            while state.recent.len() > self.capacity {
                state.recent.pop_front();
            }
        }
        evicted_archives.extend(state.trim_archives());
        drop(state);
        drop(evicted_archives);
        drop(evicted_pending);

        if spill {
            // Spawn lazily, once. The sole worker bounds compression concurrency.
            let worker = self.worker.get_or_init(|| {
                let shared = Arc::clone(&self.shared);
                let dir = self.archive_dir.as_ref()?.path().to_path_buf();
                match thread::Builder::new()
                    .name("activity-archive".to_owned())
                    .spawn(move || archive_worker(shared, dir))
                {
                    Ok(handle) => Some(handle),
                    Err(error) => {
                        eprintln!("activity archive worker unavailable: {error}");
                        None
                    }
                }
            });
            if worker.is_some() {
                self.shared.wake.notify_one();
            }
        }
        id
    }

    /// Only copy recent events. Polling and ordinary snapshots never open,
    /// decompress, or inspect the content of an archive file.
    pub fn snapshot(&self, limit: usize) -> Vec<ActivityEvent> {
        if !self.enabled {
            return Vec::new();
        }
        let state = lock_state(&self.shared);
        let count = limit.min(state.recent.len());
        state
            .recent
            .iter()
            .skip(state.recent.len() - count)
            .cloned()
            .collect()
    }

    /// An atomic recent-only snapshot with its linked but off-page endpoints.
    /// Neither the page nor its correlation lookup touches compressed chunks.
    pub fn snapshot_page(&self, limit: usize) -> ActivityPage {
        if !self.enabled {
            return ActivityPage {
                events: Vec::new(),
                correlated_events: Vec::new(),
                next_before_id: None,
                has_more: false,
            };
        }
        let state = lock_state(&self.shared);
        let limit = limit.clamp(1, MAX_PAGE_SIZE);
        let count = limit.min(state.recent.len());
        let events: Vec<_> = state
            .recent
            .iter()
            .skip(state.recent.len() - count)
            .cloned()
            .collect();
        let has_more = state.recent.len() > count;
        let correlated_events = state.correlated_events(&events, limit);
        ActivityPage {
            next_before_id: has_more.then(|| events[0].id),
            events,
            correlated_events,
            has_more,
        }
    }

    /// Fetch up to 200 events older than the exclusive ID cursor.
    /// Compressed chunks are opened only when newer RAM data is insufficient.
    pub fn page(&self, before_id: Option<u64>, limit: usize) -> ActivityPage {
        if !self.enabled {
            return ActivityPage {
                events: Vec::new(),
                correlated_events: Vec::new(),
                next_before_id: None,
                has_more: false,
            };
        }
        let limit = limit.clamp(1, MAX_PAGE_SIZE);
        let cutoff = before_id.unwrap_or(u64::MAX);
        let (generation, recent, pending, archived) = {
            let state = lock_state(&self.shared);
            (
                state.generation,
                state
                    .recent
                    .iter()
                    .rev()
                    .filter(|event| event.id < cutoff)
                    .take(limit + 1)
                    .cloned()
                    .collect::<Vec<_>>(),
                state
                    .pending
                    .iter()
                    .rev()
                    .map(|chunk| Arc::clone(&chunk.events))
                    .collect::<Vec<_>>(),
                state.archived.iter().rev().cloned().collect::<Vec<_>>(),
            )
        };

        let mut newest_first = recent;
        for chunk in pending {
            if newest_first.len() > limit {
                break;
            }
            for event in chunk.iter().rev() {
                if event.id < cutoff {
                    newest_first.push(event.clone());
                    if newest_first.len() > limit {
                        break;
                    }
                }
            }
        }
        for chunk in archived {
            if newest_first.len() > limit {
                break;
            }
            if chunk.first_id >= cutoff {
                continue;
            }
            match read_chunk(&chunk) {
                Ok(events) => {
                    for event in events.into_iter().rev() {
                        if event.id < cutoff {
                            newest_first.push(event);
                            if newest_first.len() > limit {
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    // Do not page into the older prefix beyond a corrupt/missing
                    // chunk: a page must contain a contiguous newest suffix.
                    eprintln!(
                        "activity archive read failed for IDs {}..={}: {error}",
                        chunk.first_id, chunk.last_id
                    );
                    break;
                }
            }
        }

        // A page that raced CLEAR must not surface the pre-clear generation.
        let state = lock_state(&self.shared);
        if state.generation != generation {
            return ActivityPage {
                events: Vec::new(),
                correlated_events: Vec::new(),
                next_before_id: None,
                has_more: false,
            };
        }

        let has_more = newest_first.len() > limit;
        newest_first.truncate(limit);
        let next_before_id = if has_more {
            newest_first.last().map(|event| event.id)
        } else {
            None
        };
        newest_first.reverse();
        let correlated_events = state.correlated_events(&newest_first, limit);
        ActivityPage {
            events: newest_first,
            correlated_events,
            next_before_id,
            has_more,
        }
    }

    /// Discard all active, pending, and archived history without reusing IDs.
    /// In-flight worker results carry a generation and cannot resurrect CLEAR.
    pub fn clear(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        let mut state = lock_state(&self.shared);
        state.generation = state.generation.wrapping_add(1);
        let count = state.retained_count();
        let recent = std::mem::take(&mut state.recent);
        let pending = std::mem::take(&mut state.pending);
        let archived = std::mem::take(&mut state.archived);
        state.correlations.clear();
        state.archive_bytes = 0;
        drop(state);
        drop((recent, pending, archived)); // includes unlinking archived files
        self.shared.wake.notify_all();
        count
    }
}

impl Drop for ActivityHub {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        {
            let mut state = lock_state(&self.shared);
            state.shutdown = true;
            state.generation = state.generation.wrapping_add(1);
        }
        self.shared.wake.notify_all();
        if let Some(Some(handle)) = self.worker.get_mut().map(Option::take) {
            let _ = handle.join();
        }
        // TempDir is dropped after the worker exits and removes any stragglers.
    }
}

fn archive_worker(shared: Arc<Shared>, dir: PathBuf) {
    loop {
        let chunk = {
            let mut state = lock_state(&shared);
            while state.pending.is_empty() && !state.shutdown {
                state = shared
                    .wake
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            if state.shutdown {
                return;
            }
            state
                .pending
                .front()
                .cloned()
                .expect("pending is not empty")
        };

        #[cfg(test)]
        let hook = shared
            .worker_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        #[cfg(test)]
        if let Some(hook) = &hook {
            hook.entered.wait();
            hook.resume.wait();
        }

        // All costly work happens without the activity mutex.
        let mut stored = match write_chunk(&dir, &chunk) {
            Ok(archive) => Some(archive),
            Err(error) => {
                eprintln!(
                    "activity archive write failed for IDs {}..={}: {error}",
                    chunk.first_id, chunk.last_id
                );
                None
            }
        };

        let mut state = lock_state(&shared);
        let mut retired_pending = None;
        let mut evicted_archives = Vec::new();
        if !state.shutdown
            && state.generation == chunk.generation
            && let Some(position) = state
                .pending
                .iter()
                .position(|pending| pending.first_id == chunk.first_id)
        {
            retired_pending = state.pending.remove(position);
            if let Some(archive) = stored.take() {
                state.archive_bytes += archive.file.bytes;
                state.archived.push_back(archive);
            } else {
                // A failed write creates a hole between the earlier archive
                // and newer RAM data; discard the earlier prefix too.
                evicted_archives.extend(state.archived.drain(..));
                state.archive_bytes = 0;
            }
            evicted_archives.extend(state.trim_archives());
        }
        drop(state);
        drop((stored, retired_pending, evicted_archives));
        shared.wake.notify_all();
        #[cfg(test)]
        if let Some(hook) = hook {
            hook.finished.wait();
        }
    }
}

/// Bounds both zstd output and the volume of decoded JSON from a single
/// chunk. A malformed or abnormally large activity payload cannot exhaust
/// temporary disk space or allocate an unbounded archive page.
struct BoundedWriter<W> {
    inner: W,
    remaining: u64,
}

impl<W> BoundedWriter<W> {
    fn new(inner: W, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            return Err(io::Error::other(
                "activity archive chunk byte limit exceeded",
            ));
        }
        let allowed = bytes.len().min(self.remaining as usize);
        let count = self.inner.write(&bytes[..allowed])?;
        self.remaining -= count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn write_chunk(dir: &Path, chunk: &PendingChunk) -> io::Result<ArchiveChunk> {
    let path = dir.join(format!(
        "chunk-{}-{}-{}.json.zst",
        chunk.generation, chunk.first_id, chunk.last_id
    ));
    let result = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let compressed = BoundedWriter::new(file, MAX_CHUNK_BYTES);
        let encoder = zstd::stream::write::Encoder::new(compressed, 1)?;
        let mut serialized = BoundedWriter::new(encoder, MAX_SERIALIZED_CHUNK_BYTES);
        serde_json::to_writer(&mut serialized, chunk.events.as_ref()).map_err(io::Error::other)?;
        drop(serialized.into_inner().finish()?);
        let bytes = fs::metadata(&path)?.len();
        Ok(ArchiveChunk {
            first_id: chunk.first_id,
            last_id: chunk.last_id,
            count: chunk.events.len(),
            file: Arc::new(ArchiveFile {
                path: path.clone(),
                bytes,
            }),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&path);
    }
    result
}

fn read_chunk(chunk: &ArchiveChunk) -> io::Result<Vec<ActivityEvent>> {
    let file = File::open(&chunk.file.path)?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    // The slice parser is faster than streaming serde_json::Value parsing.
    // This additional buffer is bounded to one chunk.
    let mut decoded = Vec::new();
    decoder
        .take(MAX_SERIALIZED_CHUNK_BYTES + 1)
        .read_to_end(&mut decoded)?;
    if decoded.len() as u64 > MAX_SERIALIZED_CHUNK_BYTES {
        return Err(io::Error::other(
            "activity archive decoded byte limit exceeded",
        ));
    }
    serde_json::from_slice(&decoded).map_err(io::Error::other)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::json;

    use super::*;

    #[test]
    fn disabled_sink_emits_no_history_and_allocates_no_archive() {
        let hub = ActivityHub::disabled();
        assert!(!hub.is_enabled());
        assert!(hub.archive_dir.is_none());
        assert!(hub.worker.get().is_none());

        for _ in 0..1_000 {
            assert_eq!(
                hub.emit(
                    "tool.start",
                    "info",
                    Some("session"),
                    vec![],
                    json!({"large":"value"})
                ),
                0
            );
        }
        assert!(hub.snapshot(200).is_empty());
        for before in [None, Some(1), Some(u64::MAX)] {
            let page = hub.page(before, 200);
            assert!(page.events.is_empty());
            assert_eq!(page.next_before_id, None);
            assert!(!page.has_more);
        }
        assert_eq!(hub.clear(), 0);
        assert_eq!(hub.emit("after-clear", "info", None, vec![], json!({})), 0);
        assert!(hub.worker.get().is_none());
        let state = lock_state(&hub.shared);
        assert_eq!(state.next_id, 0);
        assert!(state.recent.is_empty());
        assert!(state.pending.is_empty());
        assert!(state.archived.is_empty());
    }

    #[test]
    fn disabled_sink_skips_argument_conversion_and_enabled_history_is_unchanged() {
        struct UnexpectedStringConversion;
        impl From<UnexpectedStringConversion> for String {
            fn from(_: UnexpectedStringConversion) -> Self {
                panic!("disabled activity should never convert event labels")
            }
        }

        let disabled = ActivityHub::disabled();
        assert_eq!(
            disabled.emit(
                UnexpectedStringConversion,
                UnexpectedStringConversion,
                None,
                vec![],
                json!({})
            ),
            0
        );
        let enabled = ActivityHub::new(500);
        assert!(enabled.is_enabled());
        assert!(enabled.archive_dir.is_some());
        emit(&enabled, 500);
        await_archive(&enabled);
        assert_eq!(
            history_newest_first(&enabled),
            (1..=500).rev().collect::<Vec<_>>()
        );
        assert_eq!(
            enabled.emit("continued", "info", None, vec![], json!({})),
            501
        );
    }

    fn emit(hub: &ActivityHub, count: usize) {
        for n in 0..count {
            hub.emit("tool.start", "info", None, vec![], json!({"n":n}));
        }
    }

    fn await_archive(hub: &ActivityHub) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if lock_state(&hub.shared).pending.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "archive worker did not drain pending chunks"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn history_newest_first(hub: &ActivityHub) -> Vec<u64> {
        let mut ids = Vec::new();
        let mut before = None;
        loop {
            let page = hub.page(before, 200);
            assert!(page.events.len() <= 200);
            assert!(page.events.windows(2).all(|pair| pair[0].id < pair[1].id));
            ids.extend(page.events.iter().rev().map(|event| event.id));
            if !page.has_more {
                assert!(page.next_before_id.is_none());
                break;
            }
            let cursor = page.next_before_id.expect("older page cursor");
            assert_eq!(Some(cursor), page.events.first().map(|event| event.id));
            before = Some(cursor);
        }
        ids
    }

    #[test]
    fn small_capacity_preserves_fixed_size_history() {
        let hub = ActivityHub::new(10);
        emit(&hub, 20);
        assert_eq!(
            hub.snapshot(100)
                .iter()
                .map(|event| event.id)
                .collect::<Vec<_>>(),
            (11..=20).collect::<Vec<_>>()
        );
        assert!(hub.archive_dir.is_none());
        assert_eq!(hub.clear(), 10);
        assert!(hub.snapshot(100).is_empty());
        assert_eq!(hub.emit("again", "info", None, vec![], json!({})), 21);
    }

    #[test]
    fn spills_exactly_at_500_and_pages_across_ram_and_archive() {
        let hub = ActivityHub::new(500);
        emit(&hub, 499);
        assert_eq!(hub.snapshot(500).len(), 499);
        assert!(lock_state(&hub.shared).pending.is_empty());
        emit(&hub, 1);
        let snapshot = hub.snapshot(500);
        assert_eq!(snapshot.len(), 250);
        assert_eq!(snapshot[0].id, 251);
        assert_eq!(snapshot.last().unwrap().id, 500);

        // While compression is pending/in-flight, pages can still see those IDs.
        assert_eq!(
            history_newest_first(&hub),
            (1..=500).rev().collect::<Vec<_>>()
        );
        await_archive(&hub);
        let page = hub.page(None, 200);
        assert!(page.has_more);
        assert_eq!(page.next_before_id, Some(301));
        assert_eq!(page.events.first().unwrap().id, 301);
        assert_eq!(page.events.last().unwrap().id, 500);
        let page = hub.page(Some(301), 200);
        assert_eq!(page.events.first().unwrap().id, 101);
        assert_eq!(page.events.last().unwrap().id, 300);
        assert_eq!(
            history_newest_first(&hub),
            (1..=500).rev().collect::<Vec<_>>()
        );
    }

    #[test]
    fn rotates_8_chunks_and_enforces_retention() {
        let hub = ActivityHub::new(500);
        // Allow the archive worker to finish between spills. Retention tests
        // must not conflate an intentionally bounded overload queue with disk rotation.
        for i in 1..=2_500 {
            hub.emit("tool.start", "info", None, vec![], json!({"i":i}));
            if i % 250 == 0 {
                await_archive(&hub);
            }
        }
        await_archive(&hub);
        let state = lock_state(&hub.shared);
        assert_eq!(state.archived.len(), 8);
        assert_eq!(state.archived.front().unwrap().first_id, 251);
        assert_eq!(state.archived.back().unwrap().last_id, 2_250);
        assert_eq!(state.recent.len(), 250);
        assert!(state.archive_bytes <= MAX_ARCHIVE_BYTES);
        drop(state);
        assert_eq!(
            history_newest_first(&hub),
            (251..=2_500).rev().collect::<Vec<_>>()
        );
        assert_eq!(hub.page(None, usize::MAX).events.len(), 200);
        assert_eq!(hub.page(None, 0).events.len(), 1);
    }

    #[test]
    fn concurrent_emitters_generate_unique_ordered_ids() {
        let hub = Arc::new(ActivityHub::new(500));
        let threads: Vec<_> = (0..6)
            .map(|_| {
                let hub = Arc::clone(&hub);
                thread::spawn(move || {
                    (0..200)
                        .map(|n| hub.emit("event", "info", None, vec![], json!({"n":n})))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut returned = Vec::new();
        for task in threads {
            returned.extend(task.join().unwrap());
        }
        returned.sort_unstable();
        assert_eq!(returned, (1..=1_200).collect::<Vec<_>>());
        await_archive(&hub);
        let retained = history_newest_first(&hub);
        assert_eq!(retained, (1..=1_200).rev().collect::<Vec<_>>());
    }

    #[test]
    fn unpaced_burst_sheds_oldest_history_without_holes() {
        let hub = ActivityHub::new(500);
        emit(&hub, 3_500);
        await_archive(&hub);
        let ids = history_newest_first(&hub);
        assert!(!ids.is_empty());
        assert_eq!(ids[0], 3_500);
        assert!(
            ids.windows(2).all(|pair| pair[0] == pair[1] + 1),
            "saturated compression must leave a contiguous newest suffix"
        );
        assert!(ids.len() <= MAX_RETAINED_EVENTS);
        let state = lock_state(&hub.shared);
        assert!(state.pending.len() <= MAX_PENDING_CHUNKS);
        assert!(state.archived.len() <= MAX_ARCHIVE_CHUNKS);
        assert!(state.archive_bytes <= MAX_ARCHIVE_BYTES);
        drop(state);
        assert_eq!(hub.clear(), ids.len());
    }

    #[test]
    fn clear_invalidates_pending_and_inflight_chunks_without_reusing_ids() {
        let hub = ActivityHub::new(500);
        let hook = Arc::new(WorkerTestHook {
            entered: Barrier::new(2),
            resume: Barrier::new(2),
            finished: Barrier::new(2),
        });
        *hub.shared.worker_hook.lock().unwrap() = Some(Arc::clone(&hook));
        emit(&hub, 500);
        hook.entered.wait(); // worker has claimed the pending chunk
        let cleared = hub.clear();
        hook.resume.wait(); // let the obsolete compression finish
        hook.finished.wait(); // stale output is rejected and unlinked
        *hub.shared.worker_hook.lock().unwrap() = None;
        assert_eq!(cleared, 500);
        assert_eq!(
            fs::read_dir(hub.archive_dir.as_ref().unwrap().path())
                .unwrap()
                .count(),
            0
        );
        assert!(hub.snapshot(200).is_empty());
        assert!(hub.page(None, 200).events.is_empty());
        assert_eq!(
            hub.emit("after-clear", "info", None, vec![], json!({})),
            501
        );
        await_archive(&hub);
        assert_eq!(history_newest_first(&hub), vec![501]);
        assert_eq!(hub.clear(), 1);
        assert_eq!(hub.clear(), 0);
        assert_eq!(hub.emit("again", "info", None, vec![], json!({})), 502);
    }

    #[test]
    fn bounded_writer_rejects_oversized_serialized_or_compressed_chunks() {
        let mut output = Vec::new();
        {
            let mut bounded = BoundedWriter::new(&mut output, 3);
            assert!(bounded.write_all(b"abcdef").is_err());
        }
        assert_eq!(output, b"abc");
    }

    #[test]
    fn disk_failure_drops_pending_chunks_and_keeps_emitting() {
        let hub = ActivityHub::new(500);
        emit(&hub, 500);
        await_archive(&hub);
        assert_eq!(lock_state(&hub.shared).archived.len(), 1);
        // Simulate a temp-directory deletion, permissions, or exhausted disk.
        // The only worker's writes now fail without crashing or blocking emit.
        let dir = hub.archive_dir.as_ref().unwrap().path().to_path_buf();
        fs::remove_dir_all(&dir).unwrap();
        emit(&hub, 4_000);
        await_archive(&hub);
        let state = lock_state(&hub.shared);
        assert!(state.pending.len() <= MAX_PENDING_CHUNKS);
        assert!(state.archived.is_empty());
        assert_eq!(state.archive_bytes, 0);
        drop(state);
        assert!(hub.snapshot(200).len() <= 200);
        assert!(hub.page(None, 200).events.len() <= 200);
        let ids = history_newest_first(&hub);
        assert!(ids.windows(2).all(|pair| pair[0] == pair[1] + 1));
        assert_eq!(
            hub.emit("after-disk-failure", "info", None, vec![], json!({})),
            4_501
        );
    }

    #[test]
    fn temp_files_are_removed_on_clear_and_drop() {
        let hub = ActivityHub::new(500);
        let dir = hub.archive_dir.as_ref().unwrap().path().to_path_buf();
        emit(&hub, 500);
        await_archive(&hub);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        assert_eq!(hub.clear(), 500);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        emit(&hub, 500);
        await_archive(&hub);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        drop(hub);
        assert!(!dir.exists());
    }

    #[test]
    fn corrupted_archive_does_not_break_snapshot_or_page() {
        let hub = ActivityHub::new(500);
        emit(&hub, 500);
        await_archive(&hub);
        let file = lock_state(&hub.shared).archived[0].file.path.clone();
        fs::write(&file, b"not a zstd frame").unwrap();
        let snapshot = hub.snapshot(200);
        assert_eq!(snapshot.first().unwrap().id, 301);
        assert_eq!(snapshot.last().unwrap().id, 500);
        let page = hub.page(Some(251), 200);
        assert!(page.events.is_empty());
        assert!(!page.has_more);
    }

    #[test]
    fn disk_budget_discards_oldest_archives() {
        let hub = ActivityHub::new(500);
        emit(&hub, 500);
        await_archive(&hub);
        let mut state = lock_state(&hub.shared);
        assert_eq!(state.archived.len(), 1);
        let bytes = state.archived[0].file.bytes;
        state.archive_bytes = MAX_ARCHIVE_BYTES + 1;
        let evicted = state.trim_archives();
        assert_eq!(evicted.len(), 1);
        assert!(state.archived.is_empty());
        assert!(state.archive_bytes <= MAX_ARCHIVE_BYTES);
        drop(state);
        drop(evicted);
        assert!(bytes > 0);
    }

    #[test]
    fn typed_message_lifecycle_preserves_legacy_fields_and_is_self_contained() {
        let hub = ActivityHub::new(3);
        let args: PoolSendArgs = serde_json::from_value(json!({
            "pool":"team", "target":"Bob", "message":"hello", "in_reply_to":"m0"
        }))
        .unwrap();
        let message_context = ActivityMessage::from_pool_send_args(&args).unwrap();
        let start = hub.emit(
            "tool.start",
            "info",
            Some("session"),
            vec![],
            json!({"tool":"pool_send", "arguments":args}),
        );
        let begin = hub.snapshot(1).pop().unwrap();
        assert_eq!(begin.schema_version, 1);
        assert_eq!(begin.detail["arguments"]["message"], "hello");
        assert_eq!(
            begin.lifecycle.as_ref().unwrap().event_type,
            ActivityLifecycleType::MessageSend
        );
        assert_eq!(
            begin
                .lifecycle
                .as_ref()
                .unwrap()
                .message
                .as_ref()
                .unwrap()
                .status,
            Some(ActivityMessageStatus::Sending)
        );

        // Evict the start: finish must still contain its own complete message.
        for _ in 0..3 {
            hub.emit("noise", "info", None, vec![], json!({}));
        }
        let finish = hub.emit_with_context(
            "tool.finish",
            "info",
            Some("session"),
            vec![],
            json!({"tool":"pool_send", "start_event_id": start,
                "result":{"value":{"assigned_agent":"Alice", "message_id":"m1",
                    "recipients":["Bob"]}, "delivery_receipts":[{"pool":"team", "to":"Bob"}]}}),
            ActivityContext {
                pool: Some("team".to_owned()),
                message: Some(message_context),
            },
        );
        let event = hub.snapshot(1).pop().unwrap();
        assert_eq!(event.id, finish);
        assert_eq!(event.kind, "tool.finish");
        assert_eq!(event.detail["start_event_id"], start);
        let lifecycle = event.lifecycle.unwrap();
        assert_eq!(lifecycle.event_type, ActivityLifecycleType::MessageSend);
        assert_eq!(lifecycle.phase, ActivityPhase::Finish);
        assert_eq!(lifecycle.start_event_id, Some(start));
        assert_eq!(lifecycle.delivery_receipts.len(), 1);
        let message = lifecycle.message.unwrap();
        assert_eq!(message.status, Some(ActivityMessageStatus::Queued));
        assert_eq!(message.pool.as_deref(), Some("team"));
        assert_eq!(message.target.as_deref(), Some("Bob"));
        assert_eq!(message.body.as_deref(), Some("hello"));
        assert_eq!(message.in_reply_to.as_deref(), Some("m0"));
        assert_eq!(message.from.as_deref(), Some("Alice"));
        assert_eq!(message.message_id.as_deref(), Some("m1"));
        assert_eq!(message.recipients, Some(vec!["Bob".to_owned()]));
        // A completion referring to a non-retained start must not resurrect it.
        assert!(hub.snapshot_page(3).correlated_events.is_empty());
    }

    #[test]
    fn typed_message_error_and_tool_receives_use_explicit_lifecycle() {
        let hub = ActivityHub::new(12);
        let start = hub.emit("tool.start", "info", None, vec![],
            json!({"tool":"pool_send", "arguments":{"pool":"team", "target":"Bob", "message":"hi"}}));
        hub.emit(
            "tool.error",
            "error",
            None,
            vec![],
            json!({"tool":"pool_send", "start_event_id":start, "result":{"error":"not allowed"}}),
        );
        let failed = hub.snapshot(1).pop().unwrap();
        let lifecycle = failed.lifecycle.unwrap();
        assert_eq!(lifecycle.phase, ActivityPhase::Error);
        let message = lifecycle.message.unwrap();
        assert_eq!(message.status, Some(ActivityMessageStatus::Failed));
        assert_eq!(message.error.as_deref(), Some("not allowed"));
        assert_eq!(message.body.as_deref(), Some("hi"));

        hub.emit("tool.finish", "info", None, vec![],
            json!({"tool":"pool_wait", "start_event_id":123,
                "result":{"messages":[{"message_id":"m2", "pool":"team", "from":"Bob", "to":"Alice"}],
                    "delivery_receipts":[{"message_id":"m1", "pool":"team", "to":"Bob"}]}}));
        let lifecycle = hub.snapshot(1).pop().unwrap().lifecycle.unwrap();
        assert_eq!(lifecycle.event_type, ActivityLifecycleType::Tool);
        assert_eq!(lifecycle.peer_messages[0]["message_id"], "m2");
        assert_eq!(lifecycle.delivery_receipts[0]["message_id"], "m1");

        hub.emit(
            "tool.start",
            "info",
            None,
            vec![],
            json!({"tool":"pool_send", "arguments":{"pool":"team", "action":"leave"}}),
        );
        assert_eq!(
            hub.snapshot(1)[0].lifecycle.as_ref().unwrap().event_type,
            ActivityLifecycleType::Tool
        );
    }

    #[test]
    fn legacy_activity_fixture_deserializes_and_retains_untyped_fields() {
        let legacy = json!({"id":3, "timestamp_ms":42, "kind":"tool.start", "level":"info",
            "session":"old", "agents":[], "detail":{"tool":"pool_send", "arguments":{"message":"hi"}}});
        let parsed: ActivityEvent = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(parsed.schema_version, 0);
        assert!(parsed.lifecycle.is_none());
        assert_eq!(parsed.kind, "tool.start");
        assert_eq!(parsed.detail, legacy["detail"]);
    }

    #[test]
    fn cross_page_correlation_is_bounded_deduplicated_and_cleared() {
        let hub = ActivityHub::new(10);
        let start = hub.emit(
            "tool.start",
            "info",
            None,
            vec![],
            json!({"tool":"pool_send", "arguments":{"pool":"team", "message":"a"}}),
        );
        for _ in 0..8 {
            hub.emit("unrelated", "info", None, vec![], json!({}));
        }
        let finish = hub.emit("tool.finish", "info", None, vec![],
            json!({"tool":"pool_send", "start_event_id": start, "result":{"value":{"recipients":[]}}}));
        let latest = hub.snapshot_page(4);
        assert_eq!(latest.events.len(), 4);
        assert_eq!(
            latest
                .correlated_events
                .iter()
                .map(|e| e.id)
                .collect::<Vec<_>>(),
            vec![start]
        );
        let older = hub.page(Some(3), 2);
        assert_eq!(
            older.events.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            older
                .correlated_events
                .iter()
                .map(|e| e.id)
                .collect::<Vec<_>>(),
            vec![finish]
        );
        let full = hub.snapshot_page(10);
        assert!(
            full.correlated_events.is_empty(),
            "same-page counterparts must not be repeated"
        );
        assert_eq!(hub.clear(), 10);
        assert!(hub.snapshot_page(10).correlated_events.is_empty());
        assert!(hub.page(None, 10).correlated_events.is_empty());
        assert!(lock_state(&hub.shared).correlations.is_empty());
        assert_eq!(hub.emit("new", "info", None, vec![], json!({})), finish + 1);
    }

    #[test]
    fn archived_counterparts_are_available_without_snapshot_disk_reads() {
        let hub = ActivityHub::new(500);
        let start = hub.emit(
            "tool.start",
            "info",
            None,
            vec![],
            json!({"tool":"pool_send", "arguments":{"pool":"team", "message":"a"}}),
        );
        for _ in 0..498 {
            hub.emit("unrelated", "info", None, vec![], json!({}));
        }
        let finish = hub.emit("tool.finish", "info", None, vec![],
            json!({"tool":"pool_send", "start_event_id":start, "result":{"value":{"recipients":[]}}}));
        await_archive(&hub);
        assert_eq!(
            hub.snapshot_page(200)
                .correlated_events
                .iter()
                .map(|e| e.id)
                .collect::<Vec<_>>(),
            vec![start]
        );
        // Compressed start is readable by the history route, and it can link
        // forward into the in-memory finish without paging in the browser.
        assert_eq!(
            hub.page(Some(2), 1)
                .correlated_events
                .iter()
                .map(|e| e.id)
                .collect::<Vec<_>>(),
            vec![finish]
        );

        // Advance beyond the retained suffix: the old start cannot reappear.
        for i in 0..3500 {
            hub.emit("unrelated", "info", None, vec![], json!({"i":i}));
        }
        await_archive(&hub);
        assert!(lock_state(&hub.shared).correlations.len() <= MAX_RETAINED_EVENTS);
        assert!(hub.snapshot_page(200).correlated_events.is_empty());
    }
}

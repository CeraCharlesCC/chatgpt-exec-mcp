//! Activity history ownership: ID allocation, retention, correlations, and paging.
//!
//! A single mutex protects history and generation changes. Archive I/O uses
//! pinned immutable chunks outside the mutex; CLEAR invalidates old readers
//! and the archive worker through a generation check.

use std::collections::{BTreeMap, HashSet, VecDeque};
#[cfg(test)]
use std::fs;
use std::io;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Barrier;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};

use serde::Serialize;
use serde_json::Value;
use tempfile::TempDir;

use crate::agent_pool::AgentIdentity;
#[cfg(test)]
use crate::agent_pool::PoolSendArgs;

use super::archive::{
    ArchiveChunk, MAX_ARCHIVE_BYTES, MAX_ARCHIVE_CHUNKS, PendingChunk, read_chunk, write_chunk,
};
use super::events::{
    ActivityContext, ActivityEmissionContext, ActivityEvent, ActivityLifecycleType,
    lifecycle_for_event,
};
#[cfg(test)]
use super::events::{ActivityMessage, ActivityMessageStatus, ActivityPhase};

const RECENT_THRESHOLD: usize = 500;
const SPILL_BATCH: usize = 250;
const MAX_PENDING_CHUNKS: usize = 4;
const MAX_RETAINED_EVENTS: usize = 2_500;
const MAX_PAGE_SIZE: usize = 200;

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
        self.emit_with_context_and_communications(
            kind,
            level,
            session,
            agents,
            detail,
            ActivityEmissionContext {
                activity: context,
                communications: None,
            },
        )
    }

    /// Prefer typed communications from the operation over inspecting its
    /// presentation JSON. The legacy entry point remains available for
    /// callers that only have an activity payload.
    pub(crate) fn emit_with_context_and_communications(
        &self,
        kind: impl Into<String>,
        level: impl Into<String>,
        session: Option<&str>,
        agents: Vec<AgentIdentity>,
        detail: Value,
        emission: ActivityEmissionContext<'_>,
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
        let retained_start = detail
            .get("start_event_id")
            .and_then(Value::as_u64)
            .and_then(|id| state.correlations.get(&id))
            .and_then(|pair| pair.start.as_ref());
        let lifecycle = lifecycle_for_event(
            &kind,
            &detail,
            &agents,
            retained_start,
            emission.activity,
            emission.communications,
        );
        let event = ActivityEvent::new(id, kind, level.into(), session, agents, detail, lifecycle);
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

    /// Only copy recent events, without opening or decompressing archive files.
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

    /// Copy all retained events in ascending ID order, including compressed
    /// history. Call from a blocking worker: archive I/O runs outside the mutex.
    /// Fail rather than silently returning an incomplete export on archive errors.
    pub fn snapshot_all(&self) -> io::Result<Vec<ActivityEvent>> {
        let (generation, recent, pending, archived) = {
            let state = lock_state(&self.shared);
            (
                state.generation,
                state.recent.iter().cloned().collect::<Vec<_>>(),
                state
                    .pending
                    .iter()
                    .map(|chunk| Arc::clone(&chunk.events))
                    .collect::<Vec<_>>(),
                state.archived.iter().cloned().collect::<Vec<_>>(),
            )
        };
        // Pin both pending buffers and immutable archive files from one instant;
        // compression, retention, and new emissions cannot introduce gaps or duplicates.
        let result = (|| {
            let mut events = Vec::new();
            for chunk in archived {
                events.extend(read_chunk(&chunk)?);
            }
            for chunk in pending {
                events.extend(chunk.iter().cloned());
            }
            events.extend(recent);
            Ok(events)
        })();
        // Like history pages, a snapshot racing CLEAR must discard the old generation.
        if lock_state(&self.shared).generation != generation {
            return Ok(Vec::new());
        }
        result
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

#[cfg(test)]
mod tests;

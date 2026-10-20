//! In-memory recent activity with a bounded, temporary compressed history.
//!
//! The dashboard's normal snapshots touch only the recent queue. Older pages
//! read zstd chunks on demand; serialization and file I/O run on one background
//! worker and never hold the activity mutex.

use std::collections::VecDeque;
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

use crate::agent_pool::AgentIdentity;

const RECENT_THRESHOLD: usize = 500;
const SPILL_BATCH: usize = 250;
const MAX_ARCHIVE_CHUNKS: usize = 8;
const MAX_PENDING_CHUNKS: usize = 4;
const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CHUNK_BYTES: u64 = MAX_ARCHIVE_BYTES / MAX_ARCHIVE_CHUNKS as u64;
const MAX_SERIALIZED_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RETAINED_EVENTS: usize = 2_500;
const MAX_PAGE_SIZE: usize = 200;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityEvent {
    pub id: u64,
    pub timestamp_ms: i64,
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
struct ActivityState {
    next_id: u64,
    generation: u64,
    recent: VecDeque<ActivityEvent>,
    pending: VecDeque<PendingChunk>,
    archived: VecDeque<ArchiveChunk>,
    archive_bytes: u64,
    shutdown: bool,
}

impl ActivityState {
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
    shared: Arc<Shared>,
    capacity: usize,
    archive_dir: Option<TempDir>,
    worker: OnceLock<Option<JoinHandle<()>>>,
}

impl ActivityHub {
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
        let mut state = lock_state(&self.shared);
        // Allocation and insertion share the same lock so concurrent emitters
        // cannot put event IDs in a different order from their insertion order.
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("activity event ID overflow");
        let id = state.next_id;
        state.recent.push_back(ActivityEvent {
            id,
            timestamp_ms: now_ms(),
            kind: kind.into(),
            level: level.into(),
            session: session.map(str::to_owned),
            agents,
            detail,
        });

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
        let state = lock_state(&self.shared);
        let count = limit.min(state.recent.len());
        state
            .recent
            .iter()
            .skip(state.recent.len() - count)
            .cloned()
            .collect()
    }

    /// Fetch up to 200 events older than the exclusive ID cursor.
    /// Compressed chunks are opened only when newer RAM data is insufficient.
    pub fn page(&self, before_id: Option<u64>, limit: usize) -> ActivityPage {
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
        if lock_state(&self.shared).generation != generation {
            return ActivityPage {
                events: Vec::new(),
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
        ActivityPage {
            events: newest_first,
            next_before_id,
            has_more,
        }
    }

    /// Discard all active, pending, and archived history without reusing IDs.
    /// In-flight worker results carry a generation and cannot resurrect CLEAR.
    pub fn clear(&self) -> usize {
        let mut state = lock_state(&self.shared);
        state.generation = state.generation.wrapping_add(1);
        let count = state.retained_count();
        let recent = std::mem::take(&mut state.recent);
        let pending = std::mem::take(&mut state.pending);
        let archived = std::mem::take(&mut state.archived);
        state.archive_bytes = 0;
        drop(state);
        drop((recent, pending, archived)); // includes unlinking archived files
        self.shared.wake.notify_all();
        count
    }
}

impl Drop for ActivityHub {
    fn drop(&mut self) {
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
}

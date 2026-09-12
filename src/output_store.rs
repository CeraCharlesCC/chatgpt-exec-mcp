use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const META_VERSION: u32 = 1;
const INSTANCE_LOCK_FILE: &str = ".instance.lock";
const META_FILE: &str = "meta.json";
const RAW_FILE: &str = "raw.log";
const FILE_HEADROOM: usize = 8;

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub start: u64,
    pub end: u64,
    pub stored_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Projection {
    pub snapshot: Snapshot,
    pub output: String,
    pub truncated: bool,
    pub encoding_loss: bool,
}

#[derive(Clone, Debug)]
pub struct OutputRefInfo {
    pub path: PathBuf,
    pub stored_bytes: u64,
    pub capture_status: &'static str,
    pub expires_at_unix_seconds: Option<u64>,
    pub incomplete_reason: Option<String>,
}

#[derive(Clone)]
pub struct OutputStoreManager {
    inner: Arc<OutputStoreManagerInner>,
}

struct OutputStoreManagerInner {
    root: PathBuf,
    instance_id: String,
    retention: Duration,
    min_retention: Duration,
    max_bytes: u64,
    headroom_bytes: u64,
    max_files: usize,
    used_bytes: AtomicU64,
    used_files: AtomicUsize,
    _instance_lock: File,
}

pub struct OutputStore {
    manager: OutputStoreManager,
    dir: PathBuf,
    file: Option<File>,
    meta: ArtifactMeta,
    headroom_admitted: bool,
    deleted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArtifactMeta {
    version: u32,
    artifact_id: String,
    owner_instance: String,
    state: ArtifactState,
    created_at_unix_millis: u64,
    sealed_at_unix_millis: Option<u64>,
    committed_bytes: u64,
    retention_sticky: bool,
    retention_reason: Option<String>,
    retain_until_unix_millis: Option<u64>,
    expires_at_unix_millis: Option<u64>,
    incomplete_reason: Option<String>,
    recovery_path_published: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactState {
    Active,
    Complete,
    Incomplete,
}

#[derive(Debug)]
struct GcCandidate {
    dir: PathBuf,
    bytes: u64,
    sealed_at: u64,
    retain_until: u64,
    expires_at: u64,
}

impl OutputStoreManager {
    pub fn open(
        root: &Path,
        retention: Duration,
        min_retention: Duration,
        max_bytes: u64,
        headroom_bytes: u64,
        max_files: usize,
    ) -> io::Result<Self> {
        if retention < min_retention {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output retention must be at least the minimum retention",
            ));
        }
        if max_bytes == 0 || max_files == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output store capacity limits must be greater than zero",
            ));
        }

        fs::create_dir_all(root)?;
        set_directory_mode(root)?;
        let root = fs::canonicalize(root)?;
        let lock_path = root.join(INSTANCE_LOCK_FILE);
        let instance_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        set_file_mode(&lock_path)?;
        lock_instance(&instance_lock)?;

        let manager = Self {
            inner: Arc::new(OutputStoreManagerInner {
                root,
                instance_id: unique_id(),
                retention,
                min_retention,
                max_bytes,
                headroom_bytes,
                max_files,
                used_bytes: AtomicU64::new(0),
                used_files: AtomicUsize::new(0),
                _instance_lock: instance_lock,
            }),
        };
        manager.reconcile_startup()?;
        manager.gc()?;
        Ok(manager)
    }

    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    pub fn create_artifact(&self) -> io::Result<OutputStore> {
        self.gc()?;
        let used_bytes = self.inner.used_bytes.load(Ordering::SeqCst);
        let hard_bytes = self
            .inner
            .max_bytes
            .saturating_add(self.inner.headroom_bytes);
        if used_bytes >= hard_bytes {
            return Err(capacity_error(format!(
                "output store byte capacity exhausted ({} bytes plus {} bytes headroom)",
                self.inner.max_bytes, self.inner.headroom_bytes
            )));
        }
        let headroom_admitted = used_bytes >= self.inner.max_bytes;
        let hard_files = self.inner.max_files.saturating_add(FILE_HEADROOM);
        if self.inner.used_files.load(Ordering::SeqCst) >= hard_files {
            return Err(capacity_error(format!(
                "output store file capacity exhausted ({} files plus {} file headroom)",
                self.inner.max_files, FILE_HEADROOM
            )));
        }

        let (artifact_id, dir) = loop {
            let artifact_id = unique_id();
            let dir = self.inner.root.join(&artifact_id);
            match fs::create_dir(&dir) {
                Ok(()) => break (artifact_id, dir),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        if let Err(error) = set_directory_mode(&dir) {
            let _ = fs::remove_dir(&dir);
            return Err(error);
        }

        let raw_path = dir.join(RAW_FILE);
        let file = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&raw_path)
        {
            Ok(file) => file,
            Err(error) => {
                let _ = fs::remove_dir(&dir);
                return Err(error);
            }
        };
        if let Err(error) = set_file_mode(&raw_path) {
            drop(file);
            let _ = fs::remove_file(&raw_path);
            let _ = fs::remove_dir(&dir);
            return Err(error);
        }

        let meta = ArtifactMeta {
            version: META_VERSION,
            artifact_id,
            owner_instance: self.inner.instance_id.clone(),
            state: ArtifactState::Active,
            created_at_unix_millis: unix_millis(),
            sealed_at_unix_millis: None,
            committed_bytes: 0,
            retention_sticky: false,
            retention_reason: None,
            retain_until_unix_millis: None,
            expires_at_unix_millis: None,
            incomplete_reason: None,
            recovery_path_published: false,
        };
        if let Err(error) = write_meta(&dir, &meta) {
            drop(file);
            let _ = fs::remove_file(&raw_path);
            let _ = fs::remove_dir(&dir);
            return Err(error);
        }

        self.inner.used_files.fetch_add(1, Ordering::SeqCst);
        Ok(OutputStore {
            manager: self.clone(),
            dir,
            file: Some(file),
            meta,
            headroom_admitted,
            deleted: false,
        })
    }

    pub fn gc(&self) -> io::Result<()> {
        let now = unix_millis();
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&self.inner.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !is_managed_artifact_id(&name) {
                continue;
            }
            let dir = entry.path();
            let dir_meta = fs::symlink_metadata(&dir)?;
            if !dir_meta.file_type().is_dir() || dir_meta.file_type().is_symlink() {
                continue;
            }
            let Ok(meta) = read_meta(&dir) else {
                continue;
            };
            if meta.state == ArtifactState::Active || !meta.retention_sticky {
                continue;
            }
            let bytes = regular_file_len(&dir.join(RAW_FILE)).unwrap_or(0);
            candidates.push(GcCandidate {
                dir,
                bytes,
                sealed_at: meta
                    .sealed_at_unix_millis
                    .unwrap_or(meta.created_at_unix_millis),
                retain_until: meta.retain_until_unix_millis.unwrap_or(u64::MAX),
                expires_at: meta.expires_at_unix_millis.unwrap_or(u64::MAX),
            });
        }

        candidates.sort_by_key(|candidate| candidate.sealed_at);
        let mut deleted = vec![false; candidates.len()];
        for (index, candidate) in candidates.iter().enumerate() {
            if candidate.expires_at <= now && candidate.retain_until <= now {
                // A corrupt or concurrently modified artifact should remain
                // accounted for without making GC fail for every other artifact.
                deleted[index] = self.delete_candidate(candidate).is_ok();
            }
        }

        for (index, candidate) in candidates.iter().enumerate() {
            if deleted[index] {
                continue;
            }
            let over_bytes = self.inner.used_bytes.load(Ordering::SeqCst) > self.inner.max_bytes;
            let over_files = self.inner.used_files.load(Ordering::SeqCst) > self.inner.max_files;
            if !over_bytes && !over_files {
                break;
            }
            if candidate.retain_until <= now {
                let _ = self.delete_candidate(candidate);
            }
        }
        Ok(())
    }

    fn reconcile_startup(&self) -> io::Result<()> {
        let now = unix_millis();
        let mut total_bytes = 0u64;
        let mut total_files = 0usize;
        for entry in fs::read_dir(&self.inner.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !is_managed_artifact_id(&name) {
                continue;
            }
            let dir = entry.path();
            let dir_meta = fs::symlink_metadata(&dir)?;
            if !dir_meta.file_type().is_dir() || dir_meta.file_type().is_symlink() {
                continue;
            }
            if let Some(bytes) = regular_file_len(&dir.join(RAW_FILE)) {
                total_bytes = total_bytes.saturating_add(bytes);
                total_files = total_files.saturating_add(1);
                if let Ok(mut meta) = read_meta(&dir) {
                    meta.committed_bytes = bytes;
                    let was_active = meta.state == ArtifactState::Active;
                    let needs_restart_retention = meta.state == ArtifactState::Active
                        || (meta.state == ArtifactState::Complete && !meta.retention_sticky);
                    if needs_restart_retention {
                        meta.state = ArtifactState::Incomplete;
                        meta.retention_sticky = true;
                        meta.retention_reason = Some(if was_active {
                            "server_restart_orphan".into()
                        } else {
                            "restart_unconfirmed_delivery".into()
                        });
                        meta.incomplete_reason = meta.retention_reason.clone();
                        self.apply_retention_deadlines(&mut meta, now);
                    }
                    let _ = write_meta(&dir, &meta);
                } else {
                    let mut meta = ArtifactMeta {
                        version: META_VERSION,
                        artifact_id: name.into_owned(),
                        owner_instance: "unknown".into(),
                        state: ArtifactState::Incomplete,
                        created_at_unix_millis: now,
                        sealed_at_unix_millis: None,
                        committed_bytes: bytes,
                        retention_sticky: true,
                        retention_reason: Some("restart_missing_or_invalid_metadata".into()),
                        retain_until_unix_millis: None,
                        expires_at_unix_millis: None,
                        incomplete_reason: Some("restart_missing_or_invalid_metadata".into()),
                        recovery_path_published: false,
                    };
                    self.apply_retention_deadlines(&mut meta, now);
                    let _ = write_meta(&dir, &meta);
                }
            }
        }
        self.inner.used_bytes.store(total_bytes, Ordering::SeqCst);
        self.inner.used_files.store(total_files, Ordering::SeqCst);
        Ok(())
    }

    fn apply_retention_deadlines(&self, meta: &mut ArtifactMeta, sealed_at: u64) {
        meta.sealed_at_unix_millis = Some(sealed_at);
        meta.retain_until_unix_millis =
            Some(sealed_at.saturating_add(duration_millis(self.inner.min_retention)));
        meta.expires_at_unix_millis =
            Some(sealed_at.saturating_add(duration_millis(self.inner.retention)));
    }

    fn reserve_bytes(&self, bytes: u64, allow_headroom: bool) -> io::Result<()> {
        let hard_limit = if allow_headroom {
            self.inner
                .max_bytes
                .saturating_add(self.inner.headroom_bytes)
        } else {
            self.inner.max_bytes
        };
        loop {
            let current = self.inner.used_bytes.load(Ordering::SeqCst);
            let next = current.saturating_add(bytes);
            if next > hard_limit {
                return Err(capacity_error(format!(
                    "output store capacity exceeded: attempted {next} bytes with hard limit {hard_limit}"
                )));
            }
            if self
                .inner
                .used_bytes
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release_bytes(&self, bytes: u64) {
        atomic_saturating_sub_u64(&self.inner.used_bytes, bytes);
    }

    fn delete_candidate(&self, candidate: &GcCandidate) -> io::Result<()> {
        remove_managed_artifact(&candidate.dir)?;
        self.release_bytes(candidate.bytes);
        atomic_saturating_sub_usize(&self.inner.used_files, 1);
        Ok(())
    }
}

impl OutputStore {
    pub fn append(&mut self, bytes: &[u8]) -> io::Result<u64> {
        if self.deleted || self.meta.state != ArtifactState::Active {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "output artifact is no longer writable",
            ));
        }
        if bytes.is_empty() {
            return Ok(self.meta.committed_bytes);
        }
        self.manager
            .reserve_bytes(bytes.len() as u64, self.headroom_admitted)?;
        let before = self.meta.committed_bytes;
        let file = self.file.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "output artifact file is closed")
        })?;
        file.seek(SeekFrom::End(0))?;
        let result = file.write_all(bytes).and_then(|_| file.flush());
        match result {
            Ok(()) => {
                self.meta.committed_bytes = before.saturating_add(bytes.len() as u64);
                Ok(self.meta.committed_bytes)
            }
            Err(error) => {
                let actual_len = file.metadata().map(|meta| meta.len()).unwrap_or(before);
                let actual_delta = actual_len.saturating_sub(before).min(bytes.len() as u64);
                let unused_reservation = (bytes.len() as u64).saturating_sub(actual_delta);
                self.manager.release_bytes(unused_reservation);
                self.meta.committed_bytes = actual_len;
                self.meta.retention_sticky = true;
                self.meta.retention_reason = Some("capture_write_error".into());
                self.meta.incomplete_reason = Some(error.to_string());
                let _ = write_meta(&self.dir, &self.meta);
                Err(error)
            }
        }
    }

    pub fn snapshot(&self, start: u64) -> Snapshot {
        Snapshot {
            start: start.min(self.meta.committed_bytes),
            end: self.meta.committed_bytes,
            stored_bytes: self.meta.committed_bytes,
            path: self.dir.join(RAW_FILE),
        }
    }

    pub fn project(
        &self,
        mut snapshot: Snapshot,
        budget: usize,
        hold_incomplete_utf8: bool,
    ) -> io::Result<Projection> {
        let mut file = File::open(&snapshot.path)?;
        let take = budget.max(1) as u64;
        let mut bytes = Vec::new();
        let range = snapshot.end.saturating_sub(snapshot.start);
        let mut truncated = range > take;
        if !truncated {
            file.seek(SeekFrom::Start(snapshot.start))?;
            file.take(range).read_to_end(&mut bytes)?;
            if hold_incomplete_utf8
                && let Err(error) = std::str::from_utf8(&bytes)
                && error.error_len().is_none()
            {
                bytes.truncate(error.valid_up_to());
                snapshot.end = snapshot.start.saturating_add(bytes.len() as u64);
            }
        } else {
            let marker = format!("\n... {} bytes omitted ...\n", range.saturating_sub(take));
            if marker.len() as u64 >= take {
                let short = b"...";
                bytes.extend_from_slice(&short[..take.min(short.len() as u64) as usize]);
            } else {
                let content_budget = take - marker.len() as u64;
                let head_budget = content_budget / 2;
                let tail_budget = content_budget.saturating_sub(head_budget);
                let head =
                    read_utf8_safe_head(&mut file, snapshot.start, range, head_budget as usize)?;
                let mut tail = read_utf8_safe_tail(
                    &mut file,
                    snapshot.start,
                    snapshot.end,
                    tail_budget as usize,
                )?;
                if hold_incomplete_utf8
                    && let Err(error) = std::str::from_utf8(&tail)
                    && error.error_len().is_none()
                {
                    let dropped = tail.len().saturating_sub(error.valid_up_to());
                    tail.truncate(error.valid_up_to());
                    snapshot.end = snapshot.end.saturating_sub(dropped as u64);
                }
                let omitted = range
                    .saturating_sub(head.len() as u64)
                    .saturating_sub(tail.len() as u64);
                bytes.extend_from_slice(&head);
                bytes.extend_from_slice(format!("\n... {omitted} bytes omitted ...\n").as_bytes());
                bytes.extend_from_slice(&tail);
            }
        }
        let (mut output, encoding_loss) = match String::from_utf8(bytes) {
            Ok(output) => (output, false),
            Err(error) => (String::from_utf8_lossy(error.as_bytes()).into_owned(), true),
        };
        if output.len() > budget.max(1) {
            truncate_string_bytes(&mut output, budget.max(1));
            truncated = true;
        }
        Ok(Projection {
            snapshot,
            output,
            truncated,
            encoding_loss,
        })
    }

    pub fn recovery_path_published(&self) -> bool {
        self.meta.recovery_path_published
    }

    pub fn mark_recovery_required(&mut self, reason: &str) -> io::Result<()> {
        self.meta.retention_sticky = true;
        self.meta.recovery_path_published = true;
        if self.meta.retention_reason.is_none() {
            self.meta.retention_reason = Some(reason.to_owned());
        }
        self.meta.committed_bytes = self.current_len();
        if self.meta.state != ArtifactState::Active && self.meta.retain_until_unix_millis.is_none()
        {
            let sealed_at = self.meta.sealed_at_unix_millis.unwrap_or_else(unix_millis);
            self.manager
                .apply_retention_deadlines(&mut self.meta, sealed_at);
        }
        write_meta(&self.dir, &self.meta)
    }

    pub fn mark_incomplete_intent(&mut self, reason: &str) -> io::Result<()> {
        self.meta.retention_sticky = true;
        if self.meta.retention_reason.is_none() {
            self.meta.retention_reason = Some(reason.to_owned());
        }
        self.meta.incomplete_reason = Some(reason.to_owned());
        self.meta.committed_bytes = self.current_len();
        write_meta(&self.dir, &self.meta)
    }

    pub fn seal_complete(&mut self) -> io::Result<()> {
        self.meta.state = ArtifactState::Complete;
        self.meta.committed_bytes = self.current_len();
        if self.meta.retention_sticky {
            let sealed_at = unix_millis();
            self.manager
                .apply_retention_deadlines(&mut self.meta, sealed_at);
        } else {
            self.meta.sealed_at_unix_millis = Some(unix_millis());
        }
        write_meta(&self.dir, &self.meta)
    }

    pub fn seal_incomplete(&mut self, reason: &str) -> io::Result<()> {
        self.meta.state = ArtifactState::Incomplete;
        self.meta.retention_sticky = true;
        self.meta.incomplete_reason = Some(reason.to_owned());
        if self.meta.retention_reason.is_none() {
            self.meta.retention_reason = Some(reason.to_owned());
        }
        self.meta.committed_bytes = self.current_len();
        let sealed_at = unix_millis();
        self.manager
            .apply_retention_deadlines(&mut self.meta, sealed_at);
        write_meta(&self.dir, &self.meta)
    }

    pub fn ref_info(&self) -> OutputRefInfo {
        OutputRefInfo {
            path: self.dir.join(RAW_FILE),
            stored_bytes: self.meta.committed_bytes,
            capture_status: match self.meta.state {
                ArtifactState::Active => "open",
                ArtifactState::Complete => "complete",
                ArtifactState::Incomplete => "incomplete",
            },
            expires_at_unix_seconds: self.meta.expires_at_unix_millis.map(|millis| millis / 1000),
            incomplete_reason: self.meta.incomplete_reason.clone(),
        }
    }

    pub fn delete(&mut self) -> io::Result<()> {
        if self.deleted {
            return Ok(());
        }
        let bytes = self.current_len();
        self.file.take();
        remove_managed_artifact(&self.dir)?;
        self.manager.release_bytes(bytes);
        atomic_saturating_sub_usize(&self.manager.inner.used_files, 1);
        self.deleted = true;
        Ok(())
    }

    pub fn committed_bytes(&self) -> u64 {
        self.meta.committed_bytes
    }

    fn current_len(&self) -> u64 {
        self.file
            .as_ref()
            .and_then(|file| file.metadata().ok())
            .map(|meta| meta.len())
            .unwrap_or(self.meta.committed_bytes)
    }
}

fn read_meta(dir: &Path) -> io::Result<ArtifactMeta> {
    let bytes = fs::read(dir.join(META_FILE))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn write_meta(dir: &Path, meta: &ArtifactMeta) -> io::Result<()> {
    let tmp_path = dir.join("meta.json.tmp");
    match fs::remove_file(&tmp_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let bytes = serde_json::to_vec_pretty(meta)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    set_file_mode(&tmp_path)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.flush()?;
    drop(file);
    fs::rename(&tmp_path, dir.join(META_FILE))
}

fn remove_managed_artifact(dir: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(dir)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "refusing to remove non-directory output artifact",
        ));
    }
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name != RAW_FILE && name != META_FILE && name != "meta.json.tmp" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected entry in output artifact: {name}"),
            ));
        }
    }
    for name in [RAW_FILE, META_FILE, "meta.json.tmp"] {
        let path = dir.join(name);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    match fs::remove_dir(dir) {
        Ok(()) => Ok(()),
        Err(error) => Err(error),
    }
}

fn regular_file_len(path: &Path) -> Option<u64> {
    let metadata = fs::symlink_metadata(path).ok()?;
    (metadata.file_type().is_file() && !metadata.file_type().is_symlink()).then_some(metadata.len())
}

fn is_managed_artifact_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn unique_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

pub(crate) fn read_utf8_safe_head(
    file: &mut File,
    start: u64,
    range: u64,
    budget: usize,
) -> io::Result<Vec<u8>> {
    if budget == 0 || range == 0 {
        return Ok(Vec::new());
    }
    let read_len = range.min((budget.saturating_add(3)) as u64);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity(read_len as usize);
    Read::by_ref(file).take(read_len).read_to_end(&mut bytes)?;
    if bytes.len() <= budget {
        return Ok(bytes);
    }
    let limit = budget.min(bytes.len());
    let boundary = match std::str::from_utf8(&bytes) {
        Ok(text) => floor_char_boundary(text, limit),
        Err(error) if error.valid_up_to() >= limit => {
            let valid = unsafe { std::str::from_utf8_unchecked(&bytes[..error.valid_up_to()]) };
            floor_char_boundary(valid, limit)
        }
        Err(_) => limit,
    };
    bytes.truncate(boundary);
    Ok(bytes)
}

pub(crate) fn read_utf8_safe_tail(
    file: &mut File,
    start: u64,
    end: u64,
    budget: usize,
) -> io::Result<Vec<u8>> {
    let range = end.saturating_sub(start);
    if budget == 0 || range == 0 {
        return Ok(Vec::new());
    }
    let read_len = range.min((budget.saturating_add(3)) as u64);
    let read_start = end.saturating_sub(read_len);
    file.seek(SeekFrom::Start(read_start))?;
    let mut bytes = Vec::with_capacity(read_len as usize);
    Read::by_ref(file).take(read_len).read_to_end(&mut bytes)?;
    if bytes.len() <= budget {
        return Ok(bytes);
    }
    let target = bytes.len().saturating_sub(budget);
    let search_end = target.saturating_add(3).min(bytes.len());
    for boundary in target..=search_end {
        if std::str::from_utf8(&bytes[boundary..]).is_ok() {
            return Ok(bytes.split_off(boundary));
        }
    }
    Ok(bytes.split_off(target))
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn truncate_string_bytes(value: &mut String, budget: usize) {
    let boundary = floor_char_boundary(value, budget);
    value.truncate(boundary);
}

fn capacity_error(message: String) -> io::Error {
    io::Error::other(message)
}

fn atomic_saturating_sub_u64(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
        Some(current.saturating_sub(amount))
    });
}

fn atomic_saturating_sub_usize(value: &AtomicUsize, amount: usize) {
    let _ = value.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
        Some(current.saturating_sub(amount))
    });
}

#[cfg(unix)]
fn set_directory_mode(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_directory_mode(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_file_mode(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_file_mode(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn lock_instance(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("output store is already owned by another server instance: {error}"),
        ))
    }
}

#[cfg(not(unix))]
fn lock_instance(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn manager(
        root: &Path,
        retention: Duration,
        min_retention: Duration,
        max_bytes: u64,
    ) -> OutputStoreManager {
        OutputStoreManager::open(root, retention, min_retention, max_bytes, 0, 32).unwrap()
    }

    #[test]
    fn published_artifact_is_sticky_and_gc_respects_minimum_retention() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let manager = manager(
            &root,
            Duration::from_millis(50),
            Duration::from_millis(30),
            1024,
        );
        let mut store = manager.create_artifact().unwrap();
        store.append(b"hello").unwrap();
        store.mark_recovery_required("truncated").unwrap();
        store.seal_complete().unwrap();
        let path = store.ref_info().path;
        drop(store);

        manager.gc().unwrap();
        assert!(path.exists());
        std::thread::sleep(Duration::from_millis(70));
        manager.gc().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn active_artifact_is_never_collected() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let manager = manager(&root, Duration::ZERO, Duration::ZERO, 1);
        let mut store = manager.create_artifact().unwrap();
        store.append(b"x").unwrap();
        let path = store.ref_info().path;
        manager.gc().unwrap();
        assert!(path.exists());
        store.delete().unwrap();
    }

    #[test]
    fn restart_turns_active_artifact_into_retained_orphan() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let raw_path = {
            let manager = manager(
                &root,
                Duration::from_secs(60),
                Duration::from_secs(60),
                1024,
            );
            let mut store = manager.create_artifact().unwrap();
            store.append(b"orphaned").unwrap();
            store.ref_info().path
        };

        let manager = manager(
            &root,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1024,
        );
        manager.gc().unwrap();
        assert!(raw_path.exists());
        let meta = read_meta(raw_path.parent().unwrap()).unwrap();
        assert_eq!(meta.state, ArtifactState::Incomplete);
        assert!(meta.retention_sticky);
        assert!(
            meta.incomplete_reason
                .as_deref()
                .unwrap()
                .contains("restart")
        );
    }

    #[test]
    fn second_server_instance_is_rejected() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let _manager = manager(&root, Duration::from_secs(60), Duration::ZERO, 1024);
        let error =
            OutputStoreManager::open(&root, Duration::from_secs(60), Duration::ZERO, 1024, 0, 32)
                .err()
                .expect("second instance must fail");
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn capacity_failure_keeps_written_prefix() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let manager = manager(&root, Duration::from_secs(60), Duration::ZERO, 4);
        let mut store = manager.create_artifact().unwrap();
        store.append(b"abcd").unwrap();
        let error = store.append(b"e").unwrap_err();
        assert!(error.to_string().contains("capacity"));
        assert_eq!(fs::read(store.ref_info().path).unwrap(), b"abcd");
    }

    #[test]
    fn ordinary_capture_cannot_consume_recovery_headroom() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let manager =
            OutputStoreManager::open(&root, Duration::from_secs(60), Duration::ZERO, 4, 4, 32)
                .unwrap();

        let mut ordinary = manager.create_artifact().unwrap();
        ordinary.append(b"abcd").unwrap();
        let error = ordinary.append(b"e").unwrap_err();
        assert!(error.to_string().contains("capacity"));

        let mut recovery = manager.create_artifact().unwrap();
        recovery.append(b"ok").unwrap();
        assert_eq!(fs::read(recovery.ref_info().path).unwrap(), b"ok");
    }

    #[test]
    fn nonempty_artifact_directory_does_not_release_accounting() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let manager = manager(&root, Duration::from_secs(60), Duration::ZERO, 1024);
        let mut store = manager.create_artifact().unwrap();
        store.append(b"hello").unwrap();
        let artifact_dir = store.ref_info().path.parent().unwrap().to_path_buf();
        fs::write(artifact_dir.join("unexpected"), b"foreign").unwrap();

        let error = store.delete().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(artifact_dir.exists());
        assert!(store.ref_info().path.exists());
        assert!(artifact_dir.join(META_FILE).exists());
        assert_eq!(manager.inner.used_bytes.load(Ordering::SeqCst), 5);
        assert_eq!(manager.inner.used_files.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn corrupt_gc_candidate_does_not_block_new_artifacts() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("outputs");
        let manager = manager(&root, Duration::ZERO, Duration::ZERO, 1024);
        let mut store = manager.create_artifact().unwrap();
        store.append(b"hello").unwrap();
        store.mark_recovery_required("published").unwrap();
        store.seal_complete().unwrap();
        let artifact_dir = store.ref_info().path.parent().unwrap().to_path_buf();
        fs::write(artifact_dir.join("unexpected"), b"foreign").unwrap();
        drop(store);

        manager.gc().unwrap();
        assert!(artifact_dir.join(RAW_FILE).exists());
        assert!(artifact_dir.join(META_FILE).exists());
        assert_eq!(manager.inner.used_bytes.load(Ordering::SeqCst), 5);
        assert_eq!(manager.inner.used_files.load(Ordering::SeqCst), 1);

        let mut next = manager.create_artifact().unwrap();
        next.append(b"ok").unwrap();
        assert_eq!(manager.inner.used_bytes.load(Ordering::SeqCst), 7);
        assert_eq!(manager.inner.used_files.load(Ordering::SeqCst), 2);
    }
}

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use crate::output_projection::{Projection, Snapshot};

const RAW_FILE: &str = "raw.log";

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
    retention: Duration,
    max_bytes: u64,
    used_bytes: AtomicU64,
    // Serializes GC with creation/deletion and protects all live stores.
    active: Mutex<HashSet<PathBuf>>,
    _instance_lock: File,
}

pub struct OutputStore {
    manager: OutputStoreManager,
    dir: PathBuf,
    file: Option<File>,
    committed_bytes: u64,
    published: bool,
    finished: bool,
    incomplete_reason: Option<String>,
}

impl OutputStoreManager {
    pub fn open(root: &Path, retention: Duration, max_bytes: u64) -> io::Result<Self> {
        if max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output capacity must be positive",
            ));
        }
        fs::create_dir_all(root)?;
        set_directory_mode(root)?;
        let root = fs::canonicalize(root)?;
        let lock_path = root.join(".instance.lock");
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
                retention,
                max_bytes,
                used_bytes: AtomicU64::new(0),
                active: Mutex::new(HashSet::new()),
                _instance_lock: instance_lock,
            }),
        };
        let bytes = manager.logs()?.iter().map(|(_, meta)| meta.len()).sum();
        manager.inner.used_bytes.store(bytes, Ordering::SeqCst);
        manager.gc()?;
        Ok(manager)
    }

    fn logs(&self) -> io::Result<Vec<(PathBuf, fs::Metadata)>> {
        let mut logs = Vec::new();
        for entry in fs::read_dir(&self.inner.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.len() != 32
                || !name.bytes().all(|b| b.is_ascii_hexdigit())
                || !entry.file_type()?.is_dir()
            {
                continue;
            }
            if let Ok(meta) = fs::symlink_metadata(entry.path().join(RAW_FILE))
                && meta.is_file()
            {
                logs.push((entry.path(), meta));
            }
        }
        Ok(logs)
    }

    pub fn create_artifact(&self) -> io::Result<OutputStore> {
        self.gc()?;
        let mut active = self.inner.active.lock().unwrap();
        if self.inner.used_bytes.load(Ordering::SeqCst) >= self.inner.max_bytes {
            return Err(io::Error::other("output store capacity exhausted"));
        }
        let dir = loop {
            let dir = self
                .inner
                .root
                .join(format!("{:032x}", rand::random::<u128>()));
            match fs::create_dir(&dir) {
                Ok(()) => break dir,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        let create = || {
            set_directory_mode(&dir)?;
            let path = dir.join(RAW_FILE);
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&path)?;
            set_file_mode(&path)?;
            Ok(file)
        };
        let file = match create() {
            Ok(file) => file,
            Err(error) => {
                let _ = remove_log(&dir);
                return Err(error);
            }
        };
        active.insert(dir.clone());
        Ok(OutputStore {
            manager: self.clone(),
            dir,
            file: Some(file),
            committed_bytes: 0,
            published: false,
            finished: false,
            incomplete_reason: None,
        })
    }

    pub fn gc(&self) -> io::Result<()> {
        let active = self.inner.active.lock().unwrap();
        for (dir, meta) in self.logs()? {
            if !active.contains(&dir)
                && meta.modified()?.elapsed().unwrap_or_default() >= self.inner.retention
                && remove_log(&dir).is_ok()
            {
                self.release_bytes(meta.len());
            }
        }
        Ok(())
    }

    fn reserve_bytes(&self, bytes: u64) -> io::Result<()> {
        self.inner
            .used_bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.inner.max_bytes)
            })
            .map(|_| ())
            .map_err(|_| io::Error::other("output store capacity exceeded"))
    }

    fn release_bytes(&self, bytes: u64) {
        self.inner.used_bytes.fetch_sub(bytes, Ordering::SeqCst);
    }
}

impl OutputStore {
    pub fn append(&mut self, bytes: &[u8]) -> io::Result<u64> {
        if self.finished {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "output capture is finished",
            ));
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "output file is closed"))?;
        self.manager.reserve_bytes(bytes.len() as u64)?;
        let before = self.committed_bytes;
        match file.write_all(bytes) {
            Ok(()) => {
                self.committed_bytes += bytes.len() as u64;
                Ok(self.committed_bytes)
            }
            Err(error) => {
                self.committed_bytes = file.metadata().map(|m| m.len()).unwrap_or(before);
                self.manager.release_bytes(
                    bytes.len() as u64 - self.committed_bytes.saturating_sub(before),
                );
                Err(error)
            }
        }
    }

    pub fn snapshot(&self, start: u64) -> Snapshot {
        Snapshot {
            start: start.min(self.committed_bytes),
            end: self.committed_bytes,
            stored_bytes: self.committed_bytes,
            path: self.dir.join(RAW_FILE),
        }
    }

    /// Compatibility entry point; projection reads a snapshot independently of storage.
    #[deprecated(note = "use output_projection::project instead")]
    pub fn project(
        &self,
        snapshot: Snapshot,
        budget: usize,
        hold_incomplete_utf8: bool,
    ) -> io::Result<Projection> {
        crate::output_projection::project(snapshot, budget, hold_incomplete_utf8)
    }

    pub fn recovery_path_published(&self) -> bool {
        self.published
    }

    pub fn finish(&mut self, reason: Option<String>) -> io::Result<()> {
        self.finished = true;
        self.incomplete_reason = reason;
        if let Some(file) = &self.file {
            file.set_modified(SystemTime::now())?;
        }
        Ok(())
    }

    pub fn publish(&mut self) {
        self.published = true;
    }

    pub fn ref_info(&self) -> OutputRefInfo {
        let expires = self
            .finished
            .then(|| {
                self.file
                    .as_ref()?
                    .metadata()
                    .ok()?
                    .modified()
                    .ok()?
                    .checked_add(self.manager.inner.retention)?
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_secs())
            })
            .flatten();
        OutputRefInfo {
            path: self.dir.join(RAW_FILE),
            stored_bytes: self.committed_bytes,
            capture_status: if self.incomplete_reason.is_some() {
                "incomplete"
            } else if self.finished {
                "complete"
            } else {
                "open"
            },
            expires_at_unix_seconds: expires,
            incomplete_reason: self.incomplete_reason.clone(),
        }
    }

    pub fn delete(&mut self) -> io::Result<()> {
        if self.file.is_none() {
            return Ok(());
        }
        let _active = self.manager.inner.active.lock().unwrap();
        remove_log(&self.dir)?;
        self.file.take();
        self.manager.release_bytes(self.committed_bytes);
        Ok(())
    }

    pub fn committed_bytes(&self) -> u64 {
        self.committed_bytes
    }
}

impl Drop for OutputStore {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.delete();
        }
        self.manager.inner.active.lock().unwrap().remove(&self.dir);
    }
}

fn remove_log(dir: &Path) -> io::Result<()> {
    // Old sidecars can be discarded without reading or reconciling their state.
    const FILES: [&str; 3] = [RAW_FILE, "meta.json", "meta.json.tmp"];
    if !fs::symlink_metadata(dir)?.is_dir()
        || fs::read_dir(dir)?.any(|entry| {
            !entry.is_ok_and(|entry| FILES.iter().any(|name| entry.file_name() == *name))
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected entry in output directory",
        ));
    }
    // Remove sidecars first so any failure leaves the accounted raw log intact.
    for name in FILES.iter().rev() {
        match fs::remove_file(dir.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    fs::remove_dir(dir)
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

    fn manager(root: &Path, max_bytes: u64) -> OutputStoreManager {
        OutputStoreManager::open(root, Duration::from_secs(60), max_bytes).unwrap()
    }

    fn age(path: &Path) {
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(120))
            .unwrap();
    }

    #[test]
    fn only_published_logs_survive_drop_and_gc_uses_mtime() {
        let root = TempDir::new().unwrap();
        let manager = manager(root.path(), 1024);
        for published in [false, true] {
            let mut store = manager.create_artifact().unwrap();
            store.append(b"hello").unwrap();
            store.finish(None).unwrap();
            let path = store.ref_info().path;
            if published {
                store.publish();
            }
            drop(store);
            assert_eq!(path.exists(), published);
            if published {
                manager.gc().unwrap();
                assert!(path.exists());
                age(&path);
                manager.gc().unwrap();
                assert!(!path.exists());
            }
        }
        assert_eq!(manager.inner.used_bytes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn live_stores_are_protected_and_finish_refreshes_mtime() {
        let root = TempDir::new().unwrap();
        let manager = manager(root.path(), 1024);
        let mut store = manager.create_artifact().unwrap();
        store.append(b"hello").unwrap();
        store.publish();
        let path = store.ref_info().path;
        age(&path);
        manager.gc().unwrap();
        assert!(path.exists());
        store.finish(None).unwrap();
        assert_eq!(store.ref_info().capture_status, "complete");
        assert!(store.ref_info().expires_at_unix_seconds.is_some());
        drop(store);
        manager.gc().unwrap();
        assert!(path.exists());
    }

    #[test]
    fn startup_accounts_existing_raw_logs_and_expires_old_ones() {
        let root = TempDir::new().unwrap();
        for (id, expired) in [(1, true), (2, false)] {
            let dir = root.path().join(format!("{id:032x}"));
            fs::create_dir(&dir).unwrap();
            let path = dir.join(RAW_FILE);
            fs::write(&path, b"orphan").unwrap();
            // The raw log's mtime remains the only expiration input even for
            // captures left by older versions with unreadable metadata.
            fs::write(dir.join("meta.json"), b"invalid metadata").unwrap();
            if expired {
                age(&path);
            }
        }
        let manager = manager(root.path(), 6);
        assert_eq!(manager.inner.used_bytes.load(Ordering::SeqCst), 6);
        assert!(manager.create_artifact().is_err());
        assert!(!root.path().join(format!("{:032x}", 1)).exists());
        let path = root.path().join(format!("{:032x}", 2)).join(RAW_FILE);
        assert_eq!(fs::read(&path).unwrap(), b"orphan");
        age(&path);
        assert!(manager.create_artifact().is_ok());
    }

    #[test]
    fn single_capacity_limit_keeps_written_prefix_and_releases_deleted_bytes() {
        let root = TempDir::new().unwrap();
        let manager = manager(root.path(), 4);
        let mut first = manager.create_artifact().unwrap();
        let mut second = manager.create_artifact().unwrap();
        first.append(b"abc").unwrap();
        second.append(b"d").unwrap();
        assert!(
            first
                .append(b"e")
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );
        first.finish(Some("capacity exceeded".into())).unwrap();
        first.publish();
        let reference = first.ref_info();
        assert_eq!(reference.capture_status, "incomplete");
        assert_eq!(fs::read(reference.path).unwrap(), b"abc");
        drop(second);
        let mut third = manager.create_artifact().unwrap();
        third.append(b"f").unwrap();
        assert!(third.append(b"g").is_err());
    }

    #[test]
    fn second_instance_is_rejected() {
        let root = TempDir::new().unwrap();
        let _manager = manager(root.path(), 1024);
        let error = OutputStoreManager::open(root.path(), Duration::ZERO, 1024)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn foreign_entries_are_preserved_without_losing_accounting() {
        let root = TempDir::new().unwrap();
        let manager = manager(root.path(), 1024);
        let mut store = manager.create_artifact().unwrap();
        store.append(b"hello").unwrap();
        fs::write(store.dir.join("foreign"), b"keep").unwrap();
        store.publish();
        let path = store.ref_info().path;
        age(&path);
        assert_eq!(
            store.delete().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(store);
        manager.gc().unwrap();
        assert!(path.exists());
        assert_eq!(manager.inner.used_bytes.load(Ordering::SeqCst), 5);
        assert!(manager.create_artifact().is_ok());
    }
}

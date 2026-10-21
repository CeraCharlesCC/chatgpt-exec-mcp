//! Temporary chunk ownership, bounded zstd encoding, and archive file I/O.
//!
//! Chunk files are removed when their final owner is dropped. No history
//! state mutex is held during compression, file reads, or file deletion.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::events::ActivityEvent;

pub(super) const MAX_ARCHIVE_CHUNKS: usize = 8;
pub(super) const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CHUNK_BYTES: u64 = MAX_ARCHIVE_BYTES / MAX_ARCHIVE_CHUNKS as u64;
const MAX_SERIALIZED_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

/// An immutable chunk stays readable while a page is in flight, even if CLEAR
/// or retention simultaneously removes the chunk from the hub.
#[derive(Debug)]
pub(super) struct ArchiveFile {
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
}

impl Drop for ArchiveFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Clone, Debug)]
pub(super) struct ArchiveChunk {
    pub(super) first_id: u64,
    pub(super) last_id: u64,
    pub(super) count: usize,
    pub(super) file: Arc<ArchiveFile>,
}

#[derive(Clone)]
pub(super) struct PendingChunk {
    pub(super) generation: u64,
    pub(super) first_id: u64,
    pub(super) last_id: u64,
    pub(super) events: Arc<Vec<ActivityEvent>>,
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

pub(super) fn write_chunk(dir: &Path, chunk: &PendingChunk) -> io::Result<ArchiveChunk> {
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

pub(super) fn read_chunk(chunk: &ArchiveChunk) -> io::Result<Vec<ActivityEvent>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_writer_rejects_oversized_serialized_or_compressed_chunks() {
        let mut output = Vec::new();
        {
            let mut bounded = BoundedWriter::new(&mut output, 3);
            assert!(bounded.write_all(b"abcdef").is_err());
        }
        assert_eq!(output, b"abc");
    }
}

//! Bounded local image loading. Images are passed through without decoding or resizing.

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};

pub(crate) const MAX_IMAGE_BYTES: u64 = 10 * 1024 * 1024;

pub(crate) struct LoadedImage {
    pub data: String,
    pub mime_type: &'static str,
    pub byte_len: usize,
}

pub(crate) async fn load(workspace: &Path, path: String) -> anyhow::Result<LoadedImage> {
    if path.is_empty() {
        bail!("path must not be empty");
    }
    let path = workspace.join(path);
    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.read(true);
        // Opening a FIFO must not block before we can reject it as non-regular.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = options.open(path).context("image open failed")?;
        let metadata = file.metadata().context("image metadata failed")?;
        if !metadata.is_file() {
            bail!("image path must be a regular file");
        }
        if metadata.len() > MAX_IMAGE_BYTES {
            bail!("image exceeds the 10 MiB limit");
        }
        let mut bytes = Vec::new();
        // Also bound the actual read in case the file grows after metadata().
        file.take(MAX_IMAGE_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("image read failed")?;
        if bytes.len() as u64 > MAX_IMAGE_BYTES {
            bail!("image exceeds the 10 MiB limit");
        }
        let mime_type = mime_type(&bytes)
            .context("unsupported image format; expected PNG, JPEG, GIF, or WebP")?;
        Ok(LoadedImage {
            data: STANDARD.encode(&bytes),
            mime_type,
            byte_len: bytes.len(),
        })
    })
    .await
    .context("image loader task failed")?
}

/// Sniff the file signature, independent of the file name or extension.
fn mime_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else {
        None
    }
}

#[cfg(test)]
mod tests;

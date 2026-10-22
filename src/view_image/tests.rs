use super::*;

#[test]
fn sniffs_supported_signatures_and_rejects_other_content() {
    for (bytes, expected) in [
        (b"\x89PNG\r\n\x1a\n".as_slice(), "image/png"),
        (b"\xff\xd8\xff\xe0", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
        (b"RIFF\x00\x00\x00\x00WEBP", "image/webp"),
    ] {
        assert_eq!(mime_type(bytes), Some(expected));
    }
    for bytes in [
        b"".as_slice(),
        b"not an image",
        b"\x89PNG",
        b"RIFF\x00\x00\x00\x00WAVE",
        b"<svg xmlns=\"http://www.w3.org/2000/svg\"/>",
    ] {
        assert_eq!(mime_type(bytes), None);
    }
}

#[tokio::test]
async fn file_errors_are_bounded_and_do_not_return_image_content() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("text.png"), b"not an image").unwrap();
    let oversized = std::fs::File::create(dir.path().join("large.png")).unwrap();
    oversized.set_len(MAX_IMAGE_BYTES + 1).unwrap();
    for (path, expected) in [
        ("", "path must not be empty"),
        ("missing.png", "image open failed"),
        (".", "regular file"),
        ("text.png", "unsupported image format"),
        ("large.png", "10 MiB limit"),
    ] {
        let error = load(dir.path(), path.into()).await.err().unwrap();
        assert!(error.to_string().contains(expected), "{error:#}");
    }
}

#[tokio::test]
async fn accepts_exact_size_limit_without_truncating_the_image() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let mut file = std::fs::File::create(dir.path().join("image.png")).unwrap();
    file.write_all(b"\x89PNG\r\n\x1a\n").unwrap();
    file.set_len(MAX_IMAGE_BYTES).unwrap();
    let image = load(dir.path(), "image.png".into()).await.unwrap();
    let bytes = STANDARD.decode(&image.data).unwrap();
    assert_eq!(bytes.len() as u64, MAX_IMAGE_BYTES);
    assert!(bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
    assert_eq!(image.byte_len as u64, MAX_IMAGE_BYTES);
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_fifo_without_waiting_for_a_writer() {
    use std::os::unix::ffi::OsStrExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fifo.png");
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        load(dir.path(), "fifo.png".into()),
    )
    .await
    .expect("opening FIFO blocked");
    assert!(result.err().unwrap().to_string().contains("regular file"));
}

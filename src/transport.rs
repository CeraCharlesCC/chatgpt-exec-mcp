//! Stateless MCP over a private Unix socket. rmcp owns protocol dispatch.

use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

use crate::ExecMcpServer;

const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

pub async fn serve_unix(
    server: ExecMcpServer,
    path: &Path,
    shutdown: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) -> anyhow::Result<()> {
    prepare_socket_path(path)?;
    let listener = tokio::net::UnixListener::bind(path).context("bind MCP Unix socket failed")?;
    let socket = SocketGuard::new(path)?;
    let server = server.with_http_protocol();
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.stateless_protocol_metadata_required = true;
    config.json_response = true;
    config.max_request_body_bytes = MAX_BODY_BYTES;
    let cancellation = config.cancellation_token.clone();
    let service: StreamableHttpService<ExecMcpServer, LocalSessionManager> =
        StreamableHttpService::new(move || Ok(server.clone()), Default::default(), config);
    let router = axum::Router::new()
        .route("/readyz", axum::routing::get(|| async { "ready" }))
        .nest_service("/mcp", service);
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown.await;
            cancellation.cancel();
        })
        .await
        .context("streamable HTTP server failed");
    drop(socket);
    result
}

fn prepare_socket_path(path: &Path) -> anyhow::Result<()> {
    use std::io::ErrorKind;
    use std::os::unix::fs::FileTypeExt as _;
    anyhow::ensure!(
        path.is_absolute(),
        "listen Unix socket path must be absolute"
    );
    let parent = path.parent().context("listen Unix socket has no parent")?;
    anyhow::ensure!(parent.is_dir(), "listen Unix socket parent must exist");
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "listen Unix socket path exists and is not a socket"
            );
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => anyhow::bail!("listen Unix socket is already active"),
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::ConnectionRefused | ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path).context("remove stale Unix socket failed")?;
                }
                Err(error) => return Err(error).context("probe existing Unix socket failed"),
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect Unix socket failed"),
    }
    Ok(())
}

/// Remove only the socket this process bound, never a replacement path.
struct SocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl SocketGuard {
    fn new(path: &Path) -> anyhow::Result<Self> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let metadata = std::fs::symlink_metadata(path)?;
        let guard = Self {
            path: path.to_owned(),
            dev: metadata.dev(),
            ino: metadata.ino(),
        };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        Ok(guard)
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket()
                && metadata.dev() == self.dev
                && metadata.ino() == self.ino
        }) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn refuses_active_sockets_regular_files_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.sock");
        let listener = UnixListener::bind(&path).unwrap();
        assert!(prepare_socket_path(&path).is_err());
        assert!(path.exists());
        drop(listener);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "keep").unwrap();
        assert!(prepare_socket_path(&path).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep");
        let link = dir.path().join("link.sock");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(prepare_socket_path(&link).is_err());
        assert!(link.is_symlink());
    }

    #[test]
    fn stale_socket_is_reclaimed_and_cleanup_preserves_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp.sock");
        drop(UnixListener::bind(&path).unwrap());
        prepare_socket_path(&path).unwrap();
        assert!(!path.exists());
        let listener = UnixListener::bind(&path).unwrap();
        let guard = SocketGuard::new(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();
        drop(guard);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "replacement");
        drop(listener);
    }
}

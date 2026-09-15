use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::ServiceExt;

use chatgpt_exec_mcp::Config;
use chatgpt_exec_mcp::ExecMcpServer;
use chatgpt_exec_mcp::ProcessManager;

#[cfg(unix)]
const FORCED_PROTOCOL_VERSION: &str = "2025-11-25";
#[cfg(unix)]
const STREAMABLE_HTTP_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
struct RunArgs {
    config_path: PathBuf,
    listen_unix: Option<PathBuf>,
}

enum StartupAction {
    Run(RunArgs),
    Help,
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match parse_args(std::env::args_os().skip(1).collect())? {
        StartupAction::Help => {
            println!(
                "Usage: chatgpt-exec-mcp --config <PATH> [--listen-unix <PATH>]\n       chatgpt-exec-mcp --help | --version"
            );
            return Ok(());
        }
        StartupAction::Version => {
            println!("chatgpt-exec-mcp {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        StartupAction::Run(args) => run(args).await,
    }
}

fn parse_args(args: Vec<OsString>) -> anyhow::Result<StartupAction> {
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        return Ok(StartupAction::Help);
    }
    if args.len() == 1 && (args[0] == "--version" || args[0] == "-V") {
        return Ok(StartupAction::Version);
    }

    let mut config_path = None;
    let mut listen_unix = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        index += 1;
        let destination = match flag.to_str() {
            Some("--config") => &mut config_path,
            Some("--listen-unix") => &mut listen_unix,
            _ => anyhow::bail!("unexpected argument; use --config <PATH> [--listen-unix <PATH>]"),
        };
        anyhow::ensure!(
            destination.is_none(),
            "duplicate argument: {}",
            flag.to_string_lossy()
        );
        let value = args
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("{} requires a value", flag.to_string_lossy()))?;
        anyhow::ensure!(
            !value.is_empty(),
            "{} requires a non-empty value",
            flag.to_string_lossy()
        );
        *destination = Some(PathBuf::from(value));
        index += 1;
    }

    let config_path = config_path.ok_or_else(|| {
        anyhow::anyhow!("usage: chatgpt-exec-mcp --config <PATH> [--listen-unix <PATH>]")
    })?;
    if let Some(path) = listen_unix.as_ref() {
        anyhow::ensure!(
            path.is_absolute(),
            "--listen-unix requires an absolute path"
        );
        #[cfg(not(unix))]
        anyhow::bail!("--listen-unix is only supported on Unix");
    }
    Ok(StartupAction::Run(RunArgs {
        config_path,
        listen_unix,
    }))
}

async fn run(args: RunArgs) -> anyhow::Result<()> {
    let config = Config::load(&args.config_path)?;
    let manager = ProcessManager::new(config)?;
    let reaper = manager.spawn_reaper();

    let result = if let Some(path) = args.listen_unix.as_deref() {
        run_streamable_http(manager.clone(), path).await
    } else {
        run_stdio(manager.clone()).await
    };

    manager.shutdown_all().await;
    reaper.abort();
    let _ = reaper.await;
    result
}

async fn run_stdio(manager: Arc<ProcessManager>) -> anyhow::Result<()> {
    let service = ExecMcpServer::new(manager);
    let running = service
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await?;

    tokio::select! {
        result = running.waiting() => {
            let _ = result?;
        }
        result = shutdown_signal() => {
            result?;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn run_streamable_http(
    manager: Arc<ProcessManager>,
    socket_path: &Path,
) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };

    prepare_socket_path(socket_path)?;
    let factory_manager = manager;
    let http_config = StreamableHttpServerConfig::default();
    let cancellation = http_config.cancellation_token.clone();
    let service: StreamableHttpService<ExecMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(ExecMcpServer::new(factory_manager.clone())),
            Default::default(),
            http_config,
        );
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(axum::middleware::from_fn(force_legacy_protocol_initialize));
    let listener = tokio::net::UnixListener::bind(socket_path)
        .with_context(|| format!("bind unix socket failed: {}", socket_path.display()))?;

    let serve_result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_signal().await;
            cancellation.cancel();
        })
        .await
        .context("streamable HTTP server failed");
    cleanup_socket_path(socket_path);
    serve_result
}

#[cfg(unix)]
async fn force_legacy_protocol_initialize(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::body::Body;
    use axum::http::{HeaderValue, StatusCode, header::CONTENT_LENGTH};

    if request.method() != axum::http::Method::POST
        || request.headers().contains_key("mcp-session-id")
    {
        return next.run(request).await;
    }

    let (mut parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, STREAMABLE_HTTP_MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return axum::response::Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(Body::from("request body exceeds Streamable HTTP limit"))
                .expect("static response is valid");
        }
    };
    let mut value = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return next
                .run(axum::http::Request::from_parts(parts, Body::from(bytes)))
                .await;
        }
    };

    let is_initialize =
        value.get("method").and_then(serde_json::Value::as_str) == Some("initialize");
    if !is_initialize {
        return next
            .run(axum::http::Request::from_parts(parts, Body::from(bytes)))
            .await;
    }

    if let Some(params) = value
        .get_mut("params")
        .and_then(serde_json::Value::as_object_mut)
    {
        params.insert(
            "protocolVersion".to_owned(),
            serde_json::Value::String(FORCED_PROTOCOL_VERSION.to_owned()),
        );
    }
    parts.headers.remove(CONTENT_LENGTH);
    parts.headers.insert(
        "mcp-protocol-version",
        HeaderValue::from_static(FORCED_PROTOCOL_VERSION),
    );
    let body = serde_json::to_vec(&value).expect("JSON value serialization cannot fail");
    next.run(axum::http::Request::from_parts(parts, Body::from(body)))
        .await
}

#[cfg(unix)]
fn prepare_socket_path(path: &Path) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use std::io::ErrorKind;
    use std::os::unix::fs::FileTypeExt as _;

    anyhow::ensure!(
        path.is_absolute(),
        "listen unix socket path must be absolute"
    );
    let parent = path
        .parent()
        .context("listen unix socket path has no parent")?;
    anyhow::ensure!(parent.is_dir(), "listen unix socket parent must exist");

    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "listen unix socket path exists and is not a socket"
            );
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => anyhow::bail!("listen unix socket is already active"),
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::ConnectionRefused | ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path).context("remove stale unix socket failed")?;
                }
                Err(error) => return Err(error).context("probe existing unix socket failed"),
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect unix socket path failed"),
    }
    Ok(())
}

#[cfg(unix)]
fn cleanup_socket_path(path: &Path) {
    use std::os::unix::fs::FileTypeExt as _;
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_socket()) {
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> anyhow::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = terminate.recv() => {},
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

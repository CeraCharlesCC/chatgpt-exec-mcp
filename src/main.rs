use std::ffi::OsString;
use std::path::PathBuf;

use rmcp::ServiceExt;

use chatgpt_exec_mcp::Config;
use chatgpt_exec_mcp::ExecMcpServer;
use chatgpt_exec_mcp::ProcessManager;

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
    let events = config
        .events
        .clone()
        .map(|settings| {
            chatgpt_exec_mcp::events::EventStore::open(&settings.database_path)
                .map(|store| (std::sync::Arc::new(store), settings.principal))
        })
        .transpose()?;
    let manager = ProcessManager::new(config)?;
    let reaper = manager.spawn_reaper();
    let mut server = ExecMcpServer::new(manager.clone());
    let worker = events.map(|(store, principal)| {
        let worker = store.spawn_worker();
        server = server.clone().with_events(store, principal);
        worker
    });

    let result = if let Some(path) = args.listen_unix.as_deref() {
        run_http(server, path).await
    } else {
        run_stdio(server).await
    };

    manager.shutdown_all().await;
    reaper.abort();
    let _ = reaper.await;
    if let Some(worker) = worker {
        worker.abort();
        let _ = worker.await;
    }
    result
}

#[cfg(unix)]
async fn run_http(server: ExecMcpServer, path: &std::path::Path) -> anyhow::Result<()> {
    chatgpt_exec_mcp::transport::serve_unix(server, path, shutdown_signal()).await
}

#[cfg(not(unix))]
async fn run_http(_server: ExecMcpServer, _path: &std::path::Path) -> anyhow::Result<()> {
    anyhow::bail!("--listen-unix is only supported on Unix")
}

async fn run_stdio(service: ExecMcpServer) -> anyhow::Result<()> {
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

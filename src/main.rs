use rmcp::ServiceExt;

use chatgpt_exec_mcp::Config;
use chatgpt_exec_mcp::ExecMcpServer;
use chatgpt_exec_mcp::ProcessManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    let config_path = match first.as_deref() {
        Some(flag) if flag == "--config" => args.next().map(std::path::PathBuf::from),
        Some(flag) if (flag == "--help" || flag == "-h") && args.len() == 0 => {
            println!(
                "Usage: chatgpt-exec-mcp --config <PATH>\n       chatgpt-exec-mcp --help | --version"
            );
            return Ok(());
        }
        Some(flag) if (flag == "--version" || flag == "-V") && args.len() == 0 => {
            println!("chatgpt-exec-mcp {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        _ => None,
    };
    anyhow::ensure!(
        args.next().is_none(),
        "unexpected argument; use --config <PATH>"
    );
    let config_path =
        config_path.ok_or_else(|| anyhow::anyhow!("usage: chatgpt-exec-mcp --config <PATH>"))?;
    let config = Config::load(&config_path)?;
    let manager = ProcessManager::new(config)?;
    let reaper = manager.spawn_reaper();
    let service = ExecMcpServer::new(manager.clone());
    let running = service
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await?;

    tokio::select! {
        result = running.waiting() => {
            result?;
        }
        result = shutdown_signal() => {
            result?;
        }
    }

    manager.shutdown_all().await;
    reaper.abort();
    let _ = reaper.await;
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

use clap::Parser;
use rmcp::ServiceExt;

use chatgpt_exec_mcp::Config;
use chatgpt_exec_mcp::ExecMcpServer;
use chatgpt_exec_mcp::ProcessManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
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

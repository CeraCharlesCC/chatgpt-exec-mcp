use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(name = "chatgpt-exec-mcp", version, about)]
pub struct Config {
    /// Base directory used to resolve relative workdir values. This is not a
    /// sandbox and does not restrict absolute paths.
    #[arg(long, env = "CHATGPT_EXEC_WORKSPACE", default_value = ".")]
    pub workspace: PathBuf,

    /// Shell used for command strings.
    #[arg(long, env = "CHATGPT_EXEC_SHELL", default_value = "/bin/bash")]
    pub shell: PathBuf,

    #[arg(long, default_value_t = 3)]
    pub max_explicit_sessions: usize,

    #[arg(long, default_value_t = 3)]
    pub max_exec_continuations: usize,

    #[arg(long, default_value = "3600", value_parser = parse_duration_seconds)]
    pub explicit_session_idle_timeout: Duration,

    #[arg(long, default_value = "900", value_parser = parse_duration_seconds)]
    pub exec_continuation_idle_timeout: Duration,

    #[arg(long, default_value = "30", value_parser = parse_duration_seconds)]
    pub reaper_interval: Duration,

    #[arg(long, default_value_t = 1_048_576)]
    pub output_cap_bytes: usize,

    /// Directory for raw output artifacts. If omitted, defaults under workspace.
    #[arg(long, env = "CHATGPT_EXEC_OUTPUT_STORE_DIR")]
    pub output_store_dir: Option<PathBuf>,

    #[arg(long, default_value = "604800", value_parser = parse_duration_seconds)]
    pub output_store_retention: Duration,

    #[arg(long, default_value = "3600", value_parser = parse_duration_seconds)]
    pub output_store_min_retention: Duration,

    #[arg(long, default_value_t = 4_294_967_296_u64)]
    pub output_store_max_bytes: u64,

    #[arg(long, default_value_t = 16_777_216_u64)]
    pub output_store_headroom_bytes: u64,

    #[arg(long, default_value_t = 2048)]
    pub output_store_max_files: usize,
}

fn parse_duration_seconds(value: &str) -> Result<Duration, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|error| format!("invalid duration `{value}`: {error}"))?;
    Ok(Duration::from_secs(seconds))
}

impl Default for Config {
    fn default() -> Self {
        Self {
            workspace: PathBuf::from("."),
            shell: PathBuf::from("/bin/bash"),
            max_explicit_sessions: 3,
            max_exec_continuations: 3,
            explicit_session_idle_timeout: Duration::from_secs(3600),
            exec_continuation_idle_timeout: Duration::from_secs(900),
            reaper_interval: Duration::from_secs(30),
            output_cap_bytes: 1_048_576,
            output_store_dir: None,
            output_store_retention: Duration::from_secs(7 * 24 * 60 * 60),
            output_store_min_retention: Duration::from_secs(60 * 60),
            output_store_max_bytes: 4_294_967_296,
            output_store_headroom_bytes: 16_777_216,
            output_store_max_files: 2048,
        }
    }
}

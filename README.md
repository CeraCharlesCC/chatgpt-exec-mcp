# chatgpt-exec-mcp

An MCP server for shell commands, stateful terminals, and coordination between
multiple agents.

It supports stdio and Unix-socket Streamable HTTP, bounded command output with
recoverable logs, optional agent pools, and a loopback-only debugging WebUI.

## Features

- Run shell commands through MCP.
- Keep interactive shells, REPLs, and long-running processes alive across calls.
- Recover full logs when command output is too large for a tool response.
- Exchange messages between cooperating agents with optional persistent pools.
- Serve MCP over stdio or a Unix socket.
- Inspect recent MCP activity and pool state with an optional local WebUI.

## Install

Linux x86_64 binaries are available from
[GitHub Releases](https://github.com/CeraCharlesCC/chatgpt-exec-mcp/releases).

To build from source, Rust 1.88 or newer is required:

```bash
cargo build --release --locked
```

## Quick start

Start from [examples/minimal.json](examples/minimal.json), then edit the
workspace, shell, output directory, and child environment for your system.

```bash
chatgpt-exec-mcp --config /absolute/path/to/config.json
```

A typical stdio MCP client configuration is:

```json
{
  "mcpServers": {
    "exec": {
      "command": "/absolute/path/to/chatgpt-exec-mcp",
      "args": ["--config", "/absolute/path/to/config.json"]
    }
  }
}
```

See [Configuration](docs/configuration.md) for all settings.

## Tools

| Tool | Purpose |
| --- | --- |
| `exec_command` | Run a shell command. Long-running commands can continue through a session ID. |
| `start_session` | Start a stateful shell, REPL, or long-running process. Uses a PTY by default. |
| `write_stdin` | Send input, interrupt a process, or poll pending output. |
| `wait_for_exit` | Wait for a running process to finish without polling ordinary output. |

Use `exec_command` for ordinary stateless commands and `start_session` when
shell state or the working directory must persist across calls.

Large outputs are bounded in MCP responses. When the complete capture is stored
separately, the result includes an `output_ref` that points to the retained log.

## Agent pools

Optional agent pools add two tools for coordination between MCP clients:

```text
pool_members(pool="project")
pool_send(pool="project", target="global", message="Tests passed")
```

Messages are queued and delivered with later MCP tool calls; they do not wake an
idle client. See [Agent pools](docs/agent-pools.md) for delivery semantics,
membership behavior, and deployment notes.

## Streamable HTTP

To serve MCP over a Unix socket:

```bash
chatgpt-exec-mcp \
  --config /absolute/path/to/config.json \
  --listen-unix /absolute/path/mcp.sock
```

Clients must support MCP `2026-07-28`. The MCP endpoint is `/mcp`, and
`/readyz` reports local readiness. See
[Unix Streamable HTTP](docs/configuration.md#unix-streamable-http) for details.

## Debug WebUI

With Unix Streamable HTTP enabled, add a loopback-only WebUI for recent activity
and agent-pool administration:

```text
--webui-listen 127.0.0.1:19162
```

See [Debug WebUI](docs/configuration.md#debug-webui).

## Security

`workspace` sets the default working directory; it is **not a filesystem
sandbox**. Commands run with the permissions of the server process. Use an
external sandbox when filesystem isolation is required.

Run HTTP and agent-pool deployments behind an appropriately authenticated
private transport. See [Configuration](docs/configuration.md) for environment
and deployment options.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development, testing, and release
artifact instructions.

## License

Apache License 2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE), and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for vendored OpenAI Codex code.

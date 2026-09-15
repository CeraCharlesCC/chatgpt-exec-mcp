# chatgpt-exec-mcp

An MCP server for running shell commands over stdio or Streamable HTTP on a Unix socket.

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the vendored PTY utility.

## Tools

The MCP server exposes five tools:

| Tool | Purpose |
| --- | --- |
| `exec_command` | Run a shell command and return its exit code. If it is still running after the initial yield, return a session ID instead. |
| `start_session` | Start an explicitly stateful shell, REPL, or long-running process. Uses a PTY by default. |
| `write_stdin` | Send input, send Ctrl-C, or immediately poll output from a running session. |
| `wait_for_exit` | Wait for a running session to finish without waking on ordinary stdout/stderr activity. |
| `session_probe` | Report the request transport, MCP session ID, negotiated protocol version, and client implementation for session-isolation diagnostics. |

Session IDs are short memorable handles such as `amber-river`. The server enforces separate quotas and idle timeouts for explicit sessions and `exec_command` continuations.

## Build

Rust 1.85 or newer is required (edition 2024).

```bash
cargo build --release --locked
cargo test --all-targets --locked
```

For a distributable standalone binary, use the fixed offline build recipe after
fetching the locked dependencies. It builds in an isolated target directory and
remaps builder paths out of runtime diagnostics:

```bash
python3 scripts/build-standalone.py --output /tmp/chatgpt-exec-mcp
```

Linux x86_64 binaries are available from [Releases](https://github.com/CeraCharlesCC/chatgpt-exec-mcp/releases). Optional provenance verification: `gh attestation verify <binary> --repo CeraCharlesCC/chatgpt-exec-mcp`.

Run the server over stdio:

```bash
./target/release/chatgpt-exec-mcp --config examples/minimal.json
```

On Unix, run the same server over Streamable HTTP using a Unix-domain socket:

```bash
./target/release/chatgpt-exec-mcp \
  --config examples/minimal.json \
  --listen-unix /absolute/path/to/chatgpt-exec-mcp.sock
```

The Streamable HTTP endpoint is `/mcp`. This deployment intentionally pins negotiation to
MCP `2025-11-25`: an incoming `initialize` that requests a later revision is normalized to
`2025-11-25` before rmcp classifies the request, so the connection uses the legacy stateful
Streamable HTTP lifecycle and receives an `Mcp-Session-Id`. `session_probe` exposes exactly
what the handler sees, which makes it useful for checking whether two client conversations
are actually routed as distinct MCP sessions.

This experiment did not achieve conversation-level isolation in ChatGPT: in observed use,
ChatGPT changed `Mcp-Session-Id` between calls even within the same conversation. The ID
therefore could not serve as the stable per-conversation key the design required.

Versioned JSON configuration is required. Workspace, shell, output directory and child environment policy are explicit; see [configuration version 1](docs/configuration.md) for defaults, validation and the breaking changes from the old CLI.

A typical stdio MCP client configuration has this shape (the exact configuration format depends on the client):

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

## Execution behavior

`exec_command` invokes the configured shell with `-c` (without login profiles). It waits up to 10 seconds by default; if the command is still running, the response includes a `session_id` that can be passed to `wait_for_exit` or `write_stdin`.

Use `start_session` when state must persist between calls, such as an interactive shell or REPL. Use `wait_for_exit` when a process only needs more time. Use `write_stdin` for input, Ctrl-C, or output polling.

Child processes receive only the configured `child_env` allowlist and matching conditional rules, plus `CHATGPT_EXEC_SESSION`. Both pipe and PTY clear inherited environment. Credential values and host-specific runtime defaults belong in the deployment layer.

## Output behavior

Command output is captured to a managed raw-output store. If the output fits the response budget, it is returned in full. Oversized output returns a bounded head/tail projection and an `output_ref` describing the raw log so a client running in the same filesystem namespace can recover the omitted bytes.

The ordinary implicit display budget is approximately 8,000 tokens; recognized `rustc`, Gradle, and `cargo build/check/test` invocations use approximately 2,000 tokens. Token budgets are estimated at four bytes per token and are not tokenizer-accurate. An explicit `max_output_tokens` overrides the implicit budget.

For recognized builds, the projection can additionally preserve a few buried summary lines such as Cargo completion/test summaries or Gradle build status. Unknown or ambiguous shell commands use the generic output policy.

Raw output is captured in `raw.log` under `output_store_dir`. Only logs exposed through
`output_ref` are retained after a session ends. Inactive logs expire by mtime; all logs
share one byte capacity limit. See [configuration](docs/configuration.md#optional-limits).

Tool argument validation uses rmcp’s standard `isError: true` tool results; unknown
tool names return a JSON-RPC invalid-params error.

In stdio mode the server writes MCP JSON-RPC to stdout and diagnostics to stderr. In
Streamable HTTP mode MCP traffic is served only through the configured Unix socket.

## License

The original code in this repository is offered under the Apache License 2.0. Vendored OpenAI Codex code is also Apache-2.0 licensed and is documented in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

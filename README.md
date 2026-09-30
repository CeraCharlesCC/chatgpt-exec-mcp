# chatgpt-exec-mcp

An MCP server for running shell commands and exchanging messages between agents
through MCP Events. It supports stdio and Unix-socket Streamable HTTP.

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the vendored PTY utility.

## Tools

The MCP server exposes four execution tools:

| Tool | Purpose |
| --- | --- |
| `exec_command` | Run a shell command and return its exit code. If it is still running after the initial yield, return a session ID instead. |
| `start_session` | Start an explicitly stateful shell, REPL, or long-running process. Uses a PTY by default. |
| `write_stdin` | Send input, send Ctrl-C, or immediately poll output from a running session. |
| `wait_for_exit` | Wait for a running session to finish without waking on ordinary stdout/stderr activity. |

Session IDs are short memorable handles such as `amber-river`. The server enforces separate quotas and idle timeouts for explicit sessions and `exec_command` continuations.

With `events` configured, `pool_members(pool)` lists active members and
`pool_send(pool, agent, message)` queues a `multiagent.message` webhook. Subscribe
through `events/subscribe` with `pool` and a unique `agent` to join; refresh the
subscription before expiry and use `events/unsubscribe` to leave. `events/list`
describes the event. The reserved send target `global` reaches other active members
of the pool and cannot be subscribed as an agent name.

The sender is resolved from matching `_meta["openai/session"]` on subscription
and tool calls. When a subscription lacks that correlation value, `pool_send`
accepts `from_agent`. Successful sends confirm queueing, not ChatGPT activation.
Delivery uses Standard Webhooks signatures, HTTPS public callbacks, and up to six
attempts with bounded backoff; there is no replay cursor. Memberships and pending
deliveries persist in SQLite. Each instance is for one authenticated account;
deploy separate core, tunnel, socket and database for different accounts. See
[Events configuration](docs/configuration.md#events-and-account-scope).

## Build

Rust 1.88 or newer is required (edition 2024).

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

Or run HTTP with an existing socket parent directory:

```bash
./target/release/chatgpt-exec-mcp --config examples/minimal.json --listen-unix /absolute/path/mcp.sock
```

The HTTP endpoint `/mcp` advertises only MCP `2026-07-28`, requires modern
per-request metadata, and uses JSON responses without `Mcp-Session-Id`.
`/readyz` probes local readiness. Run the forwarding tunnel as the same Unix user
as core to access the `0600` socket. Separate core and tunnel services preserve
core-owned PTYs across tunnel restarts; restarting core ends PTYs. See
[HTTP configuration](docs/configuration.md#unix-streamable-http).

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

In stdio mode, the server writes MCP JSON-RPC to stdout and diagnostics to stderr.

## License

The original code in this repository is offered under the Apache License 2.0. Vendored OpenAI Codex code is also Apache-2.0 licensed and is documented in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

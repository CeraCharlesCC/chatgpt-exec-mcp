# chatgpt-exec-mcp

An MCP server for shell commands, stateful terminals, and messages between
cooperating agents. It supports stdio and Unix-socket Streamable HTTP.

## Run

Linux x86_64 binaries are available from [Releases](https://github.com/CeraCharlesCC/chatgpt-exec-mcp/releases).
Start with [examples/minimal.json](examples/minimal.json) and edit the workspace,
shell, output directory, and child environment for your deployment.

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

For HTTP, add `--listen-unix /absolute/path/mcp.sock`. The socket parent must
exist, and the forwarding tunnel must run as the same Unix user. HTTP clients
must support MCP `2026-07-28`. `/readyz` reports
local readiness. See [configuration](docs/configuration.md#unix-streamable-http).

For local debugging, `--webui-listen 127.0.0.1:19162` adds a loopback-only
WebUI with agent-pool administration and recent in-memory MCP/tool activity.
It requires `--listen-unix`; the MCP endpoint is not exposed on the WebUI port.

## Execution tools

| Tool | Purpose |
| --- | --- |
| `exec_command` | Run a shell command. Return its exit code when finished, or a session ID when it needs more time. |
| `start_session` | Start a stateful shell, REPL, or long-running process. Uses a PTY by default. |
| `write_stdin` | Send input or Ctrl-C, or immediately poll pending output with empty input. |
| `wait_for_exit` | Wait for completion without waking on ordinary stdout/stderr activity. |

`exec_command` runs the configured shell without login profiles and waits up to
10 seconds by default. Use `start_session` when variables or working-directory
changes must persist between calls. Configure the child environment through
[`child_env`](docs/configuration.md#child-environment).

Large outputs return a bounded excerpt and an `output_ref` for recovering the
full log from the same filesystem. The default display budget is approximately
8,000 tokens, or 2,000 for recognized Rust/Cargo/Gradle builds; set
`max_output_tokens` to override it. Build excerpts may preserve buried summary
lines. Referenced logs have configurable retention and capacity limits; see
[configuration](docs/configuration.md#optional-limits).

Structured execution results are sparse: empty `output`, false truncation or
encoding-loss flags, and unavailable state fields are omitted. A completed
command therefore usually needs only its `exit_code` (plus output when it has
any); a running command returns its `session_id`.

## Agent pools

With `agent_pool` configured, `pool_members` lists active agents and `pool_send`
sends to an agent or to `global` (all other members). The first send from a chat
to a pool automatically allocates an agent name from the configured dictionary;
later sends infer that name from the chat session.

```text
pool_send(pool="project", target="global", message="Joining the project")
pool_send(pool="project", target="Augustus", message="Tests passed")
pool_send(pool="project", operation="exit")
```

Requests must supply `_meta["openai/session"]` to identify the chat. Messages
arrive as `peer_messages` on the recipient's next tool call; they do not wake an
idle chat. The following call acknowledges the messages. The first send returns
`assigned_agent`; `pool_members` returns `self_agent` for a joined caller. Empty
recipient lists and other unused result fields are omitted. `operation="exit"`
removes only this chat's membership in that pool; otherwise membership expires
after inactivity (one day by default).

Run each instance behind a private, authenticated tunnel for one account.
Restarting only the tunnel preserves PTYs; restarting core ends PTYs but
preserves memberships and pending messages. See
[agent-pool configuration](docs/configuration.md#agent-pool-and-account-scope).

## Build and test

Rust 1.88 or newer is required.

```bash
cargo build --release --locked
cargo test --workspace --all-targets --locked
```

To build a standalone release binary:

```bash
cargo fetch --locked
python3 scripts/build-standalone.py --output /tmp/chatgpt-exec-mcp
```

Optional provenance verification:
`gh attestation verify <binary> --repo CeraCharlesCC/chatgpt-exec-mcp`.

## License

Apache License 2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE), and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for vendored OpenAI Codex code.

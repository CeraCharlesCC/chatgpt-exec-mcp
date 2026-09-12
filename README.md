# chatgpt-exec-mcp

An stdio MCP server for running shell commands

The server is intentionally small: it provides execution primitives, not an approval layer or a sandbox.

> [!WARNING]
> This server can execute arbitrary commands with the permissions and inherited environment of the server process. `--workspace` is a base directory for relative paths, **not** a security boundary; callers may use absolute paths, `..`, or shell commands that change directory. Run it only for trusted clients and put it inside an OS/container sandbox if you need filesystem, network, credential, or process isolation.

This project is not an official OpenAI or ChatGPT component. It vendors a small Apache-2.0-licensed PTY utility from OpenAI Codex; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Tools

The MCP server exposes four tools:

| Tool | Purpose |
| --- | --- |
| `exec_command` | Run a shell command and return its exit code. If it is still running after the initial yield, return a session ID instead. |
| `start_session` | Start an explicitly stateful shell, REPL, or long-running process. Uses a PTY by default. |
| `write_stdin` | Send input, send Ctrl-C, or immediately poll output from a running session. |
| `wait_for_exit` | Wait for a running session to finish without waking on ordinary stdout/stderr activity. |

Session IDs are short memorable handles such as `amber-river`. The server enforces separate quotas and idle timeouts for explicit sessions and `exec_command` continuations.

## Build

Rust 1.85 or newer is required (edition 2024).

```bash
cargo build --release --locked
cargo test --all-targets --locked
```

Run the server over stdio:

```bash
./target/release/chatgpt-exec-mcp --workspace /path/to/workspace
```

The default workspace is the server's current directory. The default shell is `/bin/bash`. Run `chatgpt-exec-mcp --help` for session limits, output-store limits, retention settings, and environment-variable overrides.

A typical stdio MCP client configuration has this shape (the exact configuration format depends on the client):

```json
{
  "mcpServers": {
    "exec": {
      "command": "/absolute/path/to/chatgpt-exec-mcp",
      "args": ["--workspace", "/path/to/workspace"]
    }
  }
}
```

## Execution behavior

`exec_command` invokes the configured shell with `-lc`. It waits up to 10 seconds by default; if the command is still running, the response includes a `session_id` that can be passed to `wait_for_exit` or `write_stdin`.

Use `start_session` when state must persist between calls, such as an interactive shell or REPL. Use `wait_for_exit` when a process only needs more time. Use `write_stdin` for input, Ctrl-C, or output polling.

Child processes inherit the server environment, with `CHATGPT_EXEC_SESSION` added for the running session. The generic server does not inject credentials or build-tool-specific environment defaults.

## Output behavior

Command output is captured to a managed raw-output store. If the output fits the response budget, it is returned in full. Oversized output returns a bounded head/tail projection and an `output_ref` describing the raw log so a client running in the same filesystem namespace can recover the omitted bytes.

The ordinary implicit display budget is approximately 8,000 tokens; recognized `rustc`, Gradle, and `cargo build/check/test` invocations use approximately 2,000 tokens. Token budgets are estimated at four bytes per token and are not tokenizer-accurate. An explicit `max_output_tokens` overrides the implicit budget.

For recognized builds, the projection can additionally preserve a few buried summary lines such as Cargo completion/test summaries or Gradle build status. Unknown or ambiguous shell commands use the generic output policy.

Raw output can contain secrets printed by child processes. By default artifacts live under `.chatgpt-exec-outputs` in the workspace and are retained subject to the configured time, byte, and file-count limits. Treat that directory as sensitive data.

## Security model

The MCP layer does **not** provide:

- command approval or allowlisting;
- filesystem confinement;
- network isolation;
- environment-variable filtering;
- per-command containers or namespaces;
- privilege dropping.

Those controls belong outside this process. A reasonable deployment runs the server as an unprivileged account with only the workspace, network access, and credentials the client actually needs.

The server writes MCP JSON-RPC only to stdout; diagnostics go to stderr.

## License

The original code in this repository is offered under the Apache License 2.0. Vendored OpenAI Codex code is also Apache-2.0 licensed and is documented in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

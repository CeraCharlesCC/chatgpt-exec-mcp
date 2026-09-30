# Configuration version 1

Run `chatgpt-exec-mcp --config config.json`; start from the
[minimal example](../examples/minimal.json). `--help` and `--version` also work
without a config. Legacy flags and `CHATGPT_EXEC_*` configuration variables are
rejected. `CHATGPT_EXEC_SESSION` is reserved runtime metadata.

## Required fields and paths

The JSON object requires `version: 1`, `workspace`, `shell`, `output_store_dir`,
and `child_env` (with explicit `inherit` and `rules` arrays, which may be empty).
Unknown fields, duplicate keys at any depth, null values, incorrect types,
unsupported versions and unreadable inputs are rejected before MCP starts.

`--config` is relative to the initial cwd. Paths **inside** the file are relative
to the parent of that supplied config path (including when the file is a symlink).
Workspace and shell must exist and be usable. Their canonical paths are resolved
at startup. Shell must be an executable regular file. Only the specified output
directory is created unless `agent_pool` is configured, which also creates its private
database directory; creation, locking or write failure aborts startup.

`instructions_file` is optional. When present, its nonempty path must refer to a
readable UTF-8 file. Its contents are appended to the core instructions. An empty
file is allowed; null and an empty path are errors. Content is frozen at startup.

MCP `workdir` is relative to the configured workspace. Only omission uses workspace;
an explicit empty string is rejected. Cwd must resolve to an existing directory.
Every spawn revalidates and canonicalizes it, then uses that same canonical cwd for
both rule matching and process creation. Absolute paths and `..` are accepted.

## Optional limits

Durations are integer seconds. Omitted fields use these defaults; invalid values
are rejected. Session and continuation quotas apply separately.

| Field | Default | Allowed inclusive range |
| --- | ---: | ---: |
| `max_explicit_sessions` | 3 | 1–1024 |
| `max_exec_continuations` | 3 | 1–1024 |
| `explicit_session_idle_timeout` | 3600 | 1–31536000 |
| `exec_continuation_idle_timeout` | 900 | 1–31536000 |
| `reaper_interval` | 30 | 1–86400 |
| `output_cap_bytes` | 1048576 | 1–1073741824 |
| `output_store_retention` | 604800 | 1–31536000 |
| `output_store_max_bytes` | 4294967296 | 1–1125899906842624 |

Raw output is captured in `raw.log`. After a session ends, only logs whose
`output_ref` was returned are retained. GC deletes inactive logs when their mtime
is older than `output_store_retention`; live sessions are protected. Finishing a
capture refreshes its mtime. After a crash, leftover logs expire by the same rule.
`output_store_max_bytes` is the single capacity limit for all raw logs, including
active captures. If it is exhausted, capture stops and reports an incomplete
output reference to the stored prefix. There is no early eviction of unexpired logs.

## Child environment

The process starts with an empty environment. `inherit` copies only named parent
values, and matching rules copy a source value to a destination name:

```json
{
  "inherit": ["HOME", "PATH", "LANG"],
  "rules": [{
    "tool": "exec_command",
    "workdir_under": "/path/to/project",
    "set_from_env": {"BUILD_REGISTRY_TOKEN": "MCP_PRIVATE_REGISTRY_TOKEN"}
  }]
}
```

Every inherited value and rule source must be present, nonempty UTF-8 without
NUL at startup, including rules that have not matched any calls yet. Values are
snapshotted; changes require restart.

Names follow `[A-Za-z_][A-Za-z0-9_]*`. Inherited, source and destination name sets
must be disjoint. Duplicate inherit names, duplicate destinations across rules,
reserved names, unknown tool names and empty rule mappings are errors. A source
can feed distinct destinations, but it is never exported under its source name.

Rules support `exec_command` and `start_session`, matched by the actual tool origin,
independently of PTY selection. Roots must exist and are canonicalized at startup.
A rule matches when the canonical spawn cwd is the root or a component-wise child
of it. Symlink escapes and paths sharing only a string prefix do not match.
Matching uses the initial cwd; a `cd` within the command does not change it.

Core adds `CHATGPT_EXEC_SESSION` to each child. Both pipe and PTY use this policy.
Commands run as `shell -c`. Set HOME/PATH/XDG in the launcher and list the values
you need in `inherit`; login profiles are not loaded.

## Agent pool and account scope

The optional agent_pool object enables pool_members and pool_send. database_path and principal
are required and unknown fields are rejected. Example:

    {
      "agent_pool": {
        "database_path": "/private/state/chatgpt-exec-mcp/agent-pool.sqlite3",
        "principal": "chatgpt-owner",
        "membership_ttl_seconds": 86400
      }
    }

database_path is resolved relative to the config directory like other paths.
The database and its parent directory must be owner-only (0600 and 0700);
missing directories are created with private permissions. Symlink database
files and permissive database files/directories are rejected. Keep the SQLite
database and its WAL/SHM files outside release directories so memberships and
pending inbox rows survive core restart and deployment rollback.

membership_ttl_seconds is optional and defaults to 86400 (one day). It accepts
180 through 604800 seconds and is a deployment setting, not a model-facing control.
The three-minute minimum is longer than any single blocking execution-tool wait,
so active tool calls cannot outlive a freshly renewed lease.

principal is a fixed account scope (nonempty, at most 128 bytes, no control
characters or surrounding whitespace). The server trusts its deployment behind
an authenticated, private, single-account-only tunnel; request headers and
request metadata do not authenticate or select a principal. Do not share a
connector backed by this instance with other accounts. Deploy a separate core,
tunnel, socket, and database for every account.

Without agent_pool, pool tools are unavailable. A session joins a pool on its
first pool_send, which requires from_agent and _meta["openai/session"]. The
server atomically claims the agent name and binds it to that session. Later
sends infer the sender; supplying a different from_agent is rejected. Agent
names are unique among active members of a pool, and global is reserved as the
broadcast target.

Membership is a configurable inactivity lease (one day by default). Any ordinary tool call from
the bound session refreshes all of that session's memberships. Expired members
and their undelivered inbox rows are deleted automatically. Pool and agent names
contain 1-128 characters with no control characters or surrounding whitespace.
Message text is 1-65536 bytes. The instance-wide inbox is bounded to 10,000
pending member deliveries.

Messages are durable SQLite rows, not webhooks. Every model-callable execution
tool piggybacks a bounded batch of unread messages in peer_messages when the
current openai/session has memberships. The response text also contains a
conspicuous peer-message block. The highest offered inbox cursor is retained;
the same session's next tool call acknowledges that offer before collecting the
next batch. If no later tool call happens, the offered rows remain recoverable
until membership expiry. Cursor transitions are serialized per session so
parallel calls cannot acknowledge offers out of order. Correlation is not MCP
transport session management or account authentication.

## Unix Streamable HTTP

Use `--listen-unix /absolute/path/mcp.sock` with `--config` to serve `/mcp` over
Streamable HTTP instead of stdio. The parent must already exist. The socket is
`0600`, so run the forwarding tunnel as the same Unix user. Active sockets and
non-socket paths are refused; stale sockets are removed and a graceful shutdown
removes only the socket created by this process. `/readyz` is a local readiness
probe.

Production HTTP advertises only MCP `2026-07-28`. Every request needs
`MCP-Protocol-Version: 2026-07-28`, `Mcp-Method` matching its JSON-RPC method,
`Mcp-Name` matching the tool name for `tools/call`, and the protocol's per-request
`_meta` (protocolVersion, clientInfo and
clientCapabilities under the `io.modelcontextprotocol/` namespace). Ordinary
responses are JSON. The endpoint has no `Mcp-Session-Id` dependency.

Run core and tunnel as separate processes/services. The shared process manager
keeps PTYs alive across tunnel reconnects and restarts. Restarting core ends PTYs;
durable agent-pool memberships and pending inbox rows are recovered from SQLite.

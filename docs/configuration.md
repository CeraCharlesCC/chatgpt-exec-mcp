# Configuration version 1

Start from the [minimal example](../examples/minimal.json) and run
`chatgpt-exec-mcp --config config.json`. Configuration and environment changes
require a restart.

## Required fields and paths

| Field | Value |
| --- | --- |
| `version` | `1` |
| `workspace` | Existing directory used as the default working directory. |
| `shell` | Executable shell file. Commands run without login profiles. |
| `output_store_dir` | Writable directory for command logs; created if missing. |
| `child_env` | Object containing `inherit` and `rules` arrays; both may be empty. |

Paths in the configuration are relative to the config file's directory.
`workspace` sets the default working directory; it does not restrict filesystem
access. Use an external sandbox when access restrictions are needed.

Optional `instructions_file` points to a UTF-8 file whose contents are appended
to the server's instructions. Use only the documented fields, and omit unused
optional fields rather than setting them to null.

## Optional limits

Durations are integer seconds. Omitted fields use these defaults.
Session and command-continuation quotas apply separately.

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

`output_cap_bytes` limits each displayed response. Logs exposed through
`output_ref` are retained for `output_store_retention` seconds while inactive;
unreferenced logs are removed when their session ends.
`output_store_max_bytes` limits total stored output, including active captures.
When full, capture stops and reports incomplete output; unexpired logs are kept.

## Child environment

Use `inherit` to pass parent variables to children and `rules` to supply values
conditionally. Include `HOME`, `PATH`, or other variables your commands need.

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

A rule supports `exec_command` or `start_session` and applies when the command
starts in `workdir_under` or a subdirectory. The directory must exist. Symlinks
are resolved before matching; a later `cd` does not change which values apply.
`set_from_env` maps child variable names to parent variable names.

All inherited and rule-source values must be present and nonempty at startup,
even for rules that have not matched a command. Variable names must follow
`[A-Za-z_][A-Za-z0-9_]*`. Inherited, source, and destination names must not overlap;
inherited and destination names must be unique. Names starting with
`CHATGPT_EXEC_` are reserved. Rule mappings must not be empty.

## Agent pool and account scope

The optional `agent_pool` object enables [agent messaging](../README.md#agent-pools).
It requires `database_path` and `principal`.

```json
{
  "agent_pool": {
    "database_path": "/private/state/chatgpt-exec-mcp/agent-pool.sqlite3",
    "principal": "chatgpt-owner",
    "membership_ttl_seconds": 86400
  }
}
```

Keep the database outside release directories so updates and rollbacks preserve
memberships and pending messages. Its parent directory and database must be
owner-only (`0700` and `0600`); missing directories are created automatically.
The database file must not be a symlink.

`principal` identifies the owning account: use 1–128 bytes without control
characters or surrounding whitespace. It does not provide authentication.
Run each instance behind a private, authenticated tunnel for one account;
use a separate instance for each account and do not share its connector.

`membership_ttl_seconds` defaults to 86400 (one day) and accepts 180–604800.
Tool activity renews membership. Inactive memberships and their pending messages
are removed after this interval.

## Unix Streamable HTTP

Add `--listen-unix /absolute/path/mcp.sock` to serve HTTP. The socket's parent
directory must already exist; run the forwarding tunnel as the same Unix user
as core to access the socket.

Connect an MCP `2026-07-28` compatible client or tunnel to `/mcp`.
Use `/readyz` to check local readiness.

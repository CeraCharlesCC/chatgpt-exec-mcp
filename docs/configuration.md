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
directory is created; creation, locking or write failure aborts startup.

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

# Configuration version 1

Start with `chatgpt-exec-mcp --config /absolute/path/to/config.json`. `--config`
is required; only `--help` and `--version` work without it. There are no individual
setting flags, environment overrides, default config search, or compatibility
reader. Obsolete `CHATGPT_EXEC_*` inputs cause startup failure even when empty.
The sole exception is `CHATGPT_EXEC_SESSION`, runtime metadata replaced for each
spawn. All of that namespace is reserved and cannot appear in `child_env`.

Use [the minimal example](../examples/minimal.json) for standalone use. It needs
no private files or credentials. Its paths refer to the checkout containing it.
Copy and edit its explicit paths for another workspace.

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
both policy and process creation. This permits absolute paths and `..`; it does
not confine filesystem access.

## Optional limits

All durations are integer seconds. Omission uses the fixed values below. Explicit
invalid values are errors and are never clamped. Quotas apply separately.

| Field | Default | Allowed inclusive range |
| --- | ---: | ---: |
| `max_explicit_sessions` | 3 | 1–1024 |
| `max_exec_continuations` | 3 | 1–1024 |
| `explicit_session_idle_timeout` | 3600 | 1–31536000 |
| `exec_continuation_idle_timeout` | 900 | 1–31536000 |
| `reaper_interval` | 30 | 1–86400 |
| `output_cap_bytes` | 1048576 | 1–1073741824 |
| `output_store_retention` | 604800 | 1–31536000 |
| `output_store_min_retention` | 3600 | 1–31536000 |
| `output_store_max_bytes` | 4294967296 | 1–1125899906842624 |
| `output_store_headroom_bytes` | 16777216 | 0–`output_store_max_bytes` |
| `output_store_max_files` | 2048 | 1–1000000 |

Minimum retention must not exceed retention. Headroom is reserved *in addition*
to ordinary storage capacity, matching the output store's recovery semantics;
version 1 bounds that reserve to at most ordinary capacity.

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
snapshotted; changes require restart. Values are redacted from Config Debug and
configuration errors. Avoid printing credentials in child commands: captured
command output is deliberately not redacted.

Names follow `[A-Za-z_][A-Za-z0-9_]*`. Inherited, source and destination name sets
must be disjoint. Duplicate inherit names, duplicate destinations across rules,
reserved names, unknown tool names and empty rule mappings are errors. A source
can feed distinct destinations, but it is never exported under its source name.
There are no rule priorities, optional credentials or unrestricted inheritance.

Rules support `exec_command` and `start_session`, matched by the actual tool origin,
independently of PTY selection. Roots must exist and are canonicalized at startup.
A rule matches when the canonical spawn cwd is the root or a component-wise child
of it. Symlink escapes and paths sharing only a string prefix do not match.
Commands and their internal `cd` are not parsed. A normal nonmatch receives no
rule values; it cannot resurrect a destination value from the parent.

Core adds only `CHATGPT_EXEC_SESSION`. The pipe and PTY backends both clear their
inherited environment. The shell/runtime may create its own variables such as
`PWD`, `SHLVL`, or `PATH`; this is distinct from copying a parent environment.

Commands now run as `shell -c`, **without the previous login-shell `-l`**. Configure
HOME/PATH/XDG in the launcher and explicitly allow them. Do not rely on login
profiles to initialize them. A caller can still launch an interactive/login shell
or source files; those are separate filesystem and sandbox boundaries. This policy
controls initial distribution, not reading, movement or redistribution afterward.

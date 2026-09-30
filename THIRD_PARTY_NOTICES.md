# Third-party notices

## OpenAI Codex `codex-utils-pty`

This repository vendors `codex-rs/utils/pty` from the OpenAI Codex repository.

- Upstream: <https://github.com/openai/codex>
- Commit: `6478a751fde8884b2fdc76486fe23175a8e795d4`
- License: Apache License 2.0
- Local path: `vendor/codex-utils-pty`

The package manifest and tests are adapted to this Cargo workspace rather than
inheriting metadata and dependencies from the larger Codex workspace.
The vendored source also contains local changes that surface output-reader and
transport-lag failures through the process handle instead of silently treating
them as normal end-of-output. In particular, the changes touch `pipe.rs`,
`process.rs`, `pty.rs`, and `unix_io.rs`.

The upstream Codex NOTICE identifies:

> OpenAI Codex
>
> Copyright 2025 OpenAI

The full Apache License 2.0 text is included in `LICENSE`.

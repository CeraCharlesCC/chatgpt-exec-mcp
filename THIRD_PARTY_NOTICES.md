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

## Prism.js

The WebUI embeds Prism.js 1.30.0 core and the JSON and Bash grammars from the
upstream minified components. The ES module uses a module-local initialization
scope instead of a global `window.Prism` binding. Automatic highlighting remains
disabled; the WebUI highlights only explicit code blocks and supplies its own
theme.

- Upstream: <https://github.com/PrismJS/prism/tree/v1.30.0>
- Components: `components/prism-core.min.js`, `components/prism-json.min.js`,
  `components/prism-bash.min.js`
- License: MIT, Copyright (c) 2012 Lea Verou
- Local bundle: `src/webui/vendor/prism.js` (exported as an ES module)
- Full license: `vendor/prism/LICENSE`, also included in the embedded bundle

To update the bundle, replace the three labeled component sections in
`src/webui/vendor/prism.js` with the corresponding upstream files, retaining
the license and adapting the core's `_self` initializer to the existing
module-local scope with `Prism.manual` set to `true`. No CDN or build-time
download is needed.

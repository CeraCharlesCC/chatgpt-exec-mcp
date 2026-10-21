# Third-party notices

## OpenAI Codex `codex-utils-pty`

This repository vendors `codex-rs/utils/pty` from the OpenAI Codex repository.

- Upstream: <https://github.com/openai/codex>
- Commit: `6478a751fde8884b2fdc76486fe23175a8e795d4`
- License: Apache License 2.0
- Local path: `vendor/codex-utils-pty`

The package manifest is adapted to this Cargo workspace. The vendored source
includes local changes to process error reporting.

The upstream Codex NOTICE identifies:

> OpenAI Codex
>
> Copyright 2025 OpenAI

The full Apache License 2.0 text is included in `LICENSE`.

## Prism.js

The WebUI embeds Prism.js 1.30.0 core and the JSON and Bash grammars as a local
ES module.

- Upstream: <https://github.com/PrismJS/prism/tree/v1.30.0>
- Components: `components/prism-core.min.js`, `components/prism-json.min.js`,
  `components/prism-bash.min.js`
- License: MIT, Copyright (c) 2012 Lea Verou
- Local bundle: `src/webui/vendor/prism.js` (exported as an ES module)
- Full license: `vendor/prism/LICENSE`, also included in the embedded bundle

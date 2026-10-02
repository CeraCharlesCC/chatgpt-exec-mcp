## Development setup

Rust 1.88 or newer is required. Clone the repository and run:

```bash
cargo build --locked
cargo test --workspace --all-targets --locked
```

Before opening a pull request, also run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

CI uses the locked dependency graph and runs the Rust tests and Clippy checks.

## Browser tests

The WebUI regression tests require Node.js 20+ and Playwright:

```bash
cd tests/webui
npm ci
npx playwright install chromium
npm test
```

To use an existing Chrome installation instead of downloading Chromium:

```bash
WEBUI_BROWSER=/absolute/path/to/chrome npm test
```

## Release artifact

To reproduce the standalone binary assembled by the release workflow:

```bash
cargo fetch --locked
python3 scripts/build-standalone.py --output /tmp/chatgpt-exec-mcp
```

Published binaries can be checked with GitHub's attestation tooling:

```bash
gh attestation verify <binary> --repo CeraCharlesCC/chatgpt-exec-mcp
```

Tagged releases are published by the GitHub Actions release workflow; normal
contributions do not need to create release artifacts manually.

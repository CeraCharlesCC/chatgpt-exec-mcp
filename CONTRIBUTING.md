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

CI runs on pull requests and pushes to main. It tests, lints, and builds the
standalone binary using the locked dependency graph.

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

The release workflow runs when a v* tag is pushed. The tagged commit must be in
main history; the workflow then tests, builds, attests, and publishes the binary.
Normal contributions do not need to create release artifacts manually.

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

`npm test` runs both the Playwright UI regressions (`ui.test.cjs`) and the
browser-independent conversation model tests (`model.test.cjs`). To run only
the model tests, use `npm run test:model`. From the repository root, run
`cargo test --test webui_http --locked` for HTTP routing, asset, and admin API
coverage. The Rust and browser suites are complementary: run both when changing
the WebUI.

## WebUI development

The WebUI uses browser-native ES modules in `src/webui/`, with markup in
`src/webui.html` and styles in `src/webui/styles.css`. These assets are embedded
in the Rust binary by `src/webui.rs`; no frontend build step is required.

Keep application coordination in `app.js`, rendering in `views/`, and
browser-independent logic in `model/`. When changing polling or rendering,
preserve user interactions such as scrolling and text selection, and guard
against stale responses restoring cleared history.

When adding an asset, register it in `src/webui.rs` and the Playwright HTTP
fixture in `tests/webui/ui.test.cjs`. Cover UI changes with browser tests and
model changes with Node tests, using the commands above.

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

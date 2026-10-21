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

For a shorter local feedback loop, select the affected unit module or integration
target before running the full suite:

```bash
cargo test --lib agent_pool --locked
cargo test --lib process_manager --locked
cargo test --test mcp_stdio --locked
cargo test --test webui_http --locked
```

Keep private unit tests in child `tests` modules beside their implementation.
Split integration scenarios into child modules of the existing test target when
they grow; this preserves one Cargo target and shared setup. Share small fixture
helpers, and keep scenario assertions and synchronization visible in the tests.
Focused runs complement the full workspace suite required above.

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

`npm test` runs the Playwright UI regressions (`ui.test.cjs`) and the
browser-independent model and controller tests. Use `npm run test:unit` for
all Node unit tests, `npm run test:model` for models only, or
`npm run test:controllers` for pagination and request invalidation tests.
From the repository root, run
`cargo test --test webui_http --locked` for HTTP routing, asset, and admin API
coverage. The Rust and browser suites are complementary: run both when changing
the WebUI.

## WebUI development

The WebUI uses browser-native ES modules in `src/webui/`, with markup in
`src/webui.html` and styles in `src/webui/styles.css`. These assets are embedded
in the Rust binary by `src/webui.rs`; no frontend build step is required.

Keep application coordination in `app.js`, rendering in `views/`, request and
pagination state in `controllers/`, and pure event projections in `model/`.
When changing polling or rendering,
preserve user interactions such as scrolling and text selection, and guard
against stale responses restoring cleared history.

When adding an asset, register its relative path and content type in
`src/webui/assets.rs`. Cover UI changes with browser tests and model/controller
changes with Node tests, using the commands above.

The WebUI also includes a vendored Prism.js module in `src/webui/vendor/prism.js`.
Keep [third-party notices](THIRD_PARTY_NOTICES.md) in sync when updating vendored assets.

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

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use chatgpt_exec_mcp::activity::ActivityHub;
use chatgpt_exec_mcp::agent_pool::{AgentPoolStore, PoolSendArgs};
use chatgpt_exec_mcp::webui::{WebUiState, router};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::oneshot;

struct Dashboard {
    client: Client,
    url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Dashboard {
    async fn start(state: WebUiState) -> Self {
        Self::start_router(router(state)).await
    }

    async fn start_router(app: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stopped) = oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            url,
            shutdown: Some(shutdown),
        }
    }

    async fn snapshot(&self) -> Value {
        self.snapshot_after(None).await
    }

    async fn snapshot_after(&self, cursor: Option<&str>) -> Value {
        let mut request = self
            .client
            .get(format!("{}/_admin/api/snapshot", self.url))
            .query(&[("all", "false")]);
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = request.send().await.unwrap();
        if response.status() != StatusCode::OK {
            panic!(
                "snapshot request failed: {}",
                response.text().await.unwrap()
            );
        }
        assert_eq!(response.headers()["content-type"], "application/json");
        response.json().await.unwrap()
    }

    async fn full_snapshot(&self) -> Value {
        let response = self
            .client
            .get(format!("{}/_admin/api/snapshot", self.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        response.json().await.unwrap()
    }

    async fn events_page(&self, before_id: Option<u64>, limit: Option<usize>) -> Value {
        let mut query = Vec::new();
        if let Some(before_id) = before_id {
            query.push(("before_id", before_id.to_string()));
        }
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        let response = self
            .client
            .get(format!("{}/_admin/api/events", self.url))
            .query(&query)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        response.json().await.unwrap()
    }

    async fn post(&self, endpoint: &str, body: Value, status: StatusCode) -> Value {
        let token = self.snapshot().await["csrf_token"]
            .as_str()
            .unwrap()
            .to_owned();
        let response = self
            .client
            .post(format!("{}/_admin/api/{endpoint}", self.url))
            .header("origin", &self.url)
            .header("x-webui-csrf-token", token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let text = response.text().await.unwrap();
        if status == StatusCode::OK {
            serde_json::from_str(&text).unwrap()
        } else {
            json!(text)
        }
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn send_args(target: Option<&str>, message: Option<&str>) -> PoolSendArgs {
    PoolSendArgs {
        pool: "team".into(),
        target: target.map(str::to_owned),
        message: message.map(str::to_owned),
        in_reply_to: None,
        action: None,
    }
}

const ASSET_PREFIX: &str = "/_admin/assets/";

fn html_asset_references(html: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for attribute in ["href=\"", "src=\"", "href='", "src='"] {
        let quote = attribute.chars().last().unwrap();
        let mut remaining = html;
        while let Some((_, tail)) = remaining.split_once(attribute) {
            let (value, rest) = tail
                .split_once(quote)
                .expect("HTML attributes must be quoted");
            assert!(
                !value.starts_with("https:")
                    && !value.starts_with("http:")
                    && !value.starts_with("//"),
                "WebUI must not load a CDN or external browser asset: {value}"
            );
            if value.starts_with(ASSET_PREFIX) {
                paths.insert(value.to_owned());
            }
            remaining = rest;
        }
    }
    paths
}

fn source_asset_path(uri: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/webui")
        .join(uri.strip_prefix(ASSET_PREFIX).unwrap())
}

fn module_imports(source: &str) -> Vec<String> {
    let mut imports = Vec::new();
    for line in source.lines().map(str::trim) {
        if !(line.starts_with("import ")
            || line.starts_with("export ")
            || line.starts_with("} from "))
        {
            continue;
        }
        for marker in [" from '", " from \"", "import '", "import \""] {
            if let Some((_, rest)) = line.split_once(marker) {
                let quote = marker.chars().last().unwrap();
                let (specifier, _) = rest
                    .split_once(quote)
                    .expect("module import must be quoted");
                imports.push(specifier.to_owned());
                break;
            }
        }
    }
    imports
}

fn resolve_asset_import(parent: &str, specifier: &str) -> String {
    assert!(
        specifier.starts_with("./") || specifier.starts_with("../"),
        "ES modules must use relative, embedded imports: {specifier}"
    );
    let mut parts: Vec<String> = parent
        .trim_start_matches('/')
        .split('/')
        .map(str::to_owned)
        .collect();
    parts.pop();
    for segment in specifier.split('/') {
        match segment {
            "." => {}
            ".." => {
                assert!(
                    parts.len() > 2,
                    "module import escapes asset root: {specifier}"
                );
                parts.pop();
            }
            "" => panic!("empty module import segment in {specifier}"),
            _ => parts.push(segment.to_owned()),
        }
    }
    let resolved = format!("/{}", parts.join("/"));
    assert!(
        resolved.starts_with(ASSET_PREFIX),
        "import escapes asset root: {resolved}"
    );
    resolved
}

async fn assert_embedded_assets(server: &Dashboard) {
    let page = server.client.get(&server.url).send().await.unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(page.headers()["content-type"], "text/html; charset=utf-8");
    let html = page.text().await.unwrap();
    let references = html_asset_references(&html);
    assert!(references.contains("/_admin/assets/styles.css"));
    assert!(references.contains("/_admin/assets/app.js"));

    let mut pending: VecDeque<String> = references.into_iter().collect();
    let mut seen = BTreeSet::new();
    while let Some(uri) = pending.pop_front() {
        if !seen.insert(uri.clone()) {
            continue;
        }
        assert!(
            source_asset_path(&uri).is_file(),
            "asset reference lacks a source file: {uri}"
        );
        let response = server
            .client
            .get(format!("{}{uri}", server.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let mime = if uri.ends_with(".css") {
            "text/css; charset=utf-8"
        } else {
            "text/javascript; charset=utf-8"
        };
        assert_eq!(response.headers()["content-type"], mime, "{uri}");
        assert_eq!(response.headers()["cache-control"], "no-store", "{uri}");
        assert_eq!(
            response.headers()["x-content-type-options"],
            "nosniff",
            "{uri}"
        );
        let body = response.text().await.unwrap();
        assert_eq!(
            body,
            std::fs::read_to_string(source_asset_path(&uri)).unwrap(),
            "{uri}"
        );
        if uri.ends_with(".js") {
            for specifier in module_imports(&body) {
                pending.push_back(resolve_asset_import(&uri, &specifier));
            }
        }
    }
    // Check the reachable asset graph, not every source file. An unused module
    // must not break an HTTP behavior test before it is wired into the UI.

    for unknown in [
        "/_admin/assets/unknown.js",
        "/_admin/assets/views/missing.js",
        "/_admin/assets/vendor/prism.css",
        "/_admin/assets/package.json",
    ] {
        let response = server
            .client
            .get(format!("{}{unknown}", server.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{unknown}");
    }
}

#[tokio::test]
async fn embedded_assets_and_admin_api_work_on_standalone_and_merged_routers() {
    for merged in [false, true] {
        let state = WebUiState::without_pool(Arc::new(ActivityHub::new(10)));
        let app = if merged {
            // Match transport.rs: the WebUI router is merged with the main MCP routes.
            Router::new()
                .route("/readyz", get(|| async { "ready" }))
                .route("/mcp", get(|| async { "mcp" }))
                .merge(router(state))
        } else {
            // Match the separately bound loopback WebUI listener.
            router(state)
        };
        let server = Dashboard::start_router(app).await;
        assert_embedded_assets(&server).await;

        if merged {
            let ready = server
                .client
                .get(format!("{}/readyz", server.url))
                .send()
                .await
                .unwrap();
            assert_eq!(ready.text().await.unwrap(), "ready");
            let mcp = server
                .client
                .get(format!("{}/mcp", server.url))
                .send()
                .await
                .unwrap();
            assert_eq!(mcp.text().await.unwrap(), "mcp");
        }
        let snapshot = server.snapshot().await;
        assert_eq!(snapshot["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(snapshot["pools"], json!([]));
        assert_eq!(
            server
                .post(
                    "send",
                    json!({"pool":"example","target":"global","message":"hello"}),
                    StatusCode::SERVICE_UNAVAILABLE,
                )
                .await,
            "agent pool is not configured"
        );
        assert_eq!(
            server.post("clear", json!({}), StatusCode::OK).await,
            json!({"cleared_events":0,"cleared_admin_messages":0})
        );
    }
}

#[tokio::test]
async fn disabled_pool_snapshot_keeps_activity_and_rejects_controls() {
    let activity = Arc::new(ActivityHub::new(10));
    activity.emit(
        "tool.finish",
        "info",
        Some("session"),
        vec![],
        json!({"output": "ready"}),
    );
    let server = Dashboard::start(WebUiState::without_pool(activity.clone())).await;
    let snapshot = server.snapshot().await;
    assert_eq!(snapshot["version"], env!("CARGO_PKG_VERSION"));
    assert!(snapshot["generated_ms"].as_i64().unwrap() > 0);
    assert!(snapshot["uptime_seconds"].is_u64());
    assert_eq!(snapshot["pools"], json!([]));
    assert_eq!(snapshot["admin_messages"], json!([]));
    assert_eq!(snapshot["events"][0]["detail"]["output"], "ready");
    for (endpoint, body) in [
        (
            "send",
            json!({"pool":"team", "target":"Alice", "message":"hello"}),
        ),
        ("terminate", json!({"pool":"team", "agent":"Alice"})),
        ("ack", json!({"message_id":"msg_example"})),
    ] {
        assert_eq!(
            server
                .post(endpoint, body, StatusCode::SERVICE_UNAVAILABLE)
                .await,
            "agent pool is not configured"
        );
    }
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await,
        json!({"cleared_events":1, "cleared_admin_messages":0})
    );
    assert_eq!(server.snapshot().await["events"], json!([]));
    let next_id = activity.emit("tool.start", "info", None, vec![], json!({}));
    assert_eq!(next_id, 2, "clearing must not reuse event IDs");
}

#[tokio::test]
async fn activity_history_paginates_across_archive_and_memory_without_gaps() {
    let activity = Arc::new(ActivityHub::new(500));
    for id in 1..=2635 {
        assert_eq!(
            activity.emit("tool.finish", "info", None, vec![], json!({"sequence": id})),
            id
        );
        // Mimic sustained activity rather than an instantaneous producer that
        // intentionally overflows the archive's bounded pending-task queue.
        if id % 250 == 0 {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    let server = Dashboard::start(WebUiState::without_pool(activity.clone())).await;
    let snapshot = server.snapshot().await;
    let recent = snapshot["events"].as_array().unwrap();
    assert_eq!(
        recent.len(),
        200,
        "snapshots must never send over 200 events"
    );
    assert_eq!(recent[0]["id"], 2436);
    assert_eq!(recent[199]["id"], 2635);
    assert_eq!(snapshot["events_has_more"], true);
    assert_eq!(snapshot["events_next_before_id"], 2436);

    // The independent history endpoint must support a configurable page size,
    // and even its first page must not return more than 200 rows.
    let latest = server.events_page(None, None).await;
    assert_eq!(latest["events"].as_array().unwrap().len(), 200);
    let small = server.events_page(Some(2436), Some(5)).await;
    let small_ids: Vec<u64> = small["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["id"].as_u64().unwrap())
        .collect();
    assert_eq!(small_ids, [2431, 2432, 2433, 2434, 2435]);
    assert_eq!(small["next_before_id"], 2431);
    assert_eq!(small["has_more"], true);

    let mut newest_to_oldest = recent
        .iter()
        .rev()
        .map(|event| event["id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    let mut before_id = snapshot["events_next_before_id"].as_u64();
    let mut page_count = 0;
    while let Some(cursor) = before_id {
        page_count += 1;
        assert!(page_count <= 15, "pagination cursor did not terminate");
        let page = server.events_page(Some(cursor), None).await;
        let ids = page["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["id"].as_u64().unwrap())
            .collect::<Vec<_>>();
        assert!(!ids.is_empty());
        assert!(ids.len() <= 200);
        assert!(ids.iter().all(|&id| id < cursor));
        assert!(ids.windows(2).all(|pair| pair[0] + 1 == pair[1]));
        if page["has_more"] == true {
            assert_eq!(page["next_before_id"], ids[0]);
            before_id = Some(ids[0]);
        } else {
            assert!(page["next_before_id"].is_null());
            before_id = None;
        }
        newest_to_oldest.extend(ids.into_iter().rev());
    }

    assert!(page_count >= 11, "older compressed chunks were not exposed");
    assert!((2250..=2500).contains(&newest_to_oldest.len()));
    assert_eq!(newest_to_oldest[0], 2635);
    assert!(
        newest_to_oldest
            .windows(2)
            .all(|pair| pair[0] == pair[1] + 1)
    );

    // The default snapshot includes the entire retained suffix, decoding zstd
    // chunks as needed, with each event appearing exactly once in ID order.
    let full = server.full_snapshot().await;
    let all_ids: Vec<_> = full["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["id"].as_u64().unwrap())
        .collect();
    assert_eq!(
        all_ids,
        newest_to_oldest.iter().rev().copied().collect::<Vec<_>>()
    );
    assert_eq!(full["events_has_more"], false);
    assert!(full["events_next_before_id"].is_null());
    assert_eq!(full["correlated_events"], json!([]));

    // CLEAR must invalidate both the RAM page and any pending/finished archive.
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await["cleared_events"],
        newest_to_oldest.len()
    );
    assert_eq!(server.snapshot().await["events"], json!([]));
    assert_eq!(server.events_page(None, None).await["events"], json!([]));
    assert_eq!(server.full_snapshot().await["events"], json!([]));
    assert_eq!(
        activity.emit("tool.start", "info", None, vec![], json!({})),
        2636
    );
    assert_eq!(
        server.events_page(None, None).await["events"][0]["id"],
        2636
    );
}

#[tokio::test]
async fn activity_http_pages_return_only_missing_counterparts_and_clear_them() {
    let activity = Arc::new(ActivityHub::new(500));
    let start = activity.emit(
        "tool.start",
        "info",
        Some("session"),
        vec![],
        json!({"tool":"pool_send", "arguments":{"pool":"team", "target":"bob", "message":"hi"}}),
    );
    for _ in 0..498 {
        activity.emit("noise", "info", None, vec![], json!({}));
    }
    let finish = activity.emit(
        "tool.finish",
        "info",
        Some("session"),
        vec![],
        json!({"tool":"pool_send", "start_event_id":start,
            "result":{"value":{"assigned_agent":"alice", "message_id":"m1", "recipients":["bob"]}}}),
    );
    let server = Dashboard::start(WebUiState::without_pool(activity)).await;

    let snapshot = server.snapshot().await;
    assert_eq!(snapshot["events"].as_array().unwrap().len(), 200);
    assert_eq!(snapshot["events"][199]["id"], finish);
    assert_eq!(snapshot["correlated_events"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["correlated_events"][0]["id"], start);
    assert_eq!(snapshot["correlated_events"][0]["schema_version"], 1);
    assert_eq!(
        snapshot["correlated_events"][0]["lifecycle"]["type"],
        "message_send"
    );
    assert_eq!(
        snapshot["events"][199]["lifecycle"]["message"]["status"],
        "queued"
    );
    assert_eq!(
        snapshot["events"][199]["lifecycle"]["message"]["body"],
        "hi"
    );
    assert_eq!(
        snapshot["events"][199]["lifecycle"]["message"]["from"],
        "alice"
    );

    // A one-row page obtains one missing counterpart; links never pollute
    // normal events, inflate the cursor, or expand into full-history loads.
    let latest = server.events_page(None, Some(1)).await;
    assert_eq!(latest["events"].as_array().unwrap().len(), 1);
    assert_eq!(latest["events"][0]["id"], finish);
    assert_eq!(latest["next_before_id"], finish);
    assert_eq!(latest["correlated_events"].as_array().unwrap().len(), 1);
    assert_eq!(latest["correlated_events"][0]["id"], start);

    let oldest = server.events_page(Some(start + 1), Some(1)).await;
    assert_eq!(oldest["events"][0]["id"], start);
    assert_eq!(oldest["correlated_events"].as_array().unwrap().len(), 1);
    assert_eq!(oldest["correlated_events"][0]["id"], finish);

    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await["cleared_events"],
        500
    );
    assert_eq!(server.snapshot().await["correlated_events"], json!([]));
    assert_eq!(
        server.events_page(None, Some(1)).await["correlated_events"],
        json!([])
    );
}

#[tokio::test]
async fn unpaced_activity_burst_keeps_a_contiguous_bounded_suffix() {
    const BURST: u64 = 3500;
    let activity = Arc::new(ActivityHub::new(500));
    // Deliberately outpace zstd: dropping pending chunks is allowed to keep
    // MCP operations responsive, but history must not have internal holes.
    for id in 1..=BURST {
        activity.emit("tool.start", "info", None, vec![], json!({"sequence": id}));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let server = Dashboard::start(WebUiState::without_pool(activity.clone())).await;
    let mut newest_first = Vec::new();
    let mut before_id = None;
    for page_count in 0..15 {
        let page = server.events_page(before_id, None).await;
        let ids = page["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["id"].as_u64().unwrap())
            .collect::<Vec<_>>();
        assert!(!ids.is_empty(), "page {page_count} is unexpectedly empty");
        assert!(ids.len() <= 200, "page {page_count} exceeds browser cap");
        assert!(ids.windows(2).all(|pair| pair[0] + 1 == pair[1]));
        if let Some(cursor) = before_id {
            assert!(ids.iter().all(|&id| id < cursor));
        }
        newest_first.extend(ids.iter().rev().copied());
        if page["has_more"] != true {
            assert!(page["next_before_id"].is_null());
            break;
        }
        assert_eq!(page["next_before_id"], ids[0]);
        before_id = Some(ids[0]);
        assert!(page_count < 14, "pagination never reached the oldest event");
    }
    assert_eq!(newest_first.first().copied(), Some(BURST));
    assert!(!newest_first.is_empty() && newest_first.len() <= 2500);
    assert!(
        newest_first.windows(2).all(|pair| pair[0] == pair[1] + 1),
        "dropping pending archives must not leave gaps behind a newer suffix"
    );
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await["cleared_events"],
        newest_first.len()
    );
    assert_eq!(server.events_page(None, None).await["events"], json!([]));
}

#[tokio::test]
async fn history_endpoint_rejects_invalid_cursors_page_sizes_and_query_fields() {
    let activity = Arc::new(ActivityHub::new(500));
    for id in 1..=200 {
        activity.emit("tool.start", "info", None, vec![], json!({"id":id}));
    }
    let server = Dashboard::start(WebUiState::without_pool(activity)).await;
    let snapshot = server.snapshot().await;
    assert_eq!(snapshot["events"].as_array().unwrap().len(), 200);
    assert_eq!(snapshot["events_has_more"], false);
    assert!(snapshot["events_next_before_id"].is_null());
    let beyond = server.events_page(Some(1), Some(1)).await;
    assert_eq!(beyond["events"], json!([]));
    assert_eq!(beyond["has_more"], false);

    for suffix in [
        "?before_id=0",
        "?before_id=-1",
        "?before_id=not-a-number",
        "?before_id=18446744073709551616",
        "?limit=0",
        "?limit=201",
        "?limit=-1",
        "?limit=1.5",
        "?unrecognized=1",
    ] {
        let response = server
            .client
            .get(format!("{}/_admin/api/events{suffix}", server.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{suffix}");
    }
    let response = server
        .client
        .get(format!("{}/_admin/api/events", server.url))
        .header("host", "example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_posts_require_same_origin_loopback_host_json_and_csrf_token() {
    let workspace = TempDir::new().unwrap();
    let store =
        Arc::new(AgentPoolStore::open(&workspace.path().join("private/pool.sqlite")).unwrap());
    let sent = store
        .send(
            "account-a",
            send_args(Some("admin"), Some("unread")),
            Some("session"),
        )
        .unwrap();
    let message_id = sent.message_id.unwrap();
    let activity = Arc::new(ActivityHub::new(20));
    activity.emit(
        "tool.finish",
        "info",
        None,
        vec![],
        json!({"output":"keep"}),
    );
    let server = Dashboard::start(
        WebUiState::for_principal(store.clone(), "account-a".into(), activity).unwrap(),
    )
    .await;
    let token = server.snapshot().await["csrf_token"]
        .as_str()
        .unwrap()
        .to_owned();

    for (endpoint, body) in [
        (
            "send",
            json!({"pool":"team", "target":"global", "message":"attack"}),
        ),
        ("ack", json!({"message_id":message_id})),
        ("clear", json!({})),
        ("terminate", json!({"pool":"team", "agent":"Alice"})),
    ] {
        let uri = format!("{}/_admin/api/{endpoint}", server.url);
        let body = body.to_string();
        // Browser no-cors fetch: this simple text/plain request used to mutate state.
        let response = server
            .client
            .post(&uri)
            .header("origin", "http://localhost:9999")
            .header("content-type", "text/plain")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{endpoint}: cross-origin simple POST"
        );

        let response = server
            .client
            .post(&uri)
            .header("origin", "http://localhost:9999")
            .header("x-webui-csrf-token", &token)
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{endpoint}: cross-origin with token"
        );

        let response = server
            .client
            .post(&uri)
            .header("origin", &server.url)
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{endpoint}: missing token"
        );

        let response = server
            .client
            .post(&uri)
            .header("x-webui-csrf-token", &token)
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{endpoint}: missing Origin"
        );

        let response = server
            .client
            .post(&uri)
            .header("origin", &server.url)
            .header("x-webui-csrf-token", &token)
            .header("content-type", "text/plain")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{endpoint}: wrong content type"
        );

        let response = server
            .client
            .post(&uri)
            .header("host", "evil.example")
            .header("origin", "http://evil.example")
            .header("x-webui-csrf-token", &token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{endpoint}: DNS rebinding Host"
        );
    }

    // A DNS-rebound GET must not be allowed to read the bearer CSRF token.
    let response = server
        .client
        .get(format!("{}/_admin/api/snapshot", server.url))
        .header("host", "evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    assert_eq!(
        server.snapshot().await["events"].as_array().unwrap().len(),
        1
    );
    assert!(!store.admin_messages("account-a", 10).unwrap()[0].acknowledged);
    assert_eq!(store.admin_members("account-a").unwrap().len(), 1);
    let turn = store
        .start_tool("account-a", Some("session"))
        .await
        .unwrap();
    let inbox = store
        .finish_tool_turn("account-a", Some("session"), turn.as_ref())
        .await
        .unwrap();
    assert!(
        inbox.messages.is_empty(),
        "cross-origin send must not queue messages"
    );
}

#[tokio::test]
async fn admin_inbox_cursor_pages_reach_older_unread_messages_and_offer_receipts() {
    let workspace = TempDir::new().unwrap();
    let path = workspace.path().join("private/pool.sqlite");
    let store = Arc::new(AgentPoolStore::open(&path).unwrap());
    let mut expected = HashSet::new();
    let mut oldest_id = String::new();
    for i in 0..755 {
        let message = format!("message-{i:03}");
        let result = store
            .send(
                "account-a",
                send_args(Some("admin"), Some(&message)),
                Some("session"),
            )
            .unwrap();
        let id = result.message_id.unwrap();
        if i == 0 {
            oldest_id = id.clone();
        }
        expected.insert(id);
    }
    // Force identical timestamps: cursor pagination must break ties by ID.
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute(
        "UPDATE admin_messages SET created=1000 WHERE principal='account-a'",
        [],
    )
    .unwrap();
    let server = Dashboard::start(
        WebUiState::for_principal(
            store.clone(),
            "account-a".into(),
            Arc::new(ActivityHub::new(10)),
        )
        .unwrap(),
    )
    .await;

    let mut seen = HashSet::new();
    let mut cursor = None;
    let mut lengths = vec![];
    let mut last_page_cursor = None;
    let mut older_page_id = String::new();
    for _ in 0..4 {
        let page = server.snapshot_after(cursor.as_deref()).await;
        let messages = page["admin_messages"].as_array().unwrap();
        lengths.push(messages.len());
        older_page_id = messages.last().unwrap()["message_id"]
            .as_str()
            .unwrap()
            .to_owned();
        for message in messages {
            assert_eq!(message["acknowledged"], false);
            assert!(
                seen.insert(message["message_id"].as_str().unwrap().to_owned()),
                "duplicate page item"
            );
        }
        // The lookahead row that establishes has_more must not create a receipt.
        let offered: i64 = db.query_row("SELECT COUNT(*) FROM admin_messages WHERE offered_at IS NOT NULL AND principal='account-a'", [], |row| row.get(0)).unwrap();
        let receipts: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM delivery_receipts WHERE recipient='admin'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(offered as usize, seen.len());
        assert_eq!(receipts as usize, seen.len());

        last_page_cursor = cursor;
        cursor = page["admin_messages_next_cursor"]
            .as_str()
            .map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(lengths, vec![200, 200, 200, 155]);
    assert_eq!(
        seen, expected,
        "all 755 retained messages must be reachable"
    );
    assert!(seen.contains(&oldest_id));

    assert_eq!(
        server
            .post("ack", json!({"message_id":older_page_id}), StatusCode::OK)
            .await,
        json!({"acknowledged":true})
    );
    let last_page = server.snapshot_after(last_page_cursor.as_deref()).await;
    let oldest = last_page["admin_messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["message_id"] == older_page_id)
        .unwrap();
    assert_eq!(oldest["acknowledged"], true);

    let full = server.full_snapshot().await;
    let all_messages: HashSet<_> = full["admin_messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["message_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(all_messages, expected);
    assert!(full["admin_messages_next_cursor"].is_null());
    assert!(
        full["admin_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["message_id"] == older_page_id && message["acknowledged"] == true
            })
    );

    for cursor in [
        "garbage",
        "1000:not-a-uuid",
        "1000:00000000-0000-0000-0000-000000000000:extra",
    ] {
        let response = server
            .client
            .get(format!("{}/_admin/api/snapshot", server.url))
            .query(&[("all", "false"), ("cursor", cursor)])
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "malformed cursor {cursor}"
        );
    }
}

#[tokio::test]
async fn snapshot_rejects_invalid_full_export_queries() {
    let server = Dashboard::start(WebUiState::without_pool(Arc::new(ActivityHub::new(10)))).await;
    for query in [
        "all=invalid",
        "all=true&cursor=anything",
        "cursor=anything",
        "unknown=true",
    ] {
        let response = server
            .client
            .get(format!("{}/_admin/api/snapshot?{query}", server.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
    }
}

#[tokio::test]
async fn clear_removes_all_history_and_only_the_current_accounts_admin_inbox() {
    let workspace = TempDir::new().unwrap();
    let path = workspace.path().join("private/pool.sqlite");
    let store = Arc::new(AgentPoolStore::open(&path).unwrap());
    // Include messages beyond the dashboard's 200-row snapshot, in two pools.
    for i in 0..205 {
        let mut args = send_args(Some("admin"), Some("history"));
        if i % 2 == 1 {
            args.pool = "second-pool".into();
        }
        let sent = store.send("account-a", args, Some("session")).unwrap();
        if i == 0 {
            store
                .admin_ack("account-a", sent.message_id.as_deref().unwrap())
                .unwrap();
        }
    }
    store
        .send(
            "account-b",
            send_args(Some("admin"), Some("keep")),
            Some("session"),
        )
        .unwrap();
    let members = store.admin_members("account-a").unwrap();
    let agent = &members.iter().find(|m| m.pool == "team").unwrap().agent;
    store
        .admin_send("account-a", "team", agent, "pending delivery", None)
        .unwrap();
    let activity = Arc::new(ActivityHub::new(500));
    for _ in 0..450 {
        activity.emit(
            "tool.finish",
            "info",
            None,
            vec![],
            json!({"output":"history"}),
        );
    }
    let server = Dashboard::start(
        WebUiState::for_principal(store.clone(), "account-a".into(), activity.clone()).unwrap(),
    )
    .await;
    server
        .post(
            "clear",
            json!({"principal":"account-b"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    let before = server.snapshot().await;
    assert_eq!(before["events"].as_array().unwrap().len(), 200);
    assert_eq!(before["admin_messages"].as_array().unwrap().len(), 200);
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await,
        json!({"cleared_events":450, "cleared_admin_messages":205})
    );
    for _ in 0..2 {
        let after = server.snapshot().await;
        assert_eq!(after["events"], json!([]));
        assert_eq!(after["admin_messages"], json!([]));
        assert_eq!(after["pools"], before["pools"]);
        assert_eq!(
            store.admin_messages("account-b", 10).unwrap()[0].message,
            "keep"
        );
    }
    assert_eq!(
        server.post("clear", json!({}), StatusCode::OK).await,
        json!({"cleared_events":0, "cleared_admin_messages":0})
    );
    let turn = store
        .start_tool("account-a", Some("session"))
        .await
        .unwrap();
    let offered = store
        .finish_tool_turn("account-a", Some("session"), turn.as_ref())
        .await
        .unwrap();
    assert_eq!(offered.messages.len(), 1);
    assert_eq!(offered.messages[0].message, "pending delivery");
    let id = activity.emit("tool.start", "info", None, vec![], json!({}));
    assert_eq!(id, 451);
    assert_eq!(server.snapshot().await["events"][0]["id"], id);
    drop(server);
    drop(store);
    let reopened = AgentPoolStore::open(&path).unwrap();
    assert!(
        reopened
            .admin_messages("account-a", 500)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened.admin_messages("account-b", 10).unwrap()[0].message,
        "keep"
    );
}

#[tokio::test]
async fn handlers_scope_snapshot_send_ack_and_termination_to_principal() {
    let workspace = TempDir::new().unwrap();
    let store = Arc::new(
        AgentPoolStore::open_with_names(
            &workspace.path().join("private/pool.sqlite"),
            Duration::from_secs(60),
            vec!["Alice".into(), "Bob".into()],
        )
        .unwrap(),
    );
    for principal in ["account-a", "account-b"] {
        let sent = store
            .send(
                principal,
                send_args(Some("admin"), Some(principal)),
                Some("session"),
            )
            .unwrap();
        assert_eq!(sent.assigned_agent.as_deref(), Some("Alice"));
    }
    let server = Dashboard::start(
        WebUiState::for_principal(
            store.clone(),
            "account-a".into(),
            Arc::new(ActivityHub::new(10)),
        )
        .unwrap(),
    )
    .await;
    let snapshot = server.full_snapshot().await;
    assert_eq!(snapshot["pools"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["pools"][0]["agents"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["pools"][0]["agents"][0]["agent"], "Alice");
    assert_eq!(snapshot["admin_messages"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["admin_messages"][0]["message"], "account-a");
    assert_eq!(snapshot["admin_messages"][0]["acknowledged"], false);

    let message_id = snapshot["admin_messages"][0]["message_id"].clone();
    let other_message = store.admin_messages("account-b", 10).unwrap().remove(0);
    for body in [
        json!({"message_id":message_id, "principal":"account-b"}),
        json!({"message_id":""}),
        json!({}),
    ] {
        server.post("ack", body, StatusCode::BAD_REQUEST).await;
    }
    for id in [&other_message.message_id, "msg_missing"] {
        assert_eq!(
            server
                .post("ack", json!({"message_id":id}), StatusCode::OK)
                .await,
            json!({"acknowledged":false})
        );
    }
    // Retrying an ACK is safe, and snapshots retain the message and its status.
    for _ in 0..2 {
        assert_eq!(
            server
                .post("ack", json!({"message_id":message_id}), StatusCode::OK)
                .await,
            json!({"acknowledged":true})
        );
        let messages = server.snapshot().await["admin_messages"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["message_id"], message_id);
        assert_eq!(messages[0]["message"], "account-a");
        assert_eq!(messages[0]["acknowledged"], true);
        assert!(!store.admin_messages("account-b", 10).unwrap()[0].acknowledged);
    }

    server
        .post(
            "send",
            json!({"pool":"team", "target":"Alice", "message":"hello", "principal":"account-b"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    server
        .post(
            "terminate",
            json!({"pool":"team", "agent":"Alice", "principal":"account-b"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    let sent = server
        .post(
            "send",
            json!({"pool":"team", "target":"Alice", "message":"hello"}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(sent["recipients"], json!(["Alice"]));
    let a_turn = store
        .start_tool("account-a", Some("session"))
        .await
        .unwrap();
    let b_turn = store
        .start_tool("account-b", Some("session"))
        .await
        .unwrap();
    let a_messages = store
        .finish_tool_turn("account-a", Some("session"), a_turn.as_ref())
        .await
        .unwrap();
    let b_messages = store
        .finish_tool_turn("account-b", Some("session"), b_turn.as_ref())
        .await
        .unwrap();
    assert_eq!(a_messages.messages.len(), 1);
    assert_eq!(a_messages.messages[0].message, "hello");
    assert!(b_messages.messages.is_empty());
    assert_eq!(a_messages.delivery_receipts.len(), 1);
    assert_eq!(a_messages.delivery_receipts[0].message_id, message_id);
    assert_eq!(b_messages.delivery_receipts.len(), 1);
    assert_eq!(
        b_messages.delivery_receipts[0].message_id,
        other_message.message_id
    );
    assert_eq!(b_messages.delivery_receipts[0].to, "admin");

    assert_eq!(
        server
            .post(
                "terminate",
                json!({"pool":"team", "agent":"Alice"}),
                StatusCode::OK
            )
            .await,
        json!({"terminated":true})
    );
    assert_eq!(server.snapshot().await["pools"], json!([]));
    assert_eq!(store.admin_members("account-b").unwrap().len(), 1);
    assert_eq!(
        server
            .post(
                "terminate",
                json!({"pool":"team", "agent":"Alice"}),
                StatusCode::OK
            )
            .await,
        json!({"terminated":false})
    );
    server
        .post(
            "send",
            json!({"pool":"team", "target":"Alice", "message":"after termination"}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    let events = server.snapshot().await["events"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        events
            .iter()
            .map(|event| event["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "admin.message.send",
            "admin.membership.terminate",
            "admin.membership.terminate"
        ]
    );
}

#[test]
fn public_dashboard_constructor_rejects_invalid_principals() {
    let workspace = TempDir::new().unwrap();
    let store =
        Arc::new(AgentPoolStore::open(&workspace.path().join("private/pool.sqlite")).unwrap());
    assert!(WebUiState::for_principal(store, "".into(), Arc::new(ActivityHub::new(10))).is_err());
}

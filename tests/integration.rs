/// End-to-end integration tests.
///
/// Architecture:
///   test → reqwest::Client → shunt (axum, real TcpListener) → mock_upstream (axum, real TcpListener)
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use reqwest::Client;
use serde_json::json;
use tokio::net::TcpListener;

use shunt::config::{AccountConfig, Config, RoutingStrategy, ServerConfig};
use shunt::credential::Credential;
use shunt::oauth::OAuthCredential;
use shunt::provider::Provider;
use shunt::proxy::create_app_with_state;
use shunt::state::StateStore;

// ---------------------------------------------------------------------------
// Test server helper
// ---------------------------------------------------------------------------

struct TestServer {
    pub addr: SocketAddr,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    async fn start(app: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { rx.await.ok(); })
                .await
                .ok();
        });

        Self { addr, _shutdown_tx: tx }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

// ---------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Captures {
    inner: Arc<Mutex<Vec<CapturedRequest>>>,
}

impl Captures {
    fn push(&self, r: CapturedRequest) { self.inner.lock().unwrap().push(r); }
    fn get(&self, i: usize) -> CapturedRequest { self.inner.lock().unwrap()[i].clone() }
    fn len(&self) -> usize { self.inner.lock().unwrap().len() }
}

#[derive(Clone)]
struct CapturedRequest {
    pub headers: reqwest::header::HeaderMap,
    pub body: Bytes,
    pub uri: String,
}

fn make_mock_upstream(captures: Captures, streaming: bool, status: u16) -> Router {
    Router::new()
        .route("/v1/messages", post({
            let caps = captures.clone();
            move |req: Request| handle_request(req, caps.clone(), streaming, status)
        }))
        .route("/v1/messages/count_tokens", post({
            let caps = captures.clone();
            move |req: Request| handle_count_tokens(req, caps.clone())
        }))
}

async fn handle_request(req: Request, caps: Captures, streaming: bool, status: u16) -> Response {
    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    caps.push(CapturedRequest {
        headers: to_reqwest_headers(&parts.headers),
        body: body_bytes,
        uri: parts.uri.to_string(),
    });

    if status != 200 {
        // Include a rate-limit reset header far in the future (>5h) so the proxy's
        // wait_deadline_ms is immediately exceeded and it returns 503 without looping.
        let far_future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() + 10 * 3600;
        return Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .header("anthropic-ratelimit-unified-5h-utilization", "1.0")
            .header("anthropic-ratelimit-unified-5h-reset", far_future.to_string())
            .body(Body::from(
                serde_json::to_vec(&json!({"type":"error","error":{"type":"rate_limit_error","message":"slow down"}})).unwrap()
            ))
            .unwrap();
    }

    if streaming {
        let sse = b"data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hello\"}}\n\n\
                    data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n\
                    data: [DONE]\n\n";
        return Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(Body::from(Bytes::from_static(sse)))
            .unwrap();
    }

    axum::Json(json!({"id":"msg_test","type":"message","content":[{"type":"text","text":"Hi"}]}))
        .into_response()
}

async fn handle_count_tokens(req: Request, caps: Captures) -> impl IntoResponse {
    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    caps.push(CapturedRequest {
        headers: to_reqwest_headers(&parts.headers), body: body_bytes,
        uri: parts.uri.to_string(),
    });
    axum::Json(json!({"input_tokens": 99}))
}

fn to_reqwest_headers(h: &axum::http::HeaderMap) -> reqwest::header::HeaderMap {
    let mut out = reqwest::header::HeaderMap::new();
    for (k, v) in h.iter() {
        if let (Ok(n), Ok(v)) = (
            reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            out.insert(n, v);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const TEST_TOKEN: &str = "test-oauth-token-abc123";

fn test_credential() -> Credential {
    Credential::Oauth(OAuthCredential {
        email: None,
        access_token: TEST_TOKEN.into(),
        refresh_token: "test-refresh-token".into(),
        expires_at: u64::MAX / 2,
        id_token: None,
        chatgpt_account_id: None,
        chatgpt_account_is_fedramp: false,
    })
}

fn test_account() -> AccountConfig {
    AccountConfig {
        name: "test".into(),
        plan_type: "pro".into(),
        provider: Provider::default(),
        credential: Some(test_credential()),
        upstream_url: None, model: None,
    }
}

async fn setup(streaming: bool, upstream_status: u16) -> (TestServer, TestServer, Captures, Client) {
    let caps = Captures::default();
    let upstream = TestServer::start(make_mock_upstream(caps.clone(), streaming, upstream_status)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: upstream.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            ..ServerConfig::default()
        },
        accounts: vec![test_account()],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    (proxy, upstream, caps, Client::new())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health() {
    let (proxy, _up, _caps, client) = setup(false, 200).await;
    let resp = client.get(format!("{}/health", proxy.url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["status"], "ok");
}

#[tokio::test]
async fn test_status() {
    let (proxy, _up, _caps, client) = setup(false, 200).await;
    let resp = client.get(format!("{}/status", proxy.url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["accounts"][0]["name"], "test");
}

#[tokio::test]
async fn test_bearer_token_injected() {
    // Proxy strips client's Authorization and injects the account's Bearer token.
    let (proxy, _up, caps, client) = setup(false, 200).await;

    let body = r#"{"model":"claude-opus-4-5","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        // Client sends its own token — must be replaced
        .header("authorization", "Bearer sk-ant-client-wrong-token")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    let received = caps.get(0);
    // Proxy injected account token
    assert_eq!(
        received.headers.get("authorization").unwrap().to_str().unwrap(),
        format!("Bearer {TEST_TOKEN}")
    );
    // anthropic-version preserved
    assert_eq!(
        received.headers.get("anthropic-version").unwrap().to_str().unwrap(),
        "2023-06-01"
    );
    // x-api-key NOT injected (Claude Code mode uses Bearer)
    assert!(received.headers.get("x-api-key").is_none());
}

#[tokio::test]
async fn test_request_body_byte_exact() {
    // Proxy must NOT re-serialize JSON — unusual whitespace/ordering must survive.
    let (proxy, _up, caps, client) = setup(false, 200).await;
    let raw = b"{\"model\":  \"claude-opus-4-5\"  ,  \"max_tokens\":1,\"messages\":[]}";

    client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body(raw.as_ref())
        .send()
        .await
        .unwrap();

    assert_eq!(caps.get(0).body.as_ref(), raw.as_ref());
}

#[tokio::test]
async fn test_streaming_forward() {
    let (proxy, _up, _caps, client) = setup(true, 200).await;

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-opus-4-5","max_tokens":10,"stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers()["content-type"].to_str().unwrap().contains("text/event-stream"));
    let content = resp.bytes().await.unwrap();
    assert!(content.windows(b"content_block_delta".len()).any(|w| w == b"content_block_delta"));
    assert!(content.windows(b"[DONE]".len()).any(|w| w == b"[DONE]"));
}

#[tokio::test]
async fn test_upstream_error_returned_to_client() {
    // Single account returning 429 → all accounts exhausted → proxy returns a
    // graceful 429 + Retry-After (backpressure), not a hard 503.
    let (proxy, _up, _caps, client) = setup(false, 429).await;
    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["type"], "error");
}

#[tokio::test]
async fn test_count_tokens_forwarded() {
    let (proxy, _up, caps, client) = setup(false, 200).await;
    let resp = client
        .post(format!("{}/v1/messages/count_tokens", proxy.url()))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-opus-4-5","messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["input_tokens"], 99);
    assert_eq!(caps.len(), 1);
}

#[tokio::test]
async fn test_hop_by_hop_headers_stripped() {
    let (proxy, _up, caps, client) = setup(false, 200).await;
    client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();

    let received = caps.get(0);
    assert!(received.headers.get("connection").is_none());
    assert!(received.headers.get("transfer-encoding").is_none());
    // Bearer token is injected
    assert!(received.headers.get("authorization").unwrap()
        .to_str().unwrap().starts_with("Bearer "));
}

#[tokio::test]
async fn test_concurrent_requests() {
    let (proxy, _up, caps, client) = setup(false, 200).await;
    let client = Arc::new(client);
    let url = proxy.url();

    let handles: Vec<_> = (0..10u32)
        .map(|i| {
            let c = client.clone();
            let u = format!("{url}/v1/messages");
            tokio::spawn(async move {
                c.post(u)
                    .header("content-type", "application/json")
                    .body(format!("{{\"i\":{i}}}"))
                    .send()
                    .await
                    .unwrap()
                    .status()
            })
        })
        .collect();

    let statuses: Vec<_> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert!(statuses.iter().all(|s| s.as_u16() == 200));
    assert_eq!(caps.len(), 10);
}

// ---------------------------------------------------------------------------
// Phase 2: multi-account failover + stickiness
// ---------------------------------------------------------------------------

const TEST_TOKEN_2: &str = "test-oauth-token-second-account";

fn test_account2() -> AccountConfig {
    AccountConfig {
        name: "second".into(),
        plan_type: "pro".into(),
        provider: Provider::default(),
        credential: Some(Credential::Oauth(OAuthCredential {
            email: None,
            access_token: TEST_TOKEN_2.into(),
            refresh_token: "test-refresh-2".into(),
            expires_at: u64::MAX / 2,
            id_token: None,
            chatgpt_account_id: None,
            chatgpt_account_is_fedramp: false,
        })),
        upstream_url: None, model: None,
    }
}

/// Mock upstream that returns 429 when it sees account1's token, 200 otherwise.
fn make_token_aware_upstream(captures: Captures) -> Router {
    Router::new().route("/v1/messages", post({
        let caps = captures.clone();
        move |req: Request| async move {
            let auth = req.headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let (parts, body) = req.into_parts();
            let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
            caps.push(CapturedRequest {
                headers: to_reqwest_headers(&parts.headers), body: body_bytes,
                uri: parts.uri.to_string(),
            });

            if auth == format!("Bearer {TEST_TOKEN}") {
                // First account → rate limited
                return (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    axum::Json(json!({"type":"error","error":{"type":"rate_limit_error","message":"slow down"}})),
                ).into_response();
            }
            axum::Json(json!({"id":"msg_ok","type":"message","content":[{"type":"text","text":"ok"}]}))
                .into_response()
        }
    }))
}

async fn setup_multi() -> (TestServer, TestServer, Captures, Client) {
    let caps = Captures::default();
    let upstream = TestServer::start(make_token_aware_upstream(caps.clone())).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: upstream.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            // Carousel ensures deterministic ordering (account1 first) and a short
            // request_timeout avoids waiting for cooldowns in tests.
            routing_strategy: RoutingStrategy::Carousel,
            request_timeout_secs: 1,
            ..ServerConfig::default()
        },
        accounts: vec![test_account(), test_account2()],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    (proxy, upstream, caps, Client::new())
}

#[tokio::test]
async fn test_failover_to_second_account() {
    // First account gets 429 → proxy retries with second account → 200
    let (proxy, _up, caps, client) = setup_multi().await;

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-haiku-4-5-20251001","max_tokens":8,"messages":[{"role":"user","content":"hello failover"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200, "expected success after failover");

    // Two upstream requests were made: first with token1 (429), then token2 (200)
    assert_eq!(caps.len(), 2);
    assert_eq!(
        caps.get(0).headers.get("authorization").unwrap().to_str().unwrap(),
        format!("Bearer {TEST_TOKEN}")
    );
    assert_eq!(
        caps.get(1).headers.get("authorization").unwrap().to_str().unwrap(),
        format!("Bearer {TEST_TOKEN_2}")
    );
}

#[tokio::test]
async fn test_stickiness_same_conversation() {
    // Two requests with the same fingerprint body → same account used both times.
    // Both accounts are healthy — stickiness pins to the first chosen account.
    let caps = Captures::default();
    let upstream = TestServer::start(make_mock_upstream(caps.clone(), false, 200)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: upstream.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            ..ServerConfig::default()
        },
        accounts: vec![test_account(), test_account2()],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    // Same system + first user message = same fingerprint
    let body = r#"{"model":"claude-haiku-4-5-20251001","max_tokens":8,"system":"You are helpful","messages":[{"role":"user","content":"sticky question"},{"role":"assistant","content":"answer"},{"role":"user","content":"follow-up"}]}"#;

    for _ in 0..3 {
        client
            .post(format!("{}/v1/messages", proxy.url()))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
    }

    // All 3 requests should use the same account (same Bearer token)
    let token0 = caps.get(0).headers.get("authorization").unwrap().to_str().unwrap().to_owned();
    for i in 1..3 {
        assert_eq!(
            caps.get(i).headers.get("authorization").unwrap().to_str().unwrap(),
            token0,
            "request {i} used a different account — stickiness broken"
        );
    }
}

#[tokio::test]
async fn test_all_accounts_exhausted_returns_503() {
    // All accounts return 429 → proxy returns 503
    let caps = Captures::default();
    let upstream = TestServer::start(make_mock_upstream(caps.clone(), false, 429)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: upstream.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            // Short timeout: don't wait for cooldowns to expire during the test.
            request_timeout_secs: 1,
            // Fail fast without re-trying recovered accounts during the wait,
            // so each account is tried exactly once (cooling-wait is now bounded
            // by this knob, not request_timeout_secs).
            max_startup_wait_ms: 0,
            routing_strategy: RoutingStrategy::Carousel,
            ..ServerConfig::default()
        },
        accounts: vec![test_account(), test_account2()],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();

    // All accounts unavailable now returns 429 + Retry-After (graceful
    // backpressure) instead of a hard 503, so clients back off and retry
    // rather than failing the run.
    assert_eq!(resp.status(), 429);
    // Both accounts were tried
    assert_eq!(caps.len(), 2);
}

#[tokio::test]
async fn test_status_shows_account_status() {
    // After a 429 the account shows "cooling" in /status
    let (proxy, _up, _caps, client) = setup_multi().await;

    // This request hits account1 (429) then succeeds on account2,
    // leaving account1 in cooling state.
    client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body(r#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":"x"}]}"#)
        .send()
        .await
        .unwrap();

    let status: serde_json::Value = client
        .get(format!("{}/status", proxy.url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let accounts = status["accounts"].as_array().unwrap();
    let a1 = accounts.iter().find(|a| a["name"] == "test").unwrap();
    let a2 = accounts.iter().find(|a| a["name"] == "second").unwrap();

    assert_eq!(a1["status"], "cooling", "account1 should be cooling after 429");
    assert_eq!(a2["status"], "available", "account2 should still be available");
}

// ---------------------------------------------------------------------------
// New-feature coverage
// ---------------------------------------------------------------------------

/// /status must expose the correct canonical field names — no legacy duplicates.
#[tokio::test]
async fn test_status_response_shape() {
    let (proxy, _up, _caps, client) = setup(false, 200).await;
    let body: serde_json::Value = client
        .get(format!("{}/status", proxy.url()))
        .send().await.unwrap()
        .json().await.unwrap();

    // Top-level keys
    assert!(body.get("version").is_some());
    assert!(body.get("started_ms").is_some());
    assert!(body.get("accounts").is_some());
    // pinned_account / last_used_account present (may be null)
    assert!(body.as_object().unwrap().contains_key("pinned_account"),
        "top-level key 'pinned_account' must be present");
    assert!(body.as_object().unwrap().contains_key("last_used_account"),
        "top-level key 'last_used_account' must be present");
    assert!(body.get("recent_requests").is_some());

    // Account-level: canonical names, no legacy duplicates
    let acc = &body["accounts"][0];
    assert_eq!(acc["name"], "test");
    assert!(acc.get("plan_type").is_some(), "account must have 'plan_type'");
    assert!(acc.get("plan").is_none(),       "legacy 'plan' field must be absent");
    assert!(acc.get("status").is_some());
    assert!(acc.get("available").is_some());
    assert!(acc.get("disabled").is_some());
    assert!(acc.get("cooldown_until_ms").is_some());
}

/// remote_key: requests without / with wrong key must be rejected; correct key passes.
#[tokio::test]
async fn test_remote_key_auth() {
    let caps = Captures::default();
    let upstream = TestServer::start(make_mock_upstream(caps.clone(), false, 200)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: upstream.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            remote_key: Some("mysecret".into()),
            ..ServerConfig::default()
        },
        accounts: vec![test_account()],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();
    let body = r#"{"model":"claude-opus-4-5","max_tokens":1,"messages":[]}"#;

    // No key → 401
    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body(body)
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "missing key must be rejected");

    // Wrong key → 401
    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .header("x-api-key", "wrongkey")
        .body(body)
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "wrong key must be rejected");

    // Correct key → 200 and upstream receives exactly one request
    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .header("x-api-key", "mysecret")
        .body(body)
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "correct key must be accepted");
    assert_eq!(caps.len(), 1, "only the authenticated request should reach upstream");
}

/// /use pins an account; subsequent requests go straight to it without failover.
#[tokio::test]
async fn test_account_pinning() {
    // setup_multi: account "test" → 429, account "second" → 200
    let (proxy, _up, caps, client) = setup_multi().await;

    // Pin the healthy account
    let pin: serde_json::Value = client
        .post(format!("{}/use", proxy.url()))
        .json(&json!({"account": "second"}))
        .send().await.unwrap()
        .json().await.unwrap();
    assert_eq!(pin["pinned"], "second");

    // Request should go directly to "second" — no retry to "test"
    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body(r#"{"model":"claude-haiku-4-5-20251001","max_tokens":1,"messages":[]}"#)
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(caps.len(), 1, "pinned account means no failover attempt");
    assert_eq!(
        caps.get(0).headers.get("authorization").unwrap().to_str().unwrap(),
        format!("Bearer {TEST_TOKEN_2}"),
        "must use the pinned account's token"
    );

    // Unpin → /use with "auto"
    let unpin: serde_json::Value = client
        .post(format!("{}/use", proxy.url()))
        .json(&json!({"account": "auto"}))
        .send().await.unwrap()
        .json().await.unwrap();
    assert_eq!(unpin["pinned"], "auto");
}

/// After a successful proxied request, /status must report last_used_account.
#[tokio::test]
async fn test_last_used_account_tracked() {
    let (proxy, _up, _caps, client) = setup(false, 200).await;

    // No request yet
    let status: serde_json::Value = client
        .get(format!("{}/status", proxy.url()))
        .send().await.unwrap().json().await.unwrap();
    assert!(status["last_used_account"].is_null(), "should start null");

    // Successful request
    client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .body("{}")
        .send().await.unwrap();

    let status: serde_json::Value = client
        .get(format!("{}/status", proxy.url()))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(status["last_used_account"], "test");
}

/// /use with an unknown account name returns an error.
#[tokio::test]
async fn test_use_unknown_account_returns_error() {
    let (proxy, _up, _caps, client) = setup(false, 200).await;
    let resp: serde_json::Value = client
        .post(format!("{}/use", proxy.url()))
        .json(&json!({"account": "nonexistent"}))
        .send().await.unwrap()
        .json().await.unwrap();
    assert!(resp.get("error").is_some(), "unknown account must return error");
}

// ---------------------------------------------------------------------------
// Cross-protocol interop tests
// ---------------------------------------------------------------------------

const OPENAI_TOKEN: &str = "openai-test-token-xyz";

/// Mock upstream that speaks the OpenAI /v1/chat/completions protocol.
fn make_openai_upstream(captures: Captures, streaming: bool) -> Router {
    Router::new().route("/v1/chat/completions", post({
        let caps = captures.clone();
        move |req: Request| async move {
            let (parts, body) = req.into_parts();
            let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
            caps.push(CapturedRequest {
                headers: to_reqwest_headers(&parts.headers), body: body_bytes,
                uri: parts.uri.to_string(),
            });

            if streaming {
                let sse = b"data: {\"id\":\"chatcmpl-x\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n\
                            data: {\"id\":\"chatcmpl-x\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n\
                            data: [DONE]\n\n";
                return Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(Bytes::from_static(sse)))
                    .unwrap();
            }

            axum::Json(json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "model": "gpt-4o",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hello from OpenAI"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            })).into_response()
        }
    }))
}

fn openai_account(upstream_url: String) -> AccountConfig {
    AccountConfig {
        name: "codex".into(),
        plan_type: "pro".into(),
        provider: Provider::OpenAIApi,
        credential: Some(Credential::Oauth(OAuthCredential {
            email: None,
            access_token: OPENAI_TOKEN.into(),
            refresh_token: "openai-refresh".into(),
            expires_at: u64::MAX / 2,
            id_token: None,
            chatgpt_account_id: None,
            chatgpt_account_is_fedramp: false,
        })),
        upstream_url: Some(upstream_url), model: None,
    }
}

fn make_codex_responses_upstream(captures: Captures) -> Router {
    Router::new().route("/backend-api/codex/responses", post({
        let caps = captures.clone();
        move |req: Request| async move {
            let (parts, body) = req.into_parts();
            let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
            caps.push(CapturedRequest {
                headers: to_reqwest_headers(&parts.headers), body,
                uri: parts.uri.to_string(),
            });
            let sse = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_native\"}}\n\n\
event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"id\":\"msg_native\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}}\n\n\
event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_native\",\"usage\":{\"input_tokens\":4,\"output_tokens\":1,\"total_tokens\":5}}}\n\n";
            Response::builder().status(200)
                .header("content-type", "text/event-stream")
                .header("x-codex-turn-state", "server-turn-state")
                .header("x-codex-primary-used-percent", "25")
                .body(Body::from(sse)).unwrap()
        }
    }))
}

#[tokio::test]
async fn test_native_codex_responses_is_byte_transparent_and_injects_identity() {
    let caps = Captures::default();
    let upstream = TestServer::start(make_codex_responses_upstream(caps.clone())).await;
    let credential = Credential::Oauth(OAuthCredential {
        email: Some("codex@example.test".into()),
        access_token: "codex-access-token".into(),
        refresh_token: "codex-refresh-token".into(),
        expires_at: u64::MAX / 2,
        id_token: Some("identity-token-must-not-be-bearer".into()),
        chatgpt_account_id: Some("workspace-123".into()),
        chatgpt_account_is_fedramp: true,
    });
    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(), port: 0, log_level: "error".into(),
            remote_key: Some("codex-client-secret".into()),
            ..ServerConfig::default()
        },
        accounts: vec![AccountConfig {
            name: "codex/native".into(), plan_type: "plus".into(), provider: Provider::OpenAI,
            credential: Some(credential), upstream_url: Some(upstream.url()), model: None,
        }],
        config_file: "/dev/null".into(), model_mapping: Default::default(), api_overflow: Default::default(),
        schema_version: 2, pools: Default::default(), secrets: Default::default(),
        classifier: Default::default(), bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let body = br#"{"model":"gpt-5.4","stream":true,"input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}]}"#;
    let rejected = Client::new().post(format!("{}/backend-api/codex/responses", proxy.url()))
        .header("authorization", "Bearer wrong-client-token")
        .body(body.as_slice()).send().await.unwrap();
    assert_eq!(rejected.status(), 401);
    assert_eq!(caps.len(), 0);
    let response = Client::new().post(format!("{}/backend-api/codex/responses?store=false", proxy.url()))
        .header("content-type", "application/json")
        .header("authorization", "Bearer codex-client-secret")
        .header("chatgpt-account-id", "hostile-workspace")
        .header("x-openai-fedramp", "false")
        .header("session-id", "session-123")
        .header("x-client-request-id", "request-123")
        .body(body.as_slice()).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().get("x-codex-turn-state").unwrap(), "server-turn-state");
    let returned = response.bytes().await.unwrap();
    assert!(String::from_utf8_lossy(&returned).contains("response.output_item.done"));
    let captured = caps.get(0);
    assert_eq!(captured.body.as_ref(), body);
    assert_eq!(captured.uri, "/backend-api/codex/responses?store=false");
    assert_eq!(captured.headers.get("authorization").unwrap(), "Bearer codex-access-token");
    assert_eq!(captured.headers.get("chatgpt-account-id").unwrap(), "workspace-123");
    assert_eq!(captured.headers.get("x-openai-fedramp").unwrap(), "true");
    assert_eq!(captured.headers.get("session-id").unwrap(), "session-123");
    assert_eq!(captured.headers.get("x-client-request-id").unwrap(), "request-123");
}

#[tokio::test]
async fn test_codex_soft_affinity_fails_over_but_turn_state_is_strict() {
    let caps = Captures::default();
    let first_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let upstream = TestServer::start(Router::new().route("/backend-api/codex/responses", post({
        let caps = caps.clone();
        let first_calls = first_calls.clone();
        move |req: Request| {
            let caps = caps.clone();
            let first_calls = first_calls.clone();
            async move {
                let (parts, body) = req.into_parts();
                let auth = parts.headers.get("authorization").and_then(|v| v.to_str().ok())
                    .unwrap_or("").to_owned();
                let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                caps.push(CapturedRequest {
                    headers: to_reqwest_headers(&parts.headers), body,
                    uri: parts.uri.to_string(),
                });
                if auth == "Bearer codex-one" && first_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                    return Response::builder().status(500).body(Body::from("retryable")).unwrap();
                }
                let turn = if auth == "Bearer codex-one" { "turn-one" } else { "turn-two" };
                let sse = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
                Response::builder().status(200).header("content-type", "text/event-stream")
                    .header("x-codex-turn-state", turn).body(Body::from(sse)).unwrap()
            }
        }
    }))).await;
    let account = |name: &str, token: &str| AccountConfig {
        name: name.into(), plan_type: "plus".into(), provider: Provider::OpenAI,
        credential: Some(Credential::Oauth(OAuthCredential {
            email: None, access_token: token.into(), refresh_token: "refresh".into(),
            expires_at: u64::MAX / 2, id_token: None, chatgpt_account_id: None,
            chatgpt_account_is_fedramp: false,
        })),
        upstream_url: Some(upstream.url()), model: None,
    };
    let cfg = Config {
        server: ServerConfig { host: "127.0.0.1".into(), port: 0, log_level: "error".into(), ..ServerConfig::default() },
        accounts: vec![account("codex/one", "codex-one"), account("codex/two", "codex-two")],
        config_file: "/dev/null".into(), model_mapping: Default::default(), api_overflow: Default::default(),
        schema_version: 2, pools: Default::default(), secrets: Default::default(),
        classifier: Default::default(), bridge: Default::default(),
    };
    let state = StateStore::new_empty().scoped("codex");
    state.set_pinned(Some("codex/one".into()));
    let (app, _, _) = create_app_with_state(cfg, state.clone(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let body = json!({"model":"gpt-5.4","stream":true,"input":"hello"});

    let first = Client::new().post(format!("{}/backend-api/codex/responses", proxy.url()))
        .header("session-id", "soft-session").json(&body).send().await.unwrap();
    assert_eq!(first.status(), 200);
    assert_eq!(first.headers().get("x-codex-turn-state").unwrap(), "turn-one");
    let _ = first.bytes().await.unwrap();
    state.set_pinned(None);

    let soft = Client::new().post(format!("{}/backend-api/codex/responses", proxy.url()))
        .header("session-id", "soft-session").json(&body).send().await.unwrap();
    assert_eq!(soft.status(), 200, "soft affinity should fail over after a pre-stream 500");
    assert_eq!(soft.headers().get("x-codex-turn-state").unwrap(), "turn-two");
    let _ = soft.bytes().await.unwrap();

    let strict = Client::new().post(format!("{}/backend-api/codex/responses", proxy.url()))
        .header("x-codex-turn-state", "turn-one").json(&body).send().await.unwrap();
    assert_eq!(strict.status(), 500, "known turn state must not cross accounts");
    assert_eq!(caps.len(), 4);
    assert_eq!(caps.get(0).headers["authorization"], "Bearer codex-one");
    assert_eq!(caps.get(1).headers["authorization"], "Bearer codex-one");
    assert_eq!(caps.get(2).headers["authorization"], "Bearer codex-two");
    assert_eq!(caps.get(3).headers["authorization"], "Bearer codex-one");
}

#[tokio::test]
async fn test_codex_api_overflow_reserves_budget_before_dispatch() {
    let caps = Captures::default();
    let upstream = TestServer::start(Router::new().route("/v1/responses", post({
        let caps = caps.clone();
        move |req: Request| handle_request(req, caps.clone(), false, 200)
    }))).await;
    let account_name = "codex/api-overflow".to_owned();
    let mut overflow = shunt::config::ApiOverflowConfig::default();
    overflow.enabled = true;
    overflow.account = Some(account_name.clone());
    overflow.daily_budget_usd = 0.01;
    let mut pools = shunt::config::PoolsConfig::default();
    pools.codex.overflow = overflow.clone();
    let cfg = Config {
        server: ServerConfig { host: "127.0.0.1".into(), port: 0, log_level: "error".into(), ..ServerConfig::default() },
        accounts: vec![AccountConfig {
            name: account_name, plan_type: "api-overflow".into(), provider: Provider::OpenAIApi,
            credential: Some(Credential::Apikey { key: "sk-test".into() }), upstream_url: Some(upstream.url()), model: None,
        }],
        config_file: "/dev/null".into(), model_mapping: Default::default(), api_overflow: overflow,
        schema_version: 2, pools, secrets: Default::default(), classifier: Default::default(), bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let response = Client::new().post(format!("{}/backend-api/codex/responses", proxy.url()))
        .json(&json!({"model":"gpt-5.4","input":"hello","stream":true})).send().await.unwrap();
    assert_eq!(response.status(), 429);
    assert_eq!(caps.len(), 0, "over-budget request must be rejected before upstream dispatch");
}

/// Anthropic request routed to an OpenAI account — shunt translates A→O,
/// forwards to OpenAI mock, translates response O→A back to client.
#[tokio::test]
async fn test_interop_anthropic_request_to_openai_account() {
    let caps = Captures::default();
    let openai_up = TestServer::start(make_openai_upstream(caps.clone(), false)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: "http://unused-anthropic".into(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            ..ServerConfig::default()
        },
        accounts: vec![openai_account(openai_up.url())],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .json(&json!({
            "model": "claude-opus-4-6",
            "max_tokens": 64,
            "system": "You are helpful.",
            "messages": [{"role": "user", "content": "Hello"}]
        }))
        .send().await.unwrap();

    assert_eq!(resp.status(), 200, "cross-protocol proxy must return 200");

    // Upstream (OpenAI mock) must have received an OpenAI-format body.
    let upstream_req: serde_json::Value = serde_json::from_slice(&caps.get(0).body).unwrap();
    assert!(upstream_req.get("messages").is_some(), "must have messages array");
    assert!(upstream_req.get("max_tokens").is_some(), "must have max_tokens");
    // model must be mapped to an OpenAI model
    let sent_model = upstream_req["model"].as_str().unwrap();
    assert!(!sent_model.starts_with("claude-"), "claude-* model must be mapped to OpenAI model, got: {sent_model}");
    // system must be prepended as system message
    let first_msg = &upstream_req["messages"][0];
    assert_eq!(first_msg["role"], "system");
    assert_eq!(first_msg["content"], "You are helpful.");
    // Anthropic-specific headers must be stripped
    assert!(caps.get(0).headers.get("anthropic-version").is_none(), "anthropic-version must not be forwarded to OpenAI");

    // Client must receive Anthropic-format response.
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message", "response must be Anthropic message type");
    assert!(body["content"].as_array().is_some(), "must have content array");
    assert!(body["stop_reason"].is_string(), "must have stop_reason");
    assert!(body["usage"]["input_tokens"].is_number(), "must have input_tokens");
}

/// Anthropic streaming request routed to an OpenAI account — OpenAI SSE → Anthropic SSE.
#[tokio::test]
async fn test_interop_anthropic_streaming_to_openai_account() {
    let caps = Captures::default();
    let openai_up = TestServer::start(make_openai_upstream(caps.clone(), true)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: "http://unused-anthropic".into(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            ..ServerConfig::default()
        },
        accounts: vec![openai_account(openai_up.url())],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .json(&json!({
            "model": "claude-opus-4-6",
            "max_tokens": 32,
            "stream": true,
            "messages": [{"role": "user", "content": "Hi"}]
        }))
        .send().await.unwrap();

    assert_eq!(resp.status(), 200);
    assert!(resp.headers()["content-type"].to_str().unwrap().contains("text/event-stream"),
        "response must be SSE");

    let body = resp.bytes().await.unwrap();
    let text = String::from_utf8_lossy(&body);
    // Must contain Anthropic SSE event types
    assert!(text.contains("message_start"), "must emit message_start: {text}");
    assert!(text.contains("content_block_delta"), "must emit content_block_delta: {text}");
    assert!(text.contains("message_stop"), "must emit message_stop: {text}");
    // Must NOT contain raw OpenAI chunk format
    assert!(!text.contains("chat.completion.chunk"), "must not expose raw OpenAI format");
}

/// OpenAI request routed to an Anthropic account — shunt translates O→A,
/// forwards to Anthropic mock, translates response A→O back to client.
#[tokio::test]
async fn test_interop_openai_request_to_anthropic_account() {
    let caps = Captures::default();
    let anthropic_up = TestServer::start(make_mock_upstream(caps.clone(), false, 200)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: anthropic_up.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            ..ServerConfig::default()
        },
        accounts: vec![AccountConfig {
            name: "claude".into(),
            plan_type: "pro".into(),
            provider: Provider::default(), // Anthropic
            credential: Some(test_credential()),
            upstream_url: None, model: None,
        }],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("{}/v1/chat/completions", proxy.url()))
        .header("content-type", "application/json")
        .json(&json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "Be concise."},
                {"role": "user", "content": "Say hi"}
            ]
        }))
        .send().await.unwrap();

    assert_eq!(resp.status(), 200, "cross-protocol proxy must return 200");

    // Upstream (Anthropic mock) must have received Anthropic-format body.
    let upstream_req: serde_json::Value = serde_json::from_slice(&caps.get(0).body).unwrap();
    assert!(upstream_req.get("messages").is_some());
    // system must be extracted from messages and put in top-level field
    assert!(upstream_req.get("system").is_some(), "system must be extracted: {upstream_req}");
    // model must be mapped to claude-*
    let sent_model = upstream_req["model"].as_str().unwrap();
    assert!(sent_model.starts_with("claude-"), "gpt-4o must map to claude-*, got: {sent_model}");

    // Client must receive OpenAI-format response.
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["choices"].as_array().is_some(), "response must have choices");
    assert!(body["choices"][0]["message"]["content"].is_string(), "message content must be string");
    assert!(body["choices"][0]["finish_reason"].is_string(), "must have finish_reason");
}

/// Mixed accounts: Anthropic account rate-limited → failover to OpenAI account with translation.
#[tokio::test]
async fn test_interop_failover_anthropic_to_openai() {
    let anthro_caps = Captures::default();
    let openai_caps = Captures::default();

    // Anthropic mock always returns 429
    let anthropic_up = TestServer::start(make_mock_upstream(anthro_caps.clone(), false, 429)).await;
    // OpenAI mock always returns 200
    let openai_up = TestServer::start(make_openai_upstream(openai_caps.clone(), false)).await;

    let cfg = Config {
        server: ServerConfig {
            upstream_url: anthropic_up.url(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            routing_strategy: RoutingStrategy::Carousel,
            request_timeout_secs: 1,
            ..ServerConfig::default()
        },
        accounts: vec![
            AccountConfig {
                name: "claude-account".into(),
                plan_type: "pro".into(),
                provider: Provider::default(), // Anthropic
                credential: Some(test_credential()),
                upstream_url: None, model: None,
            },
            openai_account(openai_up.url()),
        ],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };
    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .json(&json!({
            "model": "claude-sonnet-4-6",
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "Fallback test"}]
        }))
        .send().await.unwrap();

    assert_eq!(resp.status(), 200, "must succeed via OpenAI fallback");
    // Anthropic account was tried (and 429'd)
    assert_eq!(anthro_caps.len(), 1, "Anthropic account must have been tried");
    // OpenAI account was then used
    assert_eq!(openai_caps.len(), 1, "OpenAI account must have been used as fallback");
    // OpenAI received OpenAI-format body
    let openai_req: serde_json::Value = serde_json::from_slice(&openai_caps.get(0).body).unwrap();
    assert!(openai_req.get("messages").is_some());
    // Client got Anthropic-format response
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message", "client must receive Anthropic-format response");
}

// ---------------------------------------------------------------------------
// Live test — skipped unless ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN set
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_live_api() {
    // Accepts either an OAuth token (Claude Code) or API key
    let (token, is_bearer) =
        if let Ok(t) = std::env::var("CLAUDE_CODE_OAUTH_TOKEN") {
            (t, true)
        } else if let Ok(k) = std::env::var("ANTHROPIC_API_KEY") {
            (k, false)
        } else {
            eprintln!("CLAUDE_CODE_OAUTH_TOKEN or ANTHROPIC_API_KEY not set — skipping");
            return;
        };

    let credential = Credential::Oauth(OAuthCredential {
        email: None,
        access_token: if is_bearer { token.clone() } else { token.clone() },
        refresh_token: String::new(),
        expires_at: u64::MAX / 2,
        id_token: None,
        chatgpt_account_id: None,
        chatgpt_account_is_fedramp: false,
    });

    let cfg = Config {
        server: ServerConfig {
            upstream_url: "https://api.anthropic.com".into(),
            host: "127.0.0.1".into(),
            port: 0,
            log_level: "error".into(),
            ..ServerConfig::default()
        },
        accounts: vec![AccountConfig { name: "live".into(), plan_type: "pro".into(), provider: Provider::default(), credential: Some(credential), upstream_url: None, model: None }],
        config_file: std::path::PathBuf::from("/dev/null"),
        model_mapping: Default::default(),
        api_overflow: Default::default(),
        schema_version: 1,
        pools: Default::default(),
        secrets: Default::default(),
        classifier: Default::default(),
        bridge: Default::default(),
    };

    let (app, _, _) = create_app_with_state(cfg, StateStore::new_empty(), None).unwrap();
    let proxy = TestServer::start(app).await;
    let client = Client::new();

    let resp = client
        .post(format!("{}/v1/messages", proxy.url()))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
        .json(&json!({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "Reply with exactly: OK"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200, "body: {}", resp.text().await.unwrap());
}

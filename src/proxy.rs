use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex as ParkingMutex;

use axum::extract::{Path as AxumPath, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use serde_json::json;
use sha2::Digest;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::{state_path, Config, CredentialsStore};
use crate::credential::Credential;
use crate::forwarder::Forwarder;
use crate::provider::Provider;
use crate::quota;
use crate::router;
use crate::state::{RateLimitInfo, StateStore};
use crate::telemetry::{SupabaseTelemetry, TelemetryClient};

/// 100 MB limit — sufficient for any LLM request including large context windows.
const MAX_REQUEST_BODY: usize = 100 * 1024 * 1024;

/// Process-wide admission limiter. The proxy app and the control app (which
/// serves `/status`) are built by separate `build_app_state` calls, and every
/// per-provider proxy app must share one AIMD view of the pool — so the limiter
/// lives here, cloned (Arc-backed) into each AppState rather than created per app.
static ADMISSION: std::sync::OnceLock<crate::limiter::AdmissionLimiter> = std::sync::OnceLock::new();

fn shared_admission() -> crate::limiter::AdmissionLimiter {
    ADMISSION.get_or_init(crate::limiter::AdmissionLimiter::new).clone()
}

/// Process-wide warm-start map, shared across provider apps (same rationale as
/// the admission limiter — one view of per-session request counts).
static WARM_START: std::sync::OnceLock<Arc<ParkingMutex<HashMap<String, (u64, u64)>>>> = std::sync::OnceLock::new();

fn shared_warm_start() -> Arc<ParkingMutex<HashMap<String, (u64, u64)>>> {
    WARM_START.get_or_init(|| Arc::new(ParkingMutex::new(HashMap::new()))).clone()
}

/// Decide whether this request should warm-start on the API overflow lane:
/// true while the session (identified by `trace`) is within its first
/// `warmup_requests` OR younger than `warmup_ms`. Increments the per-trace
/// counter as a side effect. Requests without a trace never warm-start.
fn warm_start_active(
    warm: &ParkingMutex<HashMap<String, (u64, u64)>>,
    trace: Option<&str>,
    warmup_requests: u64,
    warmup_ms: u64,
) -> bool {
    let Some(trace) = trace else { return false };
    let now = now_ms();
    let mut map = warm.lock();
    let entry = map.entry(trace.to_owned()).or_insert((0, now));
    let (served, first_seen) = *entry;
    entry.0 = served.saturating_add(1);
    // Opportunistic prune to bound memory (sessions are short-lived).
    if map.len() > 4096 {
        map.retain(|_, (_, seen)| now.saturating_sub(*seen) < 3_600_000);
    }
    served < warmup_requests || now.saturating_sub(first_seen) < warmup_ms
}

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    forwarder: Arc<Forwarder>,
    state: StateStore,
    /// Live credentials — can be refreshed at runtime without restarting.
    credentials: Arc<RwLock<HashMap<String, Credential>>>,
    /// Per-account mutex that serialises concurrent token-refresh attempts.
    ///
    /// When multiple in-flight requests hit a 401 for the same account at the
    /// same time, only one should call the upstream OAuth endpoint; the others
    /// should wait and then re-use the fresh token instead of each making their
    /// own refresh call (which would rotate the refresh_token out from under the
    /// others and cause cascading auth failures).
    refresh_locks: Arc<ParkingMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Epoch-ms when this proxy instance started.
    started_ms: u64,
    /// If set, /v1/chat/completions requests are translated and forwarded here
    /// (the Anthropic proxy base URL, e.g. "http://127.0.0.1:8082").
    anthropic_base_url: Option<String>,
    /// Optional relay-server telemetry client.
    telemetry: Option<TelemetryClient>,
    /// Optional Supabase telemetry client.
    supabase: Option<Arc<SupabaseTelemetry>>,
    /// Per-IP token-bucket rate limiter (#16). None when rate_limit_rpm == 0.
    rate_limiter: Option<Arc<ParkingMutex<HashMap<IpAddr, TokenBucket>>>>,
    /// Adaptive per-account admission control (AIMD). Paces requests under each
    /// account's burst ceiling so the pool doesn't synchronise into a 429 storm.
    admission: crate::limiter::AdmissionLimiter,
    /// Warm-start bookkeeping keyed by `x-shunt-trace`: (requests_served,
    /// first_seen_ms). Lets a session's first prompts route to the API overflow
    /// lane for fast startup, then graduate to the subscription pool.
    warm_start: Arc<ParkingMutex<HashMap<String, (u64, u64)>>>,
    /// Codex turn/session affinity. `x-codex-turn-state` entries are strict;
    /// session/thread/prompt-cache entries are soft hints.
    codex_affinity: Arc<ParkingMutex<HashMap<String, (String, u64)>>>,
    codex_models_cache: Arc<ParkingMutex<Option<(Bytes, String, u64)>>>,
}

/// Simple token-bucket for per-IP rate limiting.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64) -> Self {
        Self { tokens: capacity, last_refill: Instant::now() }
    }

    /// Refill tokens proportional to elapsed time, then consume one.
    /// Returns true if the request is allowed.
    fn check_and_consume(&mut self, rpm: f64) -> bool {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        self.last_refill = Instant::now();
        // Refill at rpm/60 tokens per second; cap at burst (10 tokens).
        let burst = (rpm / 6.0).max(10.0);
        self.tokens = (self.tokens + elapsed * rpm / 60.0).min(burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// RAII release of an admission slot. Dropped on every exit path of a request
/// loop iteration (success return, retry `continue`, or error `?`), so a slot is
/// never leaked regardless of how the attempt ends.
struct SlotGuard {
    limiter: crate::limiter::AdmissionLimiter,
    account: String,
}

struct BudgetGuard {
    state: StateStore,
    id: Option<String>,
}

impl BudgetGuard {
    fn new(state: &StateStore, id: String) -> Self { Self { state: state.clone(), id: Some(id) } }
    fn handoff(mut self) -> Option<String> { self.id.take() }
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() { self.state.release_budget_reservation(&id); }
    }
}

impl SlotGuard {
    fn new(limiter: crate::limiter::AdmissionLimiter, account: String) -> Self {
        Self { limiter, account }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.limiter.release(&self.account);
    }
}

pub fn create_app(config: Config) -> anyhow::Result<Router> {
    let (app, _, _) = create_app_with_state(config, StateStore::load(&state_path()), None)?;
    Ok(app)
}

/// Shared live credentials map — can be written to without restarting the proxy.
pub type LiveCredentials = Arc<RwLock<HashMap<String, Credential>>>;

/// Create a pure proxy app (no management routes).
/// Registers /v1/messages, /v1/chat/completions, /v1/models, and a fallback.
/// Build a shared `AppState` and the `LiveCredentials` handle it references.
fn build_app_state(
    config: Config,
    state: StateStore,
    anthropic_base_url: Option<String>,
    supabase: Option<Arc<SupabaseTelemetry>>,
) -> anyhow::Result<(AppState, LiveCredentials)> {
    // Production sharing: proxy app and control app are built by separate calls
    // and must share one AIMD/warm-start view, hence the process-wide statics.
    build_app_state_with(config, state, anthropic_base_url, supabase, shared_admission(), shared_warm_start())
}

/// Like `build_app_state` but with an explicit admission limiter + warm-start
/// map, so the combined single-process app (tests, single-port mode) can use a
/// FRESH pair per app for isolation instead of the process-wide statics.
fn build_app_state_with(
    config: Config,
    state: StateStore,
    anthropic_base_url: Option<String>,
    supabase: Option<Arc<SupabaseTelemetry>>,
    admission: crate::limiter::AdmissionLimiter,
    warm_start: Arc<ParkingMutex<HashMap<String, (u64, u64)>>>,
) -> anyhow::Result<(AppState, LiveCredentials)> {
    let forwarder = Forwarder::new(config.server.request_timeout_secs)?;

    for a in &config.accounts {
        if a.provider.auth_kind() == crate::provider::AuthKind::None {
            // Local providers never need credentials — clear any stale auth_failed from disk.
            state.clear_auth_failed(&a.name);
        } else if a.credential.is_none() {
            state.set_auth_failed(&a.name);
        }
    }

    let credentials: LiveCredentials = Arc::new(RwLock::new(
        config.accounts.iter()
            .filter_map(|a| a.credential.as_ref().map(|c| (a.name.clone(), c.clone())))
            .collect::<HashMap<_, _>>(),
    ));

    let telemetry = config.server.telemetry_url.as_deref().map(|url| {
        TelemetryClient::new(url, config.server.telemetry_token.clone(), config.server.instance_name.clone())
    });

    let rate_limiter = if config.server.rate_limit_rpm > 0 {
        Some(Arc::new(ParkingMutex::new(HashMap::<IpAddr, TokenBucket>::new())))
    } else {
        None
    };

    let app_state = AppState {
        config: Arc::new(config),
        forwarder: Arc::new(forwarder),
        state,
        credentials: Arc::clone(&credentials),
        refresh_locks: Arc::new(ParkingMutex::new(HashMap::new())),
        started_ms: now_ms(),
        anthropic_base_url,
        telemetry,
        supabase,
        rate_limiter,
        admission,
        warm_start,
        codex_affinity: Arc::new(ParkingMutex::new(HashMap::new())),
        codex_models_cache: Arc::new(ParkingMutex::new(None)),
    };

    Ok((app_state, credentials))
}

pub fn create_proxy_app(
    config: Config,
    state: StateStore,
    anthropic_base_url: Option<String>,
    supabase: Option<Arc<SupabaseTelemetry>>,
) -> anyhow::Result<(Router, LiveCredentials)> {
    let (app_state, credentials) = build_app_state(config, state, anthropic_base_url, supabase)?;

    let app = Router::new()
        .route("/backend-api/codex/responses", post(codex_handler))
        .route("/backend-api/codex/responses/compact", post(codex_handler))
        .route("/backend-api/codex/models", get(codex_handler))
        .route("/backend-api/codex/memories/trace_summarize", post(codex_handler))
        .route("/v1/messages", post(proxy_handler))
        .route("/v1/messages/count_tokens", post(proxy_handler))
        .route("/v1/chat/completions", post(openai_compat_handler))
        .route("/v1/models", get(openai_models_handler))
        .fallback(proxy_handler)
        .with_state(app_state);

    Ok((app, credentials))
}

/// Create a control plane app (management routes only — sees ALL accounts).
/// Registers /health, /status, /use.
pub fn create_control_app(
    config: Config,
    state: StateStore,
) -> anyhow::Result<Router> {
    let (app_state, _) = build_app_state(config, state, None, None)?;

    let app = Router::new()
        .route("/health", get(health))
        .route("/status", get(status_handler))
        .route("/use", post(use_handler))
        .route("/pools/:pool/status", get(pool_status_handler))
        .route("/pools/:pool/use", post(pool_use_handler))
        .route("/pools/:pool/model", get(pool_model_get_handler).post(pool_model_set_handler).delete(pool_model_clear_handler))
        .route("/pools/:pool/strategy", get(pool_strategy_get_handler).post(pool_strategy_set_handler).delete(pool_strategy_clear_handler))
        .route("/bridge/tools/:tool", post(bridge_tool_handler))
        .route("/model", get(model_get_handler).post(model_set_handler).delete(model_clear_handler))
        .route("/strategy", get(strategy_get_handler).post(strategy_set_handler).delete(strategy_clear_handler))
        .route("/burst-limit", get(burst_limit_get_handler).post(burst_limit_set_handler).delete(burst_limit_clear_handler))
        .route("/fallback", get(fallback_get_handler).post(fallback_set_handler).delete(fallback_clear_handler))
        .route("/effort", get(effort_get_handler).post(effort_set_handler).delete(effort_clear_handler))
        .route("/thinking", get(thinking_get_handler).post(thinking_set_handler).delete(thinking_clear_handler))
        .route("/alerts", get(alerts_get_handler).post(alerts_set_handler))
        .with_state(app_state);

    Ok(app)
}

/// Combined app used by tests and the single-port fallback mode.
/// Includes both proxy routes and management routes (/health, /status, /use)
/// sharing a single AppState so state changes are visible across all routes.
pub fn create_app_with_state(
    config: Config,
    state: StateStore,
    anthropic_base_url: Option<String>,
) -> anyhow::Result<(Router, LiveCredentials, Option<TelemetryClient>)> {
    // Combined single-process app: use a FRESH admission limiter + warm-start map
    // (not the process-wide statics) so in-process tests are isolated from each other.
    let (app_state, credentials) = build_app_state_with(
        config, state, anthropic_base_url, None,
        crate::limiter::AdmissionLimiter::new(),
        Arc::new(ParkingMutex::new(HashMap::new())),
    )?;
    let telemetry = app_state.telemetry.clone();

    let app = Router::new()
        // Management routes
        .route("/health", get(health))
        .route("/status", get(status_handler))
        .route("/use", post(use_handler))
        .route("/model", get(model_get_handler).post(model_set_handler).delete(model_clear_handler))
        .route("/strategy", get(strategy_get_handler).post(strategy_set_handler).delete(strategy_clear_handler))
        .route("/burst-limit", get(burst_limit_get_handler).post(burst_limit_set_handler).delete(burst_limit_clear_handler))
        .route("/fallback", get(fallback_get_handler).post(fallback_set_handler).delete(fallback_clear_handler))
        .route("/effort", get(effort_get_handler).post(effort_set_handler).delete(effort_clear_handler))
        .route("/thinking", get(thinking_get_handler).post(thinking_set_handler).delete(thinking_clear_handler))
        .route("/alerts", get(alerts_get_handler).post(alerts_set_handler))
        // Proxy routes
        .route("/backend-api/codex/responses", post(codex_handler))
        .route("/backend-api/codex/responses/compact", post(codex_handler))
        .route("/backend-api/codex/models", get(codex_handler))
        .route("/backend-api/codex/memories/trace_summarize", post(codex_handler))
        .route("/v1/messages", post(proxy_handler))
        .route("/v1/messages/count_tokens", post(proxy_handler))
        .route("/v1/chat/completions", post(openai_compat_handler))
        .route("/v1/models", get(openai_models_handler))
        .fallback(proxy_handler)
        .with_state(app_state);

    Ok((app, credentials, telemetry))
}

/// Build a status JSON snapshot from config + state — used by the heartbeat loop.
pub fn build_status_snapshot(config: &Config, state: &StateStore, started_ms: u64) -> serde_json::Value {
    let account_states = state.account_states();
    let rate_limits    = state.rate_limit_snapshot();

    let accounts: Vec<_> = config.accounts.iter().map(|a| {
        let st            = account_states.get(&a.name);
        let rl            = rate_limits.get(&a.name);
        let utilization_5h = rl.and_then(|r| r.utilization_5h).unwrap_or(0.0);
        let utilization_7d = rl.and_then(|r| r.utilization_7d).unwrap_or(0.0);
        let reset_5h       = rl.and_then(|r| r.reset_5h);
        let reset_7d       = rl.and_then(|r| r.reset_7d);
        let disabled       = st.map(|s| s.disabled).unwrap_or(false);
        let auth_failed    = st.map(|s| s.auth_failed).unwrap_or(false);
        let health_check_failed = st.map(|s| s.health_check_failed).unwrap_or(false);
        let cooldown_until_ms = st.map(|s| s.cooldown_until_ms).unwrap_or(0);
        let available      = state.is_available(&a.name);
        let email          = a.credential.as_ref().and_then(|c| c.email()).map(|e| e.to_owned());

        json!({
            "name": a.name,
            "email": email,
            "provider": a.provider.to_string(),
            "available": available,
            "disabled": disabled,
            "auth_failed": auth_failed,
            "health_check_failed": health_check_failed,
            "cooldown_until_ms": cooldown_until_ms,
            "utilization_5h": utilization_5h,
            "reset_5h": reset_5h,
            "utilization_7d": utilization_7d,
            "reset_7d": reset_7d,
        })
    }).collect();

    json!({
        "started_ms": started_ms,
        "accounts": accounts,
        "pinned_account": state.get_pinned(),
        "last_used_account": state.get_last_used(),
    })
}

async fn health() -> impl IntoResponse {
    axum::Json(json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

fn scoped_pool_state(mut state: AppState, pool: &str) -> Result<AppState, Response> {
    let pool_kind = match pool {
        "claude" => crate::config::PoolKind::Claude,
        "codex" => crate::config::PoolKind::Codex,
        _ => return Err((StatusCode::NOT_FOUND, axum::Json(json!({"error":"unknown pool"}))).into_response()),
    };
    let mut config = (*state.config).clone();
    config.accounts.retain(|account| match pool_kind {
        crate::config::PoolKind::Claude => matches!(account.provider, Provider::Anthropic | Provider::AnthropicApi),
        crate::config::PoolKind::Codex => matches!(account.provider, Provider::OpenAI | Provider::OpenAIApi),
        crate::config::PoolKind::Legacy => false,
    });
    match pool_kind {
        crate::config::PoolKind::Claude => {
            config.api_overflow = config.pools.claude.overflow.clone();
            config.server.routing_strategy = config.pools.claude.routing_strategy;
        }
        crate::config::PoolKind::Codex => {
            config.api_overflow = config.pools.codex.overflow.clone();
            config.server.routing_strategy = config.pools.codex.routing_strategy;
        }
        crate::config::PoolKind::Legacy => {}
    }
    state.config = Arc::new(config);
    state.state = state.state.scoped(pool);
    Ok(state)
}

async fn pool_status_handler(AxumPath(pool): AxumPath<String>, State(state): State<AppState>) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => status_handler(State(state)).await.into_response(),
        Err(response) => response,
    }
}

async fn pool_use_handler(
    AxumPath(pool): AxumPath<String>, State(state): State<AppState>, body: axum::Json<serde_json::Value>,
) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => use_handler(State(state), body).await,
        Err(response) => response,
    }
}

async fn pool_model_get_handler(AxumPath(pool): AxumPath<String>, State(state): State<AppState>) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => model_get_handler(State(state)).await.into_response(),
        Err(response) => response,
    }
}

async fn pool_model_set_handler(
    AxumPath(pool): AxumPath<String>, State(state): State<AppState>, body: axum::Json<serde_json::Value>,
) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => model_set_handler(State(state), body).await,
        Err(response) => response,
    }
}

async fn pool_model_clear_handler(AxumPath(pool): AxumPath<String>, State(state): State<AppState>) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => model_clear_handler(State(state)).await.into_response(),
        Err(response) => response,
    }
}

async fn pool_strategy_get_handler(AxumPath(pool): AxumPath<String>, State(state): State<AppState>) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => strategy_get_handler(State(state)).await.into_response(),
        Err(response) => response,
    }
}

async fn pool_strategy_set_handler(
    AxumPath(pool): AxumPath<String>, State(state): State<AppState>, body: axum::Json<serde_json::Value>,
) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => strategy_set_handler(State(state), body).await,
        Err(response) => response,
    }
}

async fn pool_strategy_clear_handler(AxumPath(pool): AxumPath<String>, State(state): State<AppState>) -> Response {
    match scoped_pool_state(state, &pool) {
        Ok(state) => strategy_clear_handler(State(state)).await.into_response(),
        Err(response) => response,
    }
}

async fn bridge_tool_handler(
    AxumPath(tool): AxumPath<String>, State(state): State<AppState>,
    headers: axum::http::HeaderMap, axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let bearer = headers.get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let expected = crate::config::local_client_token("bridge").ok();
    let authorized = matches!((expected.as_deref(), bearer), (Some(expected), Some(actual)) if expected == actual);
    if !authorized {
        return (StatusCode::UNAUTHORIZED, axum::Json(json!({"error":"invalid bridge token"}))).into_response();
    }
    let caller = body.get("caller").and_then(|v| v.as_str()).unwrap_or("unknown");
    let arguments = body.get("arguments").cloned().unwrap_or_else(|| json!({}));
    let depth = body.get("depth").and_then(|v| v.as_u64()).unwrap_or(0).min(u8::MAX as u64) as u8;
    match crate::bridge::dispatch_tool(&tool, caller, arguments, depth, Some(&state.config.config_file)).await {
        Ok(result) => axum::Json(result).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, axum::Json(json!({"error": error.to_string()}))).into_response(),
    }
}

async fn status_handler(State(s): State<AppState>) -> impl IntoResponse {
    let account_states = s.state.account_states();
    let quotas = s.state.quota_snapshot();
    let rate_limits = s.state.rate_limit_snapshot();
    let admission = s.admission.snapshot();

    let accounts: Vec<_> = s.config.accounts.iter().map(|a| {
        let st = account_states.get(&a.name);
        let avail_status = if st.map(|s| s.auth_failed).unwrap_or(false) {
            "reauth_required"
        } else if st.map(|s| s.disabled).unwrap_or(false) {
            "disabled"
        } else if st.map(|s| s.health_check_failed).unwrap_or(false) {
            "unhealthy"
        } else if s.state.is_available(&a.name) {
            "available"
        } else {
            "cooling"
        };

        let quota = quotas.get(&a.name);
        let window_expires_ms = quota.and_then(|q| q.window_expires_ms());
        let window_expires_ms = window_expires_ms.filter(|&e| e > now_ms());
        let tokens_used = quota.map(|q| json!({
            "input": q.input_tokens,
            "output": q.output_tokens,
            "total": q.total_tokens(),
        }));

        let rl = rate_limits.get(&a.name);
        let rate_limit = rl.map(|r| json!({
            "utilization_5h": r.utilization_5h,
            "reset_5h": r.reset_5h,
            "status_5h": r.status_5h,
            "utilization_7d": r.utilization_7d,
            "reset_7d": r.reset_7d,
            "status_7d": r.status_7d,
            "representative_claim": r.representative_claim,
            "updated_ms": r.updated_ms,
        }));

        let acc_state = account_states.get(&a.name);
        let email = a.credential.as_ref().and_then(|c| c.email()).map(|e| e.to_owned());
        let disabled = acc_state.map(|s| s.disabled).unwrap_or(false);
        let auth_failed = acc_state.map(|s| s.auth_failed).unwrap_or(false);
        let health_check_failed = acc_state.map(|s| s.health_check_failed).unwrap_or(false);
        let cooldown_until_ms = acc_state.map(|s| s.cooldown_until_ms).unwrap_or(0);
        let utilization_5h = rl.and_then(|r| r.utilization_5h).unwrap_or(0.0);
        let reset_5h = rl.and_then(|r| r.reset_5h);
        let status_5h = rl.and_then(|r| r.status_5h.clone());
        let utilization_7d = rl.and_then(|r| r.utilization_7d).unwrap_or(0.0);
        let reset_7d = rl.and_then(|r| r.reset_7d);
        let status_7d = rl.and_then(|r| r.status_7d.clone());
        let available = s.state.is_available(&a.name);
        // AIMD admission state: current in-flight, adaptive concurrency limit,
        // and the decaying recent-429 signal. Surfaces pool pressure per lane.
        let adm = admission.get(&a.name);
        let admission_json = adm.map(|(in_flight, limit, recent_429)| json!({
            "in_flight": in_flight,
            "limit": limit,
            "recent_429": recent_429,
        }));

        json!({
            "name": a.name,
            "email": email,
            "plan_type": a.plan_type,
            "provider": a.provider.to_string(),
            "status": avail_status,
            "available": available,
            "disabled": disabled,
            "auth_failed": auth_failed,
            "health_check_failed": health_check_failed,
            "cooldown_until_ms": cooldown_until_ms,
            "utilization_5h": utilization_5h,
            "reset_5h": reset_5h,
            "status_5h": status_5h,
            "utilization_7d": utilization_7d,
            "reset_7d": reset_7d,
            "status_7d": status_7d,
            "window_expires_ms": window_expires_ms,
            "tokens_used": tokens_used,
            "rate_limit": rate_limit,
            "admission": admission_json,
        })
    }).collect();

    let recent_requests = s.state.recent_requests_snapshot();
    let savings = s.state.savings_snapshot();

    // Effective free capacity: the "not cooling" green count overstates usable
    // capacity because a just-recovered lane re-trips instantly. Count only
    // subscription accounts that are available AND have a free AIMD slot AND a
    // low recent-429 signal — the lanes that will actually serve without a storm.
    const EFFECTIVE_FREE_MAX_RECENT_429: f64 = 0.5;
    let reserved = [s.config.server.classifier_account.as_deref(),
                    s.config.server.classifier_fallback_account.as_deref()];
    let apparent_free = s.config.accounts.iter()
        .filter(|a| s.state.is_available(&a.name))
        .count();
    let effective_free = s.config.accounts.iter()
        .filter(|a| !reserved.iter().flatten().any(|r| *r == a.name.as_str()))
        .filter(|a| s.state.is_available(&a.name)
            && s.admission.has_free_slot(&a.name)
            && s.admission.recent_429(&a.name) < EFFECTIVE_FREE_MAX_RECENT_429)
        .count();

    axum::Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "started_ms": s.started_ms,
        "accounts": accounts,
        "pinned_account": s.state.get_pinned(),
        "last_used_account": s.state.get_last_used(),
        "recent_requests": recent_requests,
        "savings": savings,
        "classifier_account": s.config.server.classifier_account,
        "apparent_free": apparent_free,
        "effective_free": effective_free,
    }))
}

async fn use_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let account = body["account"].as_str().map(|s| s.to_owned());
    // Validate the account name exists (unless clearing to auto)
    if let Some(ref name) = account {
        if name != "auto" && !s.config.accounts.iter().any(|a| &a.name == name) {
            return (StatusCode::BAD_REQUEST, axum::Json(json!({
                "error": format!("unknown account '{name}'")
            }))).into_response();
        }
        let pinned = if name == "auto" { None } else { Some(name.clone()) };
        s.state.set_pinned(pinned);
        axum::Json(json!({ "pinned": name })).into_response()
    } else {
        s.state.set_pinned(None);
        axum::Json(json!({ "pinned": null })).into_response()
    }
}

async fn model_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    let model = s.state.get_model_override();
    axum::Json(json!({ "model": model }))
}

async fn model_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let Some(model) = body["model"].as_str() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing model field" }))).into_response();
    };
    s.state.set_model_override(model.to_owned());
    info!(model, "model override set");
    axum::Json(json!({ "model": model })).into_response()
}

async fn model_clear_handler(State(s): State<AppState>) -> impl IntoResponse {
    s.state.clear_model_override();
    info!("model override cleared");
    axum::Json(json!({ "model": null }))
}

async fn strategy_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    let (strategy_str, source) = match s.state.get_routing_strategy() {
        Some(st) => (st.as_str(), "override"),
        None => (s.config.server.routing_strategy.as_str(), "config"),
    };
    axum::Json(json!({ "strategy": strategy_str, "source": source }))
}

async fn strategy_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let Some(name) = body["strategy"].as_str() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing strategy field" }))).into_response();
    };
    let Some(strategy) = crate::config::RoutingStrategy::from_str(name) else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": format!("unknown strategy '{name}'") }))).into_response();
    };
    s.state.set_routing_strategy(strategy);
    info!(strategy = name, "routing strategy override set");
    axum::Json(json!({ "strategy": strategy.as_str(), "source": "override" })).into_response()
}

async fn strategy_clear_handler(State(s): State<AppState>) -> impl IntoResponse {
    s.state.clear_routing_strategy();
    info!("routing strategy override cleared");
    let strategy_str = s.config.server.routing_strategy.as_str();
    axum::Json(json!({ "strategy": strategy_str, "source": "config" }))
}

// ── Burst RPM limit ────────────────────────────────────────────────────────

async fn burst_limit_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    let (limit, source) = match s.state.get_burst_rpm_limit_override() {
        Some(l) => (l, "override"),
        None => (s.config.server.burst_rpm_limit, if s.config.server.burst_rpm_limit == 10 { "default" } else { "config" }),
    };
    axum::Json(json!({ "burst_rpm_limit": limit, "source": source }))
}

async fn burst_limit_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let Some(limit) = body["burst_rpm_limit"].as_u64() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing burst_rpm_limit field (integer)" }))).into_response();
    };
    let limit = limit as u32;
    s.state.set_burst_rpm_limit_override(limit);
    info!(limit, "burst RPM limit override set");
    axum::Json(json!({ "burst_rpm_limit": limit, "source": "override" })).into_response()
}

async fn burst_limit_clear_handler(State(s): State<AppState>) -> impl IntoResponse {
    s.state.clear_burst_rpm_limit_override();
    info!("burst RPM limit override cleared");
    let limit = s.config.server.burst_rpm_limit;
    axum::Json(json!({ "burst_rpm_limit": limit, "source": if limit == 10 { "default" } else { "config" } }))
}

// ── Fallback model ─────────────────────────────────────────────────────────

async fn fallback_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    match s.state.get_fallback_model_override() {
        Some(Some(model)) => axum::Json(json!({ "fallback_model": model, "source": "override" })),
        Some(None) => axum::Json(json!({ "fallback_model": null, "source": "override", "disabled": true })),
        None => match &s.config.server.fallback_model {
            Some(model) => axum::Json(json!({ "fallback_model": model, "source": "config" })),
            None => axum::Json(json!({ "fallback_model": "auto", "source": "auto" })),
        },
    }
}

async fn fallback_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    if body["fallback_model"].is_null() || body.get("disabled").and_then(|v| v.as_bool()) == Some(true) {
        s.state.set_fallback_model_override(None);
        info!("fallback model explicitly disabled");
        return axum::Json(json!({ "fallback_model": null, "source": "override", "disabled": true })).into_response();
    }
    let Some(model) = body["fallback_model"].as_str() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing fallback_model field" }))).into_response();
    };
    let model = model.to_owned();
    s.state.set_fallback_model_override(Some(model.clone()));
    info!(model = %model, "fallback model override set");
    axum::Json(json!({ "fallback_model": model, "source": "override" })).into_response()
}

async fn fallback_clear_handler(State(s): State<AppState>) -> impl IntoResponse {
    s.state.clear_fallback_model_override();
    info!("fallback model override cleared");
    match &s.config.server.fallback_model {
        Some(model) => axum::Json(json!({ "fallback_model": model, "source": "config" })),
        None => axum::Json(json!({ "fallback_model": "auto", "source": "auto" })),
    }
}

async fn effort_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    match s.state.get_effort_override() {
        Some(effort) => axum::Json(json!({ "effort": effort, "source": "override" })),
        None => axum::Json(json!({ "effort": null, "source": "passthrough" })),
    }
}

async fn effort_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let Some(effort) = body["effort"].as_str() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing effort string field" }))).into_response();
    };
    let valid = ["low", "medium", "high", "xhigh", "max"];
    if !valid.contains(&effort) {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "effort must be one of: low, medium, high, max" }))).into_response();
    }
    s.state.set_effort_override(effort.to_owned());
    info!(effort, "effort override set");
    axum::Json(json!({ "effort": effort, "source": "override" })).into_response()
}

async fn effort_clear_handler(State(s): State<AppState>) -> impl IntoResponse {
    s.state.clear_effort_override();
    info!("effort override cleared");
    axum::Json(json!({ "effort": null, "source": "passthrough" }))
}

async fn thinking_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    match s.state.get_thinking_override() {
        Some(mode) => axum::Json(json!({ "thinking": mode, "source": "override" })),
        None => axum::Json(json!({ "thinking": null, "source": "passthrough" })),
    }
}

async fn thinking_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let Some(mode) = body["thinking"].as_str() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing thinking string field" }))).into_response();
    };
    let valid = ["adaptive", "disabled"];
    if !valid.contains(&mode) {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "thinking must be one of: adaptive, disabled" }))).into_response();
    }
    s.state.set_thinking_override(mode.to_owned());
    info!(mode, "thinking override set");
    axum::Json(json!({ "thinking": mode, "source": "override" })).into_response()
}

async fn thinking_clear_handler(State(s): State<AppState>) -> impl IntoResponse {
    s.state.clear_thinking_override();
    info!("thinking override cleared");
    axum::Json(json!({ "thinking": null, "source": "passthrough" }))
}

async fn alerts_get_handler(State(s): State<AppState>) -> impl IntoResponse {
    let muted = s.state.get_alerts_muted();
    axum::Json(json!({ "muted": muted }))
}

async fn alerts_set_handler(
    State(s): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let Some(muted) = body["muted"].as_bool() else {
        return (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "missing muted bool field" }))).into_response();
    };
    s.state.set_alerts_muted(muted);
    info!(muted, "alerts mute state changed");
    axum::Json(json!({ "muted": muted })).into_response()
}

use crate::state::now_ms_pub as now_ms;

/// Extract client IP for rate limiting.
///
/// `X-Real-IP` is only trusted when `trust_proxy_headers` is explicitly enabled
/// in config — otherwise any client could spoof the header to rotate its bucket.
/// When not trusted (the default), all requests share a single loopback bucket,
/// giving a global RPM cap rather than a per-IP one.
fn extract_client_ip(req: &Request, trust_proxy_headers: bool) -> IpAddr {
    if trust_proxy_headers {
        if let Some(ip) = req.headers()
            .get("x-real-ip")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
        {
            return ip;
        }
    }
    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
}

fn codex_affinity_keys(headers: &axum::http::HeaderMap, body: &[u8]) -> (Option<String>, Vec<String>) {
    let strict = headers.get("x-codex-turn-state").and_then(|v| v.to_str().ok())
        .map(|v| format!("turn:{}", hex::encode(sha2::Sha256::digest(v.as_bytes()))));
    let mut soft = Vec::new();
    for name in ["session-id", "thread-id"] {
        if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok()) {
            soft.push(format!("{name}:{value}"));
        }
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) {
        for name in ["prompt_cache_key", "conversation_id"] {
            if let Some(value) = value.get(name).and_then(|v| v.as_str()) {
                soft.push(format!("{name}:{value}"));
            }
        }
    }
    (strict, soft)
}

fn codex_upstream(account: &crate::config::AccountConfig, local_path: &str) -> (String, String) {
    let base = account.upstream_url.as_deref()
        .unwrap_or_else(|| account.provider.default_upstream_url())
        .trim_end_matches('/').to_owned();
    let relative = local_path.strip_prefix("/backend-api/codex").unwrap_or(local_path);
    match account.provider {
        Provider::OpenAI => {
            if base.ends_with("/backend-api/codex") {
                (base, relative.to_owned())
            } else {
                (base, format!("/backend-api/codex{relative}"))
            }
        }
        Provider::OpenAIApi => (base, format!("/v1{relative}")),
        _ => (base, relative.to_owned()),
    }
}

async fn codex_guardian_response(s: &AppState, request_body: &[u8]) -> Option<Response> {
    if !s.config.classifier.enabled { return None; }
    let upstream = s.config.classifier.upstream_url.as_deref()?;
    let classifier_model = s.config.classifier.model.as_deref()?;
    let start = request_body.len().saturating_sub(64 * 1024);
    let transcript = String::from_utf8_lossy(&request_body[start..]);
    let prompt = format!(
        "You are a fail-closed coding-agent safety reviewer. Review the following Codex auto-review request. Return only JSON with outcome (allow or deny), risk_level, user_authorization, and rationale. If uncertain, deny.\n\n{transcript}");
    let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(20)).build().ok()?;
    let result = client.post(format!("{}/v1/chat/completions", upstream.trim_end_matches('/')))
        .json(&json!({"model": classifier_model, "messages": [{"role":"user","content":prompt}], "stream":false}))
        .send().await.ok().and_then(|r| if r.status().is_success() { Some(r) } else { None });
    let content = match result {
        Some(response) => response.json::<serde_json::Value>().await.ok()
            .and_then(|v| v.pointer("/choices/0/message/content").and_then(|v| v.as_str()).map(ToOwned::to_owned)),
        None => None,
    };
    let mut verdict = content.as_deref().and_then(|text| {
        let trimmed = text.trim().trim_start_matches("```json").trim_start_matches("```").trim_end_matches("```").trim();
        serde_json::from_str::<serde_json::Value>(trimmed).ok()
    }).unwrap_or_else(|| json!({
        "outcome":"deny", "risk_level":"high", "user_authorization":"low",
        "rationale":"Local guardian classifier failed closed."
    }));
    if verdict.get("outcome").and_then(|v| v.as_str()) != Some("allow") {
        verdict["outcome"] = serde_json::Value::String("deny".into());
    }
    let response_id = format!("resp_shunt_guardian_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let item_id = format!("msg_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let text = verdict.to_string();
    let events = [
        json!({"type":"response.created","response":{"id":response_id}}),
        json!({"type":"response.output_item.done","item":{"type":"message","role":"assistant","id":item_id,"content":[{"type":"output_text","text":text}]}}),
        json!({"type":"response.completed","response":{"id":response_id,"usage":{"input_tokens":0,"input_tokens_details":null,"output_tokens":0,"output_tokens_details":null,"total_tokens":0}}}),
    ];
    let mut sse = String::new();
    for event in events {
        let kind = event["type"].as_str().unwrap_or("response.completed");
        sse.push_str(&format!("event: {kind}\ndata: {event}\n\n"));
    }
    Some(Response::builder().status(200)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(axum::body::Body::from(sse)).expect("guardian response"))
}

async fn claude_classifier_response(s: &AppState, request_body: &[u8]) -> Option<Response> {
    if !s.config.classifier.enabled { return None; }
    let upstream = s.config.classifier.upstream_url.as_deref()?;
    let classifier_model = s.config.classifier.model.as_deref()?;
    let start = request_body.len().saturating_sub(CLASSIFIER_MAX_TRANSCRIPT_CHARS);
    let transcript = String::from_utf8_lossy(&request_body[start..]);
    let prompt = format!(
        "Review this coding-agent action for safety. Return exactly <block>yes</block> to block it or <block>no</block> to allow it. Fail closed when uncertain.\n\n{transcript}");
    let content = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
    {
        Ok(client) => match client.post(format!("{}/v1/chat/completions", upstream.trim_end_matches('/')))
            .json(&json!({"model":classifier_model,"messages":[{"role":"user","content":prompt}],"stream":false}))
            .send().await {
            Ok(response) if response.status().is_success() => response.json::<serde_json::Value>().await.ok()
                .and_then(|v| v.pointer("/choices/0/message/content").and_then(|v| v.as_str()).map(ToOwned::to_owned)),
            _ => None,
        },
        Err(_) => None,
    };
    let verdict = match content.as_deref() {
        Some(text) if text.contains("<block>no</block>") => "<block>no</block>",
        Some(text) if text.contains("<block>yes</block>") => "<block>yes</block>",
        _ if s.config.classifier.fail_closed => "<block>yes</block>",
        _ => return None,
    };
    let streaming = serde_json::from_slice::<serde_json::Value>(request_body).ok()
        .and_then(|v| v.get("stream").and_then(|v| v.as_bool())).unwrap_or(false);
    let id = format!("msg_shunt_classifier_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    if streaming {
        let events = [
            ("message_start", json!({"type":"message_start","message":{"id":id,"type":"message","role":"assistant","model":classifier_model,"content":[],"stop_reason":null,"usage":{"input_tokens":0,"output_tokens":0}}})),
            ("content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})),
            ("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":verdict}})),
            ("content_block_stop", json!({"type":"content_block_stop","index":0})),
            ("message_delta", json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}})),
            ("message_stop", json!({"type":"message_stop"})),
        ];
        let mut body = String::new();
        for (event, data) in events { body.push_str(&format!("event: {event}\ndata: {data}\n\n")); }
        Some(Response::builder().status(200).header("content-type", "text/event-stream")
            .body(axum::body::Body::from(body)).expect("classifier SSE response"))
    } else {
        Some(axum::Json(json!({
            "id":id,"type":"message","role":"assistant","model":classifier_model,
            "content":[{"type":"text","text":verdict}],"stop_reason":"end_turn",
            "usage":{"input_tokens":0,"output_tokens":1}
        })).into_response())
    }
}

/// Stock Codex Responses transport. Subscription traffic is forwarded without
/// JSON/SSE translation; only API-overflow output caps may rewrite the body.
async fn codex_handler(State(s): State<AppState>, req: Request) -> Result<Response, ProxyError> {
    if let Some(ref expected) = s.config.server.remote_key {
        let api_key = req.headers().get("x-api-key").and_then(|v| v.to_str().ok()).unwrap_or("");
        let bearer = req.headers().get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
        let managed_matches = bearer.starts_with("shunt_")
            && crate::config::local_client_token("codex").ok().as_deref() == Some(bearer);
        if api_key != expected && bearer != expected && !managed_matches {
            return Err(ProxyError::Unauthorized);
        }
    }

    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let path_and_query = req.uri().path_and_query().map(|v| v.as_str()).unwrap_or(&path).to_owned();
    let headers = req.headers().clone();
    let backend_only = path.ends_with("/models") || path.ends_with("/memories/trace_summarize");
    if method == "GET" && path.ends_with("/models") {
        if let Some((bytes, content_type, expires)) = s.codex_models_cache.lock().clone() {
            if expires > now_ms() {
                return Ok(Response::builder().status(200).header("content-type", content_type)
                    .header("x-shunt-cache", "hit").body(axum::body::Body::from(bytes)).expect("cached models response"));
            }
        }
    }
    let mut body = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY).await
        .map_err(|_| ProxyError::BodyRead)?;
    let mut model = serde_json::from_slice::<serde_json::Value>(&body).ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(ToOwned::to_owned))
        .unwrap_or_default();
    if let Some(override_model) = s.state.get_model_override() {
        if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&body) {
            if value.get("model").is_some() {
                value["model"] = serde_json::Value::String(override_model.clone());
                body = Bytes::from(serde_json::to_vec(&value).map_err(|_| ProxyError::BodyRead)?);
                model = override_model;
            }
        }
    }
    if model == "codex-auto-review" {
        let local_configured = s.config.classifier.enabled
            && s.config.classifier.upstream_url.is_some()
            && s.config.classifier.model.is_some();
        if local_configured {
            if let Some(response) = codex_guardian_response(&s, &body).await { return Ok(response); }
            if s.config.classifier.fail_closed { return Err(ProxyError::AllAccountsUnavailable(None)); }
        }
    }
    let (strict_key, soft_keys) = codex_affinity_keys(&headers, &body);
    let now = now_ms();
    let (affinity_account, strict_bound) = {
        let mut map = s.codex_affinity.lock();
        map.retain(|_, (_, expires)| *expires > now);
        let strict = strict_key.as_ref().and_then(|key| map.get(key).map(|(account, _)| account.clone()));
        let strict_bound = strict.is_some();
        let account = strict.or_else(|| {
            soft_keys.iter().find_map(|key| map.get(key).map(|(account, _)| account.clone()))
        });
        (account, strict_bound)
    };
    let mut tried = HashSet::new();
    let mut refreshed = HashSet::new();
    let mut fallback_index = 0usize;

    loop {
        let overflow_name = s.config.api_overflow.account.as_deref();
        let bound_account = affinity_account.as_deref().and_then(|bound| {
            s.config.accounts.iter().find(|a| a.name == bound && !tried.contains(&a.name)
                && s.state.is_available(&a.name)
                && (!backend_only || a.provider == Provider::OpenAI))
        });
        let selected = bound_account.or_else(|| {
            if strict_bound { return None; }
            let mut excluded = tried.clone();
            if let Some(name) = overflow_name { excluded.insert(name.to_owned()); }
            let snapshot = s.state.routing_snapshot();
            let soft_fp = soft_keys.first().map(String::as_str);
            router::pick_account(
                &s.config.accounts, &s.state, &snapshot, soft_fp, &excluded,
                s.config.server.sticky_ttl_ms, s.config.server.expiry_soon_secs,
                s.config.server.routing_strategy, s.config.server.burst_rpm_limit, None,
            ).filter(|a| matches!(a.provider, Provider::OpenAI | Provider::OpenAIApi)
                && (!backend_only || a.provider == Provider::OpenAI))
                .or_else(|| {
                    if backend_only { return None; }
                    let name = overflow_name?;
                    s.config.accounts.iter().find(|a| a.name == name && !tried.contains(&a.name)
                        && a.provider == Provider::OpenAIApi && s.state.is_available(&a.name))
                })
        });
        let Some(account) = selected else {
            if path.ends_with("/responses") {
                if let Some(fallback) = s.config.pools.codex.fallback_models.get(fallback_index) {
                    if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&body) {
                        value["model"] = serde_json::Value::String(fallback.clone());
                        body = Bytes::from(serde_json::to_vec(&value).map_err(|_| ProxyError::BodyRead)?);
                        model = fallback.clone();
                        fallback_index += 1;
                        tried.clear();
                        continue;
                    }
                }
            }
            return Err(ProxyError::AllAccountsUnavailable(Some(5)));
        };
        let account_name = account.name.clone();
        let credential = s.credentials.read().await.get(&account_name).cloned()
            .or_else(|| account.credential.clone());
        let Some(credential) = credential else {
            s.state.set_auth_failed(&account_name);
            tried.insert(account_name);
            continue;
        };

        let mut reservation = None;
        if account.provider == Provider::OpenAIApi {
            let cap = s.config.api_overflow.max_output_tokens;
            if method == "POST" {
                let mut value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| ProxyError::BodyRead)?;
                let requested = value.get("max_output_tokens").and_then(|v| v.as_u64()).unwrap_or(cap);
                value["max_output_tokens"] = serde_json::Value::from(requested.min(cap));
                model = value.get("model").and_then(|v| v.as_str()).unwrap_or("").to_owned();
                body = Bytes::from(serde_json::to_vec(&value).map_err(|_| ProxyError::BodyRead)?);
            }
            let input_upper_bound = body.len() as u64;
            let Some(estimated) = crate::pricing::strict_api_cost_usd(&model, input_upper_bound, cap) else {
                warn!(%model, "rejecting Codex API overflow for unpriced model");
                return Err(ProxyError::AllAccountsUnavailable(Some(60)));
            };
            reservation = s.state.reserve_budget(&account_name, estimated, s.config.api_overflow.daily_budget_usd);
            if reservation.is_none() {
                warn!(account = %account_name, estimated_usd = estimated, "Codex API overflow daily budget reservation rejected");
                return Err(ProxyError::AllAccountsUnavailable(Some(60)));
            }
        }

        let (upstream, upstream_path) = codex_upstream(account, &path_and_query);
        let response = s.forwarder.forward_codex(
            &upstream, &method, &upstream_path, body.clone(), &headers, account, &credential,
        ).await;
        let mut response = match response {
            Ok(response) => response,
            Err(error) => {
                if let Some(id) = reservation.as_deref() { s.state.release_budget_reservation(id); }
                warn!(account = %account_name, error = %error, "Codex upstream failed before response headers");
                if strict_bound { return Err(ProxyError::Upstream); }
                tried.insert(account_name);
                continue;
            }
        };

        if let Some(rate) = account.provider.parse_rate_limits(response.headers()) {
            s.state.update_rate_limits(&account_name, rate);
        }
        let status = response.status().as_u16();
        if matches!(status, 401 | 429 | 500 | 502 | 503 | 504) {
            if let Some(id) = reservation.as_deref() { s.state.release_budget_reservation(id); }
            if status == 401 {
                if !refreshed.contains(&account_name) {
                    if let Some(oauth) = credential.as_oauth() {
                        if let Ok(Ok(fresh)) = tokio::time::timeout(
                            std::time::Duration::from_secs(20), account.provider.refresh_token(oauth),
                        ).await {
                            s.credentials.write().await.insert(account_name.clone(), Credential::Oauth(fresh.clone()));
                            let mut store = CredentialsStore::load();
                            store.insert_resolved(account_name.clone(), Credential::Oauth(fresh));
                            let _ = store.save();
                            s.state.clear_auth_failed(&account_name);
                            refreshed.insert(account_name);
                            continue;
                        }
                    }
                }
                s.state.set_auth_failed(&account_name);
            } else if status == 429 {
                s.state.set_cooldown(&account_name, parse_retry_after_ms(response.headers()).unwrap_or(5_000));
            }
            if strict_bound { return Ok(response); }
            tried.insert(account_name);
            continue;
        }

        if method == "GET" && path.ends_with("/models") && status == 200 {
            let content_type = response.headers().get("content-type").and_then(|v| v.to_str().ok())
                .unwrap_or("application/json").to_owned();
            let (parts, response_body) = response.into_parts();
            let bytes = axum::body::to_bytes(response_body, 16 * 1024 * 1024).await
                .map_err(|_| ProxyError::Upstream)?;
            *s.codex_models_cache.lock() = Some((bytes.clone(), content_type, now_ms() + 300_000));
            return Ok(Response::from_parts(parts, axum::body::Body::from(bytes)));
        }

        let expires = now_ms().saturating_add(s.config.server.sticky_ttl_ms);
        {
            let mut map = s.codex_affinity.lock();
            for key in &soft_keys { map.insert(key.clone(), (account_name.clone(), expires)); }
            if let Some(value) = response.headers().get("x-codex-turn-state").and_then(|v| v.to_str().ok()) {
                let key = format!("turn:{}", hex::encode(sha2::Sha256::digest(value.as_bytes())));
                map.insert(key, (account_name.clone(), now_ms() + 24 * 60 * 60 * 1_000));
            }
        }
        s.state.set_last_used(&account_name);
        let request_id = uuid::Uuid::new_v4().to_string()[..8].to_owned();
        response = tap_usage(
            response, &s.state, s.telemetry.as_ref(), s.supabase.as_ref(), &account_name,
            &account.provider.to_string(), &model, now, &request_id, &path, tried.len(), false, "",
            reservation,
        ).await;
        return Ok(response);
    }
}

async fn proxy_handler(
    State(s): State<AppState>,
    req: Request,
) -> Result<Response, ProxyError> {
    // Remote auth: if a remote_key is configured, the client must supply it as x-api-key.
    if let Some(ref expected) = s.config.server.remote_key {
        let provided = req.headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let managed_matches = provided.starts_with("shunt_")
            && crate::config::local_client_token("claude").ok().as_deref() == Some(provided);
        if provided != expected && !managed_matches {
            return Err(ProxyError::Unauthorized);
        }
    }

    // #16: per-IP rate limiting (token bucket, configurable via rate_limit_rpm).
    if let Some(ref rl) = s.rate_limiter {
        let ip = extract_client_ip(&req, s.config.server.trust_proxy_headers);
        let rpm = s.config.server.rate_limit_rpm as f64;
        let allowed = rl.lock().entry(ip).or_insert_with(|| TokenBucket::new(rpm)).check_and_consume(rpm);
        if !allowed {
            return Err(ProxyError::RateLimited);
        }
    }

    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let headers = req.headers().clone();
    // Correlation key: the eval-claude wrapper stamps each Claude Code request
    // with x-shunt-trace=<host>/<worktree>/<session>[/<agent>]. Recorded on the
    // request log for session<->shunt correlation, and used to gate warm-start.
    let trace_id = headers.get("x-shunt-trace")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let body_bytes: Bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY)
        .await
        .map_err(|_| ProxyError::BodyRead)?;

    // Apply model override: if set, patch the `model` field in the JSON body before forwarding.
    // Also strip unsupported params for models that don't support them (e.g. Haiku).
    // Parse once and reuse the value to extract the model name (avoids double-parse).
    // Set when this request is Claude Code's auto-mode safety-classifier side-call.
    // Threaded through routing (dedicated lane + fail-fast) and the request log.
    let mut is_classifier = false;
    let (mut body_bytes, model) = if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
        let mut changed = false;
        if let Some(override_model) = s.state.get_model_override() {
            if val.get("model").is_some() {
                val["model"] = serde_json::Value::String(override_model);
                changed = true;
            }
        }
        // Auto-mode safety classifier: Claude Code fires a separate classifier
        // call (its system prompt begins "You are a security monitor for
        // autonomous AI coding agents.") before gated Bash/Write/Edit tools. It
        // inherits the active model (often opus) and fails closed if that model
        // is unavailable — which, behind a pooling proxy, happens whenever the
        // pooled accounts are cooling, hard-blocking every gated tool. Pin it to
        // Haiku (always-available, cheap, still returns a valid verdict). Only
        // the model is rewritten; the `system` array — including the
        // `x-anthropic-billing-header:` block Anthropic uses to recognize Claude
        // Code OAuth traffic — is preserved so first-party recognition survives.
        if is_safety_classifier(&val) {
            is_classifier = true;
            // When a custom classifier prompt is configured, fully replace the
            // `system` field with our own harness rubric and let the dedicated
            // classifier lane's account `model` pin choose the model. The
            // replacement must still instruct the model to emit the
            // `<block>yes|no</block>` grammar Claude Code parses. If the file
            // cannot be read, leave Claude Code's original system prompt intact
            // (still a valid, parseable contract) rather than shipping an empty
            // prompt.
            if let Some(path) = s.config.server.classifier_system_prompt_path.as_deref() {
                match std::fs::read_to_string(path) {
                    Ok(prompt) if !prompt.trim().is_empty() => {
                        val["system"] = serde_json::Value::String(prompt);
                        changed = true;
                        // A self-hosted small model has a bounded context window
                        // (e.g. 32K). Claude Code's classifier transcript can be
                        // far larger; if the prompt overflows, the backend
                        // truncates it, the model loses the `<block>` instruction
                        // and answers in prose, and the unparseable verdict fails
                        // closed — silently blocking everything. The action under
                        // review is the LAST tool call at the end of the
                        // transcript, so keep the tail of any oversized text and
                        // drop the earlier history (context only).
                        if bound_classifier_transcript(&mut val, CLASSIFIER_MAX_TRANSCRIPT_CHARS) {
                            changed = true;
                        }
                    }
                    Ok(_) => {
                        warn!(path, "classifier_system_prompt_path is empty — keeping Claude Code's prompt");
                    }
                    Err(e) => {
                        warn!(path, error = %e, "failed to read classifier_system_prompt_path — keeping Claude Code's prompt");
                    }
                }
            } else if pin_model_to_classifier(&mut val) {
                // No custom prompt: preserve legacy behavior (pin to Haiku).
                changed = true;
            }
        }
        // Apply effort override: inject output_config.effort before simple-model stripping.
        if let Some(effort) = s.state.get_effort_override() {
            if val.get("output_config").is_none() {
                val["output_config"] = serde_json::json!({});
            }
            val["output_config"]["effort"] = serde_json::Value::String(effort);
            changed = true;
        }
        // Apply thinking mode override: inject thinking object before simple-model stripping.
        if let Some(thinking_mode) = s.state.get_thinking_override() {
            val["thinking"] = serde_json::json!({ "type": thinking_mode });
            changed = true;
        }
        let resolved_model = val["model"].as_str().unwrap_or("").to_owned();
        if is_simple_model(&resolved_model) && strip_unsupported_params_for_simple_model(&mut val) {
            changed = true;
        }
        let model = val["model"].as_str().unwrap_or("").to_owned();
        let bytes = if changed {
            Bytes::from(serde_json::to_vec(&val).unwrap_or_else(|_| body_bytes.to_vec()))
        } else {
            body_bytes
        };
        (bytes, model)
    } else {
        (body_bytes, String::new())
    };

    // Strip capability betas the outgoing model can't honor. Simple models (Haiku)
    // support neither extended thinking/effort nor a 1M context window, so when the
    // resolved model is simple — including the classifier pin to Haiku — drop those
    // beta flags. The 1M beta in particular otherwise triggers
    // `400 The long context beta is not yet available for this subscription`.
    let mut headers = headers;
    if is_classifier
        && s.config.classifier.upstream_url.is_some()
        && s.config.classifier.model.is_some()
    {
        if let Some(response) = claude_classifier_response(&s, &body_bytes).await { return Ok(response); }
    }
    if is_simple_model(&model) {
        strip_beta_header_for_simple_model(&mut headers);
        strip_long_context_beta(&mut headers);
    }

    let req_start_ms = now_ms();
    let request_id = uuid::Uuid::new_v4().to_string()[..8].to_owned();

    let fp = router::fingerprint(&body_bytes);
    let fp_ref = fp.as_deref();

    let mut tried: HashSet<String> = HashSet::new();
    // Accounts skipped this search only because their AIMD admission slots are
    // full (not failed). Excluded from selection until we either admit someone
    // or wait briefly for a slot to free, then cleared. Distinct from `tried` so
    // a busy-but-healthy account is never mistaken for a cooling/failed one.
    let mut saturated: HashSet<String> = HashSet::new();
    // Track accounts we've already attempted a token refresh for this request.
    let mut refreshed: HashSet<String> = HashSet::new();
    // Guard: only attempt model fallback once per request.
    let mut fell_back = false;
    // Guard: only self-heal a long-context 400 once per request (strip 1M beta + retry).
    let mut stripped_1m = false;

    // --- API overflow lane (budget-capped anthropic-api) --------------------
    // Resolve the configured overflow account name if the lane is enabled and the
    // account actually exists in the pool. It is reserved from normal routing
    // (added to `excluded`) and only reachable via warm-start or overflow spill.
    let ov = &s.config.api_overflow;
    let api_lane_name: Option<String> = if ov.enabled && !is_classifier {
        ov.account.clone()
            .filter(|n| s.config.accounts.iter().any(|a| &a.name == n))
    } else {
        None
    };
    // Warm-start decision (computed once — it increments the per-trace counter):
    // a session's first prompts prefer the API lane for fast time-to-first-token.
    let want_warm_start = api_lane_name.is_some()
        && warm_start_active(&s.warm_start, trace_id.as_deref(), ov.warmup_requests, ov.warmup_ms);
    // Closure: the API lane account IF currently usable (exists, not tried, not
    // disabled/auth-failed, and under today's USD budget). Availability by
    // cooldown is not required — API keys have high RPM ceilings.
    let api_lane_usable = |tried: &HashSet<String>| -> bool {
        let Some(name) = api_lane_name.as_deref() else { return false };
        if tried.contains(name) { return false; }
        if s.state.account_spend_today_usd(name) + s.state.reserved_today_usd(name) >= ov.daily_budget_usd { return false; }
        s.config.accounts.iter().any(|a| a.name == name && a.credential.is_some())
            && s.state.is_available(name)
    };

    // Bound the time a request may spend in the all-cooling wait loop. Previously
    // this was the full request timeout (600s), which is why prompts stalled for
    // tens of seconds then errored. Cap it to max_startup_wait_ms so we spill to
    // the API overflow lane or return fast 429+Retry-After instead of open-ended
    // waiting. The actual upstream forward still uses the full request timeout.
    let req_started_ms = now_ms();
    let wait_deadline_ms = req_started_ms + s.config.server.max_startup_wait_ms;

    loop {
        let effective_strategy = s.state.get_routing_strategy()
            .unwrap_or(s.config.server.routing_strategy);
        let mut snap = s.state.routing_snapshot();
        // Enrich the snapshot with the AIMD burst-429 signal so Maximus routing
        // deprioritizes lanes that are near their burst ceiling (the state store
        // doesn't own the limiter, so we fold it in here at the routing boundary).
        for (name, data) in snap.accounts.iter_mut() {
            data.recent_429 = s.admission.recent_429(name);
        }
        let effective_burst_rpm = s.state.get_burst_rpm_limit_override()
            .unwrap_or(s.config.server.burst_rpm_limit);

        // Auto-mode safety-classifier side-calls take a dedicated lane when one is
        // configured (`server.classifier_account`), bypassing the pooled OAuth
        // accounts entirely. When no lane is configured they fall through to the
        // normal pool but still fail fast below (never the cooldown wait), because
        // Claude Code's classifier has a short client-side deadline and fails
        // closed — a delayed answer is as good as no answer.
        let classifier_lane = if is_classifier {
            s.config.server.classifier_account.as_deref()
        } else {
            None
        };
        // Resolve the API overflow account handle when the closure says it's usable.
        let api_lane_pick = |tried: &HashSet<String>| -> Option<&crate::config::AccountConfig> {
            if !api_lane_usable(tried) { return None; }
            let name = api_lane_name.as_deref()?;
            s.config.accounts.iter().find(|a| a.name == name)
        };
        let selected = if let Some(lane) = classifier_lane {
            // Prefer the dedicated classifier account; if it has already been
            // tried this request (transient upstream error), fall back once to
            // the configured `classifier_fallback_account` before giving up, so
            // a single blip on the primary lane doesn't force Claude Code to
            // block. Both are matched against `tried` so we never loop forever.
            s.config.accounts.iter()
                .find(|a| a.name == lane && !tried.contains(&a.name))
                .or_else(|| {
                    s.config.server.classifier_fallback_account.as_deref().and_then(|fb| {
                        s.config.accounts.iter().find(|a| a.name == fb && !tried.contains(&a.name))
                    })
                })
        } else {
            // Exclude failed (`tried`), admission-saturated, AND the reserved API
            // overflow account so it never enters normal rotation (it would score
            // ~1.0 and monopolize routing, blowing budget). It is reachable only
            // via the warm-start / overflow gates below.
            let mut excluded: HashSet<String> = tried.union(&saturated).cloned().collect();
            if let Some(name) = api_lane_name.as_deref() { excluded.insert(name.to_owned()); }
            let pick_sub = router::pick_account(
                &s.config.accounts, &s.state, &snap, fp_ref, &excluded,
                s.config.server.sticky_ttl_ms, s.config.server.expiry_soon_secs,
                effective_strategy, effective_burst_rpm,
                s.config.server.classifier_account.as_deref(),
            );
            if want_warm_start {
                // Warm-start: prefer the API lane for fast startup, fall back to subs.
                api_lane_pick(&tried).or(pick_sub)
            } else {
                // Steady state: subscriptions first; spill to the API lane only when
                // no subscription is available (overflow / never-timeout guarantee).
                pick_sub.or_else(|| api_lane_pick(&tried))
            }
        };
        let account = match selected {
            Some(a) => a,
            None => {
                // Classifier requests never block on cooldown — fail fast so the
                // client's own retry budget governs instead of hanging the call.
                if is_classifier {
                    return Err(ProxyError::AllAccountsUnavailable(None));
                }
                // Admission-saturation: healthy accounts exist but all their AIMD
                // slots are busy right now. Wait a short beat for a slot to free
                // (not a full cooldown), then reconsider them. This is the pacing
                // that keeps aggregate throughput high without a 429 storm.
                if !saturated.is_empty() {
                    if now_ms() + 250 <= wait_deadline_ms {
                        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
                        saturated.clear();
                        continue;
                    }
                    // No time left to wait for a slot — treat like all-cooling below.
                }
                // Model fallback: before waiting, try switching to a cheaper model.
                // Rate limits are often per-model, so the fallback may succeed immediately.
                // Override chain: runtime override → config → auto-detect from model name.
                if !fell_back && !model.is_empty() {
                    let fallback: Option<String> = match s.state.get_fallback_model_override() {
                        Some(Some(m)) => Some(m),              // explicit override
                        Some(None) => None,                     // explicitly disabled
                        None => s.config.server.fallback_model.clone()
                            .or_else(|| auto_fallback_model(&model).map(|s| s.to_owned())),
                    };
                    if let Some(ref fb) = fallback {
                        if model != *fb {
                            if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                                warn!(from = %model, to = %fb, "all accounts cooling — falling back to cheaper model");
                                val["model"] = serde_json::Value::String(fb.clone());
                                // The initial param strip keyed on the ORIGINAL model. Downgrading
                                // to a simpler model (e.g. Haiku) means its unsupported params must
                                // be dropped now too — otherwise the fallback request 400s with
                                // "This model does not support the effort parameter" (and similar).
                                if is_simple_model(fb.as_str()) {
                                    strip_unsupported_params_for_simple_model(&mut val);
                                    strip_beta_header_for_simple_model(&mut headers);
                                }
                                // shunt chose this fallback model; it cannot prove the target is
                                // entitled to a 1M window (Haiku has none; Sonnet 1M needs credits
                                // even on Max). Never advertise the 1M beta on a shunt-rewritten
                                // model, or the upstream returns the long-context 400.
                                normalize_model_suffix(&mut val);
                                strip_long_context_beta(&mut headers);
                                body_bytes = Bytes::from(serde_json::to_vec(&val).unwrap_or_else(|_| body_bytes.to_vec()));
                                fell_back = true;
                                tried.clear();
                                continue;
                            }
                        }
                    }
                }

                // Check whether any accounts are just temporarily cooling down
                // (429/529 backoff) rather than permanently disabled / auth_failed.
                // If so, wait for the soonest one to recover and retry.
                let account_states = s.state.account_states();
                let now = now_ms();
                let soonest_ms = s.config.accounts.iter()
                    .filter_map(|a| {
                        let st = account_states.get(&a.name)?;
                        if st.disabled { return None; } // auth_failed or permanently off
                        if st.cooldown_until_ms > now { Some(st.cooldown_until_ms) } else { None }
                    })
                    .min();

                match soonest_ms {
                    Some(wake_ms) if wake_ms <= wait_deadline_ms => {
                        let wait_ms = wake_ms.saturating_sub(now_ms()) + 50; // +50 ms buffer
                        warn!(wait_ms, "all accounts cooling — waiting for next available account");
                        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                        tried.clear(); // accounts may have recovered; try them again
                        saturated.clear();
                    }
                    // Everything is cooling past our deadline (or nothing is
                    // recoverable in time). Instead of a hard 503 — which Claude
                    // Code escalates to a fatal "exceeded max retries" and kills the
                    // subagent — return 429 + Retry-After so the client backs off and
                    // retries gracefully. Retry-After = soonest reset, clamped 1-30s.
                    _ => {
                        let retry_after = soonest_ms
                            .map(|w| (w.saturating_sub(now_ms()) / 1_000).clamp(1, 30))
                            .unwrap_or(5);
                        return Err(ProxyError::AllAccountsUnavailable(Some(retry_after)));
                    }
                }
                continue;
            }
        };

        let account_name = account.name.clone();

        // Admission control (AIMD). The dedicated classifier lane bypasses it —
        // it is a separate account with a short client deadline and must fail fast
        // rather than queue. For pooled accounts, reserve a slot; if none is free,
        // mark the account saturated and reconsider another lane.
        let _slot: Option<SlotGuard> = if classifier_lane.is_none() {
            if s.admission.try_acquire(&account_name) {
                Some(SlotGuard::new(s.admission.clone(), account_name.clone()))
            } else {
                saturated.insert(account_name.clone());
                continue;
            }
        } else {
            None
        };
        // A slot was admitted (or classifier lane) — this is a fresh forward, so
        // the saturation set from the just-finished search is stale; reset it.
        saturated.clear();

        s.state.record_request_burst(&account_name);

        // Use the live (possibly refreshed) token rather than the one baked into config.
        // OAuth accounts use their access token. API-key accounts return the
        // configured key directly.
        let token = {
            let creds = s.credentials.read().await;
            let cred = creds.get(&account_name)
                .cloned()
                .or_else(|| account.credential.clone());
            match cred {
                Some(c) => c.bearer_token().to_owned(),
                None => String::new(),
            }
        };

        // Detect request and account protocols.  When they differ, translate
        // the request body + path before forwarding and translate the response
        // back so the client always sees its native wire format.
        let req_is_anthropic = path.starts_with("/v1/messages");
        let acct_is_anthropic = account.provider.wire_protocol()
            == crate::provider::WireProtocol::Anthropic;
        // chatgpt.com (Provider::OpenAI) uses a proprietary backend-api path + sentinel token.
        // All other OpenAI-compat providers (OpenAIApi, Groq, Mistral, …) use /v1/chat/completions.
        let acct_is_chatgpt = matches!(account.provider, Provider::OpenAI);

        // log_model: what we actually send to the upstream (after resolve_model).
        // Defaults to the incoming model; overridden in the OpenAI-compat branch.
        let mut log_model = model.clone();

        let (fwd_path, mut fwd_body, mut fwd_headers) = if req_is_anthropic == acct_is_anthropic {
            // Same wire protocol — pass through unchanged.
            (path.clone(), body_bytes.clone(), headers.clone())
        } else if req_is_anthropic && acct_is_chatgpt {
            // Anthropic client → chatgpt.com account: translate to backend-api format.
            let val = serde_json::from_slice::<serde_json::Value>(&body_bytes).unwrap_or(json!({}));
            let translated = translate_anthropic_req_to_chatgpt(&val);
            let mut h = headers.clone();
            for name in &["anthropic-version", "anthropic-beta", "anthropic-dangerous-direct-browser-access"] {
                h.remove(*name);
            }
            (
                "/backend-api/conversation".to_owned(),
                bytes::Bytes::from(serde_json::to_vec(&translated).unwrap_or_default()),
                h,
            )
        } else if req_is_anthropic {
            // Anthropic client → standard OpenAI-compat account (OpenAIApi, Groq, Mistral, …).
            let val = serde_json::from_slice::<serde_json::Value>(&body_bytes).unwrap_or(json!({}));
            // Resolve the target model: account pin → global mapping → provider default.
            let target_model = resolve_model(&model, account, &s.config.model_mapping);
            log_model = target_model.clone();
            let translated = translate_anthropic_req_to_openai(val, &target_model);
            let mut h = headers.clone();
            for name in &["anthropic-version", "anthropic-beta", "anthropic-dangerous-direct-browser-access"] {
                h.remove(*name);
            }
            (
                "/v1/chat/completions".to_owned(),
                bytes::Bytes::from(serde_json::to_vec(&translated).unwrap_or_default()),
                h,
            )
        } else {
            // OpenAI client → Anthropic account: translate O→A.
            let val = serde_json::from_slice::<serde_json::Value>(&body_bytes).unwrap_or(json!({}));
            let translated = translate_to_anthropic(val);
            (
                "/v1/messages".to_owned(),
                bytes::Bytes::from(serde_json::to_vec(&translated).unwrap_or_default()),
                headers.clone(),
            )
        };

        let mut budget_guard = None;
        if api_lane_name.as_deref() == Some(account_name.as_str()) {
            if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&fwd_body) {
                let output_field = if value.get("max_output_tokens").is_some() { "max_output_tokens" } else { "max_tokens" };
                let requested = value.get(output_field).and_then(|v| v.as_u64()).unwrap_or(ov.max_output_tokens);
                value[output_field] = serde_json::Value::from(requested.min(ov.max_output_tokens));
                fwd_body = Bytes::from(serde_json::to_vec(&value).map_err(|_| ProxyError::BodyRead)?);
            }
            let Some(estimated) = crate::pricing::strict_api_cost_usd(
                &log_model, fwd_body.len() as u64, ov.max_output_tokens,
            ) else {
                warn!(model = %log_model, "rejecting Claude API overflow for unpriced model");
                tried.insert(account_name);
                continue;
            };
            let Some(reservation) = s.state.reserve_budget(&account_name, estimated, ov.daily_budget_usd) else {
                warn!(account = %account_name, estimated_usd = estimated, "Claude API overflow daily budget reservation rejected");
                tried.insert(account_name);
                continue;
            };
            budget_guard = Some(BudgetGuard::new(&s.state, reservation));
        }

        // Resolve upstream URL: per-account override (set at load time for non-primary
        // providers, or explicitly in tests) → config server URL.
        let upstream = account.upstream_url.as_deref()
            .unwrap_or(&s.config.server.upstream_url);

        // Inject chatgpt.com sentinel token — only for the chatgpt.com proprietary path.
        // Wrap in tokio::time::timeout (3s) to guarantee we don't block on Cloudflare challenges.
        if req_is_anthropic && acct_is_chatgpt {
            tracing::info!(account = %account_name, upstream = %upstream, "routing to chatgpt.com — fetching sentinel");
            let sentinel_client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap_or_default();
            let sentinel_opt = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                fetch_sentinel_token(&sentinel_client, upstream, &token),
            ).await.ok().flatten();
            if let Some(sentinel) = sentinel_opt {
                if let Ok(name) = axum::http::header::HeaderName::from_bytes(
                    b"openai-sentinel-chat-requirements-token",
                ) {
                    if let Ok(val) = axum::http::HeaderValue::from_str(&sentinel) {
                        fwd_headers.insert(name, val);
                    }
                }
            }
        }

        // Apply a hard 15s cap only for chatgpt.com: Cloudflare may hold the TCP connection
        // open indefinitely for certain TLS fingerprints.  Standard API providers don't need this.
        let response = if acct_is_chatgpt {
            tracing::info!(account = %account_name, path = %fwd_path, "forwarding to chatgpt.com (15s cap)");
            match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                s.forwarder.forward(upstream, &method, &fwd_path, fwd_body, &fwd_headers, account, &token),
            ).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    error!(account = %account_name, "chatgpt.com forward error: {:#}", e);
                    s.state.set_cooldown(&account_name, 5 * 60_000);
                    tried.insert(account_name);
                    continue;
                }
                Err(_) => {
                    warn!(account = %account_name, "chatgpt.com request timed out (Cloudflare) — cooling 5min");
                    s.state.set_cooldown(&account_name, 5 * 60_000);
                    tried.insert(account_name);
                    continue;
                }
            }
        } else {
            s.forwarder
                .forward(upstream, &method, &fwd_path, fwd_body, &fwd_headers, account, &token)
                .await
                .map_err(|e| {
                    error!("Forward error: {:#}", e);
                    ProxyError::Upstream
                })?
        };

        match response.status().as_u16() {
            200..=299 => {
                s.state.set_last_used(&account_name);
                // AIMD additive-increase: this account tolerated the request, so
                // grow its allowed concurrency a notch.
                s.admission.on_success(&account_name);
                if let Some(info) = account.provider.parse_rate_limits(response.headers()) {
                    // "Buy more tokens" avoidance: if this account has consumed its
                    // included subscription quota on a window AND overage/extra-usage
                    // is disabled, Anthropic can't overflow and Claude Code surfaces
                    // the buy-more prompt. Cool the account so subsequent turns skip
                    // it instead of repeatedly landing on it. The current response
                    // already succeeded, so it is still returned to the client.
                    if let Some(reset_secs) = overage_exhausted_reset(&info) {
                        let cool_ms = reset_secs
                            .saturating_mul(1_000)
                            .saturating_sub(now_ms())
                            .clamp(60_000, 10 * 60_000);
                        warn!(account = %account_name, cool_ms,
                            "included quota spent with overage disabled — cooling to avoid buy-more prompt");
                        s.state.update_rate_limits(&account_name, info);
                        s.state.set_cooldown(&account_name, cool_ms);
                    } else if near_cap_7d_warning(&info) {
                        // Proactive buy-more avoidance: the 7d window is within
                        // ~10% of the weekly cap. Cool briefly so routing prefers
                        // fresher accounts and Claude Code doesn't tip this one
                        // over its included quota. Still succeeds this turn.
                        let util = info.utilization_7d.unwrap_or_default();
                        warn!(account = %account_name, util, cool_ms = NEAR_CAP_COOL_MS,
                            "7d window near weekly cap — cooling briefly to avoid buy-more prompt");
                        s.state.update_rate_limits(&account_name, info);
                        s.state.set_cooldown(&account_name, NEAR_CAP_COOL_MS);
                    } else {
                        s.state.update_rate_limits(&account_name, info);
                    }
                }
                // Translate response back to the client's expected protocol.
                let response = if req_is_anthropic == acct_is_anthropic {
                    response
                } else if req_is_anthropic && acct_is_chatgpt {
                    // Got chatgpt.com response; client expects Anthropic.
                    translate_response_chatgpt_to_anthropic(response, &model).await
                } else if req_is_anthropic {
                    // Got standard OpenAI-compat response; client expects Anthropic.
                    translate_response_openai_to_anthropic(response, &model).await
                } else {
                    // Got Anthropic response; client expects OpenAI.
                    translate_response_anthropic_to_openai(response).await
                };
                let reservation = budget_guard.and_then(BudgetGuard::handoff);
                return Ok(tap_usage(response, &s.state, s.telemetry.as_ref(), s.supabase.as_ref(), &account_name, &account.provider.to_string(), &log_model, req_start_ms, &request_id, &path, tried.len(), is_classifier, trace_id.as_deref().unwrap_or(""), reservation).await);
            }
            429 => {
                let info = account.provider.parse_rate_limits(response.headers());
                let is_exhausted = is_exhausted_response(info.as_ref());
                // AIMD multiplicative-decrease on burst 429 only: it means we
                // exceeded this account's short-term ceiling, so back its allowed
                // concurrency off. Exhaustion is a window cap, not a pacing signal.
                if !is_exhausted {
                    s.admission.on_burst_429(&account_name);
                }
                // Only stagger EXHAUSTED accounts: after a real quota-drain 429,
                // re-selecting the same account immediately would just hit the
                // wall again, so add 30s so other accounts get the window first.
                // Burst (non-exhausted) 429s clear in seconds and the account
                // still has quota, so honor Anthropic's retry-after as-is and let
                // compute_429_cooldown_ms keep the bench short — over-parking a
                // healthy lane is what drains the pool under many agents.
                let retry_after_ms = parse_retry_after_ms(response.headers())
                    .map(|ms| if is_exhausted { ms.saturating_add(30_000) } else { ms });
                let cooldown_ms = compute_429_cooldown_ms(info.as_ref(), retry_after_ms, is_exhausted);
                warn!(account = %account_name, cooldown_ms, exhausted = is_exhausted, "429 rate-limited — cooling");
                if let Some(info) = info {
                    s.state.update_rate_limits(&account_name, info);
                }
                if let Some(ref sb) = s.supabase {
                    let available = s.config.accounts.iter()
                        .filter(|a| s.state.is_available(&a.name))
                        .count();
                    sb.emit_rate_limit_hit(&account.provider.to_string(), cooldown_ms, available);
                }
                s.state.set_cooldown_staggered(&account_name, cooldown_ms);
                if cooldown_ms >= 5 * 60_000 && !s.state.get_alerts_muted() {
                    let mins = cooldown_ms / 60_000;
                    notify(
                        "shunt: Rate Limited",
                        &format!("Account '{account_name}' hit quota limit — cooling {mins}m."),
                        "Ping",
                    );
                }
                tried.insert(account_name);
            }
            529 => {
                warn!(account = %account_name, "529 overloaded — cooling 30s");
                if let Some(info) = account.provider.parse_rate_limits(response.headers()) {
                    s.state.update_rate_limits(&account_name, info);
                }
                s.state.set_cooldown(&account_name, 30_000);
                tried.insert(account_name);
            }
            401 => {
                if !refreshed.contains(&account_name) {
                    // Access token invalidated (e.g. user logged out) — try refresh.
                    //
                    // Acquire the per-account refresh lock so concurrent requests
                    // for the same account serialise here. The first waiter to get
                    // the lock does the actual OAuth refresh; subsequent waiters
                    // re-check credentials and skip the refresh if the token was
                    // already rotated while they were queued.
                    let account_lock = {
                        let mut locks = s.refresh_locks.lock();
                        locks.entry(account_name.clone())
                            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                            .clone()
                    };
                    let _guard = account_lock.lock().await;

                    // Re-read credentials after acquiring the lock — another task
                    // may have already refreshed while we were waiting.
                    let cred_before = {
                        let creds = s.credentials.read().await;
                        creds.get(&account_name).cloned()
                            .or_else(|| account.credential.clone())
                    };
                    let Some(cred) = cred_before else {
                        tried.insert(account_name);
                        continue;
                    };

                    // Check if the token already changed while we were waiting.
                    let token_before = cred.access_token().to_owned();
                    let already_refreshed = {
                        let creds = s.credentials.read().await;
                        creds.get(&account_name)
                            .map(|c| c.access_token() != token_before)
                            .unwrap_or(false)
                    };

                    if already_refreshed {
                        // Another concurrent request already refreshed — just retry.
                        warn!(account = %account_name, "401 — token was refreshed by concurrent request, retrying");
                        refreshed.insert(account_name);
                    } else if let Some(oauth_cred) = cred.as_oauth() {
                        // OAuth account — attempt token refresh.
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            account.provider.refresh_token(oauth_cred),
                        ).await {
                            Ok(Ok(fresh)) => {
                                warn!(account = %account_name, "401 — token refreshed, retrying");
                                {
                                    let mut creds = s.credentials.write().await;
                                    creds.insert(account_name.clone(), Credential::Oauth(fresh.clone()));
                                }
                                // Persist to disk so the refreshed token survives a restart.
                                let name = account_name.clone();
                                let fresh = fresh.clone();
                                tokio::task::spawn_blocking(move || {
                                    let mut store = CredentialsStore::load();
                                    store.insert_resolved(name, Credential::Oauth(fresh.clone()));
                                    store.save().ok();
                                });
                                // Mark as refreshed but don't add to tried — retry this account.
                                refreshed.insert(account_name);
                            }
                            _ => {
                                // Refresh failed/timed out — cool down, don't permanently disable.
                                error!(account = %account_name, "401 — token refresh failed, cooling 5min");
                                s.state.set_cooldown(&account_name, 5 * 60_000);
                                if let Some(ref sb) = s.supabase {
                                    sb.emit_auth_failure(&account.provider.to_string());
                                }
                                tried.insert(account_name);
                            }
                        }
                    } else {
                        // API-key account — 401 means the key is invalid; no refresh possible.
                        error!(account = %account_name, "401 — API key rejected, cooling 5min");
                        s.state.set_cooldown(&account_name, 5 * 60_000);
                        if let Some(ref sb) = s.supabase {
                            sb.emit_auth_failure(&account.provider.to_string());
                        }
                        tried.insert(account_name);
                    }
                } else {
                    // Already refreshed once and still 401 — cool down this account.
                    error!(account = %account_name, "401 after refresh — cooling 5min");
                    s.state.set_cooldown(&account_name, 5 * 60_000);
                    if let Some(ref sb) = s.supabase {
                        sb.emit_auth_failure(&account.provider.to_string());
                    }
                    tried.insert(account_name);
                }
            }
            403 => {
                // Forbidden — could be a Cloudflare challenge (non-Anthropic providers)
                // or a genuine subscription/org block (Anthropic). Use a short cooldown
                // for non-Anthropic accounts so a CF block doesn't lock them out for 30m.
                if acct_is_anthropic {
                    error!(account = %account_name, "403 forbidden — cooling 30min");
                    s.state.set_cooldown(&account_name, 30 * 60_000);
                    if !s.state.get_alerts_muted() {
                        notify(
                            "shunt: Account Forbidden",
                            &format!("Account '{account_name}' got 403 — subscription may have lapsed (cooling 30m)."),
                            "Basso",
                        );
                    }
                } else {
                    warn!(account = %account_name, "403 from chatgpt.com (Cloudflare) — cooling 5min");
                    s.state.set_cooldown(&account_name, 5 * 60_000);
                }
                tried.insert(account_name);
            }
            400 if !stripped_1m
                && req_is_anthropic
                && headers.get("anthropic-beta")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.contains(LONG_CONTEXT_BETA))
                    .unwrap_or(false) =>
            {
                // Self-heal a long-context 400: the request still advertises the 1M
                // beta but this account isn't entitled (e.g. a Pro account in the pool,
                // or Sonnet 1M without credits). Confirm the error is the long-context
                // rejection, then strip the 1M beta + [1m] suffix and retry once, rather
                // than hard-failing the user's request. Buffer the body to inspect it.
                let (parts, body) = response.into_parts();
                let raw = axum::body::to_bytes(body, MAX_REQUEST_BODY).await.unwrap_or_default();
                let text = String::from_utf8_lossy(&raw);
                let is_long_ctx = text.contains("long context beta")
                    || text.contains("not yet available for this subscription");
                if is_long_ctx {
                    warn!(account = %account_name, "400 long-context beta rejected — stripping 1M beta and retrying");
                    strip_long_context_beta(&mut headers);
                    if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                        if normalize_model_suffix(&mut val) {
                            body_bytes = Bytes::from(serde_json::to_vec(&val).unwrap_or_else(|_| body_bytes.to_vec()));
                        }
                    }
                    stripped_1m = true;
                    // Retry the same account first (it may now succeed at 200K); do not
                    // mark it tried, and do not clear tried for others.
                    continue;
                }
                // Some other 400 — return the buffered response unchanged.
                return Ok(Response::from_parts(parts, axum::body::Body::from(raw)));
            }
            _ => {
                // 400, 404, 500, etc. — return as-is, no retry
                return Ok(response);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Usage extraction
// ---------------------------------------------------------------------------

/// Intercept a successful response to record token usage, then pass it through.
///
/// - Streaming: wraps the body stream with an SSE scanner (zero latency).
/// - Non-streaming: buffers the body, parses usage, rebuilds the response.
async fn tap_usage(
    resp: Response,
    state: &StateStore,
    telemetry: Option<&TelemetryClient>,
    supabase: Option<&Arc<SupabaseTelemetry>>,
    account: &str,
    provider: &str,
    model: &str,
    req_start_ms: u64,
    request_id: &str,
    path: &str,
    retries: usize,
    is_classifier: bool,
    trace: &str,
    budget_reservation: Option<String>,
) -> Response {
    use axum::body::Body;
    use crate::state::RequestLog;

    let streaming = quota::is_streaming_response(&resp);

    if streaming {
        let state      = state.clone();
        let telem      = telemetry.cloned();
        let sb         = supabase.cloned();
        let account    = account.to_owned();
        let provider   = provider.to_owned();
        let is_codex_provider = provider == "openai";
        let rate_state = state.clone();
        let rate_account = account.clone();
        let model      = model.to_owned();
        let request_id = request_id.to_owned();
        let path       = path.to_owned();
        let trace      = trace.to_owned();
        let reservation = budget_reservation.clone();
        let on_complete = Arc::new(move |input: u64, output: u64| {
            let duration_ms = now_ms().saturating_sub(req_start_ms);
            info!(
                request_id = %request_id,
                account    = %account,
                model      = %model,
                status     = 200,
                latency_ms = duration_ms,
                path       = %path,
                stream     = true,
                input_tokens  = input,
                output_tokens = output,
                retries    = retries,
                trace      = %trace,
                "request complete"
            );
            let log = RequestLog {
                ts_ms: req_start_ms,
                account: account.clone(),
                model: model.clone(),
                status: 200,
                input_tokens: input,
                output_tokens: output,
                duration_ms,
                classifier: is_classifier,
                trace: trace.clone(),
            };
            state.record_usage(&account, input, output);
            state.record_global(&model, input, output);
            if let Some(ref id) = reservation {
                state.reconcile_budget_reservation(id, &model, input, output);
            } else {
                state.record_account_cost(&account, &model, input, output);
            }
            if let Some(ref t) = telem { t.push_event(&log); }
            if let Some(ref sb) = sb {
                sb.emit_request_complete(&model, &provider, duration_ms, input, output);
            }
            state.record_request(log);
        });
        let (parts, body) = resp.into_parts();
        let wrapped = if is_codex_provider {
            let on_rate = Arc::new(move |update: quota::CodexRateUpdate| {
                let primary = update.primary_used_percent.map(|v| (v / 100.0).clamp(0.0, 1.0));
                let secondary = update.secondary_used_percent.map(|v| (v / 100.0).clamp(0.0, 1.0));
                rate_state.update_rate_limits(&rate_account, RateLimitInfo {
                    utilization_5h: primary,
                    reset_5h: update.primary_reset_at,
                    status_5h: primary.map(|v| if v >= 1.0 { "exhausted".into() } else { "allowed".into() }),
                    utilization_7d: secondary,
                    reset_7d: update.secondary_reset_at,
                    status_7d: secondary.map(|v| if v >= 1.0 { "exhausted".into() } else { "allowed".into() }),
                    overage_status: None,
                    overage_disabled_reason: None,
                    representative_claim: None,
                    updated_ms: now_ms(),
                });
            });
            quota::wrap_streaming_body_with_codex_rates(body, on_complete, on_rate)
        } else {
            quota::wrap_streaming_body(body, on_complete)
        };
        return Response::from_parts(parts, wrapped);
    }

    // Non-streaming: buffer, extract, rebuild
    let (parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };
    let (input, output) = quota::extract_usage_from_json(&bytes);
    let duration_ms = now_ms().saturating_sub(req_start_ms);
    info!(
        request_id    = %request_id,
        account       = %account,
        model         = %model,
        status        = 200,
        latency_ms    = duration_ms,
        path          = %path,
        stream        = false,
        input_tokens  = input,
        output_tokens = output,
        retries       = retries,
        trace         = %trace,
        "request complete"
    );
    let log = RequestLog {
        ts_ms: req_start_ms,
        account: account.to_owned(),
        model: model.to_owned(),
        status: 200,
        input_tokens: input,
        output_tokens: output,
        duration_ms,
        classifier: is_classifier,
        trace: trace.to_owned(),
    };
    state.record_usage(account, input, output);
    state.record_global(model, input, output);
    if let Some(ref id) = budget_reservation {
        state.reconcile_budget_reservation(id, model, input, output);
    } else {
        state.record_account_cost(account, model, input, output);
    }
    if let Some(t) = telemetry { t.push_event(&log); }
    if let Some(sb) = supabase {
        sb.emit_request_complete(model, provider, duration_ms, input, output);
    }
    state.record_request(log);
    Response::from_parts(parts, Body::from(bytes))
}


// ---------------------------------------------------------------------------
// Rate limit prefetch
// ---------------------------------------------------------------------------

/// For any account with no rate-limit data yet, make a cheap request directly
/// to the upstream API so we populate metrics without waiting for a real user
/// request. Runs as a background task after startup.
pub async fn prefetch_rate_limits(config: Arc<Config>, state: StateStore, live_creds: LiveCredentials) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_default();

    let existing_rl = state.rate_limit_snapshot();
    for account in &config.accounts {
        // Skip if we already have data for this account.
        if let Some(r) = existing_rl.get(&account.name) {
            if r.utilization_5h.is_some() || r.utilization_7d.is_some() {
                continue;
            }
        }

        // Skip accounts with no credentials or no prefetch support.
        let cred = match account.credential.clone() {
            Some(c) => c,
            None => continue,
        };

        let Some((path, body)) = account.provider.prefetch_request() else {
            // No POST prefetch for this provider — do a lightweight GET auth check instead.
            if let Some(probe_path) = account.provider.auth_probe_get_path() {
                auth_probe_get(&client, probe_path, account, &state).await;
            }
            continue;
        };
        let upstream = account.upstream_url.as_deref()
            .unwrap_or(&config.server.upstream_url)
            .trim_end_matches('/');
        let url = format!("{upstream}{path}");

        let resp = prefetch_send(&client, &url, &account.provider, cred.bearer_token(), &body).await;

        let r = match resp {
            Ok(r) => r,
            Err(e) => { tracing::warn!(account = %account.name, "prefetch failed: {e}"); continue; }
        };

        if r.status() == reqwest::StatusCode::UNAUTHORIZED {
            tracing::info!(account = %account.name, "prefetch: token expired, refreshing");
            let Some(oauth_cred) = cred.as_oauth() else {
                // API-key account — 401 during prefetch means the key is invalid.
                tracing::error!(account = %account.name, "prefetch 401 — API key rejected");
                state.set_auth_failed(&account.name);
                continue;
            };
            let fresh = match account.provider.refresh_token(oauth_cred).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(account = %account.name, "token refresh failed: {e}");
                    state.set_auth_failed(&account.name);
                    continue;
                }
            };
            let mut store = crate::config::CredentialsStore::load();
            store.insert_resolved(account.name.clone(), Credential::Oauth(fresh.clone()));
            store.save().ok();
            // Update live credentials so the proxy uses the fresh token immediately.
            live_creds.write().await.insert(account.name.clone(), Credential::Oauth(fresh.clone()));

            match prefetch_send(&client, &url, &account.provider, &fresh.access_token, &body).await {
                Ok(r2) if r2.status() == reqwest::StatusCode::UNAUTHORIZED => {
                    tracing::error!(account = %account.name, "401 after refresh — needs re-authorization");
                    state.set_auth_failed(&account.name);
                }
                Ok(r2) => {
                    if let Some(info) = account.provider.parse_rate_limits(r2.headers()) {
                        state.update_rate_limits(&account.name, info);
                    }
                }
                Err(e) => tracing::warn!(account = %account.name, "prefetch retry failed: {e}"),
            }
        } else {
            tracing::info!(account = %account.name, status = %r.status(), "prefetch response");
            if let Some(info) = account.provider.parse_rate_limits(r.headers()) {
                state.update_rate_limits(&account.name, info);
            }
        }
    }
}

/// Build and send a prefetch request for the given provider + token.
async fn prefetch_send(
    client: &reqwest::Client,
    url: &str,
    provider: &crate::provider::Provider,
    token: &str,
    body: &serde_json::Value,
) -> anyhow::Result<reqwest::Response> {
    let mut headers = reqwest::header::HeaderMap::new();
    provider.inject_auth_headers(&mut headers, token)?;
    for (name, value) in provider.prefetch_extra_headers() {
        headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
            reqwest::header::HeaderValue::from_static(value),
        );
    }
    Ok(client.post(url).headers(headers).json(body).send().await?)
}

/// GET a cheap endpoint to verify credentials are still valid for providers that
/// don't expose rate-limit headers (e.g. OpenAI). On 401, attempts a token refresh;
/// marks the account as `reauth_required` if the refresh also fails.
async fn auth_probe_get(
    client: &reqwest::Client,
    path: &str,
    account: &crate::config::AccountConfig,
    state: &StateStore,
) {
    let cred = match account.credential.clone() {
        Some(c) => c,
        None => return,
    };
    let upstream = account.upstream_url.as_deref()
        .unwrap_or_else(|| account.provider.default_upstream_url());
    let url = format!("{}{}", upstream, path);

    let do_get = |token: &str| -> reqwest::RequestBuilder {
        let mut headers = reqwest::header::HeaderMap::new();
        let _ = account.provider.inject_auth_headers(&mut headers, token);
        client.get(&url).headers(headers)
    };

    let resp = match do_get(cred.bearer_token()).send().await {
        Ok(r) => r,
        Err(e) => { tracing::warn!(account = %account.name, "auth probe failed: {e}"); return; }
    };

    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        tracing::info!(account = %account.name, "auth probe: token rejected, refreshing");
        let Some(oauth_cred) = cred.as_oauth() else {
            // API-key account — key is invalid; no refresh possible.
            tracing::error!(account = %account.name, "auth probe 401 — API key rejected");
            state.set_auth_failed(&account.name);
            return;
        };
        let fresh = match account.provider.refresh_token(oauth_cred).await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(account = %account.name, "token refresh failed: {e}");
                state.set_auth_failed(&account.name);
                return;
            }
        };
        let mut store = crate::config::CredentialsStore::load();
        store.insert_resolved(account.name.clone(), Credential::Oauth(fresh.clone()));
        store.save().ok();

        let fresh_token = &fresh.access_token;
        match do_get(fresh_token).send().await {
            Ok(r2) if r2.status() == reqwest::StatusCode::UNAUTHORIZED => {
                tracing::error!(account = %account.name, "401 after refresh — needs re-authorization");
                state.set_auth_failed(&account.name);
            }
            Ok(_) => tracing::info!(account = %account.name, "auth probe ok after refresh"),
            Err(e) => tracing::warn!(account = %account.name, "auth probe retry failed: {e}"),
        }
    } else {
        tracing::info!(account = %account.name, status = %resp.status(), "auth probe ok");
        // Access token is valid. Do NOT refresh here — rotating the refresh_token races
        // with codex CLI, which also tries to refresh at startup using the same token.
        // Proactive refreshing is handled solely by openai_token_refresh_loop.
    }
}

// ---------------------------------------------------------------------------
// Proactive OpenAI token refresh loop
// ---------------------------------------------------------------------------

/// Returns true if the access_token inside `cred` has fewer than `threshold_mins`
/// minutes remaining. Falls back to the stored `expires_at` if the JWT cannot be decoded.
fn access_token_expires_soon(cred: &crate::oauth::OAuthCredential, threshold_mins: u64) -> bool {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let exp_ms = crate::oauth::jwt_exp_ms(&cred.access_token)
        .unwrap_or(cred.expires_at);
    exp_ms < now_ms + threshold_mins * 60 * 1_000
}

/// Sync live_creds from auth.json if auth.json has a newer token.
///
/// Codex CLI refreshes its own token and writes auth.json. Before we refresh,
/// we pull that in so we don't use a stale refresh_token that codex already rotated.
async fn sync_live_creds_from_auth_json(
    account_name: &str,
    live_creds: &LiveCredentials,
) {
    let Some(from_file) = crate::oauth::read_codex_credentials() else { return };
    let current_exp = live_creds.read().await
        .get(account_name)
        .and_then(|c| c.as_oauth())
        .map(|c| c.expires_at)
        .unwrap_or(0);
    if from_file.expires_at > current_exp {
        tracing::info!(account = %account_name, "synced fresher token from auth.json");
        live_creds.write().await.insert(account_name.to_owned(), Credential::Oauth(from_file));
    }
}

/// Perform a single proactive refresh for one account and persist the result.
async fn do_proactive_refresh(
    account: &crate::config::AccountConfig,
    creds: &crate::oauth::OAuthCredential,
    live_creds: &LiveCredentials,
    state: &StateStore,
) {
    tracing::info!(account = %account.name, "proactive OpenAI token refresh");
    match account.provider.refresh_token(creds).await {
        Ok(fresh) => {
            tracing::info!(account = %account.name, "proactive refresh ok — Shunt credential updated");
            {
                let mut map = live_creds.write().await;
                map.insert(account.name.clone(), Credential::Oauth(fresh.clone()));
            }
            let mut store = crate::config::CredentialsStore::load();
            store.insert_resolved(account.name.clone(), Credential::Oauth(fresh.clone()));
            store.save().ok();
            state.clear_auth_failed(&account.name);
        }
        Err(e) => {
            tracing::warn!(account = %account.name, "proactive refresh failed: {e}");
            state.set_auth_failed(&account.name);
        }
    }
}


/// Refreshes Codex credentials without collapsing multiple native-pool accounts.
/// Legacy configs retain the historical auth.json sync; v2+ treats auth.json as
/// an import-only source and thereafter updates Shunt's own credential store.
pub async fn openai_token_refresh_loop(
    config: Arc<Config>,
    state: StateStore,
    live_creds: LiveCredentials,
) {
    // Startup: sync from auth.json first (Codex may have refreshed since shunt last ran).
    for account in config.accounts.iter()
        .filter(|a| a.provider == crate::provider::Provider::OpenAI)
    {
        if state.account_states().get(&account.name).map(|s| s.auth_failed).unwrap_or(false) {
            continue;
        }
        if config.schema_version < crate::config::NATIVE_POOLS_SCHEMA_VERSION {
            sync_live_creds_from_auth_json(&account.name, &live_creds).await;
        }

        let creds = {
            let map = live_creds.read().await;
            map.get(&account.name).cloned().or_else(|| account.credential.clone())
        };
        if let Some(creds) = creds {
            if let Some(oauth) = creds.as_oauth() {
                if access_token_expires_soon(oauth, 30) {
                    // access_token is nearly expired — refresh now so shunt can serve requests immediately.
                    do_proactive_refresh(account, oauth, &live_creds, &state).await;
                } else {
                    tracing::info!(account = %account.name, "access_token fresh at startup");
                }
            }
        }
    }

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5 * 60)).await;
        for account in config.accounts.iter()
            .filter(|a| a.provider == crate::provider::Provider::OpenAI)
        {
            if config.schema_version < crate::config::NATIVE_POOLS_SCHEMA_VERSION {
                sync_live_creds_from_auth_json(&account.name, &live_creds).await;
                continue;
            }
            let creds = live_creds.read().await.get(&account.name).cloned();
            if let Some(oauth) = creds.as_ref().and_then(Credential::as_oauth) {
                if access_token_expires_soon(oauth, 10) {
                    do_proactive_refresh(account, oauth, &live_creds, &state).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

enum ProxyError {
    BodyRead,
    Upstream,
    /// Pool has nothing servable in time. Carries an optional Retry-After
    /// (seconds) so the response is a graceful 429 the client backs off on,
    /// rather than a hard 503 that Claude Code escalates to "exceeded max
    /// retries" and kills the subagent. `None` = classifier fast-fail.
    AllAccountsUnavailable(Option<u64>),
    Unauthorized,
    RateLimited,
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        match self {
            ProxyError::RateLimited => {
                let mut resp = (
                    StatusCode::TOO_MANY_REQUESTS,
                    axum::Json(json!({
                        "type": "error",
                        "error": {"type": "rate_limit_error", "message": "too many requests — slow down"}
                    })),
                ).into_response();
                resp.headers_mut().insert(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("60"),
                );
                resp
            }
            ProxyError::AllAccountsUnavailable(retry_after) => {
                // 429 (not 503) with a rate_limit_error body + Retry-After, so
                // Claude Code's native backoff waits and retries instead of
                // failing the run. Without a retry hint (classifier fast-fail),
                // omit Retry-After so the caller decides immediately.
                let mut resp = (
                    StatusCode::TOO_MANY_REQUESTS,
                    axum::Json(json!({
                        "type": "error",
                        "error": {
                            "type": "rate_limit_error",
                            "message": "pool saturated — all accounts cooling; retry shortly"
                        }
                    })),
                ).into_response();
                if let Some(secs) = retry_after {
                    if let Ok(v) = axum::http::HeaderValue::from_str(&secs.to_string()) {
                        resp.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
                    }
                }
                resp
            }
            other => {
                let (status, msg) = match other {
                    ProxyError::BodyRead => (StatusCode::BAD_REQUEST, "failed to read request body"),
                    ProxyError::Upstream => (StatusCode::BAD_GATEWAY, "upstream request failed"),
                    ProxyError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid or missing api key"),
                    ProxyError::RateLimited | ProxyError::AllAccountsUnavailable(_) => unreachable!(),
                };
                (status, axum::Json(json!({
                    "type": "error",
                    "error": {"type": "api_error", "message": msg}
                }))).into_response()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Recovery watcher — periodically retries token refresh for auth_failed accounts
// ---------------------------------------------------------------------------

/// Runs as a background task. Every 2 minutes, tries to refresh tokens for any
/// auth_failed account. If refresh succeeds the account is brought back online
/// without a process restart. If all accounts remain unrecoverable, fires a
/// macOS notification (at most once per hour).
pub async fn recovery_watcher(
    config: Arc<Config>,
    state: StateStore,
    credentials: LiveCredentials,
) {
    use std::time::{Duration, Instant};
    const CHECK_INTERVAL: Duration = Duration::from_secs(120);
    const NOTIFY_COOLDOWN: Duration = Duration::from_secs(3600);

    let account_names: Vec<String> = config.accounts.iter().map(|a| a.name.clone()).collect();
    let mut last_notified: Option<Instant> = None;

    loop {
        tokio::time::sleep(CHECK_INTERVAL).await;

        let name_refs: Vec<&str> = account_names.iter().map(String::as_str).collect();
        let failed = state.auth_failed_accounts(&name_refs);
        if failed.is_empty() {
            last_notified = None;
            continue;
        }

        tracing::warn!(
            accounts = ?failed,
            "recovery: {} account(s) auth_failed, attempting token refresh",
            failed.len()
        );

        let mut any_recovered = false;

        for name in &failed {
            let cred = {
                let map = credentials.read().await;
                map.get(*name).cloned()
            };
            let Some(cred) = cred else { continue };
            if !cred.has_refresh_token() { continue; }
            let Some(oauth_cred) = cred.as_oauth().cloned() else { continue };

            let provider = config.accounts.iter()
                .find(|a| a.name == *name)
                .map(|a| a.provider.clone())
                .unwrap_or_default();

            let result = tokio::time::timeout(
                Duration::from_secs(20),
                provider.refresh_token(&oauth_cred),
            ).await;

            match result {
                Ok(Ok(fresh)) => {
                    tracing::info!(account = %name, "recovery: token refreshed — account back online");
                    {
                        let mut map = credentials.write().await;
                        map.insert(name.to_string(), Credential::Oauth(fresh.clone()));
                    }
                    let name_owned = name.to_string();
                    let fresh_owned = fresh.clone();
                    tokio::task::spawn_blocking(move || {
                        let mut store = crate::config::CredentialsStore::load();
                        store.insert_resolved(name_owned, Credential::Oauth(fresh_owned.clone()));
                        store.save().ok();
                    });
                    state.clear_auth_failed(name);
                    any_recovered = true;
                }
                Ok(Err(e)) => {
                    tracing::error!(account = %name, error = %e, "recovery: token refresh failed");
                    if !state.get_alerts_muted() {
                        notify(
                            "shunt: Reauth Required",
                            &format!("Account '{name}' needs re-authorization. Run `shunt add-account`."),
                            "Basso",
                        );
                    }
                }
                Err(_) => {
                    tracing::error!(account = %name, "recovery: token refresh timed out");
                    if !state.get_alerts_muted() {
                        notify(
                            "shunt: Reauth Required",
                            &format!("Account '{name}' token refresh timed out. Run `shunt add-account`."),
                            "Basso",
                        );
                    }
                }
            }
        }

        if any_recovered {
            tracing::info!("recovery: at least one account is back online");
            continue;
        }

        // All accounts still auth_failed after refresh attempts — notify.
        let still_failed = state.auth_failed_accounts(&name_refs);
        if still_failed.len() == account_names.len() {
            let should_notify = last_notified
                .map(|t| t.elapsed() >= NOTIFY_COOLDOWN)
                .unwrap_or(true);
            if should_notify {
                error!(
                    "ALL accounts are offline (auth failed). \
                     Run `shunt add-account` to re-authorize."
                );
                if !state.get_alerts_muted() {
                    notify(
                        "shunt: All Accounts Offline",
                        "All accounts need re-authorization. Run `shunt add-account`.",
                        "Basso",
                    );
                }
                last_notified = Some(Instant::now());
            }
        }
    }
}

/// Sends a single lightweight prefetch request for `account` immediately after its
/// cooldown expires, so the router has fresh rate-limit headers before the next
/// real request arrives.
async fn post_cooldown_prefetch(
    client: &reqwest::Client,
    account: &crate::config::AccountConfig,
    token: &str,
    state: &StateStore,
    upstream_url: &str,
) {
    let Some((path, body)) = account.provider.prefetch_request() else {
        if let Some(probe_path) = account.provider.auth_probe_get_path() {
            auth_probe_get(client, probe_path, account, state).await;
        }
        return;
    };
    let upstream = account.upstream_url.as_deref()
        .unwrap_or(upstream_url)
        .trim_end_matches('/');
    let url = format!("{upstream}{path}");
    match prefetch_send(client, &url, &account.provider, token, &body).await {
        Ok(r) => {
            // If the prefetch itself gets 429'd, the upstream rate limit hasn't
            // actually reset yet — re-cool the account to prevent waiting
            // requests from immediately hitting the same 429.
            let info = account.provider.parse_rate_limits(r.headers());
            if r.status().as_u16() == 429 {
                let retry_after_ms = parse_retry_after_ms(r.headers());
                let is_exhausted = is_exhausted_response(info.as_ref());
                let cooldown_ms = compute_429_cooldown_ms(info.as_ref(), retry_after_ms, is_exhausted);
                tracing::warn!(account = %account.name, cooldown_ms, exhausted = is_exhausted, "post-cooldown prefetch got 429 — re-cooling");
                if let Some(info) = info {
                    state.update_rate_limits(&account.name, info);
                }
                state.set_cooldown_staggered(&account.name, cooldown_ms);
                return;
            }
            if let Some(info) = info {
                state.update_rate_limits(&account.name, info);
                tracing::info!(account = %account.name, "post-cooldown prefetch: quota refreshed");
            }
        }
        Err(e) => warn!(account = %account.name, "post-cooldown prefetch failed: {e}"),
    }
}

/// Periodic health-check loop: probes every account at a configurable interval
/// to detect dead/invalid accounts before real traffic hits them.
///
/// Uses exponential backoff (base_interval * 2^min(failures, 3)) per account,
/// capped at ~40 min. Marks accounts as `health_check_failed` after 2 consecutive
/// failures (tolerates one transient blip). On 401, delegates to `set_auth_failed`.
pub async fn health_check_loop(
    config: Arc<Config>,
    state: StateStore,
    live_creds: LiveCredentials,
) {
    if !config.server.health_check_enabled {
        return;
    }

    // Let prefetch_rate_limits finish first.
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    let base_interval_ms = config.server.health_check_interval_secs * 1000;
    let timeout = std::time::Duration::from_secs(config.server.health_check_timeout_secs);
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .unwrap_or_default();

    const FAILURE_THRESHOLD: u32 = 2;
    const MAX_BACKOFF_EXP: u32 = 3; // 2^3 = 8x → 40 min at 5-min base

    loop {
        for account in &config.accounts {
            // Skip accounts already handled by recovery_watcher.
            {
                let states = state.account_states();
                if let Some(acc_state) = states.get(&account.name) {
                    if acc_state.disabled || acc_state.auth_failed {
                        continue;
                    }
                }
            }

            // Exponential backoff based on consecutive failure count.
            let (last_check_ms, failures) = state.health_check_info(&account.name);
            let backoff_factor = 1u64 << failures.min(MAX_BACKOFF_EXP);
            let effective_interval_ms = base_interval_ms.saturating_mul(backoff_factor);
            let now = crate::state::now_ms_pub();
            if last_check_ms > 0 && now.saturating_sub(last_check_ms) < effective_interval_ms {
                continue;
            }

            state.update_last_health_check(&account.name);

            // Resolve current credential from live_creds (may have been refreshed).
            let cred = {
                let creds = live_creds.read().await;
                creds.get(&account.name).cloned()
            }.or_else(|| account.credential.clone());

            let cred = match cred {
                Some(c) => c,
                None => {
                    // Local providers have no cred — probe reachability via GET /v1/models.
                    if let Some(probe_path) = account.provider.auth_probe_get_path() {
                        let upstream = account.upstream_url.as_deref()
                            .unwrap_or_else(|| account.provider.default_upstream_url());
                        let url = format!("{upstream}{probe_path}");
                        match client.get(&url).send().await {
                            Ok(r) if r.status().is_success() => {
                                if state.is_health_check_failed(&account.name) {
                                    tracing::info!(account = %account.name, "health check recovered");
                                }
                                state.clear_health_check_failed(&account.name);
                            }
                            Ok(r) => {
                                let count = state.record_health_check_failure(&account.name, FAILURE_THRESHOLD);
                                tracing::warn!(account = %account.name, status = %r.status(),
                                    failures = count, "health check failed");
                            }
                            Err(e) => {
                                let count = state.record_health_check_failure(&account.name, FAILURE_THRESHOLD);
                                tracing::warn!(account = %account.name, failures = count,
                                    "health check unreachable: {e}");
                            }
                        }
                    }
                    continue;
                }
            };

            let token = cred.bearer_token();
            let upstream = account.upstream_url.as_deref()
                .unwrap_or(&config.server.upstream_url);

            // Try POST prefetch (Anthropic) or GET auth probe (other providers).
            if let Some((path, body)) = account.provider.prefetch_request() {
                let url = format!("{upstream}{path}");
                match prefetch_send(&client, &url, &account.provider, token, &body).await {
                    Ok(r) => {
                        let status = r.status();
                        if status == reqwest::StatusCode::UNAUTHORIZED {
                            // Attempt refresh for OAuth accounts.
                            if let Some(oauth_cred) = cred.as_oauth() {
                                match account.provider.refresh_token(oauth_cred).await {
                                    Ok(fresh) => {
                                        let mut store = crate::config::CredentialsStore::load();
                                        store.insert_resolved(account.name.clone(), Credential::Oauth(fresh.clone()));
                                        store.save().ok();
                                        live_creds.write().await.insert(account.name.clone(), Credential::Oauth(fresh));
                                        state.clear_auth_failed(&account.name);
                                        if state.is_health_check_failed(&account.name) {
                                            state.clear_health_check_failed(&account.name);
                                        }
                                        tracing::info!(account = %account.name, "health check: token refreshed");
                                    }
                                    Err(e) => {
                                        tracing::error!(account = %account.name, "health check: refresh failed: {e}");
                                        state.set_auth_failed(&account.name);
                                    }
                                }
                            } else {
                                tracing::error!(account = %account.name, "health check: 401 — API key rejected");
                                state.set_auth_failed(&account.name);
                            }
                        } else if status.is_server_error() {
                            let count = state.record_health_check_failure(&account.name, FAILURE_THRESHOLD);
                            tracing::warn!(account = %account.name, status = %status,
                                failures = count, "health check: server error");
                        } else {
                            // Success — update rate limits if available.
                            if let Some(info) = account.provider.parse_rate_limits(r.headers()) {
                                state.update_rate_limits(&account.name, info);
                            }
                            if state.is_health_check_failed(&account.name) {
                                tracing::info!(account = %account.name, "health check recovered");
                            }
                            state.clear_health_check_failed(&account.name);
                        }
                    }
                    Err(e) => {
                        let count = state.record_health_check_failure(&account.name, FAILURE_THRESHOLD);
                        tracing::warn!(account = %account.name, failures = count,
                            "health check probe failed: {e}");
                    }
                }
            } else if let Some(probe_path) = account.provider.auth_probe_get_path() {
                let probe_upstream = account.upstream_url.as_deref()
                    .unwrap_or_else(|| account.provider.default_upstream_url());
                let url = format!("{probe_upstream}{probe_path}");
                let mut headers = reqwest::header::HeaderMap::new();
                let _ = account.provider.inject_auth_headers(&mut headers, token);
                match client.get(&url).headers(headers).send().await {
                    Ok(r) => {
                        let status = r.status();
                        if status == reqwest::StatusCode::UNAUTHORIZED {
                            if let Some(oauth_cred) = cred.as_oauth() {
                                match account.provider.refresh_token(oauth_cred).await {
                                    Ok(fresh) => {
                                        let mut store = crate::config::CredentialsStore::load();
                                        store.insert_resolved(account.name.clone(), Credential::Oauth(fresh.clone()));
                                        store.save().ok();
                                        live_creds.write().await.insert(account.name.clone(), Credential::Oauth(fresh));
                                        state.clear_auth_failed(&account.name);
                                        state.clear_health_check_failed(&account.name);
                                        tracing::info!(account = %account.name, "health check: token refreshed (GET probe)");
                                    }
                                    Err(e) => {
                                        tracing::error!(account = %account.name, "health check: refresh failed: {e}");
                                        state.set_auth_failed(&account.name);
                                    }
                                }
                            } else {
                                tracing::error!(account = %account.name, "health check: 401 — API key rejected");
                                state.set_auth_failed(&account.name);
                            }
                        } else if status.is_server_error() {
                            let count = state.record_health_check_failure(&account.name, FAILURE_THRESHOLD);
                            tracing::warn!(account = %account.name, status = %status,
                                failures = count, "health check: server error (GET probe)");
                        } else {
                            if state.is_health_check_failed(&account.name) {
                                tracing::info!(account = %account.name, "health check recovered");
                            }
                            state.clear_health_check_failed(&account.name);
                        }
                    }
                    Err(e) => {
                        let count = state.record_health_check_failure(&account.name, FAILURE_THRESHOLD);
                        tracing::warn!(account = %account.name, failures = count,
                            "health check probe failed: {e}");
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(config.server.health_check_interval_secs)).await;
    }
}

/// Watches for account cooldowns expiring and triggers a post-cooldown prefetch
/// so each account re-enters rotation with fresh rate-limit metrics.
///
/// Analogous to `recovery_watcher` (which handles `auth_failed` accounts), but
/// for timed cooldowns (429 / 529 / 401 / 403 backoffs). Sleeps precisely until
/// the next cooldown deadline rather than polling at a fixed interval.
///
/// Also handles stale rate-limit data: if an account's rate-limit snapshot is
/// older than STALE_RL_MS and the account is available, a lightweight prefetch
/// is triggered so the router always has fresh utilization metrics.
pub async fn cooldown_watcher(
    config: Arc<Config>,
    state: StateStore,
    credentials: LiveCredentials,
) {
    /// Re-fetch rate-limit headers if data is older than 1 hour.
    const STALE_RL_MS: u64 = 60 * 60_000;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_default();

    // In-memory: the cooldown_until_ms value we already ran a post-resume for.
    // Prevents re-triggering on every poll after expiry.
    let mut last_resumed: HashMap<String, u64> = HashMap::new();
    // Accounts whose cooldown was long enough (≥5 min) to deserve a "back online" notification.
    let mut notify_on_resume: HashSet<String> = HashSet::new();
    // Epoch-ms of the last successful stale-prefetch per account.
    let mut last_stale_prefetch: HashMap<String, u64> = HashMap::new();

    loop {
        let states = state.account_states();
        let rl_snapshot = state.rate_limit_snapshot();
        let now = now_ms();
        let mut next_wake_ms: Option<u64> = None;

        for account in &config.accounts {
            let Some(st) = states.get(&account.name) else { continue };
            if st.disabled { continue; } // auth_failed or permanently disabled
            let cdl = st.cooldown_until_ms;

            if cdl > 0 && cdl <= now {
                // Cooldown expired — skip if we already handled this exact deadline
                let handled = last_resumed.get(&account.name).map(|&t| t >= cdl).unwrap_or(false);
                if !handled {
                    tracing::info!(account = %account.name, "cooldown expired — strong resume prefetch");
                    let token = {
                        let creds = credentials.read().await;
                        creds.get(&account.name).map(|c| c.bearer_token().to_owned())
                    };
                    if let Some(token) = token {
                        post_cooldown_prefetch(
                            &client, account, &token, &state,
                            &config.server.upstream_url,
                        ).await;
                    }
                    if notify_on_resume.remove(&account.name) && !state.get_alerts_muted() {
                        notify(
                            "shunt: Account Resumed",
                            &format!("Account '{}' is back online.", account.name),
                            "Glass",
                        );
                    }
                    last_resumed.insert(account.name.clone(), cdl);
                    last_stale_prefetch.insert(account.name.clone(), now);
                }
            } else if cdl > now {
                // Still cooling — schedule wake at expiry; flag for notification if long
                let remaining = cdl - now;
                if remaining >= 5 * 60_000 {
                    notify_on_resume.insert(account.name.clone());
                }
                next_wake_ms = Some(next_wake_ms.map(|m| m.min(cdl)).unwrap_or(cdl));
            } else {
                // Not in cooldown — check for stale rate-limit data
                let rl_age = rl_snapshot
                    .get(&account.name)
                    .map(|r| now.saturating_sub(r.updated_ms))
                    .unwrap_or(u64::MAX); // no data → treat as infinitely stale
                let last_fetched = last_stale_prefetch.get(&account.name).copied().unwrap_or(0);
                let fetched_ago = now.saturating_sub(last_fetched);

                if rl_age >= STALE_RL_MS && fetched_ago >= STALE_RL_MS {
                    tracing::debug!(
                        account = %account.name,
                        age_min = rl_age / 60_000,
                        "rate-limit data stale — refreshing"
                    );
                    let token = {
                        let creds = credentials.read().await;
                        creds.get(&account.name).map(|c| c.bearer_token().to_owned())
                    };
                    if let Some(token) = token {
                        post_cooldown_prefetch(
                            &client, account, &token, &state,
                            &config.server.upstream_url,
                        ).await;
                    }
                    last_stale_prefetch.insert(account.name.clone(), now);
                }
            }
        }

        // Sleep exactly until the next cooldown expires; fall back to 30s poll
        let sleep_ms = next_wake_ms
            .map(|wake| wake.saturating_sub(now_ms()).max(50))
            .unwrap_or(30_000);
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
    }
}

use crate::notify::notify;
use crate::translate::{
    translate_to_anthropic,
    translate_from_anthropic,
    uuid_v4,
    translate_anthropic_stream,
    translate_anthropic_req_to_chatgpt,
    translate_response_chatgpt_to_anthropic,
    translate_anthropic_req_to_openai,
    translate_response_openai_to_anthropic,
    translate_response_anthropic_to_openai,
};

// ---------------------------------------------------------------------------
// OpenAI-compatible API (translates to Anthropic Claude)
// ---------------------------------------------------------------------------
//
// When the OpenAI proxy receives a request at /v1/chat/completions, if an
// anthropic_base_url is configured, it translates the request to Anthropic
// Messages format and forwards it to the Anthropic proxy (which handles
// account selection, token management, and rate limiting).
// The response is translated back to OpenAI Chat Completions format.




/// GET /v1/models — return Claude models in OpenAI format.
async fn openai_models_handler() -> impl IntoResponse {
    axum::Json(json!({
        "object": "list",
        "data": [
            { "id": "claude-fable-5",              "object": "model", "owned_by": "anthropic" },
            { "id": "claude-mythos-5",             "object": "model", "owned_by": "anthropic" },
            { "id": "claude-opus-4-8",             "object": "model", "owned_by": "anthropic" },
            { "id": "claude-opus-4-7",             "object": "model", "owned_by": "anthropic" },
            { "id": "claude-opus-4-6",             "object": "model", "owned_by": "anthropic" },
            { "id": "claude-sonnet-4-6",           "object": "model", "owned_by": "anthropic" },
            { "id": "claude-haiku-4-5-20251001",   "object": "model", "owned_by": "anthropic" },
        ]
    }))
}

/// POST /v1/chat/completions — translate OpenAI request to Anthropic, proxy through Claude pool.
async fn openai_compat_handler(
    State(s): State<AppState>,
    req: Request,
) -> Result<Response, ProxyError> {
    let Some(ref anthropic_url) = s.anthropic_base_url else {
        // No Anthropic proxy configured — fall back to normal forwarding
        return proxy_handler(State(s), req).await;
    };

    let body_bytes = axum::body::to_bytes(req.into_body(), MAX_REQUEST_BODY)
        .await
        .map_err(|_| ProxyError::BodyRead)?;

    let openai_body: serde_json::Value = serde_json::from_slice(&body_bytes)
        .unwrap_or(json!({}));

    let stream = openai_body["stream"].as_bool().unwrap_or(false);
    let anthropic_body = translate_to_anthropic(openai_body);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|_| ProxyError::Upstream)?;

    let mut req_builder = client
        .post(format!("{anthropic_url}/v1/messages"))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "claude-code-20250219,oauth-2025-04-20")
        .header("x-shunt-compat", "openai");
    if let Some(ref key) = s.config.server.remote_key {
        req_builder = req_builder.header("x-api-key", key.as_str());
    }
    let resp = req_builder
        .json(&anthropic_body)
        .send()
        .await
        .map_err(|_| ProxyError::Upstream)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let code = status.as_u16();
        return Ok(axum::response::Response::builder()
            .status(code)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap());
    }

    if stream {
        // Translate Anthropic SSE stream → OpenAI SSE stream
        let chat_id = format!("chatcmpl-{}", &uuid_v4()[..8]);
        let stream = translate_anthropic_stream(resp, chat_id);
        Ok(axum::response::Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .body(axum::body::Body::from_stream(stream))
            .unwrap())
    } else {
        let anthropic_resp: serde_json::Value = resp.json().await.map_err(|_| ProxyError::Upstream)?;
        let openai_resp = translate_from_anthropic(anthropic_resp);
        Ok(axum::Json(openai_resp).into_response())
    }
}

// ---------------------------------------------------------------------------
// ChatGPT backend API translation (chatgpt.com /backend-api/conversation)
// ---------------------------------------------------------------------------

/// Fetch the sentinel token required by chatgpt.com's backend API.
/// Returns None if the request fails or proof-of-work is required.
async fn fetch_sentinel_token(client: &reqwest::Client, upstream: &str, token: &str) -> Option<String> {
    let url = format!("{}/backend-api/sentinel/chat-requirements", upstream);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    if json["proofofwork"]["required"].as_bool() == Some(true) {
        return None;
    }
    json["token"].as_str().map(ToOwned::to_owned)
}


/// Parse a 429 response's retry delay, preferring the millisecond-precision
/// `retry-after-ms` header (used by Anthropic's SDK-level burst throttling)
/// over the standard `retry-after` (whole seconds).
fn parse_retry_after_ms(headers: &axum::http::HeaderMap) -> Option<u64> {
    if let Some(ms) = headers.get("retry-after-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        return Some(ms.max(500));
    }
    headers.get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(|secs| secs.saturating_mul(1_000).max(500))
}

/// Mirrors `StateStore::is_exhausted`'s semantics: a window only counts as
/// exhausted if Anthropic's status says so AND its reset is still in the future.
/// Detect the "buy more tokens" condition from unified rate-limit info: a window
/// is exhausted (utilization at/over its included allotment) while extra-usage
/// (overage) is disabled/rejected, so Anthropic cannot overflow and Claude Code
/// shows the buy-more prompt. Returns the binding window's reset (epoch seconds)
/// so the caller can cool the account until it frees up. Returns None when the
/// account can still serve normally (overage allowed, or not exhausted).
fn overage_exhausted_reset(info: &RateLimitInfo) -> Option<u64> {
    // Only relevant when overage is explicitly disabled/rejected. When the header
    // is absent or "allowed", the account can overflow and won't show buy-more.
    let overage_blocked = matches!(info.overage_status.as_deref(), Some("rejected") | Some("disabled"));
    if !overage_blocked {
        return None;
    }
    let now_secs = now_ms() / 1_000;
    let ex_5h = info.status_5h.as_deref() == Some("exhausted")
        && info.reset_5h.map(|t| t > now_secs).unwrap_or(false);
    let ex_7d = info.status_7d.as_deref() == Some("exhausted")
        && info.reset_7d.map(|t| t > now_secs).unwrap_or(false);
    match (ex_5h, ex_7d) {
        (true, true) => info.reset_5h.min(info.reset_7d), // soonest of the two
        (true, false) => info.reset_5h,
        (false, true) => info.reset_7d,
        (false, false) => None,
    }
}

/// Utilization at which a 7-day window in `allowed_warning` is treated as
/// "about to tip into the weekly cap". Above this, we briefly cool the account
/// so routing prefers fresher lanes — keeping Claude Code from landing on an
/// account right as it crosses its included weekly quota (which surfaces the
/// buy-more/upgrade prompt). Left generous (10% headroom) so accounts stay
/// usable as a last resort when everything fresher is cooling.
const NEAR_CAP_7D_UTILIZATION: f64 = 0.9;

/// Soft-deprioritize cooldown for a near-cap 7d window. Short on purpose: it
/// nudges routing toward fresher accounts without removing capacity for long,
/// and re-applies each time the account is used while still near the cap.
const NEAR_CAP_COOL_MS: u64 = 2 * 60_000;

/// Detect a 7-day window that is at/over [`NEAR_CAP_7D_UTILIZATION`] while
/// Anthropic still reports it usable (`allowed_warning`) or already `exhausted`.
/// Complements [`overage_exhausted_reset`], which only fires when the
/// `overage-status` header is present; most subscriptions don't send it, so
/// this utilization-based check is what actually protects them. Returns true
/// when the account should be briefly cooled.
fn near_cap_7d_warning(info: &RateLimitInfo) -> bool {
    let util_high = info.utilization_7d.map(|u| u >= NEAR_CAP_7D_UTILIZATION).unwrap_or(false);
    if !util_high {
        return false;
    }
    // Only when the reset is still in the future (otherwise the window is about
    // to roll over and there is nothing to protect).
    let now_secs = now_ms() / 1_000;
    let reset_future = info.reset_7d.map(|r| r > now_secs).unwrap_or(false);
    let warning_or_exhausted = matches!(
        info.status_7d.as_deref(),
        Some("allowed_warning") | Some("warning") | Some("exhausted")
    );
    reset_future && warning_or_exhausted
}

fn is_exhausted_response(info: Option<&RateLimitInfo>) -> bool {
    let Some(info) = info else { return false };
    let now_secs = now_ms() / 1_000;
    let exhausted_5h = info.status_5h.as_deref() == Some("exhausted")
        && info.reset_5h.map(|t| t > now_secs).unwrap_or(false);
    let exhausted_7d = info.status_7d.as_deref() == Some("exhausted")
        && info.reset_7d.map(|t| t > now_secs).unwrap_or(false);
    exhausted_5h || exhausted_7d
}

/// Compute how long to cool an account after a 429.
///
/// Anthropic's unified rate limiter reports `reset_5h`/`reset_7d` on essentially
/// every response once an account has made one request — even when the 429 was
/// just a transient per-minute burst throttle, not real quota exhaustion. Treating
/// every 429 as "derive cooldown from reset_5h/7d" therefore forced a near-5-minute
/// cooldown on routine burst hits, which is the dominant case with only 1-2
/// accounts in the pool and made simultaneous "all accounts cooling" stalls likely.
///
/// Only use the long reset-derived cooldown when a window is genuinely exhausted;
/// otherwise trust `Retry-After`, which Anthropic sets to the real, short burst
/// delay (seconds, occasionally given as `retry-after-ms`).
fn compute_429_cooldown_ms(info: Option<&RateLimitInfo>, retry_after_ms: Option<u64>, is_exhausted: bool) -> u64 {
    const MAX_EXHAUSTED_COOLDOWN_MS: u64 = 5 * 60_000;
    // A burst (non-exhausted) 429 means the account still has 5h/7d quota and
    // only tripped Anthropic's short-term rate/concurrency limit, which clears
    // in seconds. Parking it for minutes drains an otherwise-healthy pool under
    // many concurrent agents, so keep the burst bench short: honor retry-after
    // but cap it low. Exhaustion (real quota drain) still cools until reset.
    const BURST_DEFAULT_MS: u64 = 5_000;
    const BURST_MIN_MS: u64 = 2_000;
    const MAX_BURST_COOLDOWN_MS: u64 = 15_000;
    if is_exhausted {
        info.and_then(|i| i.reset_5h.or(i.reset_7d))
            .map(|reset_secs| {
                let reset_ms = reset_secs.saturating_mul(1_000);
                reset_ms.saturating_sub(now_ms()).saturating_add(500) // +500ms buffer
            })
            .or(retry_after_ms)
            .unwrap_or(90_000)
            .min(MAX_EXHAUSTED_COOLDOWN_MS)
    } else {
        let base = retry_after_ms
            .unwrap_or(BURST_DEFAULT_MS)
            .clamp(BURST_MIN_MS, MAX_BURST_COOLDOWN_MS);
        // Add sub-window jitter so accounts that burst-429 together don't all
        // expire at the same instant and re-trip as a synchronized herd (the
        // "post-cooldown prefetch got 429 — re-cooling" loop). Bounded so the
        // bench stays short.
        base.saturating_add(burst_cooldown_jitter_ms())
    }
}

/// Small randomized cooldown jitter (0..3000ms) to break herd synchronization.
/// Uses sub-millisecond clock entropy — no extra dependency, good enough to
/// spread near-simultaneous 429 cooldowns across different wake instants.
fn burst_cooldown_jitter_ms() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % 3_000
}

/// Returns true if the model lacks support for extended thinking / effort.
/// These params must be stripped before forwarding.
fn is_simple_model(model: &str) -> bool {
    model.contains("haiku")
}

/// Remove request-body parameters that simpler models (e.g. Haiku) reject:
/// top-level `thinking` / `effort` / `reasoning_effort` / `context_management`,
/// `effort` nested inside `output_config`, and the interleaved-thinking beta in
/// a `betas` array. Returns true if anything was removed.
///
/// Must be applied whenever the outgoing model becomes a simple model — both on
/// the initial request and after a fallback downgrade — otherwise the upstream
/// returns e.g. `400 This model does not support the effort parameter`.
fn strip_unsupported_params_for_simple_model(val: &mut serde_json::Value) -> bool {
    let Some(obj) = val.as_object_mut() else { return false };
    let mut changed = false;
    for key in &["thinking", "effort", "reasoning_effort", "context_management"] {
        if obj.remove(*key).is_some() {
            changed = true;
        }
    }
    if let Some(serde_json::Value::Object(oc)) = obj.get_mut("output_config") {
        if oc.remove("effort").is_some() {
            changed = true;
        }
        if oc.is_empty() {
            obj.remove("output_config");
            changed = true;
        }
    }
    if let Some(serde_json::Value::Array(betas)) = obj.get_mut("betas") {
        let before = betas.len();
        betas.retain(|b| b.as_str() != Some("interleaved-thinking-2025-05-14"));
        if betas.len() != before {
            changed = true;
        }
    }
    changed
}

/// Strip thinking/effort-related beta flags from the `anthropic-beta` header for
/// simple models. Mirrors [`strip_unsupported_params_for_simple_model`] on the
/// header side so a fallback downgrade doesn't leave an incompatible beta flag.
fn strip_beta_header_for_simple_model(headers: &mut axum::http::HeaderMap) {
    let Some(beta_val) = headers
        .get("anthropic-beta")
        .and_then(|v| v.to_str().ok().map(|s| s.to_owned()))
    else {
        return;
    };
    let filtered: Vec<&str> = beta_val
        .split(',')
        .map(|s| s.trim())
        .filter(|b| !b.contains("thinking") && !b.contains("effort"))
        .collect();
    if filtered.is_empty() {
        headers.remove("anthropic-beta");
    } else if let Ok(v) = axum::http::HeaderValue::from_str(&filtered.join(",")) {
        headers.insert("anthropic-beta", v);
    }
}

/// The 1M-context beta capability flag Claude Code sends in `anthropic-beta`
/// when a `[1m]` model is selected. On the direct pass-through path it must be
/// forwarded verbatim (entitled Max accounts need it), but when shunt REWRITES
/// the model to something without a 1M window (Haiku, or a fallback target it
/// can't prove is entitled), the flag must be dropped — otherwise Anthropic
/// returns `400 The long context beta is not yet available for this subscription`.
const LONG_CONTEXT_BETA: &str = "context-1m-2025-08-07";

/// Remove the 1M-context beta flag from the `anthropic-beta` header. Preserves
/// every other flag (oauth, tool streaming, etc.) and removes the header only
/// when nothing remains. Returns true if the flag was present and removed.
fn strip_long_context_beta(headers: &mut axum::http::HeaderMap) -> bool {
    let Some(beta_val) = headers
        .get("anthropic-beta")
        .and_then(|v| v.to_str().ok().map(|s| s.to_owned()))
    else {
        return false;
    };
    if !beta_val.contains(LONG_CONTEXT_BETA) {
        return false;
    }
    let filtered: Vec<&str> = beta_val
        .split(',')
        .map(|s| s.trim())
        .filter(|b| *b != LONG_CONTEXT_BETA && !b.is_empty())
        .collect();
    if filtered.is_empty() {
        headers.remove("anthropic-beta");
    } else if let Ok(v) = axum::http::HeaderValue::from_str(&filtered.join(",")) {
        headers.insert("anthropic-beta", v);
    }
    true
}

/// Strip a literal `[1m]` (or `[1M]`) context suffix from a request's `model`
/// field. Claude Code normally sends the 1M request as the `context-1m` beta
/// header with a bare model name, but some paths carry the suffix inline; if it
/// survives onto a model shunt rewrote, the upstream rejects it. Returns true if
/// the model string changed.
fn normalize_model_suffix(val: &mut serde_json::Value) -> bool {
    let Some(model) = val.get("model").and_then(|m| m.as_str()) else {
        return false;
    };
    let trimmed = model
        .strip_suffix("[1m]")
        .or_else(|| model.strip_suffix("[1M]"));
    if let Some(base) = trimmed {
        let base = base.to_owned();
        val["model"] = serde_json::Value::String(base);
        return true;
    }
    false
}

/// Detect Claude Code's auto-mode safety classifier request. The classifier's
/// system prompt begins "You are a security monitor for autonomous AI coding
/// agents." The `system` field may be a plain string or an array of content
/// blocks ({"type":"text","text":...}); check both shapes.
fn is_safety_classifier(val: &serde_json::Value) -> bool {
    const NEEDLE: &str = "security monitor for autonomous AI coding agents";
    match val.get("system") {
        Some(serde_json::Value::String(s)) => s.contains(NEEDLE),
        Some(serde_json::Value::Array(blocks)) => blocks.iter().any(|b| {
            b.get("text").and_then(|t| t.as_str()).map(|t| t.contains(NEEDLE)).unwrap_or(false)
        }),
        _ => false,
    }
}

/// Model the classifier is pinned to: always-available, cheap, and returns a
/// valid allow/block verdict. Claude Code ignores `CLAUDE_CODE_AUTO_MODE_MODEL`,
/// so the proxy is the only place this redirect can happen.
const CLASSIFIER_MODEL: &str = "claude-haiku-4-5-20251001";

/// Character budget for the classifier transcript when a custom (self-hosted)
/// prompt is used. A small model has a bounded context window (~32K tokens);
/// this keeps the transcript tail well inside it (~20-25K tokens) with room for
/// the system prompt and the verdict. Only applied on the custom-prompt path.
const CLASSIFIER_MAX_TRANSCRIPT_CHARS: usize = 80_000;

/// Bound the total size of a classifier request so it cannot overflow a small
/// model's context window. Claude Code puts the action under review at the END
/// (the last tool call, then the "<block> immediately" suffix), so when the
/// text is too large we keep the TAIL and drop earlier history — which is
/// context only. This is total-aware across the whole `messages` array and
/// robust to either shape Claude Code uses: one big string, or many small
/// `{type,text}` content blocks. We walk text from the FRONT, blanking whole
/// blocks/messages until the remaining total fits, then partially trim the
/// boundary block. The final text block (the action + verdict suffix) is always
/// preserved. Returns true if anything was trimmed.
fn bound_classifier_transcript(val: &mut serde_json::Value, max_chars: usize) -> bool {
    const NOTICE: &str = "[earlier transcript truncated to fit classifier context]\n";
    let Some(messages) = val.get_mut("messages").and_then(|m| m.as_array_mut()) else {
        return false;
    };
    if messages.is_empty() {
        return false;
    }
    // Total text across every message/block. Note: each retained message also
    // costs chat-template envelope tokens, so blanking text but keeping
    // thousands of empty messages does NOT help. We therefore collapse to the
    // FINAL message (which carries the <transcript>, the action under review as
    // its last tool call, and the "<block> immediately" suffix) and drop all
    // earlier conversation, which is context only for the classifier.
    let text_len = |m: &serde_json::Value| -> usize {
        match m.get("content") {
            Some(serde_json::Value::String(s)) => s.chars().count(),
            Some(serde_json::Value::Array(blocks)) => blocks
                .iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .map(|t| t.chars().count())
                .sum(),
            _ => 0,
        }
    };
    let total: usize = messages.iter().map(|m| text_len(m)).sum();
    let dropping_earlier = messages.len() > 1;
    // Keep only the last message.
    let mut last = messages.pop().expect("non-empty");
    let last_len = text_len(&last);

    // Within the last message, trim from the front if it alone still overflows.
    let mut trimmed_last = false;
    if last_len > max_chars {
        match last.get_mut("content") {
            Some(serde_json::Value::String(s)) => {
                let n = s.chars().count();
                let tail: String = s.chars().skip(n - max_chars).collect();
                *s = format!("{NOTICE}{tail}");
                trimmed_last = true;
            }
            Some(serde_json::Value::Array(blocks)) => {
                // Keep whole blocks from the END until we hit the budget; always
                // keep the final block (holds the action + verdict suffix).
                let mut kept: std::collections::VecDeque<serde_json::Value> = Default::default();
                let mut acc = 0usize;
                for b in blocks.iter().rev() {
                    let l = b.get("text").and_then(|t| t.as_str()).map(|t| t.chars().count()).unwrap_or(0);
                    if kept.is_empty() || acc + l <= max_chars {
                        acc += l;
                        kept.push_front(b.clone());
                    } else {
                        break;
                    }
                }
                // If the single kept block still overflows, trim its front.
                if kept.len() == 1 {
                    if let Some(t) = kept[0].get("text").and_then(|t| t.as_str()) {
                        let n = t.chars().count();
                        if n > max_chars {
                            let tail: String = t.chars().skip(n - max_chars).collect();
                            kept[0]["text"] = serde_json::Value::String(format!("{NOTICE}{tail}"));
                        }
                    }
                }
                *blocks = kept.into_iter().collect();
                trimmed_last = true;
            }
            _ => {}
        }
    }
    messages.clear();
    messages.push(last);

    // Only report a change when we actually altered the payload.
    (dropping_earlier && total > max_chars) || trimmed_last
}

/// Rewrite a classifier request's `model` to [`CLASSIFIER_MODEL`], leaving every
/// other field — crucially the `system` array with its `x-anthropic-billing-header`
/// block — untouched. Returns true if the model changed.
fn pin_model_to_classifier(val: &mut serde_json::Value) -> bool {
    let cur = val.get("model").and_then(|m| m.as_str()).unwrap_or("");
    if cur == CLASSIFIER_MODEL {
        return false;
    }
    val["model"] = serde_json::Value::String(CLASSIFIER_MODEL.to_owned());
    true
}

/// Auto-detect a fallback model based on the current model name.
/// opus → sonnet, sonnet → haiku, anything else → None.
fn auto_fallback_model(model: &str) -> Option<&'static str> {
    if model.contains("opus") {
        Some("claude-sonnet-4-6")
    } else if model.contains("sonnet") {
        Some("claude-haiku-4-5-20251001")
    } else {
        None
    }
}

/// Resolve the target model name for a non-Anthropic account.
///
/// Priority: per-account `model` pin → global `model_mapping` → provider `default_model()`.
/// If the provider is `Local` (default_model = ""), the incoming model name is passed through.
fn resolve_model(
    incoming: &str,
    account: &crate::config::AccountConfig,
    mapping: &std::collections::HashMap<String, String>,
) -> String {
    // 1. Per-account pin (highest priority).
    if let Some(m) = &account.model {
        return m.clone();
    }
    // 2. Global mapping for this specific incoming model name.
    if let Some(m) = mapping.get(incoming) {
        return m.clone();
    }
    // 3. Provider default.
    let default = account.provider.default_model();
    if !default.is_empty() {
        return default.to_owned();
    }
    // 4. Pass through (Local provider — model name is server-defined).
    incoming.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn anthropic_prefetch_uses_the_account_upstream_override() {
        use axum::{routing::post, Json, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_route = hits.clone();
        let app = Router::new().route(
            "/v1/messages",
            post(move || {
                let hits = hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    (
                        [
                            ("anthropic-ratelimit-unified-5h-utilization", "0.1"),
                            ("anthropic-ratelimit-unified-7d-utilization", "0.2"),
                        ],
                        Json(json!({"type": "message"})),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let account_name = "claude/smoke".to_owned();
        let config = crate::config::Config {
            schema_version: crate::config::CONFIG_SCHEMA_VERSION,
            server: crate::config::ServerConfig {
                upstream_url: "http://127.0.0.1:1".to_owned(),
                ..Default::default()
            },
            accounts: vec![crate::config::AccountConfig {
                name: account_name.clone(),
                plan_type: "api".to_owned(),
                provider: Provider::AnthropicApi,
                credential: Some(Credential::Apikey { key: "test-key".to_owned() }),
                upstream_url: Some(format!("http://{address}/")),
                model: None,
            }],
            config_file: "/dev/null".into(),
            model_mapping: Default::default(),
            api_overflow: Default::default(),
            pools: Default::default(),
            secrets: Default::default(),
            classifier: Default::default(),
            bridge: Default::default(),
            manual_swarm: Default::default(),
        };
        let state = StateStore::new_empty();
        let live_credentials = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        prefetch_rate_limits(Arc::new(config), state.clone(), live_credentials).await;

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let limits = state.rate_limit_snapshot();
        assert_eq!(limits[&account_name].utilization_5h, Some(0.1));
        assert_eq!(limits[&account_name].utilization_7d, Some(0.2));
        assert!(!state.account_states().get(&account_name).map(|s| s.auth_failed).unwrap_or(false));
        server.abort();
    }

    fn make_info(status_5h: Option<&str>, reset_5h: Option<u64>, status_7d: Option<&str>, reset_7d: Option<u64>) -> RateLimitInfo {
        RateLimitInfo {
            status_5h: status_5h.map(str::to_owned),
            reset_5h,
            status_7d: status_7d.map(str::to_owned),
            reset_7d,
            utilization_5h: Some(0.5),
            utilization_7d: Some(0.3),
            ..Default::default()
        }
    }

    fn future_secs() -> u64 {
        now_ms() / 1_000 + 3600
    }

    // NB: burst cooldowns add up to 3s of anti-herd jitter on top of the base,
    // so these assert the base within a [base, base+3000) band rather than exact.
    const JITTER_MAX_MS: u64 = 3_000;

    #[test]
    fn burst_429_uses_retry_after_within_short_cap() {
        let info = make_info(Some("allowed"), Some(future_secs()), Some("allowed"), None);
        // A small retry-after is honored as-is (burst limits clear in seconds).
        let cd = compute_429_cooldown_ms(Some(&info), Some(8_000), false);
        assert!((8_000..8_000 + JITTER_MAX_MS).contains(&cd), "burst 429 honors short retry_after (+jitter): {cd}");
    }

    #[test]
    fn burst_429_caps_at_15s() {
        // Burst cooldown must stay short so a healthy pool is not drained; even a
        // large retry-after is clamped to the 15s burst cap (plus jitter).
        let cd = compute_429_cooldown_ms(None, Some(999_999), false);
        assert!((15_000..15_000 + JITTER_MAX_MS).contains(&cd), "burst cooldown caps at 15s (+jitter): {cd}");
    }

    #[test]
    fn burst_429_default_when_no_retry_after() {
        let cd = compute_429_cooldown_ms(None, None, false);
        assert!((5_000..5_000 + JITTER_MAX_MS).contains(&cd), "burst cooldown defaults to 5s (+jitter): {cd}");
    }

    #[test]
    fn burst_429_floor_prevents_thrash() {
        // A tiny/zero retry-after is floored so we don't hot-loop the account.
        let cd = compute_429_cooldown_ms(None, Some(100), false);
        assert!((2_000..2_000 + JITTER_MAX_MS).contains(&cd), "burst cooldown floors at 2s (+jitter): {cd}");
    }

    #[test]
    fn exhausted_429_uses_reset_derived_cooldown() {
        let reset = now_ms() / 1_000 + 120; // resets in 2 min
        let info = make_info(Some("exhausted"), Some(reset), Some("allowed"), None);
        let cd = compute_429_cooldown_ms(Some(&info), Some(5_000), true);
        assert!(cd > 60_000 && cd <= 120_500, "exhausted should use reset-derived cooldown (~120s), got {cd}");
    }

    #[test]
    fn exhausted_429_caps_at_5_min() {
        let reset = now_ms() / 1_000 + 7200; // resets in 2 hours
        let info = make_info(Some("exhausted"), Some(reset), Some("allowed"), None);
        let cd = compute_429_cooldown_ms(Some(&info), None, true);
        assert_eq!(cd, 5 * 60_000, "exhausted cooldown should cap at 5 min");
    }

    #[test]
    fn is_exhausted_requires_status_and_future_reset() {
        let future = future_secs();
        let past = now_ms() / 1_000 - 3600;
        assert!(is_exhausted_response(Some(&make_info(Some("exhausted"), Some(future), None, None))));
        assert!(!is_exhausted_response(Some(&make_info(Some("exhausted"), Some(past), None, None))));
        assert!(!is_exhausted_response(Some(&make_info(Some("allowed"), Some(future), None, None))));
        assert!(!is_exhausted_response(None));
    }

    #[test]
    fn parse_retry_after_ms_prefers_ms_header() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("retry-after-ms", "1500".parse().unwrap());
        headers.insert("retry-after", "30".parse().unwrap());
        assert_eq!(parse_retry_after_ms(&headers), Some(1500));
    }

    #[test]
    fn parse_retry_after_ms_falls_back_to_seconds() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("retry-after", "5".parse().unwrap());
        assert_eq!(parse_retry_after_ms(&headers), Some(5_000));
    }

    #[test]
    fn classifier_detected_when_system_is_string() {
        let v = json!({
            "model": "claude-fable-5",
            "system": "You are a security monitor for autonomous AI coding agents.\n..."
        });
        assert!(is_safety_classifier(&v));
    }

    #[test]
    fn classifier_detected_when_system_is_block_array() {
        // Modern Claude Code sends `system` as an array of {type,text} blocks.
        let v = json!({
            "model": "claude-fable-5",
            "system": [
                {"type": "text", "text": "You are a security monitor for autonomous AI coding agents."},
                {"type": "text", "text": "## Context ..."}
            ]
        });
        assert!(is_safety_classifier(&v));
    }

    #[test]
    fn classifier_not_detected_for_normal_request() {
        let v = json!({
            "model": "claude-fable-5",
            "system": [{"type": "text", "text": "You are Claude Code, Anthropic's CLI."}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        assert!(!is_safety_classifier(&v));
        // Missing system entirely must also be false (not a classifier call).
        assert!(!is_safety_classifier(&json!({"model": "claude-fable-5"})));
    }

    #[test]
    fn pin_model_rewrites_only_the_model_field() {
        let mut v = json!({
            "model": "claude-opus-4-8",
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.200; cc_entrypoint=cli; cch=abc123;"},
                {"type": "text", "text": "You are a security monitor for autonomous AI coding agents."}
            ],
            "messages": [{"role": "user", "content": "Action: echo hi"}]
        });
        let changed = pin_model_to_classifier(&mut v);
        assert!(changed, "opus should be rewritten to the classifier model");
        assert_eq!(v["model"], json!(CLASSIFIER_MODEL));
        // The billing-header block MUST survive: it is how Anthropic recognizes
        // Claude Code OAuth traffic. Losing it triggers a misleading 429.
        let system = v["system"].as_array().expect("system array preserved");
        assert_eq!(system.len(), 2, "no system blocks dropped");
        assert!(
            system.iter().any(|b| b["text"].as_str()
                .map(|t| t.starts_with("x-anthropic-billing-header:"))
                .unwrap_or(false)),
            "x-anthropic-billing-header block must be preserved",
        );
        // Messages untouched.
        assert_eq!(v["messages"][0]["content"], json!("Action: echo hi"));
    }

    #[test]
    fn pin_model_is_noop_when_already_classifier_model() {
        let mut v = json!({ "model": CLASSIFIER_MODEL, "system": "You are a security monitor for autonomous AI coding agents." });
        assert!(!pin_model_to_classifier(&mut v), "already on classifier model → no change");
        assert_eq!(v["model"], json!(CLASSIFIER_MODEL));
    }

    /// Replicates Claude Code's auto-mode verdict parser `gKi`: it strips
    /// <thinking>…</thinking>, then matches /<block>(yes|no)\b(<\/block>)?/i and
    /// reads the first capture. Returns Some(true)=block, Some(false)=allow,
    /// None=unparseable (which Claude Code treats as block / fail-closed).
    /// This guards the contract our custom Hetzner classifier prompt must emit.
    fn parse_claude_code_verdict(resp: &str) -> Option<bool> {
        // Strip thinking blocks like Claude Code's AKi() does.
        let mut cleaned = String::with_capacity(resp.len());
        let mut rest = resp;
        while let Some(start) = rest.find("<thinking>") {
            cleaned.push_str(&rest[..start]);
            match rest[start..].find("</thinking>") {
                Some(end) => rest = &rest[start + end + "</thinking>".len()..],
                None => {
                    rest = "";
                    break;
                }
            }
        }
        cleaned.push_str(rest);
        // First <block>yes|no marker (case-insensitive), word-boundary after.
        let lower = cleaned.to_ascii_lowercase();
        let idx = lower.find("<block>")?;
        let after = &lower[idx + "<block>".len()..];
        if let Some(v) = after.strip_prefix("yes") {
            if v.is_empty() || !v.starts_with(|c: char| c.is_ascii_alphanumeric()) {
                return Some(true);
            }
        }
        if let Some(v) = after.strip_prefix("no") {
            if v.is_empty() || !v.starts_with(|c: char| c.is_ascii_alphanumeric()) {
                return Some(false);
            }
        }
        None
    }

    #[test]
    fn custom_classifier_outputs_parse_as_claude_code_verdicts() {
        // The exact shapes our harness-classifier prompt instructs the Hetzner
        // model to emit must parse under Claude Code's own grammar.
        assert_eq!(parse_claude_code_verdict("<block>no</block>"), Some(false));
        assert_eq!(
            parse_claude_code_verdict(
                "<block>yes</block><category>Protected Mainline Push</category>\
                 <reason>[Protected Mainline Push] direct push to main</reason>"
            ),
            Some(true)
        );
        // Thinking prefix is stripped before the marker is read.
        assert_eq!(
            parse_claude_code_verdict("<thinking>looks safe</thinking><block>no</block>"),
            Some(false)
        );
        // A reply with no <block> marker is a parse failure -> Claude Code blocks.
        assert_eq!(parse_claude_code_verdict("I think this is fine."), None);
    }

    #[test]
    fn bound_classifier_transcript_keeps_tail_when_oversized() {
        // The action under review is at the END; trimming must preserve it.
        let head = "x".repeat(50);
        let action = "\nAssistant tool call: run_command {\"command\":\"git status\"}</transcript>\nErr on the side of blocking. <block> immediately.";
        let big = format!("<transcript>\n{head}{action}");
        let mut v = json!({"messages":[{"role":"user","content": big}]});
        assert!(bound_classifier_transcript(&mut v, 60));
        let out = v["messages"][0]["content"].as_str().unwrap();
        assert!(out.contains("truncated to fit"), "notice prepended");
        assert!(out.contains("<block> immediately."), "tail (the action) preserved");
        assert!(out.chars().count() <= 60 + 80, "trimmed near budget");
        // Under-budget content is left untouched.
        let mut small = json!({"messages":[{"role":"user","content":"short"}]});
        assert!(!bound_classifier_transcript(&mut small, 60));
        assert_eq!(small["messages"][0]["content"], json!("short"));
    }

    #[test]
    fn bound_classifier_transcript_trims_across_many_messages() {
        // Real Claude Code shape: bulk of tokens spread across many prior
        // messages, action at the end. Total must be bounded and the last
        // slot (the action) preserved.
        let mut msgs = Vec::new();
        for i in 0..500 {
            msgs.push(json!({"role":"assistant","content": format!("step {i} ran a command with some padding text here")}));
            msgs.push(json!({"role":"user","content":"ok continue"}));
        }
        msgs.push(json!({"role":"user","content":"ACTION git push origin main </transcript> <block> immediately."}));
        let mut v = json!({"messages": msgs});
        let before: usize = v["messages"].as_array().unwrap().iter()
            .filter_map(|m| m["content"].as_str()).map(|s| s.chars().count()).sum();
        assert!(bound_classifier_transcript(&mut v, 2000), "should trim; before={before}");
        let after: usize = v["messages"].as_array().unwrap().iter()
            .filter_map(|m| m["content"].as_str()).map(|s| s.chars().count()).sum();
        assert!(after <= 2000 + 120, "bounded near budget, got {after}");
        // The action (last slot) survives.
        let last = v["messages"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap();
        assert!(last.contains("git push origin main"), "action preserved: {last}");
    }

    #[test]
    fn custom_classifier_prompt_file_declares_the_verdict_contract() {
        // Ship-time guard: the checked-in prompt must still tell the model to
        // emit the <block> grammar, or every verdict silently fails closed.
        let prompt = include_str!("../examples/classifier-harness-prompt.txt");
        assert!(prompt.contains("<block>no</block>"), "prompt must show the allow token");
        assert!(prompt.contains("<block>yes</block>"), "prompt must show the block token");
    }

    #[test]
    fn strip_removes_effort_and_thinking_for_simple_model() {
        // Simulates a fallback downgrade to Haiku of an opus request that carried
        // high-effort + thinking params (the /compact 400 scenario).
        let mut v = json!({
            "model": "claude-haiku-4-5-20251001",
            "effort": "high",
            "output_config": { "effort": "high", "max_output_tokens": 4096 },
            "thinking": { "type": "enabled" },
            "context_management": { "edits": [] },
            "betas": ["interleaved-thinking-2025-05-14", "some-other-beta"],
            "messages": [{"role": "user", "content": "summarize"}]
        });
        assert!(strip_unsupported_params_for_simple_model(&mut v));
        assert!(v.get("effort").is_none(), "top-level effort dropped");
        assert!(v.get("thinking").is_none(), "thinking dropped");
        assert!(v.get("context_management").is_none(), "context_management dropped");
        assert!(v["output_config"].get("effort").is_none(), "output_config.effort dropped");
        // output_config kept because it still has a supported field.
        assert_eq!(v["output_config"]["max_output_tokens"], json!(4096));
        let betas = v["betas"].as_array().unwrap();
        assert!(!betas.iter().any(|b| b == "interleaved-thinking-2025-05-14"), "thinking beta dropped");
        assert!(betas.iter().any(|b| b == "some-other-beta"), "unrelated beta preserved");
        // Non-param fields untouched.
        assert_eq!(v["messages"][0]["content"], json!("summarize"));
    }

    #[test]
    fn strip_drops_output_config_when_only_effort() {
        let mut v = json!({
            "model": "claude-haiku-4-5-20251001",
            "output_config": { "effort": "high" }
        });
        assert!(strip_unsupported_params_for_simple_model(&mut v));
        assert!(v.get("output_config").is_none(), "empty output_config removed entirely");
    }

    #[test]
    fn strip_is_noop_when_no_unsupported_params() {
        let mut v = json!({
            "model": "claude-haiku-4-5-20251001",
            "messages": [{"role": "user", "content": "hi"}]
        });
        assert!(!strip_unsupported_params_for_simple_model(&mut v), "nothing to strip → no change");
    }

    #[test]
    fn strip_beta_header_removes_thinking_effort_flags() {
        let mut h = axum::http::HeaderMap::new();
        h.insert("anthropic-beta", "interleaved-thinking-2025-05-14,oauth-2025-04-20,effort-2025".parse().unwrap());
        strip_beta_header_for_simple_model(&mut h);
        let v = h.get("anthropic-beta").unwrap().to_str().unwrap();
        assert!(!v.contains("thinking"), "thinking beta removed");
        assert!(!v.contains("effort"), "effort beta removed");
        assert!(v.contains("oauth-2025-04-20"), "unrelated beta preserved");
    }

    #[test]
    fn near_cap_7d_warning_fires_only_near_cap_and_future_reset() {
        let now = now_ms() / 1_000;
        // At/over threshold + allowed_warning + future reset -> cool.
        assert!(near_cap_7d_warning(&crate::state::RateLimitInfo {
            utilization_7d: Some(0.92),
            reset_7d: Some(now + 3600),
            status_7d: Some("allowed_warning".into()),
            ..Default::default()
        }));
        // Exhausted also qualifies.
        assert!(near_cap_7d_warning(&crate::state::RateLimitInfo {
            utilization_7d: Some(0.99),
            reset_7d: Some(now + 3600),
            status_7d: Some("exhausted".into()),
            ..Default::default()
        }));
        // Below threshold -> leave it alone (this is the common "fresh" case).
        assert!(!near_cap_7d_warning(&crate::state::RateLimitInfo {
            utilization_7d: Some(0.64),
            reset_7d: Some(now + 3600),
            status_7d: Some("allowed_warning".into()),
            ..Default::default()
        }));
        // High util but plain "allowed" (no warning) -> not our signal.
        assert!(!near_cap_7d_warning(&crate::state::RateLimitInfo {
            utilization_7d: Some(0.95),
            reset_7d: Some(now + 3600),
            status_7d: Some("allowed".into()),
            ..Default::default()
        }));
        // Reset already past -> nothing to protect.
        assert!(!near_cap_7d_warning(&crate::state::RateLimitInfo {
            utilization_7d: Some(0.95),
            reset_7d: Some(now.saturating_sub(10)),
            status_7d: Some("allowed_warning".into()),
            ..Default::default()
        }));
    }

    #[test]
    fn warm_start_active_by_request_count_then_graduates() {
        let warm = ParkingMutex::new(HashMap::new());
        // warmup_requests=2, warmup_ms=0 so only the count gate applies.
        assert!(warm_start_active(&warm, Some("t1"), 2, 0), "1st request warms");
        assert!(warm_start_active(&warm, Some("t1"), 2, 0), "2nd request warms");
        assert!(!warm_start_active(&warm, Some("t1"), 2, 0), "3rd graduates to subs");
    }

    #[test]
    fn warm_start_active_by_age_window() {
        let warm = ParkingMutex::new(HashMap::new());
        // warmup_requests=0 so only the age gate applies; large window keeps it warm.
        assert!(warm_start_active(&warm, Some("t2"), 0, 60_000), "within age window warms");
    }

    #[test]
    fn warm_start_never_for_untraced_requests() {
        let warm = ParkingMutex::new(HashMap::new());
        assert!(!warm_start_active(&warm, None, 5, 60_000), "no trace → never warm-start");
    }

    #[test]
    fn burst_jitter_stays_in_bounds() {
        for _ in 0..1000 {
            assert!(burst_cooldown_jitter_ms() < 3_000, "jitter must stay under 3s");
        }
    }

    #[test]
    fn burst_cooldown_short_and_bounded_with_jitter() {
        // Burst (non-exhausted) 429: base clamps to 2-15s, plus <3s jitter.
        for _ in 0..200 {
            let ms = compute_429_cooldown_ms(None, None, false);
            assert!((2_000..=18_000).contains(&ms), "burst cooldown {ms} out of expected 2-18s band");
        }
    }

    #[test]
    fn exhausted_cooldown_ignores_jitter_and_caps_at_5min() {
        let now = now_ms() / 1_000;
        let info = crate::state::RateLimitInfo {
            status_5h: Some("exhausted".into()),
            reset_5h: Some(now + 10_000), // far future
            ..Default::default()
        };
        let ms = compute_429_cooldown_ms(Some(&info), None, true);
        assert_eq!(ms, 5 * 60_000, "exhausted cooldown caps at 5 min");
    }

    #[test]
    fn overage_reset_none_when_overage_allowed() {
        let now = now_ms() / 1_000;
        let info = crate::state::RateLimitInfo {
            status_5h: Some("exhausted".into()),
            reset_5h: Some(now + 3600),
            overage_status: Some("allowed".into()),
            ..Default::default()
        };
        // Overage allowed → account can overflow → no buy-more, no cool.
        assert_eq!(overage_exhausted_reset(&info), None);
    }

    #[test]
    fn overage_reset_none_when_not_exhausted() {
        let now = now_ms() / 1_000;
        let info = crate::state::RateLimitInfo {
            status_5h: Some("allowed".into()),
            reset_5h: Some(now + 3600),
            status_7d: Some("allowed".into()),
            reset_7d: Some(now + 7200),
            overage_status: Some("rejected".into()),
            ..Default::default()
        };
        assert_eq!(overage_exhausted_reset(&info), None);
    }

    #[test]
    fn overage_reset_returns_binding_reset_when_exhausted_and_rejected() {
        let now = now_ms() / 1_000;
        let info = crate::state::RateLimitInfo {
            status_5h: Some("exhausted".into()),
            reset_5h: Some(now + 1800),
            status_7d: Some("exhausted".into()),
            reset_7d: Some(now + 9999),
            overage_status: Some("rejected".into()),
            ..Default::default()
        };
        // Both exhausted → return the soonest reset (5h here).
        assert_eq!(overage_exhausted_reset(&info), Some(now + 1800));
    }

    #[test]
    fn overage_reset_ignores_past_reset() {
        let now = now_ms() / 1_000;
        let info = crate::state::RateLimitInfo {
            status_5h: Some("exhausted".into()),
            reset_5h: Some(now.saturating_sub(100)), // already reset
            overage_status: Some("rejected".into()),
            ..Default::default()
        };
        assert_eq!(overage_exhausted_reset(&info), None);
    }

    #[test]
    fn strip_beta_header_removes_header_when_all_flags_stripped() {
        let mut h = axum::http::HeaderMap::new();
        h.insert("anthropic-beta", "interleaved-thinking-2025-05-14".parse().unwrap());
        strip_beta_header_for_simple_model(&mut h);
        assert!(h.get("anthropic-beta").is_none(), "header removed when nothing left");
    }

    #[test]
    fn strip_long_context_beta_removes_flag_preserves_others() {
        let mut h = axum::http::HeaderMap::new();
        h.insert("anthropic-beta", "context-1m-2025-08-07,oauth-2025-04-20,claude-code-20250219".parse().unwrap());
        assert!(strip_long_context_beta(&mut h), "1M beta present → removed");
        let v = h.get("anthropic-beta").unwrap().to_str().unwrap();
        assert!(!v.contains("context-1m-2025-08-07"), "1M beta removed");
        assert!(v.contains("oauth-2025-04-20"), "oauth preserved");
        assert!(v.contains("claude-code-20250219"), "claude-code beta preserved");
    }

    #[test]
    fn strip_long_context_beta_removes_header_when_only_flag() {
        let mut h = axum::http::HeaderMap::new();
        h.insert("anthropic-beta", "context-1m-2025-08-07".parse().unwrap());
        assert!(strip_long_context_beta(&mut h));
        assert!(h.get("anthropic-beta").is_none(), "header removed when 1M was the only flag");
    }

    #[test]
    fn strip_long_context_beta_noop_when_absent() {
        let mut h = axum::http::HeaderMap::new();
        h.insert("anthropic-beta", "oauth-2025-04-20".parse().unwrap());
        assert!(!strip_long_context_beta(&mut h), "no 1M beta → no change");
        assert_eq!(h.get("anthropic-beta").unwrap().to_str().unwrap(), "oauth-2025-04-20");
    }

    #[test]
    fn normalize_model_suffix_strips_1m() {
        let mut v = json!({ "model": "claude-opus-4-8[1m]" });
        assert!(normalize_model_suffix(&mut v));
        assert_eq!(v["model"], json!("claude-opus-4-8"));
        // Uppercase variant too.
        let mut v2 = json!({ "model": "claude-sonnet-4-6[1M]" });
        assert!(normalize_model_suffix(&mut v2));
        assert_eq!(v2["model"], json!("claude-sonnet-4-6"));
        // No suffix → no change.
        let mut v3 = json!({ "model": "claude-opus-4-8" });
        assert!(!normalize_model_suffix(&mut v3));
        assert_eq!(v3["model"], json!("claude-opus-4-8"));
    }

    #[test]
    fn simple_model_rewrite_drops_both_effort_and_1m_beta() {
        // Simulates a classifier pin / fallback downgrade to Haiku of an opus[1m]
        // request that carried effort + the 1M beta (the /compact + long-context 400 scenario).
        let mut v = json!({
            "model": "claude-haiku-4-5-20251001",
            "effort": "high",
            "messages": [{"role": "user", "content": "summarize"}]
        });
        assert!(strip_unsupported_params_for_simple_model(&mut v));
        assert!(v.get("effort").is_none(), "effort dropped for haiku");

        let mut h = axum::http::HeaderMap::new();
        h.insert("anthropic-beta", "context-1m-2025-08-07,interleaved-thinking-2025-05-14,oauth-2025-04-20".parse().unwrap());
        strip_beta_header_for_simple_model(&mut h);
        strip_long_context_beta(&mut h);
        let v = h.get("anthropic-beta").unwrap().to_str().unwrap();
        assert!(!v.contains("context-1m-2025-08-07"), "1M beta dropped");
        assert!(!v.contains("thinking"), "thinking beta dropped");
        assert!(v.contains("oauth-2025-04-20"), "oauth preserved for subscription recognition");
    }
}

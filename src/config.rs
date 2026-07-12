use anyhow::{bail, Context, Result};

/// Validate that an upstream URL uses http/https and does not point to
/// loopback or link-local addresses (SSRF guard).
/// Pass `allow_loopback = true` for Local-provider accounts (e.g. Ollama).
fn validate_upstream_url(url: &str, allow_loopback: bool) -> Result<()> {
    let parsed = url::Url::parse(url).with_context(|| format!("Invalid upstream URL: {url}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        s => bail!("Upstream URL must use http or https, got scheme '{s}': {url}"),
    }
    if !allow_loopback {
        if let Some(host) = parsed.host_str() {
            let blocked = matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
                || host.starts_with("169.254.")
                || host.starts_with("fd");
            if blocked {
                bail!("Upstream URL must not point to loopback or link-local addresses: {url}");
            }
        }
    }
    Ok(())
}
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use crate::credential::{deserialize_credential_map, Credential};
use crate::provider::Provider;

pub const APP_NAME: &str = "shunt";

pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("config.toml")
}

pub fn credentials_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("credentials.json")
}

pub fn state_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("state.json")
}

pub fn log_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("proxy.log")
}

pub fn notify_log_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("notify.log")
}

pub fn install_id_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("install_id")
}

/// Stable bearer token used by managed local clients. It is derived from the
/// per-install identifier so the underlying subscription credential is never
/// written into client configuration.
pub fn local_client_token(pool: &str) -> Result<String> {
    use sha2::Digest;

    let path = install_id_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let id = match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => id,
        _ => {
            let id = uuid::Uuid::new_v4().to_string();
            std::fs::write(&path, &id)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            }
            id
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    let digest = sha2::Sha256::digest(format!("shunt-client:{pool}:{}", id.trim()).as_bytes());
    Ok(format!("shunt_{}", hex::encode(digest)))
}

pub fn pid_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(APP_NAME)
        .join("shunt.pid")
}

// ---------------------------------------------------------------------------
// Credentials store  (separate file from config — never commit this)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CredentialsStore {
    #[serde(deserialize_with = "deserialize_credential_map", default)]
    pub accounts: HashMap<String, Credential>,
    /// Schema-v2 credentials are scoped by native pool so equal display names
    /// cannot collapse two independent subscription identities.
    #[serde(default)]
    pub pools: HashMap<String, HashMap<String, Credential>>,
}

impl CredentialsStore {
    pub fn load() -> Self {
        let p = credentials_path();
        if !p.exists() {
            return Self::default();
        }
        match std::fs::read_to_string(&p) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self) -> Result<()> {
        let p = credentials_path();
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = p.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(self)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(&tmp, &p)?;
        // On Windows, restrict the file to the current user via icacls (best-effort).
        #[cfg(windows)]
        {
            if let Some(path_str) = p.to_str() {
                let username = std::env::var("USERNAME").unwrap_or_default();
                if !username.is_empty() {
                    let _ = std::process::Command::new("icacls")
                        .arg(path_str)
                        .arg("/inheritance:r")
                        .arg("/grant:r")
                        .arg(format!("{username}:F"))
                        .status();
                }
            }
        }
        Ok(())
    }

    pub fn get(&self, pool: PoolKind, name: &str) -> Option<Credential> {
        if pool == PoolKind::Legacy {
            self.accounts.get(name).cloned()
        } else {
            self.pools
                .get(pool.as_str())
                .and_then(|m| m.get(name))
                .cloned()
        }
    }

    pub fn insert(&mut self, pool: PoolKind, name: String, credential: Credential) {
        if pool == PoolKind::Legacy {
            self.accounts.insert(name, credential);
        } else {
            self.pools
                .entry(pool.as_str().to_owned())
                .or_default()
                .insert(name, credential);
        }
    }

    pub fn insert_resolved(&mut self, resolved_name: String, credential: Credential) {
        if let Some((prefix, name)) = resolved_name.split_once('/') {
            let pool = match prefix {
                "claude" => Some(PoolKind::Claude),
                "codex" => Some(PoolKind::Codex),
                _ => None,
            };
            if let Some(pool) = pool {
                self.insert(pool, name.to_owned(), credential);
                return;
            }
        }
        self.insert(PoolKind::Legacy, resolved_name, credential);
    }

    pub fn get_mut_resolved(&mut self, resolved_name: &str) -> Option<&mut Credential> {
        if let Some((pool, name)) = resolved_name.split_once('/') {
            if matches!(pool, "claude" | "codex") {
                return self.pools.get_mut(pool).and_then(|m| m.get_mut(name));
            }
        }
        self.accounts.get_mut(resolved_name)
    }

    pub fn resolved_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.accounts.keys().cloned().collect();
        for (pool, accounts) in &self.pools {
            names.extend(accounts.keys().map(|name| format!("{pool}/{name}")));
        }
        names.sort();
        names
    }

    pub fn remove_resolved(&mut self, resolved_name: &str) -> bool {
        if let Some((pool, name)) = resolved_name.split_once('/') {
            if matches!(pool, "claude" | "codex") {
                return self
                    .pools
                    .get_mut(pool)
                    .and_then(|accounts| accounts.remove(name))
                    .is_some();
            }
        }
        self.accounts.remove(resolved_name).is_some()
    }
}

// ---------------------------------------------------------------------------
// Raw TOML config types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct RawConfig {
    #[serde(default)]
    schema_version: Option<u32>,
    #[serde(default)]
    server: RawServer,
    #[serde(default)]
    accounts: Vec<RawAccount>,
    /// Global model-name mapping: `"claude-sonnet-4-6" = "llama-3.3-70b-versatile"`
    /// Applied when routing Anthropic-format requests to non-Anthropic providers.
    #[serde(default)]
    model_mapping: HashMap<String, String>,
    #[serde(default)]
    api_overflow: Option<RawApiOverflow>,
    #[serde(default)]
    secrets: RawSecrets,
    #[serde(default)]
    pools: RawPools,
    #[serde(default)]
    classifier: RawClassifier,
    #[serde(default)]
    bridge: RawBridge,
    #[serde(default)]
    website: RawWebsite,
    #[serde(default)]
    manual_swarm: RawManualSwarm,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawWebsite {
    base_url: Option<String>,
    cache_max_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawManualSwarm {
    #[serde(default)]
    enabled: Option<bool>,
    control_url: Option<String>,
    default_target: Option<String>,
    #[serde(default)]
    allowed_targets: Vec<String>,
    #[serde(default)]
    default_agents: Option<usize>,
    #[serde(default)]
    max_agents: Option<usize>,
    #[serde(default)]
    default_duration_secs: Option<u64>,
    #[serde(default)]
    max_duration_secs: Option<u64>,
    #[serde(default)]
    request_timeout_secs: Option<u64>,
    apply_policy: Option<String>,
    network_ceiling: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawServer {
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_control_port")]
    control_port: u16,
    #[serde(default = "default_log_level")]
    log_level: String,
    upstream_url: Option<String>,
    remote_key: Option<String>,
    relay_url: Option<String>,
    pub custom_domain: Option<String>,
    /// Conversation stickiness TTL in minutes (default: 10)
    sticky_ttl_minutes: Option<u64>,
    /// "use-it-or-lose-it" expiry window in minutes (default: 30)
    expiry_soon_minutes: Option<u64>,
    /// Account selection strategy: "earliest-expiry" (default), "round-robin", "least-utilized"
    routing_strategy: Option<String>,
    /// Upstream request timeout in seconds (default: 600)
    request_timeout_secs: Option<u64>,
    /// Per-IP rate limit in requests per minute (0 = disabled, default disabled).
    rate_limit_rpm: Option<u32>,
    /// Trust X-Real-IP / X-Forwarded-For headers for per-IP rate limiting.
    /// Set to true only when shunt sits behind a trusted reverse proxy (e.g. cloudflared).
    /// When false (default), all requests share one rate-limit bucket.
    trust_proxy_headers: Option<bool>,
    /// Enable periodic health-check probes for all accounts (default: true).
    health_check_enabled: Option<bool>,
    /// Seconds between health-check probe rounds (default: 300 = 5 min).
    health_check_interval_secs: Option<u64>,
    /// Per-account probe timeout in seconds (default: 10).
    health_check_timeout_secs: Option<u64>,
    /// URL of a shunt relay-server instance for multi-machine history aggregation.
    /// e.g. "http://relay.internal:3001"
    telemetry_url: Option<String>,
    /// Bearer token sent to the relay-server. Must match RELAY_TOKEN on the server.
    telemetry_token: Option<String>,
    /// Human-readable name for this shunt instance (shown in the relay dashboard).
    /// Defaults to the system hostname.
    instance_name: Option<String>,
    /// Per-account burst rate limit in requests per minute (0 = disabled, default disabled).
    /// When set, accounts approaching this limit are deprioritized in routing.
    burst_rpm_limit: Option<u32>,
    /// Fallback model to use when all accounts are on cooldown.
    /// If set, requests are retried with this model before waiting.
    fallback_model: Option<String>,
    /// Send anonymous usage telemetry to Supabase (default: true).
    /// Also disabled by SHUNT_NO_TELEMETRY=1 env var.
    telemetry: Option<bool>,
    /// Name of the account reserved for auto-mode safety-classifier side-calls.
    /// When set, classifier requests are routed only to this account (and it is
    /// excluded from normal rotation). Intended for an `anthropic-api` account.
    classifier_account: Option<String>,
    /// Path to a custom system prompt that fully replaces Claude Code's built-in
    /// auto-mode safety-classifier prompt. When set, the `system` field of a
    /// detected classifier request is replaced with this file's contents. The
    /// replacement must still instruct the model to emit the `<block>yes|no</block>`
    /// verdict grammar Claude Code parses, or verdicts fail closed (block).
    classifier_system_prompt_path: Option<String>,
    /// Account to try once if the primary classifier lane errors before failing
    /// the request. Lets a transient upstream blip fall back (e.g. to a pooled or
    /// Console lane) instead of forcing Claude Code to block. When unset, the
    /// classifier lane fails fast and Claude Code fails closed.
    classifier_fallback_account: Option<String>,
    /// Max time (ms) a request may spend waiting in the all-cooling loop before
    /// shunt spills to the API overflow lane or returns 429+Retry-After. Bounds
    /// the "long startup then error" stall. Default 8000.
    max_startup_wait_ms: Option<u64>,
}

impl Default for RawServer {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            control_port: default_control_port(),
            log_level: default_log_level(),
            upstream_url: None,
            remote_key: None,
            relay_url: None,
            custom_domain: None,
            sticky_ttl_minutes: None,
            expiry_soon_minutes: None,
            routing_strategy: None,
            request_timeout_secs: None,
            rate_limit_rpm: None,
            trust_proxy_headers: None,
            health_check_enabled: None,
            health_check_interval_secs: None,
            health_check_timeout_secs: None,
            telemetry_url: None,
            telemetry_token: None,
            instance_name: None,
            burst_rpm_limit: None,
            fallback_model: None,
            telemetry: None,
            classifier_account: None,
            classifier_system_prompt_path: None,
            classifier_fallback_account: None,
            max_startup_wait_ms: None,
        }
    }
}

/// `[api_overflow]` config section: a budget-capped pay-per-token Anthropic API
/// lane used for warm-start (fast first prompts) and overflow (when the
/// subscription pool is saturated), never as the steady-state default.
#[derive(Debug, Clone, Deserialize, Default)]
struct RawApiOverflow {
    /// Master enable. Default false (opt-in).
    #[serde(default)]
    enabled: Option<bool>,
    /// Name of the anthropic-api account acting as the overflow lane.
    #[serde(default)]
    account: Option<String>,
    /// Daily USD budget cap for the lane. Default 500.
    #[serde(default)]
    daily_budget_usd: Option<f64>,
    /// Warm-start: serve a session's first N requests on the API lane. Default 3.
    #[serde(default)]
    warmup_requests: Option<u64>,
    /// Warm-start also applies while a session is younger than this (ms). Default 20000.
    #[serde(default)]
    warmup_ms: Option<u64>,
    /// Environment variable containing the API key. The value is never persisted.
    #[serde(default)]
    key_env: Option<String>,
    /// Conservative output-token cap used by the reservation gate.
    #[serde(default)]
    max_output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawSecrets {
    env_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawPools {
    claude: Option<RawPool>,
    codex: Option<RawPool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawPool {
    port: Option<u16>,
    routing_strategy: Option<String>,
    #[serde(default)]
    accounts: Vec<RawAccount>,
    #[serde(default)]
    overflow: Option<RawApiOverflow>,
    #[serde(default)]
    fallback_models: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawClassifier {
    #[serde(default)]
    enabled: Option<bool>,
    upstream_url: Option<String>,
    model: Option<String>,
    account: Option<String>,
    fallback_account: Option<String>,
    system_prompt_path: Option<PathBuf>,
    #[serde(default)]
    fail_closed: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawBridge {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    concurrency_per_provider: Option<usize>,
    #[serde(default)]
    queue_capacity: Option<usize>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    max_depth: Option<u8>,
    #[serde(default)]
    retention_hours: Option<u64>,
    #[serde(default)]
    network_ceiling: Option<String>,
    #[serde(default)]
    codex_fallback_models: Vec<String>,
    #[serde(default)]
    claude_fallback_models: Vec<String>,
    #[serde(default)]
    required_checks: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ApiOverflowConfig {
    pub enabled: bool,
    pub account: Option<String>,
    pub daily_budget_usd: f64,
    pub warmup_requests: u64,
    pub warmup_ms: u64,
    pub key_env: Option<String>,
    pub max_output_tokens: u64,
}

impl Default for ApiOverflowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            account: None,
            daily_budget_usd: 500.0,
            warmup_requests: 3,
            warmup_ms: 20_000,
            key_env: None,
            max_output_tokens: 32_768,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawAccount {
    name: String,
    #[serde(default = "default_plan_type")]
    plan_type: String,
    /// "anthropic" (default) | "openai" / "codex" | "groq" | "mistral" | "local" | …
    #[serde(default)]
    provider: Option<String>,
    /// Inline API key (use api_key_env for better security).
    #[serde(default)]
    api_key: Option<String>,
    /// Name of an environment variable that holds the API key.
    #[serde(default)]
    api_key_env: Option<String>,
    /// Per-account upstream URL override (required for Local provider).
    #[serde(default)]
    upstream_url: Option<String>,
    /// Pin this account to a specific model, overriding global model_mapping
    /// and the provider's default_model(). Useful for mixing model tiers.
    #[serde(default)]
    model: Option<String>,
    /// Optional descriptive ownership/source metadata retained for migration.
    #[serde(default)]
    #[serde(rename = "owner")]
    _owner: Option<String>,
    #[serde(default)]
    credential_source: Option<String>,
    /// Opaque identifier in the selected credential source. This is metadata,
    /// never the credential value itself.
    #[serde(default)]
    credential_id: Option<String>,
}

fn default_host() -> String {
    "127.0.0.1".into()
}

pub fn default_instance_name() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "shunt".into())
}
fn default_port() -> u16 {
    8082
}
fn default_control_port() -> u16 {
    19081
}
fn default_log_level() -> String {
    "info".into()
}
fn default_plan_type() -> String {
    "pro".into()
}

// ---------------------------------------------------------------------------
// Resolved config types
// ---------------------------------------------------------------------------

/// Account-selection algorithm used when no sticky or pinned account applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutingStrategy {
    /// Harvest every token before the window expires — use-it-or-lose-it.
    /// Drains accounts whose quota windows expire soonest first, then prefers
    /// the account with the most remaining quota. Maximises total token usage over time.
    /// Config: `"reaper"`
    Reaper,
    /// Spins through accounts in a fixed round-robin cycle, ignoring quota state.
    /// Config: `"carousel"`
    Carousel,
    /// Always routes to the account with the softest landing — the most remaining
    /// capacity across both 5h and 7d windows (binding window primary, secondary as tiebreak).
    /// Config: `"cushion"`
    Cushion,
    /// Time-weighted dual-window optimizer. Scores each account as:
    ///   health_5h = 1 - (time_fraction_5h × util_5h)
    ///   health_7d = 1 - (time_fraction_7d × util_7d)
    ///   score     = health_5h × health_7d
    /// where time_fraction = secs_to_reset / window_duration (0 = resetting now, 1 = just started).
    /// Accounts for how much quota remains AND how soon each window refreshes.
    /// Config: `"maximus"`
    #[default]
    Maximus,
}

impl RoutingStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reaper => "reaper",
            Self::Carousel => "carousel",
            Self::Cushion => "cushion",
            Self::Maximus => "maximus",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "reaper" | "earliest-expiry" | "earliest_expiry" => Some(Self::Reaper),
            "carousel" | "round-robin" | "round_robin" => Some(Self::Carousel),
            "cushion" | "most-available" | "most_available" => Some(Self::Cushion),
            "maximus" => Some(Self::Maximus),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Port for the control plane (/status, /use, /health) — sees all accounts.
    pub control_port: u16,
    pub log_level: String,
    pub upstream_url: String,
    /// When set, remote requests must supply this value as `x-api-key`.
    pub remote_key: Option<String>,
    /// Relay URL for `shunt push` / `shunt login`. Overridable via SHUNT_RELAY_URL.
    pub relay_url: String,
    /// Custom domain for permanent online sharing (e.g. https://shunt.mysite.com).
    pub custom_domain: Option<String>,
    /// Conversation stickiness TTL in milliseconds.
    pub sticky_ttl_ms: u64,
    /// Accounts whose 5h window resets within this many seconds are preferred ("use-it-or-lose-it").
    pub expiry_soon_secs: u64,
    /// Which routing algorithm to use for account selection.
    pub routing_strategy: RoutingStrategy,
    /// Upstream request timeout in seconds.
    pub request_timeout_secs: u64,
    /// Per-IP rate limit in requests per minute (0 = disabled, default disabled).
    pub rate_limit_rpm: u32,
    /// Trust X-Real-IP for per-IP rate limiting (only when behind a trusted proxy).
    pub trust_proxy_headers: bool,
    /// Enable periodic health-check probes for all accounts.
    pub health_check_enabled: bool,
    /// Seconds between health-check probe rounds.
    pub health_check_interval_secs: u64,
    /// Per-account probe timeout in seconds.
    pub health_check_timeout_secs: u64,
    /// Optional relay-server URL for cross-instance history aggregation.
    pub telemetry_url: Option<String>,
    /// Bearer token for the relay-server.
    pub telemetry_token: Option<String>,
    /// Identifier for this shunt instance sent in telemetry payloads.
    pub instance_name: String,
    /// Per-account burst rate limit in requests per minute (0 = disabled).
    pub burst_rpm_limit: u32,
    /// Fallback model when all accounts are on cooldown.
    pub fallback_model: Option<String>,
    /// Send anonymous usage telemetry to Supabase (default: true).
    pub telemetry: bool,
    /// Account reserved for auto-mode safety-classifier side-calls (see RawServer).
    pub classifier_account: Option<String>,
    /// Path to a custom system prompt that replaces Claude Code's built-in
    /// auto-mode classifier prompt (see RawServer).
    pub classifier_system_prompt_path: Option<String>,
    /// Account to try once if the primary classifier lane errors (see RawServer).
    pub classifier_fallback_account: Option<String>,
    /// Max ms a request waits in the all-cooling loop before spill/backpressure.
    pub max_startup_wait_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 8082,
            control_port: 19081,
            log_level: "info".into(),
            upstream_url: "https://api.anthropic.com".into(),
            remote_key: None,
            relay_url: "https://relay.ramcharan.shop".into(),
            custom_domain: None,
            sticky_ttl_ms: 10 * 60 * 1000,
            expiry_soon_secs: 30 * 60,
            routing_strategy: RoutingStrategy::Maximus,
            request_timeout_secs: 600,
            rate_limit_rpm: 0,
            trust_proxy_headers: false,
            health_check_enabled: true,
            health_check_interval_secs: 300,
            health_check_timeout_secs: 10,
            telemetry_url: None,
            telemetry_token: None,
            instance_name: default_instance_name(),
            burst_rpm_limit: 10,
            fallback_model: None,
            telemetry: true,
            classifier_account: None,
            classifier_system_prompt_path: None,
            classifier_fallback_account: None,
            max_startup_wait_ms: 8_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AccountConfig {
    pub name: String,
    pub plan_type: String,
    pub provider: Provider,
    /// `None` when the account has no credential.
    /// OAuth accounts: None means reauth required (shown as auth_failed).
    /// ApiKey accounts: None means key not yet configured.
    /// Local accounts: None is normal (no auth required).
    pub credential: Option<Credential>,
    /// Override the upstream base URL for this account.
    /// `None` means use `config.server.upstream_url` (primary provider) or
    /// `provider.default_upstream_url()` (non-primary provider).
    pub upstream_url: Option<String>,
    /// Pin this account to a specific model name.
    /// Overrides both `model_mapping` and `provider.default_model()`.
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentInfo {
    pub name: String,
    pub provider: String,
    pub credential_source: String,
    pub credential_id: String,
}

/// Read attachment metadata without resolving any secret material. This also
/// works when every account has been detached, unlike the runtime loader.
pub fn attachment_inventory(path: &Path) -> Result<Vec<AttachmentInfo>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config: {}", path.display()))?;
    let raw: RawConfig = toml::from_str(&text)
        .with_context(|| format!("Failed to parse config: {}", path.display()))?;
    let schema = raw.schema_version.unwrap_or(1);
    let mut specs: Vec<(PoolKind, RawAccount)> = Vec::new();
    if schema >= NATIVE_POOLS_SCHEMA_VERSION {
        if let Some(pool) = raw.pools.claude {
            specs.extend(pool.accounts.into_iter().map(|a| (PoolKind::Claude, a)));
        }
        if let Some(pool) = raw.pools.codex {
            specs.extend(pool.accounts.into_iter().map(|a| (PoolKind::Codex, a)));
        }
    }
    specs.extend(raw.accounts.into_iter().map(|a| (PoolKind::Legacy, a)));
    specs
        .into_iter()
        .map(|(pool, account)| {
            let source = CredentialSourceKind::parse(account.credential_source.as_deref())?;
            let name = scoped_account_name(pool, &account.name, schema);
            Ok(AttachmentInfo {
                credential_id: account.credential_id.unwrap_or_else(|| name.clone()),
                provider: account.provider.unwrap_or_else(|| match pool {
                    PoolKind::Codex => "openai".into(),
                    _ => "anthropic".into(),
                }),
                credential_source: source.as_str().into(),
                name,
            })
        })
        .collect()
}

/// Schema v2 introduced native Claude and Codex pools. Keep this boundary
/// separate from the latest schema so v2 configs continue to load safely.
pub const NATIVE_POOLS_SCHEMA_VERSION: u32 = 2;
/// Schema v3 makes account blocks explicit routing attachments to credential
/// references. Removing an attachment must not delete its source credential.
pub const CONFIG_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialSourceKind {
    LocalStore,
    ProviderCli,
    EnvFile,
    WebsiteBroker,
    None,
}

impl CredentialSourceKind {
    pub fn parse(value: Option<&str>) -> Result<Self> {
        match value.unwrap_or("local-store") {
            "local-store" | "shunt_store" => Ok(Self::LocalStore),
            "provider-cli" | "codex_auth_file" | "claude_credentials_file" | "local_cli" => {
                Ok(Self::ProviderCli)
            }
            "env-file" => Ok(Self::EnvFile),
            "website-broker" => Ok(Self::WebsiteBroker),
            "none" => Ok(Self::None),
            other => bail!("Unsupported credential_source '{other}'"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalStore => "local-store",
            Self::ProviderCli => "provider-cli",
            Self::EnvFile => "env-file",
            Self::WebsiteBroker => "website-broker",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolKind {
    Claude,
    Codex,
    Legacy,
}

impl PoolKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Legacy => "legacy",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub port: u16,
    pub routing_strategy: RoutingStrategy,
    pub overflow: ApiOverflowConfig,
    pub fallback_models: Vec<String>,
}

impl PoolConfig {
    fn claude_default() -> Self {
        Self {
            port: 8082,
            routing_strategy: RoutingStrategy::Maximus,
            overflow: ApiOverflowConfig::default(),
            fallback_models: Vec::new(),
        }
    }

    fn codex_default() -> Self {
        Self {
            port: 8083,
            routing_strategy: RoutingStrategy::Maximus,
            overflow: ApiOverflowConfig::default(),
            fallback_models: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolsConfig {
    pub claude: PoolConfig,
    pub codex: PoolConfig,
}

impl Default for PoolsConfig {
    fn default() -> Self {
        Self {
            claude: PoolConfig::claude_default(),
            codex: PoolConfig::codex_default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SecretsConfig {
    pub env_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ClassifierConfig {
    pub enabled: bool,
    pub upstream_url: Option<String>,
    pub model: Option<String>,
    pub fail_closed: bool,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            upstream_url: None,
            model: None,
            fail_closed: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkPolicy {
    None,
    Allowlisted,
    Unrestricted,
}

impl NetworkPolicy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "allowlisted" | "allow-list" => Some(Self::Allowlisted),
            "unrestricted" => Some(Self::Unrestricted),
            _ => None,
        }
    }

    pub fn permits(self, requested: Self) -> bool {
        let rank = |v| match v {
            Self::None => 0,
            Self::Allowlisted => 1,
            Self::Unrestricted => 2,
        };
        rank(requested) <= rank(self)
    }
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub enabled: bool,
    pub concurrency_per_provider: usize,
    pub queue_capacity: usize,
    pub timeout_secs: u64,
    pub max_depth: u8,
    pub retention_hours: u64,
    pub network_ceiling: NetworkPolicy,
    pub codex_fallback_models: Vec<String>,
    pub claude_fallback_models: Vec<String>,
    pub required_checks: Vec<String>,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            concurrency_per_provider: 2,
            queue_capacity: 32,
            timeout_secs: 1_800,
            max_depth: 1,
            retention_hours: 24,
            network_ceiling: NetworkPolicy::Allowlisted,
            codex_fallback_models: Vec::new(),
            claude_fallback_models: Vec::new(),
            required_checks: Vec::new(),
        }
    }
}

pub const MANUAL_SWARM_CAPABILITY_VERSION: &str = "manual-swarm/v1";
pub const MANUAL_SWARM_MAX_AGENTS: usize = 32;
// Website3's signed Manual Swarm grant is intentionally capped at one hour.
// Keep the local configuration bound identical so a plan can never be
// accepted locally and rejected later by the authorization plane.
pub const MANUAL_SWARM_MAX_DURATION_SECS: u64 = 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualSwarmApplyPolicy {
    Explicit,
    Disabled,
}

#[derive(Debug, Clone)]
pub struct ManualSwarmConfig {
    pub enabled: bool,
    /// Website3 public API route. Fabric and Auto Swarm are never addressed directly.
    pub control_url: String,
    pub default_target: String,
    pub allowed_targets: BTreeSet<String>,
    pub default_agents: usize,
    pub max_agents: usize,
    pub default_duration_secs: u64,
    pub max_duration_secs: u64,
    pub request_timeout_secs: u64,
    pub apply_policy: ManualSwarmApplyPolicy,
    pub network_ceiling: NetworkPolicy,
}

impl Default for ManualSwarmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            control_url: format!(
                "{}/api/shunt/manual-swarms",
                crate::website::DEFAULT_WEBSITE_URL.trim_end_matches('/')
            ),
            default_target: "auto".into(),
            allowed_targets: ["auto", "local", "build-fra1", "hetzner-backup-substrate"]
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            default_agents: 4,
            max_agents: 8,
            default_duration_secs: 2_700,
            max_duration_secs: MANUAL_SWARM_MAX_DURATION_SECS,
            request_timeout_secs: 30,
            apply_policy: ManualSwarmApplyPolicy::Explicit,
            // The feature itself is disabled by default. Once explicitly
            // enabled, local go_native currently proves unrestricted network
            // only; lower policies fail closed server-side until enforced.
            network_ceiling: NetworkPolicy::Unrestricted,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub schema_version: u32,
    pub server: ServerConfig,
    pub accounts: Vec<AccountConfig>,
    pub config_file: PathBuf,
    /// Global model-name overrides: claude model → provider model.
    /// e.g. `"claude-sonnet-4-6" → "llama-3.3-70b-versatile"`
    pub model_mapping: HashMap<String, String>,
    /// Budget-capped API overflow lane config.
    pub api_overflow: ApiOverflowConfig,
    /// Native client pools. Legacy flat configs are projected into these defaults.
    pub pools: PoolsConfig,
    pub secrets: SecretsConfig,
    pub classifier: ClassifierConfig,
    pub bridge: BridgeConfig,
    pub manual_swarm: ManualSwarmConfig,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

fn load_env_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.is_absolute() {
        bail!(
            "secrets.env_file must be an absolute path: {}",
            path.display()
        );
    }
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("Failed to stat secrets.env_file: {}", path.display()))?;
    if !metadata.is_file() {
        bail!("secrets.env_file is not a regular file: {}", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "secrets.env_file must not be accessible by group/others (chmod 600): {}",
                path.display()
            );
        }
    }

    let text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read secrets.env_file: {}", path.display()))?;
    let mut values = HashMap::new();
    for (line_no, raw_line) in text.lines().enumerate() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim_start();
        }
        let Some((key, raw_value)) = line.split_once('=') else {
            bail!(
                "Invalid env assignment at {}:{}",
                path.display(),
                line_no + 1
            );
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .bytes()
                .enumerate()
                .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
        {
            bail!(
                "Invalid environment variable name at {}:{}",
                path.display(),
                line_no + 1
            );
        }
        let raw_value = raw_value.trim();
        let value = if raw_value.len() >= 2
            && ((raw_value.starts_with('"') && raw_value.ends_with('"'))
                || (raw_value.starts_with('\'') && raw_value.ends_with('\'')))
        {
            raw_value[1..raw_value.len() - 1].to_owned()
        } else {
            raw_value.to_owned()
        };
        values.insert(key.to_owned(), value);
    }
    Ok(values)
}

fn selected_secret(values: &HashMap<String, String>, key: &str) -> Option<String> {
    // Publishing credentials are never valid Shunt runtime credentials. This
    // guard prevents an overly broad env file from accidentally attaching one.
    if matches!(key, "NPMJS" | "NPM_TOKEN" | "NODE_AUTH_TOKEN") {
        return None;
    }
    std::env::var(key).ok().or_else(|| values.get(key).cloned())
}

fn resolve_overflow(raw: Option<RawApiOverflow>, default_key_env: &str) -> ApiOverflowConfig {
    let raw = raw.unwrap_or_default();
    ApiOverflowConfig {
        enabled: raw.enabled.unwrap_or(false),
        account: raw.account,
        daily_budget_usd: raw.daily_budget_usd.unwrap_or(500.0),
        warmup_requests: raw.warmup_requests.unwrap_or(3),
        warmup_ms: raw.warmup_ms.unwrap_or(20_000),
        key_env: raw.key_env.or_else(|| Some(default_key_env.to_owned())),
        max_output_tokens: raw.max_output_tokens.unwrap_or(32_768),
    }
}

fn resolve_pool(raw: Option<RawPool>, kind: PoolKind) -> PoolConfig {
    let defaults = match kind {
        PoolKind::Claude => PoolConfig::claude_default(),
        PoolKind::Codex => PoolConfig::codex_default(),
        PoolKind::Legacy => PoolConfig::claude_default(),
    };
    let Some(raw) = raw else { return defaults };
    let key_env = match kind {
        PoolKind::Claude => "ANTHROPIC_API_KEY",
        _ => "OPENAI_API_KEY",
    };
    PoolConfig {
        port: raw.port.unwrap_or(defaults.port),
        routing_strategy: raw
            .routing_strategy
            .as_deref()
            .and_then(RoutingStrategy::from_str)
            .unwrap_or(defaults.routing_strategy),
        overflow: resolve_overflow(raw.overflow, key_env),
        fallback_models: raw.fallback_models,
    }
}

fn valid_manual_swarm_target(target: &str) -> bool {
    !target.is_empty()
        && target.len() <= 64
        && target
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn normalize_manual_swarm_control_url(value: &str) -> Result<String> {
    let parsed = url::Url::parse(value).context("manual_swarm.control_url is not a valid URL")?;
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("manual_swarm.control_url must not contain credentials, query, or fragment");
    }
    let host = parsed
        .host_str()
        .context("manual_swarm.control_url requires a host")?;
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]");
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback) {
        bail!(
            "manual_swarm.control_url must use HTTPS (HTTP is allowed only for loopback testing)"
        );
    }
    if !parsed.path().ends_with("/api/shunt/manual-swarms") {
        bail!("manual_swarm.control_url must end with /api/shunt/manual-swarms");
    }
    Ok(value.trim_end_matches('/').to_owned())
}

fn resolve_manual_swarm(raw: RawManualSwarm, website_base_url: &str) -> Result<ManualSwarmConfig> {
    let defaults = ManualSwarmConfig::default();
    let default_control_url = format!(
        "{}/api/shunt/manual-swarms",
        website_base_url.trim_end_matches('/')
    );
    let control_url = normalize_manual_swarm_control_url(
        raw.control_url.as_deref().unwrap_or(&default_control_url),
    )?;
    let max_agents = raw.max_agents.unwrap_or(defaults.max_agents);
    if !(1..=MANUAL_SWARM_MAX_AGENTS).contains(&max_agents) {
        bail!("manual_swarm.max_agents must be between 1 and {MANUAL_SWARM_MAX_AGENTS}");
    }
    let default_agents = raw.default_agents.unwrap_or(defaults.default_agents);
    if default_agents == 0 || default_agents > max_agents {
        bail!("manual_swarm.default_agents must be between 1 and manual_swarm.max_agents");
    }
    let max_duration_secs = raw.max_duration_secs.unwrap_or(defaults.max_duration_secs);
    if !(60..=MANUAL_SWARM_MAX_DURATION_SECS).contains(&max_duration_secs) {
        bail!("manual_swarm.max_duration_secs must be between 60 and {MANUAL_SWARM_MAX_DURATION_SECS}");
    }
    let default_duration_secs = raw
        .default_duration_secs
        .unwrap_or(defaults.default_duration_secs);
    if default_duration_secs < 60 || default_duration_secs > max_duration_secs {
        bail!("manual_swarm.default_duration_secs must be between 60 and manual_swarm.max_duration_secs");
    }
    let request_timeout_secs = raw
        .request_timeout_secs
        .unwrap_or(defaults.request_timeout_secs);
    if !(2..=120).contains(&request_timeout_secs) {
        bail!("manual_swarm.request_timeout_secs must be between 2 and 120");
    }
    let allowed_targets: BTreeSet<String> = if raw.allowed_targets.is_empty() {
        defaults.allowed_targets
    } else {
        raw.allowed_targets
            .into_iter()
            .map(|target| target.trim().to_owned())
            .collect()
    };
    if allowed_targets.is_empty()
        || allowed_targets
            .iter()
            .any(|target| !valid_manual_swarm_target(target))
    {
        bail!("manual_swarm.allowed_targets contains an invalid target identifier");
    }
    let default_target = raw.default_target.unwrap_or(defaults.default_target);
    if !allowed_targets.contains(&default_target) {
        bail!("manual_swarm.default_target must appear in manual_swarm.allowed_targets");
    }
    let apply_policy = match raw.apply_policy.as_deref().unwrap_or("explicit") {
        "explicit" => ManualSwarmApplyPolicy::Explicit,
        "disabled" => ManualSwarmApplyPolicy::Disabled,
        _ => bail!("manual_swarm.apply_policy must be 'explicit' or 'disabled'"),
    };
    let network_ceiling = match raw.network_ceiling.as_deref().unwrap_or("unrestricted") {
        "none" => NetworkPolicy::None,
        "restricted" | "allowlisted" => NetworkPolicy::Allowlisted,
        "unrestricted" => NetworkPolicy::Unrestricted,
        _ => bail!("manual_swarm.network_ceiling must be none, restricted, or unrestricted"),
    };
    Ok(ManualSwarmConfig {
        enabled: raw.enabled.unwrap_or(false),
        control_url,
        default_target,
        allowed_targets,
        default_agents,
        max_agents,
        default_duration_secs,
        max_duration_secs,
        request_timeout_secs,
        apply_policy,
        network_ceiling,
    })
}

fn scoped_account_name(kind: PoolKind, name: &str, schema_version: u32) -> String {
    if schema_version >= NATIVE_POOLS_SCHEMA_VERSION && kind != PoolKind::Legacy {
        format!("{}/{}", kind.as_str(), name)
    } else {
        name.to_owned()
    }
}

pub fn load_config(path: Option<&Path>) -> Result<Config> {
    let p = path.map(PathBuf::from).unwrap_or_else(config_path);

    if !p.exists() {
        bail!(
            "Config not found: {}\nRun `shunt setup` to get started.",
            p.display()
        );
    }

    let raw_text = std::fs::read_to_string(&p)
        .with_context(|| format!("Failed to read config: {}", p.display()))?;

    let raw: RawConfig = toml::from_str(&raw_text)
        .with_context(|| format!("Failed to parse config: {}", p.display()))?;

    let schema_version = raw.schema_version.unwrap_or(1);
    if schema_version > CONFIG_SCHEMA_VERSION {
        bail!("Config schema_version {schema_version} is newer than this shunt supports ({CONFIG_SCHEMA_VERSION})");
    }
    let has_native_pools = raw.pools.claude.is_some() || raw.pools.codex.is_some();
    if has_native_pools && schema_version < NATIVE_POOLS_SCHEMA_VERSION {
        bail!("[pools.*] requires schema_version >= {NATIVE_POOLS_SCHEMA_VERSION}");
    }

    let env_values = match raw.secrets.env_file.as_deref() {
        Some(env_file) => load_env_file(env_file)?,
        None => HashMap::new(),
    };
    let website_config = crate::website::BrokerConfig {
        base_url: raw
            .website
            .base_url
            .clone()
            .or_else(|| std::env::var("SHUNT_WEBSITE_URL").ok())
            .unwrap_or_else(|| crate::website::DEFAULT_WEBSITE_URL.into()),
        cache_max_secs: raw
            .website
            .cache_max_secs
            .unwrap_or(crate::website::MAX_GRACE_CACHE_SECS)
            .min(crate::website::MAX_GRACE_CACHE_SECS),
    };

    let claude_raw_pool = raw.pools.claude.clone();
    let codex_raw_pool = raw.pools.codex.clone();
    let mut pools = PoolsConfig {
        claude: resolve_pool(claude_raw_pool.clone(), PoolKind::Claude),
        codex: resolve_pool(codex_raw_pool.clone(), PoolKind::Codex),
    };

    let mut account_specs: Vec<(PoolKind, RawAccount)> = Vec::new();
    if schema_version >= NATIVE_POOLS_SCHEMA_VERSION {
        if let Some(pool) = claude_raw_pool {
            account_specs.extend(pool.accounts.into_iter().map(|a| (PoolKind::Claude, a)));
        }
        if let Some(pool) = codex_raw_pool {
            account_specs.extend(pool.accounts.into_iter().map(|a| (PoolKind::Codex, a)));
        }
        account_specs.extend(raw.accounts.iter().cloned().map(|a| (PoolKind::Legacy, a)));
    } else {
        account_specs.extend(raw.accounts.iter().cloned().map(|a| (PoolKind::Legacy, a)));
    }

    // Derive the legacy server URL from the first account. Native pool accounts
    // always carry their provider-specific upstream and do not share this value.
    let primary_provider_derived = account_specs
        .first()
        .map(|(kind, a)| {
            a.provider
                .as_deref()
                .map(Provider::from_str)
                .unwrap_or_else(|| match kind {
                    PoolKind::Codex => Provider::OpenAI,
                    _ => Provider::Anthropic,
                })
        })
        .unwrap_or_default();
    let default_upstream = primary_provider_derived.default_upstream_url().to_owned();

    let upstream_url = raw
        .server
        .upstream_url
        .clone()
        .or_else(|| std::env::var("SHUNT_UPSTREAM_URL").ok())
        .unwrap_or(default_upstream);

    let relay_url = raw
        .server
        .relay_url
        .clone()
        .or_else(|| std::env::var("SHUNT_RELAY_URL").ok())
        .unwrap_or_else(|| "https://relay.ramcharan.shop".into());

    let telemetry_url = raw
        .server
        .telemetry_url
        .clone()
        .or_else(|| std::env::var("SHUNT_TELEMETRY_URL").ok());
    let telemetry_token = raw
        .server
        .telemetry_token
        .clone()
        .or_else(|| std::env::var("SHUNT_TELEMETRY_TOKEN").ok());
    let instance_name = raw
        .server
        .instance_name
        .clone()
        .or_else(|| std::env::var("SHUNT_INSTANCE_NAME").ok())
        .unwrap_or_else(default_instance_name);

    // #6 SSRF: validate the server-level upstream URL.
    // Allow loopback only when the URL was derived from a Local provider's default
    // (e.g. an all-Ollama config); explicit upstream_url entries are never allowed to
    // use loopback unless explicitly set via SHUNT_UPSTREAM_URL (trust the operator).
    let server_url_is_local_derived = raw.server.upstream_url.is_none()
        && std::env::var("SHUNT_UPSTREAM_URL").is_err()
        && matches!(primary_provider_derived, Provider::Local);
    validate_upstream_url(&upstream_url, server_url_is_local_derived)
        .with_context(|| "server.upstream_url failed validation")?;

    let classifier_account = raw
        .classifier
        .account
        .clone()
        .or(raw.server.classifier_account.clone())
        .or_else(|| std::env::var("SHUNT_CLASSIFIER_ACCOUNT").ok())
        .map(|n| scoped_account_name(PoolKind::Claude, &n, schema_version));
    let classifier_fallback_account = raw
        .classifier
        .fallback_account
        .clone()
        .or(raw.server.classifier_fallback_account.clone())
        .or_else(|| std::env::var("SHUNT_CLASSIFIER_FALLBACK_ACCOUNT").ok())
        .map(|n| scoped_account_name(PoolKind::Claude, &n, schema_version));
    let classifier_system_prompt_path = raw
        .classifier
        .system_prompt_path
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .or(raw.server.classifier_system_prompt_path.clone())
        .or_else(|| std::env::var("SHUNT_CLASSIFIER_SYSTEM_PROMPT_PATH").ok());

    let server = ServerConfig {
        host: raw.server.host,
        port: if schema_version >= NATIVE_POOLS_SCHEMA_VERSION {
            pools.claude.port
        } else {
            raw.server.port
        },
        control_port: raw.server.control_port,
        log_level: raw.server.log_level,
        upstream_url,
        remote_key: raw.server.remote_key,
        relay_url,
        custom_domain: raw.server.custom_domain,
        sticky_ttl_ms: raw.server.sticky_ttl_minutes.unwrap_or(10) * 60 * 1000,
        expiry_soon_secs: raw.server.expiry_soon_minutes.unwrap_or(30) * 60,
        routing_strategy: if schema_version >= NATIVE_POOLS_SCHEMA_VERSION {
            pools.claude.routing_strategy
        } else {
            raw.server
                .routing_strategy
                .as_deref()
                .and_then(RoutingStrategy::from_str)
                .unwrap_or_default()
        },
        request_timeout_secs: raw.server.request_timeout_secs.unwrap_or(600),
        rate_limit_rpm: raw.server.rate_limit_rpm.unwrap_or(0),
        trust_proxy_headers: raw.server.trust_proxy_headers.unwrap_or(false),
        health_check_enabled: raw.server.health_check_enabled.unwrap_or(true),
        health_check_interval_secs: raw.server.health_check_interval_secs.unwrap_or(300),
        health_check_timeout_secs: raw.server.health_check_timeout_secs.unwrap_or(10),
        telemetry_url,
        telemetry_token,
        instance_name,
        burst_rpm_limit: raw.server.burst_rpm_limit.unwrap_or(10),
        fallback_model: if schema_version >= NATIVE_POOLS_SCHEMA_VERSION {
            pools
                .claude
                .fallback_models
                .first()
                .cloned()
                .or(raw.server.fallback_model)
        } else {
            raw.server.fallback_model
        },
        telemetry: raw.server.telemetry.unwrap_or(true)
            && std::env::var("SHUNT_NO_TELEMETRY")
                .map(|v| v == "1")
                .unwrap_or(false)
                == false,
        classifier_account,
        classifier_system_prompt_path,
        classifier_fallback_account,
        max_startup_wait_ms: raw.server.max_startup_wait_ms.unwrap_or(8_000),
    };

    let mut api_overflow = resolve_overflow(raw.api_overflow.clone(), "ANTHROPIC_API_KEY");
    if schema_version < NATIVE_POOLS_SCHEMA_VERSION {
        api_overflow.account = api_overflow
            .account
            .or_else(|| std::env::var("SHUNT_API_OVERFLOW_ACCOUNT").ok());
        api_overflow.daily_budget_usd = std::env::var("SHUNT_API_OVERFLOW_DAILY_BUDGET_USD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(api_overflow.daily_budget_usd);
        pools.claude.overflow = api_overflow.clone();
    }

    if account_specs.is_empty() && !pools.claude.overflow.enabled && !pools.codex.overflow.enabled {
        bail!("Config has no accounts. Run `shunt setup` to add one.");
    }

    let store = CredentialsStore::load();

    let primary_provider = primary_provider_derived;

    let mut accounts = Vec::new();
    let mut seen_names = std::collections::HashSet::new();
    for (kind, a) in &account_specs {
        let provider = a
            .provider
            .as_deref()
            .map(Provider::from_str)
            .unwrap_or_else(|| match kind {
                PoolKind::Codex => Provider::OpenAI,
                _ => Provider::Anthropic,
            });
        match kind {
            PoolKind::Claude
                if !matches!(provider, Provider::Anthropic | Provider::AnthropicApi) =>
            {
                bail!(
                    "Claude pool account '{}' must use provider anthropic or anthropic-api",
                    a.name
                );
            }
            PoolKind::Codex if !matches!(provider, Provider::OpenAI | Provider::OpenAIApi) => {
                bail!(
                    "Codex pool account '{}' must use provider openai/codex or openai-api",
                    a.name
                );
            }
            PoolKind::Legacy
                if schema_version >= NATIVE_POOLS_SCHEMA_VERSION
                    && matches!(
                        provider,
                        Provider::Anthropic
                            | Provider::AnthropicApi
                            | Provider::OpenAI
                            | Provider::OpenAIApi
                    ) =>
            {
                bail!("Native provider account '{}' must be declared under [pools.claude] or [pools.codex]", a.name);
            }
            _ => {}
        }
        let resolved_name = scoped_account_name(*kind, &a.name, schema_version);
        if !seen_names.insert(resolved_name.clone()) {
            bail!("Duplicate account name after pool scoping: {resolved_name}");
        }

        // Resolve credential.
        //
        // OAuth providers (Anthropic, OpenAI): credentials.json first, then
        // auto-import from the provider's local CLI tool.
        //
        // API-key providers: credentials.json first, then inline api_key field,
        // then api_key_env field, then the provider's well-known env var.
        let source = CredentialSourceKind::parse(a.credential_source.as_deref())?;
        if schema_version >= CONFIG_SCHEMA_VERSION
            && a.credential_id.as_deref().unwrap_or("").is_empty()
            && source != CredentialSourceKind::None
        {
            bail!(
                "Account '{}' must declare credential_id in schema v{CONFIG_SCHEMA_VERSION}",
                a.name
            );
        }
        let allow_cli_import = source == CredentialSourceKind::ProviderCli;
        let legacy_or_local = || {
            store.get(*kind, &a.name)
            .or_else(|| {
                // Inline api_key from TOML (less secure, but convenient for testing).
                a.api_key.as_deref().map(|k| {
                    tracing::warn!(account = %a.name, "Inline api_key in config.toml is insecure — use api_key_env instead");
                    Credential::Apikey { key: k.to_owned() }
                })
            })
            .or_else(|| {
                // api_key_env: name of env var holding the key.
                a.api_key_env.as_deref()
                    .and_then(|var| selected_secret(&env_values, var))
                    .map(|k| Credential::Apikey { key: k })
            })
            .or_else(|| {
                // Auto-import from provider's CLI tool (OAuth providers) or
                // well-known env var (API-key providers).
                if allow_cli_import { provider.read_local_credentials() } else { None }
            })
        };
        let cred: Option<Credential> = match source {
            CredentialSourceKind::WebsiteBroker => Some(
                crate::website::resolve_credential(
                    &website_config,
                    a.credential_id
                        .as_deref()
                        .context("website-broker account missing credential_id")?,
                )
                .with_context(|| {
                    format!(
                        "Failed to lease Website3 credential for account '{}'",
                        a.name
                    )
                })?,
            ),
            CredentialSourceKind::EnvFile => a
                .api_key_env
                .as_deref()
                .and_then(|var| selected_secret(&env_values, var))
                .map(|key| Credential::Apikey { key }),
            CredentialSourceKind::None => None,
            CredentialSourceKind::LocalStore | CredentialSourceKind::ProviderCli => {
                legacy_or_local()
            }
        };

        // Upstream URL: per-account override from TOML takes priority, then
        // non-primary-provider accounts get the provider's default URL so
        // the forwarder knows where to send requests.
        let is_local = matches!(provider, Provider::Local);
        if let Some(ref url) = a.upstream_url {
            // #6 SSRF: allow loopback only for Local provider (e.g. Ollama at localhost).
            validate_upstream_url(url, is_local)
                .with_context(|| format!("account '{}' upstream_url failed validation", a.name))?;
        }
        let acct_upstream = a.upstream_url.clone().or_else(|| {
            if schema_version >= NATIVE_POOLS_SCHEMA_VERSION || provider != primary_provider {
                Some(provider.default_upstream_url().to_owned())
            } else {
                None
            }
        });

        accounts.push(AccountConfig {
            name: resolved_name,
            plan_type: a.plan_type.clone(),
            provider,
            credential: cred,
            upstream_url: acct_upstream,
            model: a.model.clone(),
        });
    }

    // Native overflow accounts are synthesized from environment references.
    // This keeps API keys out of both config.toml and credentials.json.
    for (kind, pool, provider) in [
        (PoolKind::Claude, &mut pools.claude, Provider::AnthropicApi),
        (PoolKind::Codex, &mut pools.codex, Provider::OpenAIApi),
    ] {
        if !pool.overflow.enabled {
            continue;
        }
        let configured = pool
            .overflow
            .account
            .clone()
            .unwrap_or_else(|| "api-overflow".to_owned());
        let resolved_name = scoped_account_name(kind, &configured, schema_version);
        pool.overflow.account = Some(resolved_name.clone());
        if accounts.iter().any(|a| a.name == resolved_name) {
            continue;
        }
        let key = pool
            .overflow
            .key_env
            .as_deref()
            .and_then(|key_env| selected_secret(&env_values, key_env));
        accounts.push(AccountConfig {
            name: resolved_name,
            plan_type: "api-overflow".into(),
            provider: provider.clone(),
            credential: key.map(|key| Credential::Apikey { key }),
            upstream_url: Some(provider.default_upstream_url().to_owned()),
            model: None,
        });
    }

    api_overflow = pools.claude.overflow.clone();

    let classifier = ClassifierConfig {
        enabled: raw.classifier.enabled.unwrap_or(true),
        upstream_url: raw.classifier.upstream_url.clone(),
        model: raw.classifier.model.clone(),
        fail_closed: raw.classifier.fail_closed.unwrap_or(true),
    };
    if let Some(url) = classifier.upstream_url.as_deref() {
        validate_upstream_url(url, true)
            .with_context(|| "classifier.upstream_url failed validation")?;
    }
    let bridge = BridgeConfig {
        enabled: raw.bridge.enabled.unwrap_or(true),
        concurrency_per_provider: raw.bridge.concurrency_per_provider.unwrap_or(2).max(1),
        queue_capacity: raw.bridge.queue_capacity.unwrap_or(32).max(1),
        timeout_secs: raw.bridge.timeout_secs.unwrap_or(1_800).max(1),
        max_depth: raw.bridge.max_depth.unwrap_or(1),
        retention_hours: raw.bridge.retention_hours.unwrap_or(24).max(1),
        network_ceiling: raw
            .bridge
            .network_ceiling
            .as_deref()
            .and_then(NetworkPolicy::parse)
            .unwrap_or(NetworkPolicy::Allowlisted),
        codex_fallback_models: raw.bridge.codex_fallback_models.clone(),
        claude_fallback_models: raw.bridge.claude_fallback_models.clone(),
        required_checks: raw.bridge.required_checks.clone(),
    };
    let manual_swarm = resolve_manual_swarm(raw.manual_swarm, &website_config.base_url)?;

    Ok(Config {
        schema_version,
        server,
        accounts,
        config_file: p,
        model_mapping: raw.model_mapping,
        api_overflow,
        pools,
        secrets: SecretsConfig {
            env_file: raw.secrets.env_file,
        },
        classifier,
        bridge,
        manual_swarm,
    })
}

/// Load only Manual Swarm control-plane settings. Unlike `load_config`, this
/// never resolves provider credentials, reads secret env files, or leases
/// Website3 accounts. MCP control operations therefore stay fast and cannot
/// broaden their secret access as an incidental side effect.
pub fn load_manual_swarm_config(path: Option<&Path>) -> Result<(ManualSwarmConfig, NetworkPolicy)> {
    let p = path.map(PathBuf::from).unwrap_or_else(config_path);
    let text = std::fs::read_to_string(&p)
        .with_context(|| format!("Failed to read config: {}", p.display()))?;
    let raw: RawConfig = toml::from_str(&text)
        .with_context(|| format!("Failed to parse config: {}", p.display()))?;
    let schema_version = raw.schema_version.unwrap_or(1);
    if schema_version > CONFIG_SCHEMA_VERSION {
        bail!("Config schema_version {schema_version} is newer than this shunt supports ({CONFIG_SCHEMA_VERSION})");
    }
    let website_base = raw
        .website
        .base_url
        .or_else(|| std::env::var("SHUNT_WEBSITE_URL").ok())
        .unwrap_or_else(|| crate::website::DEFAULT_WEBSITE_URL.into());
    let manual = resolve_manual_swarm(raw.manual_swarm, &website_base)?;
    let network_ceiling = manual.network_ceiling;
    Ok((manual, network_ceiling))
}

fn upgrade_account_reference(account: &mut toml::Value, pool: PoolKind) {
    let Some(table) = account.as_table_mut() else {
        return;
    };
    let Some(name) = table
        .get("name")
        .and_then(toml::Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return;
    };
    let provider = table
        .get("provider")
        .and_then(toml::Value::as_str)
        .unwrap_or(match pool {
            PoolKind::Codex => "openai",
            _ => "anthropic",
        });
    let old_source = table.get("credential_source").and_then(toml::Value::as_str);
    let source = if table.contains_key("api_key_env") {
        "env-file"
    } else if matches!(provider, "local") {
        "none"
    } else {
        match old_source {
            Some("codex_auth_file" | "claude_credentials_file" | "local_cli" | "provider-cli") => {
                "provider-cli"
            }
            Some("env-file") => "env-file",
            Some("website-broker") => "website-broker",
            Some("none") => "none",
            _ => "local-store",
        }
    };
    let id = match source {
        "env-file" => table
            .get("api_key_env")
            .and_then(toml::Value::as_str)
            .map(|key| format!("env:{key}")),
        "none" => Some(format!("none:{name}")),
        _ => Some(if pool == PoolKind::Legacy {
            name.clone()
        } else {
            format!("{}/{name}", pool.as_str())
        }),
    };
    table.insert(
        "credential_source".into(),
        toml::Value::String(source.into()),
    );
    if !table.contains_key("credential_id") {
        if let Some(id) = id {
            table.insert("credential_id".into(), toml::Value::String(id));
        }
    }
}

fn upgrade_v3_references(root: &mut toml::Table) {
    if let Some(accounts) = root.get_mut("accounts").and_then(toml::Value::as_array_mut) {
        for account in accounts {
            upgrade_account_reference(account, PoolKind::Legacy);
        }
    }
    if let Some(pools) = root.get_mut("pools").and_then(toml::Value::as_table_mut) {
        for (name, kind) in [("claude", PoolKind::Claude), ("codex", PoolKind::Codex)] {
            if let Some(accounts) = pools
                .get_mut(name)
                .and_then(toml::Value::as_table_mut)
                .and_then(|pool| pool.get_mut("accounts"))
                .and_then(toml::Value::as_array_mut)
            {
                for account in accounts {
                    upgrade_account_reference(account, kind);
                }
            }
        }
    }
}

fn write_migrated_config(path: &Path, migrated: &str, backup_suffix: &str) -> Result<()> {
    let backup = path.with_extension(backup_suffix);
    if !backup.exists() {
        std::fs::copy(path, &backup)?;
    }
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, migrated)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Upgrade config metadata to the latest schema. Dry-run returns the complete
/// proposed TOML without touching disk. Apply is backup-first and idempotent.
pub fn migrate_config_file(path: &Path, apply: bool, env_file: Option<&Path>) -> Result<String> {
    let original = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config: {}", path.display()))?;
    let mut root: toml::Table = toml::from_str(&original)
        .with_context(|| format!("Failed to parse config: {}", path.display()))?;
    let old_schema = root
        .get("schema_version")
        .and_then(toml::Value::as_integer)
        .unwrap_or(1);
    if old_schema > CONFIG_SCHEMA_VERSION as i64 {
        bail!("Config schema_version {old_schema} is newer than this shunt supports ({CONFIG_SCHEMA_VERSION})");
    }
    if old_schema == CONFIG_SCHEMA_VERSION as i64 {
        return Ok(original);
    }

    // Native-pool v2 already has the correct topology. Its v3 migration only
    // adds attachment references; rebuilding pools here would lose accounts.
    if old_schema >= NATIVE_POOLS_SCHEMA_VERSION as i64 {
        upgrade_v3_references(&mut root);
        root.insert(
            "schema_version".into(),
            toml::Value::Integer(CONFIG_SCHEMA_VERSION as i64),
        );
        if let Some(env_file) = env_file {
            if !env_file.is_absolute() {
                bail!("--env-file must be an absolute path");
            }
            let secrets = root
                .entry("secrets")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .context("secrets must be a TOML table")?;
            secrets.insert(
                "env_file".into(),
                toml::Value::String(env_file.to_string_lossy().into_owned()),
            );
        }
        let migrated = toml::to_string_pretty(&root)?;
        if apply {
            write_migrated_config(path, &migrated, "toml.bak-v2")?;
        }
        return Ok(migrated);
    }

    let accounts = root
        .remove("accounts")
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let legacy_overflow = root.remove("api_overflow");
    let classifier_account = root
        .get("server")
        .and_then(toml::Value::as_table)
        .and_then(|t| t.get("classifier_account"))
        .and_then(toml::Value::as_str)
        .map(ToOwned::to_owned);
    let local_classifier = classifier_account.as_deref().and_then(|name| {
        accounts.iter().find_map(|account| {
            let table = account.as_table()?;
            if table.get("name").and_then(toml::Value::as_str) != Some(name) {
                return None;
            }
            if table.get("provider").and_then(toml::Value::as_str) != Some("local") {
                return None;
            }
            Some((
                table.get("upstream_url")?.as_str()?.to_owned(),
                table.get("model")?.as_str()?.to_owned(),
            ))
        })
    });
    let old_port = root
        .get("server")
        .and_then(toml::Value::as_table)
        .and_then(|t| t.get("port"))
        .and_then(toml::Value::as_integer)
        .unwrap_or(8082);
    if let Some(server) = root.get_mut("server").and_then(toml::Value::as_table_mut) {
        server.remove("port");
        if local_classifier.is_some() {
            server.remove("classifier_account");
        }
    }

    let mut claude_accounts = Vec::new();
    let mut codex_accounts = Vec::new();
    let mut legacy_accounts = Vec::new();
    for mut account in accounts {
        let provider = account
            .as_table()
            .and_then(|t| t.get("provider"))
            .and_then(toml::Value::as_str)
            .unwrap_or("anthropic")
            .to_ascii_lowercase();
        let account_name = account
            .as_table()
            .and_then(|t| t.get("name"))
            .and_then(toml::Value::as_str);
        if local_classifier.is_some()
            && provider == "local"
            && account_name == classifier_account.as_deref()
        {
            continue;
        }
        if let Some(table) = account.as_table_mut() {
            let source = match provider.as_str() {
                "openai" | "codex" => Some("codex_auth_file"),
                "anthropic" => Some("claude_credentials_file"),
                _ => None,
            };
            if let Some(source) = source {
                table
                    .entry("credential_source")
                    .or_insert(toml::Value::String(source.into()));
            }
        }
        match provider.as_str() {
            "anthropic" | "anthropic-api" | "anthropic_api" => claude_accounts.push(account),
            "openai" | "codex" | "openai-api" | "openai_api" => codex_accounts.push(account),
            _ => legacy_accounts.push(account),
        }
    }

    let mut claude = toml::Table::new();
    claude.insert("port".into(), toml::Value::Integer(old_port));
    claude.insert(
        "routing_strategy".into(),
        toml::Value::String("maximus".into()),
    );
    claude.insert(
        "accounts".into(),
        toml::Value::Array(claude_accounts.clone()),
    );
    if let Some(overflow) = legacy_overflow {
        claude.insert("overflow".into(), overflow);
    }
    let mut codex = toml::Table::new();
    codex.insert("port".into(), toml::Value::Integer(8083));
    codex.insert(
        "routing_strategy".into(),
        toml::Value::String("maximus".into()),
    );
    codex.insert(
        "accounts".into(),
        toml::Value::Array(codex_accounts.clone()),
    );
    let mut pools = toml::Table::new();
    pools.insert("claude".into(), toml::Value::Table(claude));
    pools.insert("codex".into(), toml::Value::Table(codex));
    root.insert(
        "schema_version".into(),
        toml::Value::Integer(CONFIG_SCHEMA_VERSION as i64),
    );
    root.insert("pools".into(), toml::Value::Table(pools));
    if !legacy_accounts.is_empty() {
        root.insert("accounts".into(), toml::Value::Array(legacy_accounts));
    }
    if let Some(env_file) = env_file {
        if !env_file.is_absolute() {
            bail!("--env-file must be an absolute path");
        }
        let mut secrets = toml::Table::new();
        secrets.insert(
            "env_file".into(),
            toml::Value::String(env_file.to_string_lossy().into_owned()),
        );
        root.insert("secrets".into(), toml::Value::Table(secrets));
    }
    root.entry("classifier").or_insert_with(|| {
        let mut classifier = toml::Table::from_iter([
            ("enabled".into(), toml::Value::Boolean(true)),
            ("fail_closed".into(), toml::Value::Boolean(true)),
        ]);
        if let Some((upstream, model)) = &local_classifier {
            classifier.insert("upstream_url".into(), toml::Value::String(upstream.clone()));
            classifier.insert("model".into(), toml::Value::String(model.clone()));
        }
        toml::Value::Table(classifier)
    });
    root.entry("bridge").or_insert_with(|| {
        toml::Value::Table(toml::Table::from_iter([
            ("enabled".into(), toml::Value::Boolean(true)),
            ("retention_hours".into(), toml::Value::Integer(24)),
            (
                "network_ceiling".into(),
                toml::Value::String("allowlisted".into()),
            ),
        ]))
    });
    upgrade_v3_references(&mut root);
    let migrated = toml::to_string_pretty(&root)?;
    if !apply {
        return Ok(migrated);
    }

    write_migrated_config(path, &migrated, "toml.bak-v1")?;

    let credential_backup = credentials_path().with_extension("json.bak-v1");
    if credentials_path().exists() && !credential_backup.exists() {
        std::fs::copy(credentials_path(), &credential_backup)?;
    }
    let mut store = CredentialsStore::load();
    let mut move_credential = |pool: PoolKind, account: &toml::Value| {
        let Some(name) = account
            .as_table()
            .and_then(|t| t.get("name"))
            .and_then(toml::Value::as_str)
        else {
            return;
        };
        if let Some(credential) = store.accounts.remove(name) {
            store.insert(pool, name.to_owned(), credential);
        }
    };
    for account in &claude_accounts {
        move_credential(PoolKind::Claude, account);
    }
    for account in &codex_accounts {
        move_credential(PoolKind::Codex, account);
    }
    store.save()?;

    let state_backup = state_path().with_extension("json.bak-v1");
    if state_path().exists() {
        if !state_backup.exists() {
            std::fs::copy(state_path(), &state_backup)?;
        }
        let claude_names = claude_accounts
            .iter()
            .filter_map(|a| a.as_table()?.get("name")?.as_str().map(ToOwned::to_owned))
            .collect();
        let codex_names = codex_accounts
            .iter()
            .filter_map(|a| a.as_table()?.get("name")?.as_str().map(ToOwned::to_owned))
            .collect();
        migrate_state_account_names(&state_path(), &claude_names, &codex_names)?;
    }
    Ok(migrated)
}

fn migrate_state_account_names(
    path: &Path,
    claude_names: &std::collections::HashSet<String>,
    codex_names: &std::collections::HashSet<String>,
) -> Result<()> {
    let mut value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let scoped = |name: &str| {
        if claude_names.contains(name) {
            format!("claude/{name}")
        } else if codex_names.contains(name) {
            format!("codex/{name}")
        } else {
            name.to_owned()
        }
    };
    for field in ["accounts", "quota", "rate_limits", "per_account_daily"] {
        if let Some(map) = value
            .get_mut(field)
            .and_then(serde_json::Value::as_object_mut)
        {
            let old = std::mem::take(map);
            for (name, entry) in old {
                map.insert(scoped(&name), entry);
            }
        }
    }
    if let Some(sticky) = value
        .get_mut("sticky")
        .and_then(serde_json::Value::as_object_mut)
    {
        for entry in sticky.values_mut() {
            if let Some(name) = entry.get("account_name").and_then(|v| v.as_str()) {
                entry["account_name"] = serde_json::Value::String(scoped(name));
            }
        }
    }
    for (field, pool_field) in [
        ("pinned_account", "pinned_by_pool"),
        ("last_used_account", "last_used_by_pool"),
    ] {
        if let Some(name) = value
            .get(field)
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned)
        {
            let scoped_name = scoped(&name);
            if codex_names.contains(&name) {
                let root = value
                    .as_object_mut()
                    .context("state root must be a JSON object")?;
                let pools = root
                    .entry(pool_field)
                    .or_insert_with(|| serde_json::json!({}));
                pools
                    .as_object_mut()
                    .context("pool state must be a JSON object")?
                    .insert("codex".into(), serde_json::Value::String(scoped_name));
                root.insert(field.into(), serde_json::Value::Null);
            } else {
                value[field] = serde_json::Value::String(scoped_name);
            }
        }
    }
    if let Some(requests) = value
        .get_mut("recent_requests")
        .and_then(serde_json::Value::as_array_mut)
    {
        for request in requests {
            if let Some(name) = request.get("account").and_then(|v| v.as_str()) {
                request["account"] = serde_json::Value::String(scoped(name));
            }
        }
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&value)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub fn config_needs_migration(path: &Path) -> Result<bool> {
    let text = std::fs::read_to_string(path)?;
    let value: toml::Value = toml::from_str(&text)?;
    Ok(value
        .get("schema_version")
        .and_then(toml::Value::as_integer)
        != Some(CONFIG_SCHEMA_VERSION as i64))
}

// ---------------------------------------------------------------------------
// Config file template
// ---------------------------------------------------------------------------

pub fn config_template(accounts: &[(&str, &str)]) -> String {
    let mut out = String::from(
        "schema_version = 3\n\n[server]\nhost = \"127.0.0.1\"\ncontrol_port = 19081\nlog_level = \"info\"\n\n[pools.claude]\nport = 8082\nrouting_strategy = \"maximus\"\n\n[pools.claude.overflow]\nenabled = false\nkey_env = \"ANTHROPIC_API_KEY\"\ndaily_budget_usd = 500.0\nmax_output_tokens = 32768\n\n[pools.codex]\nport = 8083\nrouting_strategy = \"maximus\"\n\n[pools.codex.overflow]\nenabled = false\nkey_env = \"OPENAI_API_KEY\"\ndaily_budget_usd = 500.0\nmax_output_tokens = 32768\n\n[classifier]\nenabled = true\nfail_closed = true\n\n[bridge]\nenabled = true\nconcurrency_per_provider = 2\nqueue_capacity = 32\ntimeout_secs = 1800\nmax_depth = 1\nretention_hours = 24\nnetwork_ceiling = \"allowlisted\"\n",
    );
    out.push_str("\n[manual_swarm]\nenabled = false\ndefault_target = \"auto\"\ndefault_agents = 4\nmax_agents = 8\ndefault_duration_secs = 2700\nmax_duration_secs = 3600\napply_policy = \"explicit\"\nnetwork_ceiling = \"unrestricted\"\n");
    for (name, plan_type) in accounts {
        out.push_str(&format!(
            "\n[[pools.claude.accounts]]\nname = \"{name}\"\nplan_type = \"{plan_type}\"\nprovider = \"anthropic\"\ncredential_source = \"local-store\"\ncredential_id = \"claude/{name}\"\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("shunt-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn v2_config_scopes_native_accounts_and_overflow() {
        let dir = temp_dir("config-v2");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
schema_version = 2
[server]
host = "127.0.0.1"
control_port = 19081
[pools.claude]
port = 8082
[[pools.claude.accounts]]
name = "main"
provider = "anthropic"
credential_source = "shunt_store"
[pools.codex]
port = 8083
[[pools.codex.accounts]]
name = "main"
provider = "openai"
credential_source = "shunt_store"
[pools.codex.overflow]
enabled = true
key_env = "SHUNT_TEST_MISSING_OPENAI_KEY"
daily_budget_usd = 500
"#,
        )
        .unwrap();
        let config = load_config(Some(&path)).unwrap();
        assert_eq!(config.schema_version, 2);
        assert_eq!(config.pools.codex.port, 8083);
        assert!(config.accounts.iter().any(|a| a.name == "claude/main"));
        assert!(config.accounts.iter().any(|a| a.name == "codex/main"));
        assert_eq!(
            config.pools.codex.overflow.account.as_deref(),
            Some("codex/api-overflow")
        );
        assert!(config
            .accounts
            .iter()
            .any(|a| a.name == "codex/api-overflow" && a.provider == Provider::OpenAIApi));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn migration_partitions_native_and_legacy_accounts() {
        let dir = temp_dir("migration");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
[server]
port = 8123
classifier_account = "classifier"
[[accounts]]
name = "claude"
[[accounts]]
name = "codex"
provider = "openai"
[[accounts]]
name = "groq"
provider = "groq"
[[accounts]]
name = "classifier"
provider = "local"
upstream_url = "http://127.0.0.1:11434"
model = "qwen-test"
"#,
        )
        .unwrap();
        let preview = migrate_config_file(&path, false, None).unwrap();
        assert!(preview.contains("schema_version = 3"));
        assert!(preview.contains("[pools.claude]"));
        assert!(preview.contains("[pools.codex]"));
        assert!(preview.contains("[[pools.claude.accounts]]"));
        assert!(preview.contains("[[pools.codex.accounts]]"));
        assert!(preview.contains("[[accounts]]"));
        assert!(preview.contains("port = 8123"));
        assert!(preview.contains("upstream_url = \"http://127.0.0.1:11434\""));
        assert!(preview.contains("model = \"qwen-test\""));
        let migrated: toml::Value = toml::from_str(&preview).unwrap();
        let legacy = migrated
            .get("accounts")
            .and_then(toml::Value::as_array)
            .unwrap();
        assert_eq!(legacy.len(), 1);
        assert_eq!(
            legacy[0].get("name").and_then(toml::Value::as_str),
            Some("groq")
        );
        assert!(migrated
            .get("server")
            .and_then(|v| v.get("classifier_account"))
            .is_none());
        assert_eq!(
            migrated
                .get("classifier")
                .and_then(|v| v.get("model"))
                .and_then(toml::Value::as_str),
            Some("qwen-test")
        );
        assert!(!std::fs::read_to_string(&path)
            .unwrap()
            .contains("schema_version"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn secret_env_file_must_be_absolute() {
        let err = load_env_file(Path::new(".env.local"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("absolute"));
    }

    #[test]
    fn v2_migration_preserves_native_accounts_and_adds_references() {
        let dir = temp_dir("migration-v2-v3");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
schema_version = 2
[server]
host = "127.0.0.1"
[pools.claude]
port = 8082
[[pools.claude.accounts]]
name = "main"
provider = "anthropic"
credential_source = "shunt_store"
[pools.codex]
port = 8083
[[pools.codex.accounts]]
name = "work"
provider = "openai"
credential_source = "codex_auth_file"
"#,
        )
        .unwrap();
        let preview = migrate_config_file(&path, false, None).unwrap();
        let migrated: toml::Value = toml::from_str(&preview).unwrap();
        assert_eq!(migrated["schema_version"].as_integer(), Some(3));
        let claude = migrated["pools"]["claude"]["accounts"].as_array().unwrap();
        let codex = migrated["pools"]["codex"]["accounts"].as_array().unwrap();
        assert_eq!(claude.len(), 1);
        assert_eq!(codex.len(), 1);
        assert_eq!(claude[0]["credential_source"].as_str(), Some("local-store"));
        assert_eq!(claude[0]["credential_id"].as_str(), Some("claude/main"));
        assert_eq!(codex[0]["credential_source"].as_str(), Some("provider-cli"));
        assert_eq!(codex[0]["credential_id"].as_str(), Some("codex/work"));
        assert_eq!(
            std::fs::read_to_string(&path)
                .unwrap()
                .contains("schema_version = 2"),
            true
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn publishing_tokens_are_not_selectable_runtime_secrets() {
        let values = HashMap::from([
            ("NPMJS".to_owned(), "publish-secret".to_owned()),
            ("ANTHROPIC_API_KEY".to_owned(), "runtime-secret".to_owned()),
        ]);
        assert_eq!(selected_secret(&values, "NPMJS"), None);
        assert_eq!(
            selected_secret(&values, "ANTHROPIC_API_KEY").as_deref(),
            Some("runtime-secret")
        );
    }

    #[test]
    fn schema_v2_rejects_native_accounts_outside_their_pool() {
        let dir = temp_dir("unscoped-native");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
schema_version = 2
[[accounts]]
name = "misplaced"
provider = "openai"
"#,
        )
        .unwrap();
        let error = load_config(Some(&path)).unwrap_err().to_string();
        assert!(error.contains("must be declared under"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn state_migration_places_codex_runtime_values_in_the_codex_namespace() {
        let dir = temp_dir("state-migration");
        let path = dir.join("state.json");
        std::fs::write(
            &path,
            r#"{
            "pinned_account":"codex-main",
            "last_used_account":"codex-main",
            "recent_requests":[{"account":"claude-main"}]
        }"#,
        )
        .unwrap();
        let claude = std::collections::HashSet::from(["claude-main".to_owned()]);
        let codex = std::collections::HashSet::from(["codex-main".to_owned()]);
        migrate_state_account_names(&path, &claude, &codex).unwrap();
        let state: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(state["pinned_account"].is_null());
        assert_eq!(state["pinned_by_pool"]["codex"], "codex/codex-main");
        assert!(state["last_used_account"].is_null());
        assert_eq!(state["last_used_by_pool"]["codex"], "codex/codex-main");
        assert_eq!(state["recent_requests"][0]["account"], "claude/claude-main");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn manual_swarm_defaults_are_disabled_and_bounded() {
        let config =
            resolve_manual_swarm(RawManualSwarm::default(), "https://example.test").unwrap();
        assert!(!config.enabled);
        assert_eq!(
            config.control_url,
            "https://example.test/api/shunt/manual-swarms"
        );
        assert_eq!(config.default_agents, 4);
        assert!(config.default_agents <= config.max_agents);
        assert_eq!(config.apply_policy, ManualSwarmApplyPolicy::Explicit);
    }

    #[test]
    fn manual_swarm_rejects_unsafe_control_urls_and_operator_bounds() {
        for control_url in [
            "http://example.test/api/shunt/manual-swarms",
            "https://token@example.test/api/shunt/manual-swarms",
            "https://example.test/api/shunt/manual-swarms?redirect=evil",
            "https://example.test/not-manual-swarms",
        ] {
            let raw = RawManualSwarm {
                control_url: Some(control_url.into()),
                ..Default::default()
            };
            assert!(
                resolve_manual_swarm(raw, "https://example.test").is_err(),
                "accepted {control_url}"
            );
        }
        let too_many = RawManualSwarm {
            max_agents: Some(MANUAL_SWARM_MAX_AGENTS + 1),
            ..Default::default()
        };
        assert!(resolve_manual_swarm(too_many, "https://example.test").is_err());
        let too_long = RawManualSwarm {
            max_duration_secs: Some(MANUAL_SWARM_MAX_DURATION_SECS + 1),
            ..Default::default()
        };
        assert!(resolve_manual_swarm(too_long, "https://example.test").is_err());
    }

    #[test]
    fn manual_swarm_explicit_target_must_be_allowlisted() {
        let raw = RawManualSwarm {
            default_target: Some("build-fra1".into()),
            allowed_targets: vec!["local".into()],
            ..Default::default()
        };
        assert!(resolve_manual_swarm(raw, "https://example.test").is_err());
    }

    #[test]
    fn manual_swarm_control_load_does_not_resolve_broker_credentials() {
        let dir = temp_dir("manual-control-only");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
schema_version = 3
[server]
[pools.claude]
[[pools.claude.accounts]]
name = "remote"
provider = "anthropic"
credential_source = "website-broker"
credential_id = "opaque-account"
[manual_swarm]
enabled = true
default_target = "local"
allowed_targets = ["local"]
"#,
        )
        .unwrap();
        let (manual, ceiling) = load_manual_swarm_config(Some(&path)).unwrap();
        assert!(manual.enabled);
        assert_eq!(manual.default_target, "local");
        assert_eq!(ceiling, NetworkPolicy::Unrestricted);
        let _ = std::fs::remove_dir_all(dir);
    }
}

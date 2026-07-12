//! Website3-authenticated Manual Swarm control client.
//!
//! Manual Swarm is an execution runtime (`go_native`), not a third model
//! provider. Remote workers select Claude and Codex lanes through their normal
//! Shunt pools. All authority flows through the Website3 bearer session; local
//! Shunt never contacts Fabric, Auto Swarm, Kubernetes, or Doppler directly.

use anyhow::{bail, Context, Result};
use base64::Engine;
use futures_util::StreamExt;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;

use crate::config::{
    load_manual_swarm_config, ManualSwarmApplyPolicy, ManualSwarmConfig, NetworkPolicy,
    MANUAL_SWARM_CAPABILITY_VERSION,
};

const MAX_OBJECTIVE_BYTES: usize = 8 * 1024;
const MAX_GUIDANCE_BYTES: usize = 8 * 1024;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_INSPECT_BYTES: usize = 1024 * 1024;
const MAX_PATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_CURSOR_BYTES: usize = 1024;
const MAX_TOKEN_BYTES: usize = 48 * 1024;
const MAX_REFERENCE_BYTES: usize = 2048;
const MAX_SUBSCRIPTION_IDS: usize = 16;
const MAX_EVENT_LIMIT: u64 = 200;
const MAX_WAIT_SECS: u64 = 300;
const MAX_LOCAL_BINDINGS: usize = 128;
static STATE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static SENSITIVE_ASSIGNMENT: OnceLock<regex::Regex> = OnceLock::new();
static HTTP_CLIENT: OnceLock<Mutex<Option<(u64, reqwest::Client)>>> = OnceLock::new();

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn validate_identifier(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 160
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        bail!("{label} must be a 1-160 character opaque identifier");
    }
    Ok(())
}

fn validate_scope_identifier(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 160
        || value
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        bail!("{label} must be a 1-160 character scoped identifier");
    }
    Ok(())
}

fn validate_opaque(value: &str, label: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
    {
        bail!("{label} is invalid or exceeds {max} bytes");
    }
    Ok(())
}

fn validate_text(value: &str, label: &str, max: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        bail!("{label} must be non-empty and at most {max} bytes");
    }
    Ok(())
}

fn validate_source_ref(value: &str) -> Result<()> {
    validate_text(value, "sourceRef", MAX_REFERENCE_BYTES)?;
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        bail!("sourceRef contains control characters");
    }
    if let Ok(parsed) = url::Url::parse(value) {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            bail!("sourceRef must not contain embedded credentials");
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            bail!("sourceRef must not contain a query or fragment");
        }
        if !matches!(parsed.scheme(), "https" | "ssh" | "git" | "file") {
            bail!("sourceRef uses an unsupported URL scheme");
        }
    }
    Ok(())
}

fn validate_hosted_source_ref(value: &str) -> Result<()> {
    if let Ok(parsed) = url::Url::parse(value) {
        if matches!(parsed.scheme(), "https" | "ssh" | "git") {
            return Ok(());
        }
        bail!("hosted sourceRef must use https, ssh, or git");
    }
    let scp_style = value.split_once(':').is_some_and(|(host, path)| {
        host.contains('@') && !host.contains('/') && !path.is_empty() && !path.starts_with('/')
    });
    if !scp_style {
        bail!("hosted sourceRef must be a broker-authorized Git URL");
    }
    Ok(())
}

fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn deterministic_idempotency_key(operation: &str, body: &Value) -> Result<String> {
    let install_key = crate::config::local_client_token("manual-swarm-idempotency")?;
    let canonical = serde_json::to_vec(body)?;
    let mut digest = Sha256::new();
    digest.update(b"manual-swarm/v1\0");
    digest.update(operation.as_bytes());
    digest.update(b"\0");
    digest.update(install_key.as_bytes());
    digest.update(b"\0");
    digest.update(canonical);
    Ok(format!("msw_{}", hex::encode(digest.finalize())))
}

fn idempotency_key(input: Option<&str>, operation: &str, body: &Value) -> Result<String> {
    if let Some(value) = input {
        validate_scope_identifier(value, "idempotencyKey")?;
        if value.len() < 16 || value.len() > 128 {
            bail!("idempotencyKey must be between 16 and 128 characters");
        }
        return Ok(value.to_owned());
    }
    deterministic_idempotency_key(operation, body)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreviewBinding {
    workspace: PathBuf,
    base_commit: String,
    target: String,
    expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionBinding {
    workspace: PathBuf,
    base_commit: String,
    target: String,
    created_at: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LocalState {
    #[serde(default)]
    previews: BTreeMap<String, PreviewBinding>,
    #[serde(default)]
    sessions: BTreeMap<String, SessionBinding>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateEnvelope {
    payload: LocalState,
    mac: String,
}

fn state_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(crate::config::APP_NAME)
        .join("manual-swarm-state.json")
}

fn state_mac(payload: &[u8]) -> Result<String> {
    // Minimal HMAC-SHA256 to authenticate non-secret local metadata. The key
    // is derived from Shunt's per-install private identifier.
    let mut key = crate::config::local_client_token("manual-swarm-state")?.into_bytes();
    if key.len() > 64 {
        key = Sha256::digest(&key).to_vec();
    }
    key.resize(64, 0);
    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for (index, byte) in key.iter().enumerate() {
        inner_pad[index] ^= byte;
        outer_pad[index] ^= byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(payload);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner);
    Ok(hex::encode(outer.finalize()))
}

fn read_state() -> LocalState {
    let path = state_path();
    if std::fs::metadata(&path)
        .ok()
        .is_some_and(|metadata| metadata.len() > 1024 * 1024)
    {
        return LocalState::default();
    }
    let Some(envelope) = std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<StateEnvelope>(&bytes).ok())
    else {
        return LocalState::default();
    };
    let Ok(payload) = serde_json::to_vec(&envelope.payload) else {
        return LocalState::default();
    };
    let Ok(expected) = state_mac(&payload) else {
        return LocalState::default();
    };
    let matches = expected.len() == envelope.mac.len()
        && expected
            .bytes()
            .zip(envelope.mac.bytes())
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0;
    if matches {
        envelope.payload
    } else {
        LocalState::default()
    }
}

fn write_state(mut state: LocalState) -> Result<()> {
    let now = now_secs();
    state.previews.retain(|_, binding| binding.expires_at > now);
    while state.previews.len() > MAX_LOCAL_BINDINGS {
        let Some(key) = state
            .previews
            .iter()
            .min_by_key(|(_, binding)| binding.expires_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        state.previews.remove(&key);
    }
    while state.sessions.len() > MAX_LOCAL_BINDINGS {
        let Some(key) = state
            .sessions
            .iter()
            .min_by_key(|(_, binding)| binding.created_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        state.sessions.remove(&key);
    }

    let path = state_path();
    let parent = path
        .parent()
        .context("manual swarm state path has no parent")?;
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let tmp = parent.join(format!(".manual-swarm-{}.tmp", uuid::Uuid::new_v4()));
    let payload = serde_json::to_vec(&state)?;
    let envelope = StateEnvelope {
        mac: state_mac(&payload)?,
        payload: state,
    };
    std::fs::write(&tmp, serde_json::to_vec_pretty(&envelope)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn remember_preview(token: &str, binding: PreviewBinding) -> Result<()> {
    let _guard = STATE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow::anyhow!("Manual Swarm local state lock is poisoned"))?;
    let mut state = read_state();
    state.previews.insert(token_hash(token), binding);
    write_state(state)
}

fn preview_binding(token: &str) -> Result<PreviewBinding> {
    let state = read_state();
    let binding = state
        .previews
        .get(&token_hash(token))
        .context("preview token is not bound to this Shunt installation; request a fresh plan")?
        .clone();
    if binding.expires_at <= now_secs() {
        bail!("preview token expired; request a fresh plan");
    }
    Ok(binding)
}

fn remember_session(id: &str, preview: &PreviewBinding) -> Result<()> {
    let _guard = STATE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow::anyhow!("Manual Swarm local state lock is poisoned"))?;
    let mut state = read_state();
    state.sessions.insert(
        id.to_owned(),
        SessionBinding {
            workspace: preview.workspace.clone(),
            base_commit: preview.base_commit.clone(),
            target: preview.target.clone(),
            created_at: now_secs(),
        },
    );
    write_state(state)
}

fn session_binding(id: &str) -> Result<SessionBinding> {
    read_state().sessions.get(id).cloned().context(
        "session has no trusted local apply binding; start it from this Shunt installation",
    )
}

fn forget_session(id: &str) -> Result<()> {
    let _guard = STATE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow::anyhow!("Manual Swarm local state lock is poisoned"))?;
    let mut state = read_state();
    if state.sessions.remove(id).is_none() {
        return Ok(());
    }
    write_state(state)
}

fn redact_token_patterns(mut text: String) -> String {
    for prefix in [
        "sk-ant-",
        "sk-proj-",
        "sk-",
        "ghp_",
        "github_pat_",
        "npm_",
        "dp.st.",
        "ya29.",
        "Bearer ",
        "bearer ",
    ] {
        while let Some(start) = text.find(prefix) {
            let end = text[start..]
                .find(|ch: char| {
                    ch.is_whitespace() || matches!(ch, '"' | '\'' | ',' | '}' | ']' | '\\')
                })
                .map(|offset| start + offset)
                .unwrap_or(text.len());
            text.replace_range(start..end, "[REDACTED]");
        }
    }
    let pattern = SENSITIVE_ASSIGNMENT.get_or_init(|| regex::Regex::new(
        r#"(?i)(authorization|cookie|openai_api_key|anthropic_api_key|doppler_token|npm_token|node_auth_token)\s*[:=]\s*[^\s,}\]"']+"#
    ).expect("static secret redaction regex must compile"));
    text = pattern.replace_all(&text, "$1=[REDACTED]").into_owned();
    text
}

fn sanitize_remote_value(value: &mut Value, depth: usize) -> Result<()> {
    if depth > 16 {
        bail!("Manual Swarm response nesting exceeds safety limit");
    }
    match value {
        Value::Object(object) => {
            let keys: Vec<String> = object.keys().cloned().collect();
            for key in keys {
                let normalized = key.to_ascii_lowercase();
                if normalized == "session_token" {
                    object.remove(&key);
                    continue;
                }
                if matches!(
                    normalized.as_str(),
                    "access_token"
                        | "refresh_token"
                        | "id_token"
                        | "website_cookie"
                        | "cookie"
                        | "doppler_token"
                        | "provider_credential"
                ) || normalized == "api_key"
                    || normalized.ends_with("_api_key")
                    || normalized == "authorization"
                    || normalized.ends_with("_secret")
                    || normalized.contains(".env.local")
                {
                    bail!("Manual Swarm server returned forbidden credential material");
                }
                if let Some(child) = object.get_mut(&key) {
                    if matches!(
                        normalized.as_str(),
                        "patch_base64" | "content_base64" | "inline_base64"
                    ) {
                        let encoded = child
                            .as_str()
                            .context("patch content must be base64 text")?;
                        if encoded.len() > MAX_PATCH_BYTES.saturating_mul(2) {
                            bail!("encoded patch content exceeds safety limit");
                        }
                        continue;
                    }
                    sanitize_remote_value(child, depth + 1)?;
                }
            }
        }
        Value::Array(values) => {
            if values.len() > MAX_EVENT_LIMIT as usize {
                bail!("Manual Swarm response array exceeds safety limit");
            }
            for child in values {
                sanitize_remote_value(child, depth + 1)?;
            }
        }
        Value::String(text) => {
            *text = redact_token_patterns(std::mem::take(text));
            if text.len() > 64 * 1024 {
                text.truncate(64 * 1024);
                text.push_str("\n[truncated]");
            }
        }
        _ => {}
    }
    Ok(())
}

fn contains_embedded_content(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            matches!(
                key.to_ascii_lowercase().as_str(),
                "patch_base64" | "content_base64" | "inline_base64"
            ) || contains_embedded_content(child)
        }),
        Value::Array(values) => values.iter().any(contains_embedded_content),
        _ => false,
    }
}

fn ensure_capability(value: &Value) -> Result<()> {
    let capability = value
        .get("capability_version")
        .or_else(|| value.pointer("/session/capability_version"))
        .and_then(Value::as_str);
    if capability != Some(MANUAL_SWARM_CAPABILITY_VERSION) {
        bail!(
            "Manual Swarm server is unavailable or incompatible; required capability {}",
            MANUAL_SWARM_CAPABILITY_VERSION
        );
    }
    Ok(())
}

struct ManualClient {
    config: ManualSwarmConfig,
    bearer: String,
    http: reqwest::Client,
}

impl ManualClient {
    fn new(config: ManualSwarmConfig, bearer: String) -> Result<Self> {
        let mut cached = HTTP_CLIENT
            .get_or_init(|| Mutex::new(None))
            .lock()
            .map_err(|_| anyhow::anyhow!("Manual Swarm HTTP client lock is poisoned"))?;
        let http = if let Some((_, client)) = cached
            .as_ref()
            .filter(|(timeout, _)| *timeout == config.request_timeout_secs)
        {
            client.clone()
        } else {
            let timeout = std::time::Duration::from_secs(config.request_timeout_secs);
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(timeout.min(std::time::Duration::from_secs(10)))
                .timeout(timeout)
                .user_agent(format!("shunt/{}/manual-swarm", env!("CARGO_PKG_VERSION")))
                .build()?;
            *cached = Some((config.request_timeout_secs, client.clone()));
            client
        };
        Ok(Self {
            config,
            bearer,
            http,
        })
    }

    async fn request(
        &self,
        method: Method,
        suffix: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
        max_bytes: usize,
    ) -> Result<Value> {
        if suffix.contains("..") || suffix.bytes().any(|byte| byte.is_ascii_control()) {
            bail!("invalid Manual Swarm route");
        }
        let url = format!("{}{}", self.config.control_url, suffix);
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(&self.bearer)
            .header("Accept", "application/json")
            .query(query);
        if let Some(value) = body {
            let encoded =
                serde_json::to_vec(value).context("failed to encode Manual Swarm request JSON")?;
            if encoded.len() > MAX_REQUEST_BYTES {
                bail!("Manual Swarm request exceeds {MAX_REQUEST_BYTES} byte limit");
            }
            request = request
                .header("Content-Type", "application/json")
                .body(encoded);
            if let Some(key) = value.get("idempotency_key").and_then(Value::as_str) {
                request = request.header("Idempotency-Key", key);
            }
        }
        let response = request
            .send()
            .await
            .context("Website3 Manual Swarm API is unavailable")?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > max_bytes as u64)
        {
            bail!("Manual Swarm response exceeds {max_bytes} byte limit");
        }
        let mut bytes = Vec::with_capacity(
            response.content_length().unwrap_or(0).min(max_bytes as u64) as usize,
        );
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("failed to read Manual Swarm response")?;
            if bytes.len().saturating_add(chunk.len()) > max_bytes {
                bail!("Manual Swarm response exceeds {max_bytes} byte limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut value: Value = serde_json::from_slice(&bytes)
            .context("Website3 returned an invalid Manual Swarm JSON response")?;
        sanitize_remote_value(&mut value, 0)?;
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            bail!("Website3 denied Manual Swarm authorization ({status})");
        }
        if !status.is_success() {
            let message = value
                .get("error")
                .and_then(Value::as_str)
                .or_else(|| value.get("message").and_then(Value::as_str))
                .unwrap_or("Manual Swarm request failed");
            bail!("{message} ({status})");
        }
        ensure_capability(&value)?;
        Ok(value)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ManualToolInput {
    objective: Option<String>,
    workspace: Option<PathBuf>,
    space_id: Option<String>,
    swarm_id: Option<String>,
    source_ref: Option<String>,
    target: Option<String>,
    agents: Option<usize>,
    codex_agents: Option<usize>,
    claude_agents: Option<usize>,
    mode: Option<String>,
    network: Option<String>,
    #[serde(default)]
    allowed_domains: Vec<String>,
    duration_secs: Option<u64>,
    #[serde(default)]
    allowed_subscription_ids: Vec<String>,
    preview_token: Option<String>,
    id: Option<String>,
    worker_id: Option<String>,
    guidance: Option<String>,
    reason: Option<String>,
    kind: Option<String>,
    reference: Option<String>,
    cursor: Option<String>,
    limit: Option<u64>,
    timeout: Option<u64>,
    idempotency_key: Option<String>,
    #[serde(default)]
    ordered_change_refs: Vec<String>,
    #[serde(default)]
    required_checks: Vec<String>,
    confirm: Option<bool>,
}

async fn git_output(workspace: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn canonical_repo(workspace: &Path) -> Result<PathBuf> {
    if !workspace.is_absolute() {
        bail!("workspace must be an absolute path");
    }
    let canonical = tokio::fs::canonicalize(workspace)
        .await
        .context("workspace does not exist")?;
    let root = git_output(&canonical, &["rev-parse", "--show-toplevel"]).await?;
    tokio::fs::canonicalize(root)
        .await
        .context("failed to canonicalize repository root")
}

fn validate_commit(value: &str) -> Result<()> {
    if !(40..=64).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("base commit is not a full object identifier");
    }
    Ok(())
}

fn target_for(config: &ManualSwarmConfig, requested: Option<&str>) -> Result<String> {
    let target = requested.unwrap_or(&config.default_target);
    if !config.allowed_targets.contains(target) {
        bail!("target '{target}' is not allowed by this Shunt installation");
    }
    Ok(target.to_owned())
}

fn control_url_is_loopback(control_url: &str) -> bool {
    url::Url::parse(control_url)
        .ok()
        .and_then(|url| url.host_str().map(ToOwned::to_owned))
        .is_some_and(|host| matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1" | "[::1]"))
}

fn provider_mix(input: &ManualToolInput, agents: usize) -> Result<(usize, usize)> {
    match (input.codex_agents, input.claude_agents) {
        (None, None) => Ok((agents.div_ceil(2), agents / 2)),
        (codex, claude) => {
            let codex = codex.unwrap_or(agents.saturating_sub(claude.unwrap_or(0)));
            let claude = claude.unwrap_or(agents.saturating_sub(codex));
            if codex.saturating_add(claude) != agents {
                bail!("codexAgents plus claudeAgents must equal agents");
            }
            Ok((codex, claude))
        }
    }
}

fn validate_response_target(
    value: &Value,
    requested: &str,
    config: &ManualSwarmConfig,
) -> Result<()> {
    let resolved = value
        .get("target")
        .or_else(|| value.pointer("/preview/target"))
        .or_else(|| value.pointer("/session/target"))
        .or_else(|| value.pointer("/session/spec/target"))
        .and_then(Value::as_str)
        .context("Manual Swarm response omitted its authorized target")?;
    if !config.allowed_targets.contains(resolved) {
        bail!("server selected a target outside the local allowlist");
    }
    if requested != "auto" && resolved != requested {
        bail!("server changed the explicitly requested target; refusing fallback");
    }
    if requested == "auto" && resolved == "local" {
        bail!("hosted target selection silently fell back to local; refusing launch");
    }
    Ok(())
}

async fn capabilities(client: &ManualClient) -> Result<Value> {
    let response = client
        .request(Method::GET, "/capabilities", &[], None, MAX_RESPONSE_BYTES)
        .await?;
    if response.get("enabled").and_then(Value::as_bool) != Some(true) {
        bail!("Manual Swarm is disabled by Website3/Fabric");
    }
    if !matches!(
        response.get("api_version").and_then(Value::as_str),
        Some("autoswarm.manual-swarm/v1" | "v1" | "1")
    ) {
        bail!("Manual Swarm server advertised an incompatible api_version");
    }
    let max_workers = response
        .get("max_workers")
        .and_then(Value::as_u64)
        .context("Manual Swarm capabilities omitted max_workers")?;
    if max_workers == 0 || max_workers > crate::config::MANUAL_SWARM_MAX_AGENTS as u64 {
        bail!("Manual Swarm server advertised an invalid worker ceiling");
    }
    let operations = response
        .get("operations")
        .and_then(Value::as_array)
        .context("Manual Swarm capabilities omitted operations")?;
    for required in [
        "preview",
        "start",
        "status",
        "events",
        "inspect",
        "steer",
        "cancel",
        "review",
        "integrate",
        "cleanup",
    ] {
        if !operations
            .iter()
            .any(|operation| operation.as_str() == Some(required))
        {
            bail!("Manual Swarm server does not advertise required operation '{required}'");
        }
    }
    let targets = response
        .get("targets")
        .and_then(Value::as_array)
        .context("Manual Swarm capabilities omitted targets")?;
    if targets.is_empty()
        || targets.len() > 32
        || targets.iter().any(|target| {
            target.as_str().is_none_or(|target| {
                target.is_empty()
                    || target.len() > 64
                    || !target.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            })
        })
    {
        bail!("Manual Swarm server advertised invalid targets");
    }
    Ok(response)
}

async fn plan(
    client: &ManualClient,
    network_ceiling: NetworkPolicy,
    input: &ManualToolInput,
) -> Result<Value> {
    let objective = input
        .objective
        .as_deref()
        .context("objective is required")?;
    validate_text(objective, "objective", MAX_OBJECTIVE_BYTES)?;
    let workspace = canonical_repo(
        input
            .workspace
            .as_deref()
            .context("workspace is required")?,
    )
    .await?;
    let base_commit = git_output(&workspace, &["rev-parse", "HEAD"]).await?;
    validate_commit(&base_commit)?;
    let target = target_for(&client.config, input.target.as_deref())?;
    if target == "local" && !control_url_is_loopback(&client.config.control_url) {
        bail!("target=local requires a loopback Manual Swarm control plane; refusing to disclose a local workspace path remotely");
    }
    let agents = input.agents.unwrap_or(client.config.default_agents);
    if agents == 0 || agents > client.config.max_agents {
        bail!("agents must be between 1 and {}", client.config.max_agents);
    }
    let advertised = capabilities(client).await?;
    let remote_max = advertised
        .get("max_workers")
        .and_then(Value::as_u64)
        .context("Manual Swarm capabilities omitted max_workers")? as usize;
    if agents > remote_max {
        bail!("agents exceeds the server-advertised maximum of {remote_max}");
    }
    let targets = advertised
        .get("targets")
        .and_then(Value::as_array)
        .context("Manual Swarm capabilities omitted targets")?;
    if target != "auto" && !targets.iter().any(|item| item.as_str() == Some(&target)) {
        bail!("requested target is not advertised by Website3/Fabric");
    }
    let (codex, claude) = provider_mix(input, agents)?;
    let duration = input
        .duration_secs
        .unwrap_or(client.config.default_duration_secs);
    if duration < 60 || duration > client.config.max_duration_secs {
        bail!(
            "durationSecs must be between 60 and {}",
            client.config.max_duration_secs
        );
    }
    let remote_max_duration = advertised
        .get("maximum_duration_seconds")
        .and_then(Value::as_u64)
        .context("Manual Swarm capabilities omitted maximum_duration_seconds")?;
    if duration > remote_max_duration {
        bail!("durationSecs exceeds the server-advertised maximum of {remote_max_duration}");
    }
    let mode = input.mode.as_deref().unwrap_or("patch");
    if !matches!(mode, "plan" | "patch") {
        bail!("Manual Swarm mode must be plan or patch; apply is always a separate local action");
    }
    let requested_network = input.network.as_deref().unwrap_or(if target == "local" {
        "unrestricted"
    } else {
        "restricted"
    });
    let network = match requested_network {
        "none" => NetworkPolicy::None,
        "restricted" => NetworkPolicy::Allowlisted,
        "unrestricted" => NetworkPolicy::Unrestricted,
        _ => bail!("network must be none, restricted, or unrestricted"),
    };
    if !network_ceiling.permits(network) {
        bail!("requested network policy exceeds the Shunt operator ceiling");
    }
    if !input.allowed_domains.is_empty() {
        bail!(
            "per-domain Manual Swarm allowlists are not runtime-enforced and cannot be requested"
        );
    }
    let space_id = input.space_id.as_deref().context("spaceId is required")?;
    validate_scope_identifier(space_id, "spaceId")?;
    let swarm_id = input.swarm_id.as_deref().context("swarmId is required")?;
    validate_scope_identifier(swarm_id, "swarmId")?;
    if input.allowed_subscription_ids.is_empty()
        || input.allowed_subscription_ids.len() > MAX_SUBSCRIPTION_IDS
    {
        bail!("allowedSubscriptionIds must contain 1-{MAX_SUBSCRIPTION_IDS} accounts");
    }
    for id in &input.allowed_subscription_ids {
        validate_scope_identifier(id, "allowedSubscriptionIds entry")?;
    }

    let source_ref = if target == "local" {
        workspace.to_string_lossy().into_owned()
    } else if let Some(source_ref) = input.source_ref.clone() {
        source_ref
    } else {
        git_output(&workspace, &["remote", "get-url", "origin"])
            .await
            .context("hosted Manual Swarm requires sourceRef or a credential-free origin remote")?
    };
    validate_source_ref(&source_ref)?;
    if target != "local" {
        validate_hosted_source_ref(&source_ref)?;
    }

    let mut body = json!({
        "objective": objective,
        "space_id": space_id,
        "swarm_id": swarm_id,
        "source_ref": source_ref,
        "base_commit": base_commit,
        "target": target,
        "mode": mode,
        "requested_agents": agents,
        "provider_mix": {"codex": codex, "claude": claude},
        "allowed_subscription_ids": input.allowed_subscription_ids,
        "network_policy": requested_network,
        "maximum_duration_secs": duration,
    });
    let key = idempotency_key(input.idempotency_key.as_deref(), "preview", &body)?;
    body["idempotency_key"] = Value::String(key);
    let response = client
        .request(
            Method::POST,
            "/preview",
            &[],
            Some(&body),
            MAX_RESPONSE_BYTES,
        )
        .await?;
    validate_response_target(&response, &target, &client.config)?;
    let preview_token = response
        .get("preview_token")
        .or_else(|| response.pointer("/preview/token"))
        .and_then(Value::as_str)
        .context("Manual Swarm preview omitted preview_token")?;
    validate_opaque(preview_token, "preview_token", MAX_TOKEN_BYTES)?;
    let expires_at = response
        .get("preview_expires_at")
        .or_else(|| response.pointer("/preview/expires_at"))
        .and_then(Value::as_u64)
        .context("Manual Swarm preview omitted numeric preview_expires_at")?;
    if expires_at <= now_secs() || expires_at > now_secs().saturating_add(15 * 60) {
        bail!("Manual Swarm preview expiry is invalid or exceeds the 15-minute local ceiling");
    }
    remember_preview(
        preview_token,
        PreviewBinding {
            workspace: workspace.clone(),
            base_commit: base_commit.clone(),
            target: response
                .get("target")
                .or_else(|| response.pointer("/preview/target"))
                .and_then(Value::as_str)
                .unwrap_or(&target)
                .to_owned(),
            expires_at,
        },
    )?;
    let mut output = response;
    output["workspace"] = Value::String(workspace.to_string_lossy().into_owned());
    output["base_commit"] = Value::String(base_commit);
    output["execution_runtime"] = Value::String("go_native".into());
    Ok(output)
}

async fn start(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let token = input
        .preview_token
        .as_deref()
        .context("previewToken is required")?;
    validate_opaque(token, "previewToken", MAX_TOKEN_BYTES)?;
    let binding = preview_binding(token)?;
    let mut body = json!({"preview_token": token});
    body["idempotency_key"] = Value::String(idempotency_key(
        input.idempotency_key.as_deref(),
        "start",
        &body,
    )?);
    let response = client
        .request(Method::POST, "/start", &[], Some(&body), MAX_RESPONSE_BYTES)
        .await?;
    validate_response_target(&response, &binding.target, &client.config)?;
    let id = response
        .pointer("/session/id")
        .or_else(|| response.get("id"))
        .and_then(Value::as_str)
        .context("Manual Swarm start response omitted session id")?;
    validate_identifier(id, "session id")?;
    if let Some(base) = response
        .pointer("/session/base_commit")
        .or_else(|| response.pointer("/session/spec/base_commit"))
        .or_else(|| response.get("base_commit"))
        .and_then(Value::as_str)
    {
        if base != binding.base_commit {
            bail!("Manual Swarm start response changed the approved base commit");
        }
    }
    remember_session(id, &binding)?;
    Ok(response)
}

async fn status(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    validate_identifier(id, "session id")?;
    let response = client
        .request(
            Method::GET,
            &format!("/{id}"),
            &[],
            None,
            MAX_RESPONSE_BYTES,
        )
        .await?;
    let returned = response
        .get("id")
        .or_else(|| response.pointer("/session/id"))
        .and_then(Value::as_str)
        .context("Manual Swarm status omitted session id")?;
    if returned != id {
        bail!("Manual Swarm status returned a different session id");
    }
    Ok(response)
}

async fn wait(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    validate_identifier(id, "session id")?;
    let limit = input.limit.unwrap_or(50).clamp(1, MAX_EVENT_LIMIT);
    let timeout = input.timeout.unwrap_or(60).clamp(1, MAX_WAIT_SECS);
    let mut cursor = input.cursor.clone().unwrap_or_default();
    if !cursor.is_empty() {
        validate_opaque(&cursor, "cursor", MAX_CURSOR_BYTES)?;
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout);
    loop {
        let query = [("cursor", cursor.clone()), ("limit", limit.to_string())];
        let response = client
            .request(
                Method::GET,
                &format!("/{id}/events"),
                &query,
                None,
                MAX_RESPONSE_BYTES,
            )
            .await?;
        let has_events = response
            .get("events")
            .and_then(Value::as_array)
            .is_some_and(|events| !events.is_empty());
        let terminal = response
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_cursor = response
            .get("next_cursor")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !next_cursor.is_empty() {
            validate_opaque(next_cursor, "next_cursor", MAX_CURSOR_BYTES)?;
        }
        if has_events || terminal || tokio::time::Instant::now() >= deadline {
            return Ok(response);
        }
        cursor = next_cursor.to_owned();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
}

async fn inspect(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    validate_identifier(id, "session id")?;
    let kind = input.kind.as_deref().context("kind is required")?;
    if !matches!(kind, "worker" | "proposal") {
        bail!("unsupported inspect kind");
    }
    let reference = input.reference.as_deref().unwrap_or("");
    let body = match kind {
        "worker" => {
            if reference.is_empty() {
                bail!("worker inspect requires reference");
            }
            validate_identifier(reference, "worker reference")?;
            json!({"kind":"worker","worker_id":reference})
        }
        "proposal" => {
            if !reference.is_empty() {
                bail!("proposal inspect does not accept reference");
            }
            json!({"kind":"proposal"})
        }
        _ => unreachable!("inspect kind validated above"),
    };
    let response = client
        .request(
            Method::POST,
            &format!("/{id}/inspect"),
            &[],
            Some(&body),
            MAX_INSPECT_BYTES,
        )
        .await?;
    if contains_embedded_content(&response) {
        bail!("Manual Swarm inspect returns redacted metadata only; embedded artifact content is not exposed");
    }
    if let Some(returned) = response
        .get("id")
        .or_else(|| response.get("session_id"))
        .or_else(|| response.pointer("/record/session_id"))
        .or_else(|| response.pointer("/record/spec/session_id"))
        .and_then(Value::as_str)
    {
        if returned != id {
            bail!("Manual Swarm operation returned a different session id");
        }
    }
    Ok(response)
}

async fn write_operation(
    client: &ManualClient,
    id: &str,
    operation: &str,
    input_key: Option<&str>,
    mut body: Value,
) -> Result<Value> {
    validate_identifier(id, "session id")?;
    body["idempotency_key"] = Value::String(idempotency_key(
        input_key,
        &format!("{id}/{operation}"),
        &body,
    )?);
    let response = client
        .request(
            Method::POST,
            &format!("/{id}/{operation}"),
            &[],
            Some(&body),
            MAX_RESPONSE_BYTES,
        )
        .await?;
    if let Some(returned) = response
        .get("id")
        .or_else(|| response.get("session_id"))
        .or_else(|| response.pointer("/session/id"))
        .or_else(|| response.pointer("/proposal/session_id"))
        .or_else(|| response.pointer("/record/spec/session_id"))
        .or_else(|| response.pointer("/record/session_id"))
        .and_then(Value::as_str)
    {
        if returned != id {
            bail!("Manual Swarm operation returned a different session id");
        }
    }
    Ok(response)
}

async fn steer(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    let worker = input.worker_id.as_deref().context("workerId is required")?;
    validate_identifier(worker, "workerId")?;
    let guidance = input.guidance.as_deref().context("guidance is required")?;
    validate_text(guidance, "guidance", MAX_GUIDANCE_BYTES)?;
    write_operation(
        client,
        id,
        "steer",
        input.idempotency_key.as_deref(),
        json!({"worker_id": worker, "guidance": guidance}),
    )
    .await
}

async fn cancel(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    if let Some(worker) = input.worker_id.as_deref() {
        validate_identifier(worker, "workerId")?;
    }
    if let Some(reason) = input.reason.as_deref() {
        validate_text(reason, "reason", 1024)?;
    }
    let mut body = json!({"worker_id": input.worker_id, "reason": input.reason});
    if input.worker_id.is_none() {
        body.as_object_mut().unwrap().remove("worker_id");
    }
    if input.reason.is_none() {
        body.as_object_mut().unwrap().remove("reason");
    }
    write_operation(client, id, "cancel", input.idempotency_key.as_deref(), body).await
}

async fn review(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    if input.required_checks.len() > 32 {
        bail!("requiredChecks exceeds the 32-check limit");
    }
    for check in &input.required_checks {
        validate_text(check, "requiredChecks entry", 512)?;
    }
    let response = write_operation(
        client,
        id,
        "review",
        input.idempotency_key.as_deref(),
        json!({}),
    )
    .await?;
    validate_requested_checks(&response, &input.required_checks)?;
    Ok(response)
}

async fn cleanup(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    let id = input.id.as_deref().context("id is required")?;
    let response = write_operation(
        client,
        id,
        "cleanup",
        input.idempotency_key.as_deref(),
        json!({}),
    )
    .await?;
    forget_session(id)?;
    Ok(response)
}

#[derive(Debug)]
struct Proposal {
    id: String,
    session_id: String,
    base_commit: String,
    patch_ref: String,
    patch_sha256: String,
    patch_size: usize,
    inline_patch: Option<Vec<u8>>,
    changed_paths: BTreeSet<String>,
}

fn successful_result(value: &Value) -> bool {
    let success = |status: &str| matches!(status, "approved" | "passed" | "success" | "succeeded");
    if let Some(status) = value.as_str() {
        return success(status);
    }
    if let Some(status) = value.get("status").and_then(Value::as_str) {
        return success(status);
    }
    if let Some(verdict) = value.get("verdict").and_then(Value::as_str) {
        return verdict == "approved";
    }
    value.as_bool() == Some(true)
        || value.get("approved").and_then(Value::as_bool) == Some(true)
        || value.get("passed").and_then(Value::as_bool) == Some(true)
        || value.get("satisfied").and_then(Value::as_bool) == Some(true)
}

fn validate_requested_checks(response: &Value, required: &[String]) -> Result<()> {
    if required.is_empty() {
        return Ok(());
    }
    let proposal = response
        .get("proposal")
        .or_else(|| response.get("integration_proposal"))
        .unwrap_or(response);
    let checks = proposal
        .get("checks")
        .and_then(Value::as_array)
        .context("integration proposal omitted checks")?;
    for required_name in required {
        let check = checks
            .iter()
            .find(|check| check.get("name").and_then(Value::as_str) == Some(required_name))
            .with_context(|| format!("required check '{required_name}' is absent"))?;
        if !successful_result(check) {
            bail!("required check '{required_name}' is not passing");
        }
    }
    Ok(())
}

fn parse_proposal(response: &Value) -> Result<Proposal> {
    let value = response
        .get("integration_proposal")
        .or_else(|| response.get("proposal"))
        .unwrap_or(response);
    let id = value
        .get("id")
        .or_else(|| value.get("integration_proposal_ref"))
        .and_then(Value::as_str)
        .context("integration response omitted proposal id")?;
    validate_identifier(id, "proposal id")?;
    let session_id = value
        .get("session_id")
        .and_then(Value::as_str)
        .context("integration proposal omitted session_id")?;
    validate_identifier(session_id, "proposal session_id")?;
    let base_commit = value
        .get("base_commit")
        .and_then(Value::as_str)
        .context("integration proposal omitted base_commit")?;
    validate_commit(base_commit)?;
    let patch_value = value.get("patch").unwrap_or(value);
    let patch_ref = patch_value
        .get("artifact_ref")
        .or_else(|| value.get("combined_patch_ref"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if !patch_ref.is_empty() {
        validate_opaque(patch_ref, "patch artifact_ref", MAX_REFERENCE_BYTES)?;
    }
    let patch_sha256 = patch_value
        .get("sha256")
        .or_else(|| value.get("combined_patch_sha256"))
        .and_then(Value::as_str)
        .context("integration proposal omitted patch sha256")?;
    if patch_sha256.len() != 64 || !patch_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("integration proposal patch hash is invalid");
    }
    let patch_size = patch_value
        .get("size_bytes")
        .and_then(Value::as_u64)
        .context("integration proposal omitted patch size_bytes")? as usize;
    if patch_size == 0 || patch_size > MAX_PATCH_BYTES {
        bail!("integration proposal patch size is invalid or exceeds safety limit");
    }

    let paths = value
        .get("changed_paths")
        .and_then(Value::as_array)
        .context("integration proposal omitted changed_paths")?;
    if paths.is_empty() || paths.len() > 4096 {
        bail!("integration proposal changed_paths is empty or too large");
    }
    let mut changed_paths = BTreeSet::new();
    for path in paths {
        let path = path
            .as_str()
            .context("changed_paths must contain strings")?;
        validate_patch_path(path)?;
        changed_paths.insert(path.to_owned());
    }
    let conflicts = value
        .get("conflict_groups")
        .and_then(Value::as_array)
        .context("integration proposal omitted conflict_groups")?;
    if !conflicts.is_empty() {
        bail!("integration proposal contains unresolved conflicts");
    }
    let findings = value
        .get("unresolved_findings")
        .and_then(Value::as_array)
        .context("integration proposal omitted unresolved_findings")?;
    if !findings.is_empty() {
        bail!("integration proposal contains unresolved findings");
    }
    let checks = value
        .get("checks")
        .and_then(Value::as_array)
        .context("integration proposal omitted checks")?;
    if checks.is_empty() || !checks.iter().all(successful_result) {
        bail!("integration proposal does not have a complete passing check set");
    }
    let check_names: BTreeSet<&str> = checks
        .iter()
        .map(|check| {
            check
                .get("name")
                .and_then(Value::as_str)
                .context("check result omitted name")
        })
        .collect::<Result<_>>()?;
    if check_names.len() != checks.len()
        || check_names
            .iter()
            .any(|name| name.is_empty() || name.len() > 128)
    {
        bail!("integration proposal check names are missing, duplicate, or invalid");
    }
    let reviews = value
        .get("reviews")
        .or_else(|| value.get("review_results"))
        .and_then(Value::as_array)
        .context("integration proposal omitted reviews")?;
    if reviews.is_empty() || !reviews.iter().all(successful_result) {
        bail!("integration proposal lacks independent approval");
    }
    if reviews.iter().any(|review| {
        !matches!(
            review.get("provider").and_then(Value::as_str),
            Some("claude" | "codex")
        )
    }) {
        bail!("integration proposal review provider is missing or invalid");
    }
    let authors: BTreeSet<&str> = value
        .get("author_worker_ids")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|author| {
            author
                .as_str()
                .context("author_worker_ids must contain strings")
        })
        .collect::<Result<_>>()?;
    if authors.is_empty() {
        bail!("integration proposal omitted author identities");
    }
    for author in &authors {
        validate_identifier(author, "author worker id")?;
    }
    let reviewers: BTreeSet<&str> = value
        .get("reviewer_worker_ids")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|reviewer| {
            reviewer
                .as_str()
                .context("reviewer_worker_ids must contain strings")
        })
        .collect::<Result<_>>()?;
    let observed_reviewers: BTreeSet<&str> = reviews
        .iter()
        .map(|review| {
            review
                .get("worker_id")
                .or_else(|| review.get("reviewer_worker_id"))
                .or_else(|| review.get("worker_run_id"))
                .and_then(Value::as_str)
                .context("review result omitted worker_id")
        })
        .collect::<Result<_>>()?;
    if reviewers.is_empty() || reviewers != observed_reviewers {
        bail!("integration proposal reviewer identities are missing or inconsistent");
    }
    for reviewer in reviewers {
        validate_identifier(reviewer, "reviewer worker id")?;
        if authors.contains(reviewer) {
            bail!("a worker cannot approve its own Manual Swarm change");
        }
    }
    let preconditions = value
        .get("apply_preconditions")
        .context("integration proposal omitted apply_preconditions")?;
    let preconditions = preconditions
        .as_object()
        .context("integration proposal apply_preconditions must be an object")?;
    if preconditions.get("base_commit").and_then(Value::as_str) != Some(base_commit) {
        bail!("integration proposal apply precondition base does not match proposal base");
    }
    let clean = preconditions
        .get("clean_paths")
        .context("integration proposal apply_preconditions omitted clean_paths")?;
    match clean {
        Value::Bool(true) => {}
        Value::Array(paths) => {
            let clean_paths: BTreeSet<&str> = paths
                .iter()
                .map(|path| {
                    path.as_str()
                        .context("apply_preconditions.clean_paths must contain strings")
                })
                .collect::<Result<_>>()?;
            if clean_paths != changed_paths.iter().map(String::as_str).collect() {
                bail!("integration proposal clean_paths do not match changed_paths");
            }
        }
        _ => bail!("integration proposal clean_paths precondition is not satisfied"),
    }
    let inline_patch = patch_value
        .get("inline_base64")
        .and_then(Value::as_str)
        .map(|encoded| {
            if encoded.len() > 128 * 1024 {
                bail!("inline proposal patch exceeds safety limit");
            }
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .context("inline proposal patch is not valid base64")
        })
        .transpose()?;
    if inline_patch.is_none() && patch_ref.is_empty() {
        bail!("integration proposal omitted both inline patch and artifact_ref");
    }

    Ok(Proposal {
        id: id.to_owned(),
        session_id: session_id.to_owned(),
        base_commit: base_commit.to_owned(),
        patch_ref: patch_ref.to_owned(),
        patch_sha256: patch_sha256.to_ascii_lowercase(),
        patch_size,
        inline_patch,
        changed_paths,
    })
}

fn validate_patch_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 4096
        || value.contains('\0')
        || Path::new(value).is_absolute()
    {
        bail!("proposal contains an unsafe changed path");
    }
    let mut components = Path::new(value).components();
    let first = components
        .next()
        .context("proposal contains an empty changed path")?;
    if !matches!(first, Component::Normal(_)) {
        bail!("proposal contains an unsafe changed path");
    }
    if components.any(|component| !matches!(component, Component::Normal(_))) {
        bail!("proposal contains an unsafe changed path");
    }
    if Path::new(value)
        .components()
        .any(|component| component.as_os_str() == ".git")
    {
        bail!("proposal may not change Git metadata");
    }
    if matches!(
        Path::new(value).file_name().and_then(|name| name.to_str()),
        Some(".env" | ".env.local" | ".npmrc" | ".pypirc" | "credentials.json")
    ) {
        bail!("proposal may not change local credential files");
    }
    Ok(())
}

fn patch_contains_secret_material(patch: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(patch) else {
        return true;
    };
    ["sk-ant-", "sk-proj-", "ghp_", "github_pat_", "npm_", "dp.st.", "ya29."]
        .iter().any(|prefix| text.contains(prefix))
        || SENSITIVE_ASSIGNMENT.get_or_init(|| regex::Regex::new(
            r#"(?i)(authorization|cookie|openai_api_key|anthropic_api_key|doppler_token|npm_token|node_auth_token)\s*[:=]\s*[^\s,}\]"']+"#
        ).expect("static secret redaction regex must compile")).is_match(text)
}

async fn fetch_patch(
    client: &ManualClient,
    session_id: &str,
    proposal: &Proposal,
) -> Result<Vec<u8>> {
    // Patch refs are opaque. Shunt never dereferences file://, http(s), or any
    // other server-provided URI; bytes must return through this same-origin,
    // Website3-authenticated inspect route.
    let patch = if let Some(inline) = proposal.inline_patch.clone() {
        inline
    } else {
        let response = client
            .request(
                Method::POST,
                &format!("/{session_id}/inspect"),
                &[],
                Some(&json!({"kind":"combined_patch","ref":proposal.patch_ref})),
                MAX_PATCH_BYTES.saturating_mul(2),
            )
            .await?;
        let artifact = response
            .get("artifact")
            .context("proposal patch inspect response omitted artifact")?;
        if artifact.get("sha256").and_then(Value::as_str) != Some(&proposal.patch_sha256)
            || artifact.get("size_bytes").and_then(Value::as_u64)
                != Some(proposal.patch_size as u64)
        {
            bail!("proposal patch artifact metadata does not match the reviewed proposal");
        }
        let encoded = artifact
            .get("content_base64")
            .and_then(Value::as_str)
            .context("proposal patch inspect response omitted artifact.content_base64")?;
        if encoded.len() > MAX_PATCH_BYTES.saturating_mul(2) {
            bail!("encoded proposal patch exceeds safety limit");
        }
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .context("proposal patch is not valid base64")?
    };
    if patch.is_empty() || patch.len() > MAX_PATCH_BYTES {
        bail!("proposal patch is empty or exceeds safety limit");
    }
    if patch.len() != proposal.patch_size {
        bail!("proposal patch size does not match the reviewed proposal");
    }
    if patch.contains(&0) {
        bail!("proposal patch contains NUL bytes");
    }
    if patch_contains_secret_material(&patch) {
        bail!("proposal patch contains credential-shaped material; refusing local apply");
    }
    let actual = hex::encode(Sha256::digest(&patch));
    if actual != proposal.patch_sha256 {
        bail!("proposal patch hash does not match the reviewed proposal");
    }
    Ok(patch)
}

struct ApplyLock(PathBuf);
impl Drop for ApplyLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn acquire_apply_lock(path: &Path) -> Result<ApplyLock> {
    let create = || {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    };
    match create() {
        Ok(_) => Ok(ApplyLock(path.to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let stale = std::fs::metadata(path)
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > std::time::Duration::from_secs(10 * 60));
            if !stale {
                bail!("another Shunt patch apply is in progress");
            }
            std::fs::remove_file(path).context("failed to remove stale Shunt apply lock")?;
            create().context("another Shunt patch apply is in progress")?;
            Ok(ApplyLock(path.to_owned()))
        }
        Err(error) => Err(error).context("failed to create Shunt apply lock"),
    }
}

fn paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn patch_paths(repo: &Path, patch_path: &Path) -> Result<BTreeSet<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["apply", "--numstat", "-z"])
        .arg(patch_path)
        .stdin(Stdio::null())
        .output()
        .await?;
    if !output.status.success() {
        bail!("proposal patch is not a valid Git patch");
    }
    let mut paths = BTreeSet::new();
    let records: Vec<&[u8]> = output.stdout.split(|byte| *byte == 0).collect();
    let mut index = 0;
    while index < records.len() {
        let record = records[index];
        index += 1;
        if record.is_empty() {
            continue;
        }
        let record = std::str::from_utf8(record).context("proposal patch path is not UTF-8")?;
        let path = record
            .splitn(3, '\t')
            .nth(2)
            .context("proposal patch numstat is malformed")?;
        if path.is_empty() {
            // With --numstat -z, a rename/copy uses an empty pathname in the
            // count record followed by NUL-separated old and new paths.
            for _ in 0..2 {
                let renamed = records
                    .get(index)
                    .context("proposal rename record is incomplete")?;
                index += 1;
                let renamed =
                    std::str::from_utf8(renamed).context("proposal rename path is not UTF-8")?;
                validate_patch_path(renamed)?;
                paths.insert(renamed.to_owned());
            }
        } else {
            validate_patch_path(path)?;
            paths.insert(path.to_owned());
        }
    }
    if paths.is_empty() {
        bail!("proposal patch contains no file changes");
    }
    Ok(paths)
}

async fn dirty_paths(repo: &Path) -> Result<BTreeSet<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["status", "--porcelain=v1", "-z", "--untracked-files=all"])
        .stdin(Stdio::null())
        .output()
        .await?;
    if !output.status.success() {
        bail!("failed to inspect parent checkout");
    }
    let mut paths = BTreeSet::new();
    let records: Vec<&[u8]> = output.stdout.split(|byte| *byte == 0).collect();
    let mut index = 0;
    while index < records.len() {
        let record = records[index];
        index += 1;
        if record.is_empty() {
            continue;
        }
        let text = std::str::from_utf8(record).context("dirty path is not UTF-8")?;
        let status = text.get(..2).unwrap_or("");
        let path = text.get(3..).unwrap_or(text);
        validate_patch_path(path)?;
        paths.insert(path.to_owned());
        if status.contains('R') || status.contains('C') {
            if let Some(next) = records.get(index).filter(|next| !next.is_empty()) {
                let next = std::str::from_utf8(next).context("dirty rename path is not UTF-8")?;
                validate_patch_path(next)?;
                paths.insert(next.to_owned());
                index += 1;
            }
        }
    }
    Ok(paths)
}

async fn apply_patch(binding: &SessionBinding, proposal: &Proposal, patch: &[u8]) -> Result<Value> {
    if proposal.base_commit != binding.base_commit {
        bail!("proposal base does not match the locally approved session base");
    }
    let repo = canonical_repo(&binding.workspace).await?;
    if repo != binding.workspace {
        bail!("locally bound workspace no longer resolves to the same repository");
    }
    let common_git_dir = git_output(&repo, &["rev-parse", "--git-common-dir"]).await?;
    let common_git_dir = if Path::new(&common_git_dir).is_absolute() {
        PathBuf::from(common_git_dir)
    } else {
        repo.join(common_git_dir)
    };
    let _lock = acquire_apply_lock(&common_git_dir.join("shunt-apply.lock"))?;

    // Persist verified bytes under a Shunt-owned private directory so neither
    // a shared /tmp symlink nor a server-controlled path participates in apply.
    let patch_dir = common_git_dir.join("shunt-manual-proposals");
    std::fs::create_dir_all(&patch_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&patch_dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let patch_path = patch_dir.join(format!("{}.patch", proposal.id));
    let patch_tmp = patch_dir.join(format!(".{}.{}.tmp", proposal.id, uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&patch_tmp)?;
    file.write_all(patch)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&patch_tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(patch_tmp, &patch_path)?;

    let derived_paths = patch_paths(&repo, &patch_path).await?;
    if derived_paths != proposal.changed_paths {
        bail!("reviewed changed_paths do not match the verified patch");
    }
    let head = git_output(&repo, &["rev-parse", "HEAD"]).await?;
    if head != binding.base_commit {
        bail!("parent HEAD changed; proposal retained without applying");
    }
    let dirty = dirty_paths(&repo).await?;
    let collisions: Vec<String> = dirty
        .iter()
        .filter(|dirty| {
            derived_paths
                .iter()
                .any(|changed| paths_overlap(dirty, changed))
        })
        .cloned()
        .collect();
    if !collisions.is_empty() {
        bail!(
            "proposal overlaps dirty parent paths: {}",
            collisions.join(", ")
        );
    }

    let check = Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["apply", "--check"])
        .arg(&patch_path)
        .stdin(Stdio::null())
        .output()
        .await?;
    if !check.status.success() {
        bail!("git apply --check failed; proposal retained without applying");
    }
    // Recheck HEAD immediately before the mutation while holding the shared
    // repository apply lock.
    if git_output(&repo, &["rev-parse", "HEAD"]).await? != binding.base_commit {
        bail!("parent HEAD changed during apply validation; proposal retained");
    }
    let applied = Command::new("git")
        .arg("-C")
        .arg(&repo)
        .arg("apply")
        .arg(&patch_path)
        .stdin(Stdio::null())
        .output()
        .await?;
    if !applied.status.success() {
        bail!("git apply failed; proposal retained");
    }
    Ok(json!({
        "capability_version": MANUAL_SWARM_CAPABILITY_VERSION,
        "id": proposal.id,
        "status": "applied",
        "base_commit": binding.base_commit,
        "patch_sha256": proposal.patch_sha256,
        "changed_paths": proposal.changed_paths,
    }))
}

async fn apply(client: &ManualClient, input: &ManualToolInput) -> Result<Value> {
    if client.config.apply_policy != ManualSwarmApplyPolicy::Explicit {
        bail!("Manual Swarm apply is disabled by operator policy");
    }
    if input.confirm != Some(true) {
        bail!("confirm=true is required for the separate explicit apply operation");
    }
    let id = input.id.as_deref().context("id is required")?;
    validate_identifier(id, "session id")?;
    let binding = session_binding(id)?;
    if !input.ordered_change_refs.is_empty() {
        bail!("orderedChangeRefs is unsupported; the reviewed proposal's canonical patch order is authoritative");
    }
    if input.required_checks.len() > 32 {
        bail!("requiredChecks exceeds the 32-check limit");
    }
    for check in &input.required_checks {
        validate_text(check, "requiredChecks entry", 512)?;
    }
    let integration = write_operation(
        client,
        id,
        "integrate",
        input.idempotency_key.as_deref(),
        json!({}),
    )
    .await?;
    validate_requested_checks(&integration, &input.required_checks)?;
    let proposal = parse_proposal(&integration)?;
    if proposal.session_id != id {
        bail!("integration proposal belongs to a different Manual Swarm session");
    }
    let patch = fetch_patch(client, id, &proposal).await?;
    apply_patch(&binding, &proposal, &patch).await
}

pub fn is_manual_tool(name: &str) -> bool {
    matches!(
        name,
        "manual_swarm_capabilities"
            | "manual_swarm_plan"
            | "manual_swarm_start"
            | "manual_swarm_status"
            | "manual_swarm_wait"
            | "manual_swarm_inspect"
            | "manual_swarm_steer"
            | "manual_swarm_cancel"
            | "manual_swarm_review"
            | "manual_swarm_apply"
            | "manual_swarm_cleanup"
    )
}

pub async fn dispatch(
    name: &str,
    arguments: Value,
    depth: u8,
    config_path: Option<&Path>,
) -> Result<Value> {
    if depth != 0 {
        bail!("Manual Swarm may be started only by the parent coding session");
    }
    let (manual_config, network_ceiling) = load_manual_swarm_config(config_path)?;
    if !manual_config.enabled {
        bail!("Manual Swarm is disabled; enable [manual_swarm] explicitly");
    }
    let session = crate::website::load_session()?;
    let client = ManualClient::new(manual_config, session.access_token)?;
    let input: ManualToolInput =
        serde_json::from_value(arguments).context("invalid Manual Swarm tool arguments")?;
    match name {
        "manual_swarm_capabilities" => capabilities(&client).await,
        "manual_swarm_plan" => plan(&client, network_ceiling, &input).await,
        "manual_swarm_start" => start(&client, &input).await,
        "manual_swarm_status" => status(&client, &input).await,
        "manual_swarm_wait" => wait(&client, &input).await,
        "manual_swarm_inspect" => inspect(&client, &input).await,
        "manual_swarm_steer" => steer(&client, &input).await,
        "manual_swarm_cancel" => cancel(&client, &input).await,
        "manual_swarm_review" => review(&client, &input).await,
        "manual_swarm_apply" => apply(&client, &input).await,
        "manual_swarm_cleanup" => cleanup(&client, &input).await,
        _ => bail!("unknown Manual Swarm tool"),
    }
}

pub fn tool_definitions() -> Vec<Value> {
    vec![
        json!({"name":"manual_swarm_capabilities","description":"Verify Website3/Fabric Manual Swarm capability, limits, operations, and targets.","inputSchema":{"type":"object","properties":{}}}),
        json!({"name":"manual_swarm_plan","description":"Preview a bounded go_native Manual Swarm session without starting workers.","inputSchema":{"type":"object","properties":{"objective":{"type":"string","maxLength":MAX_OBJECTIVE_BYTES},"workspace":{"type":"string"},"spaceId":{"type":"string"},"swarmId":{"type":"string"},"sourceRef":{"type":"string"},"target":{"type":"string"},"agents":{"type":"integer","minimum":1},"codexAgents":{"type":"integer","minimum":0},"claudeAgents":{"type":"integer","minimum":0},"mode":{"type":"string","enum":["plan","patch"]},"network":{"type":"string","enum":["none","restricted","unrestricted"]},"durationSecs":{"type":"integer","minimum":60,"maximum":3600},"allowedSubscriptionIds":{"type":"array","minItems":1,"maxItems":MAX_SUBSCRIPTION_IDS,"items":{"type":"string"}},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["objective","workspace","spaceId","swarmId","allowedSubscriptionIds"]}}),
        json!({"name":"manual_swarm_start","description":"Start the exact user-approved Manual Swarm preview. Never applies changes locally.","inputSchema":{"type":"object","properties":{"previewToken":{"type":"string","maxLength":MAX_TOKEN_BYTES},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["previewToken"]}}),
        json!({"name":"manual_swarm_status","description":"Get compact current Manual Swarm session and worker state.","inputSchema":session_schema()}),
        json!({"name":"manual_swarm_wait","description":"Wait for reconnectable Manual Swarm events or terminal state.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"cursor":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":MAX_EVENT_LIMIT},"timeout":{"type":"integer","minimum":1,"maximum":MAX_WAIT_SECS}},"required":["id"]}}),
        json!({"name":"manual_swarm_inspect","description":"Inspect one redacted worker record or integration proposal.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"kind":{"type":"string","enum":["worker","proposal"]},"reference":{"type":"string","description":"Required worker id for kind=worker; omitted for kind=proposal"}},"required":["id","kind"]}}),
        json!({"name":"manual_swarm_steer","description":"Deliver bounded guidance to one active worker at a safe boundary.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"workerId":{"type":"string"},"guidance":{"type":"string","maxLength":MAX_GUIDANCE_BYTES},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["id","workerId","guidance"]}}),
        json!({"name":"manual_swarm_cancel","description":"Cancel one worker or the whole Manual Swarm session while retaining bounded artifacts.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"workerId":{"type":"string"},"reason":{"type":"string","maxLength":1024},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["id"]}}),
        json!({"name":"manual_swarm_review","description":"Request independent review and require named passing checks; this does not apply changes.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"requiredChecks":{"type":"array","items":{"type":"string"},"maxItems":32},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["id"]}}),
        json!({"name":"manual_swarm_apply","description":"Explicitly integrate, revalidate, and apply an independently reviewed proposal to its bound parent checkout.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"requiredChecks":{"type":"array","items":{"type":"string"},"maxItems":32},"confirm":{"type":"boolean","const":true},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["id","confirm"]}}),
        json!({"name":"manual_swarm_cleanup","description":"Explicitly release an expired, interrupted, or completed Manual Swarm session.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"idempotencyKey":{"type":"string","minLength":16,"maxLength":128}},"required":["id"]}}),
    ]
}

fn session_schema() -> Value {
    json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"]})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_apply_fixture(label: &str) -> (PathBuf, String, Vec<u8>) {
        let repo =
            std::env::temp_dir().join(format!("shunt-manual-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&repo).unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
            output.stdout
        };
        git(&["init", "-q"]);
        std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
        git(&["add", "README.md"]);
        git(&[
            "-c",
            "user.name=Shunt Test",
            "-c",
            "user.email=shunt@example.test",
            "commit",
            "-qm",
            "fixture",
        ]);
        let base = String::from_utf8(git(&["rev-parse", "HEAD"]))
            .unwrap()
            .trim()
            .to_owned();
        std::fs::write(repo.join("README.md"), "manual swarm change\n").unwrap();
        let patch = git(&["diff", "--binary", "HEAD"]);
        std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
        (repo, base, patch)
    }

    fn valid_proposal(patch: &[u8]) -> Value {
        json!({
            "id": "proposal_1",
            "session_id": "session_1",
            "base_commit": "a".repeat(40),
            "patch": {"artifact_ref":"artifact_1","sha256":hex::encode(Sha256::digest(patch)),"size_bytes":patch.len()},
            "changed_paths": ["src/lib.rs"],
            "conflict_groups": [],
            "checks": [{"name":"unit","status":"passed"}],
            "reviews": [{"status":"approved","worker_id":"reviewer_2","provider":"claude"}],
            "author_worker_ids": ["worker_1"],
            "reviewer_worker_ids": ["reviewer_2"],
            "unresolved_findings": [],
            "apply_preconditions": {"base_commit":"a".repeat(40),"clean_paths":["src/lib.rs"]},
        })
    }

    #[test]
    fn bounds_and_identifiers_fail_closed() {
        assert!(validate_identifier("../escape", "id").is_err());
        assert!(validate_identifier(&"a".repeat(161), "id").is_err());
        assert!(validate_scope_identifier("space/team-a", "space").is_ok());
        assert!(validate_scope_identifier("../escape", "space").is_err());
        assert!(validate_opaque("cursor\nheader", "cursor", 128).is_err());
        assert!(validate_text(
            &"x".repeat(MAX_OBJECTIVE_BYTES + 1),
            "objective",
            MAX_OBJECTIVE_BYTES
        )
        .is_err());
        assert!(validate_source_ref("https://token@example.test/repo").is_err());
        assert!(validate_source_ref("https://example.test/repo?token=secret").is_err());
        assert!(validate_hosted_source_ref("/local/repository").is_err());
        assert!(validate_hosted_source_ref("file:///local/repository").is_err());
        assert!(validate_hosted_source_ref("git@github.com:owner/repo.git").is_ok());
    }

    #[test]
    fn target_selection_never_silently_falls_back() {
        let config = ManualSwarmConfig::default();
        assert!(!control_url_is_loopback(&config.control_url));
        assert!(control_url_is_loopback(
            "http://127.0.0.1:3000/api/shunt/manual-swarms"
        ));
        assert!(validate_response_target(&json!({"target":"local"}), "auto", &config).is_err());
        assert!(validate_response_target(
            &json!({"target":"build-fra1"}),
            "hetzner-backup-substrate",
            &config
        )
        .is_err());
        assert!(validate_response_target(&json!({"target":"build-fra1"}), "auto", &config).is_ok());
        assert!(validate_response_target(
            &json!({"session":{"spec":{"target":"build-fra1"}}}),
            "build-fra1",
            &config
        )
        .is_ok());
    }

    #[test]
    fn capability_version_is_mandatory() {
        assert!(ensure_capability(&json!({})).is_err());
        assert!(ensure_capability(&json!({"capability_version":"manual-swarm/v0"})).is_err());
        assert!(
            ensure_capability(&json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION}))
                .is_ok()
        );
    }

    #[test]
    fn response_scrubber_removes_session_authority_and_rejects_credentials() {
        let mut value = json!({
            "session_token":"must-not-leak",
            "preview_token":"needed-to-start",
            "summary":"provider key sk-proj-secret"
        });
        sanitize_remote_value(&mut value, 0).unwrap();
        assert!(value.get("session_token").is_none());
        assert_eq!(value["preview_token"], "needed-to-start");
        assert!(!value["summary"].as_str().unwrap().contains("secret"));
        assert!(sanitize_remote_value(&mut json!({"refresh_token":"bad"}), 0).is_err());
        assert!(sanitize_remote_value(&mut json!({"events": vec![0; 201]}), 0).is_err());
    }

    #[test]
    fn deterministic_idempotency_is_stable_and_operation_bound() {
        let body = json!({"a":1,"b":2});
        let first = deterministic_idempotency_key("start", &body).unwrap();
        assert_eq!(
            first,
            deterministic_idempotency_key("start", &body).unwrap()
        );
        assert_ne!(
            first,
            deterministic_idempotency_key("cancel", &body).unwrap()
        );
    }

    #[test]
    fn local_state_mac_detects_metadata_changes() {
        let first = state_mac(br#"{"session":"one"}"#).unwrap();
        assert_eq!(first, state_mac(br#"{"session":"one"}"#).unwrap());
        assert_ne!(first, state_mac(br#"{"session":"two"}"#).unwrap());
    }

    #[test]
    fn proposal_requires_independent_passing_review_and_no_conflicts() {
        let patch = b"diff --git a/src/lib.rs b/src/lib.rs\n";
        assert!(parse_proposal(&valid_proposal(patch)).is_ok());

        let mut self_review = valid_proposal(patch);
        self_review["reviews"][0]["worker_id"] = json!("worker_1");
        self_review["reviewer_worker_ids"] = json!(["worker_1"]);
        assert!(parse_proposal(&self_review).is_err());

        let mut conflict = valid_proposal(patch);
        conflict["conflict_groups"] = json!([["worker_1", "worker_2"]]);
        assert!(parse_proposal(&conflict).is_err());

        let mut finding = valid_proposal(patch);
        finding["unresolved_findings"] = json!(["unsafe"]);
        assert!(parse_proposal(&finding).is_err());

        let mut contradictory = valid_proposal(patch);
        contradictory["checks"][0] = json!({"status":"failed","passed":true});
        assert!(parse_proposal(&contradictory).is_err());
    }

    #[test]
    fn patch_paths_reject_traversal_and_git_metadata() {
        for invalid in [
            "",
            "/etc/passwd",
            "../escape",
            "src/../../escape",
            ".git/config",
            ".env.local",
            ".npmrc",
            "credentials.json",
        ] {
            assert!(validate_patch_path(invalid).is_err(), "accepted {invalid}");
        }
        assert!(validate_patch_path("src/manual_swarm.rs").is_ok());
    }

    #[test]
    fn credential_shaped_patch_content_is_blocked() {
        assert!(patch_contains_secret_material(
            b"+OPENAI_API_KEY=should-not-land\n"
        ));
        assert!(patch_contains_secret_material(
            b"+token = sk-proj-sensitive\n"
        ));
        assert!(!patch_contains_secret_material(b"+ordinary source code\n"));
    }

    #[test]
    fn path_overlap_includes_parent_child_relationships() {
        assert!(paths_overlap("src", "src/lib.rs"));
        assert!(paths_overlap("src/lib.rs", "src/lib.rs"));
        assert!(!paths_overlap("src/lib.rs", "src/lib.rs.bak"));
    }

    #[test]
    fn provider_mix_is_deterministic_and_bounded() {
        let input = ManualToolInput::default();
        assert_eq!(provider_mix(&input, 5).unwrap(), (3, 2));
        let input = ManualToolInput {
            codex_agents: Some(4),
            claude_agents: Some(2),
            ..Default::default()
        };
        assert!(provider_mix(&input, 5).is_err());
    }

    #[test]
    fn requested_checks_are_verified_against_the_canonical_proposal() {
        let response = json!({"proposal":{"checks":[
            {"name":"unit","status":"passed"},
            {"name":"lint","status":"failed"}
        ]}});
        assert!(validate_requested_checks(&response, &["unit".into()]).is_ok());
        assert!(validate_requested_checks(&response, &["lint".into()]).is_err());
        assert!(validate_requested_checks(&response, &["missing".into()]).is_err());
    }

    #[test]
    fn tool_surface_has_all_eleven_distinct_operations() {
        let definitions = tool_definitions();
        assert_eq!(definitions.len(), 11);
        let names: BTreeSet<&str> = definitions
            .iter()
            .filter_map(|definition| definition.get("name").and_then(Value::as_str))
            .collect();
        assert_eq!(names.len(), 11);
        assert!(names.contains("manual_swarm_capabilities"));
        assert!(names.contains("manual_swarm_apply"));
    }

    #[tokio::test]
    async fn hardened_apply_succeeds_only_for_bound_clean_base() {
        let (repo, base, patch) = init_apply_fixture("apply");
        let proposal = Proposal {
            id: format!("proposal_{}", uuid::Uuid::new_v4()),
            session_id: "session_1".into(),
            base_commit: base.clone(),
            patch_ref: "artifact_1".into(),
            patch_sha256: hex::encode(Sha256::digest(&patch)),
            patch_size: patch.len(),
            inline_patch: None,
            changed_paths: BTreeSet::from(["README.md".into()]),
        };
        let binding = SessionBinding {
            workspace: repo.clone(),
            base_commit: base,
            target: "local".into(),
            created_at: now_secs(),
        };
        let result = apply_patch(&binding, &proposal, &patch).await.unwrap();
        assert_eq!(result["status"], "applied");
        assert_eq!(
            std::fs::read_to_string(repo.join("README.md")).unwrap(),
            "manual swarm change\n"
        );
        let _ = std::fs::remove_dir_all(repo);
    }

    #[tokio::test]
    async fn hardened_apply_blocks_dirty_overlap_and_manifest_mismatch() {
        let (repo, base, patch) = init_apply_fixture("collision");
        let mut proposal = Proposal {
            id: format!("proposal_{}", uuid::Uuid::new_v4()),
            session_id: "session_1".into(),
            base_commit: base.clone(),
            patch_ref: "artifact_1".into(),
            patch_sha256: hex::encode(Sha256::digest(&patch)),
            patch_size: patch.len(),
            inline_patch: None,
            changed_paths: BTreeSet::from(["not-the-patch.txt".into()]),
        };
        let binding = SessionBinding {
            workspace: repo.clone(),
            base_commit: base,
            target: "local".into(),
            created_at: now_secs(),
        };
        assert!(apply_patch(&binding, &proposal, &patch)
            .await
            .unwrap_err()
            .to_string()
            .contains("changed_paths"));
        proposal.changed_paths = BTreeSet::from(["README.md".into()]);
        std::fs::write(repo.join("README.md"), "user dirty change\n").unwrap();
        assert!(apply_patch(&binding, &proposal, &patch)
            .await
            .unwrap_err()
            .to_string()
            .contains("dirty parent"));
        assert_eq!(
            std::fs::read_to_string(repo.join("README.md")).unwrap(),
            "user dirty change\n"
        );
        let _ = std::fs::remove_dir_all(repo);
    }

    #[test]
    fn apply_lock_serializes_concurrent_mutations() {
        let dir = std::env::temp_dir().join(format!("shunt-lock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lock");
        let first = acquire_apply_lock(&path).unwrap();
        assert!(acquire_apply_lock(&path).is_err());
        drop(first);
        assert!(acquire_apply_lock(&path).is_ok());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn website_client_smokes_all_control_routes_with_bounded_outputs() {
        use axum::{extract::Request, response::IntoResponse, routing::any, Router};
        use std::sync::{Arc, Mutex};

        let seen = Arc::new(Mutex::new(
            Vec::<(String, String, bool, Option<Value>)>::new(),
        ));
        let captured = seen.clone();
        let app = Router::new().fallback(any(move |request: Request| {
            let captured = captured.clone();
            async move {
                let path = request.uri().path().to_owned();
                let method = request.method().as_str().to_owned();
                let authorized = request.headers().get("authorization")
                    .and_then(|value| value.to_str().ok()) == Some("Bearer website-test-token");
                let idempotent = request.headers().get("idempotency-key").is_some();
                let (_, body) = request.into_parts();
                let body = axum::body::to_bytes(body, MAX_REQUEST_BYTES).await.unwrap();
                let body = if body.is_empty() { None } else { serde_json::from_slice(&body).ok() };
                captured.lock().unwrap().push((method, path.clone(), idempotent, body));
                if !authorized {
                    return (StatusCode::FORBIDDEN, axum::Json(json!({"error":"denied"}))).into_response();
                }
                let base = "/api/shunt/manual-swarms";
                let value = if path == format!("{base}/capabilities") {
                    json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,"enabled":true,
                        "api_version":"autoswarm.manual-swarm/v1","max_workers":8,
                        "maximum_duration_seconds":3600,
                        "operations":["preview","start","status","events","inspect","steer","cancel","review","integrate","cleanup"],
                        "targets":["local","build-fra1"]})
                } else if path.ends_with("/large") {
                    json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,"summary":"x".repeat(4096)})
                } else if path.ends_with("/events") {
                    json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,"events":[{"phase":"running"}],"next_cursor":"cursor_2","terminal":false})
                } else if path.ends_with("/inspect") || path.ends_with("/steer") {
                    json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,
                        "record":{"id":"worker_1","spec":{"session_id":"session_1"},"status":{"message":"redacted"}}})
                } else if path.ends_with("/review") || path.ends_with("/integrate") {
                    json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,
                        "proposal":{"id":"proposal_1","session_id":"session_1","checks":[]}})
                } else {
                    json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,
                        "session":{"id":"session_1","spec":{"target":"build-fra1","base_commit":"a".repeat(40)},"status":{"phase":"running"}}})
                };
                (StatusCode::OK, axum::Json(value)).into_response()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = ManualSwarmConfig {
            control_url: format!("http://{address}/api/shunt/manual-swarms"),
            ..Default::default()
        };
        let client = ManualClient::new(config, "website-test-token".into()).unwrap();
        capabilities(&client).await.unwrap();
        let session = ManualToolInput {
            id: Some("session_1".into()),
            idempotency_key: Some("idem-key-12345678".into()),
            ..Default::default()
        };
        status(&client, &session).await.unwrap();
        wait(&client, &session).await.unwrap();
        inspect(
            &client,
            &ManualToolInput {
                kind: Some("worker".into()),
                reference: Some("worker_1".into()),
                ..session.clone()
            },
        )
        .await
        .unwrap();
        steer(
            &client,
            &ManualToolInput {
                worker_id: Some("worker_1".into()),
                guidance: Some("check compatibility".into()),
                ..session.clone()
            },
        )
        .await
        .unwrap();
        cancel(&client, &session).await.unwrap();
        review(&client, &session).await.unwrap();
        write_operation(
            &client,
            "session_1",
            "integrate",
            Some("idem-key-12345678"),
            json!({}),
        )
        .await
        .unwrap();
        cleanup(&client, &session).await.unwrap();
        client
            .request(
                Method::POST,
                "/preview",
                &[],
                Some(&json!({"idempotency_key":"idem-key-12345678"})),
                MAX_RESPONSE_BYTES,
            )
            .await
            .unwrap();
        client
            .request(
                Method::POST,
                "/start",
                &[],
                Some(&json!({"idempotency_key":"idem-key-12345678"})),
                MAX_RESPONSE_BYTES,
            )
            .await
            .unwrap();
        assert!(client
            .request(Method::GET, "/large", &[], None, 128)
            .await
            .is_err());
        let denied = ManualClient::new(client.config.clone(), "wrong-token".into()).unwrap();
        assert!(capabilities(&denied)
            .await
            .unwrap_err()
            .to_string()
            .contains("denied"));
        let seen = seen.lock().unwrap();
        for suffix in [
            "/capabilities",
            "/session_1",
            "/session_1/events",
            "/session_1/inspect",
            "/session_1/steer",
            "/session_1/cancel",
            "/session_1/review",
            "/session_1/integrate",
            "/session_1/cleanup",
            "/preview",
            "/start",
        ] {
            assert!(
                seen.iter().any(|(_, path, _, _)| path.ends_with(suffix)),
                "missing {suffix}"
            );
        }
        assert!(seen
            .iter()
            .filter(|(method, path, _, _)| method == "POST" && !path.ends_with("/inspect"))
            .all(|(_, _, idempotent, _)| *idempotent));
        let inspect_body = seen
            .iter()
            .find(|(_, path, _, _)| path.ends_with("/session_1/inspect"))
            .and_then(|(_, _, _, body)| body.as_ref());
        assert_eq!(
            inspect_body,
            Some(&json!({"kind":"worker","worker_id":"worker_1"})),
            "worker inspect must match Website3/Auto's canonical input envelope"
        );
        let operation_body = |suffix: &str| {
            seen.iter()
                .find(|(_, path, _, _)| path.ends_with(suffix))
                .and_then(|(_, _, _, body)| body.as_ref())
        };
        assert_eq!(
            operation_body("/session_1/steer"),
            Some(
                &json!({"worker_id":"worker_1","guidance":"check compatibility","idempotency_key":"idem-key-12345678"})
            ),
            "steer must use Auto's worker_id field"
        );
        assert_eq!(
            operation_body("/session_1/cancel"),
            Some(&json!({"idempotency_key":"idem-key-12345678"})),
            "whole-session cancellation must not invent a worker field"
        );
        assert_eq!(
            operation_body("/session_1/review"),
            Some(&json!({"idempotency_key":"idem-key-12345678"})),
            "review must retain Website3's strict canonical request shape"
        );
        server.abort();
    }

    #[tokio::test]
    async fn patch_fetch_uses_only_authenticated_same_origin_artifact_envelope() {
        use axum::{routing::post, Json, Router};
        let patch = b"diff --git a/README.md b/README.md\n".to_vec();
        let sha = hex::encode(Sha256::digest(&patch));
        let encoded = base64::engine::general_purpose::STANDARD.encode(&patch);
        let patch_len = patch.len();
        let response_sha = sha.clone();
        let app = Router::new().route("/api/shunt/manual-swarms/session_1/inspect", post(move || {
            let encoded = encoded.clone();
            let response_sha = response_sha.clone();
            async move { Json(json!({"capability_version":MANUAL_SWARM_CAPABILITY_VERSION,
                "artifact":{"content_base64":encoded,"sha256":response_sha,"size_bytes":patch_len}})) }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = ManualSwarmConfig {
            control_url: format!("http://{address}/api/shunt/manual-swarms"),
            ..Default::default()
        };
        let client = ManualClient::new(config, "token".into()).unwrap();
        let proposal = Proposal {
            id: "proposal_1".into(),
            session_id: "session_1".into(),
            base_commit: "a".repeat(40),
            patch_ref: "artifact_1".into(),
            patch_sha256: sha,
            patch_size: patch.len(),
            inline_patch: None,
            changed_paths: BTreeSet::from(["README.md".into()]),
        };
        assert_eq!(
            fetch_patch(&client, "session_1", &proposal).await.unwrap(),
            patch
        );
        server.abort();
    }
}

//! Website3 device authentication and credential-broker client.
//!
//! Website3 authenticates the person; Fabric authorizes inventory and leases.
//! Shunt never receives Doppler credentials or provider refresh tokens.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::credential::Credential;

pub const DEFAULT_WEBSITE_URL: &str = "https://beyondwork.ai";
pub const MAX_GRACE_CACHE_SECS: u64 = 60 * 60;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub base_url: String,
    pub cache_max_secs: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            base_url: std::env::var("SHUNT_WEBSITE_URL")
                .unwrap_or_else(|_| DEFAULT_WEBSITE_URL.into()),
            cache_max_secs: MAX_GRACE_CACHE_SECS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsiteSession {
    pub access_token: String,
    pub expires_at: u64,
    pub website_user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceAuthorization {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: String,
    pub expires_in: u64,
    pub interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryAccount {
    pub id: String,
    pub provider: String,
    pub label: String,
    pub plan_type: Option<String>,
    pub available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseResponse {
    credential: Credential,
    expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedLease {
    credential: Credential,
    expires_at: u64,
    grace_until: u64,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn session_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(crate::config::APP_NAME)
        .join("website-session.json")
}

fn cache_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(crate::config::APP_NAME)
        .join("website-cache.enc")
}

fn write_private(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub fn load_session() -> Result<WebsiteSession> {
    let session: WebsiteSession = serde_json::from_slice(
        &std::fs::read(session_path())
            .context("Website3 login not found; run `shunt website login`")?,
    )
    .context("Website3 session is invalid; run `shunt website login` again")?;
    if session.expires_at <= now_secs() {
        bail!("Website3 session expired; run `shunt website login`");
    }
    Ok(session)
}

pub fn save_session(session: &WebsiteSession) -> Result<()> {
    write_private(&session_path(), &serde_json::to_vec_pretty(session)?)
}

pub fn logout() -> Result<()> {
    for path in [session_path(), cache_path()] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub async fn begin_device_login(config: &BrokerConfig) -> Result<DeviceAuthorization> {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/shunt/device/start",
            config.base_url.trim_end_matches('/')
        ))
        .json(&serde_json::json!({ "client": "shunt", "version": env!("CARGO_PKG_VERSION") }))
        .send()
        .await
        .context("Failed to reach Website3")?;
    if !response.status().is_success() {
        bail!("Website3 device login failed ({})", response.status());
    }
    response
        .json()
        .await
        .context("Website3 returned an invalid device login response")
}

pub async fn poll_device_login(
    config: &BrokerConfig,
    device_code: &str,
) -> Result<Option<WebsiteSession>> {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/shunt/device/token",
            config.base_url.trim_end_matches('/')
        ))
        .json(&serde_json::json!({ "device_code": device_code }))
        .send()
        .await
        .context("Failed to reach Website3")?;
    if response.status() == reqwest::StatusCode::PRECONDITION_REQUIRED
        || response.status() == reqwest::StatusCode::BAD_REQUEST
    {
        return Ok(None);
    }
    if !response.status().is_success() {
        bail!("Website3 device token failed ({})", response.status());
    }
    Ok(Some(
        response
            .json()
            .await
            .context("Website3 returned an invalid session")?,
    ))
}

pub async fn inventory(config: &BrokerConfig) -> Result<Vec<InventoryAccount>> {
    let session = load_session()?;
    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/shunt/inventory",
            config.base_url.trim_end_matches('/')
        ))
        .bearer_auth(&session.access_token)
        .send()
        .await
        .context("Failed to reach Website3")?;
    if !response.status().is_success() {
        bail!("Website3 inventory failed ({})", response.status());
    }
    #[derive(Deserialize)]
    struct Response {
        accounts: Vec<InventoryAccount>,
    }
    Ok(response
        .json::<Response>()
        .await
        .context("Website3 returned invalid inventory")?
        .accounts)
}

pub async fn add_api_key(
    config: &BrokerConfig,
    provider: &str,
    label: &str,
    key: &str,
    space_id: Option<&str>,
) -> Result<InventoryAccount> {
    if ["NPMJS", "NPM_TOKEN", "NODE_AUTH_TOKEN"]
        .iter()
        .any(|blocked| label.to_ascii_uppercase().contains(blocked))
    {
        bail!("Package publishing credentials cannot be added to Shunt");
    }
    let session = load_session()?;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/shunt/accounts/add-key",
            config.base_url.trim_end_matches('/')
        ))
        .bearer_auth(&session.access_token)
        .json(&serde_json::json!({
            "provider": provider,
            "label": label,
            "key": key,
            "space_id": space_id,
        }))
        .send()
        .await
        .context("Failed to reach Website3")?;
    if !response.status().is_success() {
        bail!("Website3 add-key failed ({})", response.status());
    }
    response
        .json()
        .await
        .context("Website3 returned an invalid account")
}

fn cache_key() -> Result<String> {
    crate::config::local_client_token("website-grace-cache")
}

fn read_cache() -> HashMap<String, CachedLease> {
    let Ok(payload) = std::fs::read_to_string(cache_path()) else {
        return HashMap::new();
    };
    let Ok(key) = cache_key() else {
        return HashMap::new();
    };
    let Ok(bytes) = crate::sync::decrypt_bytes(&payload, &key) else {
        return HashMap::new();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn write_cache(cache: &HashMap<String, CachedLease>) -> Result<()> {
    let encrypted = crate::sync::encrypt_bytes(&serde_json::to_vec(cache)?, &cache_key()?)?;
    write_private(&cache_path(), encrypted.as_bytes())
}

fn validate_broker_credential(credential: &Credential) -> Result<()> {
    if credential
        .as_oauth()
        .is_some_and(|oauth| !oauth.refresh_token.is_empty())
    {
        bail!("Website3 broker returned a provider refresh token; refusing unsafe lease");
    }
    Ok(())
}

/// Resolve a short-lived broker lease. A previously fetched lease may be used
/// only until the earlier of its provider expiry and the configured one-hour
/// grace ceiling. Authorization failures never fall back to cache.
pub fn resolve_credential(config: &BrokerConfig, credential_id: &str) -> Result<Credential> {
    let session = load_session()?;
    let endpoint = format!(
        "{}/api/shunt/credentials/lease",
        config.base_url.trim_end_matches('/')
    );
    let access_token = session.access_token;
    let requested_id = credential_id.to_owned();
    enum Attempt {
        Lease(LeaseResponse),
        Denied(u16),
        Failed(String),
    }
    // Config loading also runs inside Tokio commands. Keep reqwest's blocking
    // runtime on a plain OS thread so it is never created/dropped in an async
    // runtime context.
    let attempt = std::thread::spawn(move || {
        match reqwest::blocking::Client::new()
            .post(endpoint)
            .bearer_auth(access_token)
            .json(&serde_json::json!({ "credential_id": requested_id }))
            .send()
        {
            Ok(response) if response.status().is_success() => response
                .json::<LeaseResponse>()
                .map(Attempt::Lease)
                .unwrap_or_else(|error| {
                    Attempt::Failed(format!(
                        "Website3 returned an invalid credential lease: {error}"
                    ))
                }),
            Ok(response)
                if response.status() == reqwest::StatusCode::UNAUTHORIZED
                    || response.status() == reqwest::StatusCode::FORBIDDEN =>
            {
                Attempt::Denied(response.status().as_u16())
            }
            Ok(response) => {
                Attempt::Failed(format!("Website3 lease failed ({})", response.status()))
            }
            Err(error) => Attempt::Failed(format!("Website3 unavailable: {error}")),
        }
    })
    .join()
    .map_err(|_| anyhow::anyhow!("Website3 lease worker panicked"))?;
    match attempt {
        Attempt::Lease(lease) => {
            validate_broker_credential(&lease.credential)?;
            let now = now_secs();
            let grace_until = lease
                .expires_at
                .min(now.saturating_add(config.cache_max_secs.min(MAX_GRACE_CACHE_SECS)));
            let mut cache = read_cache();
            cache.insert(
                credential_id.to_owned(),
                CachedLease {
                    credential: lease.credential.clone(),
                    expires_at: lease.expires_at,
                    grace_until,
                },
            );
            write_cache(&cache)?;
            Ok(lease.credential)
        }
        Attempt::Denied(status) => bail!("Website3 denied credential lease ({status})"),
        Attempt::Failed(error) => cached_or_error(credential_id, error),
    }
}

fn cached_or_error(credential_id: &str, error: String) -> Result<Credential> {
    let now = now_secs();
    if let Some(lease) = read_cache().remove(credential_id) {
        if now <= lease.grace_until && now <= lease.expires_at {
            validate_broker_credential(&lease.credential)?;
            return Ok(lease.credential);
        }
    }
    bail!("{error}; no valid encrypted grace lease is available")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::OAuthCredential;

    #[test]
    fn broker_rejects_refresh_tokens() {
        let credential = Credential::Oauth(OAuthCredential {
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: u64::MAX,
            email: None,
            id_token: None,
            chatgpt_account_id: None,
            chatgpt_account_is_fedramp: false,
        });
        assert!(validate_broker_credential(&credential).is_err());
    }

    #[test]
    fn grace_ceiling_is_one_hour() {
        assert_eq!(MAX_GRACE_CACHE_SECS, 3600);
        assert!(
            BrokerConfig {
                base_url: "x".into(),
                cache_max_secs: 7200
            }
            .cache_max_secs
                > MAX_GRACE_CACHE_SECS
        );
    }
}

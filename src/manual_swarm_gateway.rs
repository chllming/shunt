//! Request-scoped routing authority for Manual Swarm workers.
//!
//! Website3/Fabric signs the same compact grant Auto Swarm verifies. A Shunt
//! model gateway may accept that grant as the worker's API token, but only to
//! route through the exact provider and credential identifiers in the signed
//! allowlist. The grant is never persisted and never becomes a provider
//! credential.

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_TOKEN_BYTES: usize = 64 * 1024;
const MAX_GRANT_LIFETIME_SECS: u64 = 65 * 60;
const CLOCK_SKEW_SECS: u64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualGrantScope {
    pub session_id: String,
    pub providers: BTreeSet<String>,
    pub subscriptions: BTreeSet<String>,
    pub expires_at: u64,
}

impl ManualGrantScope {
    pub fn allows_provider(&self, provider: &str) -> bool {
        self.providers.contains(provider)
    }

    pub fn allows_subscription(&self, credential_id: &str) -> bool {
        self.subscriptions.contains(credential_id)
    }
}

#[derive(Clone)]
pub struct ManualGrantVerifier {
    public_key: VerifyingKey,
}

impl std::fmt::Debug for ManualGrantVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManualGrantVerifier")
            .field("configured", &true)
            .finish()
    }
}

impl ManualGrantVerifier {
    pub fn new(public_key: impl AsRef<[u8]>) -> Result<Self> {
        let bytes: [u8; 32] = public_key
            .as_ref()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Manual Swarm Ed25519 public key must be 32 bytes"))?;
        Ok(Self {
            public_key: VerifyingKey::from_bytes(&bytes)
                .context("Manual Swarm Ed25519 public key is invalid")?,
        })
    }

    pub fn from_environment() -> Result<Option<Self>> {
        let value = std::env::var("SHUNT_MANUAL_SWARM_PUBLIC_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("AUTOSWARM_MANUAL_SWARM_PUBLIC_KEY")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            });
        value
            .map(|encoded| {
                let bytes = URL_SAFE_NO_PAD
                    .decode(encoded.trim())
                    .context("Manual Swarm Ed25519 public key is not base64url")?;
                Self::new(bytes)
            })
            .transpose()
    }

    pub fn verify(&self, token: &str) -> Result<ManualGrantScope> {
        self.verify_at(token, now_secs())
    }

    pub fn verify_at(&self, token: &str, now: u64) -> Result<ManualGrantScope> {
        let token = token.trim();
        if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
            bail!("Manual Swarm grant is empty or oversized");
        }
        let mut parts = token.split('.');
        let body = parts
            .next()
            .context("Manual Swarm grant payload is missing")?;
        let signature = parts
            .next()
            .context("Manual Swarm grant signature is missing")?;
        if body.is_empty() || signature.is_empty() || parts.next().is_some() {
            bail!("Manual Swarm grant must contain exactly two segments");
        }

        let provided = URL_SAFE_NO_PAD
            .decode(signature)
            .context("Manual Swarm grant signature is not base64url")?;
        let signature = Signature::from_slice(&provided)
            .context("Manual Swarm grant signature length is invalid")?;
        self.public_key
            .verify(body.as_bytes(), &signature)
            .map_err(|_| anyhow::anyhow!("Manual Swarm grant signature is invalid"))?;

        let payload = URL_SAFE_NO_PAD
            .decode(body)
            .context("Manual Swarm grant payload is not base64url")?;
        if payload.len() > 32 * 1024 {
            bail!("Manual Swarm grant payload is oversized");
        }
        let claims: ManualGrantClaims =
            serde_json::from_slice(&payload).context("Manual Swarm grant payload is invalid")?;
        claims.validate(now)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualGrantClaims {
    version: u8,
    #[serde(rename = "iss")]
    issuer: String,
    #[serde(rename = "aud")]
    audience: String,
    grant_id: String,
    session_id: String,
    website_user_id: String,
    space_id: String,
    swarm_id: String,
    base: String,
    source: String,
    providers: Vec<String>,
    subscriptions: Vec<String>,
    max_workers: usize,
    targets: Vec<String>,
    #[serde(rename = "iat")]
    issued_at: u64,
    #[serde(rename = "nbf")]
    not_before: u64,
    #[serde(rename = "exp")]
    expires_at: u64,
    nonce: String,
}

impl ManualGrantClaims {
    fn validate(self, now: u64) -> Result<ManualGrantScope> {
        if self.version != 1
            || self.issuer != "website3-fabric"
            || self.audience != "autoswarm-manual-swarm"
        {
            bail!("Manual Swarm grant issuer, audience, or version is invalid");
        }
        for (name, value, max) in [
            ("grant_id", self.grant_id.as_str(), 160),
            ("session_id", self.session_id.as_str(), 160),
            ("website_user_id", self.website_user_id.as_str(), 160),
            ("space_id", self.space_id.as_str(), 160),
            ("swarm_id", self.swarm_id.as_str(), 160),
            ("nonce", self.nonce.as_str(), 200),
        ] {
            if !safe_identifier(value, max) {
                bail!("Manual Swarm grant {name} is invalid");
            }
        }
        if self.base.trim().is_empty()
            || self.base.len() > 200
            || self.source.trim().is_empty()
            || self.source.len() > 4096
        {
            bail!("Manual Swarm grant base or source is invalid");
        }
        if self.issued_at == 0
            || self.not_before == 0
            || self.expires_at == 0
            || self.not_before.saturating_add(CLOCK_SKEW_SECS) < self.issued_at
            || self.expires_at < self.not_before
            || self.expires_at.saturating_sub(self.issued_at) > MAX_GRANT_LIFETIME_SECS
            || now.saturating_add(CLOCK_SKEW_SECS) < self.not_before
            || now >= self.expires_at
        {
            bail!("Manual Swarm grant time window is invalid or expired");
        }
        if self.max_workers == 0 || self.max_workers > 8 {
            bail!("Manual Swarm grant worker ceiling is invalid");
        }
        if self.targets.is_empty()
            || self.targets.len() > 4
            || self.targets.iter().any(|target| {
                !matches!(
                    target.as_str(),
                    "local" | "build-fra1" | "hetzner-backup-substrate"
                )
            })
        {
            bail!("Manual Swarm grant target allowlist is invalid");
        }

        let providers: BTreeSet<_> = self
            .providers
            .into_iter()
            .map(|provider| provider.trim().to_ascii_lowercase())
            .collect();
        if providers.is_empty()
            || providers.len() > 2
            || providers
                .iter()
                .any(|provider| !matches!(provider.as_str(), "claude" | "codex"))
        {
            bail!("Manual Swarm grant provider allowlist is invalid");
        }
        let subscriptions: BTreeSet<_> = self
            .subscriptions
            .into_iter()
            .map(|subscription| subscription.trim().to_owned())
            .collect();
        if subscriptions.is_empty()
            || subscriptions.len() > 16
            || subscriptions
                .iter()
                .any(|subscription| !safe_identifier(subscription, 200))
        {
            bail!("Manual Swarm grant subscription allowlist is invalid");
        }
        Ok(ManualGrantScope {
            session_id: self.session_id,
            providers,
            subscriptions,
            expires_at: self.expires_at,
        })
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn safe_identifier(value: &str, max: usize) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value.len() <= max
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_PUBLIC_KEY: &str = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo";
    const FIXTURE_TOKEN: &str = "eyJ2ZXJzaW9uIjoxLCJpc3MiOiJ3ZWJzaXRlMy1mYWJyaWMiLCJhdWQiOiJhdXRvc3dhcm0tbWFudWFsLXN3YXJtIiwiZ3JhbnRfaWQiOiIxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEiLCJzZXNzaW9uX2lkIjoibXN3X2ZpeHR1cmUiLCJ3ZWJzaXRlX3VzZXJfaWQiOiJ1c2VyX2ZpeHR1cmUiLCJzcGFjZV9pZCI6InNwYWNlX2ZpeHR1cmUiLCJzd2FybV9pZCI6InN3YXJtX2ZpeHR1cmUiLCJiYXNlIjoiMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWYwMTIzNDU2NyIsInNvdXJjZSI6Imh0dHBzOi8vZXhhbXBsZS50ZXN0L3JlcG8uZ2l0IiwicHJvdmlkZXJzIjpbImNvZGV4IiwiY2xhdWRlIl0sInN1YnNjcmlwdGlvbnMiOlsiYWNjb3VudF9jb2RleCIsImFjY291bnRfY2xhdWRlIl0sIm1heF93b3JrZXJzIjo0LCJ0YXJnZXRzIjpbImJ1aWxkLWZyYTEiXSwiaWF0IjoxNzgzNzcxMjAwLCJuYmYiOjE3ODM3NzEyMDAsImV4cCI6MTc4Mzc3NDgwMCwibm9uY2UiOiJBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBIn0.xxWjQbVy9PxqeOHSVbCS6iSVwqe8X8iRYr6vb_4rWo1rp_33pT10TedEFbuWTagmzZa54aDPESV4s5dE1LAIBA";

    fn fixture_verifier() -> ManualGrantVerifier {
        ManualGrantVerifier::new(URL_SAFE_NO_PAD.decode(FIXTURE_PUBLIC_KEY).unwrap()).unwrap()
    }

    #[test]
    fn verifies_cross_language_fixture_and_exact_scope() {
        let verifier = fixture_verifier();
        let scope = verifier.verify_at(FIXTURE_TOKEN, 1_783_771_300).unwrap();
        assert_eq!(scope.session_id, "msw_fixture");
        assert!(scope.allows_provider("codex"));
        assert!(scope.allows_provider("claude"));
        assert!(scope.allows_subscription("account_codex"));
        assert!(!scope.allows_subscription("account_other"));
    }

    #[test]
    fn rejects_tamper_expiry_and_unknown_claims() {
        let verifier = fixture_verifier();
        let mut tampered = FIXTURE_TOKEN.as_bytes().to_vec();
        tampered[10] = if tampered[10] == b'A' { b'B' } else { b'A' };
        assert!(verifier
            .verify_at(std::str::from_utf8(&tampered).unwrap(), 1_783_771_300)
            .is_err());
        assert!(verifier.verify_at(FIXTURE_TOKEN, 1_783_774_800).is_err());

        let (body, _) = FIXTURE_TOKEN.split_once('.').unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(body).unwrap()).unwrap();
        value["admin"] = serde_json::Value::Bool(true);
        let changed = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap());
        use ed25519_dalek::{Signer, SigningKey};
        let seed = hex::decode("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
            .unwrap();
        let signing = SigningKey::from_bytes(&seed.try_into().unwrap());
        let token = format!(
            "{}.{}",
            changed,
            URL_SAFE_NO_PAD.encode(signing.sign(changed.as_bytes()).to_bytes())
        );
        assert!(verifier.verify_at(&token, 1_783_771_300).is_err());
    }
}

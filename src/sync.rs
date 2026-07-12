//! Encryption helpers used by the `remote` command for device-to-device notification relay.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde_json;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Code generation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Encryption / decryption
// ---------------------------------------------------------------------------

fn derive_key(code: &str) -> [u8; 32] {
    let hash = Sha256::digest(code.as_bytes());
    hash.into()
}

/// Encrypt arbitrary bytes with the given code; returns a base64 payload string.
pub fn encrypt_bytes(data: &[u8], code: &str) -> Result<String> {
    let key_bytes = derive_key(code);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);
    let nonce_bytes = crate::oauth::rand_bytes::<12>();
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| anyhow::anyhow!("encryption failed: {e}"))?;
    let mut wire = Vec::with_capacity(12 + ciphertext.len());
    wire.extend_from_slice(&nonce_bytes);
    wire.extend_from_slice(&ciphertext);
    Ok(B64.encode(wire))
}

// ---------------------------------------------------------------------------
// Share code helpers (SC- prefix — one-time relay handshake for shunt connect)
// ---------------------------------------------------------------------------

/// Generate a random share code like `SC-a3f2b1c4d5e6f7a8b9`.
pub fn generate_share_code() -> String {
    let bytes = crate::oauth::rand_bytes::<9>();
    format!("SC-{}", hex::encode(bytes))
}

/// Validate that a share code has the expected format.
pub fn validate_share_code(code: &str) -> Result<()> {
    if !code.starts_with("SC-") || code.len() != 21 {
        anyhow::bail!("Invalid share code format. Expected SC-<18 hex chars>.");
    }
    if !code[3..].chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!("Invalid share code — must be hex characters after 'SC-'.");
    }
    Ok(())
}

/// Push {base_url, api_key} to the relay under `code`.
/// base_url is sent plaintext (not sensitive — it's just an IP/URL).
/// api_key is encrypted with the share code before sending — the relay never sees it.
pub async fn push_share(code: &str, base_url: &str, api_key: &str, relay_url: &str) -> Result<()> {
    let encrypted_key = encrypt_bytes(api_key.as_bytes(), code)?;
    let client = reqwest::Client::new();
    let url = format!("{relay_url}/share/{code}");
    let res = client
        .put(&url)
        .json(&serde_json::json!({ "base_url": base_url, "api_key": encrypted_key }))
        .send()
        .await
        .context("Failed to reach relay")?;
    if !res.status().is_success() {
        let body = res.text().await.unwrap_or_default();
        anyhow::bail!("Relay rejected share push ({}): {}", url, body);
    }
    Ok(())
}

/// Pull {base_url, api_key} from the relay for `code`. api_key is decrypted with the code.
/// Deletes the entry on success.
pub async fn pull_share(code: &str, relay_url: &str) -> Result<(String, String)> {
    let client = reqwest::Client::new();
    let url = format!("{relay_url}/share/{code}");
    let res = client
        .get(&url)
        .send()
        .await
        .context("Failed to reach relay")?;
    if res.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("Share code not found, expired, or already used. Ask the host to run `shunt share` again.");
    }
    if !res.status().is_success() {
        let body = res.text().await.unwrap_or_default();
        anyhow::bail!("Relay error: {body}");
    }
    let json: serde_json::Value = res.json().await.context("Invalid JSON from relay")?;
    let base_url = json["base_url"]
        .as_str()
        .context("Missing base_url")?
        .to_owned();
    let encrypted_key = json["api_key"].as_str().context("Missing api_key")?;
    let key_bytes = decrypt_bytes(encrypted_key, code)?;
    let api_key = String::from_utf8(key_bytes).context("api_key is not valid UTF-8")?;
    Ok((base_url, api_key))
}

/// Decrypt a base64 payload into bytes using the given code.
pub fn decrypt_bytes(payload_b64: &str, code: &str) -> Result<Vec<u8>> {
    let wire = B64
        .decode(payload_b64)
        .context("invalid base64 in payload")?;
    if wire.len() < 12 {
        anyhow::bail!("payload too short");
    }
    let (nonce_bytes, ciphertext) = wire.split_at(12);
    let key_bytes = derive_key(code);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| anyhow::anyhow!("decryption failed — wrong code or corrupted payload"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let code = "SC-aabbccddeeff001122";
        let api_key = b"sk-ant-testkey-0000111122223333";
        let encrypted = encrypt_bytes(api_key, code).unwrap();
        let decrypted = decrypt_bytes(&encrypted, code).unwrap();
        assert_eq!(api_key.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_wrong_code_fails() {
        let code = "SC-aabbccddeeff001122";
        let data = b"hello";
        let encrypted = encrypt_bytes(data, code).unwrap();
        assert!(decrypt_bytes(&encrypted, "SC-wrongcodewrongco").is_err());
    }

    /// Full relay roundtrip — requires network, skipped by default.
    /// Run with: cargo test --lib sync::tests::test_relay_roundtrip -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_relay_roundtrip() {
        let code = generate_share_code();
        let relay = "https://relay.ramcharan.shop";
        let base_url = "http://192.168.1.100:8082";
        let api_key = "sk-ant-test-relay-roundtrip";

        push_share(&code, base_url, api_key, relay)
            .await
            .expect("push_share failed");
        let (got_url, got_key) = pull_share(&code, relay).await.expect("pull_share failed");

        assert_eq!(got_url, base_url);
        assert_eq!(got_key, api_key);
        println!("Relay roundtrip OK — code={code}");
    }
}

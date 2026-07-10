use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Response};
use bytes::Bytes;
use reqwest::Client;
use std::str::FromStr;
use uuid::Uuid;

use crate::config::AccountConfig;
use crate::credential::Credential;

/// Headers that must never be forwarded in either direction.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

/// Headers the proxy explicitly passes through to upstream.
/// All other client-supplied headers are dropped (allowlist approach, #15).
const ALLOWED_REQUEST_HEADERS: &[&str] = &[
    "content-type",
    "accept",
    "anthropic-version",
    "anthropic-beta",
    "anthropic-dangerous-direct-browser-access",
    "x-request-id",
    "user-agent",
    // chatgpt.com sentinel token — injected by proxy, pass through
    "openai-sentinel-chat-requirements-token",
];

/// Sensitive response headers that upstream must never inject into client responses (#21).
const BLOCKED_RESPONSE_HEADERS: &[&str] = &[
    "set-cookie",
    "set-cookie2",
    "access-control-allow-origin",
    "access-control-allow-credentials",
    "access-control-allow-methods",
    "access-control-allow-headers",
];

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.contains(&name.to_ascii_lowercase().as_str())
}

pub struct Forwarder {
    client: Client,
}

impl Forwarder {
    pub fn new(timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self { client })
    }

    /// Forward a request to the upstream using the given account's OAuth credential.
    ///
    /// - `upstream` overrides the base URL for this account (per-provider routing).
    /// - Strips `Authorization` and `x-api-key` from the client request.
    /// - Injects `Authorization: Bearer <token>` (live token, may differ from account.credential).
    /// - Keeps the upstream TCP connection alive for streaming responses.
    pub async fn forward(
        &self,
        upstream: &str,
        method: &str,
        path: &str,
        body: Bytes,
        client_headers: &HeaderMap,
        account: &AccountConfig,
        token: &str,
    ) -> Result<Response<Body>> {
        let _request_id = &Uuid::new_v4().to_string()[..8];
        let url = format!("{}{}", upstream, path);

        let mut upstream_headers = reqwest::header::HeaderMap::new();

        // #15: allowlist — only forward explicitly permitted client headers.
        for &name in ALLOWED_REQUEST_HEADERS {
            if let Some(value) = client_headers.get(name) {
                if let Ok(n) = reqwest::header::HeaderName::from_str(name) {
                    if let Ok(v) = reqwest::header::HeaderValue::from_bytes(value.as_bytes()) {
                        upstream_headers.insert(n, v);
                    }
                }
            }
        }

        // Inject provider-specific auth headers (Bearer token + any required protocol headers).
        account.provider.inject_auth_headers(&mut upstream_headers, token)
            .context("failed to inject auth headers")?;

        let upstream_resp = self
            .client
            .request(
                reqwest::Method::from_str(method).context("invalid method")?,
                &url,
            )
            .headers(upstream_headers)
            .body(body.clone())
            .send()
            .await
            .context("upstream request failed")?;

        let status = upstream_resp.status();

        let mut builder = Response::builder().status(status.as_u16());

        for (name, value) in upstream_resp.headers().iter() {
            let lower = name.as_str().to_ascii_lowercase();
            // #21: drop hop-by-hop and sensitive response headers.
            if is_hop_by_hop(&lower) || BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) {
                continue;
            }
            if let (Ok(n), Ok(v)) = (
                HeaderName::from_str(name.as_str()),
                HeaderValue::from_bytes(value.as_bytes()),
            ) {
                builder = builder.header(n, v);
            }
        }

        let body = Body::from_stream(upstream_resp.bytes_stream());
        Ok(builder.body(body).expect("response builder invariant"))
    }

    /// Byte-transparent forwarding for stock Codex's Responses transport.
    /// Client identity/auth headers are always stripped; routing identity is
    /// injected from the selected Shunt credential instead.
    pub async fn forward_codex(
        &self,
        upstream: &str,
        method: &str,
        path: &str,
        body: Bytes,
        client_headers: &HeaderMap,
        _account: &AccountConfig,
        credential: &Credential,
    ) -> Result<Response<Body>> {
        let url = format!("{}{}", upstream.trim_end_matches('/'), path);
        let mut upstream_headers = reqwest::header::HeaderMap::new();
        for (name, value) in client_headers {
            let lower = name.as_str().to_ascii_lowercase();
            let allowed = matches!(lower.as_str(),
                "content-type" | "accept" | "user-agent" | "session-id" | "thread-id"
                | "x-client-request-id" | "x-openai-subagent" | "originator"
                | "traceparent" | "tracestate" | "baggage" | "openai-beta")
                || lower.starts_with("x-codex-");
            let sensitive = matches!(lower.as_str(),
                "authorization" | "x-api-key" | "chatgpt-account-id" | "x-openai-fedramp"
                | "openai-organization" | "openai-project");
            if !allowed || sensitive || is_hop_by_hop(&lower) { continue; }
            if let (Ok(n), Ok(v)) = (
                reqwest::header::HeaderName::from_str(&lower),
                reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                upstream_headers.insert(n, v);
            }
        }
        upstream_headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", credential.access_token()))
                .context("invalid Codex access token")?,
        );
        if let Some(oauth) = credential.as_oauth() {
            if let Some(account_id) = oauth.chatgpt_account_id.as_deref() {
                upstream_headers.insert(
                    reqwest::header::HeaderName::from_static("chatgpt-account-id"),
                    reqwest::header::HeaderValue::from_str(account_id).context("invalid ChatGPT account id")?,
                );
            }
            if oauth.chatgpt_account_is_fedramp {
                upstream_headers.insert(
                    reqwest::header::HeaderName::from_static("x-openai-fedramp"),
                    reqwest::header::HeaderValue::from_static("true"),
                );
            }
        }

        let upstream_resp = self.client
            .request(reqwest::Method::from_str(method).context("invalid method")?, &url)
            .headers(upstream_headers)
            .body(body)
            .send().await.context("Codex upstream request failed")?;
        let mut builder = Response::builder().status(upstream_resp.status().as_u16());
        for (name, value) in upstream_resp.headers() {
            let lower = name.as_str().to_ascii_lowercase();
            if is_hop_by_hop(&lower) || BLOCKED_RESPONSE_HEADERS.contains(&lower.as_str()) { continue; }
            if let (Ok(n), Ok(v)) = (HeaderName::from_str(name.as_str()), HeaderValue::from_bytes(value.as_bytes())) {
                builder = builder.header(n, v);
            }
        }
        Ok(builder.body(Body::from_stream(upstream_resp.bytes_stream()))
            .expect("response builder invariant"))
    }
}

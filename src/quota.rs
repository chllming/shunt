/// Usage extraction from upstream responses.
///
/// - Non-streaming (application/json): parse `usage` from buffered body bytes.
/// - Streaming (text/event-stream): wrap the body with an SSE scanner that
///   extracts token counts from `message_start`/`message_delta` events and
///   calls a callback on stream end — zero added latency.
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::{Body, BodyDataStream};
use axum::http::Response;
use bytes::{Bytes, BytesMut};
use futures_util::Stream;

// ---------------------------------------------------------------------------
// Non-streaming usage extraction
// ---------------------------------------------------------------------------

/// Extract `(input_tokens, output_tokens)` from a JSON response body.
/// Returns `(0, 0)` if the body is not parseable or has no usage field.
pub fn extract_usage_from_json(body: &[u8]) -> (u64, u64) {
    let v: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return (0, 0),
    };
    let usage = &v["usage"];
    // Support both Anthropic (input_tokens/output_tokens) and OpenAI (prompt_tokens/completion_tokens).
    let input = usage["input_tokens"]
        .as_u64()
        .or_else(|| usage["prompt_tokens"].as_u64())
        .unwrap_or(0);
    let output = usage["output_tokens"]
        .as_u64()
        .or_else(|| usage["completion_tokens"].as_u64())
        .unwrap_or(0);
    (input, output)
}

// ---------------------------------------------------------------------------
// Streaming detection
// ---------------------------------------------------------------------------

pub fn is_streaming_response(resp: &Response<Body>) -> bool {
    resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("text/event-stream"))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// SSE scanner stream adapter
// ---------------------------------------------------------------------------

/// Wraps a `Body` stream, scanning SSE events for token usage.
/// Every byte is forwarded immediately; `on_complete(input, output)` is called
/// once when the stream ends.
pub fn wrap_streaming_body(
    body: Body,
    on_complete: Arc<dyn Fn(u64, u64) + Send + Sync + 'static>,
) -> Body {
    Body::from_stream(SseScanner::new(body.into_data_stream(), on_complete, None))
}

#[derive(Debug, Clone)]
pub struct CodexRateUpdate {
    pub primary_used_percent: Option<f64>,
    pub primary_reset_at: Option<u64>,
    pub secondary_used_percent: Option<f64>,
    pub secondary_reset_at: Option<u64>,
}

pub fn wrap_streaming_body_with_codex_rates(
    body: Body,
    on_complete: Arc<dyn Fn(u64, u64) + Send + Sync + 'static>,
    on_rate: Arc<dyn Fn(CodexRateUpdate) + Send + Sync + 'static>,
) -> Body {
    Body::from_stream(SseScanner::new(
        body.into_data_stream(),
        on_complete,
        Some(on_rate),
    ))
}

struct SseScanner {
    inner: BodyDataStream,
    line_buf: BytesMut,
    input_tokens: u64,
    output_tokens: u64,
    last_event: LastEvent,
    on_complete: Arc<dyn Fn(u64, u64) + Send + Sync + 'static>,
    on_codex_rate: Option<Arc<dyn Fn(CodexRateUpdate) + Send + Sync + 'static>>,
    done: bool,
}

#[derive(Default)]
enum LastEvent {
    #[default]
    None,
    MessageStart,
    MessageDelta,
}

impl SseScanner {
    fn new(
        inner: BodyDataStream,
        on_complete: Arc<dyn Fn(u64, u64) + Send + Sync + 'static>,
        on_codex_rate: Option<Arc<dyn Fn(CodexRateUpdate) + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            inner,
            line_buf: BytesMut::new(),
            input_tokens: 0,
            output_tokens: 0,
            last_event: LastEvent::None,
            on_complete,
            on_codex_rate,
            done: false,
        }
    }

    /// Process complete lines in `line_buf`, extracting token counts from SSE events.
    fn scan_lines(&mut self) {
        loop {
            let Some(pos) = self.line_buf.iter().position(|&b| b == b'\n') else {
                break;
            };
            let raw = self.line_buf.split_to(pos + 1);
            let line = raw
                .strip_suffix(b"\r\n")
                .or_else(|| raw.strip_suffix(b"\n"))
                .unwrap_or(&raw);

            if line.starts_with(b"event: message_start") {
                self.last_event = LastEvent::MessageStart;
            } else if line.starts_with(b"event: message_delta") {
                self.last_event = LastEvent::MessageDelta;
            } else if let Some(json_bytes) = line.strip_prefix(b"data: ") {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(json_bytes) {
                    if v.get("type").and_then(|v| v.as_str()) == Some("codex.rate_limits") {
                        let primary = v.pointer("/rate_limits/primary");
                        let secondary = v.pointer("/rate_limits/secondary");
                        if let Some(callback) = &self.on_codex_rate {
                            callback(CodexRateUpdate {
                                primary_used_percent: primary
                                    .and_then(|w| w.get("used_percent"))
                                    .and_then(|v| v.as_f64()),
                                primary_reset_at: primary
                                    .and_then(|w| w.get("reset_at"))
                                    .and_then(|v| v.as_u64()),
                                secondary_used_percent: secondary
                                    .and_then(|w| w.get("used_percent"))
                                    .and_then(|v| v.as_f64()),
                                secondary_reset_at: secondary
                                    .and_then(|w| w.get("reset_at"))
                                    .and_then(|v| v.as_u64()),
                            });
                        }
                    }
                    // OpenAI Responses SSE: response.completed carries the final
                    // usage object under `response.usage`.
                    let response_usage =
                        v.get("response").and_then(|r| r.get("usage")).or_else(|| {
                            if v.get("type").and_then(|t| t.as_str()).is_some_and(|t| {
                                t == "response.completed" || t == "response.incomplete"
                            }) {
                                v.get("usage")
                            } else {
                                None
                            }
                        });
                    if let Some(usage) = response_usage {
                        if let Some(input) = usage.get("input_tokens").and_then(|n| n.as_u64()) {
                            self.input_tokens = self.input_tokens.max(input);
                        }
                        if let Some(output) = usage.get("output_tokens").and_then(|n| n.as_u64()) {
                            self.output_tokens = self.output_tokens.max(output);
                        }
                    }
                    match self.last_event {
                        LastEvent::MessageStart => {
                            // Anthropic: message_start carries input token count.
                            self.input_tokens +=
                                v["message"]["usage"]["input_tokens"].as_u64().unwrap_or(0);
                        }
                        LastEvent::MessageDelta => {
                            // Anthropic: message_delta carries output token count.
                            self.output_tokens += v["usage"]["output_tokens"].as_u64().unwrap_or(0);
                        }
                        LastEvent::None => {
                            // OpenAI format: no event: lines. Usage arrives in a
                            // final chunk when stream_options.include_usage is set.
                            // Field names: prompt_tokens / completion_tokens.
                            if let Some(usage) = v.get("usage") {
                                if let Some(pt) = usage["prompt_tokens"].as_u64() {
                                    self.input_tokens = self.input_tokens.max(pt);
                                }
                                if let Some(ct) = usage["completion_tokens"].as_u64() {
                                    self.output_tokens = self.output_tokens.max(ct);
                                }
                            }
                        }
                    }
                }
                self.last_event = LastEvent::None;
            }
        }
    }
}

impl Stream for SseScanner {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                self.line_buf.extend_from_slice(&chunk);
                self.scan_lines();
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.done = true;
                (self.on_complete)(self.input_tokens, self.output_tokens);
                Poll::Ready(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[tokio::test]
    async fn responses_sse_usage_and_rate_limits_are_tapped_without_changing_bytes() {
        let raw = b"event: codex.rate_limits\ndata: {\"type\":\"codex.rate_limits\",\"rate_limits\":{\"primary\":{\"used_percent\":25.0,\"reset_at\":123},\"secondary\":{\"used_percent\":50.0,\"reset_at\":456}}}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":4}}}\n\n";
        let usage = Arc::new(Mutex::new((0, 0)));
        let rates = Arc::new(Mutex::new(None));
        let usage_out = usage.clone();
        let rates_out = rates.clone();
        let body = wrap_streaming_body_with_codex_rates(
            Body::from(Bytes::from_static(raw)),
            Arc::new(move |input, output| *usage_out.lock().unwrap() = (input, output)),
            Arc::new(move |rate| *rates_out.lock().unwrap() = Some(rate)),
        );
        let returned = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        assert_eq!(returned.as_ref(), raw);
        assert_eq!(*usage.lock().unwrap(), (10, 4));
        let rate = rates.lock().unwrap().clone().unwrap();
        assert_eq!(rate.primary_used_percent, Some(25.0));
        assert_eq!(rate.secondary_reset_at, Some(456));
    }
}

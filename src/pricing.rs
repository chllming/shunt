//! Anthropic / OpenAI API pricing table and cost helpers.
//!
//! Prices are charged per million tokens. We use the public list prices as
//! of June 2026 — close enough for "what would this have cost on the API?" display.

/// Cost in USD if the given tokens had been sent through the public API.
pub fn api_cost_usd(model: &str, input_tokens: u64, output_tokens: u64) -> f64 {
    let (input_price, output_price) = model_prices(model);
    (input_tokens as f64 / 1_000_000.0) * input_price
        + (output_tokens as f64 / 1_000_000.0) * output_price
}

/// Strict pricing lookup used by the hard overflow gate. Unlike the savings
/// display, an unknown model is rejected rather than assigned an optimistic
/// default price.
pub fn strict_api_cost_usd(model: &str, input_tokens: u64, output_tokens: u64) -> Option<f64> {
    let (input_price, output_price) = known_model_prices(model)?;
    Some(
        (input_tokens as f64 / 1_000_000.0) * input_price
            + (output_tokens as f64 / 1_000_000.0) * output_price,
    )
}

fn known_model_prices(model: &str) -> Option<(f64, f64)> {
    if model.contains("fable") || model.contains("mythos") {
        Some((10.0, 50.0))
    } else if model.contains("opus-4-5")
        || model.contains("opus-4-6")
        || model.contains("opus-4-7")
        || model.contains("opus-4-8")
    {
        Some((5.0, 25.0))
    } else if model.contains("opus") {
        Some((15.0, 75.0))
    } else if model.contains("haiku") {
        Some((1.0, 5.0))
    } else if model.contains("sonnet") || model.is_empty() {
        Some((3.0, 15.0))
    } else if model.starts_with("gpt-4")
        || model.starts_with("o1")
        || model.starts_with("o3")
        || model.starts_with("gpt-5")
    {
        Some((5.0, 15.0))
    } else {
        None
    }
}

/// (input_price_per_mtok, output_price_per_mtok) in USD.
fn model_prices(model: &str) -> (f64, f64) {
    known_model_prices(model).unwrap_or((3.0, 15.0))
}

/// Format a dollar amount compactly: "$0.04", "$1.23", "$840", "$4.2k".
pub fn fmt_cost(usd: f64) -> String {
    if usd >= 10_000.0 {
        format!("${:.0}k", usd / 1_000.0)
    } else if usd >= 1_000.0 {
        format!("${:.1}k", usd / 1_000.0)
    } else if usd >= 1.0 {
        format!("${:.2}", usd)
    } else if usd >= 0.01 {
        format!("${:.2}", usd)
    } else if usd > 0.0 {
        "<$0.01".to_owned()
    } else {
        "$0".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_prices_current_lineup() {
        // Pinned to public list prices as of June 2026 (Claude Code v2.1.181).
        assert_eq!(model_prices("claude-fable-5"), (10.0, 50.0));
        assert_eq!(model_prices("claude-mythos-5"), (10.0, 50.0));
        assert_eq!(model_prices("claude-opus-4-8"), (5.0, 25.0));
        assert_eq!(model_prices("claude-opus-4-7"), (5.0, 25.0));
        assert_eq!(model_prices("claude-opus-4-6"), (5.0, 25.0));
        assert_eq!(model_prices("claude-opus-4-1"), (15.0, 75.0));
        assert_eq!(model_prices("claude-sonnet-4-6"), (3.0, 15.0));
        assert_eq!(model_prices("claude-haiku-4-5-20251001"), (1.0, 5.0));
    }

    #[test]
    fn api_cost_matches_rate() {
        // 1M in + 1M out on opus-4-8 = $5 + $25.
        assert!((api_cost_usd("claude-opus-4-8", 1_000_000, 1_000_000) - 30.0).abs() < 1e-9);
    }
}

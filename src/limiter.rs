//! Adaptive per-account admission control.
//!
//! Under heavy concurrency the pool has aggregate quota to spare, but each
//! account has a short-term burst ceiling on Anthropic's side. Firing requests
//! and reacting to the resulting `429`s makes every account cool at once and
//! stalls the whole pool. Instead, this limiter paces requests *under* each
//! account's proven burst tolerance using AIMD (additive-increase,
//! multiplicative-decrease), the same control law TCP uses for congestion:
//!
//! - each sustained success nudges the account's allowed in-flight concurrency
//!   up by a small increment (additive increase);
//! - a burst `429` halves it (multiplicative decrease);
//! - requests are admitted only while `in_flight < limit`.
//!
//! The limiter also tracks a decaying `429` rate per account so routing can
//! prefer calmer lanes. It is intentionally simple and lock-based: the hot path
//! only does a couple of map lookups under a `parking_lot` mutex.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// Floor for an account's in-flight limit — always allow a little concurrency
/// so a single account can still make progress when it is the only option.
const MIN_LIMIT: f64 = 2.0;
/// Ceiling for an account's in-flight limit. `burst_rpm_limit` remains a
/// separate, coarser upper bound; this caps per-account concurrency.
const MAX_LIMIT: f64 = 32.0;
/// Starting limit for a freshly seen account.
const START_LIMIT: f64 = 4.0;
/// Additive increase applied per successful response.
const AI_STEP: f64 = 0.5;
/// Multiplicative decrease factor applied on a burst 429.
const MD_FACTOR: f64 = 0.5;
/// EWMA weight for the recent-429 signal (per event).
const RATE_ALPHA: f64 = 0.3;

#[derive(Debug, Clone)]
struct AccountLimit {
    /// Current AIMD concurrency limit (fractional; compared via floor).
    limit: f64,
    /// Requests currently in flight against this account.
    in_flight: u32,
    /// Decaying 0..1 signal of how often this account has been 429ing.
    recent_429: f64,
}

impl Default for AccountLimit {
    fn default() -> Self {
        Self {
            limit: START_LIMIT,
            in_flight: 0,
            recent_429: 0.0,
        }
    }
}

/// Thread-safe AIMD admission limiter shared across request handlers.
#[derive(Clone)]
pub struct AdmissionLimiter {
    inner: Arc<Mutex<HashMap<String, AccountLimit>>>,
}

impl Default for AdmissionLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl AdmissionLimiter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Try to admit one request for `account`. Returns true and reserves an
    /// in-flight slot when `in_flight < floor(limit)`; returns false otherwise.
    /// A reserved slot MUST be released with [`release`](Self::release).
    pub fn try_acquire(&self, account: &str) -> bool {
        let mut map = self.inner.lock();
        let entry = map.entry(account.to_owned()).or_default();
        if (entry.in_flight as f64) < entry.limit.floor().max(MIN_LIMIT) {
            entry.in_flight += 1;
            true
        } else {
            false
        }
    }

    /// Release a previously acquired slot.
    pub fn release(&self, account: &str) {
        let mut map = self.inner.lock();
        if let Some(entry) = map.get_mut(account) {
            entry.in_flight = entry.in_flight.saturating_sub(1);
        }
    }

    /// Record a successful response: additive-increase the limit and decay the
    /// 429 signal toward zero.
    pub fn on_success(&self, account: &str) {
        let mut map = self.inner.lock();
        let entry = map.entry(account.to_owned()).or_default();
        entry.limit = (entry.limit + AI_STEP).min(MAX_LIMIT);
        entry.recent_429 = (entry.recent_429 * (1.0 - RATE_ALPHA)).max(0.0);
    }

    /// Record a burst 429: multiplicative-decrease the limit and raise the 429
    /// signal toward one. `529`/overload should NOT call this (not per-account).
    pub fn on_burst_429(&self, account: &str) {
        let mut map = self.inner.lock();
        let entry = map.entry(account.to_owned()).or_default();
        entry.limit = (entry.limit * MD_FACTOR).max(MIN_LIMIT);
        entry.recent_429 = entry.recent_429 * (1.0 - RATE_ALPHA) + RATE_ALPHA;
    }

    /// Current recent-429 signal (0..1) for routing preference. Unknown = 0.
    pub fn recent_429(&self, account: &str) -> f64 {
        self.inner
            .lock()
            .get(account)
            .map(|e| e.recent_429)
            .unwrap_or(0.0)
    }

    /// Whether the account currently has a free admission slot.
    pub fn has_free_slot(&self, account: &str) -> bool {
        let map = self.inner.lock();
        match map.get(account) {
            Some(e) => (e.in_flight as f64) < e.limit.floor().max(MIN_LIMIT),
            None => true,
        }
    }

    /// Snapshot of (in_flight, limit, recent_429) for observability.
    pub fn snapshot(&self) -> HashMap<String, (u32, f64, f64)> {
        self.inner
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), (v.in_flight, v.limit, v.recent_429)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_up_to_floor_of_limit() {
        let l = AdmissionLimiter::new();
        // START_LIMIT = 4 → 4 slots, then refuse.
        assert!(l.try_acquire("a"));
        assert!(l.try_acquire("a"));
        assert!(l.try_acquire("a"));
        assert!(l.try_acquire("a"));
        assert!(!l.try_acquire("a"), "5th should be refused at limit 4");
        l.release("a");
        assert!(l.try_acquire("a"), "slot freed → admit again");
    }

    #[test]
    fn additive_increase_grows_capacity() {
        let l = AdmissionLimiter::new();
        for _ in 0..8 {
            l.on_success("a");
        }
        // 4 + 8*0.5 = 8 → 8 concurrent slots.
        let mut admitted = 0;
        for _ in 0..20 {
            if l.try_acquire("a") {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 8, "limit grew to 8 via additive increase");
    }

    #[test]
    fn multiplicative_decrease_on_429() {
        let l = AdmissionLimiter::new();
        // Grow to 8 first.
        for _ in 0..8 {
            l.on_success("a");
        }
        l.on_burst_429("a"); // 8 → 4
        l.on_burst_429("a"); // 4 → 2
        let mut admitted = 0;
        for _ in 0..10 {
            if l.try_acquire("a") {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 2, "two 429s halved 8→4→2");
    }

    #[test]
    fn limit_never_below_min() {
        let l = AdmissionLimiter::new();
        for _ in 0..20 {
            l.on_burst_429("a");
        }
        assert!(l.try_acquire("a"));
        assert!(l.try_acquire("a"), "MIN_LIMIT keeps 2 slots available");
        assert!(!l.try_acquire("a"), "but not more than the floor of 2");
    }

    #[test]
    fn recent_429_rises_and_decays() {
        let l = AdmissionLimiter::new();
        assert_eq!(l.recent_429("a"), 0.0);
        l.on_burst_429("a");
        let hot = l.recent_429("a");
        assert!(hot > 0.0, "429 raises the signal");
        for _ in 0..10 {
            l.on_success("a");
        }
        assert!(l.recent_429("a") < hot, "successes decay the signal");
    }

    #[test]
    fn has_free_slot_tracks_in_flight() {
        let l = AdmissionLimiter::new();
        assert!(l.has_free_slot("fresh"), "unknown account has slots");
        for _ in 0..4 {
            l.try_acquire("a");
        }
        assert!(!l.has_free_slot("a"), "saturated at limit 4");
        l.release("a");
        assert!(l.has_free_slot("a"));
    }
}

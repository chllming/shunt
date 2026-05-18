/// `shunt remote <url>` — polls a remote shunt /status endpoint and fires
/// local system notifications when account state changes (rate limits, reauth
/// required, cooldown resumed, all offline).
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::Deserialize;

use crate::term::{bold, cyan, dim, fmt_duration_ms, green, red, yellow};

// ---------------------------------------------------------------------------
// /status response types (subset of what the server sends)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
struct AccountStatus {
    name: String,
    #[serde(default)]
    available: bool,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    auth_failed: bool,
    #[serde(default)]
    cooldown_until_ms: u64,
}

#[derive(Debug, Deserialize)]
struct StatusResponse {
    #[serde(default)]
    accounts: Vec<AccountStatus>,
}

// ---------------------------------------------------------------------------
// State snapshot for diffing
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of a single account, computed at each poll.
#[derive(Debug, Clone)]
struct Snap {
    available: bool,
    auth_failed: bool,
    /// true if `cooldown_until_ms > now_ms` at snapshot time
    cooling: bool,
    disabled: bool,
}

impl Snap {
    fn from_status(acc: &AccountStatus, now_ms: u64) -> Self {
        Self {
            available: acc.available,
            auth_failed: acc.auth_failed,
            cooling: acc.cooldown_until_ms > now_ms,
            disabled: acc.disabled,
        }
    }
}

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

/// Cooldowns shorter than this are transient noise — no notification.
const LONG_COOLDOWN_MS: u64 = 5 * 60_000;
/// Minimum gap between "all accounts offline" notifications.
const ALL_OFFLINE_NOTIFY_COOLDOWN: Duration = Duration::from_secs(3_600);

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run_remote(base_url: String, interval_secs: u64) -> Result<()> {
    let status_url = format!("{}/status", base_url.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    print_header(&base_url, interval_secs);

    let mut prev: HashMap<String, Snap> = HashMap::new();
    let mut first_poll = true;
    let mut was_unreachable = false;

    // Throttle state — prevent duplicate notifications for the same event.
    // Maps account name → cooldown_until_ms we already notified about.
    let mut notified_cooldown: HashMap<String, u64> = HashMap::new();
    // Accounts we have already fired a "reauth required" notification for.
    let mut notified_auth_failed: HashSet<String> = HashSet::new();
    // Timestamp of the last "all offline" notification.
    let mut last_all_offline: Option<Instant> = None;
    // Whether all accounts were unavailable on the previous poll.
    let mut was_all_offline = false;

    loop {
        let now_ms = now_ms();

        match fetch_status(&client, &status_url).await {
            Ok(status) => {
                if was_unreachable {
                    println!("  {}  reconnected to {}", green("✓"), cyan(&base_url));
                    was_unreachable = false;
                }

                if first_poll {
                    // Initialise snapshot; print current state but no notifications.
                    print_initial_state(&status.accounts, now_ms);
                    for acc in &status.accounts {
                        prev.insert(acc.name.clone(), Snap::from_status(acc, now_ms));
                    }
                    first_poll = false;
                } else {
                    diff_and_notify(
                        &status.accounts,
                        &prev,
                        now_ms,
                        &mut notified_cooldown,
                        &mut notified_auth_failed,
                        &mut last_all_offline,
                        &mut was_all_offline,
                    );
                    // Rebuild snapshot for next cycle.
                    prev.clear();
                    for acc in &status.accounts {
                        prev.insert(acc.name.clone(), Snap::from_status(acc, now_ms));
                    }
                }
            }
            Err(e) => {
                if !was_unreachable {
                    println!("  {}  cannot reach {}  ·  {}", red("✗"), base_url, dim(&e.to_string()));
                    was_unreachable = true;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

// ---------------------------------------------------------------------------
// State diffing + notification dispatch
// ---------------------------------------------------------------------------

fn diff_and_notify(
    accounts: &[AccountStatus],
    prev: &HashMap<String, Snap>,
    now_ms: u64,
    notified_cooldown: &mut HashMap<String, u64>,
    notified_auth_failed: &mut HashSet<String>,
    last_all_offline: &mut Option<Instant>,
    was_all_offline: &mut bool,
) {
    let all_unavailable = accounts.iter().all(|a| !a.available);

    for acc in accounts {
        let Some(p) = prev.get(&acc.name) else { continue };

        // ── Reauth required (newly auth_failed) ─────────────────────────────
        if acc.auth_failed && !p.auth_failed && !notified_auth_failed.contains(&acc.name) {
            let msg = format!(
                "Account '{}' needs re-authorization. Run `shunt add-account`.",
                acc.name
            );
            println!("  {}  [{}]  reauth required", red("✗"), yellow(&acc.name));
            crate::notify::notify("shunt: Reauth Required", &msg, "Basso");
            notified_auth_failed.insert(acc.name.clone());
        }
        // Clear flag when the account recovers so we can notify again next time.
        if !acc.auth_failed {
            notified_auth_failed.remove(&acc.name);
        }

        // ── Entered cooldown (newly, long enough to matter) ──────────────────
        let curr_cooling = acc.cooldown_until_ms > now_ms;
        if curr_cooling && !p.cooling {
            let remaining_ms = acc.cooldown_until_ms - now_ms;
            let last_cdl = notified_cooldown.get(&acc.name).copied().unwrap_or(0);
            if remaining_ms >= LONG_COOLDOWN_MS && acc.cooldown_until_ms != last_cdl {
                let mins = remaining_ms / 60_000;
                let msg = format!(
                    "Account '{}' hit quota limit — cooling {}m.",
                    acc.name, mins
                );
                println!(
                    "  {}  [{}]  rate limited — cooling {}",
                    yellow("⏸"),
                    yellow(&acc.name),
                    yellow(&fmt_duration_ms(remaining_ms)),
                );
                crate::notify::notify("shunt: Rate Limited", &msg, "Ping");
                notified_cooldown.insert(acc.name.clone(), acc.cooldown_until_ms);
            }
        }

        // ── Resumed from cooldown ────────────────────────────────────────────
        if p.cooling && acc.available && !acc.auth_failed {
            println!("  {}  [{}]  back online", green("✓"), green(&acc.name));
            crate::notify::notify(
                "shunt: Account Resumed",
                &format!("Account '{}' is back online.", acc.name),
                "Glass",
            );
            notified_cooldown.remove(&acc.name);
        }

        // ── Account came back from disabled/auth_failed ──────────────────────
        if (p.auth_failed || p.disabled) && acc.available {
            println!("  {}  [{}]  back online (recovered)", green("✓"), green(&acc.name));
            crate::notify::notify(
                "shunt: Account Recovered",
                &format!("Account '{}' is back online.", acc.name),
                "Glass",
            );
        }
    }

    // ── All accounts offline ─────────────────────────────────────────────────
    if all_unavailable && !*was_all_offline {
        let should_notify = last_all_offline
            .map(|t| t.elapsed() >= ALL_OFFLINE_NOTIFY_COOLDOWN)
            .unwrap_or(true);
        if should_notify {
            println!("  {}  all accounts are offline", red("✗"));
            crate::notify::notify(
                "shunt: All Accounts Offline",
                "All accounts are offline or on cooldown.",
                "Basso",
            );
            *last_all_offline = Some(Instant::now());
        }
    }
    // Back from all-offline
    if *was_all_offline && !all_unavailable {
        println!("  {}  accounts back online", green("✓"));
    }
    *was_all_offline = all_unavailable;
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

fn print_header(base_url: &str, interval_secs: u64) {
    println!();
    println!("  {}  {}  {}", bold("◆"), bold("shunt"), dim("remote"));
    println!("  {}  {}", dim("·"), cyan(base_url));
    println!("  {}  polling every {}s  ·  press Ctrl-C to stop", dim("·"), interval_secs);
    println!();
}

fn print_initial_state(accounts: &[AccountStatus], now_ms: u64) {
    println!("  {}  connected — {} account(s)", green("✓"), accounts.len());
    for acc in accounts {
        let (sym, label) = if acc.auth_failed || acc.disabled {
            (red("✗"), red(&acc.name))
        } else if acc.cooldown_until_ms > now_ms {
            let rem = fmt_duration_ms(acc.cooldown_until_ms - now_ms);
            let label = format!("{}  cooling {}", acc.name, rem);
            (yellow("⏸"), yellow(&label))
        } else {
            (green("✓"), green(&acc.name))
        };
        println!("    {}  {}", sym, label);
    }
    println!();
}

// ---------------------------------------------------------------------------
// HTTP helper
// ---------------------------------------------------------------------------

async fn fetch_status(client: &reqwest::Client, url: &str) -> Result<StatusResponse> {
    Ok(client.get(url).send().await?.json::<StatusResponse>().await?)
}

// ---------------------------------------------------------------------------
// Time helper
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

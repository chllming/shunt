# shunt

A local proxy that pools multiple Claude (and Codex) accounts behind a single endpoint, routing requests across accounts to maximize your available rate limits.

```
  ─┐    shunt  v0.1.50
  ─┼─▶  3 accounts  ·  http://127.0.0.1:8082
  ─┘    Proxying Claude API across multiple accounts
```

## What it does

Claude's rate limits are per-account. If you hit your 5-hour or weekly limit on one account, you're stuck waiting. Shunt sits in front of the Anthropic API and automatically routes each request to whichever account has the most remaining capacity — so you get the combined limits of all your accounts.

- **Least-utilization routing** — tracks live `anthropic-ratelimit-unified-*` headers across both 5h and 7d windows, always picking the account with the most headroom
- **Auto-failover** — if one account returns 429/529, the next request goes to another automatically
- **Auto-resume** — if all accounts are exhausted, requests are held open and retried the moment the first account's limit resets (up to 5 hours), so your tools never see a hard failure mid-session
- **Strong resume** — after a cooldown expires, shunt pre-fetches quota so the next request routes instantly instead of discovering limits cold
- **Transparent** — drop-in replacement for `api.anthropic.com`; works with Claude Code, the SDK, or any tool that speaks the Anthropic API
- **Savings tracker** — shows how much you'd have paid for the same usage at API prices

## Install

```bash
curl -sSf https://raw.githubusercontent.com/ramc10/shunt/main/install.sh | sh
```

This downloads the right pre-built binary for your OS and arch — no Rust or other dependencies needed.

Or via cargo:

```bash
cargo install shunt-proxy
```

## Setup

Shunt uses OAuth — the same session Claude Code uses — so there's no API key to manage.

**Step 1: Import your existing Claude Code session**

```bash
shunt setup
```

This auto-imports the credentials Claude Code already has on disk. Takes a second.

**Step 2: Add a second account**

Log out of Claude Code, log in with your second Claude account, then:

```bash
shunt add-account secondary
```

Or give it any name you want (`work`, `personal`, etc.). This opens a browser for OAuth authorization.

**Step 3: Start the proxy**

```bash
shunt start
```

The proxy starts in the background and your terminal is immediately returned. To point Claude Code at it:

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8082
```

Add that to your `.zshrc` / `.bashrc` (shunt setup offers to do this automatically).

## Commands

```bash
shunt start              # Start (or restart) the proxy in the background
shunt start --foreground # Keep it in the terminal (for debugging)
shunt start --verbose    # Debug logging: routing decisions, token refresh details
shunt stop               # Stop the running proxy
shunt restart            # Restart the proxy
shunt status             # Show accounts, rate limit bars, reset times, savings
shunt monitor            # Live fullscreen TUI dashboard
shunt logs               # Last 50 lines of the proxy log
shunt logs -f            # Follow log output in real time
shunt logs -n 100        # Last N lines
shunt add-account <name> # Add another Claude or Codex account
shunt remove-account <name> # Remove an account
shunt logout [name]      # Log out of one account (or --all)
shunt use [account]      # Pin routing to a specific account (or "auto" to restore)
shunt share              # Expose the proxy to your LAN
shunt share --tunnel     # Expose via Cloudflare tunnel (any network)
shunt connect <code>     # Connect another device to a shared proxy
shunt remote             # Generate a watch code (host mode)
shunt remote <code>      # Watch a remote instance; receive local notifications (client mode)
shunt update             # Update shunt to the latest release
shunt setup              # First-time setup
```

### Status output

```
── ACCOUNTS ────────────────────────────────────────────────

  ✓  work        Claude Pro  you@example.com          available     1.2M tok
          5h window  ████████░░░░░░░░░░  61% remaining  ok  resets in 2h 14m
          7d window  ███░░░░░░░░░░░░░░░  79% remaining  ok  resets in 4d 6h
          Extra usage  available

  ✓  personal    Claude Pro  alt@example.com          available     fresh
          5h window  ░░░░░░░░░░░░░░░░░░  100% remaining  ok

── SAVINGS ─────────────────────────────────────────────────

  Today: 2.3M tok  ·  $6.12  ·  All-time: $48.30
```

### Monitor TUI

`shunt monitor` opens a fullscreen live dashboard showing account utilization bars, cooldown countdowns, request history, and cost — updates in real time as requests flow through.

## How routing works

Every response from the Anthropic API includes `anthropic-ratelimit-unified-5h-utilization` and `anthropic-ratelimit-unified-7d-utilization` headers (floats from 0–1). Shunt captures these and always routes the next request to the account with the **lowest utilization across the most-urgent window**. Fresh accounts (no data yet) are treated as 0% utilized and get highest priority.

If a request returns 429 or 529, shunt reads the `reset_5h` (or `reset_7d`) timestamp from the response headers and sets that account's cooldown to exactly when the window resets — no polling. It then retries with the next-best account automatically.

If every account is exhausted at once, shunt holds the request open and waits until the soonest account recovers (sleeping directly until the reset time), then retries transparently. Requests will wait up to 5 hours before giving up with a 503.

After a cooldown expires, shunt pre-fetches the account's quota state so the routing decision for the next real request is accurate immediately.

## Sharing & remote access

### Share with your LAN

```bash
shunt share
```

Binds the proxy to your LAN IP and prints a share code. Anyone on your network can run `shunt connect <code>` to automatically configure their Claude Code to route through your proxy.

### Share over any network (Cloudflare tunnel)

```bash
shunt share --tunnel
```

Creates a public Cloudflare tunnel — works across different networks, VPNs, etc. Same share code flow.

### Connect another device

```bash
shunt connect SC-a3f2b1c4d5e6f7a8b9
```

Fetches the proxy URL and API key for the given share code and writes them to your shell profile — Claude Code on that device starts routing through the shared proxy immediately.

### Remote notifications

Watch a remote shunt instance from another machine and receive local system notifications (rate limit hits, account resume, reauth needed):

```bash
# On the host machine:
shunt remote
# prints: RM-a3f2b1c4...

# On the watching device:
shunt remote RM-a3f2b1c4...
```

## System notifications

Shunt fires native system notifications when:

- An account hits a rate limit (429/529)
- An exhausted account resumes (limit reset)
- An account needs re-authentication

## Pin routing

Force all requests through a specific account:

```bash
shunt use work      # Pin to 'work'
shunt use auto      # Restore automatic least-utilization routing
shunt use           # Interactive picker
```

## Codex / OpenAI routing

Shunt supports two Codex use cases:

1. **Route Codex through your Claude pool** — translate OpenAI requests to Anthropic format on the fly. No OpenAI/ChatGPT subscription needed.
2. **Use a ChatGPT Pro account directly** — add your ChatGPT Pro account to the pool so Codex CLI authenticates through shunt. No separate login required.

When any OpenAI/Codex account is configured, shunt starts a second proxy on port **8083** that speaks the OpenAI API format.

### Add a Codex account (ChatGPT Pro)

```bash
shunt add-account codex
```

Select **OpenAI / Codex** as the provider when prompted. Shunt uses the Codex device-code flow — it prints a short code, opens your browser, and completes auth automatically.

After adding the account, shunt automatically writes `~/.codex/auth.json` with the correct credentials. You can run `codex` immediately without logging in again.

### Run Codex CLI

```bash
codex
```

Shunt keeps `~/.codex/auth.json` up to date whenever tokens are refreshed, so you never need to re-authenticate in the Codex CLI.

### Route other OpenAI-compatible tools through Claude

If you want to use Codex (or any OpenAI-compatible tool) against your **Claude** accounts instead of ChatGPT, point it at shunt's OpenAI-compat endpoint:

```bash
export OPENAI_BASE_URL=http://127.0.0.1:8083
export OPENAI_API_KEY=dummy   # any non-empty value; shunt ignores it
codex
```

Add these to your shell profile to make them permanent. Requests are translated from OpenAI format to Anthropic format and routed through your Claude pool.

### Cross-protocol interop

Shunt routes transparently across provider types. If you have both Claude and Codex accounts configured, any request can be satisfied by any compatible account — Claude Code requests can overflow to a Codex pool and vice versa.

### Model mapping (Claude routing)

When routing through Claude, OpenAI model names are mapped automatically:

| OpenAI model | Claude model |
|---|---|
| `gpt-4o`, `o1`, `o3`, `gpt-5` | `claude-opus-4-6` |
| `gpt-4o-mini`, `o1-mini`, `o3-mini` | `claude-haiku-4-5-20251001` |
| anything else | `claude-sonnet-4-6` |

Claude model names (e.g. `claude-sonnet-4-6`) pass through as-is.

### What's supported

- `POST /v1/chat/completions` — streaming and non-streaming, system messages, tool calls, temperature, stop sequences
- `GET /v1/models` — returns available Claude models in OpenAI format
- Everything else is forwarded to the ChatGPT upstream as-is

## Configuration

Config lives at `~/Library/Application Support/shunt/config.toml` (macOS) or `~/.config/shunt/config.toml` (Linux):

```toml
[server]
host = "127.0.0.1"
port = 8082
log_level = "info"

[[accounts]]
name = "work"
plan_type = "pro"

[[accounts]]
name = "personal"
plan_type = "pro"
```

Credentials are stored separately in `credentials.json` (never in the config file).

## Files

| File | Location |
|------|----------|
| Config | `~/Library/Application Support/shunt/config.toml` |
| Credentials | `~/Library/Application Support/shunt/credentials.json` |
| Logs | `~/Library/Application Support/shunt/proxy.log` |
| Status API | `http://127.0.0.1:8082/status` |

## Requirements

- One or more Claude Pro / Max accounts
- Claude Code installed (shunt borrows its OAuth credentials)

## Notes

- Accounts need to be **different Claude logins** — two sessions from the same account won't double your limits
- Shunt only proxies `/v1/messages` and `/v1/messages/count_tokens` for the Anthropic endpoint — everything else passes through untouched
- `shunt start` automatically kills and replaces any running instance

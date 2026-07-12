<div align="center">

<img src="https://shunt.live/assets/shunt_logo-removebg-preview.png" alt="shunt" width="96" style="image-rendering:pixelated"><br>

# shunt

**Native Claude and Codex subscription pools behind one local daemon.**

[![crates.io](https://img.shields.io/crates/v/shunt-proxy.svg)](https://crates.io/crates/shunt-proxy)
[![downloads](https://img.shields.io/crates/d/shunt-proxy)](https://crates.io/crates/shunt-proxy)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![platform](https://img.shields.io/badge/macOS%20%7C%20Linux-lightgrey)

<a href="https://shunt.live" target="_blank">shunt.live</a>

</div>

---

Shunt is a local proxy that combines subscription accounts into two isolated native endpoints: Anthropic Messages for Claude Code on port 8082 and OpenAI Responses for stock Codex on port 8083. It routes within each provider pool, preserves native streaming/tool semantics, and can delegate explicit cross-provider work through an isolated MCP bridge.

<div align="center">
<img src="https://raw.githubusercontent.com/ramc10/shunt/main/diagram.svg" width="600">
</div>

**Works with:** Claude Code · Cursor · Codex CLI · Windsurf · any OpenAI or Anthropic SDK

**Providers:** Anthropic · OpenAI · Gemini · Groq · Mistral · DeepSeek · OpenRouter · Together · Fireworks · Ollama · local models

---

## Install

**macOS / Linux — interactive installer:**

```bash
npx vibe-shunt
```

Choose the release, install directory, credential mode, client integration, and login service interactively. Website3 user-vault mode is recommended; local-only mode remains available. For an unattended install with recommended defaults, run `npx vibe-shunt --yes`. See all automation options with `npx vibe-shunt --help`.

**shell installer:**

```bash
curl -sSf https://raw.githubusercontent.com/ramc10/shunt/main/install.sh | sh
```

**via Cargo**

```bash
cargo install shunt-proxy
```

---

## Quick start

```bash
shunt setup --mode website --install-clients # Website3 login + user vault
# or: shunt setup --mode local --install-clients
shunt start      # start the proxy
```

That's it. Claude Code and your other tools route through shunt automatically.

Existing flat and schema-v2 configs migrate automatically on start. Preview the backup-first schema-v3 attachment migration with `shunt migrate --dry-run`. See [Native pools and bridge operations](docs/native-pools-and-bridge.md) for configuration, Website3 leases, API-overflow budgets, stock Codex setup, and bridge security.

Add more accounts to grow your pool:

```bash
shunt add-account personal   # another Claude account (OAuth)
shunt add-account work       # another Claude account
shunt add-account codex      # ChatGPT Pro (device-code flow)
shunt add-account groq       # Groq (prompts for API key)
```

---

## What shunt does

**Combines your rate limits**

N accounts = N × the limit you already pay for. Three Claude Pro accounts means three 5-hour windows and three 7-day windows, pooled and automatically load-balanced.

**Fails over silently**

When an account hits its limit, the next request goes to whichever account has capacity. No 429 errors, no broken loops. If every account is drained, shunt holds the connection open and retries the moment the first one resets.

**Live status**

```bash
shunt status
```

```
  ◆  main                                        Claude Pro
    you@example.com

    ✓  available
    5h  ████████████████░░░░  81% left  ·  resets in 2h 28m
    7d  █████████████████░░░  85% left  ·  resets in 4d 16h

  ────────────────────────────────────────────────────────

  ◆  work                                        Claude Pro
    alt@example.com

    ✓  available
    5h  ██████████████████░░  92% left  ·  resets in 2h 28m
    7d  █████████████░░░░░░░  65% left  ·  resets in 4d 5h
```

**Share with your team**

```bash
shunt share              # LAN sharing — prints a connect code
shunt share --tunnel     # any network via Cloudflare tunnel
shunt share <code>       # on another machine — configures everything
```

---

## Commands

```bash
shunt setup --install-clients # first-time setup + stock client/MCP entries
shunt setup --mode website    # Website3 device login and user inventory
shunt setup --mode website --install-clients --manual-swarm \
  --manual-swarm-max-agents 8 --manual-swarm-default-target auto
shunt website inventory       # redacted remote inventory
shunt website add-key --provider groq --label personal # vault + attach
shunt inventory               # attachments + redacted local store
shunt migrate --dry-run       # preview schema-v3 migration
shunt migrate --apply    # back up and migrate now
shunt start              # start the proxy
shunt stop               # stop the proxy
shunt restart
shunt status             # account utilization
shunt status --pool codex
shunt monitor            # live fullscreen dashboard
shunt logs               # recent logs
shunt logs -f            # follow logs
shunt config             # manage accounts interactively
shunt add-account <name> # add an account or provider
shunt add-account work --provider openai --pool codex
shunt remove-account <name>   # detach only; source credential is retained
shunt delete-credential <name># explicitly delete a detached local credential
shunt logout [name]      # log out of an account
shunt use [account]      # pin routing to a specific account
shunt use --pool codex [account]
shunt use auto           # restore automatic routing
shunt model set <name>   # force all requests through a model
shunt model clear        # restore client-supplied model
shunt strategy set <name># change routing strategy at runtime
shunt bridge mcp --caller codex # MCP adapter (normally installed by setup)
shunt share              # share on LAN
shunt share --tunnel     # share via Cloudflare tunnel
shunt share <code>       # connect to a shared proxy
shunt disconnect         # revert to localhost-only
shunt live               # persistent tunnel via relay
shunt update             # update to latest
```

The `vibe-shunt` installer can install and enable the bounded Manual Swarm workflow. Claude exposes it as `/auto-swarm`; Codex installs the `auto-swarm@shunt` plugin and invokes its skill as `$auto-swarm` or through `/skills`. A plan requires an explicit user-authorized Space, Swarm, and subscription list—Shunt never guesses an account or widens inventory. Launch approval never authorizes apply: workers produce isolated SwarmFS changes and the parent checkout changes only after a separate reviewed `manual_swarm_apply` confirmation.

The local production path requires a digest-pinned Auto Swarm worker container and Website3's Ed25519 public verifier key (`SHUNT_MANUAL_SWARM_PUBLIC_KEY`). The private signing key never leaves Website3. Shunt verifies the transient signed grant on every Claude or Codex request and maps its opaque subscription ids to the installation's schema-v3 attachment inventory before routing; missing mappings, expired grants, and ungranted lanes fail closed. The legacy host process runner remains unavailable. DigitalOcean and Hetzner also remain unavailable until their hosted worker/gateway lifecycle passes the documented live gates. See `docs/manual-swarm-plan.md` for the exact readiness boundary.

Allowlisted bridge jobs validate and record normalized hostname/IP patterns; Claude enforces them in its sandbox, while Codex bridge workers intentionally run with full local permissions and no bubblewrap. See the operations guide for policy syntax and the authenticated release smoke gate.

---

MIT License

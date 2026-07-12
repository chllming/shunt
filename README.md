<div align="center">

<img src="https://shunt.live/assets/shunt_logo-removebg-preview.png" alt="shunt" width="96" style="image-rendering:pixelated"><br>

# shunt

**Native Claude and Codex subscription pools behind one local daemon.**

[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![platform](https://img.shields.io/badge/macOS%20%7C%20Linux-lightgrey)

<a href="https://shunt.live" target="_blank">shunt.live</a>

</div>

---

## Open-core lineage

This repository is the public, MIT-licensed open core of Beyond Work's Shunt fork. It descends from [ramc10/shunt](https://github.com/ramc10/shunt) and contains the generic local proxy, native Claude/Codex pools, routing, classifier lane, and MCP bridge. Hosted identity and credential brokerage, external secret authority, signed distributed-worker grants, policy integrations, and isolated worker orchestration are maintained in the private Beyond Work adaptation and are not part of this distribution.

Shunt is a local proxy that combines subscription accounts into two isolated native endpoints: Anthropic Messages for Claude Code on port 8082 and OpenAI Responses for stock Codex on port 8083. It routes within each provider pool, preserves native streaming/tool semantics, and can delegate explicit cross-provider work through an isolated MCP bridge.

<div align="center">
<img src="https://raw.githubusercontent.com/chllming/shunt/main/diagram.svg" width="600">
</div>

**Works with:** Claude Code · Cursor · Codex CLI · Windsurf · any OpenAI or Anthropic SDK

**Providers:** Anthropic · OpenAI · Gemini · Groq · Mistral · DeepSeek · OpenRouter · Together · Fireworks · Ollama · local models

---

## Install

**macOS / Linux — interactive installer:**

```bash
npx vibe-shunt
```

Choose the release, install directory, client integration, and login service interactively. For an unattended install with recommended defaults, run `npx vibe-shunt --yes`. See all automation options with `npx vibe-shunt --help`.

**shell installer:**

```bash
curl -sSf https://raw.githubusercontent.com/chllming/shunt/main/install.sh | sh
```

**via Cargo**

```bash
cargo install --git https://github.com/chllming/shunt --locked shunt-proxy-core
```

---

## Quick start

```bash
shunt setup --install-clients  # import Claude + install managed client entries
shunt start      # start the proxy
```

That's it. Claude Code and your other tools route through shunt automatically.

Existing flat configs migrate automatically on start. Preview the backup-first migration with `shunt migrate --dry-run`. See [Native pools and bridge operations](docs/native-pools-and-bridge.md) for schema-v2 configuration, API-overflow budgets, stock Codex setup, and bridge security.

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
shunt migrate --dry-run  # preview schema-v2 migration
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
shunt remove-account <name>
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

Allowlisted bridge jobs validate and record normalized hostname/IP patterns; Claude enforces them in its sandbox, while Codex bridge workers intentionally run with full local permissions and no bubblewrap. See the operations guide for policy syntax and the authenticated release smoke gate.

---

MIT License

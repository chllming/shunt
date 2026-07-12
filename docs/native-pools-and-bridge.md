# Native pools and bridge operations

Shunt schema v3 retains schema v2's two independent native runtimes and adds explicit credential attachments:

- `claude` listens on `127.0.0.1:8082` and serves Anthropic Messages traffic from Anthropic subscription accounts, with an optional Anthropic API overflow lane.
- `codex` listens on `127.0.0.1:8083` and serves stock OpenAI Responses traffic from ChatGPT/Codex subscription accounts, with an optional OpenAI API overflow lane.
- The control API remains on `127.0.0.1:19081`. Legacy providers keep isolated provider-specific listeners and are not implicit fallback for either native pool.

There is no automatic Claude-to-Codex wire translation in native pools. Cross-provider work is explicit through the MCP agent bridge.

## Configuration

```toml
schema_version = 3

[server]
host = "127.0.0.1"
control_port = 19081
log_level = "info"

[secrets]
env_file = "/absolute/path/to/.env.local"

[pools.claude]
port = 8082
routing_strategy = "maximus"
fallback_models = ["claude-sonnet-4-6"]

[[pools.claude.accounts]]
name = "personal"
provider = "anthropic"
credential_source = "provider-cli"
credential_id = "claude/personal"

[pools.claude.overflow]
enabled = true
key_env = "ANTHROPIC_API_KEY"
daily_budget_usd = 500.0
max_output_tokens = 32768

[pools.codex]
port = 8083
routing_strategy = "maximus"
fallback_models = ["gpt-5.4"]

[[pools.codex.accounts]]
name = "personal"
provider = "openai"
credential_source = "provider-cli"
credential_id = "codex/personal"

[pools.codex.overflow]
enabled = true
key_env = "OPENAI_API_KEY"
daily_budget_usd = 500.0
max_output_tokens = 32768

[classifier]
enabled = true
upstream_url = "http://127.0.0.1:11434"
model = "qwen2.5:7b-instruct"
fail_closed = true

[bridge]
enabled = true
concurrency_per_provider = 2
queue_capacity = 32
timeout_secs = 1800
max_depth = 1
retention_hours = 24
network_ceiling = "allowlisted"
required_checks = ["cargo test"]
codex_fallback_models = ["gpt-5.4"]
claude_fallback_models = ["sonnet"]
```

The env-file path must be absolute. On Unix it must not be readable or writable by group or other users (`chmod 600`). Existing process environment values override file values. Shunt reads only keys explicitly selected by `api_key_env` or an overflow `key_env`; it does not inject the file into the process environment. `NPMJS`, `NPM_TOKEN`, and `NODE_AUTH_TOKEN` are never eligible runtime keys.

Credential sources are `local-store`, `provider-cli`, `env-file`, `website-broker`, and `none`. An account block is a routing attachment. `shunt remove-account` detaches it without deleting the source; `shunt delete-credential` deletes only a detached local-store entry.

Website3 mode uses a browser device flow and user-scoped Fabric inventory:

```bash
shunt setup --mode website --install-clients
shunt website inventory
```

Website3 and Fabric return short-lived access material only. Provider refresh tokens and Doppler credentials never reach Shunt. Successful leases are encrypted locally and may be used during an outage for at most one hour, never beyond provider expiry. Authorization denials fail closed and do not use the grace cache.

Overflow budgets are hard pre-dispatch gates. Shunt atomically reserves a conservative worst-case amount from body size, the configured output cap, and the model price before sending an API request. Actual usage reconciles a reservation. Unknown-price models are rejected, and reservations left unresolved by a crash remain charged for that UTC day.

## Migration and setup

```bash
shunt migrate --dry-run
shunt migrate --apply --env-file /absolute/path/to/.env.local
shunt setup --env-file /absolute/path/to/.env.local --install-clients
```

Migration is idempotent and backup-first. Legacy files are partitioned into native pools and upgraded directly to v3; schema-v2 files retain their pool topology and gain credential references. Legacy backups use `*.bak-v1`; v2-to-v3 config backups use `*.bak-v2`. `shunt start` applies the same migration automatically.

`--install-clients` backs up and minimally patches user client files. It installs a stock Codex Responses provider and registers the Shunt MCP bridge for Codex and Claude. Uninstall removes the managed `shunt-codex` and `shunt` MCP entries while preserving unrelated client configuration.

The installed stock Codex provider is equivalent to:

```toml
model_provider = "shunt-codex"

[model_providers.shunt-codex]
name = "Shunt Codex"
base_url = "http://127.0.0.1:8083/backend-api/codex"
wire_api = "responses"
supports_websockets = false

[model_providers.shunt-codex.auth]
command = "shunt"
args = ["client-token", "codex"]
refresh_interval_ms = 300000
```

Shunt serves `responses`, `responses/compact`, `models`, and `memories/trace_summarize`. It forwards normal Responses JSON and SSE bytes unchanged, preserves Codex session/tracing headers, strips client identity headers, and injects the selected account's access token, `ChatGPT-Account-ID`, and FedRAMP routing flag. Turn-state affinity is strict; failover stops after response streaming begins.

## Pool-aware operations

Legacy control aliases target the Claude pool. Use pool-qualified commands for Codex:

```bash
shunt status --pool codex
shunt use --pool codex personal
shunt model --pool codex set gpt-5.4
shunt strategy --pool codex set maximus
shunt add-account work --provider openai --pool codex --owner team-a
```

The equivalent control paths are `/pools/claude/...` and `/pools/codex/...` on port 19081.

## Cross-provider MCP bridge

The daemon owns the bounded bridge queue. Thin stdio adapters are launched with:

```bash
shunt bridge mcp --caller codex
shunt bridge mcp --caller claude
```

Tools are `consult_codex`, `consult_claude`, `delegate_best`, `bridge_wait`, and `bridge_cancel`. Every new job must provide an absolute workspace and an explicit network choice: `none`, `allowlisted` (with `allowedDomains`), or `unrestricted`. The request cannot exceed the operator's `network_ceiling`.

`allowedDomains` accepts exact hostnames and IP addresses plus leading `*.` DNS wildcards such as `*.github.com`. Entries are trimmed, normalized to lowercase, deduplicated, and retained as `allowed_domains` in the job status. URLs, ports, paths, blank entries, wildcard IP addresses, and the unrestricted bare `*` are rejected before a job enters the queue. Domains supplied with `none` or `unrestricted` are ignored because those policies have no effective domain list. The daemon ceiling still rejects requests above the configured policy.

The stdio adapter authenticates to the daemon with a per-install local bearer token; the bridge control route does not accept unauthenticated job submissions.

Workers run at the exact parent `HEAD` in detached Git worktrees with ephemeral client homes, direct provider API secrets removed, and recursion disabled. Claude workers use their platform sandbox. Codex workers intentionally run with full local permissions and `--dangerously-bypass-approvals-and-sandbox`, so they do not invoke bubblewrap. Consults and independent reviews remain task-constrained read-only by bridge behavior; patch/apply jobs retain a binary patch and redacted output for 24 hours by default.

Codex workers do not receive a Codex permission profile: Shunt passes the explicit full-access flag so environments without bubblewrap or user namespaces can run Codex. For Codex, `allowedDomains` is admission/audit metadata rather than an OS-level egress restriction; use the daemon ceiling and Claude workers when host-level network isolation is required.

If `codex` on `PATH` is a wrapper, set `SHUNT_CODEX_BIN` to the genuine Codex executable. Shunt supplies the full-access flag itself for every Codex bridge run.

Automatic apply requires all of the following:

1. configured checks and `git diff --check` pass in the worker;
2. an independent worker from the opposite provider emits the explicit approval marker;
3. parent `HEAD` still equals the recorded base;
4. changed paths do not overlap the parent's recorded or current dirty paths;
5. `git apply --check` succeeds while Shunt holds the repository apply lock.

If any gate fails, the parent is untouched and the patch remains in the job artifacts. Inspect retained jobs with `shunt bridge jobs` and `shunt bridge status ID`; cancel with `shunt bridge cancel ID`.

## Authenticated release smoke

Before landing a bridge or native-transport change, run an isolated smoke test with both stock clients:

1. Confirm `codex login status` and `claude auth status` report authenticated sessions, and ensure `SHUNT_CODEX_BIN` resolves to a genuine Codex executable.
2. Build Shunt and start the branch binary with temporary `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, and `XDG_CACHE_HOME` directories, three unused loopback ports, and a temporary schema-v3 config. Use pool-qualified credential references, disable the classifier and API overflow lanes, and keep the real `HOME` only for read-only provider-CLI imports when that source is explicitly selected.
3. Run a real `consult_codex` job against a disposable clean Git fixture. Verify the captured Codex session uses full-access execution without bubblewrap; treat `allowedDomains` as recorded admission metadata for this worker.
4. Run a real `consult_claude` job with `network = "none"` against the same fixture and require its expected sentinel response.
5. Verify both jobs completed, outputs contain no credential material, detached worktrees were removed, and the fixture's `HEAD` and working tree are unchanged. Stop the temporary daemon and remove its isolated directories.

The worktree bridge is supported on Linux, macOS, and WSL2. Native Windows is proxy-only.

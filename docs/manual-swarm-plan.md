# Manual Swarm plan

Status: control-plane and local fixture slice implemented; production execution gated

Date: 2026-07-11

Owners: Shunt, Auto Swarm, Website3/Fabric, and Steward

Implementation note (2026-07-11): the versioned grants, Website3/Fabric authorization boundary, Auto Swarm manual-session state machine, independent review contract, hardened Shunt MCP/apply client, Steward projections, Codex/Claude extensions, worker package, and `vibe-shunt` installer lifecycle are implemented and covered by contract/adversarial tests. Production execution remains deliberately unavailable: the direct local command executor cannot prove host-filesystem confinement or exact subscription-scoped Shunt routing, and the DigitalOcean/Hetzner Kubernetes adapter plus session-scoped worker gateway in phases 3–4 are not yet installed. Capability discovery reports those targets unavailable; it never silently falls back.

## 1. Outcome

Add an explicitly invoked **Manual Swarm** mode to Shunt. A developer remains in a normal Claude Code or Codex session and chooses when to start a bounded group of Auto Swarm Go coding workers. Workers can run locally or on the existing DigitalOcean/Hetzner SwarmFS substrates. The parent coding session remains the operator and integration authority.

Manual Swarm provides:

- parallel Go coding workers without enabling the resident Auto Swarm work reconciler;
- one immutable source snapshot and one isolated SwarmFS fork per worker;
- mixed Claude- and Codex-backed model lanes through Shunt;
- durable worker reports, artifacts, workspace snapshots, and `ChangeRef` records;
- explicit status, steering, cancellation, review, and apply operations;
- conflict-aware integration proposals returned to the parent coding session;
- Steward visibility into the session, workers, substrates, resources, and changes;
- DigitalOcean DOKS as the default hosted target and the registered Hetzner cluster as an explicit backup/fallback target.

Manual Swarm does not create unattended automode. It is a user-started, bounded execution session that terminates after producing a reviewed proposal or being cancelled.

## 2. Non-goals

Manual Swarm does not:

- run `autoswarm up` or the resident WorkItem reconciliation loop;
- automatically discover or expand project scope after launch;
- accept plans, evidence, work, or integration on behalf of the user;
- automatically land branches or merge pull requests;
- give workers Website3 sessions, provider refresh tokens, broad Doppler tokens, or `.env.local` files;
- allow several workers to edit one shared writable checkout;
- silently fall back from a requested hosted substrate to local execution;
- advertise domain allowlisting unless the selected runtime actually enforces it;
- replace full Auto Swarm mode for durable autonomous work, automatic successor runs, or continuous reconciliation.

## 3. Product modes

| Mode | Compute | Workspace | Lifecycle | Best use |
| --- | --- | --- | --- | --- |
| `local` | Local Go worker processes | Local BuildSwarmFS forks | Synchronous and bounded | Small tasks and offline development |
| `remote` | Kubernetes Jobs | Production SwarmFS snapshot/fork | Bounded session coordinator | Parallel implementation and integration |
| `managed` | Existing Auto Swarm runtime | Controller-selected SwarmFS | Full reconciled lifecycle | Autonomous governed work; outside this plan |

`remote` is the recommended Manual Swarm mode when a healthy production substrate is available. `local` uses the same session and result contracts so callers do not need a separate integration path.

## 4. User experience

### 4.1 Claude Code

Install a Claude skill at `~/.claude/skills/auto-swarm/SKILL.md` or the project equivalent. Claude Code supports direct `/skill-name` invocation, so the primary surface is:

```text
/auto-swarm implement the billing refactor
/auto-swarm plan --agents 5 --target auto the database migration
/auto-swarm status
/auto-swarm steer worker-3 focus on compatibility
/auto-swarm review
/auto-swarm apply
/auto-swarm stop
```

The skill contains `ultrathink` for the initial decomposition step, calls only Shunt MCP tools, and never shells directly into a hosted substrate.

### 4.2 Codex

Install an `auto-swarm` Codex plugin containing the reusable skill plus the Shunt MCP server configuration. The supported primary invocation is:

```text
$auto-swarm implement the billing refactor
```

The skill must also be discoverable through `/skills`, and its description should allow an ordinary prompt beginning with `auto-swarm:` to select it automatically. Do not depend on an undocumented arbitrary `/auto-swarm` Codex slash command. Codex documents plugins, skills, MCP, and `/skills` as the supported extension surfaces.

### 4.3 Interactive launch preview

Before starting remote mutations, the skill asks Shunt for a plan and renders:

```text
Manual Swarm preview
  objective: billing refactor
  target: DigitalOcean build-fra1
  agents: 5
  lanes: 3 Codex, 2 Claude
  workspace: 5 isolated SwarmFS forks
  mode: patch (no parent checkout mutation)
  maximum duration: 45m
```

The user confirms the preview unless an explicit non-interactive approval flag was supplied. Apply remains a separate action even when launch was pre-approved.

### 4.4 Ongoing interaction

The parent CLI receives compact events instead of raw worker transcripts:

```text
Manual Swarm msw_82f1 · DO build-fra1 · 5 workers

✓ api implementation       change ready
● database migration       running tests
✓ compatibility review     approved
! frontend integration     conflict in src/routes.ts
○ integration proposal     waiting

3 ChangeRefs · 1 conflict · 0 parent files changed
```

Inspect returns bounded redacted worker records and proposals. The hash-bound
combined patch is fetched only by Shunt's explicit local apply path after
independent review; raw worker logs and provider material are never exposed to
the parent client.

## 5. Architecture

```text
Claude Code / Codex
        |
        | skill or plugin
        v
Shunt MCP bridge and Manual Swarm client
        |
        | Website3-authenticated request
        v
Website3 -> Fabric authorization bridge
        |
        | scoped ManualSwarmGrant
        v
Auto Swarm Manual Session API
        |
        +--> bounded session coordinator
        +--> resource leases
        +--> SwarmFS source snapshot and forks
        +--> Kubernetes worker Jobs
        +--> session-scoped Shunt worker gateway
        |
        v
DO DOKS build-fra1 or Hetzner backup substrate
        |
        v
WorkerFinalReports + artifacts + ChangeRefs
        |
        v
conflict-aware integration proposal
        |
        v
parent CLI review and explicit apply
```

### 5.1 Separation of responsibilities

**Shunt** owns the coding-client experience, job submission, provider-pool choice, local status cache, MCP tools, and safe application to the parent checkout.

**Website3/Fabric** authenticates the user, authorizes Space and subscription access, and mints bounded delegation grants. It never executes worker code.

**Auto Swarm** owns session state, remote scheduling, resource leasing, SwarmFS lifecycle, Kubernetes Jobs, worker reports, and integration proposals. Manual sessions use explicit commands and a bounded coordinator, not the WorkItem reconciler.

**Steward** projects and explains session state. It may submit the same governed manual-session commands but is not a second scheduler or source of truth.

**Workers** execute one assigned task in one fork and emit claims and changes. They cannot accept work, freeze authority state directly, integrate, land, or broaden their assignment.

## 6. State and authority contracts

### 6.1 `ManualSwarmSession`

Add a durable session object with at least:

```text
id
website_user_id
space_id
swarm_id
objective
source_ref
base_commit
target                 local | build-fra1 | hetzner-backup-substrate
mode                   plan | patch | apply-requested
phase                  preview | admitted | provisioning | running |
                       integrating | ready | cancelled | failed | expired
requested_agents
maximum_agents
provider_mix
network_policy
deadline
grant_id
source_snapshot_id
integration_proposal_ref
created_at / updated_at / completed_at
```

The session object is coordination state. `ready` means a proposal is available, not that changes were accepted or landed.

### 6.2 `ManualSwarmWorker`

Each bounded worker record contains:

```text
session_id
worker_run_id
role
assignment
dependency_ids
model_lane             claude | codex
resource_lease_id
swarmfs_id
workspace_fork_id
phase
heartbeat
final_report_ref
change_ref
artifact_refs
known_gaps
```

Worker phase changes do not change WorkItem state because Manual Swarm does not create reconciled WorkItems.

### 6.3 `ManualSwarmGrant`

Website3/Fabric mints a signed, short-lived grant containing:

```text
grant_id
website_user_id
session_id
space_id
allowed_subscription_ids
allowed_providers
maximum_workers
target_allowlist
issued_at
expires_at
nonce
```

The grant is single-session, audience-bound, and non-refreshable. Revocation or expiry prevents new worker/model leases and causes running workers to drain at a safe boundary.

### 6.4 Integration proposal

The coordinator emits a proposal rather than mutating the parent checkout:

```text
base_commit
ordered_change_refs
combined_patch_ref
changed_paths
conflict_groups
checks
review_results
unresolved_findings
apply_preconditions
```

Shunt rechecks the parent `HEAD`, dirty-path overlap, patch applicability, and review policy immediately before apply.

## 7. Remote substrate behavior

### 7.1 Target selection

`target=auto` selects the registered default production substrate only when:

- its binding is healthy;
- the current release has the complete fresh production capability proof set;
- the requested worker capacity is available;
- the target is permitted by the user grant and operator ceiling.

DigitalOcean `build-fra1` is the default target. Hetzner `hetzner-backup-substrate` is selected only when explicitly requested or when the launch preview names it as a permitted fallback and the user approves that preview. No hosted target silently falls back to local.

### 7.2 Workspace topology

One session provisions:

1. one immutable source snapshot at the requested commit;
2. one isolated workspace fork per implementation or test worker;
3. read-only access to relevant completed worker artifacts for reviewers;
4. a separate integration workspace for combining compatible `ChangeRef` records.

Workers never share a writable fork. Integration is based on immutable changes and manifests, not copying files between live workspaces.

### 7.3 Bounded coordinator

The coordinator owns a finite state machine for one session:

1. admit the signed grant;
2. validate target and proof freshness;
3. snapshot the source;
4. create worker forks;
5. acquire worker and model leases;
6. launch and watch Kubernetes Jobs;
7. freeze completed forks;
8. collect reports and `ChangeRef` records;
9. construct and test an integration proposal;
10. release resources and terminate.

It does not poll for unrelated WorkItems or create successor work automatically. If the coordinator dies, the session becomes `orphaned` or `interrupted`; explicit resume or cleanup is required. A cleanup TTL may release resources, but must not invent or continue coding work.

## 8. Credentials and secret delivery

### 8.1 Rules

- Local Shunt never invokes the Doppler CLI.
- Website3/Fabric authorizes every user-scoped remote session.
- Shared infrastructure secrets remain server-side or are projected through External Secrets Operator into the exact Kubernetes namespace that needs them.
- User subscription secrets remain in the Website3/Fabric brokered user namespace.
- Workers receive only lease-scoped runtime material.
- Refresh tokens, ID tokens, Website3 cookies, broad service tokens, and raw `.env.local` files are forbidden worker material.

### 8.2 Session-scoped Shunt worker gateway

Run a small Shunt gateway deployment or sidecar inside the Manual Swarm namespace. It receives the `ManualSwarmGrant`, exchanges it for access-only account leases, and exposes the existing native endpoints to workers:

```text
http://shunt-worker-gateway:8082/v1/messages
http://shunt-worker-gateway:8083/backend-api/codex/responses
```

The gateway:

- keeps provider access tokens in memory only;
- never receives or persists refresh tokens;
- injects Codex account identity and routing headers;
- enforces session, account, provider, concurrency, and expiry claims;
- records usage against the correct user and Space grants;
- fails closed on Website3/Fabric authorization errors;
- terminates with the session namespace.

This avoids extending the Go model drivers with Shunt-specific subscription headers and keeps account routing in one implementation.

### 8.3 Doppler boundaries

The system may use Doppler for Tigris, registry, GitHub App, kube, observability, and other infrastructure secrets through the existing server-side/ESO path. A Manual Swarm grant is not a Doppler token, and user subscription inventory must not be copied into the shared substrate Doppler project.

## 9. Shunt interfaces

### 9.1 MCP tools

Expose the same tools to Claude and Codex:

| Tool | Purpose |
| --- | --- |
| `manual_swarm_plan` | Validate and preview an explicit user-authorized Space, Swarm, subscription list, target, roles, lanes, limits, and expected mutations |
| `manual_swarm_start` | Start a user-confirmed session from a preview token |
| `manual_swarm_status` | Return compact session and worker state |
| `manual_swarm_wait` | Wait for changes or terminal state |
| `manual_swarm_inspect` | Read one bounded redacted worker record or integration proposal |
| `manual_swarm_steer` | Deliver bounded guidance to one active worker at a safe boundary |
| `manual_swarm_cancel` | Cancel one worker or the whole session |
| `manual_swarm_review` | Request independent review or integration checks |
| `manual_swarm_apply` | Apply an approved proposal after local precondition checks |
| `manual_swarm_cleanup` | Explicitly release an expired/interrupted session |

Start and apply are separate write operations. The plan token binds the start request to the exact Website3 user, Space, Swarm, subscription list, target, base commit, limits, and provider mix shown to the user. The effective duration ceiling is one hour because it is bounded by the signed grant.

### 9.2 Shunt configuration

Add a manual-swarm section without creating a third provider pool:

```toml
[manual_swarm]
enabled = true
control_url = "https://fabric.example/website/shunt/manual-swarms"
default_target = "auto"
default_agents = 4
max_agents = 8
default_duration_secs = 2700
apply_policy = "explicit"

[manual_swarm.local_worker]
binary = "/path/to/autoswarm"
enabled = true
```

Claude and Codex remain model pools. `go_native` is an execution runtime whose `model_lane` selects one of those pools.

### 9.3 Local state

Shunt persists only non-secret session metadata and short-lived preview/apply state. Remote authority remains in Auto Swarm. Local cached status must be replaceable by refetching the remote session.

## 10. Auto Swarm interfaces

Add an authenticated Manual Session API under a dedicated route family, for example:

```text
POST   /v1/manual-swarms/preview
POST   /v1/manual-swarms
GET    /v1/manual-swarms/{id}
GET    /v1/manual-swarms/{id}/events
POST   /v1/manual-swarms/{id}/steer
POST   /v1/manual-swarms/{id}/cancel
POST   /v1/manual-swarms/{id}/review
POST   /v1/manual-swarms/{id}/integrate
POST   /v1/manual-swarms/{id}/cleanup
```

All writes require an idempotency key and the signed user/session scope supplied by Fabric. Streaming events use reconnectable cursors. Replaying a start or cancellation request must not duplicate Jobs or leases.

The implementation should reuse:

- `internal/worker/kernel` for the Go worker;
- `internal/worker/kubernetes` for remote worker Jobs;
- `internal/provisioning` for production SwarmFS lifecycle;
- `internal/resource` for capacity/model leases and usage;
- existing `WorkerFinalReport`, artifact, snapshot, and `ChangeRef` contracts;
- existing steering guidance delivery at safe turn boundaries.

Do not route Manual Swarm through `work create`, `run once`, or `up` as an implementation shortcut.

## 11. Parallel planning and integration

### 11.1 Decomposition

The parent skill requests a proposed bounded task graph. The graph includes explicit assignments, dependencies, expected paths, validation responsibilities, and a maximum number of workers. The user approves the graph as part of the launch preview.

The coordinator may schedule ready nodes but may not add new nodes beyond the approved worker and scope ceilings. A worker can report a follow-up candidate; the parent session decides whether to amend the session.

### 11.2 Conflict graph

For every completed worker, record:

- changed paths;
- base manifest/hash;
- symbol or module hints when available;
- required predecessor changes;
- checks executed;
- patch and binary artifact references.

The integration stage builds a conflict graph. Non-overlapping changes may be composed automatically in the integration workspace. Overlapping changes require an integration worker or parent decision. Conflict resolution produces a new `ChangeRef`; it never rewrites the original worker record.

### 11.3 Review policy

- A worker never approves its own change.
- Mixed-provider sessions prefer an opposite-provider reviewer.
- Single-provider sessions use a separate worker identity and clean fork.
- Required checks and `git diff --check` run in the integration workspace.
- Review success means the proposal is eligible for explicit apply, not accepted or landed.

## 12. Steward experience

Add a `manual-swarm` projection or extend the runs/substrate views with clear Manual Swarm labels. Steward should display:

- session objective, target, owner, phase, deadline, and grant expiry;
- worker assignments, dependencies, lanes, progress, and usage;
- SwarmFS snapshot/fork/freeze state;
- exceptions such as stale proofs, missing ESO projections, lease expiry, conflicts, and orphaned Jobs;
- reports, artifacts, reviews, and integration proposals;
- redacted resource/account identities;
- available governed actions: steer, cancel, review, cleanup, and apply request.

Steward sends commands to the same Manual Session API. It must not write session rows directly or invoke Kubernetes/Doppler itself.

## 13. Installation and packaging

Extend `vibe-shunt` setup with:

```text
Enable Manual Swarm? [Y/n]
Install local Auto Swarm coding worker? [Y/n]
Enable hosted targets visible to your Website3 account? [Y/n]
Default maximum parallel workers [4]:
Install Claude /auto-swarm skill? [Y/n]
Install Codex auto-swarm plugin/skill? [Y/n]
```

Package the Go coding worker separately using the existing platform-specific optional-dependency approach used by `@autoswarm/steward`, for example `@autoswarm/coding-worker` plus platform packages. Shunt may use a verified existing `autoswarm` binary, but must record an absolute path and verify a machine-readable capabilities/version response.

The installer must be repeatable, back up changed client configuration, and remove only its managed skill/plugin/MCP entries on uninstall.

## 14. Failure behavior

| Failure | Required behavior |
| --- | --- |
| Target proof stale or missing | Preview/start fails closed; no fallback |
| Website3/Fabric unavailable | No new session or model lease; running workers drain when material expires |
| Doppler/ESO infrastructure projection missing | Affected Job remains blocked; exception names the missing projection without values |
| One worker fails | Preserve its artifacts; independent workers continue; integration marks dependency impact |
| Coordinator interrupted | Durable session becomes interrupted/orphaned; explicit resume or cleanup |
| Parent checkout changes | Apply fails and preserves proposal |
| Dirty path overlaps proposal | Apply fails and names paths |
| Integration conflict | Preserve all ChangeRefs and request resolution |
| Grant expires | Stop new launches, drain active turns, freeze recoverable forks, release leases |
| Cluster unreachable | Session remains observable; no silent target switch |
| User cancels | Stop Jobs, freeze or mark incomplete forks, retain bounded artifacts, release resources |

## 15. Implementation phases

### Phase 0: Contracts and feature gates

- Define versioned `ManualSwarmSession`, worker, grant, event, and integration-proposal schemas.
- Add authority and secret-boundary documentation.
- Add `manual_swarm.enabled = false` default feature gates in all services.
- Add capability/version endpoints for Shunt and Auto Swarm.

Exit gate: schema tests, invalid-transition tests, scope tests, and a no-runtime dry-run preview.

### Phase 1: Local Manual Swarm vertical slice

- Add a direct manual-session service in Auto Swarm without WorkItems/reconcilers.
- Provision one local BuildSwarmFS per worker.
- Run two local Go workers through Shunt's Claude/Codex endpoints.
- Freeze forks, create ChangeRefs, and return an integration proposal.
- Add Shunt MCP plan/start/status/wait/cancel/inspect tools.

Exit gate: two parallel workers produce independent changes while the parent checkout remains unchanged.

### Phase 2: Website3/Fabric grants

- Add preview/start authorization routes.
- Mint and verify `ManualSwarmGrant` tokens.
- Enforce user, Space, account, target, concurrency, and expiry claims.
- Add revoke and audit records.
- Prove cross-user and cross-Space access fails.

Exit gate: a remote session cannot start or lease an account without the exact signed grant.

### Phase 3: Remote SwarmFS and Kubernetes Jobs

- Reuse the existing production source snapshot/fork path.
- Add fork-per-worker Manual Session scheduling.
- Launch Go workers with the Kubernetes runtime.
- Stream durable worker/session events.
- Freeze and clean up through explicit coordinator transitions.
- Support DO default and explicitly selected Hetzner target.

Exit gate: a three-worker no-op/fixture session passes on DO and the same acceptance run passes on Hetzner.

### Phase 4: Session-scoped Shunt gateway

- Build a minimal gateway container from the Shunt release.
- Exchange session grants for access-only user credential leases.
- Route Claude Messages and Codex Responses traffic.
- Enforce expiry, account allowlists, concurrency, and usage attribution.
- Ensure no refresh/ID token or broad Doppler credential reaches the gateway or workers.

Exit gate: mixed Claude/Codex remote workers complete through subscription accounts with zero secret findings.

### Phase 5: Integration and review

- Build changed-path/dependency conflict graphs.
- Compose non-overlapping ChangeRefs in a clean integration fork.
- Add integration-worker conflict resolution.
- Add independent review and required checks.
- Add Shunt apply with base/dirty/lock/patch revalidation.

Exit gate: clean changes combine automatically; overlapping changes remain unapplied until resolved and reviewed.

### Phase 6: Claude, Codex, and Steward UX

- Ship Claude `/auto-swarm` skill.
- Ship Codex plugin/skill and MCP configuration.
- Add compact progress/event rendering.
- Add steering, review, apply, and cleanup workflows.
- Add Steward projection and exception views.

Exit gate: the complete session can be launched, steered, reviewed, and applied from each parent CLI.

### Phase 7: Packaging and setup

- Publish the platform Go worker packages.
- Extend the `vibe-shunt` interactive installer and automation flags.
- Add capability detection, upgrades, backups, and uninstall.
- Document local-only and hosted setup modes.

Exit gate: clean-machine installs work without a source checkout and do not modify unrelated client configuration.

### Phase 8: Production rollout

- Enable for operator accounts with a conservative worker ceiling.
- Run shadow/read-only sessions first.
- Enable patch proposals without apply.
- Enable explicit apply after live safety gates pass.
- Increase worker ceilings only from observed queue, cost, integration, and failure data.

Exit gate: authenticated production smoke, rollback rehearsal, and secret-rotation smoke pass on both registered substrates.

## 16. Validation matrix

### Unit and contract tests

- state-transition and idempotency coverage;
- grant signature, audience, expiry, nonce, user, Space, and target checks;
- account and provider allowlist enforcement;
- MCP input schemas and plan-token binding;
- no refresh token, ID token, cookie, `.env.local`, or Doppler-token serialization;
- deterministic target and role selection;
- conflict-graph composition and overlap handling;
- apply precondition and dirty-path collision tests.

### Integration tests

- local two-worker Claude/Codex mixed session;
- worker cancellation and partial result retention;
- expired grant and broker outage drain behavior;
- coordinator restart/orphan detection;
- reconnectable status/event cursors;
- one source snapshot with N distinct forks;
- failed worker dependency propagation;
- review rejection preserves changes without parent mutation;
- installer repeat, upgrade, and uninstall.

### Live acceptance

Run on both DigitalOcean and Hetzner:

1. start a four-worker mixed-provider session from a clean fixture;
2. verify namespace, ESO, gateway, Jobs, leases, and forks;
3. create two compatible changes and one intentional conflict;
4. verify compatible changes integrate and the conflict remains explicit;
5. run checks and opposite-provider review;
6. apply only after user confirmation;
7. verify the parent matches the reviewed proposal;
8. verify session cleanup removes runtime material but retains bounded audit/artifact records;
9. scan logs, events, artifacts, pod specs, and environment dumps for secrets;
10. rotate broker/infrastructure material and repeat a bounded session.

## 17. Acceptance criteria

Manual Swarm is complete only when:

- Claude and Codex can both launch and control it through supported extension surfaces;
- the parent session remains responsive while workers run;
- at least four workers run concurrently on a hosted substrate;
- every worker receives a distinct SwarmFS fork from one immutable source snapshot;
- mixed Claude/Codex subscription routing works without raw user credentials on the worker substrate;
- Steward shows the complete session and exception state;
- integration produces durable, reviewable ChangeRefs and a combined proposal;
- no parent checkout mutation occurs before explicit apply;
- failed, cancelled, expired, and interrupted sessions preserve honest state and release capacity;
- no WorkItem reconciler, automatic integration request, acceptance, or landing occurs;
- secret scans find no refresh tokens, Website3 sessions, Doppler service tokens, or `.env.local` contents;
- DigitalOcean and Hetzner live smokes both pass against the same release contracts.

## 18. Release order

1. Auto Swarm schemas and local session service.
2. Shunt MCP client and local vertical slice.
3. Website3/Fabric grant routes.
4. Remote coordinator and Kubernetes/SwarmFS execution.
5. Session-scoped Shunt gateway.
6. Integration/review/apply.
7. Claude skill, Codex plugin/skill, and Steward projection.
8. Worker packaging and `vibe-shunt` installer.
9. DO and Hetzner live gates.
10. Opt-in production release.

Do not publish a setup option before its server capability check and fail-closed behavior are available. Older Shunt installations must ignore absent Manual Swarm configuration; newer clients must clearly report an older server rather than degrading into local or unmanaged execution.

## 19. Documentation references

- Codex extension surfaces: <https://learn.chatgpt.com/docs/developer-commands>
- Claude Code skills and slash invocation: <https://code.claude.com/docs/en/slash-commands>
- Shunt native pools and MCP bridge: `docs/native-pools-and-bridge.md`
- Auto Swarm workspace architecture: `auto-swarm/docs/architecture/10-workspace-backends-and-change-artifacts.md`
- Auto Swarm environment/substrate architecture: `auto-swarm/docs/architecture/14-environment-provisioning.md`
- Auto Swarm production substrate plan: `auto-swarm/docs/substrate-plan.md`
- Auto Swarm worker implementation plan: `auto-swarm/docs/plans/worker-build.md`

---
name: auto-swarm
description: Plan, launch, monitor, steer, review, cancel, and explicitly apply bounded parallel Auto Swarm coding sessions through the Shunt MCP bridge. Use when a user invokes $auto-swarm, asks for a manual swarm, requests several coding workers in parallel, wants hosted SwarmFS workers, or prefixes a request with auto-swarm:.
---

# Auto Swarm

Use ultrathink for decomposition and integration decisions. Keep the current Codex or Claude session as operator; send every swarm operation through the Shunt MCP tools.

## Route the request

- For a new objective, run the plan, confirmation, start, monitor, integrate, and review workflow below.
- For `status`, `wait`, `inspect`, `steer`, `cancel`, `review`, `apply`, or `cleanup`, call the corresponding Manual Swarm tool for the named or most recent session.
- If Manual Swarm tools are unavailable, report that Shunt or its server is too old or disabled. Do not substitute SSH, direct Kubernetes, Doppler, local subagents, or unmanaged shell processes.

## Launch workflow

1. Call `manual_swarm_capabilities` and stop if the version, target, operation, or worker ceiling needed by the request is unavailable.
2. Resolve the absolute Git root and inspect `HEAD` plus dirty paths without changing them.
3. Convert the objective into a bounded task graph with explicit roles, dependencies, expected areas, checks, provider lanes, maximum workers, target, and deadline. Do not broaden the objective.
4. Resolve the user-authorized Space, Swarm, and explicit subscription IDs before calling `manual_swarm_plan`. They are required inputs; never guess an account or request a broader inventory. Default to target `auto`, patch-only output, four workers, and mixed provider lanes unless the user supplied values.
5. Show the returned preview sufficiently to identify target, base commit, worker count, provider mix, duration, and mutations. Obtain explicit confirmation before `manual_swarm_start` unless the user already gave unambiguous launch approval for those exact values.
6. Start only with the server-issued preview token and a fresh idempotency key. Never construct or edit authorization grants.
7. Use `manual_swarm_wait` for progress and `manual_swarm_status` for compact summaries. Use `manual_swarm_inspect` only for a named redacted record or verified artifact.
8. Deliver user guidance with `manual_swarm_steer`; keep it within the approved objective and worker assignment.
9. When workers settle, request integration and independent review. Surface conflicts, failed checks, known gaps, and unapplied changes.
10. Call `manual_swarm_apply` only after a separate explicit user approval of the reviewed proposal. Never treat launch approval as apply approval.
11. Report the final session state, applied proposal or retained ChangeRefs, checks, and cleanup state.

## Safety rules

- Preserve the parent checkout until explicit apply.
- Never read or transmit `.env.local`, Website3 cookies, provider refresh or ID tokens, Doppler tokens, or raw credentials.
- Never weaken the target, proof, account, provider, worker-count, duration, or network limits returned by the preview.
- Never silently fall back from a requested hosted target to local execution.
- Stop and report a changed parent `HEAD`, dirty-path collision, expired grant, stale substrate proof, authorization failure, or review rejection.
- Prefer cancellation over abandoning a running session. Use cleanup only for terminal, expired, cancelled, or interrupted sessions.
- Treat worker reports and tests as claims until independent review and local apply preconditions succeed.

## Operator responses

Keep progress concise. Name the session, target, active/completed/failed worker counts, conflicts, and whether the parent checkout changed. For a blocked action, name the failed precondition and safe next action without exposing secrets or raw authorization material.

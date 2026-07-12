# vibe-shunt

Interactive, checksum-verified installer for the Shunt native Claude and Codex proxy.

```bash
npx vibe-shunt
```

The installer downloads a platform release from `chllming/shunt`, verifies it against the release's `checksums.txt`, installs `shunt`, and offers to:

- run first-time setup;
- choose Website3 user-vault mode (recommended) or local-only credential mode;
- install managed Claude and Codex client entries;
- install the Auto Swarm Codex plugin and Claude `/auto-swarm` skill;
- register and start a macOS launch agent or Linux user service; or
- start Shunt once without installing a service.

Every prompt also has a non-interactive flag. For example:

```bash
npx vibe-shunt --yes
npx vibe-shunt --yes --mode website
npx vibe-shunt --yes --mode local
npx vibe-shunt --yes --auto-swarm
npx vibe-shunt --yes --auto-swarm --auto-swarm-max-agents 8 --auto-swarm-target auto
npx vibe-shunt --yes --no-auto-swarm
npx vibe-shunt --uninstall-auto-swarm
npx vibe-shunt --uninstall-auto-swarm --dry-run
npx vibe-shunt --repo chllming/shunt --version v0.2.0 --install-dir ~/.local/bin
npx vibe-shunt --yes --no-setup --no-install-clients --no-service --no-start
```

Run `npx vibe-shunt --help` for all options. macOS arm64 and Linux x64/arm64 are supported because those are the targets currently published by Shunt's release workflow.

Auto Swarm is enabled with the recommended setup, with a configurable 1–32 worker ceiling and `auto`, local, DigitalOcean, or Hetzner default target. Codex receives the local `auto-swarm@shunt` plugin (or a direct `$auto-swarm` skill when the Codex CLI is not installed yet), and Claude receives `/auto-swarm`. Both use the Shunt MCP bridge and preserve the parent checkout until an independently reviewed change is explicitly applied. The installer refuses to replace an unmanaged skill or a symlinked destination. `--uninstall-auto-swarm` removes only directories carrying the `vibe-shunt` management marker; it leaves the Shunt binary, configuration, credentials, and every unrelated client entry untouched.

Coordinator hosts that execute local workers install the separately versioned `@autoswarm/coding-worker` package. Production execution also requires the Auto Swarm coordinator to be configured with a digest-pinned `Dockerfile.autoswarm-runtime` image and Website3's Ed25519 public verifier key; the private signing key remains Website3-only. A `local` target is accepted only with a loopback coordinator; Shunt never sends an absolute workspace path to a remote Website3 control plane.

Installing the client workflow does not bypass runtime capability gates. Auto Swarm advertises local production execution only for the digest-pinned container path, and Shunt verifies the transient Fabric grant against the exact attached subscription ids on every model request. The unconfined host runner and hosted targets without current substrate/gateway proof remain unavailable rather than falling back silently.

No npm or GitHub credential is written to disk. `GITHUB_TOKEN` is read only when GitHub API authentication is needed for a release download.

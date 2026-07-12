# vibe-shunt

Interactive, checksum-verified installer for the Shunt native Claude and Codex proxy.

```bash
npx vibe-shunt@latest
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
npx vibe-shunt@latest --yes
npx vibe-shunt@latest --yes --mode website
npx vibe-shunt@latest --yes --mode local
npx vibe-shunt@latest --yes --auto-swarm
npx vibe-shunt@latest --yes --auto-swarm --auto-swarm-max-agents 8 --auto-swarm-target auto
npx vibe-shunt@latest --yes --no-auto-swarm
npx vibe-shunt@latest --uninstall-auto-swarm
npx vibe-shunt@latest --uninstall-auto-swarm --dry-run
npx vibe-shunt@latest --repo chllming/shunt --version latest --install-dir ~/.local/bin
npx vibe-shunt@latest --yes --no-setup --no-install-clients --no-service --no-start
```

Run `npx vibe-shunt@latest --help` for all options. macOS arm64 and Linux x64/arm64 are supported because those are the targets currently published by Shunt's release workflow. The npm package and GitHub binary release use one version: the release workflow builds and checksums every binary, creates the GitHub release, and only then publishes `vibe-shunt` with npm provenance. Do not publish the installer independently from its matching binary release.

`package.json` is the next source release version; it is not evidence that the
version has reached npm. Verify the public release with
`npm view vibe-shunt version` and the matching binary tag with
`gh release view --repo chllming/shunt`.

The installer can enable the Manual Swarm client surface, with a configurable 1–32 worker ceiling and `auto`, local, DigitalOcean, or Hetzner requested target. Codex receives the local `auto-swarm@shunt` plugin (or a direct `$auto-swarm` skill when the Codex CLI is not installed yet), and Claude receives `/auto-swarm`. Both use the Shunt MCP bridge and preserve the parent checkout until an independently reviewed change is explicitly applied. The installer refuses to replace an unmanaged skill or a symlinked destination. `--uninstall-auto-swarm` removes only directories carrying the `vibe-shunt` management marker; it leaves the Shunt binary, configuration, credentials, and every unrelated client entry untouched.

Enabling that client surface does not install the Auto Swarm coordinator or the separately versioned `@autoswarm/coding-worker` distribution. Production local execution uses the coordinator's digest-pinned `Dockerfile.autoswarm-runtime` image and Website3's Ed25519 public verifier key; the private signing key remains Website3-only. A `local` target is accepted only with a loopback coordinator; Shunt never sends an absolute workspace path to a remote Website3 control plane.

Installing the client workflow does not bypass runtime capability gates. Auto Swarm advertises local production execution only for the digest-pinned container path, and Shunt verifies the transient Fabric grant against the exact attached subscription ids on every model request. The unconfined host runner and hosted targets without current substrate/gateway proof remain unavailable rather than falling back silently.

No npm or GitHub credential is written to disk. `GITHUB_TOKEN` is read only when GitHub API authentication is needed for a release download.

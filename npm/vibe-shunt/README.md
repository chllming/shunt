# vibe-shunt

Interactive, checksum-verified installer for the Shunt native Claude and Codex proxy.

```bash
npx vibe-shunt
```

The installer downloads a platform release from `chllming/shunt`, verifies it against the release's `checksums.txt`, installs `shunt`, and offers to:

- run first-time setup;
- install managed Claude and Codex client entries;
- register and start a macOS launch agent or Linux user service; or
- start Shunt once without installing a service.

Every prompt also has a non-interactive flag. For example:

```bash
npx vibe-shunt --yes
npx vibe-shunt --repo chllming/shunt --version v0.1.148 --install-dir ~/.local/bin
npx vibe-shunt --yes --no-setup --no-install-clients --no-service --no-start
```

Run `npx vibe-shunt --help` for all options. macOS arm64 and Linux x64/arm64 are supported because those are the targets currently published by Shunt's release workflow.

No npm or GitHub credential is written to disk. `GITHUB_TOKEN` is read only when GitHub API authentication is needed for a release download.

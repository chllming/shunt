import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  AUTO_SWARM_PLUGIN,
  DEFAULT_REPO,
  applyRecommendedDefaults,
  autoSwarmInstallPaths,
  installAutoSwarmClients,
  parseArgs,
  parseChecksums,
  replaceManagedDirectory,
  setupArgsFor,
  targetFor,
  uninstallAutoSwarmClients,
  validateArchiveListing,
  validateOptions,
  validateUninstallOptions,
  verifyChecksum,
} from "../bin/vibe-shunt.mjs";

test("recommended defaults install and configure the fork", () => {
  const parsed = parseArgs([], {});
  const options = applyRecommendedDefaults(parsed);
  assert.equal(options.repo, DEFAULT_REPO);
  assert.equal(options.version, "latest");
  assert.equal(options.setup, true);
  assert.equal(options.mode, "website");
  assert.equal(options.installClients, true);
  assert.equal(options.autoSwarm, true);
  assert.equal(options.autoSwarmMaxAgents, 8);
  assert.equal(options.autoSwarmTarget, "auto");
  assert.equal(options.service, true);
  assert.equal(options.start, false);
});

test("parses explicit unattended choices", () => {
  const options = applyRecommendedDefaults(parseArgs([
    "--yes",
    "--repo=someone/shunt",
    "--version",
    "v1.2.3",
    "--install-dir",
    "/tmp/shunt-bin",
    "--no-setup",
    "--mode",
    "local",
    "--no-install-clients",
    "--no-auto-swarm",
    "--no-service",
    "--start",
    "--force",
    "--dry-run",
  ], {}));
  validateOptions(options);
  assert.equal(options.repo, "someone/shunt");
  assert.equal(options.version, "v1.2.3");
  assert.equal(options.installDir, "/tmp/shunt-bin");
  assert.equal(options.setup, false);
  assert.equal(options.mode, "local");
  assert.equal(options.installClients, false);
  assert.equal(options.autoSwarm, false);
  assert.equal(options.service, false);
  assert.equal(options.start, true);
  assert.equal(options.force, true);
  assert.equal(options.dryRun, true);
});

test("parses a standalone managed Auto Swarm uninstall", () => {
  const options = parseArgs(["--uninstall-auto-swarm", "--dry-run"], {});
  assert.equal(options.uninstallAutoSwarm, true);
  assert.equal(options.dryRun, true);
  assert.doesNotThrow(() => validateUninstallOptions(options));
  assert.throws(
    () => validateUninstallOptions(parseArgs([
      "--uninstall-auto-swarm",
      "--install-dir",
      "/tmp/bin",
    ], {})),
    /mutually exclusive/,
  );
});

test("starts once by default when a login service is explicitly disabled", () => {
  const options = applyRecommendedDefaults(parseArgs(["--yes", "--no-service"], {}));
  assert.equal(options.service, false);
  assert.equal(options.start, true);
});

test("passes bounded Manual Swarm setup choices to Shunt", () => {
  const options = applyRecommendedDefaults(parseArgs([
    "--yes",
    "--auto-swarm",
    "--auto-swarm-max-agents",
    "12",
    "--auto-swarm-target",
    "build-fra1",
  ], {}));
  validateOptions(options);
  assert.deepEqual(setupArgsFor(options), [
    "setup",
    "--mode",
    "website",
    "--website-url",
    "https://beyondwork.ai",
    "--install-clients",
    "--manual-swarm",
    "--manual-swarm-max-agents",
    "12",
    "--manual-swarm-default-target",
    "build-fra1",
  ]);
});

test("rejects contradictory setup and service flags", () => {
  const setupConflict = applyRecommendedDefaults(parseArgs([
    "--no-setup",
    "--install-clients",
  ], {}));
  assert.throws(() => validateOptions(setupConflict), /requires --setup/);

  const serviceConflict = applyRecommendedDefaults(parseArgs([
    "--service",
    "--start",
  ], {}));
  assert.throws(() => validateOptions(serviceConflict), /mutually exclusive/);

  const autoSwarmConflict = applyRecommendedDefaults(parseArgs([
    "--no-setup",
    "--no-install-clients",
    "--auto-swarm",
  ], {}));
  assert.throws(() => validateOptions(autoSwarmConflict), /requires --setup and --install-clients/);

  const excessiveWorkers = applyRecommendedDefaults(parseArgs([
    "--auto-swarm-max-agents",
    "33",
  ], {}));
  assert.throws(() => validateOptions(excessiveWorkers), /integer between 1 and 32/);

  const disabledWorkerOptions = applyRecommendedDefaults(parseArgs([
    "--no-auto-swarm",
    "--auto-swarm-target",
    "local",
  ], {}));
  assert.throws(() => validateOptions(disabledWorkerOptions), /require --auto-swarm/);
});

test("maps only release targets produced by the workflow", () => {
  assert.equal(targetFor("darwin", "arm64"), "aarch64-apple-darwin");
  assert.equal(targetFor("linux", "x64"), "x86_64-unknown-linux-gnu");
  assert.equal(targetFor("linux", "arm64"), "aarch64-unknown-linux-gnu");
  assert.throws(() => targetFor("darwin", "x64"), /Intel release assets/);
  assert.throws(() => targetFor("win32", "x64"), /unsupported platform/);
});

test("parses GNU and binary checksum formats", () => {
  const checksums = parseChecksums([
    `${"a".repeat(64)}  shunt-v1.2.3-linux.tar.gz`,
    `${"B".repeat(64)} *shunt-v1.2.3-macos.tar.gz`,
    "not a checksum",
  ].join("\n"));
  assert.equal(checksums.get("shunt-v1.2.3-linux.tar.gz"), "a".repeat(64));
  assert.equal(checksums.get("shunt-v1.2.3-macos.tar.gz"), "b".repeat(64));
  assert.equal(checksums.size, 2);
});

test("verifies an archive and rejects a mismatched checksum", async () => {
  const root = await mkdtemp(join(tmpdir(), "vibe-shunt-test-"));
  try {
    const archiveName = "shunt-v1.2.3-test.tar.gz";
    const archive = join(root, archiveName);
    const checksums = join(root, "checksums.txt");
    const contents = Buffer.from("release archive");
    const digest = createHash("sha256").update(contents).digest("hex");
    await writeFile(archive, contents);
    await writeFile(checksums, `${digest}  ${archiveName}\n`);
    assert.equal(await verifyChecksum(archive, checksums, archiveName), digest);

    await writeFile(checksums, `${"0".repeat(64)}  ${archiveName}\n`);
    await assert.rejects(verifyChecksum(archive, checksums, archiveName), /checksum mismatch/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("rejects malformed repositories, versions, and unknown arguments", () => {
  assert.throws(() => parseArgs(["--wat"], {}), /unknown option/);
  const badRepo = applyRecommendedDefaults(parseArgs(["--repo", "not-a-repo"], {}));
  assert.throws(() => validateOptions(badRepo), /invalid GitHub repository/);
  const badVersion = applyRecommendedDefaults(parseArgs(["--version", "tomorrow"], {}));
  assert.throws(() => validateOptions(badVersion), /invalid release version/);
  const badMode = applyRecommendedDefaults(parseArgs(["--mode", "doppler"], {}));
  assert.throws(() => validateOptions(badMode), /invalid setup mode/);
  const insecureWebsite = applyRecommendedDefaults(parseArgs([
    "--website-url",
    "http://example.test",
  ], {}));
  assert.throws(() => validateOptions(insecureWebsite), /use HTTPS/);
  const loopbackWebsite = applyRecommendedDefaults(parseArgs([
    "--website-url",
    "http://127.0.0.1:3000",
  ], {}));
  assert.doesNotThrow(() => validateOptions(loopbackWebsite));
  const rootInstall = applyRecommendedDefaults(parseArgs(["--install-dir", "/"], {}));
  assert.throws(() => validateOptions(rootInstall), /filesystem root/);
});

test("rejects traversal, duplicate, and link entries in release archives", () => {
  const root = "shunt-v1.2.3-x86_64-unknown-linux-gnu";
  assert.doesNotThrow(() => validateArchiveListing(
    `${root}/\n${root}/shunt\n`,
    `drwxr-xr-x user/group 0 Jan 1 00:00 ${root}/\n-rwxr-xr-x user/group 10 Jan 1 00:00 ${root}/shunt\n`,
    root,
  ));
  assert.throws(() => validateArchiveListing(
    `${root}/shunt\n../escaped\n`,
    `-rwxr-xr-x user/group 10 Jan 1 00:00 ${root}/shunt\n-rw-r--r-- user/group 1 Jan 1 00:00 ../escaped\n`,
    root,
  ), /unexpected entry/);
  assert.throws(() => validateArchiveListing(
    `${root}/shunt\n${root}/shunt\n`,
    `-rwxr-xr-x user/group 10 Jan 1 00:00 ${root}/shunt\n-rwxr-xr-x user/group 10 Jan 1 00:00 ${root}/shunt\n`,
    root,
  ), /duplicate entry set/);
  assert.throws(() => validateArchiveListing(
    `${root}/shunt\n`,
    `lrwxrwxrwx user/group 0 Jan 1 00:00 ${root}/shunt -> /tmp/escaped\n`,
    root,
  ), /unsafe type/);
});

test("derives bounded Auto Swarm install paths and rejects relative config roots", () => {
  const paths = autoSwarmInstallPaths({
    home: "/home/tester",
    env: {
      XDG_DATA_HOME: "/state/data",
      CODEX_HOME: "/state/codex",
      CLAUDE_CONFIG_DIR: "/state/claude",
    },
  });
  assert.equal(paths.marketplaceRoot, "/state/data/shunt/codex-marketplace");
  assert.equal(paths.codexSkillRoot, "/state/codex/skills/auto-swarm");
  assert.equal(paths.claudeSkillRoot, "/state/claude/skills/auto-swarm");
  assert.throws(() => autoSwarmInstallPaths({
    home: "/home/tester",
    env: { XDG_DATA_HOME: "relative/data" },
  }), /absolute path/);
});

test("installs the Codex plugin and Claude skill atomically and idempotently", async () => {
  const root = await mkdtemp(join(tmpdir(), "vibe-shunt-auto-swarm-"));
  try {
    const shuntPath = join(root, "bin", "shunt");
    await mkdir(join(root, "bin"), { recursive: true });
    await writeFile(shuntPath, "test executable\n");
    const env = {
      ...process.env,
      XDG_DATA_HOME: join(root, "data"),
      CODEX_HOME: join(root, "codex"),
      CLAUDE_CONFIG_DIR: join(root, "claude"),
    };
    const calls = [];
    const output = { write() {} };
    const runner = (command, args, options) => calls.push({ command, args, options });

    await installAutoSwarmClients({
      shuntPath,
      home: root,
      env,
      runner,
      codexAvailable: true,
      output,
    });
    await installAutoSwarmClients({
      shuntPath,
      home: root,
      env,
      runner,
      codexAvailable: true,
      output,
    });

    const paths = autoSwarmInstallPaths({ home: root, env });
    const mcp = JSON.parse(await readFile(join(
      paths.marketplaceRoot,
      "plugins",
      "auto-swarm",
      ".mcp.json",
    ), "utf8"));
    assert.equal(mcp.mcpServers.shunt.command, shuntPath);
    assert.match(await readFile(join(paths.claudeSkillRoot, "SKILL.md"), "utf8"), /# Auto Swarm/);
    assert.equal(calls.length, 4);
    assert.deepEqual(calls[0].args.slice(0, 3), ["plugin", "marketplace", "add"]);
    assert.deepEqual(calls[1].args, ["plugin", "add", AUTO_SWARM_PLUGIN, "--json"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("falls back to a managed Codex skill when the Codex CLI is absent", async () => {
  const root = await mkdtemp(join(tmpdir(), "vibe-shunt-auto-swarm-fallback-"));
  try {
    const shuntPath = join(root, "shunt");
    await writeFile(shuntPath, "test executable\n");
    const env = {
      XDG_DATA_HOME: join(root, "data"),
      CODEX_HOME: join(root, "codex"),
      CLAUDE_CONFIG_DIR: join(root, "claude"),
    };
    await installAutoSwarmClients({
      shuntPath,
      home: root,
      env,
      codexAvailable: false,
      output: { write() {} },
    });
    const paths = autoSwarmInstallPaths({ home: root, env });
    assert.match(await readFile(join(paths.codexSkillRoot, "SKILL.md"), "utf8"), /# Auto Swarm/);
    assert.match(await readFile(join(paths.claudeSkillRoot, "SKILL.md"), "utf8"), /# Auto Swarm/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("uninstalls only marked Auto Swarm entries and is idempotent", async () => {
  const root = await mkdtemp(join(tmpdir(), "vibe-shunt-auto-swarm-uninstall-"));
  try {
    const shuntPath = join(root, "shunt");
    await writeFile(shuntPath, "test executable\n");
    const env = {
      XDG_DATA_HOME: join(root, "data"),
      CODEX_HOME: join(root, "codex"),
      CLAUDE_CONFIG_DIR: join(root, "claude"),
    };
    await installAutoSwarmClients({
      shuntPath,
      home: root,
      env,
      codexAvailable: false,
      output: { write() {} },
    });
    const paths = autoSwarmInstallPaths({ home: root, env });
    const first = await uninstallAutoSwarmClients({
      home: root,
      env,
      codexAvailable: false,
      output: { write() {} },
    });
    assert.deepEqual(new Set(first.removed), new Set([
      paths.marketplaceRoot,
      paths.codexSkillRoot,
      paths.claudeSkillRoot,
    ]));
    await assert.rejects(readFile(join(paths.claudeSkillRoot, "SKILL.md")), /ENOENT/);
    const second = await uninstallAutoSwarmClients({
      home: root,
      env,
      codexAvailable: false,
      output: { write() {} },
    });
    assert.deepEqual(second.removed, []);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("uninstall validates all destinations before refusing unmanaged or symlinked paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "vibe-shunt-auto-swarm-uninstall-adversarial-"));
  try {
    const shuntPath = join(root, "shunt");
    await writeFile(shuntPath, "test executable\n");
    const env = {
      XDG_DATA_HOME: join(root, "data"),
      CODEX_HOME: join(root, "codex"),
      CLAUDE_CONFIG_DIR: join(root, "claude"),
    };
    await installAutoSwarmClients({
      shuntPath,
      home: root,
      env,
      codexAvailable: false,
      output: { write() {} },
    });
    const paths = autoSwarmInstallPaths({ home: root, env });
    await rm(paths.claudeSkillRoot, { recursive: true });
    await mkdir(paths.claudeSkillRoot, { recursive: true });
    await writeFile(join(paths.claudeSkillRoot, "user-owned.txt"), "keep\n");
    await assert.rejects(
      uninstallAutoSwarmClients({
        home: root,
        env,
        codexAvailable: false,
        output: { write() {} },
      }),
      /unmanaged Auto Swarm destination/,
    );
    assert.match(await readFile(join(paths.marketplaceRoot, ".vibe-shunt-managed"), "utf8"), /vibe-shunt/);
    assert.equal(await readFile(join(paths.claudeSkillRoot, "user-owned.txt"), "utf8"), "keep\n");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("refuses unmanaged and symlinked managed-directory destinations", async () => {
  const root = await mkdtemp(join(tmpdir(), "vibe-shunt-managed-dir-"));
  try {
    const source = join(root, "source");
    const unmanaged = join(root, "unmanaged");
    const linked = join(root, "linked");
    await mkdir(source);
    await writeFile(join(source, "SKILL.md"), "safe\n");
    await mkdir(unmanaged);
    await writeFile(join(unmanaged, "mine.txt"), "keep\n");
    await assert.rejects(
      replaceManagedDirectory(source, unmanaged),
      /unmanaged Auto Swarm destination/,
    );
    await symlink(source, linked, "dir");
    await assert.rejects(
      replaceManagedDirectory(source, linked),
      /symlinked Auto Swarm destination/,
    );
    assert.equal(await readFile(join(unmanaged, "mine.txt"), "utf8"), "keep\n");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("packaged plugin assets stay byte-identical to the reviewed source", async () => {
  const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
  const canonical = join(repoRoot, "plugins", "auto-swarm");
  const packaged = join(
    repoRoot,
    "npm",
    "vibe-shunt",
    "assets",
    "codex-marketplace",
    "plugins",
    "auto-swarm",
  );
  for (const relative of [
    ".codex-plugin/plugin.json",
    ".mcp.json",
    "skills/auto-swarm/SKILL.md",
    "skills/auto-swarm/agents/openai.yaml",
  ]) {
    assert.equal(
      await readFile(join(packaged, relative), "utf8"),
      await readFile(join(canonical, relative), "utf8"),
      relative,
    );
  }
});

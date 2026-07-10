import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  DEFAULT_REPO,
  applyRecommendedDefaults,
  parseArgs,
  parseChecksums,
  targetFor,
  validateOptions,
  verifyChecksum,
} from "../bin/vibe-shunt.mjs";

test("recommended defaults install and configure the fork", () => {
  const parsed = parseArgs([], {});
  const options = applyRecommendedDefaults(parsed);
  assert.equal(options.repo, DEFAULT_REPO);
  assert.equal(options.version, "latest");
  assert.equal(options.setup, true);
  assert.equal(options.installClients, true);
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
    "--no-install-clients",
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
  assert.equal(options.installClients, false);
  assert.equal(options.service, false);
  assert.equal(options.start, true);
  assert.equal(options.force, true);
  assert.equal(options.dryRun, true);
});

test("starts once by default when a login service is explicitly disabled", () => {
  const options = applyRecommendedDefaults(parseArgs(["--yes", "--no-service"], {}));
  assert.equal(options.service, false);
  assert.equal(options.start, true);
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
});

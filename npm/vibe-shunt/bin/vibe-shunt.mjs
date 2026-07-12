#!/usr/bin/env node

import { createHash, randomUUID } from "node:crypto";
import { createReadStream, createWriteStream, readFileSync, realpathSync } from "node:fs";
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  unlink,
  writeFile,
} from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline/promises";

export const INSTALLER_VERSION = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
).version;
export const DEFAULT_REPO = "chllming/shunt";
export const AUTO_SWARM_MARKETPLACE = "shunt";
export const AUTO_SWARM_PLUGIN = `auto-swarm@${AUTO_SWARM_MARKETPLACE}`;

const AUTO_SWARM_ASSET_ROOT = fileURLToPath(
  new URL("../assets/codex-marketplace", import.meta.url),
);
const MANAGED_MARKER = ".vibe-shunt-managed";
const MANAGED_MARKER_PREFIX = "vibe-shunt:auto-swarm:";

const REPO_PATTERN = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const TAG_PATTERN = /^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/;
const MAX_ARCHIVE_BYTES = 256 * 1024 * 1024;
const MAX_CHECKSUM_BYTES = 1024 * 1024;

export function usage() {
  return `vibe-shunt ${INSTALLER_VERSION}

Interactive, checksum-verified installer for Shunt.

Usage:
  npx vibe-shunt [options]

Options:
  -y, --yes                  Accept recommended defaults without prompting
      --repo <owner/repo>    GitHub release repository (default: ${DEFAULT_REPO})
      --version <tag>        Shunt release tag or "latest" (default: latest)
      --install-dir <path>   Binary destination (default: ~/.local/bin)
      --setup                Run first-time setup after installation
      --no-setup             Skip first-time setup
      --mode <mode>          Setup mode: website (recommended) or local
      --website-url <url>    Website3 base URL (default: https://beyondwork.ai)
      --install-clients      Install managed Claude and Codex client entries
      --no-install-clients   Do not install managed client entries
      --auto-swarm           Install the Auto Swarm Codex plugin and Claude skill
      --no-auto-swarm        Do not install the Auto Swarm client workflow
      --auto-swarm-max-agents <n>
                             Maximum parallel workers (default: 8, hard limit: 32)
      --auto-swarm-target <target>
                             Default target: auto, local, build-fra1, or hetzner-backup-substrate
      --uninstall-auto-swarm Remove only vibe-shunt-managed Codex/Claude Auto Swarm entries
      --service              Install and start the login service
      --no-service           Do not install a login service
      --start                Start Shunt once when not installing a service
      --no-start             Do not start Shunt
      --force                Replace an existing shunt binary
      --dry-run              Print the installation plan without changing files
  -h, --help                 Show this help

Environment:
  VIBE_SHUNT_REPO            Default GitHub repository
  VIBE_SHUNT_INSTALL_DIR     Default binary destination
  GITHUB_TOKEN               Optional token for private or rate-limited releases
`;
}

function takeValue(argv, index, flag) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return value;
}

export function parseArgs(argv, env = process.env) {
  const options = {
    repo: env.VIBE_SHUNT_REPO || env.SHUNT_INSTALL_REPO || DEFAULT_REPO,
    version: "latest",
    installDir: env.VIBE_SHUNT_INSTALL_DIR || join(homedir(), ".local", "bin"),
    setup: undefined,
    mode: undefined,
    websiteUrl: env.SHUNT_WEBSITE_URL || "https://beyondwork.ai",
    installClients: undefined,
    autoSwarm: undefined,
    autoSwarmMaxAgents: undefined,
    autoSwarmTarget: undefined,
    uninstallAutoSwarm: false,
    service: undefined,
    start: undefined,
    force: false,
    dryRun: false,
    yes: false,
    help: false,
    provided: new Set(),
  };

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const [flag, inlineValue] = argument.startsWith("--")
      ? argument.split(/=(.*)/s, 2)
      : [argument, undefined];
    const value = () => {
      if (inlineValue !== undefined) return inlineValue;
      const next = takeValue(argv, index, flag);
      index += 1;
      return next;
    };

    switch (flag) {
      case "-y":
      case "--yes":
        options.yes = true;
        break;
      case "-h":
      case "--help":
        options.help = true;
        break;
      case "--repo":
        options.repo = value();
        options.provided.add("repo");
        break;
      case "--version":
        options.version = value();
        options.provided.add("version");
        break;
      case "--install-dir":
        options.installDir = value();
        options.provided.add("installDir");
        break;
      case "--setup":
        options.setup = true;
        options.provided.add("setup");
        break;
      case "--no-setup":
        options.setup = false;
        options.provided.add("setup");
        break;
      case "--mode":
        options.mode = value();
        options.provided.add("mode");
        break;
      case "--website-url":
        options.websiteUrl = value();
        options.provided.add("websiteUrl");
        break;
      case "--install-clients":
        options.installClients = true;
        options.provided.add("installClients");
        break;
      case "--no-install-clients":
        options.installClients = false;
        options.provided.add("installClients");
        break;
      case "--auto-swarm":
        options.autoSwarm = true;
        options.provided.add("autoSwarm");
        break;
      case "--no-auto-swarm":
        options.autoSwarm = false;
        options.provided.add("autoSwarm");
        break;
      case "--auto-swarm-max-agents":
        options.autoSwarmMaxAgents = value();
        options.provided.add("autoSwarmMaxAgents");
        break;
      case "--auto-swarm-target":
        options.autoSwarmTarget = value();
        options.provided.add("autoSwarmTarget");
        break;
      case "--uninstall-auto-swarm":
        options.uninstallAutoSwarm = true;
        break;
      case "--service":
        options.service = true;
        options.provided.add("service");
        break;
      case "--no-service":
        options.service = false;
        options.provided.add("service");
        break;
      case "--start":
        options.start = true;
        options.provided.add("start");
        break;
      case "--no-start":
        options.start = false;
        options.provided.add("start");
        break;
      case "--force":
        options.force = true;
        break;
      case "--dry-run":
        options.dryRun = true;
        break;
      default:
        throw new Error(`unknown option: ${argument}`);
    }
  }

  return options;
}

export function targetFor(platform = process.platform, architecture = process.arch) {
  const targets = new Map([
    ["darwin:arm64", "aarch64-apple-darwin"],
    ["linux:x64", "x86_64-unknown-linux-gnu"],
    ["linux:arm64", "aarch64-unknown-linux-gnu"],
  ]);
  const target = targets.get(`${platform}:${architecture}`);
  if (!target) {
    const detail = platform === "darwin" && architecture === "x64"
      ? "macOS Intel release assets are not currently produced"
      : "only macOS arm64 and Linux x64/arm64 are currently supported";
    throw new Error(`unsupported platform ${platform}/${architecture}: ${detail}`);
  }
  return target;
}

export function validateOptions(options) {
  if (!REPO_PATTERN.test(options.repo)) {
    throw new Error(`invalid GitHub repository "${options.repo}"; expected owner/repo`);
  }
  if (options.version !== "latest" && !TAG_PATTERN.test(options.version)) {
    throw new Error(`invalid release version "${options.version}"; expected latest or vX.Y.Z`);
  }
  if (!options.installDir.trim()) {
    throw new Error("install directory cannot be empty");
  }
  if (dirname(resolve(options.installDir)) === resolve(options.installDir)) {
    throw new Error("install directory cannot be a filesystem root");
  }
  if (options.setup === false && options.installClients === true) {
    throw new Error("--install-clients requires --setup");
  }
  if (options.autoSwarm === true && (options.setup !== true || options.installClients !== true)) {
    throw new Error("--auto-swarm requires --setup and --install-clients");
  }
  if (!Number.isInteger(options.autoSwarmMaxAgents) ||
      options.autoSwarmMaxAgents < 1 || options.autoSwarmMaxAgents > 32) {
    throw new Error("--auto-swarm-max-agents must be an integer between 1 and 32");
  }
  const autoSwarmTargets = new Set(["auto", "local", "build-fra1", "hetzner-backup-substrate"]);
  if (!autoSwarmTargets.has(options.autoSwarmTarget)) {
    throw new Error(`invalid Auto Swarm target "${options.autoSwarmTarget}"`);
  }
  if (!options.autoSwarm &&
      (options.provided.has("autoSwarmMaxAgents") || options.provided.has("autoSwarmTarget"))) {
    throw new Error("Auto Swarm worker options require --auto-swarm");
  }
  if (!new Set(["website", "local"]).has(options.mode)) {
    throw new Error(`invalid setup mode "${options.mode}"; expected website or local`);
  }
  try {
    const websiteUrl = new URL(options.websiteUrl);
    if (!new Set(["http:", "https:"]).has(websiteUrl.protocol)) throw new Error();
    const loopback = new Set(["localhost", "127.0.0.1", "[::1]", "::1"]);
    if (websiteUrl.protocol === "http:" && !loopback.has(websiteUrl.hostname)) {
      throw new Error();
    }
  } catch {
    throw new Error(`invalid Website3 URL "${options.websiteUrl}"; use HTTPS except for loopback development`);
  }
  if (options.service === true && options.start === true) {
    throw new Error("--service and --start are mutually exclusive; the service starts Shunt");
  }
  return options;
}

export function validateUninstallOptions(options) {
  const conflictingFlags = [
    "repo",
    "version",
    "installDir",
    "setup",
    "mode",
    "websiteUrl",
    "installClients",
    "autoSwarm",
    "autoSwarmMaxAgents",
    "autoSwarmTarget",
    "service",
    "start",
  ];
  const conflict = conflictingFlags.find((flag) => options.provided.has(flag));
  if (conflict || options.force) {
    throw new Error("--uninstall-auto-swarm is mutually exclusive with installation and setup options");
  }
}

export function applyRecommendedDefaults(options) {
  const service = options.service ?? true;
  const setup = options.setup ?? true;
  const installClients = options.installClients ?? setup;
  return {
    ...options,
    installDir: resolve(options.installDir.replace(/^~(?=$|\/)/, homedir())),
    setup,
    mode: options.mode ?? "website",
    installClients,
    autoSwarm: options.autoSwarm ?? (setup && installClients),
    autoSwarmMaxAgents: options.autoSwarmMaxAgents === undefined
      ? 8
      : Number(options.autoSwarmMaxAgents),
    autoSwarmTarget: options.autoSwarmTarget ?? "auto",
    service,
    start: options.start ?? !service,
  };
}

async function askText(reader, question, current) {
  const answer = (await reader.question(`${question} [${current}]: `)).trim();
  return answer || current;
}

async function askBoolean(reader, question, recommended = true) {
  const marker = recommended ? "Y/n" : "y/N";
  while (true) {
    const answer = (await reader.question(`${question} [${marker}]: `)).trim().toLowerCase();
    if (!answer) return recommended;
    if (["y", "yes"].includes(answer)) return true;
    if (["n", "no"].includes(answer)) return false;
    process.stdout.write("Please answer yes or no.\n");
  }
}

export async function collectInteractiveOptions(options, input = process.stdin, output = process.stdout) {
  const interactive = !options.yes && input.isTTY && output.isTTY;
  if (!interactive) return applyRecommendedDefaults(options);

  const reader = createInterface({ input, output });
  try {
    output.write("\nConfigure your Shunt installation\n\n");
    if (!options.provided.has("repo")) {
      options.repo = await askText(reader, "GitHub release repository", options.repo);
    }
    if (!options.provided.has("version")) {
      options.version = await askText(reader, "Release tag", options.version);
    }
    if (!options.provided.has("installDir")) {
      options.installDir = await askText(reader, "Install directory", options.installDir);
    }
    if (options.setup === undefined) {
      options.setup = await askBoolean(reader, "Run Shunt setup after installing?", true);
    }
    if (options.setup && options.mode === undefined) {
      const useWebsite = await askBoolean(
        reader,
        "Use Website3 sign-in and your user-scoped vault? (choose no for local-only setup)",
        true,
      );
      options.mode = useWebsite ? "website" : "local";
    }
    if (options.setup && options.installClients === undefined) {
      options.installClients = await askBoolean(
        reader,
        "Install managed Claude and Codex client entries?",
        true,
      );
    }
    if (options.setup && options.installClients && options.autoSwarm === undefined) {
      options.autoSwarm = await askBoolean(
        reader,
        "Install the Auto Swarm workflow for Codex and Claude?",
        true,
      );
    }
    if (options.autoSwarm && options.autoSwarmMaxAgents === undefined) {
      options.autoSwarmMaxAgents = await askText(
        reader,
        "Maximum parallel Auto Swarm workers",
        "8",
      );
    }
    if (options.autoSwarm && options.autoSwarmTarget === undefined) {
      const hosted = await askBoolean(
        reader,
        "Use Website3-authorized hosted target selection by default? (choose no only for a loopback local coordinator)",
        true,
      );
      options.autoSwarmTarget = hosted ? "auto" : "local";
    }
    if (!options.setup && options.installClients === undefined) options.installClients = false;
    if ((!options.setup || !options.installClients) && options.autoSwarm === undefined) {
      options.autoSwarm = false;
    }
    if (options.service === undefined) {
      options.service = await askBoolean(reader, "Install and start Shunt as a login service?", true);
    }
    if (options.service) {
      options.start = false;
    } else if (options.start === undefined) {
      options.start = await askBoolean(reader, "Start Shunt once after installing?", true);
    }

    const planned = applyRecommendedDefaults(options);
    validateOptions(planned);
    const destination = join(planned.installDir, "shunt");
    if (!planned.dryRun && !planned.force && await pathExists(destination)) {
      planned.force = await askBoolean(reader, `Replace existing ${destination}?`, false);
      if (!planned.force) throw new Error("installation cancelled");
    }
    printPlan(planned, targetFor(), output);
    if (!(await askBoolean(reader, "Continue with this installation?", true))) {
      throw new Error("installation cancelled");
    }
    return planned;
  } finally {
    reader.close();
  }
}

export function parseChecksums(contents) {
  const entries = new Map();
  for (const line of contents.split(/\r?\n/)) {
    const match = line.trim().match(/^([a-fA-F0-9]{64})\s+[* ]?(.+)$/);
    if (match) entries.set(match[2], match[1].toLowerCase());
  }
  return entries;
}

export async function sha256File(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

export async function verifyChecksum(archivePath, checksumsPath, assetName) {
  const checksums = parseChecksums(await readFile(checksumsPath, "utf8"));
  const expected = checksums.get(assetName);
  if (!expected) throw new Error(`checksums.txt has no entry for ${assetName}`);
  const actual = await sha256File(archivePath);
  if (actual !== expected) {
    throw new Error(`checksum mismatch for ${assetName}: expected ${expected}, received ${actual}`);
  }
  return actual;
}

function githubHeaders() {
  const headers = {
    Accept: "application/vnd.github+json",
    "User-Agent": `vibe-shunt/${INSTALLER_VERSION}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  return headers;
}

async function resolveRelease(repo, version) {
  const endpoint = version === "latest"
    ? `https://api.github.com/repos/${repo}/releases/latest`
    : `https://api.github.com/repos/${repo}/releases/tags/${encodeURIComponent(normalizeTag(version))}`;
  const response = await fetch(endpoint, { headers: githubHeaders(), redirect: "follow" });
  if (!response.ok) {
    throw new Error(`GitHub release lookup failed (${response.status}) for ${repo} ${version}`);
  }
  const release = await response.json();
  if (!release.tag_name || !Array.isArray(release.assets)) {
    throw new Error("GitHub returned an invalid release response");
  }
  if (!TAG_PATTERN.test(release.tag_name)) {
    throw new Error(`GitHub returned an unsafe release tag "${release.tag_name}"`);
  }
  return release;
}

function normalizeTag(version) {
  return version.startsWith("v") ? version : `v${version}`;
}

function releaseAsset(release, name) {
  const asset = release.assets.find((candidate) => candidate.name === name);
  if (!asset?.browser_download_url) {
    throw new Error(`release ${release.tag_name} does not contain ${name}`);
  }
  return asset.browser_download_url;
}

async function download(url, destination, maxBytes) {
  const response = await fetch(url, { headers: githubHeaders(), redirect: "follow" });
  if (!response.ok || !response.body) {
    throw new Error(`download failed (${response.status}) for ${basename(destination)}`);
  }
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > maxBytes) {
    throw new Error(`download exceeds ${maxBytes} byte limit for ${basename(destination)}`);
  }
  let received = 0;
  const limiter = new Transform({
    transform(chunk, _encoding, callback) {
      received += chunk.length;
      if (received > maxBytes) {
        callback(new Error(`download exceeds ${maxBytes} byte limit for ${basename(destination)}`));
      } else {
        callback(null, chunk);
      }
    },
  });
  await pipeline(response.body, limiter, createWriteStream(destination, { mode: 0o600 }));
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${basename(command)} ${args.join(" ")} exited with status ${result.status}`);
  }
}

function capture(command, args) {
  const result = spawnSync(command, args, {
    encoding: "utf8",
    maxBuffer: 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${basename(command)} ${args[0]} rejected the release archive`);
  }
  return result.stdout;
}

export function validateArchiveListing(namesText, verboseText, bundleRoot) {
  const names = namesText.split(/\r?\n/).filter(Boolean);
  const verbose = verboseText.split(/\r?\n/).filter(Boolean);
  const expectedDirectory = `${bundleRoot}/`;
  const expectedBinary = `${bundleRoot}/shunt`;
  const allowed = new Set([
    expectedDirectory,
    expectedBinary,
    `${bundleRoot}/README.md`,
    `${bundleRoot}/LICENSE`,
  ]);
  if (names.length === 0 || names.length > allowed.size || new Set(names).size !== names.length) {
    throw new Error("release archive contains an invalid or duplicate entry set");
  }
  for (const name of names) {
    if (!allowed.has(name)) {
      throw new Error(`release archive contains unexpected entry: ${name}`);
    }
  }
  if (!names.includes(expectedBinary) || verbose.length !== names.length) {
    throw new Error("release archive does not contain the expected Shunt binary");
  }
  names.forEach((name, index) => {
    const type = verbose[index]?.[0];
    const expectedType = name === expectedDirectory ? "d" : "-";
    if (type !== expectedType) {
      throw new Error(`release archive entry has unsafe type: ${name}`);
    }
  });
}

function validateReleaseArchive(archivePath, bundleRoot) {
  const names = capture("tar", ["-tzf", archivePath]);
  const verbose = capture("tar", ["-tvzf", archivePath]);
  validateArchiveListing(names, verbose, bundleRoot);
}

async function pathExists(path) {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
}

async function installBinary(source, destination) {
  await mkdir(dirname(destination), { recursive: true });
  const temporary = join(dirname(destination), `.shunt-${process.pid}-${Date.now()}`);
  try {
    await copyFile(source, temporary);
    await chmod(temporary, 0o755);
    await rename(temporary, destination);
  } finally {
    await unlink(temporary).catch((error) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

function checkedBasePath(value, label) {
  if (!isAbsolute(value)) {
    throw new Error(`${label} must be an absolute path`);
  }
  const normalized = resolve(value);
  if (dirname(normalized) === normalized) {
    throw new Error(`${label} cannot be a filesystem root`);
  }
  return normalized;
}

export function autoSwarmInstallPaths({ home = homedir(), env = process.env } = {}) {
  const safeHome = checkedBasePath(resolve(home), "home directory");
  const dataHome = checkedBasePath(
    env.XDG_DATA_HOME || join(safeHome, ".local", "share"),
    "XDG_DATA_HOME",
  );
  const codexHome = checkedBasePath(
    env.CODEX_HOME || join(safeHome, ".codex"),
    "CODEX_HOME",
  );
  const claudeHome = checkedBasePath(
    env.CLAUDE_CONFIG_DIR || join(safeHome, ".claude"),
    "CLAUDE_CONFIG_DIR",
  );
  return {
    codexHome,
    claudeHome,
    marketplaceRoot: join(dataHome, "shunt", "codex-marketplace"),
    codexSkillRoot: join(codexHome, "skills", "auto-swarm"),
    claudeSkillRoot: join(claudeHome, "skills", "auto-swarm"),
  };
}

async function copyTrustedTree(source, destination) {
  const metadata = await lstat(source);
  if (metadata.isSymbolicLink()) {
    throw new Error(`refusing symlink in packaged Auto Swarm assets: ${source}`);
  }
  if (metadata.isDirectory()) {
    await mkdir(destination, { recursive: true, mode: 0o755 });
    const entries = await readdir(source, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isSymbolicLink()) {
        throw new Error(`refusing symlink in packaged Auto Swarm assets: ${join(source, entry.name)}`);
      }
      await copyTrustedTree(join(source, entry.name), join(destination, entry.name));
    }
    return;
  }
  if (!metadata.isFile()) {
    throw new Error(`refusing special file in packaged Auto Swarm assets: ${source}`);
  }
  await copyFile(source, destination);
  await chmod(destination, metadata.mode & 0o777);
}

async function validateManagedDestination(destination) {
  let metadata;
  try {
    metadata = await lstat(destination);
  } catch (error) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
  if (metadata.isSymbolicLink()) {
    throw new Error(`refusing to replace symlinked Auto Swarm destination: ${destination}`);
  }
  if (!metadata.isDirectory()) {
    throw new Error(`refusing to replace non-directory Auto Swarm destination: ${destination}`);
  }
  const markerPath = join(destination, MANAGED_MARKER);
  let markerMetadata;
  try {
    markerMetadata = await lstat(markerPath);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error(`refusing to replace unmanaged Auto Swarm destination: ${destination}`);
    }
    throw error;
  }
  if (!markerMetadata.isFile() || markerMetadata.size > 256) {
    throw new Error(`invalid Auto Swarm management marker: ${markerPath}`);
  }
  const marker = await readFile(markerPath, "utf8");
  if (!marker.startsWith(MANAGED_MARKER_PREFIX)) {
    throw new Error(`refusing to replace unmanaged Auto Swarm destination: ${destination}`);
  }
  return true;
}

export async function replaceManagedDirectory(source, destination, configure = async () => {}) {
  const safeSource = checkedBasePath(resolve(source), "asset source");
  const safeDestination = checkedBasePath(resolve(destination), "managed destination");
  const existed = await validateManagedDestination(safeDestination);
  const parent = dirname(safeDestination);
  await mkdir(parent, { recursive: true, mode: 0o700 });
  const temporary = await mkdtemp(join(parent, ".auto-swarm-install-"));
  let backup;
  let installed = false;
  try {
    await copyTrustedTree(safeSource, temporary);
    await configure(temporary);
    await writeFile(
      join(temporary, MANAGED_MARKER),
      `${MANAGED_MARKER_PREFIX}${INSTALLER_VERSION}\n`,
      { mode: 0o600, flag: "wx" },
    );
    if (existed) {
      backup = join(parent, `.auto-swarm-backup-${randomUUID()}`);
      await rename(safeDestination, backup);
    }
    try {
      await rename(temporary, safeDestination);
      installed = true;
    } catch (error) {
      if (backup) await rename(backup, safeDestination);
      throw error;
    }
    if (backup) await rm(backup, { recursive: true, force: true });
  } finally {
    if (!installed) await rm(temporary, { recursive: true, force: true });
  }
}

function commandAvailable(command, env) {
  const result = spawnSync(command, ["--version"], { env, stdio: "ignore" });
  return !result.error && result.status === 0;
}

export async function installAutoSwarmClients({
  shuntPath,
  home = homedir(),
  env = process.env,
  runner = run,
  codexAvailable,
  output = process.stdout,
} = {}) {
  if (!shuntPath || !isAbsolute(shuntPath)) {
    throw new Error("Auto Swarm installation requires an absolute Shunt executable path");
  }
  const shuntMetadata = await stat(shuntPath).catch((error) => {
    if (error.code === "ENOENT") throw new Error(`Shunt executable does not exist: ${shuntPath}`);
    throw error;
  });
  if (!shuntMetadata.isFile()) throw new Error(`Shunt executable is not a file: ${shuntPath}`);

  const paths = autoSwarmInstallPaths({ home, env });
  await replaceManagedDirectory(AUTO_SWARM_ASSET_ROOT, paths.marketplaceRoot, async (root) => {
    const mcpPath = join(root, "plugins", "auto-swarm", ".mcp.json");
    const mcp = JSON.parse(await readFile(mcpPath, "utf8"));
    if (!mcp?.mcpServers?.shunt || !Array.isArray(mcp.mcpServers.shunt.args)) {
      throw new Error("packaged Auto Swarm MCP configuration is invalid");
    }
    mcp.mcpServers.shunt.command = resolve(shuntPath);
    await writeFile(mcpPath, `${JSON.stringify(mcp, null, 2)}\n`, { mode: 0o644 });
  });

  const hasCodex = codexAvailable ?? commandAvailable("codex", env);
  if (hasCodex) {
    await mkdir(paths.codexHome, { recursive: true, mode: 0o700 });
    runner("codex", ["plugin", "marketplace", "add", paths.marketplaceRoot, "--json"], { env });
    runner("codex", ["plugin", "add", AUTO_SWARM_PLUGIN, "--json"], { env });
    output.write(`Installed Codex plugin ${AUTO_SWARM_PLUGIN}.\n`);
  } else {
    const skillSource = join(
      paths.marketplaceRoot,
      "plugins",
      "auto-swarm",
      "skills",
      "auto-swarm",
    );
    await replaceManagedDirectory(skillSource, paths.codexSkillRoot);
    output.write("Codex CLI was not found; installed the Auto Swarm skill directly.\n");
  }

  const claudeSkillSource = join(
    paths.marketplaceRoot,
    "plugins",
    "auto-swarm",
    "skills",
    "auto-swarm",
  );
  await replaceManagedDirectory(claudeSkillSource, paths.claudeSkillRoot);
  output.write("Installed the Claude /auto-swarm skill.\n");
}

export async function uninstallAutoSwarmClients({
  home = homedir(),
  env = process.env,
  runner = run,
  codexAvailable,
  dryRun = false,
  output = process.stdout,
} = {}) {
  const paths = autoSwarmInstallPaths({ home, env });
  const managedPaths = [
    paths.marketplaceRoot,
    paths.codexSkillRoot,
    paths.claudeSkillRoot,
  ];
  const existing = [];
  // Validate every destination before changing Codex configuration or files.
  // One unmanaged/symlinked path aborts the whole operation.
  for (const destination of managedPaths) {
    if (await validateManagedDestination(destination)) existing.push(destination);
  }
  if (dryRun) {
    for (const destination of existing) output.write(`Would remove managed Auto Swarm entry ${destination}.\n`);
    if (existing.length === 0) output.write("No managed Auto Swarm entries found.\n");
    return { removed: [] };
  }

  if (existing.includes(paths.marketplaceRoot) &&
      (codexAvailable ?? commandAvailable("codex", env))) {
    // The filesystem marker proves this is our marketplace before we alter
    // Codex's registry. Removal commands are idempotent across Codex versions;
    // an already-absent registry entry must not prevent managed-file cleanup.
    try {
      runner("codex", ["plugin", "remove", AUTO_SWARM_PLUGIN, "--json"], { env });
    } catch {
      output.write(`Codex plugin ${AUTO_SWARM_PLUGIN} was already absent.\n`);
    }
    try {
      runner("codex", ["plugin", "marketplace", "remove", AUTO_SWARM_MARKETPLACE, "--json"], { env });
    } catch {
      output.write(`Codex marketplace ${AUTO_SWARM_MARKETPLACE} was already absent.\n`);
    }
  }
  for (const destination of existing) {
    await rm(destination, { recursive: true, force: false });
    output.write(`Removed managed Auto Swarm entry ${destination}.\n`);
  }
  if (existing.length === 0) output.write("No managed Auto Swarm entries found.\n");
  return { removed: existing };
}

function printChoice(label, value, output = process.stdout) {
  output.write(`  ${label.padEnd(18)} ${value}\n`);
}

export function printPlan(options, target, output = process.stdout) {
  output.write("\nInstallation plan\n");
  printChoice("Repository", options.repo, output);
  printChoice("Release", options.version, output);
  printChoice("Platform", target, output);
  printChoice("Destination", join(options.installDir, "shunt"), output);
  printChoice("Run setup", options.setup ? "yes" : "no", output);
  printChoice("Credential mode", options.setup ? options.mode : "not configured", output);
  if (options.setup && options.mode === "website") printChoice("Website3", options.websiteUrl, output);
  printChoice("Client entries", options.installClients ? "yes" : "no", output);
  printChoice("Auto Swarm", options.autoSwarm ? "Codex + Claude" : "no", output);
  if (options.autoSwarm) {
    printChoice("Swarm target", options.autoSwarmTarget, output);
    printChoice("Worker ceiling", String(options.autoSwarmMaxAgents), output);
  }
  printChoice("Login service", options.service ? "yes" : "no", output);
  printChoice("Start once", options.start ? "yes" : "no", output);
  output.write("\n");
}

function printPathHint(installDir) {
  const pathEntries = (process.env.PATH || "").split(process.platform === "win32" ? ";" : ":");
  if (!pathEntries.includes(installDir)) {
    process.stdout.write(`Add ${installDir} to PATH to run shunt from a new shell.\n`);
  }
}

export function setupArgsFor(options) {
  const args = ["setup", "--mode", options.mode];
  if (options.mode === "website") args.push("--website-url", options.websiteUrl);
  if (options.installClients) args.push("--install-clients");
  if (options.autoSwarm) {
    args.push(
      "--manual-swarm",
      "--manual-swarm-max-agents",
      String(options.autoSwarmMaxAgents),
      "--manual-swarm-default-target",
      options.autoSwarmTarget,
    );
  }
  return args;
}

export async function install(options) {
  validateOptions(options);
  const target = targetFor();
  const destination = join(options.installDir, "shunt");
  printPlan(options, target);

  if (options.dryRun) {
    process.stdout.write("Dry run complete; no files or services were changed.\n");
    return;
  }

  if ((await pathExists(destination)) && !options.force) {
    throw new Error(`${destination} already exists; rerun with --force to replace it`);
  }

  process.stdout.write(`Resolving ${options.repo} ${options.version}...\n`);
  const release = await resolveRelease(options.repo, options.version);
  const tag = release.tag_name;
  const assetName = `shunt-${tag}-${target}.tar.gz`;
  const archiveUrl = releaseAsset(release, assetName);
  const checksumsUrl = releaseAsset(release, "checksums.txt");
  const temporaryRoot = await mkdtemp(join(tmpdir(), "vibe-shunt-"));

  try {
    const archivePath = join(temporaryRoot, assetName);
    const checksumsPath = join(temporaryRoot, "checksums.txt");
    process.stdout.write(`Downloading ${assetName}...\n`);
    await Promise.all([
      download(archiveUrl, archivePath, MAX_ARCHIVE_BYTES),
      download(checksumsUrl, checksumsPath, MAX_CHECKSUM_BYTES),
    ]);
    const checksum = await verifyChecksum(archivePath, checksumsPath, assetName);
    process.stdout.write(`Verified SHA-256 ${checksum}\n`);

    const extractionRoot = join(temporaryRoot, "extracted");
    await mkdir(extractionRoot);
    const bundleRoot = `shunt-${tag}-${target}`;
    validateReleaseArchive(archivePath, bundleRoot);
    run("tar", ["-xzf", archivePath, "-C", extractionRoot]);
    const sourceRoot = join(extractionRoot, bundleRoot);
    const source = join(sourceRoot, "shunt");
    const rootMetadata = await lstat(sourceRoot).catch(() => undefined);
    const sourceMetadata = await lstat(source).catch(() => undefined);
    if (!rootMetadata?.isDirectory() || rootMetadata.isSymbolicLink() ||
        !sourceMetadata?.isFile() || sourceMetadata.isSymbolicLink()) {
      throw new Error("release archive did not contain a regular Shunt binary");
    }
    await installBinary(source, destination);
    process.stdout.write(`Installed Shunt ${tag} at ${destination}\n`);
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }

  if (options.setup) {
    run(destination, setupArgsFor(options));
  }
  if (options.autoSwarm) {
    await installAutoSwarmClients({ shuntPath: destination });
  }
  if (options.service) {
    run(destination, ["service", "install"]);
  } else if (options.start) {
    run(destination, ["start"]);
  }

  printPathHint(options.installDir);
  process.stdout.write("vibe-shunt installation complete.\n");
}

export async function main(argv = process.argv.slice(2)) {
  const parsed = parseArgs(argv);
  if (parsed.help) {
    process.stdout.write(usage());
    return;
  }
  if (parsed.uninstallAutoSwarm) {
    validateUninstallOptions(parsed);
    await uninstallAutoSwarmClients({ dryRun: parsed.dryRun });
    return;
  }
  const options = await collectInteractiveOptions(parsed);
  await install(options);
}

const invokedPath = process.argv[1]
  ? pathToFileURL(realpathSync(resolve(process.argv[1]))).href
  : "";
if (invokedPath === import.meta.url) {
  main().catch((error) => {
    process.stderr.write(`vibe-shunt: ${error.message}\n`);
    process.exitCode = 1;
  });
}

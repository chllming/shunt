#!/usr/bin/env node

import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, readFileSync, realpathSync } from "node:fs";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  stat,
  unlink,
} from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline/promises";

export const INSTALLER_VERSION = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
).version;
export const DEFAULT_REPO = "chllming/shunt";

const REPO_PATTERN = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const TAG_PATTERN = /^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/;

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
      --install-clients      Install managed Claude and Codex client entries
      --no-install-clients   Do not install managed client entries
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
    installClients: undefined,
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
      case "--install-clients":
        options.installClients = true;
        options.provided.add("installClients");
        break;
      case "--no-install-clients":
        options.installClients = false;
        options.provided.add("installClients");
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
  if (options.setup === false && options.installClients === true) {
    throw new Error("--install-clients requires --setup");
  }
  if (options.service === true && options.start === true) {
    throw new Error("--service and --start are mutually exclusive; the service starts Shunt");
  }
  return options;
}

export function applyRecommendedDefaults(options) {
  const service = options.service ?? true;
  return {
    ...options,
    installDir: resolve(options.installDir.replace(/^~(?=$|\/)/, homedir())),
    setup: options.setup ?? true,
    installClients: options.installClients ?? (options.setup ?? true),
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
    if (options.setup && options.installClients === undefined) {
      options.installClients = await askBoolean(
        reader,
        "Install managed Claude and Codex client entries?",
        true,
      );
    }
    if (!options.setup) options.installClients = false;
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

async function download(url, destination) {
  const response = await fetch(url, { headers: githubHeaders(), redirect: "follow" });
  if (!response.ok || !response.body) {
    throw new Error(`download failed (${response.status}) for ${basename(destination)}`);
  }
  await pipeline(response.body, createWriteStream(destination, { mode: 0o600 }));
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${basename(command)} ${args.join(" ")} exited with status ${result.status}`);
  }
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
  printChoice("Client entries", options.installClients ? "yes" : "no", output);
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

export async function install(options) {
  validateOptions(options);
  const target = targetFor();
  const destination = join(options.installDir, "shunt");
  printPlan(options, target);

  if (options.dryRun) {
    process.stdout.write("Dry run complete; no files or services were changed.\n");
    return;
  }

  if ((await pathExists(destination)) && !options.force && !options.yes) {
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
      download(archiveUrl, archivePath),
      download(checksumsUrl, checksumsPath),
    ]);
    const checksum = await verifyChecksum(archivePath, checksumsPath, assetName);
    process.stdout.write(`Verified SHA-256 ${checksum}\n`);

    const extractionRoot = join(temporaryRoot, "extracted");
    await mkdir(extractionRoot);
    run("tar", ["-xzf", archivePath, "-C", extractionRoot]);
    const source = join(extractionRoot, `shunt-${tag}-${target}`, "shunt");
    if (!(await pathExists(source))) throw new Error(`release archive did not contain ${source}`);
    await installBinary(source, destination);
    process.stdout.write(`Installed Shunt ${tag} at ${destination}\n`);
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }

  if (options.setup) {
    const setupArgs = ["setup"];
    if (options.installClients) setupArgs.push("--install-clients");
    run(destination, setupArgs);
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

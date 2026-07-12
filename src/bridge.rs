use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Semaphore;

use crate::config::{load_config, BridgeConfig, NetworkPolicy};

static CODEX_SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
static CLAUDE_SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
static ACTIVE_JOBS: AtomicUsize = AtomicUsize::new(0);

struct ActiveJobGuard;
impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        ACTIVE_JOBS.fetch_sub(1, Ordering::AcqRel);
    }
}

struct FileLockGuard(PathBuf);
impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn acquire_apply_lock(path: &Path) -> Result<FileLockGuard> {
    let create = || {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    };
    match create() {
        Ok(_) => Ok(FileLockGuard(path.to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let stale = std::fs::metadata(path)
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > std::time::Duration::from_secs(10 * 60));
            if !stale {
                bail!("another Shunt patch apply is in progress");
            }
            std::fs::remove_file(path).context("failed to remove stale Shunt apply lock")?;
            create().context("another Shunt patch apply is in progress")?;
            Ok(FileLockGuard(path.to_owned()))
        }
        Err(error) => Err(error).context("failed to create Shunt apply lock"),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn bridge_root() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("shunt/bridge/jobs")
}

fn job_dir(id: &str) -> PathBuf {
    bridge_root().join(id)
}
fn job_path(id: &str) -> PathBuf {
    job_dir(id).join("job.json")
}

fn validate_job_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        bail!("invalid bridge job id");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeJob {
    pub id: String,
    pub status: JobStatus,
    pub provider: String,
    pub caller: String,
    #[serde(default)]
    pub depth: u8,
    pub workspace: PathBuf,
    pub base_commit: Option<String>,
    pub dirty_paths: Vec<String>,
    pub mode: String,
    pub network: NetworkPolicy,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub summary: Option<String>,
    pub patch_path: Option<PathBuf>,
    pub applied: bool,
    pub review_provider: Option<String>,
    pub review_approved: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolInput {
    task: Option<String>,
    workspace: Option<PathBuf>,
    mode: Option<String>,
    model: Option<String>,
    task_kind: Option<String>,
    network: Option<NetworkPolicy>,
    #[serde(default)]
    allowed_domains: Vec<String>,
    timeout: Option<u64>,
    id: Option<String>,
}

fn save_job(job: &BridgeJob) -> Result<()> {
    let dir = job_dir(&job.id);
    std::fs::create_dir_all(&dir)?;
    let path = job_path(&job.id);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(job)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub fn get_job(id: &str) -> Result<Value> {
    validate_job_id(id)?;
    let bytes = std::fs::read(job_path(id)).with_context(|| format!("unknown bridge job {id}"))?;
    Ok(serde_json::from_slice::<Value>(&bytes)?)
}

pub fn list_jobs() -> Result<Value> {
    let mut jobs = Vec::new();
    let Ok(entries) = std::fs::read_dir(bridge_root()) else {
        return Ok(Value::Array(jobs));
    };
    for entry in entries.flatten() {
        if let Ok(bytes) = std::fs::read(entry.path().join("job.json")) {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                jobs.push(value);
            }
        }
    }
    jobs.sort_by_key(|v| {
        std::cmp::Reverse(v.get("created_ms").and_then(Value::as_u64).unwrap_or(0))
    });
    Ok(Value::Array(jobs))
}

pub fn cancel_job(id: &str) -> Result<()> {
    validate_job_id(id)?;
    let dir = job_dir(id);
    if !dir.exists() {
        bail!("unknown bridge job {id}");
    }
    std::fs::write(dir.join("cancel"), b"cancel requested\n")?;
    Ok(())
}

pub fn cleanup_jobs(retention_hours: u64) -> Result<()> {
    let cutoff = now_ms().saturating_sub(retention_hours.saturating_mul(3_600_000));
    let Ok(entries) = std::fs::read_dir(bridge_root()) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let updated = std::fs::read(path.join("job.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<BridgeJob>(&b).ok())
            .map(|job| {
                if job.updated_ms < cutoff {
                    let worktree = path.join("worktree");
                    if worktree.exists() {
                        let _ = std::process::Command::new("git")
                            .arg("-C")
                            .arg(&job.workspace)
                            .args(["worktree", "remove", "--force"])
                            .arg(&worktree)
                            .status();
                    }
                }
                job.updated_ms
            })
            .unwrap_or(0);
        if updated < cutoff {
            let _ = std::fs::remove_dir_all(path);
        }
    }
    Ok(())
}

async fn git_output(workspace: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn redact(mut text: String) -> String {
    for prefix in ["sk-ant-", "sk-proj-", "sk-"] {
        while let Some(start) = text.find(prefix) {
            let end = text[start..]
                .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | '}'))
                .map(|n| start + n)
                .unwrap_or(text.len());
            text.replace_range(start..end, "[REDACTED]");
        }
    }
    if text.len() > 256 * 1024 {
        text.truncate(256 * 1024);
        text.push_str("\n[truncated]");
    }
    text
}

fn scrub_environment(command: &mut Command) {
    for key in [
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GROQ_API_KEY",
        "MISTRAL_API_KEY",
        "OPENROUTER_API_KEY",
        "TOGETHER_API_KEY",
        "DEEPSEEK_API_KEY",
        "GEMINI_API_KEY",
    ] {
        command.env_remove(key);
    }
}

fn scrub_configured_secrets(command: &mut Command, config: &crate::config::Config) {
    for key in [
        config.pools.claude.overflow.key_env.as_deref(),
        config.pools.codex.overflow.key_env.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        command.env_remove(key);
    }
    if let Some(path) = config.secrets.env_file.as_deref() {
        if let Ok(text) = std::fs::read_to_string(path) {
            for line in text.lines() {
                let line = line
                    .trim()
                    .strip_prefix("export ")
                    .unwrap_or(line.trim())
                    .trim_start();
                if let Some((key, _)) = line.split_once('=') {
                    let key = key.trim();
                    if !key.is_empty() {
                        command.env_remove(key);
                    }
                }
            }
        }
    }
}

pub(crate) fn normalize_allowed_domains(
    network: NetworkPolicy,
    allowed_domains: &[String],
) -> Result<Vec<String>> {
    if network != NetworkPolicy::Allowlisted {
        return Ok(Vec::new());
    }
    if allowed_domains.is_empty() {
        bail!("allowlisted network requires allowedDomains");
    }

    let mut normalized = BTreeSet::new();
    for raw in allowed_domains {
        let value = raw.trim();
        if value.is_empty() || value == "*" {
            bail!("invalid allowed domain '{raw}': unrestricted '*' is not an allowlist entry");
        }

        let (wildcard, host) = value
            .strip_prefix("*.")
            .map_or((false, value), |host| (true, host));
        let host = match url::Host::parse(host) {
            Ok(url::Host::Domain(domain)) => domain.to_ascii_lowercase(),
            Ok(url::Host::Ipv4(address)) if !wildcard => address.to_string(),
            Ok(url::Host::Ipv6(address)) if !wildcard => address.to_string(),
            _ => bail!(
                "invalid allowed domain '{raw}': expected a hostname, IP address, or leading '*.' DNS wildcard"
            ),
        };
        normalized.insert(if wildcard { format!("*.{host}") } else { host });
    }

    Ok(normalized.into_iter().collect())
}

fn codex_client_config(host: &str, port: u16, shunt: &Path) -> String {
    use toml_edit::{value, Array, DocumentMut};

    let mut config = DocumentMut::new();
    config["model_provider"] = value("shunt-codex");
    config["model_providers"]["shunt-codex"]["base_url"] =
        value(format!("http://{host}:{port}/backend-api/codex"));
    config["model_providers"]["shunt-codex"]["name"] = value("Shunt Codex");
    config["model_providers"]["shunt-codex"]["wire_api"] = value("responses");
    config["model_providers"]["shunt-codex"]["supports_websockets"] = value(false);
    config["model_providers"]["shunt-codex"]["auth"]["command"] =
        value(shunt.to_string_lossy().as_ref());
    config["model_providers"]["shunt-codex"]["auth"]["args"] =
        value(Array::from_iter(["client-token", "codex"]));
    config["model_providers"]["shunt-codex"]["auth"]["refresh_interval_ms"] = value(300_000);

    config.to_string()
}

async fn run_agent(
    provider: &str,
    task: &str,
    worktree: &Path,
    model: Option<&str>,
    network: NetworkPolicy,
    allowed_domains: &[String],
    timeout_secs: u64,
    config: &crate::config::Config,
    job_home: &Path,
    read_only: bool,
    depth: u8,
) -> Result<String> {
    let mut command;
    if provider == "codex" {
        let codex_home = job_home.join("codex");
        std::fs::create_dir_all(&codex_home)?;
        let shunt = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("shunt"));
        let client_config =
            codex_client_config(&config.server.host, config.pools.codex.port, &shunt);
        std::fs::write(codex_home.join("config.toml"), client_config)?;
        command =
            Command::new(std::env::var_os("SHUNT_CODEX_BIN").unwrap_or_else(|| "codex".into()));
        command
            .current_dir(worktree)
            .args([
                "exec",
                "--json",
                "--ephemeral",
                "--dangerously-bypass-approvals-and-sandbox",
                "-C",
            ])
            .arg(worktree);
        if let Some(model) = model {
            command.args(["--model", model]);
        }
        command.arg(task).env("CODEX_HOME", codex_home);
    } else {
        let home = job_home.join("claude-home");
        let settings_dir = home.join(".claude");
        let claude_token = crate::config::local_client_token("claude")?;
        std::fs::create_dir_all(&settings_dir)?;
        let network_json = match network {
            NetworkPolicy::None => json!({"allowedDomains": []}),
            NetworkPolicy::Allowlisted => json!({"allowedDomains": allowed_domains}),
            NetworkPolicy::Unrestricted => json!({"allowedDomains": ["*"]}),
        };
        std::fs::write(
            settings_dir.join("settings.json"),
            serde_json::to_vec_pretty(&json!({
                "sandbox": {
                    "enabled": true,
                    "failIfUnavailable": true,
                    "allowUnsandboxedCommands": false,
                    "network": network_json
                },
                "env": {
                    "ANTHROPIC_BASE_URL": format!("http://{}:{}", config.server.host, config.pools.claude.port),
                    "ANTHROPIC_API_KEY": claude_token.clone()
                }
            }))?,
        )?;
        command =
            Command::new(std::env::var_os("SHUNT_CLAUDE_BIN").unwrap_or_else(|| "claude".into()));
        let permission_mode = if read_only { "plan" } else { "acceptEdits" };
        command.current_dir(worktree).args([
            "-p",
            "--output-format",
            "stream-json",
            "--permission-mode",
            permission_mode,
            "--no-session-persistence",
        ]);
        if let Some(model) = model {
            command.args(["--model", model]);
        }
        command
            .arg(task)
            .env("HOME", home)
            .env(
                "ANTHROPIC_BASE_URL",
                format!("http://{}:{}", config.server.host, config.pools.claude.port),
            )
            .env("ANTHROPIC_API_KEY", &claude_token);
    }
    scrub_environment(&mut command);
    scrub_configured_secrets(&mut command, config);
    command.env("SHUNT_BRIDGE_DEPTH", depth.saturating_add(1).to_string());
    if provider == "claude" {
        command.env(
            "ANTHROPIC_API_KEY",
            crate::config::local_client_token("claude")?,
        );
    }
    command
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn()?;
    let cancel_path = job_home.join("cancel");
    let wait = async move {
        let output = child.wait_with_output();
        tokio::pin!(output);
        loop {
            tokio::select! {
                result = &mut output => return result.map_err(anyhow::Error::from),
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {
                    if cancel_path.exists() { bail!("cancelled"); }
                }
            }
        }
    };
    let output = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), wait)
        .await
        .context("bridge agent timed out")??;
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        bail!("{provider} agent failed: {}", redact(combined));
    }
    Ok(redact(combined))
}

fn overlap_with_dirty(dirty: &[String], changed: &[String]) -> bool {
    dirty.iter().any(|line| {
        let path = line.get(3..).unwrap_or(line).trim();
        path.split(" -> ")
            .any(|path| changed.iter().any(|changed| changed == path))
    })
}

fn has_exact_review_approval(output: &str) -> bool {
    const MARKER: &str = "SHUNT_REVIEW_APPROVED";
    let exact_last_line = |text: &str| {
        text.lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .map(str::trim)
            == Some(MARKER)
    };
    if exact_last_line(output) {
        return true;
    }
    output
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .any(|value| {
            [
                value.get("result"),
                value.pointer("/item/text"),
                value.get("text"),
            ]
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .any(&exact_last_line)
        })
}

async fn execute_job(
    mut job: BridgeJob,
    input: ToolInput,
    bridge: BridgeConfig,
    config_path: Option<PathBuf>,
) {
    let _active_guard = ActiveJobGuard;
    let result: Result<()> = async {
        job.status = JobStatus::Running; job.updated_ms = now_ms(); save_job(&job)?;
        if job_dir(&job.id).join("cancel").exists() { bail!("cancelled"); }
        let config = load_config(config_path.as_deref())?;
        if !bridge.network_ceiling.permits(job.network) { bail!("requested network policy exceeds operator ceiling"); }
        if normalize_allowed_domains(job.network, &job.allowed_domains)? != job.allowed_domains {
            bail!("bridge job contains a non-canonical network allowlist");
        }
        if job.depth >= bridge.max_depth { bail!("bridge recursion depth exceeded"); }

        let repo = git_output(&job.workspace, &["rev-parse", "--show-toplevel"]).await?;
        let repo = PathBuf::from(repo);
        let base = git_output(&repo, &["rev-parse", "HEAD"]).await?;
        job.base_commit = Some(base.clone());
        let dirty = git_output(&repo, &["status", "--porcelain"]).await.unwrap_or_default();
        job.dirty_paths = dirty.lines().map(ToOwned::to_owned).collect(); save_job(&job)?;

        let worktree = job_dir(&job.id).join("worktree");
        let worktree_s = worktree.to_string_lossy().into_owned();
        let output = Command::new("git").arg("-C").arg(&repo)
            .args(["worktree", "add", "--detach", &worktree_s, &base]).output().await?;
        if !output.status.success() { bail!("failed to create isolated worktree: {}", String::from_utf8_lossy(&output.stderr)); }

        let semaphore = if job.provider == "codex" {
            CODEX_SLOTS.get_or_init(|| Arc::new(Semaphore::new(bridge.concurrency_per_provider))).clone()
        } else {
            CLAUDE_SLOTS.get_or_init(|| Arc::new(Semaphore::new(bridge.concurrency_per_provider))).clone()
        };
        let _permit = semaphore.acquire().await?;
        let task = input.task.as_deref().context("task is required")?;
        let mut models = Vec::new();
        models.push(input.model.clone());
        let fallbacks = if job.provider == "codex" { &bridge.codex_fallback_models } else { &bridge.claude_fallback_models };
        models.extend(fallbacks.iter().cloned().map(Some));
        let mut output = None;
        let mut last_error = None;
        for model in models {
            match run_agent(&job.provider, task, &worktree, model.as_deref(), job.network,
                &job.allowed_domains, input.timeout.unwrap_or(bridge.timeout_secs).min(bridge.timeout_secs),
                &config, &job_dir(&job.id), job.mode == "consult", job.depth).await {
                Ok(value) => { output = Some(value); break; }
                Err(error) => last_error = Some(error),
            }
        }
        let output = output.ok_or_else(|| last_error.unwrap_or_else(|| anyhow::anyhow!("no bridge model available")))?;
        std::fs::write(job_dir(&job.id).join("output.txt"), &output)?;
        job.summary = Some(output.lines().last().unwrap_or("agent completed").chars().take(500).collect());

        let intent = Command::new("git").arg("-C").arg(&worktree)
            .args(["add", "-N", "--", "."]).output().await?;
        if !intent.status.success() {
            bail!("failed to include new files in bridge patch: {}", String::from_utf8_lossy(&intent.stderr));
        }
        let patch = Command::new("git").arg("-C").arg(&worktree)
            .args(["diff", "--binary", &base]).output().await?;
        let patch_path = job_dir(&job.id).join("changes.patch");
        std::fs::write(&patch_path, &patch.stdout)?;
        job.patch_path = Some(patch_path.clone());

        let mut check_log = String::new();
        let diff_check = Command::new("git").arg("-C").arg(&worktree).args(["diff", "--check"]).output().await?;
        if !diff_check.status.success() { bail!("git diff --check failed"); }
        for check in &bridge.required_checks {
            let mut check_command = Command::new("sh");
            check_command.current_dir(&worktree).args(["-lc", check]);
            scrub_environment(&mut check_command);
            scrub_configured_secrets(&mut check_command, &config);
            check_command.env("SHUNT_BRIDGE_DEPTH", job.depth.saturating_add(1).to_string());
            let checked = tokio::time::timeout(
                std::time::Duration::from_secs(input.timeout.unwrap_or(bridge.timeout_secs).min(bridge.timeout_secs)),
                check_command.output(),
            ).await.context("configured check timed out")??;
            check_log.push_str(&format!("$ {check}\n{}{}\n", String::from_utf8_lossy(&checked.stdout), String::from_utf8_lossy(&checked.stderr)));
            if !checked.status.success() { bail!("configured check failed: {check}"); }
        }
        if !check_log.is_empty() { std::fs::write(job_dir(&job.id).join("checks.txt"), redact(check_log))?; }

        if job.mode == "apply" && !patch.stdout.is_empty() {
            let reviewer = if job.provider == "codex" { "claude" } else { "codex" };
            job.review_provider = Some(reviewer.into()); save_job(&job)?;
            let review_task = format!(
                "Independently review the current worktree diff against this task:\n{task}\nRun appropriate checks. If and only if it is correct and safe, end with the exact marker SHUNT_REVIEW_APPROVED. Do not edit files.");
            let review = run_agent(reviewer, &review_task, &worktree, None, NetworkPolicy::None, &[],
                input.timeout.unwrap_or(bridge.timeout_secs).min(bridge.timeout_secs), &config, &job_dir(&job.id), true, job.depth).await?;
            std::fs::write(job_dir(&job.id).join("review.txt"), &review)?;
            job.review_approved = has_exact_review_approval(&review);
            if !job.review_approved { bail!("independent review did not approve; patch retained without touching parent"); }
            let head_now = git_output(&repo, &["rev-parse", "HEAD"]).await?;
            if head_now != base { bail!("parent HEAD changed; patch retained"); }
            let changed = git_output(&worktree, &["diff", "--name-only", &base]).await?.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
            let current_dirty = git_output(&repo, &["status", "--porcelain"]).await.unwrap_or_default();
            let current_dirty = current_dirty.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
            if overlap_with_dirty(&job.dirty_paths, &changed) || overlap_with_dirty(&current_dirty, &changed) {
                bail!("patch overlaps dirty parent paths; patch retained");
            }
            let common_git_dir = git_output(&repo, &["rev-parse", "--git-common-dir"]).await?;
            let common_git_dir = if Path::new(&common_git_dir).is_absolute() { PathBuf::from(common_git_dir) } else { repo.join(common_git_dir) };
            let lock_path = common_git_dir.join("shunt-apply.lock");
            let _lock_guard = acquire_apply_lock(&lock_path)?;
            let check = Command::new("git").arg("-C").arg(&repo).args(["apply", "--check"]).arg(&patch_path).output().await?;
            if !check.status.success() { bail!("git apply --check failed; patch retained"); }
            let apply = Command::new("git").arg("-C").arg(&repo).args(["apply"]).arg(&patch_path).output().await?;
            if !apply.status.success() { bail!("git apply failed; patch retained"); }
            job.applied = true;
        }
        let _ = Command::new("git").arg("-C").arg(&repo).args(["worktree", "remove", "--force"]).arg(&worktree).output().await;
        Ok(())
    }.await;

    let worktree = job_dir(&job.id).join("worktree");
    if worktree.exists() {
        let _ = Command::new("git")
            .arg("-C")
            .arg(&job.workspace)
            .args(["worktree", "remove", "--force"])
            .arg(&worktree)
            .output()
            .await;
    }

    job.updated_ms = now_ms();
    match result {
        Ok(()) => job.status = JobStatus::Completed,
        Err(error) if error.to_string() == "cancelled" => {
            job.status = JobStatus::Cancelled;
            job.error = Some("cancelled".into());
        }
        Err(error) => {
            job.status = JobStatus::Failed;
            job.error = Some(redact(error.to_string()));
        }
    }
    let _ = save_job(&job);
}

fn tool_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "task": {"type": "string"}, "workspace": {"type": "string"},
            "mode": {"type": "string", "enum": ["consult", "patch", "apply"]},
            "model": {"type": "string"}, "taskKind": {"type": "string"},
            "network": {"type": "string", "enum": ["none", "allowlisted", "unrestricted"]},
            "allowedDomains": {"type": "array", "items": {"type": "string"}},
            "timeout": {"type": "integer", "minimum": 1}
        },
        "required": ["task", "workspace", "network"]
    })
}

async fn start_tool_job(
    name: &str,
    caller: &str,
    mut input: ToolInput,
    depth: u8,
    config_path: Option<&Path>,
) -> Result<Value> {
    #[cfg(target_os = "windows")]
    bail!(
        "The worktree bridge is supported on Linux, macOS, and WSL2; native Windows is proxy-only"
    );
    let config = load_config(config_path)?;
    if !config.bridge.enabled {
        bail!("bridge is disabled");
    }
    if depth >= config.bridge.max_depth {
        bail!("bridge recursion depth exceeded");
    }
    cleanup_jobs(config.bridge.retention_hours)?;
    let provider = match name {
        "consult_codex" => "codex",
        "consult_claude" => "claude",
        "delegate_best" => match input.task_kind.as_deref() {
            Some("review" | "frontend" | "docs") => "claude",
            Some("implementation" | "debug" | "refactor") => "codex",
            _ if caller == "codex" => "claude",
            _ => "codex",
        },
        _ => bail!("unknown bridge tool {name}"),
    };
    let workspace = input.workspace.clone().context("workspace is required")?;
    if !workspace.is_absolute() {
        bail!("workspace must be an absolute path");
    }
    let network = input.network.context("network choice is required")?;
    if !config.bridge.network_ceiling.permits(network) {
        bail!("requested network policy exceeds operator ceiling");
    }
    let allowed_domains = normalize_allowed_domains(network, &input.allowed_domains)?;
    input.allowed_domains = allowed_domains.clone();
    let mode = input.mode.take().unwrap_or_else(|| {
        if name == "delegate_best" {
            "patch".into()
        } else {
            "consult".into()
        }
    });
    if !matches!(mode.as_str(), "consult" | "patch" | "apply") {
        bail!("invalid mode");
    }
    let id = uuid::Uuid::new_v4().to_string();
    let job = BridgeJob {
        id: id.clone(),
        status: JobStatus::Queued,
        provider: provider.into(),
        caller: caller.into(),
        depth,
        workspace,
        base_commit: None,
        dirty_paths: Vec::new(),
        mode,
        network,
        allowed_domains,
        created_ms: now_ms(),
        updated_ms: now_ms(),
        summary: None,
        patch_path: None,
        applied: false,
        review_provider: None,
        review_approved: false,
        error: None,
    };
    let previous = ACTIVE_JOBS.fetch_add(1, Ordering::AcqRel);
    if previous >= config.bridge.queue_capacity {
        ACTIVE_JOBS.fetch_sub(1, Ordering::AcqRel);
        bail!("bridge queue is full");
    }
    if let Err(error) = save_job(&job) {
        ACTIVE_JOBS.fetch_sub(1, Ordering::AcqRel);
        return Err(error);
    }
    let bridge = config.bridge.clone();
    let config_path = config_path.map(Path::to_path_buf);
    let id_for_result = id.clone();
    let mut handle = tokio::spawn(execute_job(job, input, bridge, config_path));
    match tokio::time::timeout(std::time::Duration::from_secs(60), &mut handle).await {
        Ok(_) => get_job(&id_for_result),
        Err(_) => Ok(
            json!({"id": id_for_result, "status": "running", "message": "job continues asynchronously; call bridge_wait"}),
        ),
    }
}

async fn call_tool(
    name: &str,
    caller: &str,
    arguments: Value,
    depth: u8,
    config_path: Option<&Path>,
) -> Result<Value> {
    if crate::manual_swarm::is_manual_tool(name) {
        return crate::manual_swarm::dispatch(name, arguments, depth, config_path).await;
    }
    if name == "bridge_wait" {
        let input: ToolInput = serde_json::from_value(arguments)?;
        let id = input.id.context("id is required")?;
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(input.timeout.unwrap_or(60).min(300));
        loop {
            let job = get_job(&id)?;
            let state = job.get("status").and_then(Value::as_str).unwrap_or("");
            if !matches!(state, "queued" | "running") || tokio::time::Instant::now() >= deadline {
                return Ok(job);
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }
    if name == "bridge_cancel" {
        let input: ToolInput = serde_json::from_value(arguments)?;
        let id = input.id.context("id is required")?;
        cancel_job(&id)?;
        return Ok(json!({"id": id, "cancel_requested": true}));
    }
    start_tool_job(
        name,
        caller,
        serde_json::from_value(arguments)?,
        depth,
        config_path,
    )
    .await
}

pub async fn dispatch_tool(
    name: &str,
    caller: &str,
    arguments: Value,
    depth: u8,
    config_path: Option<&Path>,
) -> Result<Value> {
    call_tool(name, caller, arguments, depth, config_path).await
}

async fn call_daemon(
    name: &str,
    caller: &str,
    arguments: Value,
    config_path: Option<&Path>,
) -> Result<Value> {
    let config = load_config(config_path)?;
    let url = format!(
        "http://{}:{}/bridge/tools/{}",
        config.server.host, config.server.control_port, name
    );
    let response = reqwest::Client::new()
        .post(url)
        .bearer_auth(crate::config::local_client_token("bridge")?)
        .json(&json!({
            "caller": caller,
            "arguments": arguments,
            "depth": std::env::var("SHUNT_BRIDGE_DEPTH")
                .ok()
                .and_then(|value| value.parse::<u8>().ok())
                .unwrap_or(0)
        }))
        .send()
        .await
        .context("Shunt daemon is not running; start it before using the bridge")?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("invalid bridge daemon response")?;
    if !status.is_success() {
        bail!(
            "{}",
            value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("bridge daemon error")
        );
    }
    Ok(value)
}

pub async fn serve_mcp(caller: &str, config_path: Option<&Path>) -> Result<()> {
    #[cfg(target_os = "windows")]
    bail!(
        "The worktree bridge is supported on Linux, macOS, and WSL2; native Windows is proxy-only"
    );
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        let request: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => json!({"jsonrpc":"2.0","id":id,"result":{
                "protocolVersion":"2024-11-05","capabilities":{"tools":{}},
                "serverInfo":{"name":"shunt-bridge","version":env!("CARGO_PKG_VERSION")}}}),
            "tools/list" => {
                let mut tools = vec![
                    json!({"name":"consult_codex","description":"Ask an isolated Codex worker to analyze or patch a repository.","inputSchema":tool_schema()}),
                    json!({"name":"consult_claude","description":"Ask an isolated Claude worker to analyze or patch a repository.","inputSchema":tool_schema()}),
                    json!({"name":"delegate_best","description":"Choose a deterministic opposite-provider worker for the task.","inputSchema":tool_schema()}),
                    json!({"name":"bridge_wait","description":"Wait for an asynchronous bridge job.","inputSchema":{"type":"object","properties":{"id":{"type":"string"},"timeout":{"type":"integer"}},"required":["id"]}}),
                    json!({"name":"bridge_cancel","description":"Request cancellation of a bridge job.","inputSchema":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}}),
                ];
                tools.extend(crate::manual_swarm::tool_definitions());
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools}})
            },
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let args = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                match call_daemon(name, caller, args, config_path).await {
                    Ok(value) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":serde_json::to_string_pretty(&value)?}]}})
                    }
                    Err(error) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"content":[{"type":"text","text":error.to_string()}]}})
                    }
                }
            }
            _ => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
            }
        };
        stdout
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn init_test_repo(root: &Path) -> PathBuf {
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let git = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(args)
                .status()
                .unwrap();
            assert!(status.success(), "git {:?}", args);
        };
        git(&["init", "-q"]);
        std::fs::write(repo.join("README.md"), "fixture\n").unwrap();
        git(&["add", "README.md"]);
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args([
                "-c",
                "user.name=Shunt Test",
                "-c",
                "user.email=shunt@example.test",
                "commit",
                "-qm",
                "fixture",
            ])
            .status()
            .unwrap();
        assert!(status.success());
        repo
    }

    fn make_executable(path: &Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    fn test_job(
        id: String,
        provider: &str,
        workspace: PathBuf,
        network: NetworkPolicy,
        allowed_domains: Vec<String>,
    ) -> BridgeJob {
        BridgeJob {
            id,
            status: JobStatus::Queued,
            provider: provider.into(),
            caller: "test".into(),
            depth: 0,
            workspace,
            base_commit: None,
            dirty_paths: Vec::new(),
            mode: "patch".into(),
            network,
            allowed_domains,
            created_ms: now_ms(),
            updated_ms: now_ms(),
            summary: None,
            patch_path: None,
            applied: false,
            review_provider: None,
            review_approved: false,
            error: None,
        }
    }

    #[test]
    fn allowlisted_domains_are_canonical_and_fail_closed() {
        let normalized = normalize_allowed_domains(
            NetworkPolicy::Allowlisted,
            &[
                " Example.COM ".into(),
                "*.GitHub.COM".into(),
                "example.com".into(),
                "127.0.0.1".into(),
            ],
        )
        .unwrap();
        assert_eq!(normalized, ["*.github.com", "127.0.0.1", "example.com"]);

        for invalid in [
            "",
            "*",
            "https://example.com",
            "example.com:443",
            "example.com/path",
            "*.127.0.0.1",
        ] {
            assert!(
                normalize_allowed_domains(NetworkPolicy::Allowlisted, &[invalid.into()]).is_err(),
                "accepted invalid domain {invalid:?}"
            );
        }
        assert!(normalize_allowed_domains(NetworkPolicy::Allowlisted, &[]).is_err());
        assert_eq!(
            normalize_allowed_domains(NetworkPolicy::None, &["*".into()]).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn codex_worker_config_uses_named_provider_metadata() {
        let generated = codex_client_config("127.0.0.1", 8083, Path::new("/usr/local/bin/shunt"));
        let generated: toml::Value = toml::from_str(&generated).unwrap();
        assert_eq!(
            generated["model_providers"]["shunt-codex"]["name"].as_str(),
            Some("Shunt Codex")
        );
        assert_eq!(
            generated["model_providers"]["shunt-codex"]["base_url"].as_str(),
            Some("http://127.0.0.1:8083/backend-api/codex")
        );
        assert!(generated.get("permissions").is_none());
    }

    #[test]
    fn old_bridge_jobs_default_to_an_empty_allowlist() {
        let job: BridgeJob = serde_json::from_value(json!({
            "id": "old-job",
            "status": "completed",
            "provider": "codex",
            "caller": "test",
            "workspace": "/tmp/repo",
            "base_commit": null,
            "dirty_paths": [],
            "mode": "consult",
            "network": "none",
            "created_ms": 1,
            "updated_ms": 2,
            "summary": null,
            "patch_path": null,
            "applied": false,
            "review_provider": null,
            "review_approved": false,
            "error": null
        }))
        .unwrap();
        assert!(job.allowed_domains.is_empty());
    }

    #[tokio::test]
    async fn fake_codex_job_uses_detached_worktree_and_retains_redacted_result() {
        let _env = ENV_LOCK.lock().await;
        let root = std::env::temp_dir().join(format!("shunt-bridge-test-{}", uuid::Uuid::new_v4()));
        let repo = init_test_repo(&root);
        let capture = root.join("capture");
        std::fs::create_dir_all(&capture).unwrap();

        let fake = root.join("fake-codex");
        make_executable(
            &fake,
            "#!/bin/sh\n[ \"$SHUNT_BRIDGE_DEPTH\" = 1 ] || exit 9\nprintf '%s\\n' \"$@\" > \"$SHUNT_TEST_CAPTURE_DIR/codex-args\"\ncp \"$CODEX_HOME/config.toml\" \"$SHUNT_TEST_CAPTURE_DIR/codex-config.toml\"\nprintf 'generated by isolated worker\\n' > bridge-new.txt\necho '{\"type\":\"result\",\"result\":\"fake codex ok sk-proj-sensitive\"}'\n",
        );
        std::env::set_var("SHUNT_CODEX_BIN", &fake);
        std::env::set_var("SHUNT_TEST_CAPTURE_DIR", &capture);

        let config_path = root.join("config.toml");
        std::fs::write(
            &config_path,
            crate::config::config_template(&[("main", "pro")]),
        )
        .unwrap();
        let id = format!("test-{}", uuid::Uuid::new_v4());
        let job = test_job(
            id.clone(),
            "codex",
            repo.clone(),
            NetworkPolicy::None,
            Vec::new(),
        );
        save_job(&job).unwrap();
        ACTIVE_JOBS.fetch_add(1, Ordering::AcqRel);
        execute_job(
            job,
            ToolInput {
                task: Some("inspect fixture".into()),
                workspace: Some(repo.clone()),
                mode: Some("patch".into()),
                model: None,
                task_kind: Some("review".into()),
                network: Some(NetworkPolicy::None),
                allowed_domains: Vec::new(),
                timeout: Some(30),
                id: None,
            },
            BridgeConfig::default(),
            Some(config_path),
        )
        .await;

        let completed: BridgeJob = serde_json::from_value(get_job(&id).unwrap()).unwrap();
        assert!(matches!(completed.status, JobStatus::Completed));
        assert!(completed
            .summary
            .as_deref()
            .unwrap_or("")
            .contains("fake codex ok"));
        assert!(!completed
            .summary
            .as_deref()
            .unwrap_or("")
            .contains("sensitive"));
        let patch = std::fs::read_to_string(completed.patch_path.as_ref().unwrap()).unwrap();
        assert!(patch.contains("bridge-new.txt"));
        assert!(patch.contains("generated by isolated worker"));
        assert!(!job_dir(&id).join("worktree").exists());
        let args = std::fs::read_to_string(capture.join("codex-args")).unwrap();
        assert!(args
            .lines()
            .any(|arg| arg == "--dangerously-bypass-approvals-and-sandbox"));
        let generated = std::fs::read_to_string(capture.join("codex-config.toml")).unwrap();
        let generated: toml::Value = toml::from_str(&generated).unwrap();
        assert!(generated.get("permissions").is_none());
        std::env::remove_var("SHUNT_CODEX_BIN");
        std::env::remove_var("SHUNT_TEST_CAPTURE_DIR");
        let _ = std::fs::remove_dir_all(job_dir(&id));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn fake_claude_job_uses_normalized_sandbox_settings_and_retains_patch() {
        let _env = ENV_LOCK.lock().await;
        let root =
            std::env::temp_dir().join(format!("shunt-bridge-claude-test-{}", uuid::Uuid::new_v4()));
        let repo = init_test_repo(&root);
        let capture = root.join("capture");
        std::fs::create_dir_all(&capture).unwrap();
        let fake = root.join("fake-claude");
        make_executable(
            &fake,
            "#!/bin/sh\n[ \"$SHUNT_BRIDGE_DEPTH\" = 1 ] || exit 9\n[ -n \"$ANTHROPIC_API_KEY\" ] || exit 8\nprintf '%s\\n' \"$@\" > \"$SHUNT_TEST_CAPTURE_DIR/claude-args\"\ncp \"$HOME/.claude/settings.json\" \"$SHUNT_TEST_CAPTURE_DIR/claude-settings.json\"\nprintf 'generated by isolated claude worker\\n' > bridge-claude.txt\necho '{\"type\":\"result\",\"result\":\"fake claude ok sk-ant-sensitive\"}'\n",
        );
        std::env::set_var("SHUNT_CLAUDE_BIN", &fake);
        std::env::set_var("SHUNT_TEST_CAPTURE_DIR", &capture);

        let config_path = root.join("config.toml");
        std::fs::write(
            &config_path,
            crate::config::config_template(&[("main", "pro")]),
        )
        .unwrap();
        let id = format!("test-{}", uuid::Uuid::new_v4());
        let allowed_domains = vec!["*.github.com".into(), "example.com".into()];
        let job = test_job(
            id.clone(),
            "claude",
            repo.clone(),
            NetworkPolicy::Allowlisted,
            allowed_domains.clone(),
        );
        save_job(&job).unwrap();
        ACTIVE_JOBS.fetch_add(1, Ordering::AcqRel);
        execute_job(
            job,
            ToolInput {
                task: Some("inspect fixture".into()),
                workspace: Some(repo.clone()),
                mode: Some("patch".into()),
                model: None,
                task_kind: Some("review".into()),
                network: Some(NetworkPolicy::Allowlisted),
                allowed_domains: allowed_domains.clone(),
                timeout: Some(30),
                id: None,
            },
            BridgeConfig::default(),
            Some(config_path),
        )
        .await;

        let completed: BridgeJob = serde_json::from_value(get_job(&id).unwrap()).unwrap();
        assert!(matches!(completed.status, JobStatus::Completed));
        assert_eq!(completed.allowed_domains, allowed_domains);
        assert!(completed
            .summary
            .as_deref()
            .unwrap_or("")
            .contains("fake claude ok"));
        assert!(!completed
            .summary
            .as_deref()
            .unwrap_or("")
            .contains("sensitive"));
        let patch = std::fs::read_to_string(completed.patch_path.as_ref().unwrap()).unwrap();
        assert!(patch.contains("bridge-claude.txt"));
        assert!(patch.contains("generated by isolated claude worker"));
        let args = std::fs::read_to_string(capture.join("claude-args")).unwrap();
        assert!(args.lines().any(|arg| arg == "--permission-mode"));
        assert!(args.lines().any(|arg| arg == "acceptEdits"));
        let settings: Value =
            serde_json::from_slice(&std::fs::read(capture.join("claude-settings.json")).unwrap())
                .unwrap();
        assert_eq!(
            settings.pointer("/sandbox/network/allowedDomains"),
            Some(&json!(["*.github.com", "example.com"]))
        );
        assert!(!job_dir(&id).join("worktree").exists());

        std::env::remove_var("SHUNT_CLAUDE_BIN");
        std::env::remove_var("SHUNT_TEST_CAPTURE_DIR");
        let _ = std::fs::remove_dir_all(job_dir(&id));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn invalid_network_requests_fail_before_a_job_is_queued() {
        let root =
            std::env::temp_dir().join(format!("shunt-bridge-policy-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let config_path = root.join("config.toml");
        std::fs::write(
            &config_path,
            crate::config::config_template(&[("main", "pro")]),
        )
        .unwrap();

        let empty = start_tool_job(
            "consult_codex",
            "test",
            ToolInput {
                task: Some("must not run".into()),
                workspace: Some(root.clone()),
                mode: Some("consult".into()),
                model: None,
                task_kind: None,
                network: Some(NetworkPolicy::Allowlisted),
                allowed_domains: Vec::new(),
                timeout: Some(1),
                id: None,
            },
            0,
            Some(&config_path),
        )
        .await
        .unwrap_err();
        assert_eq!(
            empty.to_string(),
            "allowlisted network requires allowedDomains"
        );

        let malformed = start_tool_job(
            "consult_codex",
            "test",
            ToolInput {
                task: Some("must not run".into()),
                workspace: Some(root.clone()),
                mode: Some("consult".into()),
                model: None,
                task_kind: None,
                network: Some(NetworkPolicy::Allowlisted),
                allowed_domains: vec!["https://example.com".into()],
                timeout: Some(1),
                id: None,
            },
            0,
            Some(&config_path),
        )
        .await
        .unwrap_err();
        assert!(malformed.to_string().contains("invalid allowed domain"));

        let ceiling = start_tool_job(
            "consult_codex",
            "test",
            ToolInput {
                task: Some("must not run".into()),
                workspace: Some(root.clone()),
                mode: Some("consult".into()),
                model: None,
                task_kind: None,
                network: Some(NetworkPolicy::Unrestricted),
                allowed_domains: Vec::new(),
                timeout: Some(1),
                id: None,
            },
            0,
            Some(&config_path),
        )
        .await
        .unwrap_err();
        assert_eq!(
            ceiling.to_string(),
            "requested network policy exceeds operator ceiling"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn nested_job_is_rejected_at_the_configured_depth_ceiling() {
        let root =
            std::env::temp_dir().join(format!("shunt-bridge-depth-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let config_path = root.join("config.toml");
        std::fs::write(
            &config_path,
            crate::config::config_template(&[("main", "pro")]),
        )
        .unwrap();

        let error = start_tool_job(
            "consult_codex",
            "codex",
            ToolInput {
                task: Some("must not run".into()),
                workspace: Some(root.clone()),
                mode: Some("consult".into()),
                model: None,
                task_kind: None,
                network: Some(NetworkPolicy::None),
                allowed_domains: Vec::new(),
                timeout: Some(1),
                id: None,
            },
            1,
            Some(&config_path),
        )
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "bridge recursion depth exceeded");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn bridge_job_ids_cannot_escape_the_job_store() {
        assert_eq!(
            get_job("../credentials").unwrap_err().to_string(),
            "invalid bridge job id"
        );
        assert_eq!(
            cancel_job("nested/job").unwrap_err().to_string(),
            "invalid bridge job id"
        );
    }

    #[test]
    fn review_approval_requires_an_exact_final_marker() {
        assert!(has_exact_review_approval(
            "review complete\nSHUNT_REVIEW_APPROVED\n"
        ));
        assert!(has_exact_review_approval(
            r#"{"type":"result","result":"review complete\nSHUNT_REVIEW_APPROVED"}"#,
        ));
        assert!(!has_exact_review_approval(
            "I saw SHUNT_REVIEW_APPROVED in the prompt, but reject."
        ));
        assert!(!has_exact_review_approval(
            r#"{"type":"result","result":"SHUNT_REVIEW_APPROVED\nbut actually reject"}"#,
        ));
    }

    #[test]
    fn apply_lock_is_exclusive_and_released_by_its_guard() {
        let root = std::env::temp_dir().join(format!("shunt-lock-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("apply.lock");
        let first = acquire_apply_lock(&path).unwrap();
        assert!(acquire_apply_lock(&path).is_err());
        drop(first);
        let second = acquire_apply_lock(&path).unwrap();
        drop(second);
        let _ = std::fs::remove_dir_all(root);
    }
}

use shunt::term::{bold_white, brand_green, cyan, dim};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let is_start = args.iter().any(|a| a == "start");
    let is_foreground = args.iter().any(|a| a == "--foreground");
    let is_daemon = args.iter().any(|a| a == "--daemon");
    let is_force = args.iter().any(|a| a == "--force");

    if is_start && !is_daemon {
        // Idempotency guard: if a healthy daemon is already serving, do NOT kill
        // and respawn it. A plain `shunt start` (e.g. from shell startup) must be
        // a no-op when the proxy is up — otherwise every new shell tears down the
        // listener for a moment and Claude Code sees ConnectionRefused mid-request.
        // Use `--force` (or `shunt restart`) to deliberately replace a running daemon.
        if !is_force && daemon_healthy() {
            print_already_running();
            return Ok(());
        }

        // Kill any existing instance BEFORE doing anything else.
        // Must be synchronous — no runtime, no async, no hangs possible.
        preflight_kill();

        if !is_foreground {
            // Daemonize by re-execing self with --_daemon (avoids fork() issues on macOS).
            // The child runs the server in the background; we print a status line and exit.
            spawn_daemon();
            // spawn_daemon() exits — we never reach here unless spawn failed.
        }
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(shunt::cli::run())
}

fn preflight_kill() {
    let pid_path = shunt::config::pid_path();
    let Ok(content) = std::fs::read_to_string(&pid_path) else { return };
    let Ok(old_pid) = content.trim().parse::<u32>() else { return };
    if old_pid == std::process::id() { return; }

    // Safety check: verify the PID actually belongs to a shunt process before
    // killing it. If the daemon died and the OS recycled its PID to something
    // else (e.g. the user's shell), we must not kill it.
    if !pid_is_shunt(old_pid) { return; }

    // SIGKILL via libc — no subprocess, instant, cannot hang
    unsafe { libc::kill(old_pid as i32, libc::SIGKILL) };
    // Give the OS 400ms to reclaim the port
    std::thread::sleep(std::time::Duration::from_millis(400));
}

/// Synchronous liveness probe for an already-running daemon. Speaks a minimal
/// HTTP/1.0 request to the control port's `/health` endpoint using blocking std
/// sockets with tight timeouts. Runs BEFORE the tokio runtime and before any
/// kill, so it must not depend on async and must never hang.
fn daemon_healthy() -> bool {
    use std::io::{Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    use std::time::Duration;

    let control_port = shunt::config::load_config(None)
        .map(|c| c.server.control_port)
        .unwrap_or(19081);

    let mut addrs = match format!("127.0.0.1:{control_port}").to_socket_addrs() {
        Ok(it) => it,
        Err(_) => return false,
    };
    let Some(addr) = addrs.next() else { return false };
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(500)) else {
        return false;
    };
    stream.set_read_timeout(Some(Duration::from_millis(800))).ok();
    stream.set_write_timeout(Some(Duration::from_millis(500))).ok();
    let req = "GET /health HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf);
    let status_ok = text.lines().next().map(|l| l.contains(" 200")).unwrap_or(false);
    // Body is {"status":"ok","version":"..."} — require both the 200 line and the ok status.
    status_ok && text.contains("\"status\"") && text.contains("ok")
}

/// Status line printed when `shunt start` finds the daemon already healthy.
fn print_already_running() {
    let addrs = load_addrs();
    println!();
    println!("  {}  {}  {}",
        brand_green("◆"),
        bold_white("shunt"),
        bold_white("already running"));
    for (provider, addr) in &addrs {
        let label = format!("{provider:<12}");
        println!("  {}  {}  {}", dim("·"), dim(&label), cyan(addr));
    }
    println!("  {}  use {} to replace it",
        dim("·"), cyan("shunt restart"));
    println!();
}

/// Returns true if the given PID is a shunt process.
/// Uses `ps` to check the command name — cross-platform enough for macOS/Linux.
fn pid_is_shunt(pid: u32) -> bool {
    let Ok(out) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "comm="])
        .output()
    else {
        return false;
    };
    let comm = String::from_utf8_lossy(&out.stdout);
    comm.trim().contains("shunt")
}

/// Re-exec self with --_daemon flag so the child runs the server.
/// Opens the log file for the child's stdout/stderr, prints a brief status
/// line to the terminal, then exits so the shell prompt returns.
fn spawn_daemon() {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Warning: cannot locate executable ({e}), starting in foreground");
            return; // fall through to foreground
        }
    };

    let log_path = shunt::config::log_path();
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let log_file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Warning: cannot open log file ({e}), starting in foreground");
            return;
        }
    };

    // Collect original args, replace "start" with "start --_daemon"
    let mut child_args: Vec<String> = std::env::args()
        .skip(1) // skip argv[0]
        .collect();
    if !child_args.iter().any(|a| a == "--daemon") {
        child_args.push("--daemon".into());
    }

    use std::os::unix::process::CommandExt;
    let log_file2 = log_file.try_clone().ok();

    let result = unsafe {
        std::process::Command::new(&exe)
            .args(&child_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(log_file))
            .stderr(std::process::Stdio::from(log_file2.unwrap_or_else(|| {
                std::fs::OpenOptions::new()
                    .create(true).append(true).open(&log_path).unwrap()
            })))
            // setsid() creates a new session: detaches from the controlling
            // terminal entirely so the daemon survives terminal close / logout.
            .pre_exec(|| {
                libc::setsid();
                Ok(())
            })
            .spawn()
    };

    match result {
        Ok(_child) => {
            let addrs = load_addrs();
            println!();
            println!("  {}  {}  {}",
                brand_green("◆"),
                bold_white("shunt"),
                bold_white("started"));
            for (provider, addr) in &addrs {
                let label = format!("{provider:<12}");
                println!("  {}  {}  {}", dim("·"), dim(&label), cyan(addr));
            }
            println!("  {}  run {} for account details",
                dim("·"), cyan("shunt status"));
            println!();
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("Warning: could not daemonize ({e}), starting in foreground");
            // fall through — tokio will start and run normally
        }
    }
}

/// Returns `(provider_label, url)` for each provider found in the config.
/// Falls back to just the Anthropic default if the config can't be loaded.
fn load_addrs() -> Vec<(String, String)> {
    use shunt::provider::Provider;

    let Ok(cfg) = shunt::config::load_config(None) else {
        return vec![("anthropic".into(), "http://127.0.0.1:8082".into())];
    };

    let host = &cfg.server.host;
    let mut out = Vec::new();
    if cfg.accounts.iter().any(|a| matches!(a.provider, Provider::Anthropic | Provider::AnthropicApi)) {
        out.push(("claude".into(), format!("http://{host}:{}", cfg.pools.claude.port)));
    }
    if cfg.accounts.iter().any(|a| matches!(a.provider, Provider::OpenAI | Provider::OpenAIApi)) {
        out.push(("codex".into(), format!("http://{host}:{}", cfg.pools.codex.port)));
    }
    use std::collections::BTreeSet;
    let providers: BTreeSet<String> = cfg.accounts.iter().filter(|a| !matches!(a.provider,
        Provider::Anthropic | Provider::AnthropicApi | Provider::OpenAI | Provider::OpenAIApi))
        .map(|a| a.provider.to_string()).collect();
    out.extend(providers.into_iter().map(|p| {
        let port = Provider::from_str(&p).default_port();
        (p, format!("http://{host}:{port}"))
    }));
    out
}

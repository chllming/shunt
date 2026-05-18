/// Fire a macOS system notification. No-op on other platforms.
/// `sound` is a macOS alert sound name ("Basso", "Ping", "Glass", etc.)
pub fn notify(title: &str, body: &str, sound: &str) {
    #[cfg(target_os = "macos")]
    {
        let script = format!(
            "display notification {body:?} with title {title:?} sound name {sound:?}"
        );
        let _ = std::process::Command::new("osascript")
            .args(["-e", &script])
            .status();
    }
    #[cfg(not(target_os = "macos"))]
    let _ = (title, body, sound);
}

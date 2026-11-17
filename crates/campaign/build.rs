//! Embeds the git commit into `code_revision()`; falls back to "unknown".
fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads");
    println!("cargo:rerun-if-env-changed=PHOENIX_GIT_SHA");
    let sha = std::env::var("PHOENIX_GIT_SHA").ok().or_else(|| {
        let o = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()?;
        o.status
            .success()
            .then(|| String::from_utf8_lossy(&o.stdout).trim().to_owned())
    });
    if let Some(s) = sha.filter(|s| !s.is_empty()) {
        println!("cargo:rustc-env=PHOENIX_GIT_SHA={s}");
    }
}

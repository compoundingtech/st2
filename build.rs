//! Bake a LocalStamp for `st2 --version` on a plain `cargo build`, per the shared
//! build-versioning contract. Emitted as `ST2_BUILD_STAMP_LOCAL` — a private var
//! distinct from the fleet's `CLI_BUILD_STAMP`, so the flake's authoritative
//! NixStamp can never be overridden by this (see src/version.rs). A hermetic Nix
//! build has no `.git`, so this yields nothing there and the NixStamp is used.
//!
//! A full revision and commit timestamp are also exposed privately to the
//! receipt-bearing lifecycle-hook installer. They do not affect the shared
//! human/machine version contract.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

fn main() {
    if let Some(rev) = git(&["rev-parse", "--short", "HEAD"]) {
        let dirty = git(&["status", "--porcelain"]).is_some_and(|s| !s.is_empty());
        let commit_ts = git(&["log", "-1", "--format=%ct"])
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        // Hand-assembled JSON: the short-sha is hex so no escaping is needed, and
        // this avoids a build-dependency just to serialize three fields.
        let stamp =
            format!(r#"{{"type":"local","rev":"{rev}","commitTs":{commit_ts},"dirty":{dirty}}}"#);
        println!("cargo:rustc-env=ST2_BUILD_STAMP_LOCAL={stamp}");
        println!(
            "cargo:rustc-env=ST2_GIT_SHA_FULL={}",
            git(&["rev-parse", "HEAD"]).unwrap_or(rev)
        );
        println!("cargo:rustc-env=ST2_GIT_COMMIT_UNIX={commit_ts}");
    } else {
        println!("cargo:rustc-env=ST2_GIT_SHA_FULL=unknown");
        println!("cargo:rustc-env=ST2_GIT_COMMIT_UNIX=0");
    }
    // Rebuild the stamp when HEAD moves or the working tree changes (dirty flag).
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
    println!("cargo:rerun-if-changed=.git/index");
}

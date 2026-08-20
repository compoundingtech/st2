//! Controlled pi launch with a session-owned presence lease.
//!
//! pi has no MCP and no app-server: its integration point is an extension loaded into the
//! interactive process, and that extension reaches st2 by spawning `st2 driver pi-channel`. Two
//! facts have to be handed to it, and neither is discoverable from inside pi. The first is *which*
//! st2 to run — resolving `st2` from `PATH` would let a replaced control plane and its live agents
//! disagree, which the R11 control-plane replacement guarantee exists to prevent — so the wrapper
//! exports its own executable path. The second is the catalog and identity the channel must bind.
//!
//! The wrapper also owns presence for the same reason the Claude wrapper does: the extension lives
//! only as long as pi's process, and a measured SIGKILL of pi produces no terminal record at all
//! (`docs/vrs/.experiments/2026-08-18-pi-harness-integration.md`). Presence therefore decays by
//! staleness, exactly as for the other harnesses.

use std::path::Path;

use anyhow::{Context as _, Result};

use crate::provider_session::{PROVIDER_POLL, STOP, install_signal_handler, run_provider};
use crate::{hooks, message, status};

/// The extension file inside this binary's immutable hook set.
const EXTENSION: &str = "pi-channel.ts";

/// The exact st2 executable the pi extension must spawn for its channel.
pub const CHANNEL_BIN: &str = "ST2_PI_CHANNEL_BIN";
/// The catalog root that executable must be pointed at.
pub const CHANNEL_CATALOG: &str = "ST2_PI_CHANNEL_CATALOG";
/// The host-qualified bus identity the channel binds.
pub const CHANNEL_IDENTITY: &str = "ST2_PI_CHANNEL_IDENTITY";

/// pi's startup network work, which a supervised seat should not be doing.
///
/// A managed agent that update-checks or self-updates at boot makes its own launch latency and its
/// own behaviour depend on the network, and lets a release change a running fleet. Each is applied
/// only when the operator has not already set it, so a declaration's `env` still wins.
const OFFLINE_DEFAULTS: [(&str, &str); 2] = [("PI_OFFLINE", "1"), ("PI_SKIP_VERSION_CHECK", "1")];

/// Run one interactive pi provider and maintain its presence until it exits.
pub fn run(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    pi_argv: Vec<String>,
) -> Result<()> {
    let agent_dir =
        message::resolve_agent_dir(catalog_root, &identity, &crate::run::detect_host())?
            .with_context(|| format!("pi driver agent '{identity}' is not declared"))?;
    anyhow::ensure!(
        !pi_argv.is_empty(),
        "pi driver '{runtime_id}' has no provider argv"
    );
    let executable =
        std::env::current_exe().context("resolving st2 executable for the pi channel")?;
    let mut env = channel_env(&executable, catalog_root, &identity)?;
    env.extend(offline_defaults(|key| std::env::var_os(key).is_some()));
    let set = hooks::verify_required_set().with_context(|| {
        format!(
            "pi driver '{runtime_id}' needs this binary's verified hook set for {EXTENSION}; run `st2 hooks install`"
        )
    })?;
    let pi_argv = with_channel_extension(pi_argv, &set)?;
    install_signal_handler();
    run_provider(
        "pi",
        &status::status_path(&agent_dir),
        &pi_argv,
        &env,
        status::STATUS_REFRESH,
        PROVIDER_POLL,
        &STOP,
    )
    .with_context(|| format!("running pi driver '{runtime_id}'"))
}

/// Load the channel extension from the verified set, immediately after the provider program.
///
/// The declaration deliberately carries no path to it: a rendered machine-local path would pin one
/// host's layout into a catalog, and a `$ST_HOOKS` token in an argv would resolve to the
/// receipt-bearing root rather than the selected set. Resolving it here means a launch uses the
/// exact asset this binary was built with.
fn with_channel_extension(mut argv: Vec<String>, set: &Path) -> Result<Vec<String>> {
    let extension = set.join(EXTENSION);
    let extension = extension
        .to_str()
        .context("verified hook set path is not UTF-8")?
        .to_owned();
    argv.splice(1..1, ["-e".to_string(), extension]);
    Ok(argv)
}

/// The offline defaults this launch should add, skipping any the operator already declared.
fn offline_defaults(is_set: impl Fn(&str) -> bool) -> Vec<(String, String)> {
    OFFLINE_DEFAULTS
        .iter()
        .filter(|(key, _)| !is_set(key))
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

/// The environment the shipped pi extension reads to reach this exact control plane.
fn channel_env(
    executable: &Path,
    catalog_root: &Path,
    identity: &str,
) -> Result<Vec<(String, String)>> {
    let executable = executable
        .to_str()
        .context("st2 executable path is not UTF-8")?;
    let catalog_root = catalog_root.to_str().context("catalog root is not UTF-8")?;
    Ok(vec![
        (CHANNEL_BIN.to_string(), executable.to_string()),
        (CHANNEL_CATALOG.to_string(), catalog_root.to_string()),
        (CHANNEL_IDENTITY.to_string(), identity.to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::*;

    #[test]
    fn idle_pi_provider_refreshes_presence_without_channel_input() {
        let tmp = tempfile::tempdir().unwrap();
        let presence = status::status_path(tmp.path());
        status::set_state(&presence, status::State::Available).unwrap();
        let before = fs::read_to_string(&presence).unwrap();
        let stop = AtomicBool::new(false);

        run_provider(
            "pi",
            &presence,
            // Deliberately cheaper than the Claude twin: this binary already runs one
            // spawn-and-poll presence test, and the codex app-server tests beside it are
            // timing-sensitive. One refresh inside the child's life is all this needs to prove.
            &["sh".into(), "-c".into(), "sleep 0.06".into()],
            &[],
            Duration::from_millis(10),
            Duration::from_millis(5),
            &stop,
        )
        .unwrap();

        let after = fs::read_to_string(&presence).unwrap();
        assert_ne!(after, before);
        assert_eq!(status::read_state(&presence), status::State::Available);
    }

    #[test]
    fn the_channel_extension_is_injected_from_the_verified_set_not_the_declaration() {
        let argv = with_channel_extension(
            vec![
                "pi".into(),
                "-a".into(),
                "--model".into(),
                "anthropic/opus".into(),
                "Start work.".into(),
            ],
            &PathBuf::from("/state/st2/hooks/sets/sha256-abc"),
        )
        .unwrap();

        assert_eq!(
            argv,
            vec![
                "pi",
                "-e",
                "/state/st2/hooks/sets/sha256-abc/pi-channel.ts",
                "-a",
                "--model",
                "anthropic/opus",
                "Start work.",
            ]
        );
    }

    /// A supervised seat is offline by default, but an operator who declared otherwise keeps their
    /// value — otherwise the wrapper would silently overrule the declaration.
    #[test]
    fn offline_defaults_apply_only_where_the_operator_declared_nothing() {
        assert_eq!(
            offline_defaults(|_| false),
            vec![
                ("PI_OFFLINE".to_string(), "1".to_string()),
                ("PI_SKIP_VERSION_CHECK".to_string(), "1".to_string()),
            ]
        );
        assert_eq!(
            offline_defaults(|key| key == "PI_OFFLINE"),
            vec![("PI_SKIP_VERSION_CHECK".to_string(), "1".to_string())]
        );
        assert!(offline_defaults(|_| true).is_empty());
    }

    #[test]
    fn the_extension_receives_this_binary_not_a_path_lookup() {
        let env = channel_env(
            &PathBuf::from("/opt/st2/bin/st2"),
            &PathBuf::from("/catalog"),
            "host.worker",
        )
        .unwrap();

        assert_eq!(
            env,
            vec![
                (CHANNEL_BIN.to_string(), "/opt/st2/bin/st2".to_string()),
                (CHANNEL_CATALOG.to_string(), "/catalog".to_string()),
                (CHANNEL_IDENTITY.to_string(), "host.worker".to_string()),
            ]
        );
    }
}

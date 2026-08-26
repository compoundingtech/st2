//! Controlled omp launch with a session-owned presence lease.
//!
//! omp is pi-family: its integration point is a pi-style extension loaded into the interactive
//! process, which reaches st2 by spawning `st2 driver omp-channel` (`hooks/omp-channel.ts`,
//! forked from the pi channel — see `docs/vrs/06-omp-driver/spec.md` for the measured
//! divergences). The wrapper owns presence for the same reason the pi wrapper does: the extension
//! lives only as long as omp's process, and a SIGKILL of omp produces no terminal record at all,
//! so presence decays by staleness exactly as for the other harnesses.
//!
//! Unlike pi, the wrapper hard-gates the provider version (OMP-R05): the delivery-critical
//! surface — event names, the sampled idle edge, the approval events — is versioned behavior, not
//! an API contract, so an unverified minor stays refused until the admission checks are repeated.

use std::path::Path;
use std::process::ExitStatus;

use anyhow::{Context as _, Result};

use crate::provider_session::{
    PROVIDER_POLL, ProviderOutcome, STOP, install_signal_handler, run_provider_observed,
};
use crate::{harness_state, hooks, message, status};

/// The extension file inside this binary's immutable hook set.
const EXTENSION: &str = "omp-channel.ts";

/// The exact st2 executable the omp extension must spawn for its channel.
pub const CHANNEL_BIN: &str = "ST2_OMP_CHANNEL_BIN";
/// The catalog root that executable must be pointed at.
pub const CHANNEL_CATALOG: &str = "ST2_OMP_CHANNEL_CATALOG";
/// The host-qualified bus identity the channel binds.
pub const CHANNEL_IDENTITY: &str = "ST2_OMP_CHANNEL_IDENTITY";
/// The wrapper's runtime/task ID — the pty session whose liveness vouches for observed state.
pub const CHANNEL_RUNTIME_ID: &str = "ST2_OMP_CHANNEL_RUNTIME_ID";
/// The session incarnation token the wrapper mints. The channel adopts it so the wrapper's
/// terminal record owns — and thereby fences — the live records the channel writes.
pub const CHANNEL_SESSION: &str = "ST2_OMP_CHANNEL_SESSION";
/// The ownership sequence the wrapper claimed at startup.
pub const CHANNEL_SEQ: &str = "ST2_OMP_CHANNEL_SEQ";

/// omp reads its pi ancestor's env fallbacks, so the same offline defaults apply. Whether they
/// suppress the update banner in interactive boots is still open (DQ-OMP-5); shipping them is
/// harmless either way.
const OFFLINE_DEFAULTS: [(&str, &str); 2] = [("PI_OFFLINE", "1"), ("PI_SKIP_VERSION_CHECK", "1")];

/// The versions verified against the admission checks in
/// `docs/vrs/06-omp-driver/spec.md` (2026-08-25). omp releases near-daily and its
/// delivery-critical surface is versioned behavior, so admission is per exact version: a later
/// minor OR patch stays rejected until the checks are repeated against it.
const SUPPORTED_OMP_VERSIONS: [&str; 1] = ["18.0.3"];

/// What the wrapper hands the provider process: the channel environment plus the launch argv with
/// the channel extension spliced in.
type PreparedLaunch = (Vec<(String, String)>, Vec<String>);

/// Run one interactive omp provider and maintain its presence until it exits.
pub fn run(
    catalog_root: &Path,
    identity: String,
    runtime_id: String,
    omp_argv: Vec<String>,
) -> Result<()> {
    let agent_dir =
        message::resolve_agent_dir(catalog_root, &identity, &crate::run::detect_host())?
            .with_context(|| format!("omp driver agent '{identity}' is not declared"))?;
    anyhow::ensure!(
        !omp_argv.is_empty(),
        "omp driver '{runtime_id}' has no provider argv"
    );
    verify_supported_version(&omp_argv[0])?;
    let executable =
        std::env::current_exe().context("resolving st2 executable for the omp channel")?;
    let session = harness_state::session_token();
    // The claim is written: it supersedes whatever the predecessor left — including a
    // still-fresh live record — before the channel or terminal writer act under it.
    let seq = harness_state::claim(&agent_dir, identity.clone(), "omp", &session)?;
    // Every fallible step past the claim must end the record honestly on failure — the claim
    // placeholder standing as the last word would read as a takeover, not a launch that never
    // ran.
    let prepared = (|| -> Result<PreparedLaunch> {
        let mut env = channel_env(
            &executable,
            catalog_root,
            &identity,
            &runtime_id,
            &session,
            seq,
        )?;
        env.extend(offline_defaults(|key| std::env::var_os(key).is_some()));
        let set = hooks::verify_required_set().with_context(|| {
            format!(
                "omp driver '{runtime_id}' needs this binary's verified hook set for {EXTENSION}; run `st2 hooks install`"
            )
        })?;
        Ok((env, with_channel_extension(omp_argv, &set)?))
    })();
    let (env, omp_argv) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            let mut writer = harness_state::Writer::new(
                &agent_dir,
                identity.clone(),
                "omp",
                Some(runtime_id.clone()),
            )
            .with_ownership(session.clone(), seq);
            let _ = writer.observe(
                harness_state::Observation::new(
                    harness_state::Activity::Ended,
                    harness_state::BlockedOn::None,
                    harness_state::InputBuffer::Unknown,
                )
                .with_reason("launch-error")
                .with_exit("exit unknown"),
            );
            return Err(error);
        }
    };
    install_signal_handler();
    // Terminal-only: the channel owns the live record and its heartbeat, but only this wrapper
    // survives long enough to see the stop path. Same token as the channel, so the terminal
    // record fences exactly this session's live records.
    let observer = crate::provider_session::SessionObserver::terminal_only(
        &agent_dir,
        &identity,
        "omp",
        &runtime_id,
        &session,
        seq,
    );
    let outcome = run_provider_observed(
        "omp",
        &status::status_path(&agent_dir),
        &omp_argv,
        &env,
        status::STATUS_REFRESH,
        PROVIDER_POLL,
        &STOP,
        Some(&observer),
    )
    .with_context(|| format!("running omp driver '{runtime_id}'"))?;
    record_session_end(&agent_dir, &identity, &runtime_id, &session, seq, &outcome);
    match outcome {
        ProviderOutcome::Exited(exit) => {
            anyhow::ensure!(exit.success(), "omp provider exited with {exit}");
            Ok(())
        }
        ProviderOutcome::Stopped(_) => Ok(()),
    }
}

/// Refuse any provider whose major version this binary was not verified against. Failing loudly
/// at launch is the point (OMP-R05): a silently degraded observed state or delivery path would
/// read as healthy.
fn verify_supported_version(binary: &str) -> Result<()> {
    let output = std::process::Command::new(binary)
        .arg("--version")
        .output()
        .with_context(|| format!("running {binary} --version for the omp version gate"))?;
    anyhow::ensure!(output.status.success(), "{binary} --version failed");
    // omp prefixes its banner ("omp/18.0.3"), so scan every whitespace-separated token for the
    // first dotted-numeric version rather than trusting line order.
    let printed = String::from_utf8_lossy(&output.stdout);
    let version = printed
        .split_whitespace()
        .map(|token| token.trim_start_matches("omp/").trim_start_matches('v'))
        .find(|token| {
            token.split('.').all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
        });
    let version = version.with_context(|| format!("{{binary}} --version printed no version: '{printed}'"))?;
    anyhow::ensure!(
        SUPPORTED_OMP_VERSIONS.contains(&version),
        "omp {version} is unverified (admitted: {}); repeat the docs/vrs/06-omp-driver \
         admission checks before extending the gate",
        SUPPORTED_OMP_VERSIONS.join(", ")
    );
    Ok(())
}

/// The wrapper's one write into observed harness state: the terminal record. Live states and
/// heartbeats belong to the omp channel; the wrapper sees exactly one fact the channel cannot —
/// that the provider process is gone.
fn record_session_end(
    agent_dir: &Path,
    identity: &str,
    runtime_id: &str,
    session: &str,
    seq: u64,
    outcome: &ProviderOutcome,
) {
    let label = match outcome {
        ProviderOutcome::Exited(exit) | ProviderOutcome::Stopped(Some(exit)) => exit_label(*exit),
        ProviderOutcome::Stopped(None) => "stopped".to_string(),
    };
    let mut writer =
        harness_state::Writer::new(agent_dir, identity, "omp", Some(runtime_id.to_string()))
            .with_ownership(session, seq);
    if let Err(error) = writer.ended(label) {
        eprintln!("st2 omp driver: recording session end failed: {error}");
    }
}

fn exit_label(exit: ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt as _;
    match (exit.code(), exit.signal()) {
        (Some(code), _) => format!("exit {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "exited".to_string(),
    }
}

/// Load the channel extension from the verified set, immediately after the provider program.
///
/// Resolving it here means a launch uses the exact asset this binary was built with; a rendered
/// machine-local path in a declaration would pin one host's layout into a catalog.
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

/// The environment the shipped omp extension reads to reach this exact control plane.
///
/// Fresh variable names: an omp seat must never adopt a stray pi channel configuration.
fn channel_env(
    executable: &Path,
    catalog_root: &Path,
    identity: &str,
    runtime_id: &str,
    session: &str,
    seq: u64,
) -> Result<Vec<(String, String)>> {
    let executable = executable
        .to_str()
        .context("st2 executable path is not UTF-8")?;
    let catalog_root = catalog_root.to_str().context("catalog root is not UTF-8")?;
    Ok(vec![
        (CHANNEL_BIN.to_string(), executable.to_string()),
        (CHANNEL_CATALOG.to_string(), catalog_root.to_string()),
        (CHANNEL_IDENTITY.to_string(), identity.to_string()),
        (CHANNEL_RUNTIME_ID.to_string(), runtime_id.to_string()),
        (CHANNEL_SESSION.to_string(), session.to_string()),
        (CHANNEL_SEQ.to_string(), seq.to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn version_gate_admits_the_verified_major() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("omp");
        std::fs::write(&fake, "#!/bin/sh\nprintf 'omp v18.0.3\\n18.0.3\\n'\n").unwrap();
        // Keep the file writable so dropping the shebang bit back is unnecessary; make it
        // executable in place.
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(verify_supported_version(fake.to_str().unwrap()).is_ok());
    }

    #[test]
    fn version_gate_refuses_an_unverified_minor() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("omp");
        std::fs::write(&fake, "#!/bin/sh\nprintf '18.1.0\\n'\n").unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = verify_supported_version(fake.to_str().unwrap()).unwrap_err();
        assert!(error.to_string().contains("unverified"), "{error}");
    }

    #[test]
    fn version_gate_refuses_an_unverified_major() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("omp");
        std::fs::write(&fake, "#!/bin/sh\nprintf '19.0.1\\n'\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = verify_supported_version(fake.to_str().unwrap()).unwrap_err();
        assert!(error.to_string().contains("unverified"), "{error}");
    }

    #[test]
    fn version_gate_refuses_garbled_output() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("omp");
        std::fs::write(&fake, "#!/bin/sh\nprintf 'not-a-version\\n'\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(verify_supported_version(fake.to_str().unwrap()).is_err());
    }

    #[test]
    fn offline_defaults_skip_operator_declared_keys() {
        let defaults = offline_defaults(|key| key == "PI_OFFLINE");
        assert_eq!(
            defaults,
            vec![("PI_SKIP_VERSION_CHECK".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn channel_extension_is_spliced_right_after_the_program() {
        let dir = tempfile::tempdir().unwrap();
        let argv =
            with_channel_extension(vec!["omp".into(), "--model".into(), "x".into()], dir.path())
                .unwrap();
        assert_eq!(argv[0], "omp");
        assert_eq!(argv[1], "-e");
        assert!(argv[2].ends_with("omp-channel.ts"));
        assert_eq!(argv[3], "--model");
    }
}

//! Install-owned lifecycle hooks shipped with st2.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

const CODEX_SESSION_START: &[u8] = include_bytes!("../hooks/codex-session-start.sh");
const CODEX_PRE_COMPACT: &[u8] = include_bytes!("../hooks/codex-pre-compact.sh");
const CODEX_STOP: &[u8] = include_bytes!("../hooks/codex-stop.sh");
const CLAUDE_SESSION_START: &[u8] = include_bytes!("../hooks/claude-session-start.sh");
const CLAUDE_PRE_COMPACT: &[u8] = include_bytes!("../hooks/claude-pre-compact.sh");
const CLAUDE_STOP_FAILURE: &[u8] = include_bytes!("../hooks/claude-stop-failure.sh");

const HOOKS: [(&str, &[u8]); 6] = [
    ("codex-session-start.sh", CODEX_SESSION_START),
    ("codex-pre-compact.sh", CODEX_PRE_COMPACT),
    ("codex-stop.sh", CODEX_STOP),
    ("claude-session-start.sh", CLAUDE_SESSION_START),
    ("claude-pre-compact.sh", CLAUDE_PRE_COMPACT),
    ("claude-stop-failure.sh", CLAUDE_STOP_FAILURE),
];

/// Install-owned hook directory. `$ST_HOOKS` can pin a custom state layout; otherwise use
/// `$XDG_STATE_HOME/st2/hooks` or `~/.local/state/st2/hooks`.
pub fn hooks_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ST_HOOKS").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    let state = match std::env::var_os("XDG_STATE_HOME").filter(|value| !value.is_empty()) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(
            std::env::var_os("HOME").context("HOME is not set and XDG_STATE_HOME is unset")?,
        )
        .join(".local/state"),
    };
    Ok(state.join("st2/hooks"))
}

fn install_into(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    for (name, bytes) in HOOKS {
        let path = dir.join(name);
        let unchanged = fs::read(&path).is_ok_and(|existing| existing == bytes);
        if !unchanged {
            fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&path)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions)?;
        }
    }
    Ok(())
}

/// Install or refresh the hook scripts, idempotently.
pub fn ensure_installed() -> Result<PathBuf> {
    let dir = hooks_dir()?;
    install_into(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installation_is_idempotent_and_executable() {
        let tmp = tempfile::tempdir().unwrap();
        install_into(tmp.path()).unwrap();
        install_into(tmp.path()).unwrap();
        for (name, expected) in HOOKS {
            let path = tmp.path().join(name);
            assert_eq!(fs::read(&path).unwrap(), expected);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_ne!(fs::metadata(path).unwrap().permissions().mode() & 0o111, 0);
            }
        }
    }
}

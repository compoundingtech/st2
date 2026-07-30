//! One catalog-authoring lock shared by declaration publishers and snapshot readers.
//!
//! The lock file is persistent by design. Removing it would split the lock domain when an existing
//! process still has the old inode open. State-plane traffic (messages, context, resources, status)
//! deliberately does not use this lock.

use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub const CONTROL_DIR: &str = ".st2";
pub const LOCK_FILE: &str = "catalog-authoring.lock";
pub const APPLY_MARKER: &str = "catalog-apply-incomplete";

#[derive(Debug, Clone, Copy)]
enum Mode {
    Shared,
    Exclusive,
}

/// An advisory catalog-authoring lock. Dropping the guard releases the kernel lock.
#[derive(Debug)]
pub struct CatalogLock {
    file: File,
}

impl CatalogLock {
    pub fn shared(catalog: &Path) -> Result<Self> {
        Self::acquire(catalog, Mode::Shared)
    }

    pub fn exclusive(catalog: &Path) -> Result<Self> {
        Self::acquire(catalog, Mode::Exclusive)
    }

    fn acquire(catalog: &Path, mode: Mode) -> Result<Self> {
        let catalog = catalog
            .canonicalize()
            .with_context(|| format!("canonicalize catalog root {}", catalog.display()))?;
        let metadata = fs::symlink_metadata(&catalog)
            .with_context(|| format!("read catalog root {}", catalog.display()))?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "catalog root is not a real directory: {}",
            catalog.display()
        );

        let control = catalog.join(CONTROL_DIR);
        match fs::symlink_metadata(&control) {
            Ok(metadata) => anyhow::ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "catalog control path is not a real directory: {}",
                control.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&control) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&control).with_context(|| {
                            format!("re-read catalog control dir {}", control.display())
                        })?;
                        anyhow::ensure!(
                            metadata.is_dir() && !metadata.file_type().is_symlink(),
                            "catalog control path is not a real directory: {}",
                            control.display()
                        );
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("create catalog control dir {}", control.display())
                        });
                    }
                }
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read catalog control dir {}", control.display()));
            }
        }

        let path = control.join(LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
            .with_context(|| format!("open catalog authoring lock {}", path.display()))?;
        let operation = match mode {
            Mode::Shared => libc::LOCK_SH,
            Mode::Exclusive => libc::LOCK_EX,
        };
        // SAFETY: `file` owns a valid descriptor for the duration of this call and the returned
        // guard. flock does not access Rust memory.
        let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lock catalog authoring lock {}", path.display()));
        }
        let marker = control.join(APPLY_MARKER);
        match fs::symlink_metadata(&marker) {
            Ok(_) => anyhow::bail!(
                "catalog apply is incomplete: marker present at {}",
                marker.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect catalog apply marker {}", marker.display()));
            }
        }
        Ok(Self { file })
    }
}

impl Drop for CatalogLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains valid until after Drop returns.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

pub fn lock_path(catalog: &Path) -> PathBuf {
    catalog.join(CONTROL_DIR).join(LOCK_FILE)
}

pub fn apply_marker_path(catalog: &Path) -> PathBuf {
    catalog.join(CONTROL_DIR).join(APPLY_MARKER)
}

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
    mode: Mode,
    catalog: PathBuf,
}

impl CatalogLock {
    pub fn shared(catalog: &Path) -> Result<Self> {
        Self::acquire(catalog, Mode::Shared, false)
    }

    pub fn exclusive(catalog: &Path) -> Result<Self> {
        Self::acquire(catalog, Mode::Exclusive, false)
    }

    /// The whole-catalog transaction is the only operation allowed to inspect and recover an
    /// incomplete apply. Every other declaration reader/writer must keep using `shared` or
    /// `exclusive`, which fail closed while the marker exists.
    pub(crate) fn exclusive_for_catalog_apply(catalog: &Path) -> Result<Self> {
        Self::acquire(catalog, Mode::Exclusive, true)
    }

    fn acquire(catalog: &Path, mode: Mode, allow_incomplete_apply: bool) -> Result<Self> {
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
        let control_branch = match fs::symlink_metadata(&control) {
            Ok(metadata) => {
                anyhow::ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "catalog control path is not a real directory: {}",
                    control.display()
                );
                "observed"
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                test_control_creation_checkpoint();
                let branch = match fs::create_dir(&control) {
                    Ok(()) => {
                        test_control_created_checkpoint();
                        "created"
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&control).with_context(|| {
                            format!("re-read catalog control dir {}", control.display())
                        })?;
                        anyhow::ensure!(
                            metadata.is_dir() && !metadata.file_type().is_symlink(),
                            "catalog control path is not a real directory: {}",
                            control.display()
                        );
                        "raced"
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("create catalog control dir {}", control.display())
                        });
                    }
                };
                branch
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read catalog control dir {}", control.display()));
            }
        };
        // The control directory is the durability root for the persistent lock, apply stage, and
        // incomplete marker. Persist its catalog-parent entry before any caller can publish
        // declaration leaves. This is unconditional after observing a real control dir: its creator
        // may have crashed after mkdir but before its own parent fsync.
        File::open(&catalog)
            .with_context(|| format!("open catalog root {}", catalog.display()))?
            .sync_all()
            .with_context(|| format!("sync catalog root {}", catalog.display()))?;
        #[cfg(debug_assertions)]
        if let Ok(path) = std::env::var("ST2_TEST_CATALOG_CONTROL_BRANCH") {
            let _ = fs::write(path, control_branch);
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
        #[cfg(debug_assertions)]
        if matches!(mode, Mode::Exclusive)
            && let Ok(path) = std::env::var("ST2_TEST_CATALOG_LOCK_ATTEMPT")
        {
            let _ = fs::write(path, b"exclusive");
        }
        // SAFETY: `file` owns a valid descriptor for the duration of this call and the returned
        // guard. flock does not access Rust memory.
        let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("lock catalog authoring lock {}", path.display()));
        }
        if !allow_incomplete_apply {
            let marker = control.join(APPLY_MARKER);
            match fs::symlink_metadata(&marker) {
                Ok(_) => anyhow::bail!(
                    "catalog apply is incomplete: marker present at {}",
                    marker.display()
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("inspect catalog apply marker {}", marker.display())
                    });
                }
            }
        }
        Ok(Self {
            file,
            mode,
            catalog,
        })
    }

    pub(crate) fn is_exclusive_for(&self, catalog: &Path) -> bool {
        matches!(self.mode, Mode::Exclusive) && self.catalog == catalog
    }
}

#[cfg(debug_assertions)]
fn test_control_creation_checkpoint() {
    use std::time::{Duration, Instant};

    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_CATALOG_CONTROL_READY"),
        std::env::var("ST2_TEST_CATALOG_CONTROL_RELEASE"),
    ) else {
        return;
    };
    let _ = fs::write(ready, b"ready");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !Path::new(&release).exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(not(debug_assertions))]
fn test_control_creation_checkpoint() {}

#[cfg(debug_assertions)]
fn test_control_created_checkpoint() {
    use std::time::{Duration, Instant};

    let (Ok(ready), Ok(release)) = (
        std::env::var("ST2_TEST_CATALOG_CONTROL_CREATED_READY"),
        std::env::var("ST2_TEST_CATALOG_CONTROL_CREATED_RELEASE"),
    ) else {
        return;
    };
    let _ = fs::write(ready, b"ready");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !Path::new(&release).exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(not(debug_assertions))]
fn test_control_created_checkpoint() {}

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

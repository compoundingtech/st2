//! Durable, change-driven writers for derived catalog state projections.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteOutcome {
    Changed,
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectorySync {
    CreatedEntry,
    PublishedFile,
}

/// Publish exact bytes through a same-directory temporary file and rename.
///
/// The target is replaced only when its bytes differ. A successful replacement fsyncs both the
/// temporary file and containing directory, so readers see either the old complete projection or
/// the new complete projection after a crash. State projections never participate in catalog
/// declaration locking or generation.
pub(crate) fn write_atomic_if_changed(path: &Path, bytes: &[u8]) -> Result<WriteOutcome> {
    let directory = path.parent().context("state projection has no parent")?;
    let created_directory = ensure_durable_directory(directory)?;

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "state projection target is not a real regular file: {}",
                path.display()
            );
            if fs::read(path)
                .with_context(|| format!("read state projection {}", path.display()))?
                == bytes
            {
                // A prior call may have renamed these bytes successfully but failed its durability
                // barrier. Repeating the barrier is mutation-free and makes that failure retryable.
                sync_directory(directory, DirectorySync::PublishedFile)?;
                return Ok(WriteOutcome::Unchanged);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if !created_directory {
                // The directory may be left behind by a prior mkdir whose parent sync failed.
                let parent = directory
                    .parent()
                    .context("state projection directory has no parent")?;
                sync_directory(parent, DirectorySync::CreatedEntry)?;
            }
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect state projection {}", path.display()));
        }
    }

    let mut temporary = tempfile::Builder::new()
        .prefix(".state-projection-")
        .tempfile_in(directory)
        .with_context(|| {
            format!(
                "create state projection temporary in {}",
                directory.display()
            )
        })?;
    temporary
        .write_all(bytes)
        .with_context(|| format!("write state projection temporary for {}", path.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("sync state projection temporary for {}", path.display()))?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("publish state projection {}", path.display()))?;
    sync_directory(directory, DirectorySync::PublishedFile)?;
    Ok(WriteOutcome::Changed)
}

/// Create each missing directory component and durably publish its parent entry. Existing symlink
/// components are refused so a catalog state writer cannot escape through a redirected resource
/// tree.
fn ensure_durable_directory(directory: &Path) -> Result<bool> {
    let mut missing = Vec::new();
    let mut cursor = directory;
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(metadata) => {
                anyhow::ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "state projection directory is not a real directory: {}",
                    cursor.display()
                );
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(cursor.to_path_buf());
                cursor = cursor
                    .parent()
                    .context("state projection directory has no existing ancestor")?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect state projection directory {}", cursor.display())
                });
            }
        }
    }
    let mut created = false;
    for path in missing.iter().rev() {
        let parent = path
            .parent()
            .context("state projection directory has no parent")?;
        match fs::create_dir(path) {
            Ok(()) => {
                created = true;
                sync_directory(parent, DirectorySync::CreatedEntry)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path).with_context(|| {
                    format!(
                        "inspect raced state projection directory {}",
                        path.display()
                    )
                })?;
                anyhow::ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "state projection directory is not a real directory: {}",
                    path.display()
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("create state projection directory {}", path.display())
                });
            }
        }
    }
    Ok(created)
}

fn sync_directory(directory: &Path, _kind: DirectorySync) -> Result<()> {
    #[cfg(test)]
    test_sync::checkpoint(_kind)?;
    File::open(directory)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("sync state projection directory {}", directory.display()))
}

#[cfg(test)]
mod test_sync {
    use super::DirectorySync;
    use std::cell::Cell;

    thread_local! {
        static FAIL_ONCE: Cell<Option<DirectorySync>> = const { Cell::new(None) };
        static CREATED_ATTEMPTS: Cell<usize> = const { Cell::new(0) };
        static PUBLISHED_ATTEMPTS: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn fail_once(kind: DirectorySync) {
        FAIL_ONCE.set(Some(kind));
    }

    pub(super) fn attempts(kind: DirectorySync) -> usize {
        match kind {
            DirectorySync::CreatedEntry => CREATED_ATTEMPTS.get(),
            DirectorySync::PublishedFile => PUBLISHED_ATTEMPTS.get(),
        }
    }

    pub(super) fn checkpoint(kind: DirectorySync) -> anyhow::Result<()> {
        match kind {
            DirectorySync::CreatedEntry => CREATED_ATTEMPTS.set(CREATED_ATTEMPTS.get() + 1),
            DirectorySync::PublishedFile => PUBLISHED_ATTEMPTS.set(PUBLISHED_ATTEMPTS.get() + 1),
        }
        if FAIL_ONCE.get() == Some(kind) {
            FAIL_ONCE.set(None);
            anyhow::bail!("injected {kind:?} sync failure");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::fs::symlink;

    #[test]
    fn unchanged_bytes_preserve_the_published_inode() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("resources/presentation.json");
        assert_eq!(
            write_atomic_if_changed(&path, b"one\n").unwrap(),
            WriteOutcome::Changed
        );
        let inode = fs::metadata(&path).unwrap().ino();

        assert_eq!(
            write_atomic_if_changed(&path, b"one\n").unwrap(),
            WriteOutcome::Unchanged
        );
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);

        assert_eq!(
            write_atomic_if_changed(&path, b"two\n").unwrap(),
            WriteOutcome::Changed
        );
        assert_ne!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(fs::read(&path).unwrap(), b"two\n");
    }

    #[test]
    fn symlinked_directory_or_target_is_refused() {
        let temporary = tempfile::tempdir().unwrap();
        let elsewhere = temporary.path().join("elsewhere");
        fs::create_dir(&elsewhere).unwrap();
        let linked_resources = temporary.path().join("linked-resources");
        symlink(&elsewhere, &linked_resources).unwrap();
        assert!(
            write_atomic_if_changed(&linked_resources.join("presentation.json"), b"value\n")
                .unwrap_err()
                .to_string()
                .contains("not a real directory")
        );

        let resources = temporary.path().join("resources");
        fs::create_dir(&resources).unwrap();
        let target = resources.join("presentation.json");
        symlink(elsewhere.join("target"), &target).unwrap();
        assert!(
            write_atomic_if_changed(&target, b"value\n")
                .unwrap_err()
                .to_string()
                .contains("not a real regular file")
        );
    }

    #[test]
    fn retries_uncertain_directory_and_file_durability_without_rewriting_equal_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("resources/presentation.json");

        test_sync::fail_once(DirectorySync::CreatedEntry);
        assert!(
            write_atomic_if_changed(&path, b"one\n")
                .unwrap_err()
                .to_string()
                .contains("injected CreatedEntry sync failure")
        );
        assert!(path.parent().unwrap().is_dir());
        assert!(!path.exists());
        let created_attempts = test_sync::attempts(DirectorySync::CreatedEntry);
        assert_eq!(
            write_atomic_if_changed(&path, b"one\n").unwrap(),
            WriteOutcome::Changed
        );
        assert!(test_sync::attempts(DirectorySync::CreatedEntry) > created_attempts);

        test_sync::fail_once(DirectorySync::PublishedFile);
        assert!(
            write_atomic_if_changed(&path, b"two\n")
                .unwrap_err()
                .to_string()
                .contains("injected PublishedFile sync failure")
        );
        assert_eq!(fs::read(&path).unwrap(), b"two\n");
        let inode = fs::metadata(&path).unwrap().ino();
        let published_attempts = test_sync::attempts(DirectorySync::PublishedFile);
        assert_eq!(
            write_atomic_if_changed(&path, b"two\n").unwrap(),
            WriteOutcome::Unchanged
        );
        assert!(test_sync::attempts(DirectorySync::PublishedFile) > published_attempts);
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
    }
}

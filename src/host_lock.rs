//! The single-owner guard (post-v0 item b) — one supervising st2 per **(folder, host)**.
//!
//! Two st2 loops reconciling the same folder for the same host would double-spawn every agent and
//! fight over liveness. A persistent kernel `flock` prevents it and is scoped by host
//! (`<root>/.st2.<host>.lock`), because the folder is a *synced* catalog: host A's lock file
//! syncs to host B, and B must ignore it (B only reads `.st2.<B>.lock`). The lock is dot-prefixed so
//! discovery skips it. The file's pid text is diagnostic only; ownership is the retained kernel
//! lock, which the kernel releases on process exit. The inode is never removed.

use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek as _, SeekFrom, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

/// Retained, non-forgeable ownership of one canonical `(catalog, host)` runtime domain.
///
/// Runtime mutators accept this capability instead of accepting a path plus an independently
/// constructed [`HostLock`]. The lock remains held until this value is dropped.
pub struct HostOwnership {
    catalog: PathBuf,
    host: String,
    _lock: HostLock,
}

impl HostOwnership {
    pub fn acquire(catalog: &Path, host: &str) -> std::io::Result<Self> {
        let catalog = catalog.canonicalize()?;
        let metadata = fs::symlink_metadata(&catalog)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("catalog is not a real directory: {}", catalog.display()),
            ));
        }
        if host.is_empty()
            || host == "."
            || host == ".."
            || host.starts_with('.')
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "host must be one safe path component",
            ));
        }
        let lock = HostLock::new(&catalog, host);
        lock.acquire()?;
        Ok(Self {
            catalog,
            host: host.to_owned(),
            _lock: lock,
        })
    }

    pub fn catalog(&self) -> &Path {
        &self.catalog
    }

    pub fn host(&self) -> &str {
        &self.host
    }

}

/// A pid-file lock for one host's supervision of one folder.
pub struct HostLock {
    path: PathBuf,
    held: RefCell<Option<File>>,
}

impl HostLock {
    pub fn new(root: &Path, host: &str) -> Self {
        Self {
            path: root.join(format!(".st2.{host}.lock")),
            held: RefCell::new(None),
        }
    }

    pub fn pid_path(&self) -> &Path {
        &self.path
    }

    /// The diagnostic pid written by a foreign holder, if the kernel lock is
    /// currently held. The kernel `flock`, not this text, is ownership.
    pub fn live_owner(&self) -> Option<i32> {
        if self.held.borrow().is_some() {
            return None;
        }
        let file = OpenOptions::new().read(true).open(&self.path).ok()?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_UN);
            }
            return None;
        }
        if std::io::Error::last_os_error().kind() != std::io::ErrorKind::WouldBlock {
            return None;
        }
        fs::read_to_string(&self.path)
            .ok()?
            .trim()
            .parse::<i32>()
            .ok()
    }

    /// The lock inode is persistent and must never be removed. A free inode
    /// with old diagnostic text is harmless and reclaimable.
    pub fn has_stale_lock(&self) -> bool {
        self.path.exists()
            && self.live_owner().is_none()
            && fs::read_to_string(&self.path)
                .ok()
                .and_then(|raw| raw.trim().parse::<i32>().ok())
                .is_some_and(|pid| !process_alive(pid))
    }

    /// Atomically claim and retain the kernel lock until [`release`](Self::release).
    pub fn acquire(&self) -> std::io::Result<()> {
        if self.held.borrow().is_some() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&self.path)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        write!(file, "{}", std::process::id())?;
        file.sync_all()?;
        *self.held.borrow_mut() = Some(file);
        Ok(())
    }

    /// Release ownership while preserving the inode as the one lock domain.
    pub fn release(&self) {
        if let Some(file) = self.held.borrow_mut().take() {
            unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }

    /// The one clear, actionable message shown when another st2 owns this (folder, host).
    pub fn busy_warning(&self, owner_pid: i32) -> String {
        format!(
            "another st2 is already supervising this folder for this host (pid {owner_pid}) — refusing to start.\n\
             Two supervisors on one (folder, host) double-spawn every agent. Stop the other one first.\n\
             The persistent lock inode is reclaimed automatically when that process exits."
        )
    }
}

impl Drop for HostLock {
    fn drop(&mut self) {
        if let Some(file) = self.held.get_mut().take() {
            unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

/// Whether `pid` is a live process. `kill(pid, 0)` succeeds for a live process we may signal, and
/// fails with `EPERM` for a live process we may NOT signal (still alive) — both count as alive.
pub fn process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    let ret = unsafe { libc::kill(pid, 0) };
    if ret == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = HostLock::new(tmp.path(), "hetz");
        assert!(lock.live_owner().is_none());
        assert!(!lock.has_stale_lock());

        lock.acquire().unwrap();
        assert!(lock.pid_path().exists());
        assert!(
            lock.live_owner().is_none(),
            "our own lock is not a foreign owner"
        );

        lock.release();
        assert!(lock.pid_path().exists(), "kernel lock inode is persistent");
        assert!(lock.live_owner().is_none());
    }

    #[test]
    fn lock_path_is_host_scoped_and_dot_prefixed() {
        let tmp = tempfile::tempdir().unwrap();
        let a = HostLock::new(tmp.path(), "hetz");
        let b = HostLock::new(tmp.path(), "silber");
        assert_ne!(a.pid_path(), b.pid_path(), "per-host lock files");
        assert!(
            a.pid_path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with('.')
        );
    }

    #[test]
    fn free_persistent_lock_with_old_pid_text_is_reclaimable() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = HostLock::new(tmp.path(), "hetz");
        fs::write(lock.pid_path(), "2000000000").unwrap(); // almost certainly dead
        assert!(lock.live_owner().is_none());
        assert!(lock.has_stale_lock());
    }

    #[test]
    fn live_pid_text_without_a_kernel_lock_is_not_an_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = HostLock::new(tmp.path(), "hetz");
        fs::write(lock.pid_path(), "1").unwrap(); // init — always alive, not us
        assert_eq!(lock.live_owner(), None);
    }

    #[test]
    fn a_second_claim_refuses_while_the_first_kernel_lock_is_held() {
        let tmp = tempfile::tempdir().unwrap();
        let first = HostLock::new(tmp.path(), "hetz");
        let second = HostLock::new(tmp.path(), "hetz");
        first.acquire().unwrap();
        assert_eq!(second.live_owner(), Some(std::process::id() as i32));
        assert_eq!(
            second.acquire().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        first.release();
        second.acquire().unwrap();
    }

    #[test]
    fn ownership_is_canonical_validated_and_retains_the_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let ownership = HostOwnership::acquire(tmp.path(), "hetz").unwrap();
        assert_eq!(ownership.catalog(), tmp.path().canonicalize().unwrap());
        assert_eq!(ownership.host(), "hetz");
        assert_eq!(
            HostOwnership::acquire(tmp.path(), "hetz")
                .err()
                .unwrap()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(HostOwnership::acquire(tmp.path(), "../hetz").is_err());
        drop(ownership);
        HostOwnership::acquire(tmp.path(), "hetz").unwrap();
    }

    #[test]
    fn process_alive_basics() {
        assert!(process_alive(std::process::id() as i32)); // ourselves
        assert!(!process_alive(0));
        assert!(!process_alive(-1));
        assert!(!process_alive(2_000_000_000)); // dead
    }
}

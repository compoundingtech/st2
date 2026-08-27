//! Shared task isolation used by st2 and st3.

use std::ffi::OsStr;
use std::process::Command;

pub use st_runtime::Isolation;

pub fn mode() -> Isolation {
    st_runtime::warn_if_degraded("st2");
    st_runtime::isolation_mode()
}

pub fn scope_unit(task_id: &str) -> String {
    st_runtime::scope_unit("st2", task_id)
}

pub fn wrap(unit: &str, program: &OsStr, arguments: &[&OsStr]) -> Command {
    st_runtime::wrap_isolated(unit, program, arguments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn st2_scope_names_keep_the_existing_prefix() {
        let first = scope_unit("hetz.demo.agent");
        let second = scope_unit("hetz.demo.agent");
        assert!(first.starts_with("st2-hetz.demo.agent-"));
        assert!(first.ends_with(".scope"));
        assert_ne!(first, second);
    }
}

# DELTA-001: SessionStart hook evidence is not compiled

Status: open

## Divergence

The Rust acceptance test for the SessionStart hook exists in source but has not
been compiled or executed. Shell evidence does not close that axis.

## VRS

[R33](../requirements.md) requires provider-visible, non-truncating session
restoration, while R17 requires unexpected hook failures to propagate durably.
The implementation shape and remaining failure-receipt question are specified
in [Provider session-start restoration](../spec.md#provider-session-start-restoration-r07-r09-r17-r33).

## Implementation

The shell-level hook seam proves that a 256 KiB context reaches `jq` through
stdin and is preserved as exactly 262,144 context bytes in valid provider JSON.
`tests/claude_hooks.rs` encodes the same large-context case together with the
channel, staleness, ordinary cold-start, and missing-dependency cases.

No VRS check result is Rust execution evidence.

## Direction

update implementation

## Resolution Signal

`tests/claude_hooks.rs` compiles and its focused test target passes with Bash and
`jq` present. Record the exact command and result before closing this delta.
Keep DQ5 independent: the current tests do not prove durable failure
propagation.

# Doctor — Spec

This is a concise map from the [Doctor requirements](requirements.md) to the
current CLI, implementation, and tests. It does not contract check order,
private filenames, exact report wording, or fallback quirks.

## Scope and inputs

The [`doctor` CLI](../../../src/main.rs#L236-L249) selects a catalog through the
normal shared catalog rules and a host explicitly or from local detection.
`--require-supervisor` lets a caller distinguish a resident deployment from
intentional manual or one-shot operation. The resolved pair is reported so a
captured diagnosis identifies its subject.

## Check categories

The implementation is authoritative in
[`doctor_cmd`](../../../src/main.rs#L946-L1087). Its checks fall into these
categories; their internal order is not part of this sub-VRS.

| Category | Diagnostic question | Authority |
| --- | --- | --- |
| Environment | Are required runtime tools available? | [`doctor_cmd`](../../../src/main.rs#L955-L961) |
| Supervision | Is the selected host's resident/manual mode consistent with the caller's request? | [`doctor_cmd`](../../../src/main.rs#L963-L986); [mode tests](../../../tests/doctor.rs#L40-L92) |
| Catalog | Can the selected catalog be structurally understood? | [`doctor_cmd`](../../../src/main.rs#L988-L998) |
| Runtime | Can task state be read safely, and is each active local task alive? | [unified runtime view](../../../src/run.rs#L414-L449); [bounded PTY probe](../../../src/run.rs#L38-L84) |
| Presence | Does each active local declaration have maintained presence? | [`doctor_cmd`](../../../src/main.rs#L1021-L1079) |
| Retirement | Are all declared task records for a retired local declaration absent? | [retirement tests](../../../tests/doctor.rs#L191-L297) |

## Output contract

Doctor emits a human-readable report for the resolved catalog and host. Each
failure carries a subject label and available detail. It succeeds only when no
problem was diagnosed; otherwise it exits non-zero. The formatter and exit
decision are directly visible in
[`report_check`](../../../src/main.rs#L1082-L1107), while the clean-path test
proves the CLI remains usable without a predecessor transport
([evidence](../../../tests/native_only.rs#L243-L365)).

## Open questions

These are implementation gaps or unsettled product choices, not requirements:

- **Malformed presence:** unreadable or invalid fresh presence currently becomes
  `offline`; should doctor distinguish corruption?
  ([source](../../../src/status.rs#L70-L95))
- **Exec enumeration bound:** terminal-free record enumeration is synchronous
  filesystem work outside the PTY probe deadline; what bound should the whole
  diagnostic promise? ([source](../../../src/exec_backend.rs#L125-L160))
- **Tool dependency:** `pty` is required even when the selected host declares
  only terminal-free work; should required tooling follow the selected tasks?
  ([source](../../../src/main.rs#L955-L961))
- **Machine output:** the command has no stable machine-readable report; should
  callers receive structured findings in addition to the exit status?
  ([CLI surface](../../../src/main.rs#L236-L249))

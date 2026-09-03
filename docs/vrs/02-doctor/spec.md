# Doctor specification

This document maps the [Doctor requirements](requirements.md) to the current
CLI, implementation, and tests. It does not define check order, private
filenames, exact report text, or fallback behavior.

## Scope and inputs

The [`doctor` CLI](../../../src/main.rs#L240-L252) selects a catalog and a host.
Shared catalog rules resolve the catalog. The caller can select a host, or st2
can detect the local host. The `--require-supervisor` flag requires a resident
`st2 up` process. Without this flag, manual and one-shot operation are valid.
The report names the resolved catalog and host.

## Checks

[`doctor_cmd`](../../../src/main.rs#L988-L1130) is the implementation authority.
It performs these check groups:

- **Environment:** The required runtime tools are available
  ([source](../../../src/main.rs#L997-L1003)).
- **Supervision:** The host mode matches the caller's request
  ([source](../../../src/main.rs#L1005-L1028); [tests](../../../tests/doctor.rs#L40-L92)).
- **Catalog:** The selected catalog has no discovery errors
  ([source](../../../src/main.rs#L1030-L1040)).
- **Runtime:** Runtime state is readable, and each active local task is alive
  ([Doctor source](../../../src/main.rs#L1041-L1102); [unified runtime view](../../../src/run.rs#L687-L700)).
  The PTY probe has a fixed deadline and closed standard input
  ([probe source](../../../src/run.rs#L443-L458); [tests](../../../tests/doctor.rs#L95-L194)).
- **Presence:** Each active local declaration has maintained presence
  ([source](../../../src/main.rs#L1103-L1121)).
- **Outbound messages:** Each local declaration has a valid sender ledger. An unavailable ledger is
  healthy because the agent has not sent a message. An invalid ledger reports that the agent cannot
  send. Doctor does not create or repair sender state.
- **Retirement:** Every task record for a retired local declaration is absent
  ([source](../../../src/main.rs#L1068-L1089); [tests](../../../tests/doctor.rs#L196-L302)).
- **Suspension:** No task of a suspended local declaration is alive. A dead
  record is accepted only when the task or agent is keep-pinned; presence is
  not required. This remains distinct from retirement's full-record absence
  ([source](../../../src/main.rs); [tests](../../../tests/doctor.rs)).

## Output contract

Doctor writes a human-readable report for the resolved catalog and host. Each
failure has a subject label and available detail. Doctor returns zero only when
it diagnoses no problem. The formatter and result are in
[`report_check`](../../../src/main.rs#L1165-L1175) and
[`doctor_cmd`](../../../src/main.rs#L1124-L1129). The
[clean-path test](../../../tests/native_only.rs#L243-L369) proves that the CLI
does not need a predecessor transport.

## Open questions

These are implementation gaps or unsettled product choices, not requirements:

- **Malformed presence:** Unreadable or invalid fresh presence becomes
  `offline`. Should Doctor report corruption separately?
  ([source](../../../src/status.rs#L70-L95))
- **Exec enumeration bound:** Exec record enumeration is synchronous file-system
  work. It is outside the PTY deadline. What deadline should cover the full
  diagnosis? ([source](../../../src/exec_backend.rs#L190-L217))
- **Tool dependency:** Doctor requires `pty` when a host has only exec tasks.
  Should the declared tasks select the required tools?
  ([source](../../../src/main.rs#L997-L1003))
- **Machine output:** Doctor has no stable machine-readable report. Should it
  provide structured findings in addition to the exit status?
  ([CLI surface](../../../src/main.rs#L240-L252))

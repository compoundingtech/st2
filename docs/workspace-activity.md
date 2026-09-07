# Workspace activity snapshot

`st2 workspace-activity --json` emits short-lived, read-only evidence about activity in explicit
Agent Spec `workspace` paths on one host. Suspended and retired declarations remain in the snapshot
so a retained live generation cannot disappear from cleanup evidence. The command reuses st2's PTY
and exec generation observers; it does not scan arbitrary processes, reconcile tasks, authorize
cleanup, or delete anything.

The `st2.workspace-activity.v1` envelope contains `schemaVersion`, `producer`, an `epoch` bound to
the canonical catalog, host, and catalog generation, `capturedAt`, `expiresAt`, `complete`, `errors`,
and lexically sorted `claims`. Each claim contains a canonical workspace path, sorted owning agent
IDs, sorted positively running runtime IDs, and the derived `active` boolean.

Consumers must fail closed unless `complete` is true, the snapshot has not expired, and the epoch is
the one they admitted. An inactive claim means only that st2 observed no running generation for its
declared tasks in this snapshot. Cleanup still needs its own filesystem/process liveness checks and
must revalidate immediately before mutation.

The TTL must be between 1 and 300 seconds. Out-of-range values emit an incomplete envelope whose
`expiresAt` equals `capturedAt`, then exit non-zero.

This v1 precursor identifies active runtime IDs but is not a generation-bound lease: generation
PID/creation evidence remains available from `st2 tasks --json`. A deletion transaction must obtain
and revalidate that stronger evidence rather than treating this snapshot as a lock.

Example:

```console
st2 --catalog "$CATALOG" workspace-activity --host dev3 --ttl 60 --json
```

Catalog discovery errors, an unavailable PTY/exec backend, declaration drift, catalog-generation
drift, and an unresolvable declared workspace all make the envelope incomplete and the command exits
non-zero after printing the JSON evidence.

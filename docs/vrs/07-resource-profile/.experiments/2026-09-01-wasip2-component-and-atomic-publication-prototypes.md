# WASIp2 component and atomic publication prototypes

Date: 2026-09-01

## Question

Can observable Resource providers run through one capability-safe WASIp2 Component Model envelope while the host retains validation, fencing, atomic publication, and delivery ownership?

## Method

Five disposable prototypes exercised independent parts of the boundary with Wasmtime 48.0.1:

1. A GitHub Issue component imported one typed issue-source operation and exported one typed observation operation. The host enforced an exact HTTPS host, port, and path allowlist; denied redirects and non-allowlisted or loopback destinations before I/O; bounded headers and bodies; and applied connection and operation deadlines.
2. A local-observation component imported one versioned, domain-typed `pty-stats` command. The host selected the executable and fixed arguments, passed no environment, used `/` as the child working directory, bounded retained stdout and stderr, enforced a deadline, and returned typed denials and failures. The guest could not supply an executable, path, environment, working directory, argument vector, or shell text.
3. A component lifecycle benchmark compared compilation, verified AOT deserialization, fresh Store plus instance invocation, and state reuse with recorded in-process provider input.
4. A cancellation and fencing driver exercised non-yielding guest CPU, a pending async host capability, concurrent same-prior proposals, stale generation after replacement, cancellation after guest return but before commit, and acknowledgement loss after commit.
5. A runtime-neutral multiprocess publication model used an independent oracle to verify fenced compare-and-swap, deterministic outbox identity, process-crash boundaries, retry, and delivery catch-up.

The harnesses and their local artifact paths were disposable experiment machinery. They are provenance for the results below, not production modules, cache locations, state formats, or reusable runtime scaffolding.

## Evidence

### Typed component and capability boundary

- The real GitHub request returned HTTP 200 and committed one carrier plus one event intent. A cold second process sent the prior ETag, received HTTP 304, and committed neither a duplicate carrier nor a duplicate event.
- The GitHub capability exposed one typed `get-issue(issue-key)` operation. It did not expose raw HTTP. The host applied a 16 KiB response-header ceiling, 64 KiB response-body ceiling, a 3 s connect deadline, a 10 s operation deadline, and redacted typed errors.
- The real `pty stats --json` observation used one fresh Store and exactly one typed host call. Its allowlist entry selected one fixed executable and four fixed arguments. Five denied or failed observations left committed bytes unchanged.
- Unknown tool IDs and unsupported typed argument variants failed before spawn. The child received zero environment entries, retained output was capped at 64 KiB per stream, and deadline cancellation targeted and reaped the process group.
- Both components returned normalized proposals only. The host performed proposal validation and the only state commit.

### Fresh Store and compiled-code reuse

- Every measured observation received new guest memory, tables, resources, and host state. A reused Store retained guest and host state and had no complete reset contract for deadlines, cancellation, async resources, or post-trap state.
- Cancellation verification created and dropped seven Stores. After guest interruption, host-future cancellation, stale fencing, commit cancellation, and acknowledgement-loss retry, active task and capability counts were zero.
- The measured verified AOT disk-hit p50 was 0.482 ms, and fresh Store plus instance observation p50 was 47.751 µs for the small recorded GitHub component. These measurements establish that fresh-Store isolation is viable for this fixture; they are not a cross-provider latency promise.
- AOT deserialization remained an unsafe native-code trust boundary. Corrupted artifacts were rejected before deserialization, and cache identity covered the component digest, Wasmtime/runtime build, target, and complete engine-compatibility hash.

### Atomic proposal and delivery boundary

- Concurrent observations with the same prior state produced exactly one compare-and-swap winner and one event intent. A stale generation after replacement committed nothing.
- Cancellation after component return but before host commit committed nothing. Acknowledgement loss after commit retried idempotently and did not duplicate carrier or event intent.
- The independent multiprocess oracle passed three runs of 30 processes. It recomputed carrier, event, and receipt identities from canonical durable bytes rather than importing the implementation.
- A crash before atomic publication preserved the old authoritative state. A crash after publication but before acknowledgement exposed the new carrier and deterministic outbox intent; restart delivery caught up and duplicate retry converged on the same identity.
- The filesystem experiment assumed POSIX lock exclusion, same-filesystem atomic rename visibility, file `fsync` before rename, and parent-directory `fsync` for durability. It did not establish power-loss behavior, remote-filesystem behavior, Windows behavior, or storage-controller honesty.

## Result

The evidence supports one production boundary:

```text
fresh Store + one WASIp2 provider component invocation
        |
        +-- only domain-typed host capabilities
        |
        v
Unchanged | typed failure | Publication proposal
        |
        v
host validates ProposalFence + Publication
        |
        v
one atomic carrier + revision + deterministic outbox-intent transition
        |
        v
separate idempotent delivery and catch-up
```

Compiled components and compatible host-owned AOT artifacts may be reused. Stores and instances may not. Capability implementations and atomic commit remain trusted host code; component sandboxing does not sandbox the host process or make a generic subprocess safe.

## Conclusion

A WASIp2 component is a suitable universal execution envelope for observable providers when its imports are narrow domain interfaces and every observation receives a fresh Store. The host must remain the sole authority for credentials, capability policy, validation, fencing, canonical digest, atomic carrier publication, deterministic outbox intent, and delivery acknowledgement.

The evidence does not support WASIp3 production execution, Store pooling, generic exec, raw HTTP, arbitrary filesystem or socket access, a parallel native-provider lifecycle, or a runtime WAC graph. Each would enlarge or duplicate the proven boundary and requires its own evidence gate.

## VRS Impact

- The closed core-wasm resolver remains the identity-to-carrier mechanism.
- One directly linked WASIp2 provider component envelope supersedes the trusted host-process observable runtime.
- Each descriptor call or observation receives one fresh Store and instance.
- `Publication` remains the observation payload and is paired with a host-owned generation, revision, and prior-digest fence.
- One host transition commits the carrier and deterministic durable outbox intent; delivery and acknowledgement remain separate.
- Generic host authorities, parallel provider runtimes, WASIp3 production, Store pooling, and runtime component graphs remain outside the contract until their explicit evidence gates are met.

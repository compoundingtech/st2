# Resource Profile language

## Language

**Resource Profile:** The downstream-owned contract registered for one exact Resource URI scheme. It may resolve a Resource to a contained carrier and may additionally make that Resource observable.

**resolver:** The closed, pure core-wasm part of a Resource Profile that maps a preserved Resource URI and agent directory to a contained carrier denotation. A resolver has no provider or ambient host authority.

**observable provider:** The part of a Resource Profile that reads provider state, normalizes it, and proposes a current snapshot. An observable provider is not a resolver, delivery sink, or provider action API.

**provider component:** The WASIp2 Component Model artifact that implements an observable provider through the universal provider world. It receives a fresh execution context for each descriptor call and observation.

**provider world:** The versioned Component Model execution envelope shared by observable providers. It defines the descriptor and observation exports and admits only reviewed domain-capability imports.

**domain capability:** A typed host operation for one provider-domain action, including its authority, inputs, outputs, limits, and failure vocabulary. _Avoid_: generic command, raw HTTP, arbitrary filesystem, raw socket.

**ambient authority:** Host access available without a specific typed grant for the observation being performed. Environment inheritance, caller-selected processes, raw network clients, arbitrary paths, and raw sockets are ambient authority in this subsystem.

**observation:** One attempt to determine a binding's current provider state. Its result is unchanged, a typed failure, or a proposed `Publication`; it is not itself a state transition.

**ProposalFence:** The host-issued expected state for one proposal: binding generation, revision, and prior carrier digest. It prevents replaced bindings and competing observations from publishing over a different current state.

**generation:** The identity of the current active binding registration. Replacement or unregister/register creates a different generation.

**revision:** The host-owned monotonic publication version within one binding generation.

**prior digest:** The authoritative carrier digest the observation was based on. It is a compare-and-swap precondition, not a component assertion about current host state.

**Publication:** The bounded provider payload proposed as canonical current state: schema identity, media type, snapshot bytes, semantic topics, and optional ordered typed facts. It carries no publication authority.

**publication proposal:** A `Publication` paired by the host with the `ProposalFence` under which it was observed. The proposal is input to validation and commit, not durable current state by itself.

**PublicationIntent:** The deterministic durable outbox entry derived by the host from an accepted proposal. It identifies the committed publication and retains the selected semantic envelope required for delivery and catch-up.

**atomic publication:** The single host-owned transition that makes the new carrier and its publication metadata, resulting revision, and `PublicationIntent` visible together. Readers observe either the prior complete state or the successor complete state.

**carrier:** The contained local snapshot through which an agent reads the Resource's current canonical bytes.

**delivery:** The separate, idempotent attempt to convey a committed `PublicationIntent` through the built-in resync stream. Delivery does not determine whether publication committed.

**delivery acknowledgement:** Durable evidence that the sink accepted a `PublicationIntent`. Until acknowledgement, the intent remains eligible for retry.

**catch-up:** Level-triggered delivery of the latest relevant committed state after delivery was unavailable. Catch-up does not replay every provider transition.

## Structure

```text
Resource Profile
  ├─ resolver ──denotes──> carrier
  └─ observable provider
       └─ provider component
            ├─ imports ──> domain capability
            └─ observation
                 └─ proposes ──> Publication

host pairs Publication + ProposalFence
  └─ validates
       └─ atomic publication
            ├─ replaces ──> carrier
            ├─ advances ──> revision
            └─ creates ──> PublicationIntent
                              └─ delivery ──> delivery acknowledgement
                                  └─ on interruption: catch-up
```

The leitwort is **propose, then commit**. Components observe and propose; only the host validates, fences, publishes, and delivers.

# st3-client

Reusable typed Rust client for `st3.client.v0`. It consumes versioned JSON projections and actions;
it never parses st3 CLI output, KDL, Markdown, raw claim envelopes, or harness transcripts.

```rust
let client = st3_client::Client::unix("/path/to/st3.sock");
let capabilities = client.capabilities().await?;
let work = client.list("work", None, Some(100), false).await?;
```

Use `Client::fabric_loopback` with a pairing credential for remote access. Mutation callers build an
`ActionRequest` from the latest response snapshot and exact resource fences. `ClientError::Api`
preserves stable error codes such as `stale-fence` and `cursor-gap`.

Run `cargo run -p st3-client-codegen -- --check` from the repository root to verify the generated
contract table matches the normative schema and operation manifest.

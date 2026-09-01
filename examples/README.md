# st2 examples

[`native/`](native/) contains the maintained, hand-authored Codex and Claude agent declaration
shapes. Render one outside the catalog as:

```text
<catalog>/agents/<host>/<identity>/agent.kdl
```

Replace every placeholder, put its referenced files in the bundle's `assets/` directory, then publish
and gate the result before starting a process:

```sh
st2 hooks install
st2 hooks verify
input_sha256="$(st2 agent digest --bundle <bundle>)"
st2 agent publish --catalog <catalog> --bundle <bundle> \
  --input-sha256 "$input_sha256" --expect-absent --json
st2 validate --catalog <catalog>
st2 up --catalog <catalog> --host <host> --materialize-only
st2 up --catalog <catalog> --host <host> --once
```

Canonical KDL is the boundary; st2 does not compile intent. Inspect all KDL and workspace targets
before publication.

## WASIp2 resource-provider E2E

This exact command builds both immutable guest components, creates temporary GitHub Issue and PTY
catalogs/bindings, runs both through the production supervisor, checks carrier publication,
unchanged replay, and a denied GitHub scope with no partial commit, then drops both supervisors.
It performs one anonymous, read-only request for `rust-lang/rust#1`; it writes no remote state and
uses no credentials.

```sh
github_component="$(
  nix build --no-link --print-out-paths .#st2-github-issue-component
)/share/st2/providers/st2_github_issue_component.component.wasm"
pty_component="$(
  nix build --no-link --print-out-paths .#st2-pty-stats-component
)/share/st2/providers/st2_pty_stats_component.component.wasm"
ST2_GITHUB_ISSUE_COMPONENT="$github_component" \
ST2_PTY_STATS_COMPONENT="$pty_component" \
nix develop --command cargo test \
  --features wasip2-provider-runtime \
  --test resource_provider_e2e \
  both_real_components_publish_replay_unchanged_and_fail_without_commit \
  -- --ignored --exact
```

The production binary is opt-in for the same reason:

```sh
nix build .#st2-provider-runtime
```

The default `.#st2` package and default Cargo members remain Wasmtime-free.

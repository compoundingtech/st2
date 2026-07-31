# Native st2 examples

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

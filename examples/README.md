# Native st2 examples

[`native/`](native/) contains the maintained, hand-authored Codex and Claude agent declaration
shapes. Copy one to:

```text
<catalog>/agents/<host>/<identity>/agent.kdl
```

Replace every placeholder, add its referenced files under `<catalog>/_templates/`, then gate the
result before starting a process:

```sh
st2 validate --catalog <catalog>
st2 up --catalog <catalog> --host <host> --materialize-only
st2 up --catalog <catalog> --host <host> --once
```

Hand-authored KDL is canonical. `st2 compile-agent` is an experimental generation aid; inspect all
of its KDL and workspace targets before use.

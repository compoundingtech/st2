# Start-to-finish local walkthrough

This walkthrough publishes and runs a harmless invented mission. It uses no account, repository,
customer, or production data. Run it only against a disposable ST3 installation or a development
daemon.

Set up an empty workspace and inspect the exact mission before writing graph state:

```sh
walkthrough_workspace=/tmp/st3-walkthrough
mkdir -p "$walkthrough_workspace"
st3 missions publish --help
sed -n '1,200p' examples/st3/walkthrough.kdl
```

Publish the mission definition. This command creates an immutable ready revision; it does not start
a run:

```sh
st3 missions publish examples/st3/walkthrough.kdl --as person/operator
```

The definition must now appear in `missions ls` even though it has zero runs. `missions show` must
describe the ready definition rather than report it missing:

```sh
st3 missions ls
st3 missions show example/walkthrough
```

Start one pinned run and follow it until completion:

```sh
st3 missions start example/walkthrough \
  --id first-run \
  --workspace "$walkthrough_workspace" \
  --as person/operator \
  --follow
```

Use the exact `mission-run/...` subject printed by `start` for the final inspection. The run should
be completed, `write-receipt` should be completed, and the local receipt should contain one line:

```sh
st3 missions show mission-run/EXACT-RUN-ID
cat "$walkthrough_workspace/st3-walkthrough-result.txt"
```

Expected receipt:

```text
st3 walkthrough complete
```

Remove the disposable workspace when finished. The immutable mission and completed run remain in
graph history by design.

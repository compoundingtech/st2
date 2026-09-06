# Targeted revision request

Revise the current `generation-proof` plan. Preserve the current plan ID, ready state, human-only revision review, completion rule, plan goal, input declarations, and existing step IDs.

Keep `stable` unchanged. Keep `changed` dependent on completed `stable`, but change its goal to `Use the corrected work definition.`

Add one root step named `generation-environment`. Its goal is `Record the automatic successor generation variable.` Give it this gate:

```kdl
gate "the successor generation variable is present" {
  exec "test -n \"$ST_RUN_GENERATION\" && printf '%s\\n' \"$ST_RUN_GENERATION\" > observed-generation.txt"
  host "local"
  workspace "${ST_WORKSPACE}"
  time-limit "30s"
}
```

The Markdown plan must explain that this is a targeted revision of a live run. The complete KDL candidate must describe only the revised ready plan. Do not publish or run the plan. Do not change any file in the workspace.

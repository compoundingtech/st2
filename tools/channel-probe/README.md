# Claude channel probe

This probe registers no MCP tools. It advertises the Claude channel capability
and sends one or more `notifications/claude/channel` notifications.

Run the idle-wake cell with:

```sh
./run-probe.sh
```

The script creates an isolated temporary workspace. It sends Return only for
the new-workspace trust prompt and the development-channel warning. It reports
success only when the provider debug log records a channel notification and a
later engine turn. The model does not need to obey the notification content.

Run the active-turn queue cell with three simple identifiers:

```sh
PROBE_TOKENS=QUEUE-A,QUEUE-B,QUEUE-C PROBE_INTERVAL=100 ./run-probe.sh
```

Notification A must start turn 1. Notifications B and C must arrive while turn
1 is active. Both must then produce a completed turn 2. The provider may
coalesce the two waiting notifications into that turn. `PROBE_DELAY` sets the
first delay in milliseconds. The idle default remains one token after 20
seconds.

These files are evidence tools. They are not the production `st2 claude-mcp`
adapter.

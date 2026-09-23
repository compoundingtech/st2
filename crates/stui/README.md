# Smalltalk terminal client

`stui` connects to the local st3 daemon through the generated Rust `st3.client.v0` client.
It reads bounded projection pages and event feeds, and sends typed actions with snapshot and
resource fences. It does not parse CLI output or maintain an offline mutation queue.

Run `ST3_PERSON=person/<your-id> cargo run -p stui --locked` in a terminal with a running st3
daemon. `ST3_ENDPOINT` can override the discovered Unix socket. Without `ST3_PERSON`, the local
client has read-only identity; sending messages and creating launches require a person identity.

Keys: `1`–`4` switch Now, Chat, Control, and Fleet; arrow keys select an item; PageUp/PageDown
scroll the detail pane; `s` hides the sidebar; `q` quits. Chat shows messages for the selected
agent's current session and bounded
normalized session history. Press `c` to compose and Enter to send. In Control, `c` starts a
new mission launch and Enter attaches to the selected runtime terminal. During attach, the
terminal receives every key except Ctrl+backslash, which detaches. The visible Return control
can be focused by clicking it and activated with Enter. On terminals narrower than 66 columns,
the sidebar hides automatically.

Now contains only open attention addressed to the current person. Agent transcript text and
unread messages do not become person attention. All lists and timelines stop after four pages
of 50 items and show a truncation marker. The event cursor is bounded and deduplicated; a
cursor gap clears the cached timeline and reloads projections from a fresh capability cursor.

Verification:

```sh
cargo test -p stui --locked
cargo build -p stui --locked
ST3_PERSON=person/<your-id> python3 crates/stui/tests/pty_smoke.py
```

The PTY smoke test needs a running local daemon. It checks live view output, navigation,
idle silence, and alternate-screen restoration after normal exit, SIGTERM, and a debug panic.

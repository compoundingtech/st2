# Smalltalk TUI starter

This is a render-and-terminal-lifecycle proof, not a connected client. It shows the four agreed
destinations and intentionally labels all data as unavailable.

Run `cargo run -p stui --locked` in an interactive terminal. Press `1`–`4` to switch screens and
`q`, Escape, or Ctrl-C to leave. The shell uses a blocking input read, so it does not redraw on an
idle timer. The terminal guard restores the alternate screen and raw mode on ordinary exit and
Rust unwinding; signal handling and connected-client behavior belong to the product build.

`cargo test -p stui --locked` exercises rendering with Ratatui's test backend. A real PTY smoke
check should additionally verify visible output and alternate-screen restoration.

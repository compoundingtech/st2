# st2

Before changing the run loop, the message/bus wire format, teardown, or the presence model, read
**[INVARIANTS.md](INVARIANTS.md)** — the load-bearing guarantees, each with the test that proves it.
These are sacred: keep the named test green, or you broke the guarantee. Add an entry only when a
genuinely load-bearing invariant appears, and only once a real test proves it.

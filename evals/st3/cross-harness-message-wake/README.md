# Cross-harness message wake

This paid black-box eval starts one Codex agent and one Claude agent with the normal generated boot contract. It waits until each native transport is quiescent (`idle` for Codex and `ready` or `idle` for Claude), sends each a private token through durable Small Talk, and requires them to reach the same result through a fact, agreement, and final-report exchange.

The agent-facing messages describe only the coordination task. They do not mention terminal input, historical failure modes, or alternative wake mechanisms.

Held-out gates prove:

- both initial messages were sent only after both native transports reached their quiescent pre-message state;
- Codex and Claude each read the initial message and exchanged the required canonical messages;
- both normalized timelines changed from idle to working after the messages were sent;
- both agents independently reported `EMBER+ORBIT` after the peer exchange;
- no graph-authorized terminal input occurred; and
- an invisible executable shim observed no direct terminal send or attach invocation from either harness environment.

The last two checks are external observations. They are not disclosed to either model.

The controller gives the post-kickoff exchange five minutes. A transport that projects messages without starting model work fails within that bound.

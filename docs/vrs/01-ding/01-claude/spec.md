# Claude harness specification

This document defines native MCP delivery and the legacy Claude screen
grammar. It realizes [`../requirements.md`](../requirements.md) through the
selection and durability rules in [`../spec.md`](../spec.md).

## Status

Active. Native production acceptance is blocked until provider or organization
policy supplies a non-interactive channel enrollment for this integration.

## Native MCP transport

`deliver "mcp"` selects this transport. st2 starts one `st2 claude-mcp`
process for the declared agent. The process serves MCP over standard input and
standard output. It does not use the PTY, inspect the screen, or start the
legacy `ding` sidecar.

### Provider enablement

The launched Claude session must enable the MCP server as a channel source.
An MCP connection alone is not sufficient. Claude can connect to a server,
list its tools, and still discard every channel notification when the server
is not enabled as a channel.

The server advertises the `experimental.claude/channel` capability. The st2
launch configuration must select that server as a channel. The MCP wire does
not return a channel-acceptance receipt to the server, so the adapter must not
claim that it detects provider acceptance at runtime. The provider evidence
below proves that the maintained launch configuration accepts the channel.

The maintained launch path must use a provider-approved channel enrollment.
The provider's development-only channel override is observation and test
equipment. It is not production acceptance.

The bounded packaging and policy path is in
[`packaging.md`](./packaging.md). That document does not authorize building the
plugin.

### Notification

The adapter sends one MCP notification with this method:

```text
notifications/claude/channel
```

The notification parameters have this shape:

```json
{
  "content": "Subject: <subject>\n\n<body>",
  "meta": {
    "from": "<sender bus ID>",
    "messageFilename": "<message filename>",
    "threadFilename": "<thread-root filename>",
    "identity": "<recipient bus ID>"
  }
}
```

`content` is the message body. A present subject adds the shown `Subject:`
line and one blank line. A message without a subject uses the body alone.
`threadFilename` is the valid `in-reply-to` value when one exists. Otherwise,
it is `messageFilename`. The adapter does not invent sender, thread, or
recipient values from display text.

Claude presents an accepted notification as an inbound channel item. When the
session is idle, the item starts a turn without terminal input from st2. This
provider behavior is the native wake receipt. The adapter does not parse the
rendered inbound line and does not wait for a model response.

### Durable inbox and retry

The selected catalog inbox remains authoritative. The adapter does not archive
or delete a message after notification. The agent reads and archives the
message through the normal message commands.

Before each notification attempt, the adapter runs the normal message sweep.
An archive record with the same filename wins. The adapter sends nothing for a
message that the sweep removes from the inbox.

The notification attempt succeeds only when the MCP transport is open and the
notification write completes. A successful write completes this DING attempt;
it does not acknowledge, read, or archive the message. A write failure keeps
the message eligible for retry. A closed transport is a normal adapter stop.
It sends nothing further and leaves every remaining message unread.

The adapter scans the existing inbox when it starts. It then uses a file
watcher as a low-latency signal and a poll as the correctness backstop. The
poll interval is at most 15 seconds. Watcher and poll observations share one
successful-delivery set, so they do not send the same file twice in one
adapter incarnation. A failed notification does not enter that set.

The poll path must work when the watcher is disabled. This is a required test
mode and a production recovery property. The watcher may fail or miss an
event without stopping delivery.

One malformed or unreadable inbox file must not stop the scan, watcher, poll,
or later valid messages. The adapter reports the file failure, leaves that
file unread, and continues with the other files.

### Shutdown and recovery

Transport close stops the adapter without an error. st2 may start a new
adapter while the agent still declares this transport. A new adapter scans the
inbox again. It may repeat a notification that a prior adapter wrote before it
lost its process-local delivery set. This at-least-once behavior is safe
because the inbox file is still the source of truth and message actions are
idempotent.

### Provider evidence

A live Claude Code observation must prove all of these results before the
native adapter is accepted:

- Claude reports the MCP server as connected and the channel as accepted.
- A message sent to an idle agent appears as an inbound channel item.
- The inbound item starts observable work without a user keystroke.
- A message sent during active work does not corrupt user input or terminal
  state.
- The inbox file remains until the agent reads and archives it.
- A deliberately broken notification or disabled transport makes the test
  fail.

## Legacy screen transport

The remaining sections define the unchanged screen grammar selected by
`ding`.

## Locating the composer

The composer is drawn between two horizontal rules. Both are located by
structure rather than by width: a rule is a line whose trimmed content is at
least forty characters and consists entirely of the box-drawing horizontal
character. The composer occupies the rows between the last two rules on the
ANSI-stripped screen; the footer is everything below the lower rule.

The prompt is the `❯` character followed by a no-break space. A narrow space is
load-bearing here: the no-break space is Unicode whitespace, so trimming a
composer row before removing the prompt erases the prompt on an *empty*
composer and makes it unlocatable. The prompt is therefore removed before any
trimming.

Locating the composer is what identifies the harness. Startup output, including
the version banner, is not used: an inspection sees the current viewport, so
anything printed once at session start is absent from every screen after the
first (`DING-R05`). The banner is in fact weaker than that argument assumes — at
common pane widths it is not rendered at all, so it is absent from the very
first screen too.

Failing to locate a composer means this harness proves nothing about the screen;
it does not by itself make the screen `Ambiguous`. Under positional dispatch the
screen is offered to every maintained harness, so a pane holding another
harness's live composer is classified by that harness (`DING-R04`). `Ambiguous`
is the result when *no* harness locates a composer.

## Proving idle

The footer carries the permission-mode indicator that ships with the composer
chrome. Idle requires both the mode marker and the permission state to be
present.

The keybinding hint that may appear beside them is **not** an idle signal. It is
composed at runtime from a keybinding lookup, so its text changes when the
binding is remapped and it is absent from many screens that are equally idle.
It may corroborate an idle footer; it may not decide one.

## Proving empty

Two shapes count as positively empty:

- an empty composer, and
- a composer holding only the rotating example placeholder, matched as the exact
  `Try "<single-line example>"` grammar with a bounded, control-free example.

The empty composer is the stronger of the two. The placeholder is not styled
differently from typed input, so recognizing it relies on a text grammar a human
could in principle type; an empty composer cannot be confused with a human
draft, because a draft is not empty.

## Post-submit receipt

The exact notice as the complete lowest live composer is `RetainedSafe` only
with the ordinary idle proof and no blocking state; otherwise it is
`RetainedBlocked`. `Accepted` requires both an empty or recognized placeholder
in the lowest live composer and the expected notice text in the adapter's
submitted-prompt or queued-message pattern. A parsed placeholder, empty
composer, or different live draft that is not accepted is `NotRetained`.
Disappearance and unrecognized pixels are `Unproven`.

## Blocked states

Return is withheld while the screen shows an active turn or a modal. Two
distinct shapes prove an active turn:

- an explicit interrupt hint, and
- the animated progress line, which carries a rotating verb followed by an
  ellipsis, and which **carries no interrupt hint at all**.

The second shape is required. A pane mid-turn has an empty composer and
unchanged footer chrome, so without it a working agent's screen satisfies both
the idle and empty proofs and would receive a Return mid-turn. On the current
build the first shape is not observed at all: no interrupt hint accompanies the
progress line at any width, so the second shape is the only one that fires. The
first is retained because it is cheap and fails closed.

The predicate is derived from real captured screens, and two properties of those
screens constrain it more tightly than it first appears.

**The leading glyph is not a stable key.** It animates through several
box-drawing and punctuation characters within a single turn. Matching any one of
them fails open for most of the turn.

**The elapsed timer is not always present.** Some active frames render the verb
and ellipsis alone, with no parenthesized timer. Requiring a timer would fail
open on exactly those frames.

**A completed turn renders an almost identical line**, distinguished only by
using a past-tense verb with an elapsed duration in place of the ellipsis. This
is the trap: that completed line sits above *every* idle composer, immediately
after every turn. A predicate keyed on the glyph, or on any shape that both
lines share, would therefore treat every idle pane as blocked and stop delivery
permanently, rather than erring conservatively as a widened blocked-check
normally does. The distinguishing signal is the ellipsis directly following a
single verb; nothing coarser is safe in the delivery-stopping direction.

Modal prompts specific to this harness — plan prompts, retry and capacity
notices, queued-message notices — are also blocking and are owned here, not by
shared code (`DING-R06`).

## Known limits

- The permission-state literal proves one permission mode. A pane in another
  mode does not reach a positively idle state and defers indefinitely rather
  than delivering. Widening this is a behavior change to what counts as idle,
  which per [`../spec.md`](../spec.md) requires evidence from a real screen, not
  a looser match.
- Recognition is tied to this harness's current rendering. A renderer change
  defers delivery until the grammar is updated; it does not produce a wrong
  delivery, because every unrecognized shape is `Ambiguous`.

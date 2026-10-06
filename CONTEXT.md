# discord-reader

A read-only window into Discord servers, channels, messages and threads for
LLM clients (MCP). It fetches on demand, caches what it fetched, and never
touches Discord's read state or writes anything.

## Language

### Reading and freshness

**Fetch**:
One on-demand retrieval of data from Discord. Fetching never marks anything read.
_Avoid_: crawl, sync, poll (for a single retrieval)

**Coverage**:
The span of message IDs in a channel that we have confirmed we fetched
contiguously from Discord. Coverage is stated as `covered_from` / `covered_to`
plus `has_gaps`; it is knowledge, not optimism.
_Avoid_: history, backlog

**Gap**:
A hole between two coverage ranges: message IDs we know we never fetched. A
gap means "unknown", never "nothing there".
_Avoid_: hole, missing (too vague)

**Backfill**:
Fetching a channel's older history, backwards from a known point. A channel's
backfill is `complete` only when we have reached the channel's first message.
_Avoid_: history scan

**Differential sync**:
Resuming from a cursor and retrieving only what changed since: new messages
(after a message ID), edits and deletions. Distinguished from a full re-read.
_Avoid_: incremental fetch, delta

**Cursor**:
A resumable position for paging or differential sync — typically the highest
message ID confirmed so far. Cursors survive process restarts.
_Avoid_: offset, bookmark

**Tombstone**:
A recorded deletion: a message ID we observed being removed, kept so a later
diff can report "deleted" instead of silently omitting it.
_Avoid_: delete marker

### Addressing the user

**Inbox**:
The cross-server set of messages directed at the current user: direct
mentions, replies to their messages, role mentions that apply to them, and
@everyone/@here. `list_mentions` and `list_replies` are inbox views.
_Avoid_: feed, notifications (Discord's notification state is out of scope)

**Match kind**:
Why an inbox item matched: `direct` (mentioned by ID), `reply` (replies to the
user's message), `role` (mentions a role the user holds), `everyone`
(@everyone/@here). One message can match for several reasons; the strongest is
reported first.
_Avoid_: reason, type (overloaded)

### Auth and capability

**Credential kind**:
Whether the daemon authenticates as a `user` account or a `bot`. Endpoints
Discord restricts to one kind (e.g. bot-only search) are declared, not guessed.
_Avoid_: auth mode

**Capability**:
A declared, per-method statement of what works under the current credential
kind, with notes where Discord limits it. `get_capabilities` is the source of
truth; tool errors carry the same classification.
_Avoid_: feature flag

### Failure

**Error source**:
Which layer failed: `discord`, `transport`, `rate_limit`, or `client`. A
cancellation or timeout is `transport`/`client`, never "no data" and never
"channel forbidden".
_Avoid_: error type (too generic)

**Retryable**:
Whether retrying the identical request may succeed later. Rate limits and
transport blips are retryable; permission errors and bot-only endpoints are not.
_Avoid_: transient

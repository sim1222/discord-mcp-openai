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
Message-ID intervals in a channel supported by contiguous retrieval
observations. Coverage is evidence of retrieval, not a claim of current live state.
_Avoid_: history, backlog

**Gap**:
An interval without confirmed retrieval evidence. A gap means "unknown",
never "nothing there" or proof that a message was lost.
_Avoid_: hole, missing (too vague)

**Backfill**:
Fetching a channel's older history, backwards from a known point. A channel's
backfill is `complete` only when we have reached the channel's first message.
_Avoid_: history scan

**Differential sync**:
Observing changes since a prior reading position. Only actually observed new
messages, edits or deletions count as changes; a recent-page refresh is not a complete history comparison.
_Avoid_: incremental fetch, delta

**Cursor**:
A resumable position within a specific listing or retrieval scope. Keeping
that position does not imply that an interrupted job restarts automatically.
_Avoid_: offset, bookmark

**Requested-window completeness**:
Retrieval evidence covers the specified channels and time bounds, and the
relevant observations can be classified without uncertainty. This does not imply complete history or current live state.
_Avoid_: fully synced (does not specify scope)

**Activity probe**:
A comparison of observed channel activity with prior reading progress.
Unobserved or never-read activity is not necessarily recent activity.
_Avoid_: new-message check (implies freshness)

**Metadata refetch**:
Retrieving missing addressing information for retained messages. It is
distinct from discovering older messages through backfill.
_Avoid_: full resync

**Tombstone**:
A recorded deletion: a message ID we observed being removed, kept so a later
diff can report "deleted" instead of silently omitting it.
_Avoid_: delete marker

### Addressing the user

**Inbox**:
The cross-server set of messages directed at the current user: direct
mentions, replies to their messages, role mentions that apply to them, and
@everyone/@here. Authorship alone does not exclude a message addressed to this account.
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
Declared support and limitations of a reader operation under a credential
kind. A capability does not prove current permissions or successful live acceptance.
_Avoid_: feature flag

### Failure

**Error source**:
The origin of a reading failure, such as Discord, transport, input validation
or the cache. A failed observation never means a completed empty observation.
_Avoid_: error type (too generic)

**Retryable**:
Whether retrying the identical request may succeed later. Rate limits and
transport blips are retryable; permission errors and bot-only endpoints are not.
_Avoid_: transient

# SQLite cache upgrades

The cache preserves fetched data across daemon upgrades. Opening an existing
database never implies that its message history was fetched contiguously.

## Startup and migration

`Store::open` resolves SQLite's actual main database filename. Startup diagnostics
log its canonical path, `PRAGMA user_version`, and migration history (version,
timestamp, backup path). The daemon does not bind its RPC socket until opening,
migrating, and validating the database succeeds.

Version 3 accepts a fresh database, the original unversioned schema, and the
previous unversioned v2 schema. Version 1 is also accepted when its shape is
compatible. Higher versions and incompatible schemas fail startup. Each open
checks required column types, nullability, defaults, primary keys, declared
indexes, and the FTS definition against a fresh canonical schema. Version 3
must also have its migration history entry; drift is not silently repaired.

Before changing a populated database, SQLite `VACUUM INTO` creates a consistent
snapshot, including committed WAL data. The snapshot is written to a unique
file beside the actual database, with mode 0600 on Unix and a `.partial` suffix.
Only a successfully checked and synced snapshot becomes a completed backup.
Insufficient space, permissions, or backup failure stops startup. Backups are
retained for manual recovery and are never overwritten.

Migration runs in an IMMEDIATE transaction, rechecks the version under its
write lock, adds the three nullable legacy columns, creates the other current tables
and indexes, validates the complete schema, and records version 3 and its
backup path atomically. Missing FTS is rebuilt from retained message content;
existing compatible FTS is preserved. Validation failure rolls back the schema
and history. Reopening a migrated database does not migrate again or create a
new backup.

Stop other writers before upgrade. The consistent backup precedes the migration
transaction: another writer could commit in between. A daemon started with a
wrong database path can still open a valid but different cache; compare startup
diagnostics and cache counts with the expected deployment before proceeding.

## Retained data and unknown metadata

Legacy messages retain their IDs, content, timestamps, and author associations.
`message_json`, `fetched_at`, and channel `last_message_id` remain NULL until
observed from Discord. Migration does not infer `coverage`, sync cursors, or
backfill completeness from message IDs. Existing proven ranges and cursors are
preserved when adopting an unversioned v2 database.

Normalized legacy messages carry `metadata_state: "requires_refetch"`.
Available API metadata carries `metadata_state: "available"` (also the default
when reading previously stored v2 JSON). Invalid cached JSON requires retrieval
as well. Missing mentions or replies on incomplete rows do not mean confirmed
absence.

`list_mentions` and `list_replies` report classified, known-metadata,
requiring-refetch, unresolved-classification, and unverified-scope counts in
`checked`, for the requested time window. They scan retained history and page
matches by exclusive message-ID cursor. Refreshing a recent page does not erase
the unknown status of older retained rows. Missing role membership and unknown
reply authors remain unresolved; successful `roles=[]` is a known empty set.
RFC3339 timestamps are compared as instants, including subseconds and offsets;
both time boundaries are inclusive. Equal bounds are valid, reversed bounds
are rejected. Self-authored messages are not categorically excluded.
Channel discovery includes retained messages even without sync history or a
cached channel listing, but does not establish a complete live inventory.
Guild and channel filters intersect. A known conflicting guild or known DM
fails with `scope_mismatch`; unknown guild associations are not filled from the
search argument and remain unverified. Invalid timestamps return a cache data
error. See [Inbox query contract](inbox-query-contract.md).

`coverage.requested_window` identifies the current query. Its conservative
snowflake bounds include every possible ID in each boundary millisecond.
`checked_ranges` and `uncovered_ranges` are intersections and subtractions of
confirmed ID intervals, labelled `representation: "message_id"`; they do not
fabricate timestamp observations. `cached_history` separately describes
retained whole-cache ranges. A closed requested window can be complete only for
explicit channel scope, full ID interval coverage, and certain metadata,
classification, and guild scope. Open bounds and cached-only channel discovery
cannot be complete. `observation_scope: "cached_observations"` does not certify
current live edits or deletions; `history_complete` remains false.
Each channel's messages, guild association, and coverage evidence are read in
one SQLite read transaction. This consistent per-channel observation does not
freeze other channels, subsequent inbox pages, or Discord itself.
Role-member GET requests and reply-author cache lookups occur outside the
target-message transaction; those classification dependencies can be newer.
Valid timestamps beyond the snowflake horizon are accepted with clamped ID
bounds, disclosed by `requested_id_window.clamped` and per-bound flags. Their
time conditions are retained, and `window_outside_snowflake_horizon` prevents
`complete=true` even when all representable IDs are covered.

`get_sync_status` exposes `schema_version` and the total
`messages_requiring_refetch`. `cached_messages` counts retained rows;
`channels_tracked` and `coverage` describe recorded sync evidence, not every
channel with cached messages. Thus 18,384 cached messages with zero tracked
channels and empty coverage is possible for a legacy cache and is not proof of
data loss.

`list_changed_channels` makes no HTTP requests; it probes stored activity and
sync cursors. Refresh the activity observation with `list_channels` first when
needed. `reason: "never_synced"` means no sync cursor, even for an old channel;
`reason: "newer_activity"` means an observed activity ID exceeds that cursor.
The listing's current retrieval time does not make old activity new. Neither
reason proves edit or deletion detection.
Changed-channel paging filters by guild and uses the last activity ID/channel
ID pair as a cursor, probing one additional row for continuation. Synchronizing
a returned channel does not shift the next page. This is a changing activity
listing, not a frozen snapshot: activity added above the cursor is found by
starting a new scan. `messages_after` keeps its exclusive
Discord boundary and returns `next_after_message_id` (highest returned ID, or
the input on an empty page). A full page sets `has_more=true` conservatively;
the next page may be empty. A message page never replaces Discord's observed
channel `last_message_id` with its own maximum.

## User-authenticated message observation

The `MessageLookup` boundary observes one exact channel/message pair without
writing coverage, history cursors, or deletion tombstones. With user credentials,
`get_message`, `get_message_raw`, `get_attachment`, the focal message in
`message_context`, and reply resolution use `GET /channels/{id}/messages` with
`around={message_id}` and select only the matching ID and channel. Bot
credentials use the direct single-message GET route. Raw retrieval preserves
unknown fields from the selected wire object and reports the actual operation.

An absent target produces structured `message_not_observed`, not a declaration
of deletion. HTTP 403 with Discord code 20002 is `bot_only`; other permission
failures are `forbidden`. Reply views distinguish `resolved`, `deleted`,
`forbidden`, `bot_only`, `not_observed`, `unavailable`, and `unknown`. A missing
`referenced_message` field leaves reference state unknown; explicit null on a
reply establishes a deleted reference and avoids an unnecessary lookup.
Forwarded references do not receive that deleted-reply inference. These wire
semantics follow the [Discord message reference](https://github.com/discord/discord-api-docs/blob/main/developers/resources/message.mdx).
Neither a missing around target nor an unknown-channel response creates a
tombstone or removes retained data. All remote operations remain GET-only.

## Activity and differential-page contracts

`list_channels` returns explicit nullable `last_message_id` and
`last_activity_at`, plus `last_fetched_at`, listing-level `fetched_at`, and
`last_message_id_source` (`discord`, `cache`, or `unknown`). Null activity is
unknown or inapplicable, not proof that a channel is empty. A cached source
indicates retained activity evidence when the fresh channel object lacks it.
Activity time is derived from the message snowflake and describes creation,
not an edit or deletion. The observed activity also feeds
`list_changed_channels` and must agree with its cached channel record.

`messages_after` formally uses `next_after_message_id`, not `next_cursor`.
Pass it as the next call's exclusive `after_message_id`. Results are newest
first and continuation is the maximum observed ID, retaining the input on an
empty page. Page `covered_from`/`covered_to` are minimum/maximum observed
message IDs, or null for an empty page. Nested `coverage.ranges` and
`coverage.gaps` describe recorded observations and unproven intervals between
them; `coverage.has_gaps` reports those gaps. `coverage.complete` remains false.
No internal gap does not prove coverage outside the recorded ranges, and an
empty differential response alone is not a historical completeness certificate.

## Bounded legacy metadata refetch

REST message responses that omit `guild_id` preserve the retained row's guild
context (or the cached channel's guild). Both the SQL column and normalized
message metadata retain it, so refetch does not remove guild search/scan targets.

`start_sync` with `scope: "refetch"` processes retained rows whose metadata is
unknown, including rows with invalid cached JSON. Optional `guild_id` filters
known cached associations; optional `channel_ids` selects explicit targets.
`max_messages` bounds network lookup attempts per job (default 100, range
1..1000). It retrieves exact messages through `MessageLookup`, persists
successful full metadata, and leaves unavailable rows pending. It does not
synthesize empty mentions, infer deletion, advance history cursors, or add
coverage from isolated observations.

`get_sync_progress` reports `attempted`, `refetched`, `remaining`,
`remaining_channels`, `next_refetch_before`, and structured `failures` (also
`channels_failed`).
Remaining is initially unknown and can be null after a cache failure. States
are `running`, `done`, `paused` (attempt bound reached with work pending),
`done_with_gaps` (unavailable rows remain), or `failed` (cache failure).
Successful row updates survive restart. Job IDs and live progress are in memory;
restart requires a new `start_sync` with the same target filters. The durable
pending rows form the continuation queue, so already refreshed rows are skipped.
To advance past attempted unavailable rows, pass the returned per-channel
`next_refetch_before` map as `start_sync.refetch_before`. Values are exclusive
message-ID boundaries; they advance only for attempted rows. The client must
retain this map across a daemon restart. Omitting it retries all pending rows
newest first, including earlier failures. Both map keys (channel IDs) and values
must be positive numeric ID strings, and the argument is valid only for
`scope: "refetch"`. Advancing a boundary neither confirms nor deletes skipped
unavailable rows. Their counts remain in `remaining` and `remaining_channels`;
an exhausted selected range with pending rows elsewhere is `done_with_gaps`.
A zero remaining count proves metadata
availability only for retained rows in scope, not complete channel history.

`max_messages` and `refetch_before` exist in the current source. Old client
discovery alone cannot establish whether a deployment exposes them. After
updating, inspect a fresh `tools/list` schema for both properties. Other sync
scopes fetch the latest 50 messages per selected channel; they do not backfill
all history or support a requested time period.

## Parent-scoped thread search

`list_threads` requires a parent `channel_id` and verifies channel kinds
0 (text), 5 (announcement), 15 (forum), or 16 (media). It searches
`GET /channels/{id}/threads/search` for `active`, `archived`, or `all`;
guild-only scope, `joined`, and unsupported parent kinds return explicit
failures. Optional guild scope must match the observed parent.

The effective page limit is at most 25, returned as `effective_limit`.
Continuation uses a parent/filter-scoped consumed-offset cursor with a maximum
offset of 9975. Search results can change between requests; no immutable
snapshot or full history is promised. Permission, bot-only, rate-limit, search
index, and malformed-result failures remain errors rather than empty results.
When the final supported page still reports more upstream results, the page is
returned with `has_more=true`, `next_cursor=null`,
`search_window_exhausted=true`, and the corresponding coverage reason.
Unavailable continuation is not proof that the listing is complete.
Endpoint shape and limits do not establish compatibility with the particular
user credential; live verification is still pending. The route, page limit,
and offset bound follow the [Discord OpenAPI specification](https://github.com/discord/discord-api-spec/blob/main/specs/openapi.json)
entry for `/channels/{channel_id}/threads/search`. That specification is not
evidence of compatibility with a user credential.

## Failures and validation limits

Cache failures use RPC code `-32004`, `error_source: "cache"`, a stable code,
the RPC method as `operation`, and `retryable`. Schema mismatches are
`CACHE_SCHEMA_MISMATCH` and nonretryable; lock contention is `CACHE_BUSY` and
retryable. Startup migration errors identify the database in local diagnostics.
Structured cache data is also in the JSON-RPC `data` field, with the existing
JSON-encoded `message` retained for compatibility with deployed MCP clients.
All dispatched errors use `data.error` with `error_source`, `code`, `operation`,
`retryable`, and `message`. `operation` names the invoked RPC method;
`request_operation` retains external GET context when present. Local argument
errors use `INVALID_PARAMS` from `client`; unexpected daemon failures use
`INTERNAL_ERROR`. MCP transport failures use retryable `DAEMON_UNAVAILABLE` or
`DAEMON_TIMEOUT`; invalid RPC responses use nonretryable `RPC_PROTOCOL_ERROR`.
Discord bad requests remain nonretryable `invalid_request`, and bounded
validation details omit submitted values.
RPC errors do not expose filesystem paths. Coverage writes fail the request
instead of logging a warning and returning apparent success. Deletion of a
cached message, its FTS row, and its tombstone update is transactional.

Regression tests cover fresh and populated legacy databases, backup/WAL,
rollback, repeated opens, preserved cursors, unknown metadata, known mentions
and replies, changed-channel paging, after boundaries, edits/deletions, and
GET-only mock requests. These are local automated tests, not live Discord
verification. Mock tests additionally exercise user-authenticated around
lookup, exact selection, raw preservation, bot-only versus permissions errors,
deleted-reference null versus absence, bounded metadata refetch, and continuation
from durable pending rows. Live verification of these updated message routes,
user-authenticated threads, raw messages, attachments/events, sync jobs,
positive inbox examples, actual role membership, 429 handling, observed edits
and deletions, restart continuation, and requested-window coverage remains separate from
the local regression suite. No production refetch is performed by these tests.

## Version 3 account-reading state

Version 3 adds account inventory generations, a target ledger with fair scheduling
sequence and alternating historical/incremental directions, durable sync jobs,
immutable inbox snapshots, read-state observations, and explicit local inbox workflow
records. Version 2 databases receive a consistent backup before these additive
changes. Existing messages, FTS rows, coverage intervals and legacy cursors remain
unchanged; migration never manufactures channel discovery or confirmed history.
The current version and its migration history entry are validated on reopen.
Messages, FTS, observed coverage, and account target checkpoints for each account
sync page commit in one transaction. On restart pending/running account jobs are
marked interrupted; starting another account round uses their persisted target
checkpoints. Snapshot and workflow records survive restart. Snapshot retention is
currently unbounded and must be considered when sizing the local cache.

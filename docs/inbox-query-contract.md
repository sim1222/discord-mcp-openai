# Inbox query contract

`list_mentions` and `list_replies` assess cached observations for the current
account. An empty result is meaningful only together with classification counts
and evidence for the requested window. Neither result count nor retained
message count proves that all relevant Discord history was retrieved.

## Scope and time

`channel_id` selects one channel. If `guild_id` is supplied too, both must
match; the guild argument does not override observed associations. A known
different guild or known direct-message channel produces nonretryable
`scope_mismatch`. Rows with no verifiable guild association are counted in
`checked.messages_scope_unverified`, excluded from matches, and prevent
complete coverage. Cached rows associated with another guild are counted in
`messages_out_of_scope` and excluded. A cached association provides context,
not evidence of current membership or permissions.

Without `channel_id`, discovery uses channels known in cached channel records,
retained messages, and sync state. A guild filter narrows that inventory. It
does not discover every channel currently visible in Discord, so complete
guild-wide or account-wide coverage is not claimed.

`after` and `before` are RFC3339 instants, with timezone offsets and subseconds
preserved in comparisons. Both boundaries are inclusive. Equal bounds select
that instant; reversed bounds are invalid parameters. Omitted bounds remain
open. Invalid cached timestamps fail as cache data errors.

`refresh=true` fetches up to 100 recent messages for each candidate channel
before scanning retained rows. This is bounded retrieval, not a requested-time
backfill. Older rows requiring metadata remain pending. Identity and necessary
guild membership can require GET requests even when `refresh=false`.

## Classification and paging

Mention classification uses confirmed matches in order: `direct`, `reply`,
`role`, `everyone`. Replies require the referenced author's identity; cached
target authors can supply that evidence. Unknown authors remain unresolved.
An explicitly deleted reference is not an unresolved author lookup, and
forwarded references are not classified as replies. `list_replies` considers
only replies. Self-authored messages are not categorically excluded.

Role membership is retrieved for the message's actual guild, falling back to
retained channel context. Membership is fetched only when needed and reused
within the scan. Successfully retrieved `roles=[]` is a known empty membership;
unavailable or malformed membership does not imply that the account has no
roles. A confirmed direct or reply hit does not need a role lookup.

Both tools return matches in `mentions`, newest first. `limit` bounds returned
matches, not rows classified. All retained channel rows are scanned from a
per-channel cache snapshot. `next_cursor` is the last returned hit's ID when more matches exist;
pass it unchanged to exclude that ID and newer matches on the next page. These
are observations of a changing cache, not a frozen result snapshot.

`checked` separates:

| Field | Meaning within the requested time window |
| --- | --- |
| `messages_with_known_metadata` | Rows whose normalized metadata is available |
| `messages_classified` | Rows with a confirmed match or confirmed negative |
| `messages_requiring_refetch` | Legacy or invalid metadata requiring retrieval |
| `messages_unresolved` | Available metadata whose role/reply classification is uncertain |
| `messages_scope_unverified` | Rows whose requested guild association cannot be verified |
| `messages_out_of_scope` | Rows excluded because their observed guild differs |
| `refetch_channels` | Per-channel unknown metadata counts |

`channels_scanned` counts channels with available messages classified in the
window; `channels_skipped` counts those without such messages. Neither is a
temporal completeness certificate.
Messages, guild association, and coverage ranges are read in one SQLite read
transaction per channel, so evidence cannot come from a later cache update
than the rows classified. Different channels and later result pages can still
observe different cache states.
Reply-author cache lookups and role-membership GET requests occur outside that
target-message transaction. Their observations can be newer than the target
rows; the scan is not a single snapshot of all classification dependencies.

## Requested-window evidence

The `coverage` object contains `requested_window`, `requested_id_window`,
`checked_ranges`, `uncovered_ranges`, `cached_history`, `channels_checked`,
`channels_total_known`, `complete`, `history_complete`, `observation_scope`,
and `reasons`.

`requested_window` records normalized UTC bounds, inclusive boundary flags,
and `scope` (`time_window` or `cached_history` for two omitted bounds).
`requested_id_window` maps timestamps to snowflake IDs using the Discord epoch
and includes every possible ID in each boundary millisecond. Fractional
milliseconds deliberately require the whole boundary millisecond; this can
leave coverage incomplete even when the narrower actual interval was fetched.
Valid future timestamps beyond the 64-bit snowflake horizon are accepted. ID
bounds are clamped to the last representable millisecond; the original time
condition is preserved. `requested_id_window.clamped`, `after_clamped`, and
`before_clamped` disclose the mapping. `window_outside_snowflake_horizon`
prevents complete coverage, even if every representable ID is covered.
Pre-Discord-epoch lower bounds clamp to ID zero.

Confirmed ID intervals are normalized and intersected with the requested ID
interval to form `checked_ranges`; their complement forms `uncovered_ranges`.
Every interval has channel ID, inclusive endpoints, and
`representation: "message_id"`. ID evidence is not presented as an observed
timestamp range. `channels_checked` counts channels with any intersecting
confirmed interval, not channels whose entire request is proven.

`cached_history` separately reports each channel's unfiltered stored ID ranges,
envelope, gap flag, and classification uncertainty. A cached range outside the
window can therefore coexist with `channels_checked=0`, no checked ranges,
and a fully uncovered requested interval.

`complete=true` requires all of the following: both timestamp bounds supplied,
explicit channel scope, confirmed ID intervals covering every possible ID in
the conservative window, and no unknown metadata, unresolved classification,
or unverified guild association. Fully classified zero matches can satisfy
this condition; zero rows alone cannot. Guild-wide cached discovery does not
prove a complete channel inventory.

Reasons include `open_window_bound`, `unknown_channel_inventory`,
`no_confirmed_coverage`, `uncovered_requested_range`,
`metadata_requires_refetch`, `classification_unresolved`, and
`guild_unverified`, plus `window_outside_snowflake_horizon` for later-than-
representable timestamps. A complete retrieval interval can still have classification
uncertainty: empty `uncovered_ranges` alone does not imply `complete=true`.

`observation_scope` is `cached_observations`. Evidence describes stored
retrieval and classification, not current live edits, deletions, or permissions.
`history_complete` remains false. Migration never manufactures retrieval
intervals from retained message IDs or sets backfill complete without evidence.

## Related probes and failures

`list_changed_channels` performs a cached comparison, without HTTP. Refresh
`list_channels` when fresh activity observations are needed. `never_synced`
means no sync cursor; `newer_activity` means the observed activity ID exceeds
that cursor. New retrieval timestamps do not make historical activity new.
Neither reason detects edits or deletions by itself.

`messages_after` has a separate exclusive ID boundary and
`next_after_message_id`. Do not substitute the inbox cursor. Refetch jobs have
their own per-channel `next_refetch_before` boundaries, which the client must
retain to continue across process restarts. Other sync scopes retrieve recent
pages rather than the whole inbox time window.

Errors are machine-readable under `data.error`, mirrored as JSON in the RPC
`message` for compatibility. They identify `error_source`, stable `code`,
invoked-method `operation`, and `retryable`; external GET context may appear in
`request_operation`. Scope and argument errors are nonretryable. SQLite schema
mismatch is nonretryable `CACHE_SCHEMA_MISMATCH` with RPC code `-32004`.
Retrieval and coverage-write failures do not become successful empty results.

Local tests exercise scope intersection, inclusive time comparisons, unknown
metadata/classification, paging, and coverage arithmetic. Previously reported
live direct/reply/everyone positives remain valid observations. Acceptance of
this revision, including role positives, user-authenticated thread searches,
rate limits, edits/deletions, and restart behavior, needs separate live testing.

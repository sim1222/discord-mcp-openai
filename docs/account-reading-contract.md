# Account-wide reading contract (revision 4)

## Outcomes and acquisition basis

Discord-origin **channel/thread mention counts and read positions can be obtained**
with the current user credential through Gateway READY. The browser uses
`read_state.entries`, `mention_count`, `last_message_id`, and an account `version`.
A read-state entry's `last_message_id` is an ACK position, not the channel's newest
message ID. The exact source build, static evidence and live probe limits are in
[browser API evidence](browser-api-evidence.md). This is an internal user-client
protocol, not a documented bot read-state API.

**The guild UI badge cannot currently be returned directly.** The browser
aggregates eligible channels and joined active threads, applying permissions,
settings and resource/NSFW conditions. The live probe observed raw channel counts
summing to 8 at 2026-10-09T18:49:22Z; the user's reported 7 was not a simultaneous
screen observation. Neither number is used to select message IDs or force search
results. `discord_guild_badge_count` remains null with
`browser_aggregation_not_reproduced`.

**Known targets receive initial pages and resumable history without activity-based
starvation. Exhaustive account completion is not proven.** Discovery refreshes
all returned guilds, DMs/group DMs and their channels. Guild enumeration pages until
a confirmed short terminal page and records exhaustion or a remaining cursor;
parent thread search advances
one page per parent per refresh and saves continuation offsets. Gateway observation
also adds returned channels and threads to the ledger. Closed DMs, unlisted/private
or archived threads, index limits, permission changes and current edits/deletions
remain explicit gaps. `coverage.complete` stays false for account scope.

[Discord Get Channel Messages](https://docs.discord.com/developers/resources/message#get-channel-messages)
can return an empty array when READ_MESSAGE_HISTORY is missing. A first empty page
is therefore not confirmed zero; short/empty history pages preserve
`history_access_unknown` rather than asserting full history. Account target counts
cover retained discovered targets, not an estimate of unknown undiscovered targets.

## Existing, added, and unsupported behavior

| Capability | Status |
|---|---|
| Current identity, exact direct mention IDs, role/reply/everyone matching | Reused |
| SQLite, reply resolution, message pages, threads, refetch and cached interval coverage | Reused |
| Source-backed channel count/read position/version | Added, on-demand fresh READY; user credential only |
| Guild UI badge | Unavailable as a direct field; browser aggregation not reproduced |
| Fair account target ledger, initial page/backfill/incremental lanes, deadlines | Added |
| Atomic page checkpoint, durable progress/restart, explicit cancellation/failures | Added |
| Immutable account/query-scoped inbox paging, incoming DM/group DM | Added |
| Explicit independent local notified/snooze/action/completion records | Added; does not deliver notifications |
| Continuous Gateway events/resume, automatic edit/delete reconciliation | Not implemented; existing explicit refetch and observed tombstones remain available |
| Exhaustive private/archived thread and closed-DM enumeration | Not proven with these sources |
| Role/reply/notification facts with missing evidence | Unknown; not guessed |

## Input schema

Every Discord ID is a positive numeric string. Timestamps are RFC3339.
MCP publishes machine-readable input schemas for these tools.

| Tool | Input fields |
|---|---|
| `get_capabilities` | none |
| `get_read_state` | `guild_id?: string`, `channel_id?: string`, `refresh?: boolean = true` |
| `get_account_coverage` | none |
| `start_account_sync` | `max_targets?: integer (1..1000) = 50`, `page_size?: integer (1..100) = 100`, `refresh_inventory?: boolean = true` |
| `get_sync_progress` | `job_id: string` |
| `cancel_sync` | `job_id: string` (local account jobs only) |
| `record_inbox_state` | `channel_id: string`, `message_id: string`, `notified_at?: timestamp`, `snoozed_until?: timestamp`, `action_required?: boolean`, `action_evidence?: string`, `completed_at?: timestamp`, `completion_evidence?: string` |
| `list_mentions`, `list_replies` | Existing scope/window/limit/refresh fields; `next_cursor` is now an opaque snapshot cursor |

Guild and channel filters intersect. A mismatch or unverified guild association is
an argument error. Snapshot continuation rejects another account, scope, time
window, list mode or refresh attempt. Cursors are never snowflake IDs and must not
be substituted into `messages_after` or `messages_before`.

`record_inbox_state` replaces the entire prior explicit local state; omitted values
become unknown/unconfirmed. Required-action=true needs evidence. Completion needs
both timestamp and evidence. The authenticated account ID is supplied by the daemon,
not by the caller. Repetition upserts the same account/channel/message row. Reading
Discord, receiving a direct mention, or posting another message does not mark an
item notified, required, or complete.

## Output schema

The following structural schemas describe returned fields. Optional facts use
null, never a fabricated zero. Objects may include additional compatible fields.

```typescript
type ReadState = {
  channel_id: string;
  last_read_message_id: string | null;
  last_read_message_id_reason: string | null;
  discord_mention_count: number | null;
  computed_unread_candidate_count: number | null; // currently null
  source: "discord_gateway_ready";
  fetched_at: string | null;
  version: string | null;
  availability: "available" | "partial" | "unobserved" | "stale";
  reason: string | null;
};
type ReadStateResult = {
  account_user_id: string;
  as_of: string; fetched_at: string | null; version: string | null;
  scope: {kind: "channels", channel_ids: string[], guild_id: string | null};
  partial: boolean; complete: boolean; // read-state scope only, not history
  freshness: "fresh" | "stale";
  read_state: ReadState[];
  inventory_errors: string[]; // dropped malformed channels or unavailable guilds
  discord_guild_badge_count: null;
  guild_badge_reason: "browser_aggregation_not_reproduced";
  coverage: AccountCoverage;
};
type AccountCoverage = {
  scope: "account"; account_user_id: string; as_of: string;
  inventory_updated_at: string | null; inventory_stale: boolean;
  inventory: object | null;
  targets_total: number; enumerated: number; synced: number;
  unfetched: number; failed: number;
  targets: object[]; // channel, discovery/check times, ranges/gaps, deadlines, errors
  complete: false; reasons: string[];
};
type InboxResult = {
  snapshot_id: string; snapshot_created_at: string;
  next_cursor: string | null; has_more: boolean;
  mentions: Array<{
    message: object; // existing normalized author/content/mentions/reply/edit data
    matched_by: "dm" | "group_dm" | "direct" | "reply" | "role" | "everyone";
    matched_role_ids: string[];
    message_url: string | null;
    read_status: "read" | "unread" | "unknown";
    read_evidence: object; // observed ACK comparison, source/time/version
    reply_notification_status: "unknown";
    reply_mentions_authenticated_user: boolean | null;
    deletion_status: "unknown";
    notification_status: "notified" | "not_recorded";
    action_required: boolean | null; action_evidence: string | null;
    completion_status: "confirmed_complete" | "not_confirmed";
    workflow: object | null;
    classification_evidence: object;
  }>;
  checked: object; coverage: object;
};
```

Read status compares a retained message ID to a fresh observed ACK position;
it is not a reconstruction of notification delivery, suppression, or the UI badge.
Missing ACK positions (including numeric 0 wire sentinel), missing entries,
partial initialization and observations older than five minutes do not manufacture
current facts. A failed fresh observation is an error, not an automatic stale
zero fallback. Each `get_read_state(refresh=true)` creates a new READY observation,
so another device's prior ACK is reflected on the next successful observation.
Continuous MESSAGE_ACK events are verified as a browser acquisition path but are
not ingested by the current bounded transport.

Inbox entries, classification, workflow, read evidence and coverage are materialized
once and persisted. Later pages return that exact observation even after edits,
new messages or local confirmations. Capture reads channels sequentially and does
not claim an atomic Discord-wide time boundary. Existing inbox coverage's confirmed
intervals, inclusive requested window, unresolved classifications and reasons remain
in force. An empty incomplete cached inbox cannot prove no contact occurred.
`here` and `everyone` share Discord's `mention_everyone` flag; they cannot always be
reliably distinguished without additional raw evidence. Primary matching preserves
the existing precedence (direct/reply/role/everyone; incoming DM classification
when no stronger match applies), so one item is returned per message.

Account progress includes job/account/scope/generation/as_of, selected/attempted/
synced counts, failures, status, cancel reason, resume cursor and remaining coverage.
Statuses are pending/running/finished/partial/cancelled/interrupted/failed. Finished
means a bounded round ended, not that history or the account is complete. A second
concurrent account job is rejected. Targets use a persisted monotonically increasing
attempt sequence, so same-second jobs cannot starve quiet channels. Historical and
incremental lanes alternate while backfill is pending; quiet terminal-unknown
channels have a five-minute check deadline. Cancellation is checked at page
boundaries including after an in-flight request, and does not trigger a fallback
route. Confirmed access denials (403, 404, or bot-only operations) block automatic
target selection. Other failures preserve the lane and set a retry deadline of at
least five minutes or the supplied retry-after interval, whichever is longer.
Retries never advance a cursor without a successful transaction. Nonempty pages
record confirmed intervals through their request cursor boundary, bridging page
boundaries without assuming consecutive snowflake IDs. Short or empty history
pages do not fabricate coverage to the beginning of the channel. Historical pages
do not skip pending incremental ranges by advancing a previously initialized
incremental cursor.

## Read-only and verification limits

REST remains a typed GET operation allowlist. The separate Gateway transport has
only Identify and Heartbeat application operations. There is no arbitrary payload,
presence update, ACK, bulk ACK, notification-setting update, send or reaction API.
Normal Discord browser initialization is not used: its stores contain automatic
ACK and read-state deletion paths. Protocol handshake/close frames are connection
lifecycle, not message acknowledgements.

Unit/integration tests use synthetic accounts and cover unknown-versus-zero,
partial/stale read positions, snapshots across mutation/restart, synthetic direct-mention
classification, DM/role/reply/everyone, fair lanes, atomic rollback, errors and
cancellation. Live probes are separately reported in browser evidence; fixtures do
not claim to have reproduced live role changes, cross-device updates, reconnect
loss, edit/delete monitoring, or a complete account backfill.

## Verification result for this revision

Repository fixtures use synthetic IDs and generic names. Live acceptance results
are documented without server, organization or participant names, message URLs,
or real Discord IDs; synthetic fixtures do not claim to reproduce live messages.

The full workspace suite passed 195 tests (29 API, 102 daemon library,
4 daemon binary, 12 MCP, 48 store). `cargo fmt --check` and
`cargo clippy --workspace -- -D warnings` passed. Local socket mocks require an
environment that permits TCP/Unix socket binding; the filesystem sandbox alone
cannot run them. TDD captured failures before snapshot, workflow, fair sequencing,
atomic rollback and repeated-heartbeat implementations. Review corrections have
regression coverage for empty-history incremental cursors, paged guild inventory,
cancelled discovery/zero-target rounds, retry deadlines, preserved inventory
generations, active-daemon socket protection, v2 schema drift detection,
cursor-boundary coverage, independent incremental cursors, server-requested
heartbeat deadlines and explicit malformed/unavailable inventory evidence.
Design and implementation were reviewed with the `smart-friend-cc` skill;
confirmed review defects were fixed with regression tests.

The concrete Rust Gateway observer also succeeded against the configured account,
returning a nonpartial READY observation with Discord count fields. It printed
availability totals only; no account IDs, credentials or message bodies. This is
separate from unit tests. Selected-message direct-match/read-state-invariance and anonymized
guild raw-count observations are documented in [browser evidence](browser-api-evidence.md).
No exhaustive live first backfill or continuous event-loss reconciliation test was
performed, and a same-time guild UI badge comparison remains unverified.

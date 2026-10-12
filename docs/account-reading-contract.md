# Account-wide reading contract (revision 6)

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
| `get_read_state` | `guild_id?: string`, `channel_id?: string`, `refresh?: boolean = true`, `limit?: integer (1..100) = 50`, `cursor?: string`, `pagination_mode?: "snapshot" | "live" = "snapshot"` |
| `get_account_coverage`, `get_sync_status` | `guild_id?: string`, `channel_id?: string`, `include_details?: boolean = false`, `limit?: integer (1..100) = 50`, `cursor?: string` |
| `start_account_sync` | Strict allowlists/exclusions, independent discovery/body/message/time budgets, cached `dry_run`, and fair-round limits; see integrated safety contract below |
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
  inventory_errors: string[]; // bounded diagnostic sample
  inventory_errors_total: number; inventory_errors_omitted: number;
  target_basis: string; channels_total_known: number;
  next_cursor: string | null; has_more: boolean; limit: number;
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
  inventory_initialized: boolean;
  details_included: boolean; targets: object[]; // bounded page, empty in summary mode
  next_cursor: string | null; has_more: boolean | null;
  // Each target bounds retained ranges/diagnostics and discloses truncation.
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

## Bounded status inspection

`get_sync_status` and `get_account_coverage` return SQL aggregate counts by default;
empty detail arrays mean details were not requested, not an empty account.
`channel_id` or `include_details=true` enables detail pages, default 50, maximum 100.
Cursors are bound to account, operation, guild/channel scope and read-state target source; mismatched cursors
are argument errors. Status pages use live lexical-ID keyset ordering, not frozen
inbox snapshots. Inserts before the current cursor need a new inspection; each
page has its own observation time and never establishes account completeness.
Summary counts, inventory metadata and detail ranges are sequential cache reads;
`as_of_basis: "inspection_started_at"` is an approximate inspection time, not a
claim that all fields came from one database or Discord snapshot.

`get_read_state` defaults to immutable `pagination_mode="snapshot"`. The first
request retains the complete scoped set of rendered rows locally, then returns at
most 100 rows. Its cursor binds account, scope, fixed lexical-ID ordering and
snapshot identity; saved values, membership, coverage and observation clock do not
change when a subsequent refresh occurs. Cursors expire after 24 hours. Continuation
defaults to `refresh=false` at the MCP layer; explicit refresh is rejected.
`pagination_mode="live"` preserves the earlier source-bound keyset behavior and is
explicitly labeled. Live status pages have approximate sequential cache timestamps.
Saved stale observations have null current values, a stale reason and separate
`last_observed` evidence. Missing observations remain distinct from stale history.
`observation_values_complete` concerns only values in the retained scoped rows;
legacy `complete`, scope/history/continuous completeness remain false. Observed
partial sums are never presented as guild UI badges. If ledger membership is
unavailable, cached channel IDs provide an explicit fallback; that fallback does
not initialize or certify the account ledger.

Account job progress embeds summary coverage and a bounded failure sample with
omitted/total counters. Detail range samples disclose `ranges_truncated`.
Status/coverage inspections run outside async worker threads; unexpected task
failure becomes a structured tool error rather than fabricated empty data.

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

The full workspace suite passed 206 tests (29 API, 106 daemon library,
4 daemon binary, 13 MCP, 54 store). `cargo fmt --check` and
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

## Runtime failure investigation

A controlled fresh HTTP MCP session on the reported deployment successfully called
`get_read_state(refresh=false)` for a selected guild and `get_account_coverage`,
then repeated coverage retrieval in the same session. Responses were approximately
9.8 MB because nested coverage included the entire account ledger. A whole-cache
sync-status response was approximately 14.7 MB. No container restart, OOM or panic
was observed in the checked interval. This proves large response expansion and
healthy direct HTTP calls at that time; it does not establish the cause of an
earlier tunnel/client `Transport closed`. Cached read state is not a live unread
zero. The bounded revision has local regression coverage; deployment and tunnel
verification must be reported separately. No private names, IDs, message bodies
or log text are included in these results.

## Integrated safety and local notification contracts

Schema 4 separates `metadata_fetched_at`, `body_fetch_attempted_at` and
`body_last_success_at`. Migration never copies mixed legacy metadata timestamps
into body success. Successful empty pages have body success with null message
cursors; page success cannot prove history permission or complete history.
Message bodies, coverage and success cursors are saved in one transaction, including
legacy body pages. Counts share the body-success predicate; cache message count,
metadata count and history-complete count are separate.

Sync input deserialization rejects unknown fields at both MCP and RPC boundaries.
The canonical argument-error code remains `INVALID_PARAMS` and RPC `-32602` for compatibility; `argument_code="INVALID_ARGUMENT"` provides the explicit argument classification. Storage and unknown failures never receive that argument classification.
`start_account_sync` supports positive-ID `guild_ids`/`channel_ids` allowlists,
`exclude_guild_ids`/`exclude_channel_ids`, `include_dms`, `include_threads`,
`dry_run`, `max_discovery_http_requests`, `max_body_http_requests`, `max_messages`,
`max_duration_seconds` and `concurrency=1`. Empty explicit allowlists, conflicting
allow/exclude IDs, unknown fields and higher concurrency are rejected before job
creation or identity lookup. Unimplemented generic `exclude` remains rejected.

`dry_run=true` reads local cache only, creates no job and updates no state. If the
authenticated identity is not cached, selected counts stay null with that reason.
Unknown permissions and discovery costs remain unknown. Explicit exclusions persist
across jobs and restarts; discovery, account rounds, legacy jobs and direct body
routes respect retained restrictions. There is no implicit clearing or alternate
route. Restoring a retained restriction requires a separately authorized resolution
workflow, which this revision does not implement. Already in-flight HTTP attempts
cannot be retroactively unsent; checks apply at operation/page boundaries.

Budgets reserve the client's maximum retry attempts for each logical HTTP request,
including two requests for the existing thread adapter. Unused reservations are
conservative; returned attempt bounds are not observed request counts. Uncached
identity lookup consumes discovery reservation before job creation. Body and
discovery caps are independent, and the overall finite deadline includes rate-limit
waiting. No background broad sync is enabled; authorized clients repeat fair rounds.

`get_account_gaps` returns bounded, account/scope-bound chronological evidence,
including resolved historical failures. `record_account_gap` accepts only explicit
caller-reported withheld outcomes. Its `orchestration_approval` layer and provenance
never imply a Discord HTTP denial; HTTP/code remain null and cancellation actor is
unknown. These local records cannot interrogate or modify ChatGPT consent.

All error text and `structuredContent` are generated from one payload. Cache,
validation, Discord API, transport and unknown errors retain their classifications.
`get_member` validates role IDs; missing or malformed roles produce unknown rather
than an empty membership. Retained membership is exposed as historical evidence,
not a current role match. Cache diagnostics identify observation read, role-name
read and observation write stages, without sending database details or credentials
to clients. Runtime cause remains unknown until limited production diagnosis.

`get_inbox_state` loads the latest local workflow and revision.
`patch_inbox_state` requires `expected_version`: omitted fields remain, null clears,
and supplied values update. Stale revisions return `CONFLICT` without writes. The
older `record_inbox_state` remains full replacement and increments the revision.
Inboxes retain their original frozen workflow, with its version, so notification
workers must reread current state or claim against that revision.

`notification_outbox` has explicit `list`, `enqueue`, `claim`, `begin_delivery`,
`finish` and `import` operations. It does not send anything. Keys bind account,
channel, message, caller-defined material version and notification kind. Material
version policy belongs to the caller; explicit material-change re-notification is
required to bypass a prior notification confirmation. Claims mint opaque tokens;
lease reclaim before sending is safe, whereas expired sending becomes
`ambiguous_delivery` and is not automatically resent. `begin_delivery` rechecks
latest workflow revision, snooze, completion and notification state. Delivery/import
retain receipts; a supplied external receipt can reconcile earlier delivery.
`available_at` is a durable caller-defined quiet-hours/deadline schedule. There is
no OS/Discord notification policy change and no unconditional exactly-once promise.
List pages are live work-queue inspections, account/status bound, maximum 100 rows;
clients must inspect claim results and status before starting external delivery.

Inbox `attention_candidates` are low-confidence lexical hints for schedules,
deadlines, changes and cancellations, including announcements without reply requests.
They retain source IDs/URL/author and literal evidence. They do not normalize an
ambiguous timezone, infer attendance, infer completion or act on message instructions.
This partial extraction is not exhaustive natural-language understanding.

Gateway observations explicitly attempt close code 1000 and progress the handshake
for at most one cleanup second, independently of the observation deadline. Dropped
callers notify the socket-owning worker; no heartbeat task or permanent connection
is added. Forced process kill or a disconnected socket cannot guarantee a close
frame. `client_afk` stays disabled/unsupported (`insufficient_session_state`); no
OP3, fixed account status, settings, synthetic activity or message ACK is sent.
Official [Gateway disconnect documentation](https://docs.discord.com/developers/events/gateway#initiating-a-disconnect)
is the protocol basis, not proof of mobile push/call behavior. Production lock-screen
DM, mention and call A/B tests remain NOT RUN.

Build provenance can be supplied via `DISCORD_READER_BUILD_SHA=<checkout SHA>` to
Compose build arguments. Without it, source revision is null, never inferred from
a nearby Git checkout. Daemon `ping`/capabilities expose build metadata, capabilities
also expose actual local DB schema, and MCP initialization includes a supplied SHA
in its version. No deployment was performed. See
[integration acceptance evidence](integration-acceptance.md) for executed checks
and explicit remaining limits.


## Anonymous examples for integrated local contracts

A cache-only preview performs no network access, creates no job, and changes no
inventory or workflow state. Missing cached identity or scope evidence is reported
as unknown rather than zero.

```json
{"guild_ids":["42"],"exclude_channel_ids":["201"],"dry_run":true,"max_discovery_http_requests":12,"max_body_http_requests":9,"max_targets":3,"max_messages":100,"max_duration_seconds":20,"concurrency":1}
```

PATCH requires the current revision (`0` when absent). Omission preserves a field,
explicit null clears it, and a version conflict changes nothing.

```json
{"channel_id":"200","message_id":"300","expected_version":0,"action_required":true,"action_evidence":"explicit local review"}
```

A storage failure uses one model in text and structuredContent. HTTP status and
Discord code stay null when no Discord response was observed.

```json
{"error":{"code":"CACHE_SQLITE_ERROR","error_source":"cache","layer":"storage","operation":"get_member","retryable":false,"http_status":null,"discord_code":null,"current_membership":"unknown"},"rpc_code":-32004}
```

Coverage is scoped evidence. A successful page does not complete discovery or
history, and continuous editing/deletion coverage remains unsupported.

```json
{"page_complete":true,"scope_complete":false,"inventory_complete":false,"history_complete":false,"continuous_coverage":false,"complete":false}
```

LIVE read-state count summaries describe the returned page only. Its
`discord_scope_mention_total` is null; `page_observed_mention_sum` is a partial sum.
Fixed snapshots compute their evidence over the captured target scope and retain
that basis across continuation pages. Neither mode returns a server UI badge.


Member diagnostic fixtures independently exercise missing observation tables
(`CACHE_SCHEMA_MISMATCH`), role-name decoding and rejected observation writes
(`CACHE_SQLITE_ERROR`). These are candidate failure classes, not a finding that a
production database experienced one of them. Body reads can still succeed while
current membership remains unknown. Historical roles are never substituted for a
current membership decision.

For a later authorized image build, the Compose build argument reads
`DISCORD_READER_BUILD_SHA`. Supplying the checkout SHA exposes provenance in
capabilities and ping; leaving it unset returns unknown. This is operator-provided
build metadata and does not establish what an older running image contains.
No image was rebuilt or deployed during this implementation.


Discovery continues from a durable account/scope checkpoint rather than restarting
at the first guild. Guild pages, guild channel lists, DMs, explicit channels and
thread parents share a rotating queue. Completed cycles can refresh after fifteen
minutes; pending cycles keep their exact operations and cursors across rounds.
A thread operation needs capacity for the adapter's two logical HTTP calls; if its
retry reservation cannot fit, it remains pending with an insufficient-budget reason.
Observed HTTP 401/403, explicit permission or capability denials, and execution
withholding retain stable resource blocks across scope changes and parent renaming.
They are not automatically retried by a new round. Other nonretryable failures,
including invalid response payloads or pagination, remain recorded as failures but
do not create permanent access prohibitions; a later discovery cycle can retry them. This is a
bounded discovery cycle, not proof that Discord exposes every private or archived
thread or that whole-account body backfill has completed.

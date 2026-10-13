# Integrated reader acceptance evidence

Target: the working tree based on `b5d7a5257d414a89ef479a32e08051ccfdbe3ed0`.
The final source revision is the commit containing this acceptance record; its SHA
is reported with the delivery. Tests exercise that source tree locally. No
production deployment, broad account synchronization, message sending, calls,
or notification-setting changes were performed for these fixtures. Later
authorized bounded live checks are recorded separately below. Fixture accounts
and channels use synthetic IDs.

PASS means the specified local check was executed successfully. It does not prove
production completeness. NOT RUN means the full requirement has not yet been
verified; an existing test name or source observation alone is not a successful
execution. PASS entries below refer to executed anonymous local mocks or storage fixtures.
Remote runtime and device requirements remain explicitly unverified.

## Earlier remote runtime comparison

The local baseline and remote repository checkout were the same baseline SHA.
The remote binary build SHA was not independently available. A read-only runtime
tool catalog exposed 29 tools before this patch. Repository equality does not prove
that those tools came from the same binary. Runtime database access was unavailable,
and runtime schema inspection through the older status route was not performed.
That remote runtime schema and migration history therefore remain NOT RUN. Automatic approval
review rejected the older status-route inspection because it could expose broad
private channel detail. It was not retried through an alternate route. Local schema is 5.
The runtime tool catalog is an observation of the earlier deployment, not proof of
these newly implemented contracts.

## Reproduction commands

```sh
RUSTC_WRAPPER= cargo test --offline -p discord-api
RUSTC_WRAPPER= cargo test --offline -p discord-store
RUSTC_WRAPPER= cargo test --offline -p discord-store outbox
RUSTC_WRAPPER= cargo test --offline -p discord-store workflow
cargo test
cargo fmt --check
cargo clippy
```

The final workspace checks are recorded below after the last source change. Each
PASS row names representative tests from that executed suite. Static smart-friend
review identified issues in alternate read routes, discovery fairness, queued
revalidation, scope proof and LIVE count semantics; regression tests cover the fixes.
The review does not replace runtime or mobile-device testing.

## Acceptance matrix

| # | Requirement | Result and evidence | Limits |
|---|---|---|---|
| 1 | Checkout, runtime binary, catalog, schema comparison | NOT RUN for full match; source checkout matched and earlier catalog had 29 tools | Runtime binary SHA and DB schema unavailable |
| 2 | Migration and durable state after reopen | PASS local store suite: `version_three_mixed_timestamp_does_not_become_body_success`, `confirmations_are_deduplicated_account_scoped_and_survive_restart` | Backup migration and real isolated restart also passed; see below |
| 3 | Legacy classification, scope, time, thread regressions | PASS local: `legacy_db_inbox_refetch_and_after_survive_restart`, classification/scope/time/thread tests | Anonymous fixtures and reopened local DB; live regression remains unverified |
| 4 | Unknown safety fields reject before job or network | PASS local: `account_scope_and_budget_contract_is_strict`, `sync_preview_and_invalid_or_insufficient_budget_do_not_touch_network_or_jobs` | Both adapter boundaries tested; canonical legacy code retained with explicit INVALID_ARGUMENT alias |
| 5 | Scope and exclusions survive discovery/fetch/resume | PASS local: `persisted_exclusion_cannot_be_bypassed_by_new_default_or_allowlisted_round`, discovery cross-scope denial and per-request policy tests | Checks apply before logical requests; an in-flight internal retry cannot be retroactively unsent |
| 6 | Preview leaves state untouched and reports budget | PASS local: `cache_preview_preserves_unfetched_state_and_creates_no_job` | Permissions and uncached discovery costs remain unknown; preview is cache-only |
| 7 | Independent discovery/body budgets and stop rules | PASS local: `scope_and_retry_budget_fail_closed`, small-discovery-budget and cancellation fixtures | Real 429 and caller-side cancellation remain unverified |
| 8 | Metadata alone cannot mean body synced | PASS local store: `metadata_success_is_not_body_success_and_empty_body_keeps_unknown_cursor` | Legacy evidence is conservative |
| 9 | Empty success persists without fabricated cursor | PASS local store: `empty_account_page_updates_both_body_summaries_atomically` | Empty page is not complete history |
| 10 | Untouched quiet targets receive fair turns | PASS local: untouched-target rotation, `fair_round_finds_quiet_old_mention_and_rechecks_initial_empty_target`, persisted low-budget discovery fixtures | Whole-account live first pass NOT RUN |
| 11 | Page, scope, inventory, history completeness differ | PASS local: `inbox_refresh_is_not_a_complete_requested_period_backfill`, page/scope coverage fixtures | Continuous edit/delete coverage is not guaranteed |
| 12 | Single/guild/account counts agree, unknown retained | PASS local store: `legacy_body_success_matches_account_body_counts`, bounded aggregate fixtures | Live inventory remains partial |
| 13 | Failed/stale/denied/unknown never mean zero or complete | PASS local: read-state unknown/stale fixtures, failure gaps and cancellation fixtures | No whole-account live acquisition or current unread-zero assertion |
| 14 | Metadata discovery does not prove history permission | PASS local: `empty_page_keeps_history_unknown`, `retrieval_coverage_does_not_prove_unknown_metadata_or_inventory` | Actual permission-bit causes cannot be inferred from 403 alone |
| 15 | API, approval, cancellation, transport, storage layers differ | PASS local: `get_message_errors_distinguish_bot_only_permissions_and_unobserved`, `plain_rpc_errors_have_uniform_structured_fields`, MCP transport/cache fixtures | API tests distinguish transport/forbidden/bot-only locally |
| 16 | Unobserved approval refusal never gains HTTP 403 | PASS local: `caller_reported_gap_has_no_discord_error_and_cursor_is_scope_bound` | MCP cannot inspect unpublished caller approval state |
| 17 | Current failure preserves prior success and cursor | PASS local store: `failure_gap_survives_success_with_its_original_checkpoint` | Live failures not retried |
| 18 | Cancellation actor is unknown without evidence | PASS local: caller-reported gap and account cancellation fixtures retain unknown actor | No real caller approval or actor introspection |
| 19 | Gap and resume restriction survive reopen | PASS local store: `withheld_gap_survives_reopen_and_is_not_cleared_by_success` | Restrictions require explicit trusted caller evidence |
| 20 | Known empty channel and caller approval are distinct | PASS local: `known_empty_body_without_cursor_remains_successful_when_caller_withholds_next_read` | No approval denial was bypassed |
| 21 | Member-cache error text and structured fields agree | PASS local: `cache_errors_have_identical_text_and_structured_content` | Runtime cache failure cause still unknown |
| 22 | Member failure does not fabricate empty roles | PASS local: `malformed_member_roles_remain_unknown_instead_of_empty`, `inbox_membership_failure_and_unknown_reply_are_unresolved` | Current role applicability must remain unknown |
| 23 | Member read/write/schema diagnostics stay distinct | PASS local: `member_observation_read_and_write_failures_preserve_storage_classification`, role-name decode failure and migration fixtures | No production database initialization or destructive repair |
| 24 | Partial observed counts never become a server UI badge | PASS local: `partial_observed_counts_cannot_become_a_scope_total`, `live_read_state_page_sum_is_not_scope_total` | Browser badge aggregation remains unsupported |
| 25 | Stale values preserve last observation and stale reason | PASS local: `stale_values_keep_observation_history_and_explain_current_unknown` | Cached history is not a current value |
| 26 | Read-state snapshot survives intervening refresh | PASS local: `pinned_read_state_continuation_keeps_old_membership_and_observation`, `saved_pages_pin_values_scope_and_clock_and_reject_reuse` | Bounded authorized live refresh also passed; see below |
| 27 | Existing inbox snapshot remains immutable | PASS local: `inbox_snapshot_freezes_pages_and_checks_scope` | Workflow eligibility is checked separately before delivery |
| 28 | PATCH omission/null and CAS conflict | PASS local store: `patch_preserves_omissions_distinguishes_null_and_rejects_old_revision` | Full replacement intentionally still resets omitted state |
| 29 | Competing workers, crashes, expiry, receipt import | PASS local outbox: `independent_workers_cannot_both_claim_the_same_material_notification`, `claims_survive_restart_and_expired_sending_never_blindly_retries` | No external service exactly-once claim |
| 30 | Material edits renotify; ambiguous delivery does not blindly retry | PASS local outbox: `imported_dedup_and_material_edit_are_independent`, ambiguous crash fixture | Material-version policy belongs to caller |
| 31 | Important schedules and evidence; unrelated post does not resolve | PASS local lexical fixtures: `reply_unneeded_schedule_still_has_an_evidence_candidate`, `cancellation_and_missing_timezone_are_not_normalized_into_a_plan`, `unrelated_followup_has_no_completion_or_participation_decision` | Partial lexical hints require review; no normalized event or participation inference |
| 32 | Quiet-hours restart and latest pre-send state | PASS local: deferred/reclaim fixtures and `queued_revalidation_reschedules_after_snooze_clear_and_rejects_stale_writers` | Scheduling timezone and urgency policy belong to caller |
| 33 | Existing atomic commit/rollback/cancellation survive | PASS local store: `page_storage_failure_rolls_back_messages_and_checkpoint_together`, `cancellation_and_checkpoints_survive_reopening` | Graceful isolated process restart passed; forced kill NOT RUN |
| 34 | Resume/backfill boundaries do not jump or advance on failure | PASS local: account cursor/backfill/cancellation fixtures and store atomic page rollback fixtures | Local store boundary fixtures passed |
| 35 | Edit/delete limits exposed honestly | PASS source/capability fixtures: continuous coverage false; edits require explicit refetch and deletions require evidence | Pre-observation deletion and disconnected edits may be missed |
| 36 | Read-only paths exclude ACK/send/settings mutations | PASS API local tests: `every_request_uses_get`, `outbound_operations_cannot_express_ack_or_presence_updates` | Bounded authorized comparison passed; see below |
| 37 | READY/error/timeout/cancel/peer close get finite cleanup | PASS API wire mocks: `observation_errors_attempt_normal_close`, `timeout_and_caller_abort_attempt_normal_close`, `peer_close_is_flushed_and_worker_ends` | Force kill or already-broken socket cannot promise frame delivery |
| 38 | Wire close frame explicitly uses code 1000 | PASS API: `ready_and_protocol_rejection_close_with_code_1000` | `close(None)` is not treated as code 1000 |
| 39 | Worker/heartbeat terminate; no permanent connection | PASS API: `unresponsive_peer_cannot_extend_cleanup_indefinitely`, cancellation fixture checks EOF | Runtime shutdown can prevent async cleanup |
| 40 | Experimental AFK preserves own session or fails closed | NOT RUN experiment; source leaves OP3 unsupported | Insufficient trustworthy own-session data; no OP3 sent |
| 41 | No fixed status, settings, artificial activity, read ACK | PASS source and API operation-boundary test | AFK experiment not enabled |
| 42 | Authorized locked-device DM push and call A/B | NOT RUN | Requires separately authorized people, targets, times and device conditions |

## Gateway and local workflow boundaries

Gateway connection plus observation has a 30-second budget. Cleanup has its own
one-second budget and attempts an explicit code-1000 close followed by a bounded
handshake. Dropping or aborting the caller closes its result receiver; the worker
then ends observation and performs cleanup. Heartbeats run in that worker's read
loop, with no separately persistent heartbeat task. Cleanup logs only fixed
operation/outcome categories. Client AFK remains unsupported; no OP3, fixed status,
REST setting change, fake input, or application activity was added.

Outbox delivery is local bookkeeping only. `claimed` expires safely before send
begins; `sending` expiry becomes `ambiguous_delivery`, requiring receipt reconciliation
or verified not-sent evidence. An opaque claim token changes on every ownership
transfer. Completion, snooze, revision, and notification eligibility are checked
again when delivery begins. External changes after that boundary remain a caller
coordination concern. A persisted snapshot does not authorize a notification.

## Attention candidate limits

The inbox can attach conservative lexical candidates for schedules, deadlines,
changes, and cancellations, including announcements that do not request a reply.
Every hint carries message and sender IDs, the original message URL, a bounded
excerpt, exact textual markers, and explicitly unresolved interpretation. Date/time
and known timezone words remain literal hints; no date, timezone, location, urgency,
participation, or completion is guessed. Confidence is low and the source is a
lexical rule. This detector is partial and may produce false positives or miss
messages, attachments, forwarded content, or languages without supported markers.
It does not modify workflow state or authorize notification, sending, or attendance.

Outbox queue paging regression `live_queue_pages_skip_delivered_history_and_do_not_repeat_rows`
uses delivered-history fixtures ahead of due entries, bounded two-row continuation,
status filtering, and account isolation. Queue pages are live and must be restarted
when membership changes behind the cursor; they are not fixed snapshots.

The local regression `delivery_summary_exposes_prior_delivery_and_ambiguity_without_receipts`
checks imported historical evidence, ambiguous delivery, account isolation, and
receipt omission. Historical delivery is not relabelled as current-material
notification, and an absent local record is not treated as confirmed unnotified.

Queued revalidation is exercised by
`queued_revalidation_reschedules_after_snooze_clear_and_rejects_stale_writers` and
`queued_revalidation_handler_keeps_material_key_after_workflow_update`. They retain
the material key after clearing snooze, require current workflow and entry versions,
and reject stale writers or any claimed/sending/ambiguous/delivered state.

```json
{
  "operation": "revalidate",
  "key": {
    "channel_id": "2",
    "message_id": "3",
    "material_version": "v1",
    "notification_kind": "schedule"
  },
  "expected_entry_version": 1,
  "expected_workflow_version": 2,
  "available_at": "2026-10-13T10:00:00Z"
}
```

These versions are examples; callers must obtain their current versions before
issuing the explicit update. Revalidation performs no external send and does not
resolve a prior ambiguous delivery.


## Final delivery and deployment limits

The tested source tree includes schema-5 migration, 34 published MCP tools, strict
input schemas, bounded paginated state, local workflow/PATCH/CAS and outbox records.
The older runtime catalog had 29 tools and does not verify this implementation.
No deployment SHA exists for this change because deployment was not performed.
Server UI badge aggregation and client-AFK remain unsupported. A full real-account
first pass, continuous edit/delete capture, runtime read-position invariance and
mobile push/call recovery remain unverified; local PASS results do not imply them.

A later authorized smoke test can inspect ping/build/schema and tools/list first,
then one specifically permitted member and body target. Do not retry retained
refusals or launch broad synchronization without a separately confirmed scope and
budget. Mobile A/B requires designated senders/recipients, timing and device
conditions; no test sends or calls are included in this implementation.


## Executed quality gate

The complete workspace suite passed **286 tests**: API 34, daemon library 147,
daemon binary 4, MCP 19, store 82; doc tests had no failures. The executed commands
were `RUSTC_WRAPPER= cargo test --offline`, `cargo fmt --all --check`, and
`RUSTC_WRAPPER= cargo clippy --offline --all-targets -- -D warnings`.
The local test server sockets required sandbox permission; no production endpoint
was contacted. These results cover anonymous fixtures and mock services, not an
actual mobile-device or whole-account acceptance run. Rows 1, 40 and 42 remain
NOT RUN for their full requirements; the other rows report local/source evidence.

Docs and test sources were checked for the supplied private server/organization
names, real account/target IDs and deployment address; none were found. Diagnostic
and schema examples use synthetic identifiers.


## Authorized bounded local runtime verification

On 2026-10-13 JST, the owner authorized using the local database and token.
The stopped data volume was mounted read-only to copy it; an independent SQLite
backup was used for all writes. The original database was not migrated or reset.
No production deployment, broad sync, message sending, calling or setting changes
were performed. Private IDs, names, message contents and credentials are excluded
from this record.

The initial runtime was built from `0b3e64e50eaae385a648e2640224301a0efe5bdd`.
`get_capabilities` matched that source revision, database schema 5 and tool
contract revision 6; `tools/list` exposed 34 tools. Migration of the local legacy
backup from schema 0 to 5 passed, and SQLite quick-check returned `ok`.

Executed through the HTTP MCP adapter with a real user token:

- PASS: authenticated identity, current self membership and a known single
  message; cached mention classification matched the authenticated ID as direct.
- PASS: cached read-state and account-coverage calls returned without transport
  closure; sync status returned aggregate-only output. Incomplete account
  coverage remained false, and the server UI badge remained unsupported.
- PASS: two short Gateway observations around a REST message read returned the
  same observed last-read position and mention count for the selected channel.
  This is a bounded observation, not proof across every account or client action.
- PASS: an existing read-state snapshot continuation was byte-equivalent as JSON
  after fresh observations changed the live observation generation.
- PASS: cache-only preview made no job or state changes; invalid safety arguments
  created no jobs. PATCH preserved omitted workflow values; stale CAS returned
  matching text and structured errors. Repeated enqueue deduplicated, and a
  competing worker could not acquire the existing claim. These local mutations
  affected only the backup and sent no external notification.

The first limited sync exposed a preprocessing performance defect: candidate
listing checked every retained target individually before scope selection. The
backup had over ten thousand targets, so even a single-target round could consume
its time budget before fetching. The regression fixture and final retest below
separate this finding from Discord permission or transport failures.

Full-account first-pass completion, server UI badge reproduction, continuous
edit/delete detection and locked-phone push/call delivery remain NOT RUN or
unsupported as described above. The earlier remote runtime SHA and cache error
cause remain unverified; local success does not prove the remote deployment.


Final verification of the scoped-round correction used the same backup and token,
with inventory refresh disabled, one channel, one target, one message, a body HTTP
attempt cap of three and a 15-second execution budget. PASS: the round finished in
1.1 seconds with one attempted target, one synchronized target, one saved message
and zero failures; `complete` remained false. The prior unmodified binary consumed
its 60-second budget without attempting a body page. No unrelated bodies were
requested. This demonstrates the bounded case, not whole-account completion.

PASS: after restarting both isolated daemon and MCP processes with the corrected
binary, the workflow, outbox claim, caller-reported gap, saved read-state snapshot
and successful sync checkpoint remained intact. The corrective runtime explicitly
identified itself as the base revision with a local-verification suffix, rather
than claiming the modified tree was the unchanged base commit.

The correction removes candidate-listing per-target checks and retains the check
before an actual attempt. Unknown-scope candidates do not consume retrieval slots.
Discovery and retrieval share an absolute deadline; synchronous SQLite work can
still exceed the deadline before returning. A subsequent check pauses retrieval
without inventing completion or advancing the cursor.

Regression checks: `candidate_listing_defers_individual_scope_checks_until_selected_round`,
`unknown_scope_does_not_starve_next_allowed_candidate`, and
`expired_round_budget_pauses_before_attempt_or_http_reservation`. The first and
third failed against the previous implementation before the correction. Existing
inherited-exclusion and restrictions-during-round checks also passed.

The post-review regression
`synchronous_state_work_cannot_start_http_or_save_after_deadline` failed before
adding checks after synchronous progress storage and immediately before page
storage. It verifies that a deadline crossed during synchronous state work prevents
HTTP start or checkpoint save, respectively. Already-started SQLite transactions
remain non-preemptible.

Final quality checks passed: 290 Rust tests, `cargo fmt --check`, and
`cargo clippy --all-targets -- -D warnings`. Socket-based fixtures ran outside the
filesystem/network sandbox after the sandbox correctly denied local bind calls.
The final smart-friend read-only review found no further actionable regressions.
The test/docs privacy scan found no real server or organization names or supplied
real account/channel/message IDs. No token, database copy or private live-response
artifact is included in the commit.


## Member role-name follow-up

On 2026-10-13 JST, a reported storage failure at `role_name_read` prompted a
nullable-label regression. The store accepted nullable role labels but decoded an
existing row as a non-null string; SQL NULL therefore produced an invalid-column
SQLite error. This is a reproduced source defect, not proof of the remote database's
actual error or currently deployed revision.

Before the fix, the anonymous store and daemon fixtures failed for existing NULL
labels. After the fix, absent, NULL and named labels pass while malformed BLOB
labels still fail. Current role IDs remain observed when only their label is unknown.
Tests: `role_name_accepts_existing_null_name_without_hiding_invalid_types` and
`member_with_cached_null_role_name_keeps_observed_role_ids`.

The adapter supplies top-level payload aliases from the canonical nested error.
`cache_errors_have_identical_text_and_structured_content` checks serialized
`structuredContent.error_code`, all four aliases, text equality and the absence of
argument classification for storage errors. Validation and timeout fixtures preserve
their own codes and retryability.

Authorized bounded real-token HTTP MCP checks passed on another read-only volume
copy and independent backup: initial current membership, current membership with a
NULL role label injected into the backup, and a BLOB-label error injected into the
backup. The injected error returned identical text/structured content, matching
`CACHE_SQLITE_ERROR` aliases, `role_name_read`, RPC -32004, unknown current membership
and historical membership. The actual wire payload contained no `INVALID_ARGUMENT`.
The private database and processes were removed; the original database and token
were untouched. The corrected source tree was identified with a verification suffix.
No deployment or wide synchronization was performed. The external reported wrapper
and remote runtime remain unverified pending sanitized envelope/build evidence.

Follow-up quality checks passed: 292 Rust tests, formatting, all-target Clippy with
warnings denied, and the smart-friend read-only review. Anonymous tests/docs retain
no supplied real server names, organization names, account IDs or message IDs.

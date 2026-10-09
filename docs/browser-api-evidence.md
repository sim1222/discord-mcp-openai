# Discord Web badge / read-state static investigation

The static investigation used public, unauthenticated downloads only, without executing the downloaded application. Its findings are separated below from subsequent authenticated, controlled Gateway/GET probes. Those probes used the existing credential and sent no ACK, messages, reactions or notification-setting changes.

Source: https://discord.com/assets/web.843cc7edc28c426c.js . User supplied HTML identifies stable build 634304, VERSION_HASH 17e01cf978c3c10a5688429a38db59a4926e8982, API_VERSION 9. File: `/tmp/discord-browser-api/web.843cc7edc28c426c.js`, size 12,255,195 bytes, SHA-256 `3b6bf9be531555061024ba6af6a8abf5afd70f521ecdd30bcdd7369f23d6473e`. The file is ASCII; UTF-8 byte and character offsets coincide. Offsets below are zero-based and refer to this exact file.

## Direct answer

- Browser code consumes **Discord-origin channel/thread `mention_count` and last ACK message ID from Gateway READY `read_state`**. Another client ACK can arrive as Gateway MESSAGE_ACK, carrying `mention_count` and `version`. This is evidence of an acquisition path for user-client authentication, not proof that the repository's current authentication can successfully receive it.
- The **guild UI badge is computed in the browser**, from eligible channel and active joined-thread counts and user settings. A single REST “guild badge 7” getter was not found. Distinguish raw Discord channel counts, reconstructed guild UI count, and independently computed inbox candidates.
- No read-only REST read-state getter was found in the inspected main bundle. The only literal `/read-states` occurrence is the mutation `/read-states/ack-bulk`. Absence in this bundle is not proof that no getter exists elsewhere (lazy bundles, WASM, server).
- READY read-state is not a full history/channel coverage certificate. The UI excludes inactive/unjoined threads, some gated/resource channels, settings-dependent channels, and can use partial READY plus cached state. It cannot establish exhaustive first acquisition.

## Evidence offsets

| Byte offset | Evidence |
|---:|---|
| 9,151,133 | webpack module **573163**, exports internal read-state class, store, mention predicate |
| 9,176,437 | `ReadStateStore` class declaration |
| 4,841,828 | dispatch-ready maps wire `e.read_state` to `readState` |
| 9,181,671 | CONNECTION_OPEN loads `readState.entries`; clears old state only when neither cache nor `partial` applies |
| 2,117,661 | cache manager retains `readState.version`; updates it from MESSAGE_ACK / CHANNEL_PINS_ACK event versions |
| 4,844,766 | MESSAGE_ACK wire translation to channelId/messageId/manual/newMentionCount/version |
| 9,175,816 | MESSAGE_ACK store handler: manual path rebuilds with supplied count; nonmanual path locally ACKs when ID differs |
| 9,178,085 | public store count getter applies `canHaveMentions` and an imported predicate that caps some counts to 1; raw field and exposed UI count can differ; module 343328 (3,214,698) only caps a particular user DM, not guild channels |
| 8,971,025 | webpack module **458294**, GuildReadStateStore |
| 8,976,136 | READY optimization uses `entries.length<500` and entry mention counts to select guilds for recomputation; this is an optimization, not a truncation contract |
| 8,974,021 | guild recomputation iterates basic channels and active joined threads |
| 8,980,768 | guild getMentionCount = highImportanceMentionCount + lowImportanceMentionCount |
| 9,161,475 | low importance is read-state flags bit 4 |
| 9,185,142 | MESSAGE_CREATE handler; see details below |
| 6,000,706 | webpack module **451919**, direct/everyone/role mention predicates |
| 9,154,046 | private-channel predicate: private and not guild/category/channel muted |
| 9,265,984 and 9,266,043 | suppressEveryone / suppressRoles read guild settings flags |
| 654,574 | reply send options can set allowedMentions.replied_user=false |
| 636,545 | GET recent mentions route and query |
| 254,259 | BULK_ACK constant `/read-states/ack-bulk` |
| 9,167,962 | per-message ACK POST |
| 9,169,407 | read-state delete uses DELETE channel messages/ack |
| 7,232,155 | read-state enum CHANNEL=0, GUILD_EVENT=1, NOTIFICATION_CENTER=2, GUILD_HOME=3, GUILD_ONBOARDING_QUESTION=4, MESSAGE_REQUESTS=5, CONJURING_PROJECT=6 |

## Derived wire schema

Preserve missing fields; do not reproduce UI's `mention_count ?? 0` fallback as a confirmed zero.

```typescript
type Snowflake = string;
type ReadyReadState = {
  version?: number;       // account read-state version, not message sequence
  partial?: boolean;     // browser merges with cached state for partial initialization
  entries: Array<{
    id: Snowflake;
    read_state_type?: number; // omitted -> CHANNEL (0)
    mention_count?: number;   // CHANNEL entry
    last_message_id?: Snowflake | null; // CHANNEL entry ACK position, NOT latest channel message
    flags?: number;
    last_viewed?: number;
    last_pin_timestamp?: string | null;
    badge_count?: number;          // non-channel entry
    last_acked_id?: Snowflake | null; // non-channel entry
  }>;
};
type MessageAck = {
  channel_id: Snowflake;
  message_id: Snowflake;
  manual?: boolean;
  mention_count?: number;
  version?: number;
};
```

At READY, non-channel entries map badge_count -> mention_count and last_acked_id -> last_message_id. Browser overwrites `_mentionCount`, `flags`, `lastViewed`, and ACK ID, with special guessed/fallback ACK positions in some channel cases. Latest channel message comes separately from channel / passive timestamp fields. Preserve original wire ACK position separately from inferred positions.

The cache manager assigns read-state version from CONNECTION_OPEN, then updates from ACK event version when present. It stores the account version with cached states. This supports explicit source/fetched_at/version fields, but the browser snippet does not establish monotonic version semantics or a REST conditional request.

## Badge and suppression behavior

Guild aggregation includes high and low importance counts. Non-channel scheduled-event read state contributes to unread indications; guild `getMentionCount` is channel/thread aggregate, not a direct guild wire badge field. Total application mention count sums only high importance, while individual guild count adds both; different badges can have different arithmetic.

Guild channel consideration depends on permissions, NSFW access, guild-resource channel flag, opt-in behavior, muted channel/category/guild, unread setting, and whether a positive mention exists. Muting suppresses the ordinary unread indicator but does not universally erase direct mention counts. Thread aggregation uses **active joined** threads. ReadStateStore can suppress inactive or unjoined threads; this does not mean their history cannot be read.

On MESSAGE_CREATE, browser ignores blocked/ignored authors and a specific GROUP_DM recipient-remove system event for mention increments. Own messages trigger a local ACK path (browser behavior only; an independent read-only collector must not copy that). Direct user mention, enabled everyone/here, matching current member roles, or unmuted private-channel messages yield high importance. `mentionOnAllMessages` can produce low-importance increments for all-message thread notification settings or nonvocal unmuted all-message channels. `isMentionLowImportance` uses bit 4 and setter preserves importance when a count already exists.

Mention predicate uses message.mentions IDs directly; everyone requires suppressEveryone=false; roles require suppressRoles=false plus known channel, guild, and member role intersection. A missing member makes browser predicate false, whereas MCP contract should expose unknown rather than a verified nonmatch. The predicate does **not** inspect reply target author: mere reply-to-self is not a notification. Reply send options explicitly permit replied_user=false. Preserve reply relationship independently from mention notification evidence.

## Read-only REST alternative present

`GET /users/@me/mentions` uses query `before`, `limit`, `guild_id`, `roles`, `everyone`, `feature` (offset 636,545). Returned body is a message array, with pagination assumed from result size. It is a recent-mention retrieval source, **not** read-state / unread badge count. Browser also has a DELETE recent mention operation; it must be excluded. No count field proves a simultaneous guild badge value.

## Side-effect warning grounded in code

Do not import/activate Discord's normal store or navigate channels to acquire read state. Store focus/channel selection/load logic can schedule automatic ACKs. READY schedules aged read-state cleanup after ten seconds, which can DELETE persisted server read state. Executing downloaded JS is unnecessary for this research.

A separate collector should decode inbound READY/MESSAGE_ACK as passive data and allow only explicitly approved outbound read operations. Exclude MESSAGE_ACK POST, BULK_ACK POST, pins ACK, guild-feature/non-channel ACK, recent mention DELETE, read-state DELETE, message sends and settings mutations. Gateway has outbound controls beyond HTTP methods; allowlist required lifecycle opcodes/operations at transport boundary.

## Verified vs unverified

Verified statically: field consumption, event translation, cache-version propagation, candidate mention arithmetic, guild aggregation, mutation call sites, recent mentions GET signature, reply-notification suppression option.

Not verified: successful user-token or OAuth/bot access under this repository's present auth; actual READY shape for this account; simultaneous reported guild badge observation; exact GUI badge after all settings; read-state visibility for archived/private threads; Gateway authentication without side effects; live before/after unchanged ACK; cross-device runtime propagation. These must be reported unavailable/unverified with null until observed. Event consumption alone cannot guarantee exhaustive history coverage or repair offline edit/delete losses.

## Exact active joined thread evidence

Module 863005 starts at byte 7,793,602. Its builder obtains threads from active thread index 970278, then only inserts into joined set when ThreadMemberStore.joinTimestamp(thread_id) is nonnull. Unjoined threads go to a separate map. Updates require active-index membership and the same join timestamp check. Getter getActiveJoinedThreadsForGuild at 7,798,209 returns this joined map. Guild aggregation at 8,974,021 consumes this map. Thus a readable thread with positive raw read state can still be excluded from guild badge if not active/joined. Thread history coverage must enumerate more than this UI set.

NSFW helper module 874850 at 2,771,724 requires current user nsfwAllowed or neither channel NSFW nor guild NSFW. Guild resource flag is 128 (module746080). Positive raw mention entries need channel type/flags, access/basic permissions, NSFW allowance, opted-in state and active/joined thread context to reconstruct guild UI; count summation alone cannot do it.

## Root agent runtime probe (separate from this agent's static investigation)

Root reports a controlled current-auth Gateway READY probe succeeded and locally saved a minimal summary at `/tmp/discord-browser-api/ready-probe-summary.json`. At 2026-10-09T18:49:22Z, read_state.partial=false and version=5103418 with 7,713 entries. An anonymized guild raw channel mention sum was 8, while the user's reported badge is 7. These are different observations; the badge was not observed simultaneously and full filter inputs were not in this minimal summary. Do not claim badge equality or adjust message results to 7. Root owns runtime authentication and outbound operation validation. This probe does not establish account-wide initial history coverage, no-ACK before/after invariance, or complete UI badge reconstruction. No account data from the summary was sent to an external reviewer by this agent.

## Selected-channel read-only observation

At 2026-10-09T19:03:32Z a controlled probe observed the user-selected
message and matched its mention ID to the authenticated user ID (`matched_by=direct`).
It used Identify/Heartbeat and a fixed GET message-list request. The selected
channel's read-state entry was observed both before and after retrieval and was
unchanged; account read-state version was also unchanged. This is an observed
selected-channel check, not a controlled proof for every channel or every Discord
client behavior. Concurrent actions by other clients were not controlled.
The initial Python GET attempt failed; retry using the reader User-Agent succeeded.
No browser store was executed, no ACK operation was sent, and no message body or
credential was recorded in the probe report.

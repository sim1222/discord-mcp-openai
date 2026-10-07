# Bot-only endpoints are replaced, not retried

Several Discord REST endpoints that would be the "obvious" way to list threads
or search messages reject user-account tokens with HTTP 403 / code 20002
("Bot-only"). The reader routes around them with user-account-accessible
endpoints and declares the difference in `get_capabilities`, instead of
surfacing raw 20002 errors to callers.

## Context

`GET /guilds/{id}/threads/active` (used by the original `list_threads`) and
`GET /guilds/{id}/messages/search` are bot-only. With a user token they fail
with 403/20002, which callers cannot act on. Users hit exactly this and asked
for either working user-auth methods or explicit capability declarations.

## Decision

- `list_threads` uses `GET /channels/{id}/threads` (per parent channel) and
  `GET /guilds/{id}/threads/search`, both user-account accessible, and covers
  active, archived and joined threads.
- Server-side message search is exposed as `search_server_side`, channel-scoped
  (the only shape user accounts can call), with its limitation stated in the
  result and in `get_capabilities`.
- `get_capabilities` documents, per method, whether it works under the current
  credential kind, so callers can adapt before calling.

## Consequences

- Guild-wide thread listing is a per-channel operation under a user account;
  `list_threads` accepts `channel_id` for exactly this reason.
- A bot token would unlock the simpler guild-wide endpoints; the capability
  table should change if credential kind ever changes.

## Amendment (2026-10-07)

The earlier thread-route and user-accessibility claims in the Decision section
are superseded. `list_threads` now uses only `GET /channels/{id}/threads/search`
after checking the parent channel type. It exposes offset paging with an
effective page size of at most 25 and offsets up to 9975. Guild-only scope and
the `joined` filter return explicit `unsupported_scope` and
`unsupported_filter` errors. The final valid search page is retained even when
the offset bound prevents continuation.

The official API specification establishes this route and its query limits,
not user-token compatibility. That compatibility needs separate live
acceptance; no route is described as proven merely because local mocks pass.
See the [current retrieval contract](../database-upgrades.md) and
[official API specification](https://github.com/discord/discord-api-spec/blob/main/specs/openapi.json).

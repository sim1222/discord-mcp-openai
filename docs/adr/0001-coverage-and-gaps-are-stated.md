# Coverage and gaps are stated, not inferred

The reader never claims "no new messages" from an empty result alone. Every
fetch records a coverage range (contiguous message IDs confirmed from Discord)
and every listing reports `covered_from`/`covered_to`/`has_gaps`. A gap is a
hole we know we never fetched; it is reported as unknown, never as empty.

## Context

The original design fetched a channel's recent messages and searched a local
cache. That made "0 results" ambiguous: it could mean genuinely nothing, or
that the relevant channel was never fetched, or that a connection failure was
silently recorded as emptiness. Users explicitly asked for the distinction.

## Decision

- The SQLite cache stores coverage ranges per channel (merged when contiguous,
  separate when gapped) and a per-channel sync cursor that survives restarts.
- Every read tool that can return "nothing" also returns its coverage envelope.
- Failures (403, bot-only 20002, transport errors, rate limits) are reported
  per-channel as structured errors with `error_source`/`retryable`, and are
  never folded into an empty result or a blanket "channel is forbidden".

## Considered options

- Trust Discord's search endpoint for mentions: rejected — it is bot-only /
  channel-scoped for user accounts, and 0 hits there cannot prove absence
  across servers.
- Silent best-effort caching: rejected — it is exactly the ambiguity being
  fixed.

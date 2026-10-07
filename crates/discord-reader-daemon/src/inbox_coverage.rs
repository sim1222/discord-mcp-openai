//! Assess requested inbox windows from confirmed ID ranges and classification evidence.

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::BTreeSet;

const DISCORD_EPOCH_MILLIS: i64 = 1_420_070_400_000;
const SNOWFLAKE_SEQUENCE_BITS: u128 = (1 << 22) - 1;

pub(crate) struct ChannelEvidence {
    pub channel_id: String,
    pub ranges: Vec<(String, String)>,
    pub messages_classified: u64,
    pub messages_requiring_refetch: u64,
    pub messages_unresolved: u64,
    pub guild_unverified: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CoverageError {
    #[error("invalid confirmed message ID interval")]
    InvalidRange,
    #[error("after must not be later than before")]
    InvalidWindow,
}

pub(crate) fn evaluate(
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    channels: &[ChannelEvidence],
    explicit_channel_scope: bool,
) -> Result<Value, CoverageError> {
    if after
        .zip(before)
        .is_some_and(|(after, before)| after > before)
    {
        return Err(CoverageError::InvalidWindow);
    }
    let after_boundary = after.map(SnowflakeBoundary::from_time);
    let before_boundary = before.map(SnowflakeBoundary::from_time);
    let requested = IdInterval {
        from: after_boundary.map(|boundary| boundary.start).unwrap_or(0),
        to: before_boundary
            .map(|boundary| boundary.start | SNOWFLAKE_SEQUENCE_BITS)
            .unwrap_or(u64::MAX as u128),
    };
    let mut reasons = BTreeSet::new();
    if after_boundary.is_some_and(|boundary| boundary.beyond_horizon)
        || before_boundary.is_some_and(|boundary| boundary.beyond_horizon)
    {
        reasons.insert("window_outside_snowflake_horizon");
    }
    if after.is_none() || before.is_none() {
        reasons.insert("open_window_bound");
    }
    if !explicit_channel_scope {
        reasons.insert("unknown_channel_inventory");
    }
    if channels.is_empty() {
        reasons.insert("no_confirmed_coverage");
    }
    let mut checked_ranges = Vec::new();
    let mut uncovered_ranges = Vec::new();
    let mut cached_history = Vec::new();
    let mut channels_checked = 0;
    for channel in channels {
        let ranges = IdInterval::normalize(&channel.ranges)?;
        let checked: Vec<_> = ranges
            .iter()
            .filter_map(|range| range.intersection(requested))
            .collect();
        let uncovered = requested.subtract(&checked);
        let mut channel_reasons = BTreeSet::new();
        if checked.is_empty() {
            channel_reasons.insert("no_confirmed_coverage");
        } else {
            channels_checked += 1;
        }
        if !uncovered.is_empty() {
            channel_reasons.insert("uncovered_requested_range");
        }
        if channel.messages_requiring_refetch > 0 {
            channel_reasons.insert("metadata_requires_refetch");
        }
        if channel.messages_unresolved > 0 {
            channel_reasons.insert("classification_unresolved");
        }
        if channel.guild_unverified {
            channel_reasons.insert("guild_unverified");
        }
        reasons.extend(channel_reasons.iter().copied());
        checked_ranges.extend(
            checked
                .iter()
                .map(|range| range.describe(&channel.channel_id)),
        );
        uncovered_ranges.extend(
            uncovered
                .iter()
                .map(|range| range.describe(&channel.channel_id)),
        );
        cached_history.push(json!({
            "channel_id": channel.channel_id,
            "representation": "message_id",
            "from_id": ranges.first().map(|range| range.from.to_string()),
            "to_id": ranges.last().map(|range| range.to.to_string()),
            "has_gaps": ranges.len() > 1,
            "ranges": ranges.iter().map(|range| range.describe(&channel.channel_id)).collect::<Vec<_>>(),
            "history_complete": false,
            "messages_classified": channel.messages_classified,
            "messages_requiring_refetch": channel.messages_requiring_refetch,
            "messages_unresolved": channel.messages_unresolved,
            "reasons": channel_reasons,
        }));
    }
    Ok(json!({
        "requested_window": {
            "after": after.map(|time| time.to_rfc3339()),
            "before": before.map(|time| time.to_rfc3339()),
            "after_inclusive": true,
            "before_inclusive": true,
            "scope": if after.is_none() && before.is_none() { "cached_history" } else { "time_window" },
        },
        "requested_id_window": {
            "representation": "message_id",
            "from_id": after.map(|_| requested.from.to_string()),
            "to_id": before.map(|_| requested.to.to_string()),
            "inclusive": true,
            "boundary_precision": "whole_millisecond",
            "clamped": after_boundary.is_some_and(|boundary| boundary.clamped) || before_boundary.is_some_and(|boundary| boundary.clamped),
            "after_clamped": after_boundary.is_some_and(|boundary| boundary.clamped),
            "before_clamped": before_boundary.is_some_and(|boundary| boundary.clamped),
        },
        "observation_scope": "cached_observations",
        "checked_ranges": checked_ranges,
        "uncovered_ranges": uncovered_ranges,
        "cached_history": cached_history,
        "channels_checked": channels_checked,
        "channels_total_known": channels.len(),
        "complete": reasons.is_empty(),
        "history_complete": false,
        "reasons": reasons,
    }))
}

#[derive(Clone, Copy)]
struct IdInterval {
    from: u128,
    to: u128,
}

#[derive(Clone, Copy)]
struct SnowflakeBoundary {
    start: u128,
    clamped: bool,
    beyond_horizon: bool,
}

impl SnowflakeBoundary {
    fn from_time(time: DateTime<Utc>) -> Self {
        let milliseconds = time.timestamp_millis().saturating_sub(DISCORD_EPOCH_MILLIS);
        let horizon = (u64::MAX >> 22) as i64;
        Self {
            start: (milliseconds.clamp(0, horizon) as u128) << 22,
            clamped: !(0..=horizon).contains(&milliseconds),
            beyond_horizon: milliseconds > horizon,
        }
    }
}

impl IdInterval {
    fn normalize(input: &[(String, String)]) -> Result<Vec<Self>, CoverageError> {
        let mut ranges = input
            .iter()
            .map(|(from, to)| {
                let from = from
                    .parse::<u64>()
                    .map_err(|_| CoverageError::InvalidRange)? as u128;
                let to = to.parse::<u64>().map_err(|_| CoverageError::InvalidRange)? as u128;
                if from > to {
                    return Err(CoverageError::InvalidRange);
                }
                Ok(Self { from, to })
            })
            .collect::<Result<Vec<_>, _>>()?;
        ranges.sort_by_key(|range| range.from);
        let mut merged: Vec<Self> = Vec::new();
        for range in ranges {
            if let Some(previous) = merged
                .last_mut()
                .filter(|previous| range.from <= previous.to + 1)
            {
                previous.to = previous.to.max(range.to);
            } else {
                merged.push(range);
            }
        }
        Ok(merged)
    }

    fn intersection(self, other: Self) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = self.to.min(other.to);
        (from <= to).then_some(Self { from, to })
    }

    fn subtract(self, checked: &[Self]) -> Vec<Self> {
        let mut uncovered = Vec::new();
        let mut cursor = self.from;
        for range in checked {
            if cursor < range.from {
                uncovered.push(Self {
                    from: cursor,
                    to: range.from - 1,
                });
            }
            cursor = range.to + 1;
        }
        if cursor <= self.to {
            uncovered.push(Self {
                from: cursor,
                to: self.to,
            });
        }
        uncovered
    }

    fn describe(self, channel_id: &str) -> Value {
        json!({"channel_id": channel_id, "representation": "message_id", "from_id": self.from.to_string(), "to_id": self.to.to_string(), "inclusive": true})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPOCH: i64 = 1_420_070_400_000;
    const BITS: u64 = (1 << 22) - 1;

    fn time(milliseconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(EPOCH + milliseconds).unwrap()
    }

    fn evidence(ranges: &[(u64, u64)]) -> ChannelEvidence {
        ChannelEvidence {
            channel_id: "200".into(),
            ranges: ranges
                .iter()
                .map(|(from, to)| (from.to_string(), to.to_string()))
                .collect(),
            messages_classified: 0,
            messages_requiring_refetch: 0,
            messages_unresolved: 0,
            guild_unverified: false,
        }
    }

    #[test]
    fn bounded_known_zero_is_complete_when_every_possible_id_is_covered() {
        let channel = evidence(&[(0, (20 << 22) | BITS)]);
        let result = evaluate(Some(time(10)), Some(time(20)), &[channel], true).unwrap();
        assert_eq!(result["complete"], true);
        assert_eq!(result["history_complete"], false);
        assert_eq!(result["observation_scope"], "cached_observations");
        assert_eq!(result["requested_window"]["before_inclusive"], true);
        assert_eq!(
            result["checked_ranges"][0]["from_id"],
            (10_u64 << 22).to_string()
        );
        assert!(result["uncovered_ranges"].as_array().unwrap().is_empty());
    }

    #[test]
    fn disjoint_cached_history_does_not_count_as_requested_window_checked() {
        let channel = evidence(&[(0, (9 << 22) | BITS)]);
        let result = evaluate(Some(time(10)), Some(time(20)), &[channel], true).unwrap();
        assert_eq!(result["complete"], false);
        assert_eq!(result["channels_checked"], 0);
        assert!(result["checked_ranges"].as_array().unwrap().is_empty());
        assert_eq!(result["uncovered_ranges"].as_array().unwrap().len(), 1);
        assert_eq!(result["cached_history"][0]["from_id"], "0");
        assert!(result["reasons"]
            .as_array()
            .unwrap()
            .contains(&Value::from("no_confirmed_coverage")));
    }

    #[test]
    fn overlap_is_clipped_and_gaps_are_subtracted_without_filling_them() {
        let from = 10_u64 << 22;
        let to = (20_u64 << 22) | BITS;
        let channel = evidence(&[(0, from + 5), (from + 9, to + 10)]);
        let result = evaluate(Some(time(10)), Some(time(20)), &[channel], true).unwrap();
        assert_eq!(result["checked_ranges"].as_array().unwrap().len(), 2);
        assert_eq!(result["checked_ranges"][0]["from_id"], from.to_string());
        assert_eq!(result["checked_ranges"][1]["to_id"], to.to_string());
        assert_eq!(
            result["uncovered_ranges"][0]["from_id"],
            (from + 6).to_string()
        );
        assert_eq!(
            result["uncovered_ranges"][0]["to_id"],
            (from + 8).to_string()
        );
        assert_eq!(result["complete"], false);
    }

    #[test]
    fn retrieval_coverage_does_not_prove_unknown_metadata_or_inventory() {
        for reason in [
            "metadata_requires_refetch",
            "classification_unresolved",
            "guild_unverified",
            "unknown_channel_inventory",
        ] {
            let mut channel = evidence(&[(0, (20 << 22) | BITS)]);
            match reason {
                "metadata_requires_refetch" => channel.messages_requiring_refetch = 1,
                "classification_unresolved" => channel.messages_unresolved = 1,
                "guild_unverified" => channel.guild_unverified = true,
                _ => {}
            }
            let result = evaluate(
                Some(time(10)),
                Some(time(20)),
                &[channel],
                reason != "unknown_channel_inventory",
            )
            .unwrap();
            assert_eq!(result["complete"], false, "{reason}");
            assert!(result["reasons"]
                .as_array()
                .unwrap()
                .contains(&Value::from(reason)));
        }
    }

    #[test]
    fn open_windows_are_never_complete() {
        for (after, before) in [(None, None), (Some(time(10)), None), (None, Some(time(20)))] {
            let result = evaluate(after, before, &[evidence(&[(0, u64::MAX)])], true).unwrap();
            assert_eq!(result["complete"], false);
            assert!(result["reasons"]
                .as_array()
                .unwrap()
                .contains(&Value::from("open_window_bound")));
        }
    }

    #[test]
    fn timestamp_offsets_and_submilliseconds_require_whole_boundary_milliseconds() {
        let after = DateTime::parse_from_rfc3339("2015-01-01T09:00:00.010001+09:00")
            .unwrap()
            .with_timezone(&Utc);
        let before = DateTime::parse_from_rfc3339("2015-01-01T00:00:00.020999Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = evaluate(
            Some(after),
            Some(before),
            &[evidence(&[(0, (20 << 22) | BITS)])],
            true,
        )
        .unwrap();
        assert_eq!(result["complete"], true);
        assert_eq!(
            result["requested_id_window"]["from_id"],
            (10_u64 << 22).to_string()
        );
        assert_eq!(
            result["requested_id_window"]["to_id"],
            ((20_u64 << 22) | BITS).to_string()
        );
        let result = evaluate(
            Some(after),
            Some(before),
            &[evidence(&[((10 << 22) + 1, (20 << 22) | BITS)])],
            true,
        )
        .unwrap();
        assert_eq!(result["complete"], false);
    }

    #[test]
    fn every_channel_must_cover_the_window_and_cached_intervals_are_normalized() {
        let from = 10_u64 << 22;
        let to = (20_u64 << 22) | BITS;
        let mut first = evidence(&[(from + 10, to), (from, from + 9), (from + 5, from + 11)]);
        first.messages_classified = 2;
        let result = evaluate(Some(time(10)), Some(time(20)), &[first], true).unwrap();
        assert_eq!(result["complete"], true);
        assert_eq!(result["checked_ranges"].as_array().unwrap().len(), 1);
        assert_eq!(result["cached_history"][0]["messages_classified"], 2);
        let mut second = evidence(&[(0, from - 1)]);
        second.channel_id = "201".into();
        let result = evaluate(
            Some(time(10)),
            Some(time(20)),
            &[evidence(&[(from, to)]), second],
            true,
        )
        .unwrap();
        assert_eq!(result["complete"], false);
        assert_eq!(result["channels_checked"], 1);
        assert_eq!(result["uncovered_ranges"][0]["channel_id"], "201");
    }

    #[test]
    fn no_channel_evidence_is_not_complete_even_for_explicit_scope() {
        let result = evaluate(Some(time(10)), Some(time(20)), &[], true).unwrap();
        assert_eq!(result["complete"], false);
        assert_eq!(result["channels_checked"], 0);
        assert!(result["reasons"]
            .as_array()
            .unwrap()
            .contains(&Value::from("no_confirmed_coverage")));
    }

    #[test]
    fn distant_future_boundaries_are_clamped_without_claiming_completeness() {
        let future = DateTime::parse_from_rfc3339("9999-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        for after in [Some(time(10)), Some(future)] {
            let result =
                evaluate(after, Some(future), &[evidence(&[(0, u64::MAX)])], true).unwrap();
            assert_eq!(result["complete"], false);
            assert_eq!(result["requested_window"]["before"], future.to_rfc3339());
            assert_eq!(result["requested_id_window"]["to_id"], u64::MAX.to_string());
            assert_eq!(result["requested_id_window"]["clamped"], true);
            assert_eq!(result["requested_id_window"]["before_clamped"], true);
            assert!(result["reasons"]
                .as_array()
                .unwrap()
                .contains(&Value::from("window_outside_snowflake_horizon")));
            assert!(result["uncovered_ranges"].as_array().unwrap().is_empty());
        }
        let result = evaluate(
            Some(future),
            Some(future),
            &[evidence(&[(0, u64::MAX)])],
            true,
        )
        .unwrap();
        assert_eq!(
            result["requested_id_window"]["from_id"],
            ((u64::MAX as u128) & !SNOWFLAKE_SEQUENCE_BITS).to_string()
        );
        assert_eq!(result["requested_id_window"]["after_clamped"], true);
    }

    #[test]
    fn invalid_evidence_and_reversed_windows_are_rejected_and_early_dates_are_clamped() {
        let mut channel = evidence(&[]);
        channel.ranges.push(("bad".into(), "20".into()));
        assert!(matches!(
            evaluate(Some(time(10)), Some(time(20)), &[channel], true),
            Err(CoverageError::InvalidRange)
        ));
        assert!(matches!(
            evaluate(Some(time(20)), Some(time(10)), &[], true),
            Err(CoverageError::InvalidWindow)
        ));
        let result = evaluate(
            Some(time(-10)),
            Some(time(20)),
            &[evidence(&[(0, (20 << 22) | BITS)])],
            true,
        )
        .unwrap();
        assert_eq!(result["requested_id_window"]["from_id"], "0");
        assert_eq!(result["complete"], true);
    }
}

//! Conservative textual hints for attention review, without workflow decisions.

use discord_api::types::MessageView;
use serde::Serialize;

/// A textual hint requires review; it is not a scheduled event or action confirmation.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AttentionCandidate {
    pub kind: String,
    pub message_id: String,
    pub message_url: String,
    pub sender_id: String,
    pub posted_at: String,
    pub evidence_excerpt: String,
    pub explicit_markers: Vec<String>,
    pub literal_time_hints: Vec<String>,
    pub literal_timezone_hints: Vec<String>,
    pub normalized_start_at: Option<String>,
    pub normalized_location: Option<String>,
    pub ambiguities: Vec<String>,
    pub confidence: &'static str,
    pub source: &'static str,
    pub decision_scope: &'static str,
}

/// Extract bounded lexical evidence without guessing dates or updating workflow state.
pub(crate) fn candidates(message: &MessageView) -> Vec<AttentionCandidate> {
    let content = message.content.to_lowercase();
    let words: Vec<&str> = content
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect();
    let literals: Vec<&str> = message
        .content
        .split_whitespace()
        .map(|word| word.trim_matches(|ch: char| matches!(ch, '.' | ',' | ';' | '(' | ')')))
        .collect();
    let mut time_hints: Vec<String> = literals
        .iter()
        .filter(|word| {
            word.chars().any(|ch| ch.is_ascii_digit())
                && (word.contains(':')
                    || word.contains('/')
                    || word.contains('-')
                    || word.contains('時')
                    || word.contains('月')
                    || word.contains('日'))
        })
        .map(|word| word.chars().take(100).collect())
        .take(20)
        .collect();
    time_hints.sort();
    time_hints.dedup();
    let timezone_hints: Vec<String> = literals
        .iter()
        .filter(|word| matches!(word.to_ascii_uppercase().as_str(), "UTC" | "GMT" | "JST"))
        .map(|word| (*word).to_owned())
        .take(20)
        .collect();
    let rules: [(&str, &[&str], &[&str]); 4] = [
        (
            "schedule",
            &["meeting", "event", "schedule", "appointment"],
            &["予定", "開催", "集合", "日時", "場所", "案内"],
        ),
        (
            "deadline",
            &["deadline", "due"],
            &["期限", "締切", "締め切り"],
        ),
        (
            "change",
            &["changed", "rescheduled", "postponed"],
            &["変更", "延期"],
        ),
        (
            "cancellation",
            &["cancelled", "canceled", "cancel", "cancellation"],
            &["中止", "キャンセル"],
        ),
    ];
    let mut result = Vec::new();
    for (kind, english, japanese) in rules {
        let markers: Vec<String> = english
            .iter()
            .filter(|marker| words.contains(marker))
            .chain(japanese.iter().filter(|marker| content.contains(**marker)))
            .map(|marker| (*marker).to_owned())
            .collect();
        if markers.is_empty() {
            continue;
        }
        let mut ambiguities = vec![
            "datetime_literals_not_normalized".into(),
            "location_unresolved".into(),
            "participation_not_established".into(),
            "importance_requires_review".into(),
        ];
        if timezone_hints.is_empty() {
            ambiguities.push("timezone_unresolved".into());
        }
        if time_hints.is_empty() {
            ambiguities.push("date_and_time_unresolved".into());
        }
        result.push(AttentionCandidate {
            kind: kind.into(),
            message_id: message.id.clone(),
            message_url: format!(
                "https://discord.com/channels/{}/{}/{}",
                message.guild_id.as_deref().unwrap_or("@me"),
                message.channel_id,
                message.id
            ),
            sender_id: message.author.id.clone(),
            posted_at: message.timestamp.clone(),
            evidence_excerpt: message.content.chars().take(500).collect(),
            explicit_markers: markers,
            literal_time_hints: time_hints.clone(),
            literal_timezone_hints: timezone_hints.clone(),
            normalized_start_at: None,
            normalized_location: None,
            ambiguities,
            confidence: "low",
            source: "lexical_rule",
            decision_scope: "review_candidate_only",
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use discord_api::types::MessageView;
    fn message(content: &str) -> MessageView {
        serde_json::from_value(serde_json::json!({"id":"300","channel_id":"200","guild_id":"100","author":{"id":"7","name":"fixture"},"timestamp":"2026-10-13T10:00:00Z","content":content,"content_kind":"text"})).unwrap()
    }
    #[test]
    fn reply_unneeded_schedule_still_has_an_evidence_candidate() {
        let candidates = candidates(&message("Meeting 2026-10-20 14:00 JST. No reply needed."));
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.kind == "schedule")
            .unwrap();
        assert_eq!(candidate.message_id, "300");
        assert_eq!(candidate.sender_id, "7");
        assert!(candidate
            .literal_time_hints
            .iter()
            .any(|hint| hint == "14:00"));
        assert!(candidate
            .ambiguities
            .iter()
            .any(|reason| reason == "participation_not_established"));
    }
    #[test]
    fn cancellation_and_missing_timezone_are_not_normalized_into_a_plan() {
        let candidates = candidates(&message("明日14時の予定は中止です。"));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == "cancellation"));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.normalized_start_at.is_none()));
        assert!(candidates.iter().all(|candidate| candidate
            .ambiguities
            .iter()
            .any(|reason| reason == "timezone_unresolved")));
    }
    #[test]
    fn unrelated_followup_has_no_completion_or_participation_decision() {
        assert!(candidates(&message("今日は別の話です。")).is_empty());
        let result = serde_json::to_value(candidates(&message(
            "Deadline changed; 場所は後日案内します。",
        )))
        .unwrap();
        for item in result.as_array().unwrap() {
            assert!(item.get("completed_at").is_none());
            assert_eq!(item["decision_scope"], "review_candidate_only");
            assert!(item["normalized_start_at"].is_null());
        }
    }
}

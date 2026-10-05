//! Discord REST endpoint allowlist.
//!
//! Every Discord request the daemon can make is represented by a variant of
//! [`DiscordRequest`]. There is no way to express an arbitrary URL, an
//! arbitrary path, or any HTTP method other than `GET` in this API surface.
//! This is the primary write-prevention control: write endpoints simply do
//! not exist at the type level.

use crate::client::DiscordError;

/// Base URL of the Discord REST API. Requests are always built from this
/// constant; callers cannot supply their own host or scheme.
pub const API_BASE: &str = "https://discord.com/api/v10";

/// The only HTTP method this crate ever sends.
pub const READ_ONLY_METHOD: &str = "GET";

/// Every Discord REST operation the daemon supports.
///
/// All variants map to `GET`. Write operations (send/edit/delete message,
/// reactions, guild membership, profile edits, typing, webhooks, ...) are
/// deliberately not representable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscordRequest {
    /// `GET /users/@me`
    GetCurrentUser,
    /// `GET /users/@me/guilds`
    ListGuilds {
        limit: u8,
        before: Option<String>,
        after: Option<String>,
    },
    /// `GET /guilds/{guild.id}`
    GetGuild { guild_id: String },
    /// `GET /guilds/{guild.id}/channels`
    GetGuildChannels { guild_id: String },
    /// `GET /guilds/{guild.id}/threads/active`
    GetGuildActiveThreads { guild_id: String },
    /// `GET /channels/{channel.id}`
    GetChannel { channel_id: String },
    /// `GET /channels/{channel.id}/messages`
    GetMessages {
        channel_id: String,
        limit: u8,
        before: Option<String>,
        after: Option<String>,
        around: Option<String>,
    },
    /// `GET /channels/{channel.id}/messages/{message.id}`
    GetMessage {
        channel_id: String,
        message_id: String,
    },
    /// `GET /users/@me/channels` (DM / group DM listing)
    ListUserDmChannels,
}

impl DiscordRequest {
    /// The HTTP method. This is always `GET`; kept as a method so the
    /// lowest layer can re-assert the invariant on every request.
    pub fn method(&self) -> &'static str {
        READ_ONLY_METHOD
    }

    /// Route template used as the rate limit bucket key.
    pub fn bucket_key(&self) -> String {
        match self {
            Self::GetCurrentUser => "GET /users/@me".to_string(),
            Self::ListGuilds { .. } => "GET /users/@me/guilds".to_string(),
            Self::GetGuild { .. } => "GET /guilds/{guild.id}".to_string(),
            Self::GetGuildChannels { .. } => "GET /guilds/{guild.id}/channels".to_string(),
            Self::GetGuildActiveThreads { .. } => {
                "GET /guilds/{guild.id}/threads/active".to_string()
            }
            Self::GetChannel { .. } => "GET /channels/{channel.id}".to_string(),
            Self::GetMessages { .. } => "GET /channels/{channel.id}/messages".to_string(),
            Self::GetMessage { .. } => {
                "GET /channels/{channel.id}/messages/{message.id}".to_string()
            }
            Self::ListUserDmChannels => "GET /users/@me/channels".to_string(),
        }
    }

    /// Path (without query string) for this request.
    ///
    /// Identifiers are validated as Discord snowflakes (ASCII digits) here so
    /// a hostile or malformed identifier can never alter the request path.
    pub fn path(&self) -> Result<String, DiscordError> {
        let path = match self {
            Self::GetCurrentUser => "/users/@me".to_string(),
            Self::ListGuilds { .. } => "/users/@me/guilds".to_string(),
            Self::GetGuild { guild_id } => format!("/guilds/{}", snowflake(guild_id)?),
            Self::GetGuildChannels { guild_id } => {
                format!("/guilds/{}/channels", snowflake(guild_id)?)
            }
            Self::GetGuildActiveThreads { guild_id } => {
                format!("/guilds/{}/threads/active", snowflake(guild_id)?)
            }
            Self::GetChannel { channel_id } => format!("/channels/{}", snowflake(channel_id)?),
            Self::GetMessages { channel_id, .. } => {
                format!("/channels/{}/messages", snowflake(channel_id)?)
            }
            Self::GetMessage {
                channel_id,
                message_id,
            } => format!(
                "/channels/{}/messages/{}",
                snowflake(channel_id)?,
                snowflake(message_id)?
            ),
            Self::ListUserDmChannels => "/users/@me/channels".to_string(),
        };
        Ok(path)
    }

    /// Query string parameters.
    pub fn query(&self) -> Vec<(String, String)> {
        let mut q = Vec::new();
        match self {
            Self::ListGuilds {
                limit,
                before,
                after,
            } => {
                q.push(("limit".into(), limit.to_string()));
                push_opt(&mut q, "before", before);
                push_opt(&mut q, "after", after);
            }
            Self::GetMessages {
                limit,
                before,
                after,
                around,
                ..
            } => {
                q.push(("limit".into(), limit.to_string()));
                push_opt(&mut q, "before", before);
                push_opt(&mut q, "after", after);
                push_opt(&mut q, "around", around);
            }
            _ => {}
        }
        q
    }
}

fn push_opt(q: &mut Vec<(String, String)>, key: &str, value: &Option<String>) {
    if let Some(v) = value {
        q.push((key.to_string(), v.clone()));
    }
}

/// Validate a Discord snowflake: non-empty, ASCII digits only, bounded length.
fn snowflake(id: &str) -> Result<&str, DiscordError> {
    if id.is_empty() || id.len() > 20 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DiscordError::InvalidIdentifier(id.to_string()));
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_requests() -> Vec<DiscordRequest> {
        vec![
            DiscordRequest::GetCurrentUser,
            DiscordRequest::ListGuilds {
                limit: 50,
                before: None,
                after: None,
            },
            DiscordRequest::GetGuild {
                guild_id: "1".into(),
            },
            DiscordRequest::GetGuildChannels {
                guild_id: "1".into(),
            },
            DiscordRequest::GetGuildActiveThreads {
                guild_id: "1".into(),
            },
            DiscordRequest::GetChannel {
                channel_id: "2".into(),
            },
            DiscordRequest::GetMessages {
                channel_id: "2".into(),
                limit: 50,
                before: None,
                after: None,
                around: None,
            },
            DiscordRequest::GetMessage {
                channel_id: "2".into(),
                message_id: "3".into(),
            },
            DiscordRequest::ListUserDmChannels,
        ]
    }

    #[test]
    fn every_request_uses_get() {
        for req in all_requests() {
            assert_eq!(req.method(), "GET");
        }
    }

    #[test]
    fn every_request_path_is_relative_and_clean() {
        for req in all_requests() {
            let path = req.path().unwrap();
            assert!(path.starts_with('/'), "path must be relative: {path}");
            assert!(
                !path.contains("://"),
                "path must not contain a scheme: {path}"
            );
            assert!(!path.contains(".."), "path must not traverse: {path}");
            assert!(!path.contains('?'), "query belongs in query(): {path}");
        }
    }

    #[test]
    fn hostile_identifiers_are_rejected() {
        let hostile = [
            "",
            "../users/@me",
            "1/../../guilds",
            "123456789012345678901",
            "abc",
            "1;DROP",
            "1%2f2",
            "١٢٣",
        ];
        for id in hostile {
            let req = DiscordRequest::GetChannel {
                channel_id: id.to_string(),
            };
            assert!(
                matches!(req.path(), Err(DiscordError::InvalidIdentifier(_))),
                "identifier {id:?} must be rejected"
            );
        }
    }

    #[test]
    fn bucket_keys_are_stable_route_templates() {
        assert_eq!(
            DiscordRequest::GetMessages {
                channel_id: "2".into(),
                limit: 1,
                before: None,
                after: None,
                around: None,
            }
            .bucket_key(),
            "GET /channels/{channel.id}/messages"
        );
    }
}

//! Discord wire types and the LLM-friendly normalized views.
//!
//! Raw Discord JSON is deserialized into the small structs below and then
//! normalized into `*View` types. MCP never sees raw API payloads: only the
//! normalized views (plus minimal attachment/embed metadata) are returned.

use serde::{Deserialize, Serialize};

fn d<T: Default>() -> T {
    T::default()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub id: String,
    #[serde(default = "d")]
    pub username: Option<String>,
    #[serde(default = "d")]
    pub global_name: Option<String>,
    #[serde(default = "d")]
    pub bot: Option<bool>,
}

impl User {
    pub fn display_name(&self) -> String {
        self.global_name
            .clone()
            .or_else(|| self.username.clone())
            .unwrap_or_else(|| self.id.clone())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Guild {
    pub id: String,
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d")]
    pub owner: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Channel {
    pub id: String,
    #[serde(default = "d")]
    pub guild_id: Option<String>,
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d", rename = "type")]
    pub kind: i32,
    #[serde(default = "d")]
    pub parent_id: Option<String>,
    #[serde(default = "d")]
    pub topic: Option<String>,
    #[serde(default = "d")]
    pub last_message_id: Option<String>,
    #[serde(default = "d")]
    pub recipients: Vec<User>,
    #[serde(default = "d")]
    pub owner_id: Option<String>,
    #[serde(default = "d")]
    pub message_count: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attachment {
    #[serde(default = "d")]
    pub id: Option<String>,
    pub filename: String,
    #[serde(default = "d")]
    pub url: Option<String>,
    #[serde(default = "d")]
    pub content_type: Option<String>,
    #[serde(default = "d")]
    pub size: Option<u64>,
    #[serde(default = "d")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Embed {
    #[serde(default = "d", rename = "type")]
    pub kind: Option<String>,
    #[serde(default = "d")]
    pub title: Option<String>,
    #[serde(default = "d")]
    pub description: Option<String>,
    #[serde(default = "d")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageReference {
    #[serde(default = "d")]
    pub message_id: Option<String>,
    #[serde(default = "d")]
    pub channel_id: Option<String>,
    #[serde(default = "d")]
    pub guild_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    #[serde(default = "d")]
    pub channel_id: String,
    #[serde(default = "d")]
    pub guild_id: Option<String>,
    #[serde(default = "d")]
    pub author: Option<User>,
    #[serde(default = "d")]
    pub content: String,
    #[serde(default = "d")]
    pub timestamp: String,
    #[serde(default = "d")]
    pub edited_timestamp: Option<String>,
    #[serde(default = "d")]
    pub attachments: Vec<Attachment>,
    #[serde(default = "d")]
    pub embeds: Vec<Embed>,
    #[serde(default = "d")]
    pub message_reference: Option<MessageReference>,
}

/// Normalized author. Never exposes the raw user object.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthorView {
    pub id: String,
    pub name: String,
}

/// Minimal attachment metadata.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AttachmentView {
    pub filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

/// Text-only embed extract.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EmbedView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// The message shape returned to MCP clients.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MessageView {
    pub id: String,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub author: AuthorView,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_timestamp: Option<String>,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub embeds: Vec<EmbedView>,
}

impl From<&Message> for MessageView {
    fn from(m: &Message) -> Self {
        let author = m.author.clone().unwrap_or_default();
        Self {
            id: m.id.clone(),
            channel_id: m.channel_id.clone(),
            guild_id: m.guild_id.clone(),
            author: AuthorView {
                id: author.id.clone(),
                name: author.display_name(),
            },
            timestamp: m.timestamp.clone(),
            edited_timestamp: m.edited_timestamp.clone(),
            content: m.content.clone(),
            reply_to: m
                .message_reference
                .as_ref()
                .and_then(|r| r.message_id.clone()),
            attachments: m
                .attachments
                .iter()
                .map(|a| AttachmentView {
                    filename: a.filename.clone(),
                    url: a.url.clone(),
                    content_type: a.content_type.clone(),
                    size: a.size,
                })
                .collect(),
            embeds: m
                .embeds
                .iter()
                .map(|e| EmbedView {
                    title: e.title.clone(),
                    description: e.description.clone(),
                    url: e.url.clone(),
                })
                .collect(),
        }
    }
}

/// Normalized guild entry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GuildView {
    pub id: String,
    pub name: String,
}

impl From<&Guild> for GuildView {
    fn from(g: &Guild) -> Self {
        Self {
            id: g.id.clone(),
            name: g.name.clone().unwrap_or_default(),
        }
    }
}

/// Normalized channel entry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChannelView {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Discord channel type integer (0 text, 2 voice, 4 category, 5 announcement,
    /// 10/11 threads, 13 stage, 15 forum, ...).
    pub kind: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

impl From<&Channel> for ChannelView {
    fn from(c: &Channel) -> Self {
        Self {
            id: c.id.clone(),
            guild_id: c.guild_id.clone(),
            name: c.name.clone(),
            kind: c.kind,
            parent_id: c.parent_id.clone(),
            topic: c.topic.clone(),
        }
    }
}

/// Normalized DM / group DM entry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DmView {
    pub channel_id: String,
    /// 1 = DM, 3 = group DM.
    pub kind: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub participants: Vec<AuthorView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<String>,
}

impl From<&Channel> for DmView {
    fn from(c: &Channel) -> Self {
        Self {
            channel_id: c.id.clone(),
            kind: c.kind,
            name: c.name.clone(),
            participants: c
                .recipients
                .iter()
                .map(|u| AuthorView {
                    id: u.id.clone(),
                    name: u.display_name(),
                })
                .collect(),
            last_message_id: c.last_message_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message() -> Message {
        Message {
            id: "123".into(),
            channel_id: "456".into(),
            guild_id: Some("789".into()),
            author: Some(User {
                id: "111".into(),
                username: Some("example".into()),
                global_name: Some("Example".into()),
                bot: Some(false),
            }),
            content: "message".into(),
            timestamp: "2026-10-05T00:00:00.000000+00:00".into(),
            edited_timestamp: None,
            attachments: vec![Attachment {
                id: Some("a1".into()),
                filename: "notes.txt".into(),
                url: Some("https://cdn.discordapp.com/attachments/1/2/notes.txt".into()),
                content_type: Some("text/plain".into()),
                size: Some(12345),
                description: None,
            }],
            embeds: vec![Embed {
                kind: Some("rich".into()),
                title: Some("title".into()),
                description: Some("desc".into()),
                url: Some("https://example.invalid/x".into()),
            }],
            message_reference: Some(MessageReference {
                message_id: Some("42".into()),
                channel_id: Some("456".into()),
                guild_id: Some("789".into()),
            }),
        }
    }

    #[test]
    fn message_view_is_normalized() {
        let view = MessageView::from(&sample_message());
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["id"], "123");
        assert_eq!(json["channel_id"], "456");
        assert_eq!(json["guild_id"], "789");
        assert_eq!(json["author"]["id"], "111");
        assert_eq!(json["author"]["name"], "Example");
        assert_eq!(json["content"], "message");
        assert_eq!(json["reply_to"], "42");
        assert_eq!(json["attachments"][0]["filename"], "notes.txt");
        assert_eq!(json["attachments"][0]["size"], 12345);
        assert_eq!(json["embeds"][0]["title"], "title");
        // Normalized output never leaks raw API fields.
        for banned in [
            "message_reference",
            "mentions",
            "reactions",
            "components",
            "sticker_items",
        ] {
            assert!(
                json.get(banned).is_none(),
                "field {banned} must not be exposed"
            );
        }
    }

    #[test]
    fn author_falls_back_to_username_then_id() {
        let mut u = User {
            id: "111".into(),
            username: Some("example".into()),
            global_name: None,
            bot: None,
        };
        assert_eq!(u.display_name(), "example");
        u.username = None;
        assert_eq!(u.display_name(), "111");
    }
}

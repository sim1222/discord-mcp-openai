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
    #[serde(default = "d")]
    pub discriminator: Option<String>,
    #[serde(default = "d")]
    pub avatar: Option<String>,
}

/// A role referenced by a message (role mention) or held by a member.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Role {
    pub id: String,
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d")]
    pub color: Option<u32>,
    #[serde(default = "d")]
    pub position: Option<i32>,
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
    /// Thread-only: number of messages sent, when the API reports it.
    #[serde(default = "d")]
    pub total_message_sent: Option<u64>,
    /// Thread-only: how many members are in the thread.
    #[serde(default = "d")]
    pub member_count: Option<u64>,
    /// Thread-only metadata.
    #[serde(default = "d")]
    pub thread_metadata: Option<ThreadMetadata>,
    /// Thread-only: the member object for the current user, if any.
    #[serde(default = "d")]
    pub member: Option<ThreadMember>,
}

/// Thread lifecycle metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadMetadata {
    #[serde(default = "d")]
    pub archived: Option<bool>,
    /// When the thread's archive status last changed.
    #[serde(default = "d")]
    pub archive_timestamp: Option<String>,
    #[serde(default = "d")]
    pub auto_archive_duration: Option<u32>,
    #[serde(default = "d")]
    pub locked: Option<bool>,
    #[serde(default = "d")]
    pub create_timestamp: Option<String>,
}

/// Current user's membership of a thread.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadMember {
    #[serde(default = "d")]
    pub id: Option<String>,
    #[serde(default = "d")]
    pub user_id: Option<String>,
    #[serde(default = "d")]
    pub join_timestamp: Option<String>,
    #[serde(default = "d")]
    pub flags: Option<u32>,
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
    #[serde(default = "d")]
    pub height: Option<u32>,
    #[serde(default = "d")]
    pub width: Option<u32>,
    #[serde(default = "d")]
    pub proxy_url: Option<String>,
    #[serde(default = "d")]
    pub ephemeral: Option<bool>,
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
    #[serde(default = "d")]
    pub timestamp: Option<String>,
    #[serde(default = "d")]
    pub color: Option<u32>,
    #[serde(default = "d")]
    pub provider: Option<EmbedProvider>,
    #[serde(default = "d")]
    pub author: Option<EmbedAuthor>,
    #[serde(default = "d")]
    pub footer: Option<EmbedFooter>,
    #[serde(default = "d")]
    pub image: Option<EmbedMedia>,
    #[serde(default = "d")]
    pub thumbnail: Option<EmbedMedia>,
    /// Field names/values of rich embeds, in order.
    #[serde(default = "d")]
    pub fields: Vec<EmbedField>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedProvider {
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedAuthor {
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d")]
    pub url: Option<String>,
    #[serde(default = "d")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedFooter {
    #[serde(default = "d")]
    pub text: Option<String>,
    #[serde(default = "d")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedMedia {
    #[serde(default = "d")]
    pub url: Option<String>,
    #[serde(default = "d")]
    pub proxy_url: Option<String>,
    #[serde(default = "d")]
    pub height: Option<u32>,
    #[serde(default = "d")]
    pub width: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedField {
    #[serde(default = "d")]
    pub name: String,
    #[serde(default = "d")]
    pub value: String,
    #[serde(default = "d")]
    pub inline: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageReference {
    #[serde(default, rename = "type")]
    pub kind: Option<u8>,
    #[serde(default = "d")]
    pub message_id: Option<String>,
    #[serde(default = "d")]
    pub channel_id: Option<String>,
    #[serde(default = "d")]
    pub guild_id: Option<String>,
    /// When Discord returns the resolved reference object (`referenced_message`
    /// is its body), this distinguishes a real reply from a plain link.
    #[serde(default = "d")]
    pub fail_if_not_exists: Option<bool>,
}

/// One reaction on a message.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reaction {
    #[serde(default = "d")]
    pub count: Option<u64>,
    #[serde(default = "d")]
    pub count_details: Option<ReactionCountDetails>,
    #[serde(default = "d")]
    pub me: Option<bool>,
    #[serde(default = "d")]
    pub emoji: Option<Emoji>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReactionCountDetails {
    #[serde(default = "d")]
    pub burst: Option<u64>,
    #[serde(default = "d")]
    pub normal: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Emoji {
    #[serde(default = "d")]
    pub id: Option<String>,
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d")]
    pub animated: Option<bool>,
}

/// Resolved attachment/message snapshot when the original was deleted or
/// inaccessible (used by forwarded / auto-moderated messages).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageSnapshot {
    #[serde(default = "d")]
    pub message_id: Option<String>,
    #[serde(default = "d")]
    pub message: Option<Box<SnapshotMessage>>,
}

/// The snapshot's embedded message body: a partial `Message`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMessage {
    #[serde(default = "d")]
    pub id: Option<String>,
    #[serde(default = "d")]
    pub content: Option<String>,
    #[serde(default = "d")]
    pub timestamp: Option<String>,
    #[serde(default = "d")]
    pub edited_timestamp: Option<String>,
    #[serde(default = "d")]
    pub attachments: Vec<Attachment>,
    #[serde(default = "d")]
    pub embeds: Vec<Embed>,
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
    /// Discord message type integer (0 default, 19 reply, 18 thread starter,
    /// 7 user join, 46 thread created with message, ...).
    #[serde(default = "d", rename = "type")]
    pub kind: i64,
    /// Raw content: always captured verbatim, even when Discord returns
    /// something our `String` default would flatten to empty.
    #[serde(default = "d")]
    pub raw_content: Option<serde_json::Value>,
    /// User IDs mentioned with `@user` (plus the resolved user objects).
    #[serde(default = "d")]
    pub mention_everyone: Option<bool>,
    #[serde(default = "d")]
    pub mentions: Vec<User>,
    /// Role IDs mentioned with `@role`.
    #[serde(default = "d")]
    pub mention_roles: Vec<String>,
    /// Resolved referenced message (reply target), when Discord sends it.
    #[serde(default = "d")]
    pub referenced_message: Option<Box<Message>>,
    /// Flag telling whether the reply reference is known to be dangling
    /// (the referenced message was deleted / not accessible).
    #[serde(default = "d")]
    pub referenced_message_status: Option<String>,
    /// When the message starts a thread, the thread object.
    #[serde(default = "d")]
    pub thread: Option<Channel>,
    /// Forwarded / auto-moderated message snapshots.
    #[serde(default = "d")]
    pub message_snapshots: Vec<MessageSnapshot>,
    #[serde(default = "d")]
    pub reactions: Vec<Reaction>,
    /// Discord `pinned` flag (message pin state as of fetch time).
    #[serde(default = "d")]
    pub pinned: Option<bool>,
    /// Discord `flags` bitfield (e.g. 1 = crossposted, 2 = is crosspost,
    /// 16 = has thread, 64 = loading, 128 = failed to mention roles).
    #[serde(default = "d")]
    pub flags: Option<u64>,
    /// Nonce / interaction metadata omitted intentionally.
    /// Application id for application-created messages (e.g. slash commands).
    #[serde(default = "d")]
    pub application_id: Option<String>,
    /// Webhook id if the message came from a webhook.
    #[serde(default = "d")]
    pub webhook_id: Option<String>,
    /// `interaction` summary for slash-command invocations.
    #[serde(default = "d")]
    pub interaction: Option<Interaction>,
}

/// Minimal interaction summary for slash-command / component messages.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Interaction {
    #[serde(default = "d")]
    pub id: Option<String>,
    #[serde(default = "d", rename = "type")]
    pub kind: Option<i64>,
    #[serde(default = "d")]
    pub name: Option<String>,
    #[serde(default = "d")]
    pub user: Option<User>,
}

/// Normalized author. Never exposes the raw user object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthorView {
    pub id: String,
    pub name: String,
}

/// Attachment metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttachmentView {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub filename: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
}

impl From<&Attachment> for AttachmentView {
    fn from(a: &Attachment) -> Self {
        Self {
            id: a.id.clone(),
            filename: a.filename.clone(),
            url: a.url.clone(),
            proxy_url: a.proxy_url.clone(),
            content_type: a.content_type.clone(),
            size: a.size,
            description: a.description.clone(),
            height: a.height,
            width: a.width,
            ephemeral: a.ephemeral,
        }
    }
}

/// Text embed extract (title/description/url plus optional rich details).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedView {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author_name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<EmbedFieldView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbedFieldView {
    pub name: String,
    pub value: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inline: Option<bool>,
}

impl From<&Embed> for EmbedView {
    fn from(e: &Embed) -> Self {
        Self {
            kind: e.kind.clone(),
            title: e.title.clone(),
            description: e.description.clone(),
            url: e.url.clone(),
            timestamp: e.timestamp.clone(),
            provider: e.provider.as_ref().and_then(|p| p.name.clone()),
            author_name: e.author.as_ref().and_then(|a| a.name.clone()),
            footer: e.footer.as_ref().and_then(|f| f.text.clone()),
            fields: e
                .fields
                .iter()
                .map(|f| EmbedFieldView {
                    name: f.name.clone(),
                    value: f.value.clone(),
                    inline: f.inline,
                })
                .collect(),
        }
    }
}

/// One reaction summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReactionView {
    pub name: String,
    pub count: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub me: Option<bool>,
}

impl From<&Reaction> for ReactionView {
    fn from(r: &Reaction) -> Self {
        let name = r
            .emoji
            .as_ref()
            .map(|e| {
                e.name
                    .clone()
                    .unwrap_or_else(|| e.id.clone().unwrap_or_else(|| "?".into()))
            })
            .unwrap_or_else(|| "?".into());
        Self {
            name,
            count: r.count.unwrap_or(0),
            me: r.me,
        }
    }
}

/// Resolved reference to another message (reply target).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageRefView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_type: Option<u8>,
    pub message_id: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    /// `resolved` (the referenced message body is inline in `referenced`),
    /// Otherwise `deleted`, `forbidden`, `bot_only`, `not_observed`,
    /// `unavailable` or `unknown`.
    pub status: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced: Option<Box<MessageView>>,
}

/// Thread summary carried inside a message that starts a thread.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThreadRefView {
    pub id: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked: Option<bool>,
}

impl From<&Channel> for ThreadRefView {
    fn from(c: &Channel) -> Self {
        Self {
            id: c.id.clone(),
            name: c.name.clone(),
            parent_id: c.parent_id.clone(),
            archived: c.thread_metadata.as_ref().and_then(|m| m.archived),
            locked: c.thread_metadata.as_ref().and_then(|m| m.locked),
        }
    }
}

/// Normalized role entry (used for mentions and member roles).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoleView {
    pub id: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl From<&Role> for RoleView {
    fn from(r: &Role) -> Self {
        Self {
            id: r.id.clone(),
            name: r.name.clone(),
        }
    }
}

/// A message snapshot of a forwarded / moderated message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageSnapshotView {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentView>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub embeds: Vec<EmbedView>,
}

impl From<&MessageSnapshot> for MessageSnapshotView {
    fn from(s: &MessageSnapshot) -> Self {
        let inner = s.message.as_deref();
        Self {
            message_id: s
                .message_id
                .clone()
                .or_else(|| inner.and_then(|m| m.id.clone())),
            content: inner.and_then(|m| m.content.clone()),
            attachments: inner
                .map(|m| m.attachments.iter().map(AttachmentView::from).collect())
                .unwrap_or_default(),
            embeds: inner
                .map(|m| m.embeds.iter().map(EmbedView::from).collect())
                .unwrap_or_default(),
        }
    }
}

/// The message shape returned to MCP clients (schema v2).
///
/// Field semantics:
/// - `content` is the verbatim Discord content string ("" when the message
///   genuinely has no text, e.g. an attachment-only or system message);
/// - `content_kind` disambiguates an empty `content` so clients never mistake
///   "no text" for "we failed to fetch the text";
/// - `mentions` / `mention_roles` / `mention_everyone` are verbatim from the
///   API (role/user ids as sent by Discord);
/// - `reply_to` is `null` for non-replies; when set, `reply_to.status` says
///   whether the referenced message body could be resolved inline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageView {
    pub id: String,
    pub channel_id: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub author: AuthorView,
    pub timestamp: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_timestamp: Option<String>,
    /// Discord message type integer (0 default, 19 reply, ...).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_type: Option<i64>,
    /// Verbatim message content; "" only when Discord really sent no text.
    pub content: String,
    /// `text`, `empty`, `attachment_only`, `embed_only`, `system`,
    /// `forwarded` or `unknown` — why `content` is what it is.
    pub content_kind: String,
    /// `requires_refetch` for legacy/incomplete cache rows; `available` for API metadata.
    #[serde(default = "available_metadata")]
    pub metadata_state: String,
    /// `@user` mentions: resolved user ids and names as sent by Discord.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<AuthorView>,
    /// `@role` mention role ids as sent by Discord.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mention_roles: Vec<String>,
    /// `true` when the message mentioned @everyone/@here.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mention_everyone: Option<bool>,
    /// Reply reference, `null` when the message is not a reply.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<MessageRefView>,
    /// Thread this message started, when `message.type` is a thread-starter.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadRefView>,
    /// Forwarded / auto-moderated message snapshots.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub message_snapshots: Vec<MessageSnapshotView>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentView>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub embeds: Vec<EmbedView>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reactions: Vec<ReactionView>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_id: Option<String>,
}

fn available_metadata() -> String {
    "available".to_string()
}

impl MessageView {
    /// Classify why the content string looks the way it does.
    pub fn content_kind(m: &Message) -> &'static str {
        if !m.content.trim().is_empty() {
            return "text";
        }
        if m.raw_content.is_some()
            && m.content.is_empty()
            && m.raw_content.as_ref() != Some(&serde_json::Value::String(String::new()))
        {
            // Discord sent a non-string content we could not normalize.
            return "unknown";
        }
        if !m.message_snapshots.is_empty() {
            return "forwarded";
        }
        if !m.attachments.is_empty() {
            return "attachment_only";
        }
        if !m.embeds.is_empty() {
            return "embed_only";
        }
        if m.kind != 0 {
            return "system";
        }
        "empty"
    }
}

impl From<&Message> for MessageView {
    fn from(m: &Message) -> Self {
        let author = m.author.clone().unwrap_or_default();
        let reply_to = m.message_reference.as_ref().and_then(|r| {
            r.message_id.clone().map(|id| {
                let status = if m.referenced_message.is_some() {
                    "resolved"
                } else {
                    match m.referenced_message_status.as_deref() {
                        Some("deleted") => "deleted",
                        Some("forbidden") => "forbidden",
                        Some("bot_only") => "bot_only",
                        Some("not_observed") => "not_observed",
                        Some("unavailable") => "unavailable",
                        _ => "unknown",
                    }
                };
                MessageRefView {
                    reference_type: r.kind,
                    message_id: id,
                    channel_id: r.channel_id.clone(),
                    guild_id: r.guild_id.clone(),
                    status: status.to_string(),
                    referenced: m
                        .referenced_message
                        .as_ref()
                        .map(|rm| Box::new(MessageView::from(rm.as_ref()))),
                }
            })
        });
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
            message_type: Some(m.kind),
            content: m.content.clone(),
            content_kind: Self::content_kind(m).to_string(),
            metadata_state: available_metadata(),
            mentions: m
                .mentions
                .iter()
                .map(|u| AuthorView {
                    id: u.id.clone(),
                    name: u.display_name(),
                })
                .collect(),
            mention_roles: m.mention_roles.clone(),
            mention_everyone: m.mention_everyone,
            reply_to,
            thread: m.thread.as_ref().map(ThreadRefView::from),
            message_snapshots: m
                .message_snapshots
                .iter()
                .map(MessageSnapshotView::from)
                .collect(),
            attachments: m.attachments.iter().map(AttachmentView::from).collect(),
            embeds: m.embeds.iter().map(EmbedView::from).collect(),
            reactions: m.reactions.iter().map(ReactionView::from).collect(),
            pinned: m.pinned,
            flags: m.flags,
            webhook_id: m.webhook_id.clone(),
        }
    }
}

/// Normalized guild entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelView {
    pub id: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Discord channel type integer (0 text, 2 voice, 4 category, 5 announcement,
    /// 10/11 threads, 13 stage, 15 forum, ...).
    pub kind: i32,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Last message ID observed from Discord; null means it has not been reported.
    #[serde(default)]
    pub last_message_id: Option<String>,
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
            last_message_id: c.last_message_id.clone(),
        }
    }
}

/// Normalized DM / group DM entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DmView {
    pub channel_id: String,
    /// 1 = DM, 3 = group DM.
    pub kind: i32,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub participants: Vec<AuthorView>,
    #[serde(default)]
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
                discriminator: None,
                avatar: None,
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
                height: None,
                width: None,
                proxy_url: None,
                ephemeral: None,
            }],
            embeds: vec![Embed {
                kind: Some("rich".into()),
                title: Some("title".into()),
                description: Some("desc".into()),
                url: Some("https://example.invalid/x".into()),
                timestamp: None,
                color: None,
                provider: None,
                author: None,
                footer: None,
                image: None,
                thumbnail: None,
                fields: vec![],
            }],
            message_reference: Some(MessageReference {
                kind: Some(0),
                message_id: Some("42".into()),
                channel_id: Some("456".into()),
                guild_id: Some("789".into()),
                fail_if_not_exists: None,
            }),
            kind: 19,
            raw_content: None,
            mention_everyone: Some(false),
            mentions: vec![User {
                id: "222".into(),
                username: Some("bob".into()),
                global_name: None,
                bot: None,
                discriminator: None,
                avatar: None,
            }],
            mention_roles: vec!["333".into()],
            referenced_message: None,
            referenced_message_status: None,
            thread: None,
            message_snapshots: vec![],
            reactions: vec![Reaction {
                count: Some(2),
                count_details: None,
                me: Some(false),
                emoji: Some(Emoji {
                    id: None,
                    name: Some("👍".into()),
                    animated: None,
                }),
            }],
            pinned: Some(true),
            flags: Some(0),
            application_id: None,
            webhook_id: None,
            interaction: None,
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
        assert_eq!(json["content_kind"], "text");
        assert_eq!(json["message_type"], 19);
        assert_eq!(json["reply_to"]["message_id"], "42");
        assert_eq!(json["reply_to"]["status"], "unknown");
        assert_eq!(json["mentions"][0]["id"], "222");
        assert_eq!(json["mention_roles"][0], "333");
        assert_eq!(json["mention_everyone"], false);
        assert_eq!(json["attachments"][0]["filename"], "notes.txt");
        assert_eq!(json["attachments"][0]["size"], 12345);
        assert_eq!(json["embeds"][0]["title"], "title");
        assert_eq!(json["reactions"][0]["name"], "👍");
        assert_eq!(json["reactions"][0]["count"], 2);
        assert_eq!(json["pinned"], true);
        // Normalized output never leaks raw API fields.
        for banned in [
            "message_reference",
            "components",
            "sticker_items",
            "raw_content",
        ] {
            assert!(
                json.get(banned).is_none(),
                "field {banned} must not be exposed"
            );
        }
    }

    #[test]
    fn content_kind_distinguishes_empty_cases() {
        let mut m = sample_message();
        m.content = "".into();
        m.kind = 0;
        m.attachments = vec![];
        m.embeds = vec![];
        assert_eq!(MessageView::content_kind(&m), "empty");

        m.attachments = vec![Attachment::default()];
        assert_eq!(MessageView::content_kind(&m), "attachment_only");

        m.attachments = vec![];
        m.embeds = vec![Embed::default()];
        assert_eq!(MessageView::content_kind(&m), "embed_only");

        m.embeds = vec![];
        m.kind = 7;
        assert_eq!(MessageView::content_kind(&m), "system");

        m.kind = 0;
        m.raw_content = Some(serde_json::json!({"unexpected": true}));
        assert_eq!(MessageView::content_kind(&m), "unknown");

        m.raw_content = Some(serde_json::json!(""));
        assert_eq!(MessageView::content_kind(&m), "empty");
    }

    #[test]
    fn resolved_reference_is_inlined() {
        let mut m = sample_message();
        let mut target = sample_message();
        target.id = "42".into();
        target.content = "the original".into();
        target.message_reference = None;
        m.referenced_message = Some(Box::new(target));
        let json = serde_json::to_value(MessageView::from(&m)).unwrap();
        assert_eq!(json["reply_to"]["status"], "resolved");
        assert_eq!(json["reply_to"]["referenced"]["content"], "the original");
    }

    #[test]
    fn author_falls_back_to_username_then_id() {
        let mut u = User {
            id: "111".into(),
            username: Some("example".into()),
            global_name: None,
            bot: None,
            discriminator: None,
            avatar: None,
        };
        assert_eq!(u.display_name(), "example");
        u.username = None;
        assert_eq!(u.display_name(), "111");
    }
}

# discord-reader — Discord read-only MCP server (Rust)

> [!WARNING]
> This project is 100% AI Generated. Use at your own risk.

自分の Discord ユーザーアカウントで **閲覧できるメッセージだけ** を、MCP クライアント
(ChatGPT / Dots / ローカルエージェント) から探索できる read-only MCP サーバーです。

**Discord への書き込み機能は一切ありません。** `send_message` / `edit_message` /
`delete_message` / reaction / join / leave / profile / friend request / typing /
webhook はコードレベルで存在しません。HTTP クライアントの最下層で `GET` 以外を
拒否し、任意 URL へリクエストする API も公開していません。

```text
Discord
   ↑ │ read-only (GET only)
   │
discord-reader-daemon   ← Discord credential を持つ唯一のプロセス
   │ Unix domain socket (JSON Lines RPC, 0660)
   ▼
discord-reader-mcp      ← credential なし。MCP tools を公開
   │ Streamable HTTP  http://discord-mcp:3000/mcp
   ▼
OpenAI tunnel-client (ghcr.io/openai/tunnel-client:v0.0.15)
   │ outbound HTTPS :443 のみ
   ▼
Secure MCP Tunnel → ChatGPT
```

---

## セキュリティモデル

| 要件 | 実装 |
| --- | --- |
| 完全 read-only | `discord-api::endpoints::DiscordRequest` が GET エンドポイントの allowlist。`POST`/`PUT`/`PATCH`/`DELETE` は型として表現できず、`client::ensure_read_only()` が最下層で全リクエスト前に再検証 |
| 任意 URL なし | URL は `API_BASE + DiscordRequest::path()` からのみ生成。識別子は snowflake (数字) 検証でパストラバーソルを遮断 |
| credential 分離 | Discord token は `discord-reader-daemon` のみが読む。`discord-mcp` / `tunnel-client` には mount しない |
| credential 漏洩防止 | `Token` 型は `Debug`/`Display`/`Serialize` を `[REDACTED]` に。ログ・RPC・エラー文言に credential を一切出さない (テストで担保) |
| 書き込み tool なし | MCP tool は read-only 10 個のみ。tool 名に write 動詞が含まれないことをテストで検証 |
| 任意 SQL / 任意 FS なし | MCP tool は RPC メソッドのみ経由。SQL も固定ステートメント |
| ネットワーク分離 | `discord-mcp` は internal network のみ → Internet / Discord / OpenAI に到達不能。`discord-reader` は Discord API、`tunnel-client` は api.openai.com への outbound のみ |
| rate limit 尊重 | 同時 HTTP リクエスト 4 (設定可)、`X-RateLimit-*` と `Retry-After` を尊重。固定 sleep でのごまかしなし、全チャンネル同時クロールなし |

### リスクについて (重要)

本構成は **ユーザーアカウントの credential** を利用するため、Discord の self-bot 規約上
リスクがあります。このリポジトリではリスクを増やさないため以下を徹底しています。

- 書き込み API を実装しない (存在させない)
- 全履歴の自動クロール・常時巡回・バックフィルを行わない (要求された分だけ遅延取得)
- 自動化を増やすイベント購読 (Gateway) は実装しない
- 429 を必ず尊重し、同時リクエスト数を小さく保つ

利用は自己責任で、Discord の利用規約・アカウント運用ポリシーを確認してください。

### 閲覧しても既読になりません

Discord の既読状態 (未読バッジ) は `POST /channels/{channel.id}/messages/{message.id}/ack`
等の ack エンドポイントで更新されます。本実装は `GET` のみで ack 系の API を一切持たないため、

- このツールでメッセージを読んでも **既読は付きません**
- PC / スマホの Discord クライアントの **未読バッジ・通知は消えません**
- ローカルキャッシュ済みのメッセージは再読時にも Discord にアクセスしません

「見たこと」と「既読」が完全に分離されるため、過去ログの探索や実況確認をしても
普段の Discord 利用に影響しません。逆に、既読化したい場合は `POST .../ack` の実装が
必要になり read-only 保証と衝突するため、意図的に実装していません。

---

## クイックスタート (Docker Compose)

```bash
mkdir -p secrets

$EDITOR secrets/discord_token           # Discord のユーザートークンを 1 行で
$EDITOR secrets/openai_tunnel_api_key   # OpenAI の runtime API key を 1 行で

cp .env.example .env
$EDITOR .env                            # OPENAI_TUNNEL_ID を設定

docker compose up -d --build
```

状態確認:

```bash
docker compose ps
```

```text
discord-reader    healthy
discord-mcp       healthy
tunnel-client     healthy
```

運用コマンド:

```bash
docker compose restart              # 再起動
docker compose pull && docker compose up -d --build   # 更新 (tunnel-client の新 tag 等)
docker compose logs -f discord-reader discord-mcp tunnel-client
docker compose down                 # 停止
```

デバッグ用に localhost だけへポート公開する場合 (production では使わない):

```bash
docker compose -f docker-compose.yaml -f compose.dev.yaml up -d --build
# 127.0.0.1:3000 → /mcp, /healthz, /readyz
# 127.0.0.1:8080 → tunnel-client の /healthz /readyz /metrics /ui
```

> `compose.dev.yaml` はわざと `compose.override.yaml` という名前にしていません。
> Docker Compose は `compose.override.yaml` を自動マージするため、その名前だと
> production でもポートが公開されてしまいます。

### 起動順序と再接続

`depends_on: condition: service_healthy` で `discord-reader → discord-mcp → tunnel-client`
の順に ready になりますが、順序に依存しない実装でもあります。

- `discord-reader-mcp` は RPC コールごとに Unix socket へ接続し、失敗時に 1 回再試行 →
  daemon の再起動に自動追従
- `tunnel-client` は `--mcp.startup-wait-timeout=60s` で初回ポーリング前に MCP の
  リスナー到達性を待ち、それでも未起動ならポーリングを継続

### health check

| service | probe | 意味 |
| --- | --- | --- |
| `discord-reader` | `discord-reader-daemon --healthcheck` (socket へ `ping`) | daemon が RPC 応答できる |
| `discord-mcp` | `discord-reader-mcp --healthcheck` (`GET /healthz`) | HTTP リスナーが生きている (`/readyz` は daemon の `ping` 結果を返す) |
| `tunnel-client` | `wget http://127.0.0.1:8080/healthz` (公式 health endpoint) | tunnel-client が生きている (`/readyz`, `/metrics`, `/ui` も同じ listener) |

---

## ChatGPT (Secure MCP Tunnel) セットアップ

Tunnel の作成と workspace への紐付けは **OpenAI Platform 側の one-time provisioning** です。
常駐する Compose stack に Admin API key を置かないでください。通常運用で使う credential は
**Tunnels Read + Use 権限の runtime API key** のみです。

1. OpenAI Platform → **Tunnels** で Secure MCP Tunnel を作成
   (CLI で作る場合: `tunnel-client admin tunnels create --name ... --organization-id ... --workspace-id ...`、
   `OPENAI_ADMIN_KEY` が必要。作成後 25〜30 秒待ってから利用)
2. 利用する ChatGPT の workspace をその Tunnel に associate
3. **Runtime API keys** から **Tunnels Read + Use** 権限の key を発行
4. `.env` に tunnel ID を設定: `OPENAI_TUNNEL_ID=tunnel_...`
5. runtime API key を `secrets/openai_tunnel_api_key` に保存 (1 行、改行のみ残す)
6. Discord credential を `secrets/discord_token` に保存
7. `docker compose up -d --build`
8. `docker compose logs -f tunnel-client` で接続を確認 (`/readyz` が 200 になれば ready)
9. ChatGPT: `Settings → Plugins → Developer mode app → Tunnel` から作成済み Tunnel を選択し、
   MCP tools が一覧できることを確認

イメージは `ghcr.io/openai/tunnel-client:v0.0.15` に **pin** しています。`latest` は使いません。
更新は renovate / dependabot の pull request 経由で行う想定です。

### (任意) provisioning プロファイル

Tunnel の作成を CLI で自動化したい場合のみ、one-shot サービスを使えます。
デフォルト profile では起動せず、admin key は常駐コンテナに渡りません。

```bash
$EDITOR secrets/openai_admin_key        # OpenAI Admin API key
# .env に OPENAI_ORG_ID / OPENAI_WORKSPACE_ID を設定
docker compose --profile provision run --rm tunnel-provision
```

終了後に表示される tunnel ID を `.env` の `OPENAI_TUNNEL_ID` に設定すれば、
admin key は不要になります (`secrets/openai_admin_key` を削除してください)。
手動作成がデフォルトの手順です。

---

## credential の設定

### Docker (推奨)

```text
secrets/
├── discord_token            # Discord のユーザートークン (1 行)
└── openai_tunnel_api_key    # OpenAI runtime API key (Tunnels Read + Use)
```

- `discord_token` は `discord-reader` にのみ mount (`DISCORD_TOKEN_FILE`)
- `openai_tunnel_api_key` は `tunnel-client` にのみ mount
- `discord-mcp` にはどちらも渡らない

### systemd

Linux では systemd credentials を第一候補とします。

```ini
LoadCredential=discord-token:/etc/discord-reader/token
```

daemon は `$CREDENTIALS_DIRECTORY/discord-token` から読みます。

```bash
sudo install -d -m 0750 /etc/discord-reader
sudo install -m 0400 -o root -g root token.txt /etc/discord-reader/token
sudo install -m 0644 systemd/discord-reader-daemon.service /etc/systemd/system/
sudo install -m 0644 systemd/discord-reader-mcp.service /etc/systemd/system/
sudo systemctl enable --now discord-reader-daemon discord-reader-mcp
```

unit は `NoNewPrivileges` / `PrivateTmp` / `ProtectSystem=strict` / `ProtectHome` /
`RestrictAddressFamilies` などで hardening 済みです。

### 開発用 (非推奨)

環境変数 `DISCORD_TOKEN` でも起動できますが、本番では非推奨です (プロセス環境は
`/proc` から読める可能性があるため)。読み込み順序は次の通り:

1. `DISCORD_TOKEN_FILE` (ファイルパス)
2. `$CREDENTIALS_DIRECTORY/discord-token` (systemd)
3. `DISCORD_TOKEN` (開発用)

`DISCORD_TOKEN_KIND=user` (デフォルト) で Authorization ヘッダはトークンそのまま、
`bot` の場合は `Bot ` 付きになります。

---

## MCP tools

すべて read-only です。出力は巨大な生 JSON ではなく正規化された最小限のフィールドです。

| tool | 引数 | 概要 |
| --- | --- | --- |
| `get_me` | — | 自分の ID / ユーザー名 (`{"me":{...}}`) |
| `get_capabilities` | — | 認証方式ごとの対応可否・制約の一覧 |
| `list_guilds` | — | 参加中サーバーの列挙 `{"guilds":[{"id","name"}]}` |
| `list_channels` | `guild_id` | チャンネル列挙 (id / name / kind / parent_id / topic / guild_id / last_message_id / last_activity_at) |
| `list_dms` | — | DM / Group DM の列挙 (channel_id / participants / last_message_id) |
| `list_changed_channels` | `guild_id?`, `limit=50`, `cursor?` | 最終投稿 ID が前回同期より新しいチャンネルのみ |
| `recent_messages` | `channel_id`, `limit=50` (1..=100) | 最近のメッセージ取得 |
| `messages_before` | `channel_id`, `before_message_id`, `limit=50` | 過去方向ページング |
| `messages_after` | `channel_id`, `after_message_id`, `limit=50` | 差分取得 (前回以降の新着) |
| `get_message` | `channel_id`, `message_id` | メッセージ 1 件 (返信先を可能な限り解決して内包) |
| `message_context` | `channel_id`, `message_id`, `before=20`, `after=20` (各 0..=50) | 指定メッセージと前後の会話 |
| `get_message_raw` | `channel_id`, `message_id` | 元 API レスポンスの原文 + 正規化結果 (空本文調査用) |
| `get_attachment` | `channel_id`, `message_id` | 添付ファイルのメタデータと取得 URL |
| `get_message_events` | `channel_id`, `message_id` | メッセージに紐づく予定イベント |
| `list_mentions` | `guild_id?`, `channel_id?`, `after?`, `before?`, `limit=100`, `refresh?` | 自分宛てメンション受信箱 (direct / reply / role / everyone を区別) |
| `list_replies` | 同上 | 自分への返信の一覧 |
| `search_messages` | `query`, `guild_id?`, `channel_id?`, `author_id?`, `after?`, `before?`, `limit=50`, `refresh?`, `next_cursor?` | SQLite FTS5 による全文検索 (キャッシュ対象) |
| `search_server_side` | `query`, `channel_id`, `offset`, `limit`, `sort?`, `sort_order?`, `next_cursor?` | Discord 検索 (user account ではチャンネル限定) |
| `read_thread` | `thread_id`, `limit=100` | スレッド読み取り |
| `list_threads` | `guild_id?`, `channel_id?`, `filter?` (active/archived/joined/all), `limit=50`, `cursor?` | スレッド列挙 (アクティブ / アーカイブ / 参加済み) |
| `get_member` | `guild_id`, `user_id?` | サーバー内の自分の member / roles |
| `get_sync_status` | — | チャンネル別の同期位置・確認範囲・キャッシュ状態 |
| `start_sync` | `scope?`, `guild_id?`, `channel_ids?` | 差分同期ジョブの開始 (新着チャンネルのみ巡回) |
| `get_sync_progress` | `job_id` | 同期ジョブの進捗 (成功 / 失敗 / 次カーソル) |

メッセージの正規化スキーマ (v2):

```json
{
  "id": "123",
  "channel_id": "456",
  "guild_id": "789",
  "author": {"id": "111", "name": "example"},
  "timestamp": "2026-10-05T00:00:00.000000+00:00",
  "edited_timestamp": null,
  "message_type": 19,
  "content": "message",
  "content_kind": "text",
  "mentions": [{"id": "222", "name": "bob"}],
  "mention_roles": ["333"],
  "mention_everyone": false,
  "reply_to": {"message_id": "42", "status": "resolved", "referenced": {"id": "42", "content": "...", "author": {"id": "111", "name": "example"}}},
  "thread": {"id": "789", "name": "...", "parent_id": "456", "archived": false},
  "attachments": [
    {"filename": "notes.txt", "url": "https://cdn.discordapp.com/...", "content_type": "text/plain", "size": 12345}
  ],
  "embeds": [
    {"title": "title", "description": "desc", "url": "https://..."}
  ],
  "reactions": [{"name": "👍", "count": 2}],
  "pinned": false
}
```

フィールドの意味と欠損理由:

- `content` は Discord の本文そのまま。空文字は「本当にテキストが無い」場合のみ。
- `content_kind` は空本文の理由: `text` / `empty` / `attachment_only` /
  `embed_only` / `system` / `forwarded` / `unknown` (正規化前の原文が非文字列)。
- `reply_to` は非返信なら `null`。返信なら `status` で参照先の状態を返す:
  `resolved` (本文を内包) / `deleted` / `forbidden` / `unknown` (未取得)。
- `mentions` / `mention_roles` / `mention_everyone` は API のまま。ロール名の
  解決は `get_member` や `list_mentions` の `matched_role_ids` を使う。
- 添付はメタデータ + URL のみ。ファイル本体の取得は `get_attachment`。

すべての一覧・検索・履歴取得は `next_cursor` / `has_more` を返し、取得した
確認範囲は `covered_from` / `covered_to` / `has_gaps` で明示します。

### 受け入れテストの流れ

```text
「ZENVRというサーバーを探して」      → list_guilds
「generalチャンネルを探して」        → list_channels
「最近の発言を50件読んで」           → recent_messages
「展軸祭について話していたメッセージを探して」 → search_messages
「その発言の前後20件を読んで」       → message_context
```

### 受け入れ基準 (自分宛て受信箱・差分同期)

1. **未取得サーバーの自分宛てメンションを発見** — `list_mentions`
   (`refresh: true`) が新着チャンネルだけ取得し、`matched_by: "direct"` を返す。
2. **返信先を取得** — `list_replies` / `get_message` が
   `message.reply_to.referenced` に参照先本文と投稿者を内包し、削除済み・
   権限不足・未取得は `reply_to.status` で区別する。
3. **ロールメンションの本人適用を判定** — `get_member` のロール一覧と
   `matched_by: "role"` の `matched_role_ids` を突き合わせて判定できる。
4. **再起動後に続きから同期** — `messages_after` / `start_sync` が保存済み
   カーソル (`get_sync_status` の `last_synced_message_id`) から再開する。
5. **編集・削除・取得不能を新着なしと区別** — 編集は `edited_timestamp`、
   削除は tombstone (`get_sync_status` の `deletions_observed`)、取得失敗は
   `channels_failed` の構造化エラーで返り、「新着なし」と混同されない。
6. **確認済み範囲を数値で提示** — `coverage` の `covered_from` / `covered_to`
   / `has_gaps` と `get_sync_status` の `cached_messages` / `channels_tracked`。

読むだけの操作では Discord の既読状態が変わらないことは read-only 保証
(GET のみ・ack API なし) で担保しています。

### 空本文の診断手順

取得は成功したのに `content` が空、という場合の切り分け:

1. 同じメッセージ ID で `get_message_raw` を呼ぶ。`raw` が Discord の
   元レスポンス、`normalized` が MCP 返却、Discord 画面と三者照合できる。
2. `raw.content` が本当に空で `fields_present` に `attachments` / `embeds` /
   `message_snapshots` / `type` があれば、正当なシステム投稿・転送・
   添付のみの可能性が高い (`content_kind` が `system` / `forwarded` /
   `attachment_only` / `embed_only` になる)。
3. `raw.content` に本文があるのに `normalized.content` が空なら正規化の欠落。
   この場合は不具合として報告してください。
4. `raw` 自体が取れない (403 / 20002 / transport) なら取得失敗であり、
   空本文として扱ってはいけない。`error_source` / `discord_code` /
   `retryable` が原因を返す。

---

## ローカル開発

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

daemon + MCP を手元で動かす:

```bash
# 1. daemon (別ターミナル)
DISCORD_TOKEN_FILE=./secrets/discord_token \
DISCORD_READER_SOCKET=/tmp/discord-reader.sock \
DATABASE_URL=/tmp/discord.sqlite3 \
cargo run -p discord-reader-daemon

# 2. MCP (別ターミナル)
DISCORD_READER_SOCKET=/tmp/discord-reader.sock \
MCP_LISTEN_ADDR=127.0.0.1:3000 \
cargo run -p discord-reader-mcp
```

動作確認:

```bash
curl -s http://127.0.0.1:3000/healthz
curl -s http://127.0.0.1:3000/readyz

curl -s -X POST http://127.0.0.1:3000/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
```

### MCP client への登録例

**OpenCode** (`~/.config/opencode/opencode.json`):

```json
{
  "mcp": {
    "discord": {
      "type": "remote",
      "url": "http://127.0.0.1:3000/mcp",
      "enabled": true
    }
  }
}
```

**Claude Desktop / Cursor** など Streamable HTTP 対応クライアントでも
`http://127.0.0.1:3000/mcp` を指定してください。stdio 経由で使いたい場合は
`mcp-proxy` 等で Streamable HTTP に変換してください (本リポジトリは stdio を
実装していません)。

ChatGPT の場合は上記「ChatGPT (Secure MCP Tunnel) セットアップ」の通りです。

---

## データベースとキャッシュ

SQLite (`DATABASE_URL`, Docker では `/data/discord.sqlite3`) にキャッシュします。

既存DBは起動時にバックアップを取得してトランザクション内でv2へ移行します。
旧行のメンション・返信情報は要再取得として区別し、同期範囲は推測しません。
接続先・バージョン・移行履歴の確認方法と制約は
[SQLite cache upgrades](docs/database-upgrades.md) を参照してください。
**credential は保存しません。**

```sql
CREATE TABLE guilds (id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE channels (id TEXT PRIMARY KEY, guild_id TEXT, name TEXT,
                       kind INTEGER NOT NULL, parent_id TEXT, topic TEXT);
CREATE TABLE users (id TEXT PRIMARY KEY, username TEXT, global_name TEXT);
CREATE TABLE messages (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, guild_id TEXT,
                       author_id TEXT, timestamp TEXT NOT NULL,
                       edited_timestamp TEXT, content TEXT NOT NULL);
CREATE VIRTUAL TABLE messages_fts USING fts5(message_id UNINDEXED, content,
                                             tokenize = 'trigram');
```

- メッセージと FTS 行はアプリケーションコードで同一トランザクションに書き込み、
  再取得時に FTS 行を置換 (編集の反映)
- 取得は遅延的: `recent_messages` 等で要求された分だけ Discord へ取りに行って保存
- 全チャンネルのバックフィル・常時クロールは行わない

### Lazy fetching

```text
recent_messages 要求 → Discord GET → SQLite 保存 → 返却
messages_before 要求 → before=<message_id> GET → 保存 → 返却
search_messages 要求 → SQLite FTS 検索 → 結果を返却
                        (refresh=true かつ channel_id 指定時のみそのチャンネルを追加取得)
```

### 検索について

- FTS5 の **trigram** トークナイザを使用。日本語など空白で区切られない言語でも
  部分一致でヒットします (3 文字以上)。
- 3 文字未満のクエリや、FTS がヒットしない場合は `LIKE` の部分一致にフォールバック。
- クエリ内の FTS 構文 (`"`, `*`, `NEAR`, `OR` など) はエスケープして文字列として扱います。
- `relevance` は FTS5 `bm25()` の符号反転値 (大きいほど良い)。
- **検索できるのはキャッシュ済みメッセージだけです。** 未取得の履歴は
  `messages_before` 等で取得してから検索してください。

---

## Unix socket RPC

`discord-reader-daemon` と `discord-reader-mcp` の間は JSON Lines over Unix domain socket
(`0660`, 所有者は同一 UID) です。

```json
{"id":1,"method":"recent_messages","params":{"channel_id":"123","limit":50}}
{"id":1,"result":{"channel_id":"123","messages":[...]}}
{"id":2,"error":{"code":-32601,"message":"unknown method: send_message"}}
```

メソッドは `ping` / `list_guilds` / `list_channels` / `recent_messages` /
`messages_before` / `get_message` / `message_context` / `search_messages` /
`read_thread` / `list_threads` / `list_dms` のみ。書き込みメソッドは存在しません。

---

## ログ

`tracing` を使用し、以下は出力しません。

- Discord credential / Authorization ヘッダ (debug レベルでも redact)
- HTTP リクエストヘッダ一式
- DM 本文の不必要に多いダンプ (取得したメッセージをログに流さない)

`RUST_LOG` でレベル調整できます (例: `RUST_LOG=debug`)。

---

## テスト

| 領域 | 内容 |
| --- | --- |
| HTTP write 防止 | `POST`/`PUT`/`PATCH`/`DELETE` は必ずエラー。`DiscordRequest` は全 variant が GET、パスは相対・スキーマ/`..` なし、悪意ある ID は拒否 |
| credential 漏洩 | `Token` / `DiscordClient` / `DaemonApi` の Debug・Display・Serialize・RPC 出力・エラー文言に credential が含まれない |
| FTS | 保存したメッセージのキーワード検索 (日本語/英語)、複数語 AND、FTS 構文の無害化、フィルタ |
| pagination | `before` message ID による過去ページング (SQLite + HTTP 両方) |
| RPC | ping、未知メソッド拒否、limit 検証、ソケット権限 (0660)、socket round trip |
| MCP | 全 tool の存在と read-only 性 (write 動詞なし)、input schema の必須項目、limit 検証、RPC クライアントの挙動 |

---

## 設計判断 (合理的デフォルト)

- **依存**: tokio / reqwest(rustls) / serde / thiserror / tracing / rusqlite(bundled,
  FTS5 有効) / rmcp(MCP 公式 Rust SDK) / axum。Discord 専用 crate は使わず、
  必要な read-only エンドポイントだけを reqwest で実装。
- **MCP transport**: Streamable HTTP (`POST /mcp`)。stdio は非対応 (tunnel-client の
  要件が HTTP のため)。
- **RPC**: JSON Lines。単純でデバッグしやすく、Unix socket の権限制御と組み合わせる。
- **`limit` の上限**: `recent_messages` / `messages_before` / `read_thread` は 100、
  `search_messages` は 200、`message_context` の前後は各 50。範囲外はツールエラー。
- **同時 HTTP リクエスト数**: 4 (`DISCORD_MAX_CONCURRENT_REQUESTS` で変更可)。
- **`list_guilds`**: Discord API の 1 ページ (最大 200 サーバー) のみ取得。ページング
  パラメータは RPC に持たせていない (参加サーバー数が 200 を超えるケースが稀なため)。
- **タイムスタンプの比較**: ISO-8601 文字列の辞書順 (Discord の形式 `...+00:00` で
  正規化済みのため時系列順と一致)。`after` / `before` は包含境界。
- **`message_context` の順序**: `before` / `after` とも古い順に整列し、
  `before ++ message ++ after` で会話として読めるようにしている。
- **`refresh`**: `search_messages` の追加取得は明示オプトイン。想定外の通信を発生させない。

---

## 未実装 / 今後の作業

- Gateway (WebSocket) イベント受信 (自動化を増やすため意図的に未実装)
- `list_guilds` のページネーション (`before` / `after`) の MCP 暴露
- アーカイブ済みスレッド (`/channels/{id}/threads/archived/*`) の列挙
- スレッド全量バックフィル、スレッドメンバー情報
- 添付ファイル本体の取得 (メタデータのみ対応)
- Embed のフィールド / 画像 / 動画などの詳細抽出 (テキストのみ対応)
- 複数ボット/アカウントの同時運用

---

## Discord API 上の既知の制約

- ユーザートークンでの利用は Discord 規約上のリスクあり (冒頭参照)。
- `GET /users/@me/guilds` は 1 リクエスト最大 200 件。全件取得はページングが必要。
- `GET /channels/{id}/messages` は 1 リクエスト最大 100 件。
- user account の rate limit は bot より厳しく、違反するとロックアウトされやすい。
  本実装は `Retry-After` を尊重し、同時リクエストを 4 に制限している。
- `search_messages` はローカルキャッシュ検索であり、Discord のサーバーサイド検索
  (`GET /guilds/{id}/messages/search` は user account で利用不可) は使っていない。
- メッセージ ID (snowflake) は時系列順と一致するが、桁数が異なる ID の辞書順比較は
  行わず、数値比較で整列している。

//! `discord-reader-daemon` — the only process that holds a Discord credential.
//!
//! Responsibilities:
//! * read-only Discord REST access (GET only, allowlisted endpoints),
//! * rate limit handling,
//! * lazy SQLite caching + FTS index maintenance,
//! * read-only JSON-Lines RPC over a Unix domain socket.
//!
//! The credential is loaded from a file (Docker secret / systemd credential)
//! and lives only in memory. It is never logged, never written to SQLite, and
//! never returned over RPC.

use std::{path::PathBuf, process::ExitCode, sync::Arc};

use anyhow::Context;
use discord_api::{DiscordClient, Token, TokenKind};
use discord_reader_daemon::{rpc, DaemonApi};
use discord_store::Store;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_SOCKET: &str = "/run/discord-reader/reader.sock";
const DEFAULT_DATABASE: &str = "/data/discord.sqlite3";

#[derive(Debug)]
struct Options {
    socket: PathBuf,
    database: PathBuf,
    healthcheck: bool,
}

fn usage() -> String {
    format!(
        "discord-reader-daemon {}\n\
         \n\
         Usage: discord-reader-daemon [OPTIONS]\n\
         \n\
         Options:\n\
           --socket <PATH>     Unix socket path (env: DISCORD_READER_SOCKET, default: {DEFAULT_SOCKET})\n\
           --database <PATH>   SQLite cache path  (env: DATABASE_URL, default: {DEFAULT_DATABASE})\n\
           --healthcheck       Ping the running daemon over the socket and exit\n\
           --help              Show this help\n\
           --version           Show version\n\
         \n\
         Credential sources (checked in order):\n\
           1. DISCORD_TOKEN_FILE        path to a file containing the token\n\
           2. $CREDENTIALS_DIRECTORY/discord-token   (systemd LoadCredential)\n\
           3. DISCORD_TOKEN             environment variable (development only)\n",
        env!("CARGO_PKG_VERSION")
    )
}

fn parse_args() -> anyhow::Result<Options> {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from<I: Iterator<Item = String>>(args: I) -> anyhow::Result<Options> {
    let mut socket = std::env::var("DISCORD_READER_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_SOCKET));
    let mut database = std::env::var("DATABASE_URL")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_DATABASE));
    let mut healthcheck = false;

    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = PathBuf::from(args.next().context("--socket requires a value")?);
            }
            "--database" => {
                database = PathBuf::from(args.next().context("--database requires a value")?);
            }
            "--healthcheck" => healthcheck = true,
            "--help" | "-h" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            "--version" | "-V" => {
                println!("discord-reader-daemon {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}\n\n{}", usage()),
        }
    }

    Ok(Options {
        socket,
        database,
        healthcheck,
    })
}

/// Load the Discord credential.
///
/// Order:
/// 1. `DISCORD_TOKEN_FILE` — file path (Docker secret, explicit override),
/// 2. `$CREDENTIALS_DIRECTORY/discord-token` — systemd `LoadCredential=`,
/// 3. `DISCORD_TOKEN` — environment variable, development only.
///
/// The returned string is the only copy of the credential kept in the process.
fn load_credential() -> anyhow::Result<String> {
    if let Ok(path) = std::env::var("DISCORD_TOKEN_FILE") {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read DISCORD_TOKEN_FILE ({path})"))?;
        info!(source = "token-file", "loaded discord credential");
        return Ok(raw);
    }

    if let Ok(dir) = std::env::var("CREDENTIALS_DIRECTORY") {
        let path = PathBuf::from(dir).join("discord-token");
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .context("failed to read systemd credential discord-token")?;
            info!(source = "systemd-credential", "loaded discord credential");
            return Ok(raw);
        }
    }

    if let Ok(raw) = std::env::var("DISCORD_TOKEN") {
        warn!(
            "loaded discord credential from the DISCORD_TOKEN environment variable; \
             prefer DISCORD_TOKEN_FILE or systemd credentials in production"
        );
        return Ok(raw);
    }

    anyhow::bail!(
        "no discord credential found: set DISCORD_TOKEN_FILE, \
         configure systemd LoadCredential=discord-token, or (development only) DISCORD_TOKEN"
    )
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

async fn run_healthcheck(socket: &std::path::Path) -> ExitCode {
    match rpc_healthcheck(socket).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("healthcheck failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn rpc_healthcheck(socket: &std::path::Path) -> anyhow::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("cannot connect to {}", socket.display()))?;
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    let request = serde_json::json!({"id": 1, "method": "ping", "params": {}});
    let mut payload = serde_json::to_string(&request)?;
    payload.push('\n');
    write_half.write_all(payload.as_bytes()).await?;
    write_half.flush().await?;

    let line = lines
        .next_line()
        .await?
        .context("daemon closed the connection without responding")?;
    let response: rpc::RpcResponse =
        serde_json::from_str(&line).context("daemon returned invalid JSON")?;
    match (response.result, response.error) {
        (Some(result), None) if result.get("ok").and_then(|v| v.as_bool()) == Some(true) => Ok(()),
        (Some(result), None) => anyhow::bail!("unexpected ping result: {result}"),
        (_, Some(error)) => anyhow::bail!("ping failed: {error}"),
        (None, None) => anyhow::bail!("ping returned neither result nor error"),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let options = match parse_args() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("error: {error:#}");
            return ExitCode::FAILURE;
        }
    };

    if options.healthcheck {
        return run_healthcheck(&options.socket).await;
    }

    match run(options).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "daemon failed");
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(options: Options) -> anyhow::Result<()> {
    let raw_token = load_credential()?;
    let token = Token::new(raw_token).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let token_kind = match std::env::var("DISCORD_TOKEN_KIND")
        .unwrap_or_else(|_| "user".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "bot" => TokenKind::Bot,
        _ => TokenKind::User,
    };

    let client = DiscordClient::with_options(
        token,
        token_kind,
        discord_api::endpoints::API_BASE.to_string(),
        std::env::var("DISCORD_MAX_CONCURRENT_REQUESTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4),
    )
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let client: Arc<DiscordClient> = Arc::new(client);

    let store = Store::open(&options.database).with_context(|| {
        format!(
            "failed to open cache database {}",
            options.database.display()
        )
    })?;
    info!(database = %options.database.display(), "cache ready");

    let listener = rpc::bind_socket(&options.socket)
        .await
        .with_context(|| format!("failed to bind {}", options.socket.display()))?;
    store.interrupt_account_jobs(&discord_store::sqlite::now_iso())?;
    let api = Arc::new(DaemonApi::new(client, Arc::new(store)));
    info!(socket = %options.socket.display(), "read-only rpc listening");

    let server = tokio::spawn(rpc::serve(listener, api));

    wait_for_shutdown().await;
    info!("shutting down");
    server.abort();
    let _ = tokio::fs::remove_file(&options.socket).await;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_are_parsed() {
        let options = parse_args_from(
            [
                "--socket",
                "/tmp/x.sock",
                "--database",
                "/tmp/x.db",
                "--healthcheck",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert_eq!(options.socket, PathBuf::from("/tmp/x.sock"));
        assert_eq!(options.database, PathBuf::from("/tmp/x.db"));
        assert!(options.healthcheck);
    }

    #[test]
    fn unknown_args_are_rejected() {
        let error = parse_args_from(["--send-message"].into_iter().map(str::to_string))
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown argument"));
    }

    #[test]
    fn usage_mentions_credential_sources_but_no_values() {
        let usage = usage();
        assert!(usage.contains("DISCORD_TOKEN_FILE"));
        assert!(usage.contains("CREDENTIALS_DIRECTORY"));
        assert!(!usage.contains("sk-"));
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn credential_is_loaded_from_file() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "SECRET-VALUE-123456\n").unwrap();
        std::env::set_var("DISCORD_TOKEN_FILE", &path);
        let raw = load_credential().expect("credential loads from file");
        std::env::remove_var("DISCORD_TOKEN_FILE");
        assert_eq!(raw.trim(), "SECRET-VALUE-123456");
    }
}

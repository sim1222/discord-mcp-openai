//! `discord-reader-mcp` — MCP server with read-only Discord tools.
//!
//! Security posture:
//! * this process holds **no** Discord credential; it talks to
//!   `discord-reader-daemon` over a Unix domain socket,
//! * the Unix socket protocol is read-only and has no write methods,
//! * the MCP tool surface contains no write operations,
//! * the HTTP surface is `POST /mcp` (Streamable HTTP) plus health endpoints.

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc};

use anyhow::Context;
use axum::response::IntoResponse;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use serde_json::json;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod protocol;
mod tools;

use protocol::RpcClient;
use tools::DiscordReaderTools;

const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:3000";

struct Options {
    listen: SocketAddr,
    socket: PathBuf,
    allowed_hosts: Vec<String>,
    healthcheck: bool,
}

fn usage() -> String {
    format!(
        "discord-reader-mcp {}\n\
         \n\
         Usage: discord-reader-mcp [OPTIONS]\n\
         \n\
         Options:\n\
           --listen <ADDR>     HTTP listen address (env: MCP_LISTEN_ADDR, default: {DEFAULT_LISTEN_ADDR})\n\
           --socket <PATH>     daemon Unix socket (env: DISCORD_READER_SOCKET)\n\
           --healthcheck       Probe GET /healthz on the listen address and exit\n\
           --help              Show this help\n\
           --version           Show version\n\
         \n\
         This process never sees a Discord credential.\n\
         MCP endpoint: http://<listen>/mcp   health: /healthz, /readyz\n",
        env!("CARGO_PKG_VERSION")
    )
}

fn parse_args() -> anyhow::Result<Options> {
    let mut listen = std::env::var("MCP_LISTEN_ADDR")
        .unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string())
        .parse::<SocketAddr>()
        .context("MCP_LISTEN_ADDR must be ip:port")?;
    let mut socket = std::env::var("DISCORD_READER_SOCKET")
        .map(PathBuf::from)
        .context("DISCORD_READER_SOCKET must name the daemon Unix socket")?;
    let allowed_hosts = std::env::var("MCP_ALLOWED_HOSTS")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut healthcheck = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = args
                    .next()
                    .context("--listen requires a value")?
                    .parse()
                    .context("--listen must be ip:port")?;
            }
            "--socket" => {
                socket = PathBuf::from(args.next().context("--socket requires a value")?);
            }
            "--healthcheck" => healthcheck = true,
            "--help" | "-h" => {
                print!("{}", usage());
                std::process::exit(0);
            }
            "--version" | "-V" => {
                println!("discord-reader-mcp {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}\n\n{}", usage()),
        }
    }

    Ok(Options {
        listen,
        socket,
        allowed_hosts,
        healthcheck,
    })
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
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
        return run_healthcheck(options.listen).await;
    }

    match serve(options).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "mcp server failed");
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(options: Options) -> anyhow::Result<()> {
    let client = Arc::new(RpcClient::new(options.socket.clone()));

    let config = if options.allowed_hosts.is_empty() {
        // The MCP endpoint is only reachable on the private Docker network.
        StreamableHttpServerConfig::default().disable_allowed_hosts()
    } else {
        StreamableHttpServerConfig::default().with_allowed_hosts(options.allowed_hosts.clone())
    };

    let service = StreamableHttpService::new(
        {
            let client = Arc::clone(&client);
            move || Ok(DiscordReaderTools::new(Arc::clone(&client)))
        },
        Arc::new(LocalSessionManager::default()),
        config,
    );

    let app = axum::Router::new()
        .nest_service("/mcp", service)
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(readyz))
        .with_state(client);

    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .with_context(|| format!("failed to bind {}", options.listen))?;
    info!(
        listen = %options.listen,
        socket = %options.socket.display(),
        "mcp server listening (read-only, no discord credential)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

/// Liveness: the process is up.
async fn healthz() -> axum::response::Response {
    axum::response::IntoResponse::into_response((axum::http::StatusCode::OK, "ok"))
}

/// Readiness: the daemon answers `ping` over the Unix socket.
async fn readyz(
    axum::extract::State(client): axum::extract::State<Arc<RpcClient>>,
) -> axum::response::Response {
    match client.call("ping", json!({})).await {
        Ok(result) if result.get("ok").and_then(|v| v.as_bool()) == Some(true) => {
            (axum::http::StatusCode::OK, "ready").into_response()
        }
        Ok(other) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("unexpected ping result: {other}"),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("daemon unavailable: {error}"),
        )
            .into_response(),
    }
}

async fn run_healthcheck(listen: SocketAddr) -> ExitCode {
    match http_get(listen, "/healthz").await {
        Ok(200) => ExitCode::SUCCESS,
        Ok(status) => {
            eprintln!("healthcheck failed: /healthz returned {status}");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("healthcheck failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Tiny HTTP GET used only by `--healthcheck` (no HTTP client dependency).
async fn http_get(listen: SocketAddr, path: &str) -> anyhow::Result<u16> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut stream = tokio::net::TcpStream::connect((listen.ip(), listen.port()))
        .await
        .with_context(|| format!("cannot connect to {listen}"))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut lines = BufReader::new(stream).lines();
    let status_line = lines.next_line().await?.context("no HTTP response")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .context("malformed status line")?
        .parse::<u16>()
        .context("malformed status code")?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_states_no_credential() {
        let usage = usage();
        assert!(
            usage.contains("no Discord credential")
                || usage.contains("never sees a Discord credential")
        );
        assert!(!usage.contains("sk-"));
    }
}

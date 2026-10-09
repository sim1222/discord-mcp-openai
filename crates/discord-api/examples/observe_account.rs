//! Observe Gateway read-state availability without printing account data.

use discord_api::{account_observation::AccountObserver, DiscordClient, Token};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token_file = std::env::args().nth(1).ok_or("expected token file path")?;
    let token = Token::new(std::fs::read_to_string(token_file)?)?;
    let snapshot = DiscordClient::new(token)?.observe_account().await?;
    println!(
        "{}",
        serde_json::json!({
            "read_state_entries":snapshot.read_states.len(),
            "channels_observed":snapshot.channels.len(),
            "partial":snapshot.partial,
            "discord_counts_observed":snapshot.read_states.iter().filter(|state|state.discord_mention_count.is_some()).count(),
            "inventory_complete":snapshot.inventory_complete
        })
    );
    Ok(())
}

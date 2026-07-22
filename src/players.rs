//! Anonymous player creation and API-token verification.

use anyhow::{anyhow, Result};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use uuid::Uuid;

pub fn hash_api_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn generate_api_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub async fn create_player(pool: &SqlitePool) -> Result<(Uuid, String)> {
    let id = Uuid::new_v4();
    let token = generate_api_token();
    let token_hash = hash_api_token(&token);

    sqlx::query("INSERT INTO players (id, api_token_hash) VALUES (?, ?)")
        .bind(id.to_string())
        .bind(&token_hash)
        .execute(pool)
        .await?;

    Ok((id, token))
}

pub async fn verify_player_token(pool: &SqlitePool, player_id: &Uuid, token: &str) -> Result<bool> {
    let expected = hash_api_token(token);
    let row: Option<(String,)> = sqlx::query_as("SELECT api_token_hash FROM players WHERE id = ?")
        .bind(player_id.to_string())
        .fetch_optional(pool)
        .await?;

    match row {
        Some((hash,)) => Ok(hash == expected),
        None => Ok(false),
    }
}

pub async fn require_player(pool: &SqlitePool, player_id: &Uuid, token: &str) -> Result<()> {
    if verify_player_token(pool, player_id, token).await? {
        Ok(())
    } else {
        Err(anyhow!("invalid player credentials"))
    }
}

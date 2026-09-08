//! Long-lived per-device netplay keys.
//!
//! # Why these exist
//!
//! The Discord login needs a browser. The devices people want to play on
//! increasingly do not have one. So a player signs in once on a PC, and the
//! key issued there travels with a build they install elsewhere; that device
//! then authenticates with no browser, no Discord round trip, and no account
//! management of its own.
//!
//! # The shape
//!
//! A key is a **bearer credential**: whoever holds it is the player. That
//! makes three properties non-negotiable, and they are all enforced here.
//!
//! * **Hash at rest.** Only SHA-256 is stored, exactly as `players.
//!   api_token_hash` already does. The plaintext is returned once, at issue,
//!   and is unrecoverable afterwards — a database leak yields nothing usable.
//! * **One row per device.** A single shared key would mean revoking a lost
//!   handheld signs the player out of their desktop too, which is how you get
//!   people who never revoke anything.
//! * **Revocable, not deleted.** `revoked_at` is set, so a dead key still
//!   answers "when did that device stop working?".
//!
//! # What a key is not
//!
//! It is not a session. It mints short-lived session JWTs through
//! [`redeem`] and is otherwise never sent to the lobby. That keeps the
//! long-lived thing off the wire on every connection.

use crate::identity::{self, Player};
use anyhow::{anyhow, Result};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use uuid::Uuid;

/// The prefix makes a leaked key greppable and obvious in a paste, and stops
/// it being mistaken for a session token or a Discord id.
const PREFIX: &str = "rnp_";

pub fn hash(secret: &str) -> String {
    let mut h = Sha256::new();
    h.update(secret.as_bytes());
    hex::encode(h.finalize())
}

fn generate() -> String {
    let mut b = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut b);
    format!("{PREFIX}{}", hex::encode(b))
}

/// Issue a key for one device. The plaintext is returned here and never again.
pub async fn issue(pool: &SqlitePool, player: &Uuid, label: &str) -> Result<String> {
    let secret = generate();
    let label: String = label.chars().filter(|c| !c.is_control()).take(64).collect();
    sqlx::query(
        "INSERT INTO player_secrets (id, player_id, secret_hash, label) VALUES (?, ?, ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(player.to_string())
    .bind(hash(&secret))
    .bind(label.trim())
    .execute(pool)
    .await?;
    Ok(secret)
}

/// Trade a key for the player it belongs to, refreshing `last_used_at`.
///
/// A revoked key is rejected the same way an unknown one is: the caller learns
/// only that it does not work, which is all it is entitled to know.
pub async fn redeem(pool: &SqlitePool, secret: &str) -> Result<Player> {
    let h = hash(secret);
    let row: Option<(String,)> =
        sqlx::query_as("SELECT player_id FROM player_secrets WHERE secret_hash = ? AND revoked_at IS NULL")
            .bind(&h)
            .fetch_optional(pool)
            .await?;
    let Some((player_id,)) = row else {
        return Err(anyhow!("invalid_secret"));
    };
    sqlx::query("UPDATE player_secrets SET last_used_at = datetime('now') WHERE secret_hash = ?")
        .bind(&h)
        .execute(pool)
        .await?;
    let uuid = Uuid::parse_str(&player_id)?;
    identity::load_player(pool, &uuid)
        .await?
        .ok_or_else(|| anyhow!("invalid_secret"))
}

/// Revoke one key by its plaintext — what a device does when it is being
/// retired, and what "sign out on this device" means.
pub async fn revoke(pool: &SqlitePool, secret: &str) -> Result<bool> {
    let n = sqlx::query(
        "UPDATE player_secrets SET revoked_at = datetime('now') \
         WHERE secret_hash = ? AND revoked_at IS NULL",
    )
    .bind(hash(secret))
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// Revoke every key a player holds. The "I lost a device and I am not sure
/// which key was on it" button.
pub async fn revoke_all(pool: &SqlitePool, player: &Uuid) -> Result<u64> {
    Ok(sqlx::query(
        "UPDATE player_secrets SET revoked_at = datetime('now') \
         WHERE player_id = ? AND revoked_at IS NULL",
    )
    .bind(player.to_string())
    .execute(pool)
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{link_discord, DiscordProfile};

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
        sqlx::migrate::Migrator::new(dir).await.unwrap().run(&pool).await.unwrap();
        pool
    }

    async fn a_player(pool: &SqlitePool) -> Player {
        link_discord(
            pool,
            &DiscordProfile {
                id: "123456789012345678".into(),
                username: "reimu_h".into(),
                global_name: Some("Reimu".into()),
                avatar: None,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_key_redeems_to_its_player() {
        let pool = pool().await;
        let p = a_player(&pool).await;
        let secret = issue(&pool, &p.id, "Steam Deck").await.unwrap();
        assert!(secret.starts_with(PREFIX), "greppable prefix: {secret}");
        let back = redeem(&pool, &secret).await.unwrap();
        assert_eq!(back.id, p.id);
        assert_eq!(back.handle, "Reimu");
    }

    #[tokio::test]
    async fn the_plaintext_is_not_stored() {
        let pool = pool().await;
        let p = a_player(&pool).await;
        let secret = issue(&pool, &p.id, "PC").await.unwrap();
        let rows: Vec<(String,)> = sqlx::query_as("SELECT secret_hash FROM player_secrets")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(rows[0].0, secret, "a database leak must not yield a key");
        assert_eq!(rows[0].0, hash(&secret));
    }

    #[tokio::test]
    async fn revoking_one_device_leaves_the_others_alone() {
        /* The whole reason keys are per-device: losing a handheld must not
         * sign you out of your desktop. */
        let pool = pool().await;
        let p = a_player(&pool).await;
        let deck = issue(&pool, &p.id, "Steam Deck").await.unwrap();
        let pc = issue(&pool, &p.id, "PC").await.unwrap();

        assert!(revoke(&pool, &deck).await.unwrap());
        assert!(redeem(&pool, &deck).await.is_err(), "revoked key is dead");
        assert!(redeem(&pool, &pc).await.is_ok(), "the other device still works");

        /* Revoking twice is not an error the caller has to handle, but it does
         * report that nothing changed. */
        assert!(!revoke(&pool, &deck).await.unwrap());
    }

    #[tokio::test]
    async fn revoke_all_is_the_lost_device_button() {
        let pool = pool().await;
        let p = a_player(&pool).await;
        let a = issue(&pool, &p.id, "one").await.unwrap();
        let b = issue(&pool, &p.id, "two").await.unwrap();
        assert_eq!(revoke_all(&pool, &p.id).await.unwrap(), 2);
        assert!(redeem(&pool, &a).await.is_err());
        assert!(redeem(&pool, &b).await.is_err());
    }

    #[tokio::test]
    async fn an_unknown_key_is_refused_like_a_revoked_one() {
        let pool = pool().await;
        let e = redeem(&pool, "rnp_deadbeef").await.unwrap_err().to_string();
        assert_eq!(e, "invalid_secret", "no oracle for whether a key ever existed");
    }
}

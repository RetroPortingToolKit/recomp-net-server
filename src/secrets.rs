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
//! * **Never on the wire.** psxrecomp has no TLS, so a key sent in a request
//!   would be captured once on a hostile network and reused forever. Devices
//!   authenticate by [`verify_proof`] instead: the server issues a nonce, the
//!   device returns HMAC-SHA256 over it keyed by the key's verifier, and the
//!   key itself never leaves the device. Single-use nonces block replay.
//! * **One row per device.** A single shared key would mean revoking a lost
//!   handheld signs the player out of their desktop too, which is how you get
//!   people who never revoke anything.
//! * **Revocable, not deleted.** `revoked_at` is set, so a dead key still
//!   answers "when did that device stop working?".
//!
//! # The cost of challenge-response, stated plainly
//!
//! Checking a proof means the server must hold something equivalent to the
//! key, so `secret_hash` is now a *verifier*: anyone who reads the database
//! can authenticate as that player. Under the older plaintext scheme a
//! database leak was useless on its own. This is a deliberate trade, and the
//! right one here — passive sniffing of an unencrypted request on a shared
//! network is easy, repeatable and undetectable, while reading the database
//! means already being inside the server. When the client grows TLS, the
//! plaintext [`redeem`] path can come back and this trade can be revisited.
//!
//! # What a key is not
//!
//! It is not a session. It mints short-lived session JWTs and is otherwise
//! never sent to the lobby, so the long-lived thing stays off the wire on
//! every connection.

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

/// The value stored in `secret_hash`, and the key an HMAC proof is computed
/// with. Same bytes as [`hash`] -- the rename is about role, not arithmetic.
pub fn verifier(secret: &str) -> String {
    hash(secret)
}

/// Constant-time compare, so a proof cannot be recovered a byte at a time by
/// timing the reply.
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The proof a device sends: HMAC-SHA256, keyed by the verifier, over the
/// server's nonce. Hex, lowercase.
///
/// The device holds the key, hashes it into the verifier, and HMACs. So the
/// key itself never crosses the wire, which is the entire point -- psxrecomp
/// has no TLS, and a permanent bearer credential in cleartext would be
/// captured once and reused forever.
pub fn expected_proof(verifier_hex: &str, nonce: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(verifier_hex.as_bytes())
        .expect("hmac accepts any key length");
    mac.update(nonce.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verify a device's proof against every live key the player holds.
///
/// Every key is tried because the proof does not say which device sent it --
/// that is deliberate, since naming the key would be one more thing on the
/// wire for no benefit. A player has a handful of devices, so the loop is
/// short.
pub async fn verify_proof(
    pool: &SqlitePool,
    player: &Uuid,
    nonce: &str,
    proof: &str,
) -> Result<Player> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT secret_hash FROM player_secrets WHERE player_id = ? AND revoked_at IS NULL",
    )
    .bind(player.to_string())
    .fetch_all(pool)
    .await?;

    let mut matched: Option<String> = None;
    for (v,) in &rows {
        if ct_eq(&expected_proof(v, nonce), proof) {
            matched = Some(v.clone());
        }
    }
    let Some(v) = matched else {
        return Err(anyhow!("invalid_secret"));
    };
    sqlx::query("UPDATE player_secrets SET last_used_at = datetime('now') WHERE secret_hash = ?")
        .bind(&v)
        .execute(pool)
        .await?;
    identity::load_player(pool, player)
        .await?
        .ok_or_else(|| anyhow!("invalid_secret"))
}

/// Revoke every live key whose verifier answers this proof.
pub async fn revoke_by_proof(
    pool: &SqlitePool,
    player: &Uuid,
    nonce: &str,
    proof: &str,
) -> Result<bool> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT secret_hash FROM player_secrets WHERE player_id = ? AND revoked_at IS NULL",
    )
    .bind(player.to_string())
    .fetch_all(pool)
    .await?;
    for (v,) in &rows {
        if ct_eq(&expected_proof(v, nonce), proof) {
            sqlx::query(
                "UPDATE player_secrets SET revoked_at = datetime('now') WHERE secret_hash = ?",
            )
            .bind(v)
            .execute(pool)
            .await?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Trade a key for the player it belongs to, refreshing `last_used_at`.
///
/// A revoked key is rejected the same way an unknown one is: the caller learns
/// only that it does not work, which is all it is entitled to know.
///
/// NOTE: this sends the key itself, so it is only safe over TLS. The lobby
/// path uses [`verify_proof`] instead. Kept for a future HTTPS client and for
/// the tests that pin the storage properties.
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
    async fn a_proof_authenticates_without_the_key_touching_the_wire() {
        let pool = pool().await;
        let p = a_player(&pool).await;
        let secret = issue(&pool, &p.id, "Steam Deck").await.unwrap();

        /* Exactly what the device computes: hash the key it holds, HMAC the
         * server's nonce with it. The key stays on the device. */
        let proof = expected_proof(&verifier(&secret), "nonce-abc");
        let back = verify_proof(&pool, &p.id, "nonce-abc", &proof).await.unwrap();
        assert_eq!(back.id, p.id);

        /* A proof is bound to its nonce, so a captured one is worthless
         * against the next challenge. */
        assert!(verify_proof(&pool, &p.id, "nonce-xyz", &proof).await.is_err());
    }

    #[tokio::test]
    async fn a_revoked_device_cannot_prove_anything() {
        let pool = pool().await;
        let p = a_player(&pool).await;
        let deck = issue(&pool, &p.id, "Steam Deck").await.unwrap();
        let pc = issue(&pool, &p.id, "PC").await.unwrap();

        assert!(revoke(&pool, &deck).await.unwrap());
        let dead = expected_proof(&verifier(&deck), "n1");
        assert!(verify_proof(&pool, &p.id, "n1", &dead).await.is_err());

        let live = expected_proof(&verifier(&pc), "n1");
        assert!(verify_proof(&pool, &p.id, "n1", &live).await.is_ok());
    }

    #[tokio::test]
    async fn a_device_revokes_itself_by_proof() {
        let pool = pool().await;
        let p = a_player(&pool).await;
        let s1 = issue(&pool, &p.id, "one").await.unwrap();
        let s2 = issue(&pool, &p.id, "two").await.unwrap();
        assert!(revoke_by_proof(&pool, &p.id, "n", &expected_proof(&verifier(&s1), "n"))
            .await
            .unwrap());
        assert!(verify_proof(&pool, &p.id, "n2", &expected_proof(&verifier(&s1), "n2"))
            .await
            .is_err());
        assert!(verify_proof(&pool, &p.id, "n2", &expected_proof(&verifier(&s2), "n2"))
            .await
            .is_ok());
    }

    #[test]
    fn a_wrong_proof_is_rejected_in_constant_time_shape() {
        let v = verifier("rnp_whatever");
        let good = expected_proof(&v, "n");
        assert!(ct_eq(&good, &good));
        assert!(!ct_eq(&good, &expected_proof(&v, "other-nonce")));
        assert!(!ct_eq(&good, "short"));
    }

    #[tokio::test]
    async fn an_unknown_key_is_refused_like_a_revoked_one() {
        let pool = pool().await;
        let e = redeem(&pool, "rnp_deadbeef").await.unwrap_err().to_string();
        assert_eq!(e, "invalid_secret", "no oracle for whether a key ever existed");
    }
}

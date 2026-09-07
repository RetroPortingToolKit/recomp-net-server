//! Discord-linked player identity.
//!
//! # The split
//!
//! Two names, two jobs. They are named here so nobody has to remember which
//! way round it went:
//!
//! | | stable? | unique? | who sets it | shown? |
//! |---|---|---|---|---|
//! | `discord_id` (snowflake) | **yes, immutable** | yes | Discord | no |
//! | `discord_username` (`@handle`) | no | on Discord only | player, on Discord | as a disambiguator |
//! | `discord_global_name` | no | **no** | player, on Discord | only as a default |
//! | `netplay_handle` | no | **no** | player, here | yes — this is the seat-table name |
//!
//! Identity is the snowflake and nothing else. Both Discord names are mutable
//! by the user, and Discord releases a changed `@username` for somebody else
//! to claim — so a name key would let one account inherit another's history,
//! which is an impersonation route in one direction and ban evasion in the
//! other.
//!
//! `netplay_handle` is the arbitrary, player-editable, presentational one. It
//! defaults from Discord on first link and can be changed here afterwards
//! without touching identity, so a rename is free and costs nothing to audit.

use crate::names;
use anyhow::Result;
use sqlx::SqlitePool;
use uuid::Uuid;

/// What `identify` gives us back. `email` is deliberately not requested.
#[derive(Debug, Clone, Default)]
pub struct DiscordProfile {
    /// The snowflake. The key.
    pub id: String,
    /// The unique `@handle`.
    pub username: String,
    /// Discord's display name. Absent on accounts that never set one.
    pub global_name: Option<String>,
    pub avatar: Option<String>,
}

/// A player as the lobby needs to see them.
#[derive(Debug, Clone)]
pub struct Player {
    pub id: Uuid,
    pub discord_id: String,
    pub discord_username: String,
    /// The seat-table name. Never empty: see [`default_handle_for`].
    pub handle: String,
}

/// The handle a freshly linked account starts with.
///
/// Preference order is Discord's own display name, then the `@handle`, then a
/// stable fallback built from the snowflake. Each candidate has to survive the
/// shared name gate ([`names::acceptable`]) — hygiene *and* the word list.
///
/// The fallback matters. A Discord name that trips the word list must not
/// block the login: the player did not choose that name here and cannot fix it
/// from inside the game, so refusing the login would be a dead end with no
/// action available. Instead the refused name is simply not used as the
/// default, and the player gets a neutral one they can change. This is why the
/// handle being editable is load-bearing rather than a nicety.
pub fn default_handle_for(p: &DiscordProfile) -> String {
    if let Some(n) = names::acceptable(p.global_name.clone()) {
        return n;
    }
    if let Some(n) = names::acceptable(Some(p.username.clone())) {
        return n;
    }
    /* Neither Discord name is usable. Give them something that works and is
     * theirs, rather than refusing to seat them. */
    let tail: String = p.id.chars().rev().take(4).collect::<Vec<_>>()
        .into_iter().rev().collect();
    format!("Player-{tail}")
}

/// Link a Discord login to a player row, creating it on first sight.
///
/// Idempotent on `discord_id`. On a return visit the cached Discord names are
/// refreshed (they are display and audit data, and stale copies are worse than
/// useless in a moderation queue) but `netplay_handle` is left ALONE — the
/// player may have chosen it, and a Discord rename must not silently undo
/// that.
pub async fn link_discord(pool: &SqlitePool, p: &DiscordProfile) -> Result<Player> {
    let existing: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT id, netplay_handle FROM players WHERE discord_id = ?",
    )
    .bind(&p.id)
    .fetch_optional(pool)
    .await?;

    if let Some((id, handle)) = existing {
        sqlx::query(
            "UPDATE players SET discord_username = ?, discord_global_name = ?, \
             discord_avatar = ?, last_seen_at = datetime('now') WHERE discord_id = ?",
        )
        .bind(&p.username)
        .bind(&p.global_name)
        .bind(&p.avatar)
        .bind(&p.id)
        .execute(pool)
        .await?;

        /* A row predating the handle column, or one somehow blanked: give it
         * the default rather than handing the lobby an empty seat name. */
        let handle = match handle.filter(|h| !h.is_empty()) {
            Some(h) => h,
            None => {
                let h = default_handle_for(p);
                sqlx::query("UPDATE players SET netplay_handle = ? WHERE discord_id = ?")
                    .bind(&h)
                    .bind(&p.id)
                    .execute(pool)
                    .await?;
                h
            }
        };
        return Ok(Player {
            id: Uuid::parse_str(&id)?,
            discord_id: p.id.clone(),
            discord_username: p.username.clone(),
            handle,
        });
    }

    let id = Uuid::new_v4();
    let token_hash = crate::players::hash_api_token(&crate::players::generate_api_token());
    let handle = default_handle_for(p);
    sqlx::query(
        "INSERT INTO players (id, api_token_hash, discord_id, discord_username, \
         discord_global_name, discord_avatar, netplay_handle, linked_at, last_seen_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
    )
    .bind(id.to_string())
    .bind(&token_hash)
    .bind(&p.id)
    .bind(&p.username)
    .bind(&p.global_name)
    .bind(&p.avatar)
    .bind(&handle)
    .execute(pool)
    .await?;

    Ok(Player {
        id,
        discord_id: p.id.clone(),
        discord_username: p.username.clone(),
        handle,
    })
}

/// Change the presentational handle. `Err` when the requested name does not
/// survive the shared gate — here the player DID choose it, so refusing is
/// fair and actionable: they can type another one.
pub async fn set_handle(pool: &SqlitePool, player: Uuid, requested: &str) -> Result<String> {
    let Some(handle) = names::acceptable(Some(requested.to_string())) else {
        anyhow::bail!("handle_rejected");
    };
    sqlx::query("UPDATE players SET netplay_handle = ? WHERE id = ?")
        .bind(&handle)
        .bind(player.to_string())
        .execute(pool)
        .await?;
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(username: &str, global: Option<&str>) -> DiscordProfile {
        DiscordProfile {
            id: "123456789012345678".into(),
            username: username.into(),
            global_name: global.map(str::to_string),
            avatar: None,
        }
    }

    #[test]
    fn the_display_name_is_preferred_then_the_handle() {
        assert_eq!(default_handle_for(&profile("reimu_h", Some("Reimu"))), "Reimu");
        assert_eq!(default_handle_for(&profile("reimu_h", None)), "reimu_h");
    }

    #[test]
    fn a_refused_discord_name_falls_back_instead_of_blocking_the_login() {
        /* The player cannot fix their Discord name from inside the game, so a
         * refusal here has to leave them somewhere to stand. */
        let h = default_handle_for(&profile("reimu_h", Some("fuck")));
        assert_eq!(h, "reimu_h");

        let h = default_handle_for(&profile("fuck", Some("fuck")));
        assert_eq!(h, "Player-5678");
        assert!(!h.contains('*'), "never masked, only replaced");
    }

    #[test]
    fn a_discord_name_is_cut_to_our_field_on_a_character_boundary() {
        /* Discord allows 32 characters; four-byte characters make that 128
         * bytes, and the client field is 64. */
        let wide = "\u{1F600}".repeat(32);
        let h = default_handle_for(&profile("x", Some(&wide)));
        assert!(h.len() <= names::NAME_MAX_BYTES, "{} bytes", h.len());
        assert!(h.chars().all(|c| c == '\u{1F600}'));
    }

    /// Runs the real migrations against a fresh in-memory database, so this
    /// exercises `002_discord_identity.sql` itself and not a hand-written
    /// approximation of it.
    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
        sqlx::migrate::Migrator::new(dir).await.unwrap().run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn linking_twice_is_the_same_player() {
        let pool = pool().await;
        let a = link_discord(&pool, &profile("reimu_h", Some("Reimu"))).await.unwrap();
        let b = link_discord(&pool, &profile("reimu_h", Some("Reimu"))).await.unwrap();
        assert_eq!(a.id, b.id, "the snowflake is the key");
    }

    #[tokio::test]
    async fn a_discord_rename_keeps_the_identity_and_the_chosen_handle() {
        let pool = pool().await;
        let p = link_discord(&pool, &profile("reimu_h", Some("Reimu"))).await.unwrap();
        let chosen = set_handle(&pool, p.id, "Red White").await.unwrap();
        assert_eq!(chosen, "Red White");

        /* Same account, both Discord names changed -- the sort of rename that
         * would silently re-key identity if we keyed on a name. */
        let mut renamed = profile("marisa_k", Some("Marisa"));
        renamed.id = p.discord_id.clone();
        let after = link_discord(&pool, &renamed).await.unwrap();
        assert_eq!(after.id, p.id, "identity survives a rename");
        assert_eq!(after.handle, "Red White", "a Discord rename does not undo the player's choice");
        assert_eq!(after.discord_username, "marisa_k", "the cached name is refreshed");
    }

    #[tokio::test]
    async fn two_discord_accounts_are_two_players() {
        let pool = pool().await;
        let mut second = profile("marisa_k", Some("Marisa"));
        second.id = "987654321098765432".into();
        let a = link_discord(&pool, &profile("reimu_h", Some("Reimu"))).await.unwrap();
        let b = link_discord(&pool, &second).await.unwrap();
        assert_ne!(a.id, b.id);
    }

    #[tokio::test]
    async fn a_refused_handle_leaves_the_old_one_standing() {
        let pool = pool().await;
        let p = link_discord(&pool, &profile("reimu_h", Some("Reimu"))).await.unwrap();
        assert!(set_handle(&pool, p.id, "fuck").await.is_err());
        let again = link_discord(&pool, &profile("reimu_h", Some("Reimu"))).await.unwrap();
        assert_eq!(again.handle, "Reimu");
    }

    #[test]
    fn a_handle_the_player_typed_is_refused_not_replaced() {
        /* The mirror of the fallback above: here the player chose it and can
         * type another, so the refusal is actionable and stands. */
        assert!(names::acceptable(Some("fuck".into())).is_none());
        assert_eq!(names::acceptable(Some("Marisa".into())).as_deref(), Some("Marisa"));
    }
}

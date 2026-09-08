//! Discord OAuth2 login, and the pairing that gets the result back to a game.
//!
//! # Why the server holds the secret
//!
//! A desktop game is a *public client*: anything shipped inside it is readable
//! by anyone who has it, so it cannot hold `DISCORD_CLIENT_SECRET`. Rather than
//! reach for PKCE and hope every launcher implements it correctly, the whole
//! exchange happens here and the launcher never sees a Discord credential at
//! all — only an opaque pairing code and, at the end, our own session token.
//!
//! # The flow
//!
//! ```text
//!   launcher                    server                     Discord
//!      |  POST /auth/discord/start                            |
//!      | ----------------------> | (mint pairing code)        |
//!      | <- {code, url} -------- |                            |
//!      |  open url in browser -----------------------------> login
//!      |                         | <- GET /callback?code&state |
//!      |                         | -- exchange code --------> |
//!      |                         | <- access_token ---------- |
//!      |                         | -- GET /users/@me -------> |
//!      |                         | <- profile --------------- |
//!      |                         | (link_discord, park result)|
//!      |  POST /auth/discord/poll {code}                      |
//!      | ----------------------> |                            |
//!      | <- {session, handle} -- |                            |
//! ```
//!
//! Polling rather than a loopback redirect: a loopback listener means opening a
//! port on the player's machine and getting firewall prompts on Windows, and
//! consoles have no loopback browser at all. A short code the launcher polls
//! costs one endpoint and works everywhere.
//!
//! # What is deliberately not requested
//!
//! Scope is `identify` — and `guilds.members.read` only when `DISCORD_GUILD_ID`
//! is set. Never `email`: we do not need it, and not holding it is one less
//! thing to protect and to disclose.

use crate::config::Config;
use crate::identity::{self, DiscordProfile};
use anyhow::{anyhow, Context, Result};
use rand::RngCore;
use serde::Deserialize;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const DISCORD_API: &str = "https://discord.com/api/v10";

/// How long a started login may sit unfinished. Long enough to find your
/// password manager, short enough that an abandoned code is not a standing
/// invitation.
const PENDING_TTL: Duration = Duration::from_secs(10 * 60);

/// A login in flight, keyed by the pairing code the launcher polls with.
#[derive(Debug)]
struct Pending {
    started: Instant,
    /// `None` until the callback lands. `Some(Err)` is a finished failure the
    /// launcher should see once and stop polling on.
    outcome: Option<std::result::Result<Completed, String>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Completed {
    pub session: String,
    pub player_id: String,
    pub handle: String,
    /// Shown as the disambiguator when two players share a handle.
    pub discord_username: String,
}

#[derive(Clone, Default)]
pub struct LoginStore {
    inner: Arc<Mutex<HashMap<String, Pending>>>,
}

impl LoginStore {
    pub async fn begin(&self) -> String {
        let mut g = self.inner.lock().await;
        g.retain(|_, p| p.started.elapsed() < PENDING_TTL);
        let code = random_code();
        g.insert(
            code.clone(),
            Pending { started: Instant::now(), outcome: None },
        );
        code
    }

    /// Public form of [`Self::finish`] for the callback's early exits.
    pub async fn fail(&self, code: &str, why: &str) {
        self.finish(code, Err(why.to_string())).await;
    }

    async fn finish(&self, code: &str, outcome: std::result::Result<Completed, String>) {
        let mut g = self.inner.lock().await;
        if let Some(p) = g.get_mut(code) {
            p.outcome = Some(outcome);
        }
    }

    /// Take the result if there is one. Removes it: a pairing code is good for
    /// exactly one login, so a leaked code cannot be replayed into a second
    /// session.
    pub async fn take(&self, code: &str) -> Option<std::result::Result<Completed, String>> {
        let mut g = self.inner.lock().await;
        let done = g.get(code)?.outcome.is_some();
        if !done {
            /* Still waiting, and still valid -- unless it has aged out. */
            if g.get(code).is_some_and(|p| p.started.elapsed() >= PENDING_TTL) {
                g.remove(code);
                return Some(Err("expired".into()));
            }
            return None;
        }
        g.remove(code).and_then(|p| p.outcome)
    }
}

fn random_code() -> String {
    let mut b = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut b);
    hex::encode(b)
}

/// The URL the launcher opens. `state` is the pairing code, which is why the
/// callback can find the pending login without a cookie or a session.
///
/// No `prompt=none`. It would skip the consent screen for a returning player,
/// which is nicer, but a first-time authorization still has to show one and it
/// is not worth risking a login that fails on first use to save a click.
pub fn authorize_url(cfg: &Config, state: &str) -> Result<String> {
    let id = cfg
        .discord_client_id
        .as_deref()
        .ok_or_else(|| anyhow!("DISCORD_CLIENT_ID not set"))?;
    let redirect = cfg
        .discord_redirect_url
        .as_deref()
        .ok_or_else(|| anyhow!("DISCORD_REDIRECT_URL not set"))?;
    let scope = if cfg.discord_guild_id.is_some() {
        "identify%20guilds.members.read"
    } else {
        "identify"
    };
    Ok(format!(
        "{DISCORD_API}/oauth2/authorize?client_id={id}&response_type=code\
         &redirect_uri={}&scope={scope}&state={state}",
        urlencode(redirect)
    ))
}

/// Minimal percent-encoding for the one value we interpolate. A redirect URL is
/// operator-supplied, not player-supplied, but it still must not break the
/// query string it sits in.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct MeResponse {
    id: String,
    username: String,
    global_name: Option<String>,
    avatar: Option<String>,
}

/// Exchange the authorization code, read the profile, enforce guild membership
/// if configured, and link. Returns what the launcher will be handed.
pub async fn complete(
    cfg: &Config,
    pool: &SqlitePool,
    http: &reqwest::Client,
    code: &str,
) -> Result<Completed> {
    let client_id = cfg
        .discord_client_id
        .as_deref()
        .ok_or_else(|| anyhow!("DISCORD_CLIENT_ID not set"))?;
    let secret = cfg
        .discord_client_secret
        .as_deref()
        .ok_or_else(|| anyhow!("DISCORD_CLIENT_SECRET not set"))?;
    let redirect = cfg
        .discord_redirect_url
        .as_deref()
        .ok_or_else(|| anyhow!("DISCORD_REDIRECT_URL not set"))?;

    let token: TokenResponse = http
        .post(format!("{DISCORD_API}/oauth2/token"))
        .form(&[
            ("client_id", client_id),
            ("client_secret", secret),
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect),
        ])
        .send()
        .await
        .context("discord token exchange")?
        .error_for_status()
        .context("discord rejected the code")?
        .json()
        .await
        .context("discord token response")?;

    let me: MeResponse = http
        .get(format!("{DISCORD_API}/users/@me"))
        .bearer_auth(&token.access_token)
        .send()
        .await
        .context("discord users/@me")?
        .error_for_status()
        .context("discord refused the profile")?
        .json()
        .await
        .context("discord profile response")?;

    /* Guild gating, when configured. This is the lever that makes a Discord
     * ban a netplay ban: someone removed from the server stops passing here on
     * their next login, with no separate ban list to maintain. */
    if let Some(guild) = cfg.discord_guild_id.as_deref() {
        let res = http
            .get(format!("{DISCORD_API}/users/@me/guilds/{guild}/member"))
            .bearer_auth(&token.access_token)
            .send()
            .await
            .context("discord guild membership")?;
        if !res.status().is_success() {
            return Err(anyhow!("not_in_guild"));
        }
    }

    let profile = DiscordProfile {
        id: me.id,
        username: me.username,
        global_name: me.global_name,
        avatar: me.avatar,
    };
    let player = identity::link_discord(pool, &profile).await?;
    let session = crate::auth::issue_session_token(cfg, &player.id)
        .map_err(|e| anyhow!("session token: {e}"))?;

    Ok(Completed {
        session,
        player_id: player.id.to_string(),
        handle: player.handle,
        discord_username: player.discord_username,
    })
}

/// Callback handler body, split out so the route stays thin and this is
/// testable in isolation from axum.
pub async fn on_callback(
    cfg: &Config,
    pool: &SqlitePool,
    http: &reqwest::Client,
    store: &LoginStore,
    state: &str,
    code: &str,
) -> std::result::Result<Completed, String> {
    let result = complete(cfg, pool, http, code)
        .await
        .map_err(|e| e.to_string());
    store.finish(state, result.clone()).await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_pairing_code_is_pending_until_it_is_finished() {
        let s = LoginStore::default();
        let code = s.begin().await;
        assert!(s.take(&code).await.is_none(), "still waiting");
        s.finish(&code, Err("nope".into())).await;
        assert!(matches!(s.take(&code).await, Some(Err(_))));
    }

    #[tokio::test]
    async fn a_code_is_good_for_exactly_one_login() {
        /* A leaked or shoulder-surfed code must not be replayable into a
         * second session. */
        let s = LoginStore::default();
        let code = s.begin().await;
        s.finish(
            &code,
            Ok(Completed {
                session: "jwt".into(),
                player_id: "p".into(),
                handle: "Reimu".into(),
                discord_username: "reimu_h".into(),
            }),
        )
        .await;
        assert!(s.take(&code).await.is_some());
        assert!(s.take(&code).await.is_none(), "the code is spent");
    }

    #[tokio::test]
    async fn an_unknown_code_is_not_pending() {
        let s = LoginStore::default();
        assert!(s.take("nope").await.is_none());
    }

    #[test]
    fn the_authorize_url_asks_for_guild_scope_only_when_gating() {
        let mut cfg = crate::config::Config {
            discord_client_id: Some("123".into()),
            discord_redirect_url: Some("https://x.example/auth/discord/callback".into()),
            ..Default::default()
        };

        let url = authorize_url(&cfg, "st").unwrap();
        assert!(url.contains("scope=identify&"), "{url}");
        assert!(!url.contains("guilds.members.read"));
        /* The redirect must survive encoding intact -- Discord compares it byte
         * for byte against the registration. */
        assert!(url.contains("https%3A%2F%2Fx.example%2Fauth%2Fdiscord%2Fcallback"));

        cfg.discord_guild_id = Some("999".into());
        assert!(authorize_url(&cfg, "st").unwrap().contains("guilds.members.read"));
    }

    #[test]
    fn there_is_no_url_without_configuration() {
        let cfg = crate::config::Config::default();
        assert!(authorize_url(&cfg, "st").is_err());
    }
}

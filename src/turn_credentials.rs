//! Coturn time-limited credentials (HMAC-SHA1 per REST API / `use-auth-secret`).
//!
//! Shaped for recomp-net / libjuice ICE (`RNetIceConfig`).

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use std::env;
use uuid::Uuid;

type HmacSha1 = Hmac<Sha1>;

const DEFAULT_STUN_PORT: u16 = 3478;
const DEFAULT_TURN_PORT: u16 = 3478;
const DEFAULT_TURNS_PORT: u16 = 5349;
const DEFAULT_TTL_SECS: u64 = 86400;

pub struct TurnCredentialConfig {
    pub secret: String,
    pub realm: String,
    pub ttl_secs: u64,
    pub stun_host: String,
    pub stun_port: u16,
    pub turn_host: String,
    pub turn_port: u16,
    pub turns_port: u16,
}

fn env_u16(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

impl TurnCredentialConfig {
    pub fn from_env() -> Option<Self> {
        let secret = env::var("COTURN_STATIC_AUTH_SECRET")
            .ok()
            .filter(|s| !s.is_empty())?;
        let realm = env::var("COTURN_REALM").unwrap_or_else(|_| "recomp-net".to_string());
        let ttl_secs = env::var("COTURN_CREDENTIAL_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_TTL_SECS);
        let host = env::var("COTURN_HOST").ok().filter(|s| !s.is_empty())?;
        let stun_host = env::var("COTURN_STUN_HOST")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| host.clone());
        let turn_host = env::var("COTURN_TURN_HOST")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| stun_host.clone());
        Some(Self {
            secret,
            realm,
            ttl_secs,
            stun_port: env_u16("COTURN_STUN_PORT", DEFAULT_STUN_PORT),
            turn_port: env_u16("COTURN_TURN_PORT", DEFAULT_TURN_PORT),
            turns_port: env_u16("COTURN_TURNS_PORT", DEFAULT_TURNS_PORT),
            stun_host,
            turn_host,
        })
    }
}

pub fn issue_credentials(cfg: &TurnCredentialConfig, player_id: &Uuid) -> Result<(String, String)> {
    let expiry = chrono::Utc::now().timestamp() as u64 + cfg.ttl_secs;
    let username = format!("{expiry}:{player_id}");
    let mut mac = HmacSha1::new_from_slice(cfg.secret.as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid COTURN_STATIC_AUTH_SECRET length"))?;
    mac.update(username.as_bytes());
    let digest = mac.finalize().into_bytes();
    let password = STANDARD.encode(digest);
    Ok((username, password))
}

pub fn require_config() -> Result<TurnCredentialConfig> {
    TurnCredentialConfig::from_env().ok_or_else(|| {
        anyhow::anyhow!(
            "COTURN_STATIC_AUTH_SECRET and COTURN_HOST are not configured (see docs/LOBBY.md)"
        )
    })
}

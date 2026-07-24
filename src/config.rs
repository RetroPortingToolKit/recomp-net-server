//! Application configuration loaded from the environment only.
//!
//! **No signing keys, API keys, or passwords are defaulted in source.** See
//! `docs/SECURITY.md`.

use anyhow::{bail, Result};
use std::env;

/// Default local SQLite URL when `DATABASE_URL` is unset (dev convenience only).
pub const DEFAULT_DATABASE_URL: &str = "sqlite:recomp-net-server.db?mode=rwc";

/// Default recomp-net protocol magic `"RNET"` / `0x524E4554`.
pub const DEFAULT_PROTOCOL_MAGIC: u32 = 0x524E_4554;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub database_url: Option<String>,
    /// When true, JWT signing must be configured or startup fails.
    pub require_auth: bool,
    pub jwt_secret_current: Option<String>,
    pub jwt_secret_previous: Option<String>,
    pub default_input_delay: u8,
    pub default_slot_count: u8,
    pub room_idle_secs: u64,
    pub heartbeat_timeout_secs: u64,
    pub protocol_magic: u32,
    /// Empty = allow any non-empty game_id (dev). Otherwise exact match allowlist.
    pub game_allowlist: Vec<String>,
    /// UDP delay-sync input relay (star fan-out). Off disables open_session.
    pub input_relay_enabled: bool,
    /// Socket bind for the input relay (e.g. `0.0.0.0:8777`).
    pub input_relay_bind: String,
    /// Host string written into launch endpoints (public DNS / IP clients dial).
    pub input_relay_advertise_host: String,
    /// Port written into launch endpoints (may differ from bind when NAT'd).
    pub input_relay_advertise_port: u16,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        // MotK / psxrecomp clients default to ws://127.0.0.1:8765.
        let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8765".to_string());
        let database_url = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty());
        let require_auth = parse_bool_env(&env::var("REQUIRE_AUTH").unwrap_or_default());
        let jwt_secret_current = env::var("JWT_SECRET_CURRENT").ok().filter(|s| !s.is_empty());
        let jwt_secret_previous = env::var("JWT_SECRET_PREVIOUS").ok().filter(|s| !s.is_empty());

        if require_auth && jwt_secret_current.is_none() {
            bail!(
                "REQUIRE_AUTH is set but JWT_SECRET_CURRENT is missing. \
                 Set JWT_SECRET_CURRENT via environment or secrets manager (see docs/SECURITY.md)."
            );
        }

        let default_input_delay = parse_u8_env("LOBBY_DEFAULT_INPUT_DELAY", 2)?;
        let default_slot_count = parse_u8_env("LOBBY_DEFAULT_SLOT_COUNT", 2)?;
        if !(2..=5).contains(&default_slot_count) {
            bail!("LOBBY_DEFAULT_SLOT_COUNT must be between 2 and 5");
        }

        let room_idle_secs = parse_u64_env("LOBBY_ROOM_IDLE_SECS", 600);
        let heartbeat_timeout_secs = parse_u64_env("LOBBY_HEARTBEAT_TIMEOUT_SECS", 30);
        let protocol_magic = parse_u32_env("LOBBY_PROTOCOL_MAGIC", DEFAULT_PROTOCOL_MAGIC)?;

        let game_allowlist = env::var("LOBBY_GAME_ALLOWLIST")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let input_relay_enabled = match env::var("INPUT_RELAY_ENABLED") {
            Ok(s) if !s.trim().is_empty() => parse_bool_env(&s),
            _ => true,
        };
        let input_relay_bind =
            env::var("INPUT_RELAY_BIND").unwrap_or_else(|_| "0.0.0.0:8777".to_string());
        let advertise_port_default = input_relay_bind
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u16>().ok())
            .unwrap_or(8777);
        let input_relay_advertise_port =
            parse_u16_env("INPUT_RELAY_ADVERTISE_PORT", advertise_port_default)?;
        let input_relay_advertise_host = env::var("INPUT_RELAY_ADVERTISE_HOST")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "127.0.0.1".to_string());

        Ok(Config {
            bind_addr,
            database_url,
            require_auth,
            jwt_secret_current,
            jwt_secret_previous,
            default_input_delay,
            default_slot_count,
            room_idle_secs,
            heartbeat_timeout_secs,
            protocol_magic,
            game_allowlist,
            input_relay_enabled,
            input_relay_bind,
            input_relay_advertise_host,
            input_relay_advertise_port,
        })
    }

    pub fn effective_database_url(&self) -> String {
        self.database_url
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_DATABASE_URL.to_string())
    }

    pub fn jwt_verification_keys(&self) -> Vec<&str> {
        let mut keys = Vec::new();
        if let Some(ref s) = self.jwt_secret_current {
            keys.push(s.as_str());
        }
        if let Some(ref s) = self.jwt_secret_previous {
            keys.push(s.as_str());
        }
        keys
    }

    pub fn game_allowed(&self, game_id: &str) -> bool {
        if self.game_allowlist.is_empty() {
            return !game_id.is_empty();
        }
        self.game_allowlist.iter().any(|g| g == game_id)
    }
}

fn parse_bool_env(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn parse_u8_env(name: &str, default: u8) -> Result<u8> {
    match env::var(name) {
        Ok(s) if !s.is_empty() => s
            .parse::<u8>()
            .map_err(|_| anyhow::anyhow!("{name} must be a u8")),
        _ => Ok(default),
    }
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn parse_u16_env(name: &str, default: u16) -> Result<u16> {
    match env::var(name) {
        Ok(s) if !s.is_empty() => s
            .parse::<u16>()
            .map_err(|_| anyhow::anyhow!("{name} must be a u16")),
        _ => Ok(default),
    }
}

fn parse_u32_env(name: &str, default: u32) -> Result<u32> {
    match env::var(name) {
        Ok(s) if !s.is_empty() => {
            let t = s.trim();
            if let Some(hex) = t
                .strip_prefix("0x")
                .or_else(|| t.strip_prefix("0X"))
            {
                u32::from_str_radix(hex, 16)
                    .map_err(|_| anyhow::anyhow!("{name} must be a u32 (hex or decimal)"))
            } else {
                t.parse::<u32>()
                    .map_err(|_| anyhow::anyhow!("{name} must be a u32 (hex or decimal)"))
            }
        }
        _ => Ok(default),
    }
}

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

/// `Default` exists only in test builds. Production must go through
/// `Config::from_env`, which validates; a defaulted Config has an empty bind
/// address and no signing key, and offering that to non-test code is a footgun.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(Default))]
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
    /// Ceiling on distinct `game` label values in Prometheus when no allowlist
    /// is set. Games past the cap fold into `other`.
    pub metrics_game_label_limit: usize,
    /// UDP delay-sync input relay (star fan-out). Off disables open_session.
    pub input_relay_enabled: bool,
    /// Socket bind for the input relay (e.g. `0.0.0.0:8777`).
    pub input_relay_bind: String,
    /// Host string written into launch endpoints. Defaults from
    /// `INPUT_RELAY_ADVERTISE_HOST` → `PUBLIC_HOST` → `LOBBY_PUBLIC_HOST`,
    /// then STUN public IPv4 at startup. Never defaults to loopback.
    pub input_relay_advertise_host: String,
    /// Port written into launch endpoints (may differ from bind when NAT'd).
    pub input_relay_advertise_port: u16,
    /// Optional RFC1918 host for same-LAN / split-horizon lobbies. When every
    /// seated member's WebSocket peer IP is a direct LAN address (not the
    /// hairpin gateway), launch uses this instead of the public advertise.
    pub input_relay_lan_host: String,
    /// LAN default gateway / hairpin source IP (e.g. `192.168.66.1`). Peers
    /// that appear as this address are treated as NAT hairpin, not on-LAN.
    /// Empty → guess `<LAN_HOST>/24` → `.1` when `INPUT_RELAY_LAN_HOST` is set.
    pub input_relay_lan_gateway: String,
    /// When true, allow advertising 127.0.0.1 (same-machine-only testing).
    pub input_relay_allow_loopback: bool,
    /// MaxMind GeoLite2/GeoIP2 Country database (.mmdb). When set, every
    /// client's country is resolved from its TCP source IP and shown as a
    /// flag in lobbies. Unset = no flags; private / loopback peers never get one.
    pub geoip_db_path: Option<String>,
    /// Trust `X-Forwarded-For` for the client's address (GeoIP only).
    ///
    /// OFF by default, and it must stay that way: the header is trivially
    /// spoofable by anyone talking to the server directly, so trusting it
    /// unconditionally would let a client choose its own flag. Turn it on only
    /// when the server sits behind a reverse proxy that sets the header
    /// itself -- which is also the case where every peer otherwise arrives as
    /// the proxy's loopback address and nobody gets a flag at all.
    pub trust_proxy_header: bool,
    /// ---- Discord login (all optional; absent = the server behaves exactly
    /// as it did before Discord existed) ------------------------------------
    ///
    /// `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET`. The secret never appears
    /// in the repo and is read only from the environment, like the JWT keys.
    /// With either unset the login routes refuse to start a flow and every
    /// client is a guest.
    pub discord_client_id: Option<String>,
    pub discord_client_secret: Option<String>,
    /// `DISCORD_REDIRECT_URL` -- must match the Discord app registration byte
    /// for byte.
    pub discord_redirect_url: Option<String>,
    /// `DISCORD_REQUIRED`. **Default false, and that default is the
    /// compatibility promise**: a client that knows nothing about Discord
    /// connects, seats, chats and hosts exactly as it does today. Turning this
    /// on is a deliberate act that locks out every client older than the
    /// integration, so it stays off until every shipped port has caught up.
    pub discord_required: bool,
    /// `DISCORD_GUILD_ID`. When set, a login must be a member of this guild.
    ///
    /// This is what makes a Discord ban a netplay ban: someone removed from
    /// the server stops passing the membership check on their next login, with
    /// no separate ban list to maintain. Unset = any Discord account may play.
    /// Setting it also adds the `guilds.members.read` scope to the login.
    pub discord_guild_id: Option<String>,
    /// `GUEST_CAN_CHAT` / `GUEST_CAN_HOST`. Default true, i.e. today's
    /// behaviour. These are the levers to pull if abuse arrives before every
    /// client has updated -- they degrade what an unauthenticated client may
    /// do without disconnecting it.
    pub guest_can_chat: bool,
    pub guest_can_host: bool,
    /// Mask profanity / slurs in relayed chat (default on). `CHAT_FILTER=0`
    /// turns it off; clients still filter on arrival.
    pub chat_filter_enabled: bool,
    /// Optional file of extra chat-filter entries (same format as the
    /// built-in list), appended at startup. `CHAT_FILTER_EXTRA_PATH`.
    pub chat_filter_extra_path: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        // MotK / psxrecomp clients default to ws://127.0.0.1:8765.
        let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8765".to_string());
        let database_url = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty());
        let require_auth = parse_bool_env(&env::var("REQUIRE_AUTH").unwrap_or_default());
        let discord_client_id = env::var("DISCORD_CLIENT_ID").ok().filter(|s| !s.trim().is_empty());
        let discord_client_secret =
            env::var("DISCORD_CLIENT_SECRET").ok().filter(|s| !s.trim().is_empty());
        let discord_redirect_url =
            env::var("DISCORD_REDIRECT_URL").ok().filter(|s| !s.trim().is_empty());
        let discord_required = parse_bool_env(&env::var("DISCORD_REQUIRED").unwrap_or_default());
        let discord_guild_id = env::var("DISCORD_GUILD_ID").ok().filter(|s| !s.trim().is_empty());
        /* Default TRUE for both: absent configuration must mean "behaves as it
         * always did", never "quietly stricter". */
        let guest_can_chat = match env::var("GUEST_CAN_CHAT") {
            Ok(v) if !v.trim().is_empty() => parse_bool_env(&v),
            _ => true,
        };
        let guest_can_host = match env::var("GUEST_CAN_HOST") {
            Ok(v) if !v.trim().is_empty() => parse_bool_env(&v),
            _ => true,
        };
        let jwt_secret_current = env::var("JWT_SECRET_CURRENT")
            .ok()
            .filter(|s| !s.is_empty());
        let jwt_secret_previous = env::var("JWT_SECRET_PREVIOUS")
            .ok()
            .filter(|s| !s.is_empty());

        /* Discord login has three env vars of its own AND needs the JWT key,
         * because the last thing a successful login does is mint a session.
         * Say so at startup, not at the end of a player's sign-in: an
         * incomplete setup here otherwise surfaces as "Login failed" after
         * the player has already authorised in their browser, which points
         * at nothing.
         *
         * Warned, not fatal. The rest of the server -- LAN, guest play, the
         * lobby list -- works perfectly without Discord, and refusing to boot
         * would take working netplay down over an optional feature. */
        if discord_client_id.is_some()
            || discord_client_secret.is_some()
            || discord_redirect_url.is_some()
        {
            let mut missing: Vec<&str> = Vec::new();
            if discord_client_id.is_none() {
                missing.push("DISCORD_CLIENT_ID");
            }
            if discord_client_secret.is_none() {
                missing.push("DISCORD_CLIENT_SECRET");
            }
            if discord_redirect_url.is_none() {
                missing.push("DISCORD_REDIRECT_URL");
            }
            if jwt_secret_current.is_none() {
                missing.push("JWT_SECRET_CURRENT");
            }
            if !missing.is_empty() {
                tracing::warn!(
                    missing = missing.join(", "),
                    "Discord login is partly configured and cannot complete a sign-in. Players will reach 'Login failed' after authorising in their browser. Set the listed variables, or unset every DISCORD_* variable to turn sign-in off cleanly."
                );
            }
        }

        if require_auth && jwt_secret_current.is_none() {
            bail!(
                "REQUIRE_AUTH is set but JWT_SECRET_CURRENT is missing. \
                 Set JWT_SECRET_CURRENT via environment or secrets manager (see docs/SECURITY.md)."
            );
        }

        let default_input_delay = parse_u8_env("LOBBY_DEFAULT_INPUT_DELAY", 2)?;
        let default_slot_count = parse_u8_env("LOBBY_DEFAULT_SLOT_COUNT", 2)?;
        if !(2..=8).contains(&default_slot_count) {
            bail!("LOBBY_DEFAULT_SLOT_COUNT must be between 2 and 8");
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

        let metrics_game_label_limit = parse_u64_env(
            "METRICS_GAME_LABEL_LIMIT",
            crate::metrics::DEFAULT_GAME_LABEL_LIMIT as u64,
        )
        .clamp(1, 1024) as usize;

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
        /* Prefer an explicit relay / public host. Do NOT fall back to
         * COTURN_HOST — Coturn is often a different machine than the lobby
         * UDP SFU. Empty or loopback is replaced by STUN public IPv4 in
         * `resolve_input_relay_advertise` before the relay binds. */
        let input_relay_advertise_host = env::var("INPUT_RELAY_ADVERTISE_HOST")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                env::var("PUBLIC_HOST")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            })
            .or_else(|| {
                env::var("LOBBY_PUBLIC_HOST")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            })
            .unwrap_or_default();
        let input_relay_lan_host = env::var("INPUT_RELAY_LAN_HOST")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_default();
        let input_relay_lan_gateway = env::var("INPUT_RELAY_LAN_GATEWAY")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_default();
        let input_relay_allow_loopback =
            parse_bool_env(&env::var("INPUT_RELAY_ALLOW_LOOPBACK").unwrap_or_default());

        let geoip_db_path = env::var("GEOIP_DB_PATH").ok().filter(|s| !s.is_empty());
        let trust_proxy_header =
            parse_bool_env(&env::var("TRUST_PROXY_HEADER").unwrap_or_default());
        let chat_filter_enabled = match env::var("CHAT_FILTER") {
            Ok(v) if !v.trim().is_empty() => parse_bool_env(&v),
            _ => true,
        };
        let chat_filter_extra_path =
            env::var("CHAT_FILTER_EXTRA_PATH").ok().filter(|s| !s.is_empty());

        Ok(Config {
            geoip_db_path,
            trust_proxy_header,
            chat_filter_enabled,
            chat_filter_extra_path,
            bind_addr,
            database_url,
            require_auth,
            discord_client_id,
            discord_client_secret,
            discord_redirect_url,
            discord_required,
            discord_guild_id,
            guest_can_chat,
            guest_can_host,
            jwt_secret_current,
            jwt_secret_previous,
            default_input_delay,
            default_slot_count,
            room_idle_secs,
            heartbeat_timeout_secs,
            protocol_magic,
            game_allowlist,
            metrics_game_label_limit,
            input_relay_enabled,
            input_relay_bind,
            input_relay_advertise_host,
            input_relay_advertise_port,
            input_relay_lan_host,
            input_relay_lan_gateway,
            input_relay_allow_loopback,
        })
    }

    /// Hairpin / gateway IP used to reject false "all peers local" picks.
    pub fn effective_input_relay_lan_gateway(&self) -> Option<String> {
        let explicit = self.input_relay_lan_gateway.trim();
        if !explicit.is_empty() {
            return Some(explicit.to_string());
        }
        guess_lan_gateway_from_host(self.input_relay_lan_host.trim())
    }

    /// Replace empty/loopback relay advertise with STUN-discovered public IPv4.
    /// Call once after `from_env` and before `InputRelay::start`.
    pub fn resolve_input_relay_advertise(&mut self) -> Result<()> {
        use crate::public_ip::{advertise_host_needs_public, discover_ipv4};
        use std::time::Duration;

        if !self.input_relay_enabled {
            if self.input_relay_advertise_host.is_empty() {
                self.input_relay_advertise_host = "127.0.0.1".to_string();
            }
            return Ok(());
        }

        let needs = advertise_host_needs_public(&self.input_relay_advertise_host);
        if !needs {
            return Ok(());
        }

        if self.input_relay_allow_loopback {
            if self.input_relay_advertise_host.is_empty() {
                self.input_relay_advertise_host = "127.0.0.1".to_string();
            }
            tracing::warn!(
                advertise = %format!(
                    "{}:{}",
                    self.input_relay_advertise_host, self.input_relay_advertise_port
                ),
                "INPUT_RELAY_ALLOW_LOOPBACK=1 — remote peers cannot dial this relay"
            );
            return Ok(());
        }

        let ip = discover_ipv4(Duration::from_millis(750))?;
        tracing::info!(
            %ip,
            port = self.input_relay_advertise_port,
            "input relay advertise host = public IPv4 (STUN); set PUBLIC_HOST to pin a DNS name"
        );
        self.input_relay_advertise_host = ip;
        Ok(())
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

/// Guess `<a.b.c>.1` from an IPv4 LAN host (common home-router convention).
fn guess_lan_gateway_from_host(lan_host: &str) -> Option<String> {
    let parts: Vec<&str> = lan_host.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    for p in &parts {
        if p.parse::<u8>().is_err() {
            return None;
        }
    }
    Some(format!("{}.{}.{}.1", parts[0], parts[1], parts[2]))
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
            if let Some(hex) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guess_gateway_from_lan_host() {
        assert_eq!(
            guess_lan_gateway_from_host("192.168.66.3").as_deref(),
            Some("192.168.66.1")
        );
        assert_eq!(
            guess_lan_gateway_from_host("10.0.0.50").as_deref(),
            Some("10.0.0.1")
        );
        assert!(guess_lan_gateway_from_host("netplay.example.com").is_none());
        assert!(guess_lan_gateway_from_host("").is_none());
    }
}

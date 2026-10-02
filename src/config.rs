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
    /// `DISCORD_REQUIRED`. **Default false**, so a deployment that sets nothing
    /// behaves as before. When on, the server is closed to anyone without a
    /// Discord-linked session: a WebSocket must `hello` with a valid session
    /// within a short grace period or it is dropped, may send nothing else
    /// until it has, and is shown no lobbies or players; the anonymous `/v1`
    /// HTTP API is refused. This locks out every client that cannot sign in.
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
    /// `AUTOMATCH_RETENTION_DAYS`. How long the two automatch tables keep
    /// history. `0` means "prune a row once it stops affecting a decision",
    /// which is a floor, not a purge -- see `automatch::cutoff_secs`.
    ///
    /// It has a dial because `automatch_pairings` records who played whom,
    /// and keeping that forever should be a deliberate choice rather than an
    /// inherited default (`docs/PRIVACY.md`).
    pub automatch_retention_days: u64,
    /// `AUTOMATCH_REMATCH_COOLDOWN_SECS`. How long the pairing loop prefers a
    /// different opponent. Also the floor under the pairings table's prune:
    /// a row inside this window is still steering matches and must not be
    /// deleted for being old.
    pub automatch_rematch_cooldown_secs: u64,
    /// `AUTOMATCH_RULESETS_PATH`. Absent or unusable = automatch off. There is
    /// no separate enable flag: one place to look, and no state where a flag
    /// and the config disagree.
    pub automatch_rulesets_path: String,
    /// `CHAT_REPORT_DUMP_DIR`. Where a chat report's transcript is written
    /// alongside its database row. Empty disables the dumps; the row is still
    /// written, so moderation keeps working without them.
    ///
    /// One directory for every title and every console. The files are named by
    /// report id and date and NEVER by game or platform: one queue is read by
    /// one person, and splitting the evidence by console would fragment a
    /// moderation record along a line that has nothing to do with moderation.
    /// Which game it was is inside the file, where it belongs.
    pub chat_report_dump_dir: String,
    /// `AUTOMATCH_QUEUE_MAX`, `AUTOMATCH_ACCEPT_SECS`,
    /// `AUTOMATCH_START_DELAY_SECS`.
    pub automatch_queue_max: usize,
    pub automatch_accept_secs: u64,
    pub automatch_start_delay_secs: u64,
    /// `AUTOMATCH_DODGE_COOLDOWNS`, seconds, by strikes in the last 24 h.
    /// Empty means dodges are recorded but cost nothing -- a deployment may
    /// want the record before it wants the penalty.
    pub automatch_dodge_cooldowns: Vec<u64>,
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
        /* Retention has a default rather than being required, but the
         * default is a real answer (30 days) instead of "forever". */
        let automatch_retention_days = parse_u64_env("AUTOMATCH_RETENTION_DAYS", 30);
        let automatch_rematch_cooldown_secs =
            parse_u64_env("AUTOMATCH_REMATCH_COOLDOWN_SECS", 300);
        let chat_report_dump_dir = env::var("CHAT_REPORT_DUMP_DIR")
            .ok()
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "data/reports".to_string());
        let automatch_rulesets_path = env::var("AUTOMATCH_RULESETS_PATH")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "data/automatch_rulesets.toml".to_string());
        let automatch_queue_max = parse_u64_env("AUTOMATCH_QUEUE_MAX", 256) as usize;
        let automatch_accept_secs = parse_u64_env("AUTOMATCH_ACCEPT_SECS", 15).max(5);
        let automatch_start_delay_secs = parse_u64_env("AUTOMATCH_START_DELAY_SECS", 3);
        /* A malformed ladder degrades to "no penalty", not to a default
         * somebody did not ask for: the operator said something about
         * cooldowns and guessing over them is worse than charging nothing. */
        let automatch_dodge_cooldowns = match env::var("AUTOMATCH_DODGE_COOLDOWNS") {
            Ok(v) if !v.trim().is_empty() => v
                .split(',')
                .filter_map(|t| t.trim().parse::<u64>().ok())
                .collect(),
            /* One minute, flat, however many times you have declined.
             *
             * This was an escalating ladder (60 / 300 / 900). Escalation is
             * the right shape for a populated queue, where a repeat dodger
             * costs a stream of other people their match. It is the wrong
             * shape for the pools these servers actually have: three declines
             * in a testing afternoon put a player on a FIFTEEN MINUTE lockout
             * from a queue that might have two people in it, which punishes
             * the person still trying to play far harder than it deters
             * anything.
             *
             * A minute is long enough that declining is not free and short
             * enough that it is not a ban. Raise it, or restore a ladder,
             * with AUTOMATCH_DODGE_COOLDOWNS once a queue is busy enough for
             * repeat dodging to be somebody else's problem rather than just
             * the dodger's. */
            _ => vec![60],
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

        if discord_required
            && (discord_client_id.is_none()
                || discord_client_secret.is_none()
                || discord_redirect_url.is_none()
                || jwt_secret_current.is_none())
        {
            bail!(
                "DISCORD_REQUIRED is set but Discord sign-in is not fully configured \
                 (need DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET, DISCORD_REDIRECT_URL and \
                 JWT_SECRET_CURRENT). Nobody could connect."
            );
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

        /* The server no longer relays matches and the code is gone. A
         * deployment that still sets INPUT_RELAY_ENABLED is refused at startup
         * rather than silently ignored, so nobody believes they have a relay
         * they do not. */
        if let Ok(s) = env::var("INPUT_RELAY_ENABLED") {
            if parse_bool_env(&s) {
                bail!(
                    "INPUT_RELAY_ENABLED is set, but server-side match relaying (the UDP SFU) \
                     has been removed. Unset it; matches run peer-to-peer or through a host relay."
                );
            }
        }

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
            automatch_retention_days,
            automatch_rematch_cooldown_secs,
            chat_report_dump_dir,
            automatch_rulesets_path,
            automatch_queue_max,
            automatch_accept_secs,
            automatch_start_delay_secs,
            automatch_dodge_cooldowns,
            jwt_secret_current,
            jwt_secret_previous,
            default_input_delay,
            default_slot_count,
            room_idle_secs,
            heartbeat_timeout_secs,
            protocol_magic,
            game_allowlist,
            metrics_game_label_limit,
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

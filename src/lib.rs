pub mod auth;
pub mod automatch;
pub mod chat_filter;
pub mod config;
pub mod discord_auth;
pub mod identity;
pub mod input_relay;
pub mod ip_country;
pub mod metrics;
pub mod names;
pub mod players;
pub mod public_ip;
pub mod rooms;
pub mod secrets;
pub mod routes;
pub mod signal;
pub mod turn_credentials;
pub mod udp_pktinfo;
pub mod ws_lobby;

use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::Mutex;

impl AppState {
    /// Park a failed login so the launcher's poll returns an error instead of
    /// spinning until the pairing code ages out.
    pub async fn discord_logins_finish_err(&self, code: &str, why: &str) {
        self.discord_logins.fail(code, why).await;
    }
}

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<config::Config>,
    pub rooms: Arc<Mutex<rooms::RoomRegistry>>,
    pub signals: Arc<Mutex<signal::SignalStore>>,
    /// WebSocket lobby hub (MotK / psxrecomp JSON protocol).
    pub ws_lobby: ws_lobby::WsLobbyHub,
    /// UDP star-topology delay-sync input relay.
    pub input_relay: input_relay::InputRelay,
    /// Discord logins in flight, keyed by the pairing code the launcher polls.
    pub discord_logins: discord_auth::LoginStore,
    /// One-shot nonces for device challenge-response.
    pub discord_challenges: discord_auth::ChallengeStore,
    /// Shared outbound HTTP client for the Discord API.
    pub http: reqwest::Client,
    /// When true (CLI `--debug`): HTTP trace layer + verbose lobby logs.
    pub debug: bool,
}

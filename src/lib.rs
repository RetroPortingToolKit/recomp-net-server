pub mod auth;
pub mod config;
pub mod input_relay;
pub mod metrics;
pub mod players;
pub mod rooms;
pub mod routes;
pub mod signal;
pub mod turn_credentials;
pub mod ws_lobby;

use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    /// When true (CLI `--debug`): HTTP trace layer + verbose lobby logs.
    pub debug: bool,
}

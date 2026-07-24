//! Process-lifetime usage counters and Prometheus helpers.
//!
//! Labels stay low-cardinality (result codes only). Live game breakdowns are
//! exposed via `/stats` from in-memory lobby/room state — not as metric labels.

use metrics::{counter, describe_counter, describe_gauge, gauge};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

static WS_CONNECTS: AtomicU64 = AtomicU64::new(0);
static WS_DISCONNECTS: AtomicU64 = AtomicU64::new(0);
static WS_LOBBY_CREATES: AtomicU64 = AtomicU64::new(0);
static WS_LOBBY_JOINS: AtomicU64 = AtomicU64::new(0);
static WS_LOBBY_JOIN_FAILURES: AtomicU64 = AtomicU64::new(0);
static WS_LOBBY_STARTS: AtomicU64 = AtomicU64::new(0);
static WS_LOBBY_DESTROYS: AtomicU64 = AtomicU64::new(0);
static WS_SIGNALS: AtomicU64 = AtomicU64::new(0);
static HTTP_PLAYERS_CREATED: AtomicU64 = AtomicU64::new(0);
static HTTP_ROOM_CREATES: AtomicU64 = AtomicU64::new(0);
static HTTP_ROOM_JOINS: AtomicU64 = AtomicU64::new(0);
static HTTP_ROOM_JOIN_FAILURES: AtomicU64 = AtomicU64::new(0);
static HTTP_ROOM_STARTS: AtomicU64 = AtomicU64::new(0);
static HTTP_TURN_CREDENTIALS: AtomicU64 = AtomicU64::new(0);
static INPUT_RELAY_SESSIONS_OPENED: AtomicU64 = AtomicU64::new(0);
static INPUT_RELAY_SESSIONS_CLOSED: AtomicU64 = AtomicU64::new(0);
static INPUT_RELAY_FORWARDS: AtomicU64 = AtomicU64::new(0);
static INPUT_RELAY_DROPS: AtomicU64 = AtomicU64::new(0);

pub fn describe() {
    describe_counter!(
        "recomp_ws_connects_total",
        "WebSocket lobby clients that completed welcome"
    );
    describe_counter!(
        "recomp_ws_disconnects_total",
        "WebSocket lobby clients that disconnected"
    );
    describe_counter!(
        "recomp_ws_lobby_creates_total",
        "Successful WebSocket lobby creates"
    );
    describe_counter!(
        "recomp_ws_lobby_joins_total",
        "WebSocket lobby join attempts by result"
    );
    describe_counter!(
        "recomp_ws_lobby_starts_total",
        "Successful WebSocket match starts"
    );
    describe_counter!(
        "recomp_ws_lobby_destroys_total",
        "WebSocket lobbies destroyed"
    );
    describe_counter!(
        "recomp_ws_signals_total",
        "WebSocket ICE signal relay messages"
    );
    describe_counter!(
        "recomp_http_players_created_total",
        "HTTP /v1 players created"
    );
    describe_counter!(
        "recomp_http_room_creates_total",
        "Successful HTTP /v1 room creates"
    );
    describe_counter!(
        "recomp_http_room_joins_total",
        "HTTP /v1 room join attempts by result"
    );
    describe_counter!(
        "recomp_http_room_starts_total",
        "HTTP /v1 rooms marked running"
    );
    describe_counter!(
        "recomp_http_turn_credentials_total",
        "TURN credentials minted"
    );
    describe_gauge!(
        "recomp_ws_clients",
        "Currently connected WebSocket lobby clients"
    );
    describe_gauge!("recomp_ws_lobbies", "Currently open WebSocket lobbies");
    describe_gauge!("recomp_http_rooms", "Currently open HTTP /v1 rooms");
    describe_counter!(
        "recomp_input_relay_sessions_opened_total",
        "Input-relay sessions allocated at match start"
    );
    describe_counter!(
        "recomp_input_relay_sessions_closed_total",
        "Input-relay sessions closed (lobby destroy / rematch)"
    );
    describe_counter!(
        "recomp_input_relay_forwards_total",
        "Datagrams successfully fan-out to peer seats"
    );
    describe_counter!(
        "recomp_input_relay_drops_total",
        "Datagrams dropped by the input relay"
    );
    describe_gauge!(
        "recomp_input_relay_sessions",
        "Live input-relay sessions"
    );
}

fn bump(atomic: &AtomicU64) -> u64 {
    atomic.fetch_add(1, Ordering::Relaxed) + 1
}

pub fn ws_connected() {
    bump(&WS_CONNECTS);
    counter!("recomp_ws_connects_total").increment(1);
}

pub fn ws_disconnected() {
    bump(&WS_DISCONNECTS);
    counter!("recomp_ws_disconnects_total").increment(1);
}

pub fn ws_lobby_created() {
    bump(&WS_LOBBY_CREATES);
    counter!("recomp_ws_lobby_creates_total").increment(1);
}

pub fn ws_lobby_join_ok() {
    bump(&WS_LOBBY_JOINS);
    counter!("recomp_ws_lobby_joins_total", "result" => "ok").increment(1);
}

pub fn ws_lobby_join_fail(code: &'static str) {
    bump(&WS_LOBBY_JOIN_FAILURES);
    counter!("recomp_ws_lobby_joins_total", "result" => code).increment(1);
}

pub fn ws_lobby_started() {
    bump(&WS_LOBBY_STARTS);
    counter!("recomp_ws_lobby_starts_total").increment(1);
}

pub fn ws_lobby_destroyed() {
    bump(&WS_LOBBY_DESTROYS);
    counter!("recomp_ws_lobby_destroys_total").increment(1);
}

pub fn ws_signal_relayed() {
    bump(&WS_SIGNALS);
    counter!("recomp_ws_signals_total").increment(1);
}

pub fn http_player_created() {
    bump(&HTTP_PLAYERS_CREATED);
    counter!("recomp_http_players_created_total").increment(1);
}

pub fn http_room_created() {
    bump(&HTTP_ROOM_CREATES);
    counter!("recomp_http_room_creates_total").increment(1);
}

pub fn http_room_join_ok() {
    bump(&HTTP_ROOM_JOINS);
    counter!("recomp_http_room_joins_total", "result" => "ok").increment(1);
}

pub fn http_room_join_fail(code: &'static str) {
    bump(&HTTP_ROOM_JOIN_FAILURES);
    counter!("recomp_http_room_joins_total", "result" => code).increment(1);
}

pub fn http_room_started() {
    bump(&HTTP_ROOM_STARTS);
    counter!("recomp_http_room_starts_total").increment(1);
}

pub fn http_turn_credentials_issued() {
    bump(&HTTP_TURN_CREDENTIALS);
    counter!("recomp_http_turn_credentials_total").increment(1);
}

pub fn input_relay_session_opened() {
    bump(&INPUT_RELAY_SESSIONS_OPENED);
    counter!("recomp_input_relay_sessions_opened_total").increment(1);
}

pub fn input_relay_session_closed() {
    bump(&INPUT_RELAY_SESSIONS_CLOSED);
    counter!("recomp_input_relay_sessions_closed_total").increment(1);
}

pub fn input_relay_forwarded(n: u64) {
    if n == 0 {
        return;
    }
    INPUT_RELAY_FORWARDS.fetch_add(n, Ordering::Relaxed);
    counter!("recomp_input_relay_forwards_total").increment(n);
}

pub fn input_relay_drop(reason: &'static str) {
    bump(&INPUT_RELAY_DROPS);
    counter!("recomp_input_relay_drops_total", "reason" => reason).increment(1);
}

pub fn set_input_relay_sessions(n: usize) {
    gauge!("recomp_input_relay_sessions").set(n as f64);
}

pub fn set_gauges(ws_clients: usize, ws_lobbies: usize, http_rooms: usize) {
    gauge!("recomp_ws_clients").set(ws_clients as f64);
    gauge!("recomp_ws_lobbies").set(ws_lobbies as f64);
    gauge!("recomp_http_rooms").set(http_rooms as f64);
}

#[derive(Serialize)]
pub struct Totals {
    pub ws_connects: u64,
    pub ws_disconnects: u64,
    pub ws_lobby_creates: u64,
    pub ws_lobby_joins: u64,
    pub ws_lobby_join_failures: u64,
    pub ws_lobby_starts: u64,
    pub ws_lobby_destroys: u64,
    pub ws_signals: u64,
    pub http_players_created: u64,
    pub http_room_creates: u64,
    pub http_room_joins: u64,
    pub http_room_join_failures: u64,
    pub http_room_starts: u64,
    pub http_turn_credentials: u64,
    pub input_relay_sessions_opened: u64,
    pub input_relay_sessions_closed: u64,
    pub input_relay_forwards: u64,
    pub input_relay_drops: u64,
}

pub fn totals() -> Totals {
    Totals {
        ws_connects: WS_CONNECTS.load(Ordering::Relaxed),
        ws_disconnects: WS_DISCONNECTS.load(Ordering::Relaxed),
        ws_lobby_creates: WS_LOBBY_CREATES.load(Ordering::Relaxed),
        ws_lobby_joins: WS_LOBBY_JOINS.load(Ordering::Relaxed),
        ws_lobby_join_failures: WS_LOBBY_JOIN_FAILURES.load(Ordering::Relaxed),
        ws_lobby_starts: WS_LOBBY_STARTS.load(Ordering::Relaxed),
        ws_lobby_destroys: WS_LOBBY_DESTROYS.load(Ordering::Relaxed),
        ws_signals: WS_SIGNALS.load(Ordering::Relaxed),
        http_players_created: HTTP_PLAYERS_CREATED.load(Ordering::Relaxed),
        http_room_creates: HTTP_ROOM_CREATES.load(Ordering::Relaxed),
        http_room_joins: HTTP_ROOM_JOINS.load(Ordering::Relaxed),
        http_room_join_failures: HTTP_ROOM_JOIN_FAILURES.load(Ordering::Relaxed),
        http_room_starts: HTTP_ROOM_STARTS.load(Ordering::Relaxed),
        http_turn_credentials: HTTP_TURN_CREDENTIALS.load(Ordering::Relaxed),
        input_relay_sessions_opened: INPUT_RELAY_SESSIONS_OPENED.load(Ordering::Relaxed),
        input_relay_sessions_closed: INPUT_RELAY_SESSIONS_CLOSED.load(Ordering::Relaxed),
        input_relay_forwards: INPUT_RELAY_FORWARDS.load(Ordering::Relaxed),
        input_relay_drops: INPUT_RELAY_DROPS.load(Ordering::Relaxed),
    }
}

#[derive(Serialize)]
pub struct StatsSnapshot {
    pub ws_clients: usize,
    pub ws_lobbies: usize,
    pub ws_lobbies_by_game: BTreeMap<String, usize>,
    pub http_rooms: usize,
    pub http_rooms_by_game: BTreeMap<String, usize>,
    pub totals: Totals,
}

pub fn room_error_code(e: &crate::rooms::RoomError) -> &'static str {
    use crate::rooms::RoomError;
    match e {
        RoomError::NotFound => "not_found",
        RoomError::Full => "full",
        RoomError::NotJoinable => "not_joinable",
        RoomError::BadJoinCode => "bad_join_code",
        RoomError::GameMismatch => "game_mismatch",
        RoomError::VersionMismatch => "version_mismatch",
        RoomError::AlreadyJoined => "already_joined",
        RoomError::NotMember => "not_member",
        RoomError::NotHost => "not_host",
        RoomError::BadSlotCount => "bad_slot_count",
        RoomError::GameNotAllowed => "game_not_allowed",
    }
}

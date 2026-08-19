//! Process-lifetime usage counters and Prometheus helpers.
//!
//! Labels stay low-cardinality: result codes, a `surface` (`ws` / `http`), and a
//! **bounded** `game` label on match-start series only. Game names arrive from
//! clients, so `game_label` normalizes them and folds anything outside
//! `LOBBY_GAME_ALLOWLIST` (or past `METRICS_GAME_LABEL_LIMIT` when no allowlist
//! is set) into `other` — a scrape can never blow up the series count.
//! Waiting vs in-match is split: `recomp_ws_matches` / `recomp_http_rooms_running`
//! plus `recomp_input_relay_sessions_active` (SFU pads actually flowing).

use metrics::{counter, describe_counter, describe_gauge, gauge};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

/// Surface label for WebSocket lobbies.
pub const SURFACE_WS: &str = "ws";
/// Surface label for HTTP `/v1` rooms.
pub const SURFACE_HTTP: &str = "http";

/// Default ceiling on distinct `game` label values when no allowlist is set.
pub const DEFAULT_GAME_LABEL_LIMIT: usize = 64;
/// Longest normalized game label kept (longer names are truncated).
const GAME_LABEL_MAX_LEN: usize = 48;
/// Fold target for games outside the allowlist / past the cap.
const GAME_LABEL_OTHER: &str = "other";
/// Fold target for empty or all-punctuation game names.
const GAME_LABEL_UNKNOWN: &str = "unknown";

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

struct GameLabels {
    /// True when `LOBBY_GAME_ALLOWLIST` is set: only seeded names get a label.
    strict: bool,
    limit: usize,
    known: HashMap<String, &'static str>,
}

static GAME_LABELS: OnceLock<RwLock<GameLabels>> = OnceLock::new();

fn game_labels() -> &'static RwLock<GameLabels> {
    GAME_LABELS.get_or_init(|| {
        RwLock::new(GameLabels {
            strict: false,
            limit: DEFAULT_GAME_LABEL_LIMIT,
            known: HashMap::new(),
        })
    })
}

/// Seed the bounded `game` label set at startup. A non-empty allowlist pins the
/// series to exactly those games (everything else is `other`); an empty one
/// learns names as matches start, up to `limit`.
pub fn init_game_labels(allowlist: &[String], limit: usize) {
    let mut g = game_labels().write().unwrap_or_else(|e| e.into_inner());
    g.limit = limit.max(1);
    g.strict = !allowlist.is_empty();
    /* Startup-only: drop anything learned before so a strict allowlist is
     * exactly the published set. */
    g.known.clear();
    for raw in allowlist {
        let name = normalize_game(raw);
        if name.is_empty() || g.known.contains_key(&name) {
            continue;
        }
        let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
        g.known.insert(name, leaked);
    }
}

/// Lowercase, strip to `[a-z0-9._-]`, collapse separators, cap the length.
fn normalize_game(raw: &str) -> String {
    let mut out = String::new();
    let mut prev_sep = false;
    for ch in raw.trim().chars() {
        if out.len() >= GAME_LABEL_MAX_LEN {
            break;
        }
        let c = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else if matches!(ch, '.' | '-' | '_') {
            ch
        } else {
            '_'
        };
        if c == '_' {
            if prev_sep || out.is_empty() {
                continue;
            }
            prev_sep = true;
        } else {
            prev_sep = false;
        }
        out.push(c);
    }
    while out.ends_with(['_', '-', '.']) {
        out.pop();
    }
    out
}

/// Map a client-supplied game name onto a bounded, Prometheus-safe label.
pub fn game_label(raw: &str) -> &'static str {
    let name = normalize_game(raw);
    if name.is_empty() {
        return GAME_LABEL_UNKNOWN;
    }
    {
        let g = game_labels().read().unwrap_or_else(|e| e.into_inner());
        if let Some(label) = g.known.get(&name) {
            return label;
        }
        if g.strict || g.known.len() >= g.limit {
            return GAME_LABEL_OTHER;
        }
    }
    let mut g = game_labels().write().unwrap_or_else(|e| e.into_inner());
    if let Some(label) = g.known.get(&name) {
        return label;
    }
    if g.strict || g.known.len() >= g.limit {
        return GAME_LABEL_OTHER;
    }
    /* Bounded by `limit`, so the leak is a one-time per-game interning cost. */
    let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
    g.known.insert(name, leaked);
    leaked
}

/// Process-lifetime match starts keyed by `(surface, game)` — mirrors the
/// Prometheus counter so `/stats` works without a scrape target.
static MATCH_STARTS: OnceLock<RwLock<BTreeMap<(&'static str, &'static str), MatchTotals>>> =
    OnceLock::new();

#[derive(Default, Clone, Copy, Serialize)]
pub struct MatchTotals {
    pub starts: u64,
    pub players: u64,
}

fn match_starts() -> &'static RwLock<BTreeMap<(&'static str, &'static str), MatchTotals>> {
    MATCH_STARTS.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Record a match start on `surface` for `game` with `players` seated.
/// Publishes `recomp_match_starts_total` / `recomp_match_players_total`.
pub fn match_started(surface: &'static str, game: &str, players: usize) {
    let label = game_label(game);
    let players = players as u64;
    counter!("recomp_match_starts_total", "surface" => surface, "game" => label).increment(1);
    if players > 0 {
        counter!("recomp_match_players_total", "surface" => surface, "game" => label)
            .increment(players);
    }
    let mut g = match_starts().write().unwrap_or_else(|e| e.into_inner());
    let entry = g.entry((surface, label)).or_default();
    entry.starts += 1;
    entry.players += players;
}

/// Lifetime match starts for one surface, keyed by game label (for `/stats`).
pub fn match_starts_by_game(surface: &str) -> BTreeMap<String, MatchTotals> {
    let g = match_starts().read().unwrap_or_else(|e| e.into_inner());
    g.iter()
        .filter(|((s, _), _)| *s == surface)
        .map(|((_, game), totals)| ((*game).to_string(), *totals))
        .collect()
}

/// Label pairs currently published on `recomp_matches_active`, so a game that
/// drops to zero is zeroed instead of freezing at its last scraped value.
static ACTIVE_MATCH_LABELS: OnceLock<RwLock<BTreeSet<(&'static str, &'static str)>>> =
    OnceLock::new();

/// Publish live in-match counts per game for one surface. `counts` is the raw
/// in-memory breakdown; names fold onto the same bounded label set as starts.
pub fn set_matches_by_game(surface: &'static str, counts: &BTreeMap<String, usize>) {
    let mut folded: BTreeMap<&'static str, usize> = BTreeMap::new();
    for (game, n) in counts {
        *folded.entry(game_label(game)).or_insert(0) += n;
    }
    let published = ACTIVE_MATCH_LABELS.get_or_init(|| RwLock::new(BTreeSet::new()));
    let mut g = published.write().unwrap_or_else(|e| e.into_inner());
    for (s, game) in g.iter() {
        if *s == surface && !folded.contains_key(game) {
            gauge!("recomp_matches_active", "surface" => *s, "game" => *game).set(0.0);
        }
    }
    for (game, n) in &folded {
        gauge!("recomp_matches_active", "surface" => surface, "game" => *game).set(*n as f64);
    }
    g.retain(|(s, _)| *s != surface);
    g.extend(folded.keys().map(|game| (surface, *game)));
}

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
    describe_gauge!(
        "recomp_ws_matches",
        "WebSocket lobbies that have started a match"
    );
    describe_gauge!("recomp_http_rooms", "Currently open HTTP /v1 rooms");
    describe_gauge!("recomp_http_rooms_running", "HTTP /v1 rooms marked running");
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
        "Live input-relay sessions (allocated at launch)"
    );
    describe_gauge!(
        "recomp_input_relay_sessions_active",
        "Input-relay sessions with recent UDP from at least two seats"
    );
    describe_counter!(
        "recomp_match_starts_total",
        "Matches started, by surface and bounded game label"
    );
    describe_counter!(
        "recomp_match_players_total",
        "Seated players summed over match starts, by surface and game"
    );
    describe_gauge!(
        "recomp_matches_active",
        "Matches currently in progress, by surface and game"
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

pub fn ws_lobby_started(game: &str, players: usize) {
    bump(&WS_LOBBY_STARTS);
    counter!("recomp_ws_lobby_starts_total").increment(1);
    match_started(SURFACE_WS, game, players);
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

pub fn http_room_started(game: &str, players: usize) {
    bump(&HTTP_ROOM_STARTS);
    counter!("recomp_http_room_starts_total").increment(1);
    match_started(SURFACE_HTTP, game, players);
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

pub fn set_input_relay_sessions_active(n: usize) {
    gauge!("recomp_input_relay_sessions_active").set(n as f64);
}

pub fn set_gauges(
    ws_clients: usize,
    ws_lobbies: usize,
    ws_matches: usize,
    http_rooms: usize,
    http_rooms_running: usize,
) {
    gauge!("recomp_ws_clients").set(ws_clients as f64);
    gauge!("recomp_ws_lobbies").set(ws_lobbies as f64);
    gauge!("recomp_ws_matches").set(ws_matches as f64);
    gauge!("recomp_http_rooms").set(http_rooms as f64);
    gauge!("recomp_http_rooms_running").set(http_rooms_running as f64);
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
    pub ws_lobbies_waiting: usize,
    pub ws_matches: usize,
    pub ws_lobbies_by_game: BTreeMap<String, usize>,
    pub ws_matches_by_game: BTreeMap<String, usize>,
    pub http_rooms: usize,
    pub http_rooms_running: usize,
    pub http_rooms_by_game: BTreeMap<String, usize>,
    pub http_rooms_running_by_game: BTreeMap<String, usize>,
    /// Lifetime match starts per bounded game label (mirrors Prometheus).
    pub ws_match_starts_by_game: BTreeMap<String, MatchTotals>,
    pub http_match_starts_by_game: BTreeMap<String, MatchTotals>,
    pub input_relay_sessions: usize,
    pub input_relay_sessions_active: usize,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_client_game_names() {
        assert_eq!(normalize_game("  Masters of Teras Kasi "), "masters_of_teras_kasi");
        assert_eq!(normalize_game("SF-Alpha_3.2"), "sf-alpha_3.2");
        assert_eq!(normalize_game("!!!"), "");
        assert_eq!(normalize_game(""), "");
        assert_eq!(normalize_game(&"x".repeat(200)).len(), GAME_LABEL_MAX_LEN);
    }

    /// Single test drives the whole registry: it is process-global state.
    #[test]
    fn game_labels_stay_bounded() {
        // Dev mode (no allowlist): learn names up to the cap, then fold.
        init_game_labels(&[], 2);
        assert_eq!(game_label("Klonoa"), "klonoa");
        assert_eq!(game_label("klonoa "), "klonoa");
        assert_eq!(game_label("Twisted Metal 4"), "twisted_metal_4");
        assert_eq!(game_label("Ape Escape"), GAME_LABEL_OTHER);
        assert_eq!(game_label("   "), GAME_LABEL_UNKNOWN);

        // Allowlist mode: only allowlisted games get their own label.
        init_game_labels(&["Ape Escape".to_string()], 64);
        assert_eq!(game_label("ape escape"), "ape_escape");
        assert_eq!(game_label("Some Unlisted Game"), GAME_LABEL_OTHER);
        // Names learned before init are dropped, not grandfathered in.
        assert_eq!(game_label("Klonoa"), GAME_LABEL_OTHER);
    }
}

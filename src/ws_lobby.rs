//! WebSocket lobby protocol for recomp-net hosts (MotK / psxrecomp client).
//!
//! Wire format matches the client contract documented in `docs/WS_LOBBY.md`
//! (JSON text frames, `"op"` field).

use axum::extract::connect_info::ConnectInfo;
use axum::http::HeaderMap;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::metrics;
use crate::AppState;

const MAX_SLOTS: usize = 8;
const MAX_LOBBIES: usize = 64;

/// Spectator seats a host may open, on top of the player seats.
///
/// Spectators are a SEPARATE pool, not a slice of `max_slots`: an eight-player
/// room can still be watched. The relay's own ceiling was raised to
/// MAX_SLOTS + MAX_SPECTATORS so a full room plus a full gallery still fits.
const MAX_SPECTATORS: usize = 4;

/// Seat indices are one namespace on the wire so `slot` / `from_slot` /
/// `to_slot` keep their existing shape: below the base is a player seat,
/// at or above it is `index - base` in the spectator pool.
///
/// A number, rather than a second pair of fields, because every seat-carrying
/// message already has an int and older servers reject an out-of-range one --
/// which is exactly the right answer from a server that has no spectators.
const SPECTATOR_SLOT_BASE: usize = 64;

fn is_spectator_seat(seat: usize) -> bool {
    seat >= SPECTATOR_SLOT_BASE
}

fn spectator_seat(index: usize) -> usize {
    SPECTATOR_SLOT_BASE + index
}

#[derive(Clone)]
struct Slot {
    player_id: String,
    display_name: String,
    ready: bool,
    /// Peer-advertised BIOS capability (opaque JSON from set_ready).
    bios_offer: Option<Value>,
    /// Peer installed-package catalog (opaque JSON from set_ready).
    mod_offer: Option<Value>,
    /// Peer memory-card offer (opaque JSON from set_ready): whether the peer
    /// has a card and opted in to bring it. Read on seat 1 by the host.
    memcard_offer: Option<Value>,
    /// Country (alpha-2) from GeoIP on the peer's IP; "" when unknown.
    country: String,
    /// Waiting-room ICE path report: "direct" | "relay" | "fail".
    ice_path: Option<String>,
    ice_path_at: Option<Instant>,
}

/// Path reports older than this are ignored (fail closed → SFU).
#[derive(Clone)]
struct Lobby {
    lobby_id: String,
    name: String,
    game_name: String,
    /// Release / build pin (semver or tag). Peers must match to join.
    game_version: String,
    /// TOC fingerprint (lowercase hex SHA-256). Empty = legacy host (no check).
    disc_fp: String,
    host_player_id: String,
    #[allow(dead_code)]
    host_bind: String,
    host_endpoint: String,
    /// Private RFC1918 UDP endpoints for same-LAN list RTT (no loopback).
    lan_endpoints: Vec<String>,
    guest_endpoint: String,
    password_hash: Option<[u8; 32]>,
    password_salt: Option<[u8; 16]>,
    max_slots: usize,
    session_id: u32,
    slots: Vec<Option<Slot>>,
    /// Host opt-in. False leaves `spectators` empty, so a joiner can never
    /// land in the gallery of a host who did not ask for one.
    allow_spectators: bool,
    /// The gallery. Seated like players and told everything players are told,
    /// but outside `player_count`, outside `all_ready`, and -- at the relay --
    /// unable to have a packet forwarded to anyone.
    spectators: Vec<Option<Slot>>,
    /* Host-authoritative sim-affecting settings (opaque JSON object). */
    match_caps: Option<Value>,
    /// Active UDP input-relay session (closed on destroy / rematch).
    relay_session_id: Option<u32>,
    /// True after a successful `start` until the lobby is destroyed.
    started: bool,
}

struct ClientMeta {
    player_id: String,
    display_name: String,
    /// TCP source IP as seen by the lobby (LAN vs WAN / hairpin signal).
    peer_ip: String,
    /// ISO 3166-1 alpha-2 from GeoIP on peer_ip; "" when unknown / private.
    country: String,
    lobby_id: Option<String>,
    /// Password-ok join waiting on missing mods (not seated).
    pending_mod_lobby: Option<String>,
    tx: broadcast::Sender<String>,
}

struct HubInner {
    clients: HashMap<String, ClientMeta>,
    lobbies: HashMap<String, Lobby>,
    next_session: u32,
}

#[derive(Clone, Default)]
pub struct WsLobbyHub {
    inner: Arc<Mutex<HubInner>>,
    /// GeoIP country reader (None = flags off). Shared, read-only.
    geoip: Arc<Option<maxminddb::Reader<Vec<u8>>>>,
}

impl Default for HubInner {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
            lobbies: HashMap::new(),
            next_session: 1,
        }
    }
}

impl WsLobbyHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// A hub that resolves each client's country from a MaxMind Country
    /// database. A missing or unreadable file is logged and flags stay off;
    /// nothing else about the lobby depends on it.
    pub fn with_geoip(path: Option<&str>) -> Self {
        let mut hub = Self::default();
        match path {
            Some(p) => match maxminddb::Reader::open_readfile(p) {
                Ok(r) => {
                    info!(path = p, "GeoIP country database loaded; flags on");
                    hub.geoip = Arc::new(Some(r));
                }
                Err(e) => warn!(path = p, error = %e, "GeoIP database not loaded; no flags"),
            },
            /* Not an error, and no longer "no flags": the built-in table is
             * compiled in and covers every deployment with nothing to
             * install. GEOIP_DB_PATH is an accuracy upgrade, not a
             * prerequisite. */
            None => {
                let (v4, v6) = crate::ip_country::range_counts();
                info!(
                    ipv4_ranges = v4,
                    ipv6_ranges = v6,
                    "GEOIP_DB_PATH not set; using the built-in RIR country table"
                );
            }
        }
        hub
    }

    /// Country (alpha-2, upper case) for a peer IP, or "" when the lookup
    /// cannot say: a private / loopback address, an unparsable one, or an
    /// address no table covers.
    ///
    /// The built-in RIR table always answers; GEOIP_DB_PATH adds MaxMind on
    /// top and takes precedence where it has a record.
    pub fn country_for(&self, ip: &str) -> String {
        let Ok(addr) = ip.trim().parse::<std::net::IpAddr>() else {
            debug!(ip, "geoip: unparsable peer address; no flag");
            return String::new();
        };
        /* Answered FIRST, before either table, so both paths obey it. A
         * private address has no country in any database, and this is the
         * usual reason flags are missing -- in testing, and in production
         * behind a reverse proxy, where every peer arrives as the proxy's
         * loopback address. */
        let private = match addr {
            std::net::IpAddr::V4(v4) => {
                v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()
            }
            std::net::IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || (v6.segments()[0] & 0xfe00) == 0xfc00
                    || (v6.segments()[0] & 0xffc0) == 0xfe80
            }
        };
        if private {
            debug!(ip, "geoip: private/loopback peer address; no flag");
            return String::new();
        }
        let Some(reader) = self.geoip.as_ref() else {
            /* No MaxMind configured: the built-in table answers, so a
             * deployment that installs nothing still shows flags. */
            return builtin_country(addr);
        };
        /* MaxMind is configured, so it wins -- it is the more accurate answer
         * and the operator asked for it. The built-in table still backs it up
         * where the database has no record, so adding MaxMind can only ever
         * improve coverage, never reduce it. */
        let from_db = match reader.lookup(addr) {
            Ok(found) => match found.decode::<maxminddb::geoip2::Country>() {
                Ok(Some(c)) => c
                    .country
                    .iso_code
                    .map(|s| s.to_ascii_uppercase())
                    .unwrap_or_default(),
                _ => String::new(),
            },
            Err(_) => String::new(),
        };
        if !from_db.is_empty() {
            return from_db;
        }
        debug!(ip, "geoip: no MaxMind record; falling back to the built-in table");
        builtin_country(addr)
    }

    pub async fn client_count(&self) -> usize {
        self.inner.lock().await.clients.len()
    }

    pub async fn lobby_count(&self) -> usize {
        self.inner.lock().await.lobbies.len()
    }

    /// Lobbies that have completed at least one `start` (in-match / rematch).
    pub async fn match_count(&self) -> usize {
        self.inner
            .lock()
            .await
            .lobbies
            .values()
            .filter(|l| l.started)
            .count()
    }

    /// Live lobby counts keyed by `game_name` (for `/stats` only).
    pub async fn counts_by_game(&self) -> BTreeMap<String, usize> {
        let g = self.inner.lock().await;
        let mut out = BTreeMap::new();
        for lobby in g.lobbies.values() {
            *out.entry(lobby.game_name.clone()).or_insert(0) += 1;
        }
        out
    }

    /// Live in-match lobby counts keyed by `game_name` (for `/stats` only).
    pub async fn match_counts_by_game(&self) -> BTreeMap<String, usize> {
        let g = self.inner.lock().await;
        let mut out = BTreeMap::new();
        for lobby in g.lobbies.values().filter(|l| l.started) {
            *out.entry(lobby.game_name.clone()).or_insert(0) += 1;
        }
        out
    }
}

pub fn ws_router() -> Router<AppState> {
    Router::new()
        .route("/", get(ws_upgrade))
        .route("/ws", get(ws_upgrade))
}

/// The client address to attribute this connection to.
///
/// The TCP peer, unless the server is explicitly configured to sit behind a
/// reverse proxy -- in which case the first entry of `X-Forwarded-For` is the
/// original client and the TCP peer is the proxy. Gated on config and never
/// on the header's presence: the header is trivially spoofable by anyone
/// reaching the server directly, so honouring it unasked would let a client
/// pick its own flag.
fn client_ip_for(headers: &HeaderMap, addr: &SocketAddr, trust_proxy: bool) -> String {
    if trust_proxy {
        if let Some(fwd) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            /* Left-most entry is the original client; proxies append. */
            let first = fwd.split(',').next().unwrap_or("").trim();
            if !first.is_empty() {
                return first.to_string();
            }
        }
    }
    addr.ip().to_string()
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let peer_ip = client_ip_for(&headers, &addr, state.config.trust_proxy_header);
    ws.on_upgrade(move |socket| handle_socket(socket, peer_ip, state))
}

#[derive(Debug, Deserialize)]
struct InMsg {
    op: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    game_name: Option<String>,
    /// Release version / build pin for this game (create / join / list filter).
    #[serde(default)]
    game_version: Option<String>,
    /// Disc TOC fingerprint (create / join). Lowercase hex SHA-256, 64 chars.
    #[serde(default)]
    disc_fp: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    max_slots: Option<u32>,
    /// create: open a spectator gallery (default off).
    allow_spectators: Option<bool>,
    #[serde(default)]
    host_bind: Option<String>,
    /// Host STUN advertise update (`set_host_endpoint`).
    #[serde(default)]
    host_endpoint: Option<String>,
    /// Optional LAN UDP endpoints alongside `set_host_endpoint`.
    #[serde(default)]
    lan_endpoints: Option<Vec<String>>,
    #[serde(default)]
    guest_bind: Option<String>,
    #[serde(default)]
    lobby_id: Option<String>,
    #[serde(default)]
    to_player_id: Option<String>,
    #[serde(default)]
    r#type: Option<i32>,
    #[serde(default)]
    flag: Option<i32>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    ready: Option<bool>,
    /// Peer BIOS capability advertise (attached to set_ready).
    #[serde(default)]
    bios_offer: Option<Value>,
    /// Peer installed-package catalog (attached to set_ready).
    #[serde(default)]
    mod_offer: Option<Value>,
    /// Peer memory-card offer (attached to set_ready).
    #[serde(default)]
    memcard_offer: Option<Value>,
    /// Host sim settings blob (aspect, turbo_loads, bios_hle, input_delay, …).
    #[serde(default)]
    match_caps: Option<Value>,
    #[serde(default)]
    slot: Option<usize>,
    /// Host slot move: source index (paired with `to_slot`).
    #[serde(default)]
    from_slot: Option<usize>,
    /// Seat self-service: target seat of a `seat_move` / `seat_swap_request`.
    #[serde(default)]
    target_slot: Option<usize>,
    /// Seat swap verdict (`seat_swap_answer`).
    #[serde(default)]
    accept: Option<bool>,
    /// Seat swap: the player who asked (echoed back on the answer).
    #[serde(default)]
    asker_player_id: Option<String>,
    /// Host slot move: destination index (paired with `from_slot` or `slot`).
    #[serde(default)]
    to_slot: Option<usize>,
    /// Waiting-room ICE path: `direct` | `relay` | `fail` (`path_report`).
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

fn sanitize_match_caps(caps: Option<Value>) -> Option<Value> {
    let Some(v) = caps else {
        return None;
    };
    if !v.is_object() {
        return None;
    }
    let s = v.to_string();
    if s.len() > 4096 {
        return None;
    }
    Some(v)
}

fn sanitize_bios_offer(offer: Option<Value>) -> Option<Value> {
    let Some(v) = offer else {
        return None;
    };
    if !v.is_object() {
        return None;
    }
    let s = v.to_string();
    if s.len() > 512 {
        return None;
    }
    Some(v)
}

fn sanitize_memcard_offer(offer: Option<Value>) -> Option<Value> {
    let Some(v) = offer else {
        return None;
    };
    if !v.is_object() {
        return None;
    }
    let s = v.to_string();
    if s.len() > 256 {
        return None;
    }
    Some(v)
}

fn sanitize_mod_offer(offer: Option<Value>) -> Option<Value> {
    let Some(v) = offer else {
        return None;
    };
    if !v.is_object() {
        return None;
    }
    let s = v.to_string();
    if s.len() > 2048 {
        return None;
    }
    Some(v)
}

impl Lobby {
    /// The seat array a seat index addresses, and the index within it.
    fn seat_parts(&self, seat: usize) -> Option<(&Vec<Option<Slot>>, usize)> {
        if is_spectator_seat(seat) {
            let i = seat - SPECTATOR_SLOT_BASE;
            (i < self.spectators.len()).then_some((&self.spectators, i))
        } else {
            (seat < self.slots.len()).then_some((&self.slots, seat))
        }
    }

    fn seat(&self, seat: usize) -> Option<&Option<Slot>> {
        self.seat_parts(seat).map(|(v, i)| &v[i])
    }

    fn seat_mut(&mut self, seat: usize) -> Option<&mut Option<Slot>> {
        if is_spectator_seat(seat) {
            self.spectators.get_mut(seat - SPECTATOR_SLOT_BASE)
        } else {
            self.slots.get_mut(seat)
        }
    }

    /// Every seated participant, players first. Use this wherever the question
    /// is "who is in this room" -- membership, name collisions, broadcasts --
    /// and `slots` only where the question is "who is playing".
    fn everyone(&self) -> impl Iterator<Item = &Slot> {
        self.slots.iter().flatten().chain(self.spectators.iter().flatten())
    }

    fn everyone_mut(&mut self) -> impl Iterator<Item = &mut Slot> {
        self.slots
            .iter_mut()
            .flatten()
            .chain(self.spectators.iter_mut().flatten())
    }

    fn member_ids(&self) -> Vec<String> {
        self.everyone().map(|s| s.player_id.clone()).collect()
    }

    /// Which seat a player occupies, in the shared index namespace.
    fn seat_of(&self, player_id: &str) -> Option<usize> {
        if let Some(i) = self
            .slots
            .iter()
            .position(|s| s.as_ref().is_some_and(|s| s.player_id == player_id))
        {
            return Some(i);
        }
        self.spectators
            .iter()
            .position(|s| s.as_ref().is_some_and(|s| s.player_id == player_id))
            .map(spectator_seat)
    }

    fn spectator_count(&self) -> usize {
        self.spectators.iter().filter(|s| s.is_some()).count()
    }
}

fn slot_json(i: usize, slot: &Slot) -> Value {
    let mut row = json!({
        "slot": i,
        "player_id": slot.player_id,
        "display_name": slot.display_name,
        "ready": slot.ready,
    });
    if let Some(offer) = &slot.bios_offer {
        row["bios_offer"] = offer.clone();
    }
    if let Some(offer) = &slot.mod_offer {
        row["mod_offer"] = offer.clone();
    }
    if let Some(offer) = &slot.memcard_offer {
        row["memcard_offer"] = offer.clone();
    }
    if !slot.country.is_empty() {
        row["country"] = json!(slot.country);
    }
    row
}

#[derive(Serialize)]
struct LobbyListRow<'a> {
    lobby_id: &'a str,
    name: &'a str,
    game_name: &'a str,
    game_version: &'a str,
    player_count: usize,
    max_slots: usize,
    has_password: bool,
    /// match_caps.lobby_kind echoed for browser badges (0 standard,
    /// 1 PSX-Link). Opaque otherwise; absent caps read as 0.
    lobby_kind: i64,
    /// Host UDP game endpoint (rewritten for peers). Clients probe RTT here.
    host_endpoint: &'a str,
    /// Same-LAN probe candidates (RFC1918 host:port).
    lan_endpoints: &'a [String],
    /// Host's country (alpha-2) from GeoIP; "" when unknown.
    host_country: String,
    /// Gallery: whether the host opened one, its size, and how many watch.
    /// A browser shows "No" or "1/4" from these.
    allow_spectators: bool,
    max_spectators: usize,
    spectator_count: usize,
}

/// One connected client, for the browser's "players online" panel.
#[derive(Serialize)]
struct OnlinePlayerRow {
    display_name: String,
    /// Country (alpha-2) from GeoIP; "" when unknown.
    country: String,
    /// The room this player is in ("" when browsing), and its name.
    lobby_id: String,
    lobby_name: String,
    hosting: bool,
}

/// The built-in RIR table's answer, or "" when it has none. Private addresses
/// are filtered by the caller before this is reached.
fn builtin_country(addr: std::net::IpAddr) -> String {
    match crate::ip_country::lookup(addr) {
        Some(cc) => cc,
        None => {
            debug!(%addr, "geoip: built-in table has no record for this address");
            String::new()
        }
    }
}

/// Normalize empty / missing version to `"dev"` (local builds).
/// Normalize disc TOC fingerprint: lowercase hex, exactly 64 chars, or empty.
fn normalize_disc_fp(v: Option<String>) -> String {
    let Some(raw) = v else {
        return String::new();
    };
    let s = raw.trim().to_ascii_lowercase();
    if s.is_empty() {
        return String::new();
    }
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return String::new();
    }
    s
}

/// Peer mount match: when either side advertises a fingerprint, both must
/// match. Two empty values (legacy clients) skip the check.
fn disc_fp_mismatch(lobby_fp: &str, join_fp: &str) -> bool {
    if lobby_fp.is_empty() && join_fp.is_empty() {
        return false;
    }
    lobby_fp != join_fp
}

fn normalize_game_version(v: Option<String>) -> String {
    v.filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "dev".into())
}

fn lobby_list_json_filtered(
    hub: &HubInner,
    filter_game: Option<&str>,
    filter_version: Option<&str>,
) -> String {
    let rows: Vec<LobbyListRow> = hub
        .lobbies
        .values()
        .filter(|l| {
            if let Some(g) = filter_game {
                if !g.is_empty() && l.game_name != g {
                    return false;
                }
            }
            if let Some(v) = filter_version {
                if !v.is_empty() && l.game_version != v {
                    return false;
                }
            }
            true
        })
        .map(|l| LobbyListRow {
            lobby_id: &l.lobby_id,
            name: &l.name,
            game_name: &l.game_name,
            game_version: &l.game_version,
            player_count: player_count(l),
            max_slots: l.max_slots,
            has_password: l.password_hash.is_some(),
            lobby_kind: l
                .match_caps
                .as_ref()
                .and_then(|c| c.get("lobby_kind"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            host_endpoint: &l.host_endpoint,
            lan_endpoints: &l.lan_endpoints,
            host_country: l
                .everyone()
                .find(|s| s.player_id == l.host_player_id)
                .map(|s| s.country.clone())
                .unwrap_or_default(),
            allow_spectators: l.allow_spectators,
            max_spectators: l.spectators.len(),
            spectator_count: l.spectator_count(),
        })
        .collect();
    /* Everyone connected, browsing or seated, so a browser can show who is
     * around to play -- an empty lobby list with three people online reads
     * very differently from an empty list with nobody. Same title filter as
     * the rows: a client of another game is not "here" for this one.
     * Additive: an older client ignores the key. */
    let mut players: Vec<OnlinePlayerRow> = hub
        .clients
        .values()
        .map(|c| {
            let lobby = c.lobby_id.as_ref().and_then(|id| hub.lobbies.get(id));
            OnlinePlayerRow {
                display_name: c.display_name.clone(),
                country: c.country.clone(),
                lobby_id: lobby.map(|l| l.lobby_id.clone()).unwrap_or_default(),
                lobby_name: lobby.map(|l| l.name.clone()).unwrap_or_default(),
                hosting: lobby.is_some_and(|l| l.host_player_id == c.player_id),
            }
        })
        .collect();
    players.sort_by(|a, b| a.display_name.to_lowercase().cmp(&b.display_name.to_lowercase()));
    json!({ "op": "lobby_list", "lobbies": rows, "players": players }).to_string()
}

fn hash_password(password: &str, salt: &[u8; 16]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    let dig = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&dig);
    out
}

fn rewrite_endpoint(bind: &str, peer_ip: &str) -> String {
    let (host, port) = match bind.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => return bind.to_string(),
    };
    let use_host = if host.is_empty() || host == "0.0.0.0" || host == "*" || host == "::" {
        if peer_ip.is_empty() {
            "127.0.0.1"
        } else {
            peer_ip
        }
    } else {
        host
    };
    format!("{use_host}:{port}")
}

/// Validate a host-advertised UDP endpoint for list / waiting-room RTT.
fn parse_advertise_endpoint(raw: &str) -> Option<String> {
    let ep = raw.trim();
    if ep.is_empty() || ep.len() > 64 {
        return None;
    }
    let (host, port_s) = ep.rsplit_once(':')?;
    if host.is_empty()
        || host == "0.0.0.0"
        || host == "*"
        || host == "::"
        || host.contains([' ', '"', '\'', '{', '}', '\\'])
    {
        return None;
    }
    let port: u16 = port_s.parse().ok()?;
    if port == 0 {
        return None;
    }
    Some(format!("{host}:{port}"))
}

fn is_rfc1918_host(host: &str) -> bool {
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    let Ok(a) = parts[0].parse::<u8>() else {
        return false;
    };
    let Ok(b) = parts[1].parse::<u8>() else {
        return false;
    };
    if parts[2].parse::<u8>().is_err() || parts[3].parse::<u8>().is_err() {
        return false;
    }
    a == 10 || (a == 172 && (16..=31).contains(&b)) || (a == 192 && b == 168)
}

fn normalize_ws_peer_v4(ip: &str) -> &str {
    let h = ip.trim();
    h.strip_prefix("::ffff:").unwrap_or(h)
}

/// True when the WebSocket TCP peer is on-LAN (or loopback to this host).
fn is_local_ws_peer_ip(ip: &str) -> bool {
    let h = ip.trim();
    if h.is_empty() {
        return false;
    }
    if h == "::1" || h.eq_ignore_ascii_case("localhost") || h.starts_with("127.") {
        return true;
    }
    is_rfc1918_host(normalize_ws_peer_v4(h))
}

/// Direct LAN / loopback peer — excludes the LAN gateway (NAT hairpin source).
fn is_direct_lan_ws_peer(ip: &str, gateway: Option<&str>) -> bool {
    if !is_local_ws_peer_ip(ip) {
        return false;
    }
    let Some(gw) = gateway.map(str::trim).filter(|g| !g.is_empty()) else {
        return true;
    };
    normalize_ws_peer_v4(ip) != gw
}

/// Cap and validate LAN advertise list (RFC1918 only, no loopback).
fn sanitize_lan_endpoints(raw: Option<Vec<String>>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(list) = raw else {
        return out;
    };
    for item in list {
        let Some(ep) = parse_advertise_endpoint(&item) else {
            continue;
        };
        let host = ep.rsplit_once(':').map(|(h, _)| h).unwrap_or("");
        if host.starts_with("127.") || host == "localhost" || !is_rfc1918_host(host) {
            continue;
        }
        if out.iter().any(|e| e == &ep) {
            continue;
        }
        out.push(ep);
        if out.len() >= 4 {
            break;
        }
    }
    out
}

fn clear_lobby_ice_paths(lobby: &mut Lobby) {
    for s in lobby.everyone_mut() {
        s.ice_path = None;
        s.ice_path_at = None;
    }
}

fn normalize_ice_path(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "direct" | "host" | "srflx" | "prflx" => Some("direct"),
        "relay" => Some("relay"),
        "fail" | "failed" | "none" => Some("fail"),
        _ => None,
    }
}

/// Online MotK/BPE lobbies always use the lobby UDP SFU (§108).
/// Waiting-room `path_report` / ICE P2P selection was removed — it disagreed
/// with no-ICE builds and Force TURN semantics. Caps bits are ignored here;
/// `force_turn` remains a client delay-floor hint only.
fn start_use_sfu(_lobby: &Lobby, _caps: &Option<Value>) -> (bool, &'static str) {
    (true, "always_sfu")
}

fn player_count(lobby: &Lobby) -> usize {
    lobby.slots.iter().filter(|s| s.is_some()).count()
}

/// Make `requested` unique among occupied lobby seats (`Alex`, `Alex (2)`, …).
fn unique_display_name(lobby: &Lobby, requested: &str, skip_player_id: Option<&str>) -> String {
    let base = {
        let t = requested.trim();
        if t.is_empty() {
            "Guest"
        } else {
            t
        }
    };
    let taken = |name: &str| {
        /* Spectators included: two "Alex" rows are just as confusing across
         * two tables as within one, and a promotion moves a name between
         * them without renaming it. */
        lobby.everyone().any(|s| {
            if let Some(skip) = skip_player_id {
                if s.player_id == skip {
                    return false;
                }
            }
            s.display_name == name
        })
    };
    if !taken(base) {
        return base.to_string();
    }
    for n in 2..=64 {
        let candidate = format!("{base} ({n})");
        if !taken(&candidate) {
            return candidate;
        }
    }
    format!("{base} ({})", &Uuid::new_v4().to_string()[..8])
}

fn lobby_list_json(hub: &HubInner) -> String {
    lobby_list_json_filtered(hub, None, None)
}

async fn broadcast_list(hub: &WsLobbyHub) {
    let (payload, ids) = {
        let g = hub.inner.lock().await;
        let payload = lobby_list_json(&g);
        let ids: Vec<String> = g.clients.keys().cloned().collect();
        (payload, ids)
    };
    let g = hub.inner.lock().await;
    for id in ids {
        if let Some(c) = g.clients.get(&id) {
            let _ = c.tx.send(payload.clone());
        }
    }
}

async fn send_to(hub: &WsLobbyHub, player_id: &str, msg: String) {
    let g = hub.inner.lock().await;
    if let Some(c) = g.clients.get(player_id) {
        let _ = c.tx.send(msg);
    }
}

async fn emit_lobby_update(hub: &WsLobbyHub, lobby_id: &str) {
    let (msg, members) = {
        let g = hub.inner.lock().await;
        let Some(l) = g.lobbies.get(lobby_id) else {
            return;
        };
        let mut slots = Vec::new();
        for (i, s) in l.slots.iter().enumerate() {
            if let Some(slot) = s {
                slots.push(slot_json(i, slot));
            }
        }
        let mut spectators = Vec::new();
        for (i, s) in l.spectators.iter().enumerate() {
            if let Some(slot) = s {
                spectators.push(slot_json(spectator_seat(i), slot));
            }
        }
        /* Players only. A gallery that never presses Ready must not hold the
         * match, and a gallery that does must not be able to start one. */
        let all_ready = l.slots.iter().flatten().all(|s| s.ready) && player_count(l) >= 2;
        let mut msg = json!({
            "op": "lobby_update",
            "lobby_id": l.lobby_id,
            "session_id": l.session_id,
            "host_endpoint": l.host_endpoint,
            "lan_endpoints": l.lan_endpoints,
            "guest_endpoint": l.guest_endpoint,
            "player_count": player_count(l),
            "max_slots": l.max_slots,
            "host_player_id": l.host_player_id,
            "all_ready": all_ready,
            "slots": slots,
            /* Additive: a client that does not read these sees exactly the
             * lobby it saw before spectators existed. */
            "allow_spectators": l.allow_spectators,
            "max_spectators": l.spectators.len(),
            "spectator_count": l.spectator_count(),
            "spectator_slot_base": SPECTATOR_SLOT_BASE,
            "spectators": spectators,
        });
        if let Some(caps) = &l.match_caps {
            msg["match_caps"] = caps.clone();
        }
        let msg = msg.to_string();
        let members = l.member_ids();
        (msg, members)
    };
    for m in members {
        send_to(hub, &m, msg.clone()).await;
    }
}

async fn destroy_lobby(state: &AppState, lobby_id: &str) {
    let hub = &state.ws_lobby;
    let (members, relay_sid) = {
        let mut g = hub.inner.lock().await;
        let Some(l) = g.lobbies.remove(lobby_id) else {
            return;
        };
        metrics::ws_lobby_destroyed();
        let members = l.member_ids();
        for m in &members {
            if let Some(c) = g.clients.get_mut(m) {
                c.lobby_id = None;
            }
        }
        (members, l.relay_session_id)
    };
    if let Some(sid) = relay_sid {
        state.input_relay.close_session(sid).await;
    }
    let note = json!({ "op": "lobby_closed", "lobby_id": lobby_id, "ok": true }).to_string();
    for m in members {
        send_to(hub, &m, note.clone()).await;
    }
    broadcast_list(hub).await;
}

async fn client_leave(state: &AppState, player_id: &str) {
    let hub = &state.ws_lobby;
    let action = {
        let mut g = hub.inner.lock().await;
        let Some(c) = g.clients.get_mut(player_id) else {
            return;
        };
        let Some(lid) = c.lobby_id.take() else {
            return;
        };
        let Some(lobby) = g.lobbies.get(&lid) else {
            return;
        };
        if lobby.host_player_id == player_id {
            Some((lid, true, String::new()))
        } else {
            let name = lobby
                .everyone()
                .find(|s| s.player_id == player_id)
                .map(|s| s.display_name.clone())
                .unwrap_or_default();
            if let Some(lobby) = g.lobbies.get_mut(&lid) {
                for s in lobby.slots.iter_mut().chain(lobby.spectators.iter_mut()) {
                    if s.as_ref().map(|x| x.player_id.as_str()) == Some(player_id) {
                        *s = None;
                    }
                }
                lobby.guest_endpoint.clear();
                clear_lobby_ice_paths(lobby);
            }
            Some((lid, false, name))
        }
    };
    if let Some((lid, is_host, name)) = action {
        if is_host {
            destroy_lobby(state, &lid).await;
        } else {
            emit_lobby_update(hub, &lid).await;
            broadcast_list(hub).await;
            if !name.is_empty() {
                chat_system(hub, &lid, &format!("{name} has left.")).await;
            }
        }
    }
}

async fn handle_socket(socket: WebSocket, peer_ip: String, state: AppState) {
    let hub = state.ws_lobby.clone();
    let player_id = Uuid::new_v4().to_string();
    let (tx, mut rx) = broadcast::channel::<String>(64);
    {
        let mut g = hub.inner.lock().await;
        g.clients.insert(
            player_id.clone(),
            ClientMeta {
                player_id: player_id.clone(),
                display_name: format!("Player-{}", &player_id[..8.min(player_id.len())]),
                country: hub.country_for(&peer_ip),
                peer_ip: peer_ip.clone(),
                lobby_id: None,
                pending_mod_lobby: None,
                tx: tx.clone(),
            },
        );
    }

    let (mut sink, mut stream) = socket.split();
    let welcome = json!({ "op": "welcome", "ok": true, "player_id": player_id }).to_string();
    if sink.send(Message::Text(welcome.into())).await.is_err() {
        let mut g = hub.inner.lock().await;
        g.clients.remove(&player_id);
        return;
    }
    info!(%player_id, %peer_ip, "ws lobby client connected");
    metrics::ws_connected();

    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sink.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    // Periodic list push (~1 Hz) while connected.
    let hub_tick = hub.clone();
    let player_tick = player_id.clone();
    let tick_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let payload = {
                let g = hub_tick.inner.lock().await;
                if !g.clients.contains_key(&player_tick) {
                    break;
                }
                lobby_list_json(&g)
            };
            send_to(&hub_tick, &player_tick, payload).await;
        }
    });

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => {
                if let Err(e) = handle_text(&state, &player_id, &peer_ip, &text).await {
                    warn!(%player_id, error = %e, "ws lobby handler error");
                }
            }
            Message::Ping(data) => {
                let _ = tx.send(String::new()); // noop keep channel alive
                debug!(%player_id, ping_len = data.len(), "ws ping");
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    send_task.abort();
    tick_task.abort();
    client_leave(&state, &player_id).await;
    {
        let mut g = hub.inner.lock().await;
        g.clients.remove(&player_id);
    }
    info!(%player_id, "ws lobby client disconnected");
    metrics::ws_disconnected();
}

async fn handle_text(
    state: &AppState,
    player_id: &str,
    peer_ip: &str,
    text: &str,
) -> Result<(), String> {
    let hub = &state.ws_lobby;
    let msg: InMsg = serde_json::from_str(text).map_err(|e| e.to_string())?;
    match msg.op.as_str() {
        "hello" => {
            if let Some(name) = msg.display_name.filter(|s| !s.is_empty()) {
                let mut g = hub.inner.lock().await;
                if let Some(c) = g.clients.get_mut(player_id) {
                    c.display_name = name;
                }
            }
            send_to(
                hub,
                player_id,
                json!({ "op": "hello_ok", "ok": true }).to_string(),
            )
            .await;
        }
        "list" => {
            let filter_game = msg.game_name.as_deref().filter(|s| !s.is_empty());
            let filter_ver = msg
                .game_version
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let payload = {
                let g = hub.inner.lock().await;
                lobby_list_json_filtered(&g, filter_game, filter_ver)
            };
            send_to(hub, player_id, payload).await;
        }
        "ping" => {
            send_to(hub, player_id, json!({ "op": "pong" }).to_string()).await;
        }
        "create" => handle_create(hub, player_id, peer_ip, msg).await?,
        "join" => handle_join(hub, player_id, peer_ip, msg).await?,
        "set_ready" => handle_set_ready(hub, player_id, msg).await?,
        "chat" => handle_chat(hub, player_id, msg).await?,
        "set_match_caps" => handle_set_match_caps(hub, player_id, msg).await?,
        "set_host_endpoint" => handle_set_host_endpoint(hub, player_id, msg).await?,
        "path_report" => handle_path_report(hub, player_id, msg).await?,
        "start" => handle_start(state, player_id, msg).await?,
        "leave" => {
            client_leave(state, player_id).await;
            send_to(
                hub,
                player_id,
                json!({ "op": "left", "ok": true }).to_string(),
            )
            .await;
        }
        "kick" => handle_kick(hub, player_id, msg).await?,
        "move" => handle_move(hub, player_id, msg).await?,
        "seat_move" => handle_seat_move(hub, player_id, msg).await?,
        "seat_swap_request" => handle_seat_swap_request(hub, player_id, msg).await?,
        "seat_swap_answer" => handle_seat_swap_answer(hub, player_id, msg).await?,
        "close" => {
            let lid = {
                let g = hub.inner.lock().await;
                g.clients
                    .get(player_id)
                    .and_then(|c| c.lobby_id.clone())
                    .filter(|lid| {
                        g.lobbies
                            .get(lid)
                            .map(|l| l.host_player_id == player_id)
                            .unwrap_or(false)
                    })
            };
            if let Some(lid) = lid {
                destroy_lobby(state, &lid).await;
            }
        }
        "signal" => handle_signal(hub, player_id, msg).await?,
        "mod_signal" => handle_mod_signal(hub, player_id, msg).await?,
        "mod_xfer_start" => handle_mod_xfer_start(hub, player_id, msg).await?,
        "mod_xfer_cancel" => handle_mod_xfer_cancel(hub, player_id).await?,
        "mod_xfer_fail" => handle_mod_xfer_fail(hub, player_id, msg).await?,
        "get_turn_credentials" => handle_get_turn_credentials(hub, player_id).await?,
        other => {
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "unknown_op", "detail": other, "ok": false })
                    .to_string(),
            )
            .await;
        }
    }
    Ok(())
}

async fn handle_mod_xfer_start(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let lobby_id = msg.lobby_id.unwrap_or_default();
    if lobby_id.is_empty() {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "gone", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    }
    let (host_id, mods) = {
        let g = hub.inner.lock().await;
        let pending = g
            .clients
            .get(player_id)
            .and_then(|c| c.pending_mod_lobby.clone());
        if pending.as_deref() != Some(lobby_id.as_str()) {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "need_mods", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        let Some(lobby) = g.lobbies.get(&lobby_id) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "gone", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        (
            lobby.host_player_id.clone(),
            required_mod_rows(&lobby.match_caps),
        )
    };
    send_to(
        hub,
        &host_id,
        json!({
            "op": "mod_xfer_pull",
            "from_player_id": player_id,
            "lobby_id": lobby_id,
            "mods": mods,
        })
        .to_string(),
    )
    .await;
    Ok(())
}

async fn handle_mod_xfer_cancel(hub: &WsLobbyHub, player_id: &str) -> Result<(), String> {
    let mut g = hub.inner.lock().await;
    if let Some(c) = g.clients.get_mut(player_id) {
        c.pending_mod_lobby = None;
    }
    Ok(())
}

fn xfer_relay_ok(
    g: &HubInner,
    from_id: &str,
    to_id: &str,
) -> Result<String, &'static str> {
    let to = g.clients.get(to_id).ok_or("gone")?;
    let lid = to.pending_mod_lobby.as_ref().ok_or("need_mods")?;
    let lobby = g.lobbies.get(lid).ok_or("gone")?;
    if lobby.host_player_id != from_id {
        return Err("not_host");
    }
    Ok(to_id.to_string())
}

async fn handle_mod_signal(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let to = msg.to_player_id.unwrap_or_default();
    let (fwd, target) = {
        let g = hub.inner.lock().await;
        let sender = g.clients.get(player_id).ok_or_else(|| "gone".to_string())?;
        let lid = msg
            .lobby_id
            .clone()
            .or_else(|| sender.pending_mod_lobby.clone())
            .or_else(|| sender.lobby_id.clone())
            .ok_or_else(|| "no lobby".to_string())?;
        let Some(lobby) = g.lobbies.get(&lid) else {
            return Ok(());
        };
        let pending = sender.pending_mod_lobby.as_deref() == Some(lid.as_str());
        let is_host = lobby.host_player_id == player_id;
        if !pending && !is_host {
            return Ok(());
        }
        let dest = if pending {
            lobby.host_player_id.clone()
        } else {
            if to.is_empty() {
                return Ok(());
            }
            let Some(peer) = g.clients.get(&to) else {
                return Ok(());
            };
            if peer.pending_mod_lobby.as_deref() != Some(lid.as_str()) {
                return Ok(());
            }
            to.clone()
        };
        let fwd = json!({
            "op": "mod_signal",
            "lobby_id": lid,
            "from_player_id": player_id,
            "type": msg.r#type.unwrap_or(0),
            "flag": msg.flag.unwrap_or(0),
            "text": msg.text.unwrap_or_default(),
        })
        .to_string();
        (fwd, dest)
    };
    send_to(hub, &target, fwd).await;
    Ok(())
}

async fn handle_mod_xfer_fail(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let to = msg.to_player_id.unwrap_or_default();
    let err = msg.error.unwrap_or_else(|| "export failed".into());
    let dest = {
        let g = hub.inner.lock().await;
        if let Ok(d) = xfer_relay_ok(&g, player_id, &to) {
            Some(d)
        } else if let Some(c) = g.clients.get(player_id) {
            if let Some(lid) = c.pending_mod_lobby.as_ref() {
                g.lobbies.get(lid).map(|l| l.host_player_id.clone())
            } else {
                None
            }
        } else {
            None
        }
    };
    if let Some(dest) = dest {
        send_to(
            hub,
            &dest,
            json!({ "op": "mod_xfer_fail", "error": err }).to_string(),
        )
        .await;
    }
    Ok(())
}

async fn handle_get_turn_credentials(hub: &WsLobbyHub, player_id: &str) -> Result<(), String> {
    use crate::turn_credentials;

    let Ok(uuid) = Uuid::parse_str(player_id) else {
        send_to(
            hub,
            player_id,
            json!({
                "op": "turn_credentials",
                "ok": false,
                "error": "bad_player_id"
            })
            .to_string(),
        )
        .await;
        return Ok(());
    };

    let cfg = match turn_credentials::TurnCredentialConfig::from_env() {
        Some(c) => c,
        None => {
            send_to(
                hub,
                player_id,
                json!({
                    "op": "turn_credentials",
                    "ok": false,
                    "error": "coturn_unconfigured"
                })
                .to_string(),
            )
            .await;
            return Ok(());
        }
    };

    match turn_credentials::issue_credentials(&cfg, &uuid) {
        Ok((username, password)) => {
            metrics::http_turn_credentials_issued();
            send_to(
                hub,
                player_id,
                json!({
                    "op": "turn_credentials",
                    "ok": true,
                    "stun_host": cfg.stun_host,
                    "stun_port": cfg.stun_port,
                    "turn_host": cfg.turn_host,
                    "turn_port": cfg.turn_port,
                    "turns_port": cfg.turns_port,
                    "realm": cfg.realm,
                    "username": username,
                    "password": password,
                    "ttl_secs": cfg.ttl_secs
                })
                .to_string(),
            )
            .await;
        }
        Err(e) => {
            warn!(%player_id, error = %e, "ws turn credential mint failed");
            send_to(
                hub,
                player_id,
                json!({
                    "op": "turn_credentials",
                    "ok": false,
                    "error": "mint_failed"
                })
                .to_string(),
            )
            .await;
        }
    }
    Ok(())
}

async fn handle_create(
    hub: &WsLobbyHub,
    player_id: &str,
    peer_ip: &str,
    msg: InMsg,
) -> Result<(), String> {
    {
        let g = hub.inner.lock().await;
        if g.clients
            .get(player_id)
            .and_then(|c| c.lobby_id.as_ref())
            .is_some()
        {
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "already_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        if g.lobbies.len() >= MAX_LOBBIES {
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "lobby_limit", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
    }

    let name = msg
        .name
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Lobby".into());
    let game_name = msg
        .game_name
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Unknown".into());
    let game_version = normalize_game_version(msg.game_version);
    let disc_fp = normalize_disc_fp(msg.disc_fp);
    let host_bind = msg
        .host_bind
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0.0.0.0:7777".into());
    let max_slots = msg.max_slots.unwrap_or(2).clamp(2, MAX_SLOTS as u32) as usize;
    let allow_spectators = msg.allow_spectators.unwrap_or(false);

    if let Some(dn) = msg.display_name.filter(|s| !s.is_empty()) {
        let mut g = hub.inner.lock().await;
        if let Some(c) = g.clients.get_mut(player_id) {
            c.display_name = dn;
        }
    }

    let (lobby_id, session_id, host_endpoint, display_name, match_caps) = {
        let mut g = hub.inner.lock().await;
        let display_name = g
            .clients
            .get(player_id)
            .map(|c| c.display_name.clone())
            .unwrap_or_else(|| "Host".into());
        let lobby_id = Uuid::new_v4().to_string();
        let session_id = g.next_session;
        g.next_session = g.next_session.saturating_add(1);
        let host_endpoint = rewrite_endpoint(&host_bind, peer_ip);

        let mut password_hash = None;
        let mut password_salt = None;
        if let Some(pw) = msg.password.filter(|s| !s.is_empty()) {
            let mut salt = [0u8; 16];
            rand::thread_rng().fill_bytes(&mut salt);
            password_hash = Some(hash_password(&pw, &salt));
            password_salt = Some(salt);
        }

        let mut slots = vec![None; max_slots];
        slots[0] = Some(Slot {
            player_id: player_id.to_string(),
            display_name: display_name.clone(),
            ready: false,
            bios_offer: None,
            mod_offer: None,
            memcard_offer: None,
            country: String::new(),
            ice_path: None,
            ice_path_at: None,
        });
        if let Some(host_slot) = slots[0].as_mut() {
            host_slot.country = g
                .clients
                .get(player_id)
                .map(|c| c.country.clone())
                .unwrap_or_default();
        }

        let match_caps = sanitize_match_caps(msg.match_caps);
        g.lobbies.insert(
            lobby_id.clone(),
            Lobby {
                lobby_id: lobby_id.clone(),
                name,
                game_name,
                game_version,
                disc_fp,
                host_player_id: player_id.to_string(),
                host_bind,
                host_endpoint: host_endpoint.clone(),
                lan_endpoints: Vec::new(),
                guest_endpoint: String::new(),
                password_hash,
                password_salt,
                max_slots,
                session_id,
                slots,
                allow_spectators,
                spectators: vec![None; if allow_spectators { MAX_SPECTATORS } else { 0 }],
                match_caps: match_caps.clone(),
                relay_session_id: None,
                started: false,
            },
        );
        if let Some(c) = g.clients.get_mut(player_id) {
            c.lobby_id = Some(lobby_id.clone());
        }
        (
            lobby_id,
            session_id,
            host_endpoint,
            display_name,
            match_caps,
        )
    };

    let mut created = json!({
        "op": "created",
        "ok": true,
        "lobby_id": lobby_id,
        "session_id": session_id,
        "local_slot": 0,
        "host_endpoint": host_endpoint,
        "guest_endpoint": "",
        "host_player_id": player_id,
        "player_count": 1,
        "max_slots": max_slots,
        "allow_spectators": allow_spectators,
        "max_spectators": if allow_spectators { MAX_SPECTATORS } else { 0 },
        "spectator_count": 0,
        "spectator_slot_base": SPECTATOR_SLOT_BASE,
        "spectators": [],
        "slots": [{
            "slot": 0,
            "player_id": player_id,
            "display_name": display_name,
            "ready": false,
        }],
    });
    if let Some(caps) = match_caps {
        created["match_caps"] = caps;
    }
    send_to(hub, player_id, created.to_string()).await;
    broadcast_list(hub).await;
    metrics::ws_lobby_created();
    Ok(())
}

async fn handle_join(
    hub: &WsLobbyHub,
    player_id: &str,
    peer_ip: &str,
    msg: InMsg,
) -> Result<(), String> {
    let lobby_id = msg
        .lobby_id
        .filter(|s| !s.is_empty())
        .ok_or("missing lobby_id")?;
    {
        let g = hub.inner.lock().await;
        if g.clients
            .get(player_id)
            .and_then(|c| c.lobby_id.as_ref())
            .is_some()
        {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "already_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
    }

    if let Some(dn) = msg.display_name.filter(|s| !s.is_empty()) {
        let mut g = hub.inner.lock().await;
        if let Some(c) = g.clients.get_mut(player_id) {
            c.display_name = dn;
        }
    }

    let guest_bind = msg
        .guest_bind
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0.0.0.0:7778".into());
    let password = msg.password.clone();
    let join_game_name = msg.game_name.clone().filter(|s| !s.is_empty());
    let join_game_version = normalize_game_version(msg.game_version);
    let join_disc_fp = normalize_disc_fp(msg.disc_fp);
    let join_mod_offer = msg.mod_offer.clone();

    let outcome = {
        let mut g = hub.inner.lock().await;
        if !g.lobbies.contains_key(&lobby_id) {
            SeatResult::Err("gone")
        } else {
            let (game_name, game_version, lobby_disc_fp) = {
                let lobby = g.lobbies.get(&lobby_id).unwrap();
                (
                    lobby.game_name.clone(),
                    lobby.game_version.clone(),
                    lobby.disc_fp.clone(),
                )
            };
            if disc_fp_mismatch(&lobby_disc_fp, &join_disc_fp) {
                SeatResult::Err("disc_mismatch")
            } else if let Some(ref want) = join_game_name {
                if &game_name != want {
                    SeatResult::Err("game_mismatch")
                } else if game_version != join_game_version {
                    SeatResult::Err("version_mismatch")
                } else {
                    seat_joiner_locked(
                        &mut g,
                        &lobby_id,
                        player_id,
                        peer_ip,
                        &guest_bind,
                        password.as_deref(),
                        join_mod_offer.clone(),
                    )
                }
            } else if game_version != join_game_version {
                SeatResult::Err("version_mismatch")
            } else {
                seat_joiner_locked(
                    &mut g,
                    &lobby_id,
                    player_id,
                    peer_ip,
                    &guest_bind,
                    password.as_deref(),
                    join_mod_offer.clone(),
                )
            }
        }
    };

    match outcome {
        SeatResult::Err(code) => {
            metrics::ws_lobby_join_fail(code);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": code, "ok": false }).to_string(),
            )
            .await;
            Ok(())
        }
        SeatResult::NeedMods {
            mods,
            can_transfer,
        } => {
            let host_id = {
                let mut g = hub.inner.lock().await;
                if let Some(c) = g.clients.get_mut(player_id) {
                    c.pending_mod_lobby = Some(lobby_id.clone());
                }
                g.lobbies
                    .get(&lobby_id)
                    .map(|l| l.host_player_id.clone())
                    .unwrap_or_default()
            };
            send_to(
                hub,
                player_id,
                json!({
                    "op": "need_mods",
                    "ok": false,
                    "code": "need_mods",
                    "lobby_id": lobby_id,
                    "host_player_id": host_id,
                    "mods": mods,
                    "can_transfer": can_transfer,
                })
                .to_string(),
            )
            .await;
            Ok(())
        }
        SeatResult::Ok {
            slot,
            session_id,
            host_endpoint,
            guest_endpoint,
            match_caps,
        } => {
            {
                let mut g = hub.inner.lock().await;
                if let Some(c) = g.clients.get_mut(player_id) {
                    c.pending_mod_lobby = None;
                }
            }
            let mut joined = json!({
                "op": "joined",
                "ok": true,
                "lobby_id": lobby_id,
                "session_id": session_id,
                "local_slot": slot,
                /* Said outright as well as implied by local_slot: a joiner
                 * that overflowed into the gallery has to know it before it
                 * shows the player anything, and reading a role out of an
                 * index is exactly the kind of inference a client gets wrong
                 * once and then ships. */
                "spectator": is_spectator_seat(slot),
                "spectator_slot_base": SPECTATOR_SLOT_BASE,
                "host_endpoint": host_endpoint,
                "guest_endpoint": guest_endpoint,
            });
            if let Some(caps) = match_caps {
                joined["match_caps"] = caps;
            }
            send_to(hub, player_id, joined.to_string()).await;
            emit_lobby_update(hub, &lobby_id).await;
            broadcast_list(hub).await;
            metrics::ws_lobby_join_ok();
            {
                let name = {
                    let g = hub.inner.lock().await;
                    g.clients
                        .get(player_id)
                        .map(|c| c.display_name.clone())
                        .unwrap_or_default()
                };
                let line = if is_spectator_seat(slot) {
                    format!("{name} has joined as a spectator.")
                } else {
                    format!("{name} has joined.")
                };
                chat_system(hub, &lobby_id, &line).await;
            }
            Ok(())
        }
    }
}

enum SeatResult {
    Err(&'static str),
    NeedMods {
        mods: Vec<Value>,
        can_transfer: bool,
    },
    Ok {
        slot: usize,
        session_id: u32,
        host_endpoint: String,
        guest_endpoint: String,
        match_caps: Option<Value>,
    },
}

fn required_mod_rows(caps: &Option<Value>) -> Vec<Value> {
    let Some(c) = caps else {
        return Vec::new();
    };
    let Some(arr) = c.get("mods").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter(|v| {
            v.get("id")
                .or_else(|| v.get("i"))
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .is_some()
        })
        .cloned()
        .collect()
}

fn guest_has_pkg(offer: &Option<Value>, id: &str, ver: &str) -> bool {
    let Some(o) = offer else {
        return false;
    };
    let arr = o
        .get("pkgs")
        .or_else(|| o.get("mods"))
        .and_then(|v| v.as_array());
    let Some(arr) = arr else {
        return false;
    };
    arr.iter().any(|p| {
        p.get("id").or_else(|| p.get("i")).and_then(|v| v.as_str()) == Some(id)
            && p.get("ver").or_else(|| p.get("v")).and_then(|v| v.as_str()) == Some(ver)
    })
}

fn seat_joiner_locked(
    g: &mut HubInner,
    lobby_id: &str,
    player_id: &str,
    peer_ip: &str,
    guest_bind: &str,
    password: Option<&str>,
    mod_offer: Option<Value>,
) -> SeatResult {
    let requested_name = g
        .clients
        .get(player_id)
        .map(|c| c.display_name.clone())
        .unwrap_or_else(|| "Guest".into());

    let seated = {
        let lobby = match g.lobbies.get_mut(lobby_id) {
            Some(l) => l,
            None => return SeatResult::Err("gone"),
        };
        let pw_err = if let (Some(hash), Some(salt)) = (lobby.password_hash, lobby.password_salt) {
            match password.filter(|s| !s.is_empty()) {
                None => Some("need_password"),
                Some(pw) if hash_password(pw, &salt) != hash => Some("bad_password"),
                _ => None,
            }
        } else {
            None
        };
        if let Some(code) = pw_err {
            return SeatResult::Err(code);
        }
        let missing: Vec<Value> = required_mod_rows(&lobby.match_caps)
            .into_iter()
            .filter(|m| {
                let id = m
                    .get("id")
                    .or_else(|| m.get("i"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ver = m
                    .get("ver")
                    .or_else(|| m.get("v"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                !id.is_empty() && !guest_has_pkg(&mod_offer, id, ver)
            })
            .collect();
        if !missing.is_empty() {
            return SeatResult::NeedMods {
                mods: missing,
                can_transfer: true,
            };
        }
        /* A full room is only full when there is no gallery to fall into.
         * The joiner is seated as a spectator rather than refused, and the
         * host can promote it later -- which is the whole point of the two
         * tables. */
        let players_full =
            player_count(lobby) >= lobby.max_slots || lobby.slots.iter().all(|s| s.is_some());
        let seat = if !players_full {
            lobby.slots.iter().position(|s| s.is_none()).unwrap()
        } else {
            match lobby.spectators.iter().position(|s| s.is_none()) {
                Some(i) => spectator_seat(i),
                None => return SeatResult::Err("full"),
            }
        };
        let display_name = unique_display_name(lobby, &requested_name, Some(player_id));
        let mut new_slot = Slot {
            player_id: player_id.to_string(),
            display_name: display_name.clone(),
            ready: false,
            bios_offer: None,
            mod_offer: None,
            memcard_offer: None,
            country: String::new(),
            ice_path: None,
            ice_path_at: None,
        };
        new_slot.country = g
            .clients
            .get(player_id)
            .map(|c| c.country.clone())
            .unwrap_or_default();
        match lobby.seat_mut(seat) {
            Some(cell) => *cell = Some(new_slot),
            None => return SeatResult::Err("full"),
        }
        /* Only players carry Ready, so only players are un-readied. */
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        /* Membership change invalidates prior ICE path pairs. */
        clear_lobby_ice_paths(lobby);
        lobby.guest_endpoint = rewrite_endpoint(guest_bind, peer_ip);
        (
            seat,
            lobby.session_id,
            lobby.host_endpoint.clone(),
            lobby.guest_endpoint.clone(),
            lobby.match_caps.clone(),
            display_name,
        )
    };
    if let Some(c) = g.clients.get_mut(player_id) {
        c.lobby_id = Some(lobby_id.to_string());
        c.display_name = seated.5.clone();
    }
    SeatResult::Ok {
        slot: seated.0,
        session_id: seated.1,
        host_endpoint: seated.2,
        guest_endpoint: seated.3,
        match_caps: seated.4,
    }
}

async fn handle_kick(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let slot = msg.slot.ok_or("missing slot")?;
    struct KickOk {
        lid: String,
        victim: String,
        victim_name: String,
    }
    let outcome: Option<KickOk> = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            return Ok(());
        };
        if lobby.host_player_id != player_id {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_host", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        if lobby.seat(slot).is_none() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        let Some(victim) = lobby
            .seat(slot)
            .and_then(|c| c.as_ref())
            .map(|s| s.player_id.clone())
        else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "empty_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        if victim == lobby.host_player_id || victim == player_id {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "cannot_kick", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        let victim_name = lobby
            .seat_mut(slot)
            .and_then(|cell| cell.as_ref().map(|s| s.display_name.clone()))
            .unwrap_or_default();
        if let Some(cell) = lobby.seat_mut(slot) {
            *cell = None;
        }
        lobby.guest_endpoint.clear();
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        clear_lobby_ice_paths(lobby);
        if let Some(c) = g.clients.get_mut(&victim) {
            c.lobby_id = None;
        }
        Some(KickOk { lid, victim, victim_name })
    };
    if let Some(KickOk { lid, victim, victim_name }) = outcome {
        send_to(
            hub,
            &victim,
            json!({ "op": "kicked", "ok": true, "lobby_id": lid }).to_string(),
        )
        .await;
        emit_lobby_update(hub, &lid).await;
        broadcast_list(hub).await;
        if !victim_name.is_empty() {
            chat_system(hub, &lid, &format!("{victim_name} was kicked.")).await;
        }
    }
    Ok(())
}

/// Host-only: swap (or move into an empty) seat. Broadcasts `lobby_update`.
async fn handle_move(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let from = msg.from_slot.or(msg.slot);
    let to = msg.to_slot;
    let (Some(from), Some(to)) = (from, to) else {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    };
    if from == to {
        return Ok(());
    }
    let lid = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "gone", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        if lobby.host_player_id != player_id {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_host", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        if lobby.seat(from).is_none() || lobby.seat(to).is_none() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        if lobby.seat(from).is_some_and(|c| c.is_none()) {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "empty_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        /* The host is identified by host_player_id, not by slot index, so
         * the host's own seat may move like any other — including trading
         * places with a guest. */
        {
            /* Take/put rather than Vec::swap, because a promotion or demotion
             * moves a seat between two different arrays and a swap within one
             * array cannot express that. Reorders inside a table take the same
             * path, so a cross-table move cannot behave differently from one. */
            let a = lobby.seat_mut(from).and_then(Option::take);
            let b = lobby.seat_mut(to).and_then(Option::take);
            if let Some(cell) = lobby.seat_mut(to) {
                *cell = a;
            }
            if let Some(cell) = lobby.seat_mut(from) {
                *cell = b;
            }
        }
        /* Ready is a player-table property and the roster just changed on at
         * least one side of the move. */
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        clear_lobby_ice_paths(lobby);
        lid
    };
    emit_lobby_update(hub, &lid).await;
    Ok(())
}

/* ===== seat self-service =================================================
 * `move` above is the HOST rearranging anybody. These three let a player
 * manage its OWN seat: taking a free seat is immediate, taking an occupied
 * one needs that player's consent, so it is a request the occupant answers.
 * The server arbitrates, which also makes two simultaneous requests resolve
 * in arrival order rather than racing. Both tables count: a spectator may
 * take a free player seat or ask a player for a taken one, and the reverse,
 * with the same consent rule either way. */

/// A player moves ITSELF into a free seat.
async fn handle_seat_move(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let Some(to) = msg.to_slot.or(msg.target_slot) else {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    };
    let lid = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "gone", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let Some(from) = lobby.seat_of(player_id) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_seated", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        /* Either table, both directions -- but only onto an EMPTY seat. A
         * taken seat is seat_swap_request's business (consent), and the
         * gallery is not a way around it. */
        if lobby.seat(to).is_none() || from == to {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        /* Occupied seats are not takeable without consent — that is what
         * seat_swap_request is for. */
        if lobby.seat(to).is_some_and(|c| c.is_some()) {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "slot_taken", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        /* Take/put rather than swap: the two seats may live in different
         * tables (see `move`). */
        let moving = lobby.seat_mut(from).and_then(Option::take);
        if let Some(cell) = lobby.seat_mut(to) {
            *cell = moving;
        }
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        clear_lobby_ice_paths(lobby);
        lid
    };
    emit_lobby_update(hub, &lid).await;
    Ok(())
}

/// Ask the player sitting in `target_slot` to trade seats. Nothing moves yet.
async fn handle_seat_swap_request(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let Some(target) = msg.target_slot.or(msg.to_slot) else {
        return Ok(());
    };
    let (dest, ask) = {
        let g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get(&lid) else {
            return Ok(());
        };
        let Some(from) = lobby.seat_of(player_id) else {
            return Ok(());
        };
        /* Either table, both directions -- like `seat_move`, but for a TAKEN
         * seat: nothing moves until the occupant says yes, so a spectator
         * asking a player for its seat is as legitimate as a player asking
         * another player. (Whether the host may end up in the gallery is
         * the client's rule: it knows if its backend can run the match that
         * way; the launch path here already sizes the relay for it.) */
        if lobby.seat(target).is_none() || target == from {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        let Some(occupant) = lobby.seat(target).and_then(|c| c.as_ref()) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "empty_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let asker_name = lobby
            .seat(from)
            .and_then(|c| c.as_ref())
            .map(|s| s.display_name.clone())
            .unwrap_or_default();
        (
            occupant.player_id.clone(),
            json!({
                "op": "seat_swap_ask",
                "asker_player_id": player_id,
                "asker_name": asker_name,
                "from_slot": from,
                "target_slot": target,
            })
            .to_string(),
        )
    };
    send_to(hub, &dest, ask).await;
    Ok(())
}

/// The occupant answers. On accept the server performs the swap.
async fn handle_seat_swap_answer(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let accept = msg.accept.unwrap_or(false);
    let asker = msg.asker_player_id.unwrap_or_default();
    if asker.is_empty() {
        return Ok(());
    }
    let (lid, swapped) = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            return Ok(());
        };
        let (Some(mine), Some(theirs)) = (lobby.seat_of(player_id), lobby.seat_of(&asker))
        else {
            /* The asker left, or seats moved under us — decline quietly. */
            drop(g);
            send_to(
                hub,
                &asker,
                json!({ "op": "seat_swap_result", "accept": false, "ok": true }).to_string(),
            )
            .await;
            return Ok(());
        };
        if !accept || mine == theirs {
            (lid.clone(), false)
        } else {
            /* Take/put rather than `slots.swap`: the two seats may live in
             * different tables (a spectator trading into the match). */
            let a = lobby.seat_mut(mine).and_then(Option::take);
            let b = lobby.seat_mut(theirs).and_then(Option::take);
            if let Some(cell) = lobby.seat_mut(mine) {
                *cell = b;
            }
            if let Some(cell) = lobby.seat_mut(theirs) {
                *cell = a;
            }
            for s in lobby.slots.iter_mut().flatten() {
                s.ready = false;
            }
            /* A seat change across tables changes relay slots, as it does
             * for `seat_move`; the paths are re-measured after it. */
            if is_spectator_seat(mine) != is_spectator_seat(theirs) {
                clear_lobby_ice_paths(lobby);
            }
            (lid.clone(), true)
        }
    };
    send_to(
        hub,
        &asker,
        json!({ "op": "seat_swap_result", "accept": swapped, "ok": true }).to_string(),
    )
    .await;
    if swapped {
        emit_lobby_update(hub, &lid).await;
    }
    Ok(())
}

/// Host publishes a STUN-discovered UDP endpoint for list / pre-join RTT.
async fn handle_set_host_endpoint(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let Some(raw) = msg.host_endpoint.filter(|s| !s.trim().is_empty()) else {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_host_endpoint", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    };
    let Some(endpoint) = parse_advertise_endpoint(&raw) else {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_host_endpoint", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    };
    let lid = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "gone", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        if lobby.host_player_id != player_id {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_host", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        /* Launch already rewrote endpoints to the input relay — leave them. */
        if lobby.relay_session_id.is_some() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "relay_locked", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        lobby.host_endpoint = endpoint;
        lobby.lan_endpoints = sanitize_lan_endpoints(msg.lan_endpoints);
        lid
    };
    emit_lobby_update(hub, &lid).await;
    broadcast_list(hub).await;
    send_to(
        hub,
        player_id,
        json!({ "op": "host_endpoint_ok", "ok": true }).to_string(),
    )
    .await;
    Ok(())
}

async fn handle_path_report(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let Some(raw) = msg.path.as_deref() else {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_path_report", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    };
    let Some(kind) = normalize_ice_path(raw) else {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_path_report", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    };
    let err: Option<&'static str> = 'path: {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            break 'path Some("not_in_lobby");
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            break 'path Some("gone");
        };
        for s in lobby.slots.iter_mut().flatten() {
            if s.player_id == player_id {
                s.ice_path = Some(kind.to_string());
                s.ice_path_at = Some(Instant::now());
                debug!(%player_id, path = kind, "lobby ICE path_report");
                break 'path None;
            }
        }
        Some("not_in_lobby")
    };
    if let Some(code) = err {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": code, "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    }
    send_to(
        hub,
        player_id,
        json!({ "op": "path_report_ok", "ok": true, "path": kind }).to_string(),
    )
    .await;
    Ok(())
}

async fn handle_set_match_caps(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let caps = sanitize_match_caps(msg.match_caps);
    if caps.is_none() {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "bad_match_caps", "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    }
    let lid = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "gone", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        if lobby.host_player_id != player_id {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_host", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        lobby.match_caps = caps;
        lid
    };
    emit_lobby_update(hub, &lid).await;
    Ok(())
}

async fn handle_set_ready(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let ready = msg.ready.unwrap_or(true);
    let bios_offer = sanitize_bios_offer(msg.bios_offer);
    let mod_offer = sanitize_mod_offer(msg.mod_offer);
    let memcard_offer = sanitize_memcard_offer(msg.memcard_offer);
    enum ReadyOut {
        Err(&'static str),
        Ok(String),
    }
    let outcome = {
        let mut g = hub.inner.lock().await;
        match g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) {
            None => ReadyOut::Err("not_in_lobby"),
            Some(lid) => match g.lobbies.get_mut(&lid) {
                None => ReadyOut::Err("gone"),
                Some(lobby) => {
                    /* Offers from everyone, Ready from players only.
                     *
                     * A spectator runs the same simulation, so its BIOS and
                     * mod catalogue matter just as much as a player's and the
                     * host needs to see them. What it must not have is a vote
                     * on whether the match may start -- so `ready` stays where
                     * `all_ready` can see it, which is the player table. */
                    let is_player = !lobby
                        .seat_of(player_id)
                        .is_some_and(is_spectator_seat);
                    for s in lobby.everyone_mut() {
                        if s.player_id == player_id {
                            if is_player {
                                s.ready = ready;
                            }
                            if bios_offer.is_some() {
                                s.bios_offer = bios_offer.clone();
                            }
                            if mod_offer.is_some() {
                                s.mod_offer = mod_offer.clone();
                            }
                            if memcard_offer.is_some() {
                                s.memcard_offer = memcard_offer.clone();
                            }
                            break;
                        }
                    }
                    ReadyOut::Ok(lid)
                }
            },
        }
    };
    match outcome {
        ReadyOut::Err(code) => {
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": code, "ok": false }).to_string(),
            )
            .await;
        }
        ReadyOut::Ok(lid) => emit_lobby_update(hub, &lid).await,
    }
    Ok(())
}

async fn handle_start(state: &AppState, player_id: &str, msg: InMsg) -> Result<(), String> {
    enum StartOut {
        Err(&'static str),
        Ok {
            msg: String,
            members: Vec<String>,
            game_name: String,
        },
    }
    let hub = &state.ws_lobby;
    let fresh_caps = sanitize_match_caps(msg.match_caps);

    // Phase 1: validate + allocate session_id (hold lobby lock briefly).
    let prepared = 'prep: {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            break 'prep Err("not_in_lobby");
        };
        let Some(lobby) = g.lobbies.get(&lid) else {
            break 'prep Err("gone");
        };
        if lobby.host_player_id != player_id {
            break 'prep Err("not_host");
        }
        let n = player_count(lobby);
        if n < 2 {
            break 'prep Err("need_players");
        }
        let caps_for_relay = fresh_caps.as_ref().or(lobby.match_caps.as_ref()).cloned();
        /* §108: online lobbies always open lobby UDP SFU (no ice_p2p). */
        let (use_relay, path_why) = start_use_sfu(lobby, &caps_for_relay);
        info!(
            lobby_id = %lid,
            seated = n,
            use_sfu = use_relay,
            reason = path_why,
            "lobby start transport decision"
        );
        if use_relay && !state.input_relay.enabled() {
            break 'prep Err("relay_unavailable");
        }
        /* Same-LAN / split-horizon: every seated peer's WS TCP source is a
         * direct private/loopback address (not the hairpin gateway) → prefer
         * INPUT_RELAY_LAN_HOST. Guests that dial the WAN name and NAT-hairpin
         * often appear as the router (.1) — that must not force LAN advertise. */
        let mut peer_ips = Vec::new();
        /* Everyone seated, gallery included: the relay endpoint advertised
         * here is the one SPECTATORS dial too, so a remote spectator has to
         * be able to veto the LAN address exactly as a remote player does. */
        for slot in lobby.everyone() {
            if let Some(c) = g.clients.get(&slot.player_id) {
                peer_ips.push(c.peer_ip.clone());
            }
        }
        let lan_host = state.config.input_relay_lan_host.trim();
        let gateway = state.config.effective_input_relay_lan_gateway();
        let prefer_lan = !lan_host.is_empty()
            && !peer_ips.is_empty()
            && peer_ips.len() == n + lobby.spectator_count()
            && peer_ips
                .iter()
                .all(|ip| is_direct_lan_ws_peer(ip, gateway.as_deref()));
        let old_relay = lobby.relay_session_id;
        /* Read off `lobby` before the borrow ends: `g.next_session` below
         * needs `g` mutably. */
        let spectators_n = lobby.spectator_count();
        /* The host may watch from the gallery and still run the match. It
         * keeps session slot 0 -- the seat every host-only path (save states,
         * card sync, the start) keys on -- with its pad muted, and the player
         * seats shift up one session slot behind it. The relay therefore
         * needs one more forwarded slot, and the gallery starts one higher. */
        let host_spectates = lobby.seat_of(player_id).is_some_and(is_spectator_seat);
        let max_slots_for_relay = lobby.max_slots + usize::from(host_spectates);
        /* Fresh session_id per match so rematch UDP HELLO/BYE cannot be
         * confused with packets from the previous delay-sync session. */
        let sid = g.next_session;
        g.next_session = g.next_session.saturating_add(1);
        Ok((
            lid,
            sid,
            n,
            spectators_n,
            max_slots_for_relay,
            use_relay,
            old_relay,
            prefer_lan,
            peer_ips,
            host_spectates,
        ))
    };

    let (
        lid,
        sid,
        n,
        spectators_n,
        max_slots_for_relay,
        use_relay,
        old_relay,
        prefer_lan,
        peer_ips,
        host_spectates,
    ) = match prepared {
        Ok(v) => v,
        Err(code) => {
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": code, "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
    };

    // Phase 2: open/close UDP relay outside the lobby lock.
    if let Some(prev) = old_relay {
        state.input_relay.close_session(prev).await;
    }
    let relay_endpoint = if use_relay {
        /* The relay is sized by the seat CEILING, not by how many seats are
         * filled.
         *
         * A player's packet carries its lobby seat index, and the relay rejects
         * any index at or beyond the session's player_slots. In a sparse room
         * -- seats 0 and 3 occupied after a move, so n == 2 -- passing `n`
         * rejects the player in seat 3 outright. Passing max_slots leaves every
         * player seat addressable and puts the gallery immediately above it, so
         * a spectator's relay slot is max_slots + its gallery index and can
         * never be confused with a player's.
         *
         * n stays the PLAYER count in the launch message, where it belongs:
         * that is what sizes the peers' rollback, and the peers already carry
         * occupied_mask for the holes. */
        match state
            .input_relay
            .open_session(sid, max_slots_for_relay as u8, spectators_n as u8)
            .await
        {
            Ok(()) => {
                let ep = state.input_relay.pick_advertise_endpoint(prefer_lan);
                let gateway = state.config.effective_input_relay_lan_gateway();
                if prefer_lan {
                    info!(
                        session_id = sid,
                        advertise = %ep,
                        ?peer_ips,
                        ?gateway,
                        "input relay advertise = LAN (all WS peers direct LAN)"
                    );
                } else if peer_ips.iter().any(|ip| {
                    gateway
                        .as_deref()
                        .is_some_and(|gw| normalize_ws_peer_v4(ip) == gw)
                }) {
                    info!(
                        session_id = sid,
                        advertise = %ep,
                        ?peer_ips,
                        ?gateway,
                        "input relay advertise = public (WS peer via LAN gateway / hairpin)"
                    );
                }
                Some(ep)
            }
            Err(e) => {
                warn!(error = %e, session_id = sid, "input relay open_session failed");
                send_to(
                    hub,
                    player_id,
                    json!({ "op": "error", "code": "relay_unavailable", "ok": false }).to_string(),
                )
                .await;
                return Ok(());
            }
        }
    } else {
        None
    };

    // Phase 3: commit lobby state + build launch payload.
    let outcome = 'out: {
        let mut g = hub.inner.lock().await;
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            break 'out StartOut::Err("gone");
        };
        if let Some(caps) = fresh_caps {
            lobby.match_caps = Some(caps);
        }
        lobby.session_id = sid;
        lobby.started = true;
        if let Some(ref ep) = relay_endpoint {
            lobby.host_endpoint = ep.clone();
            lobby.guest_endpoint = ep.clone();
            lobby.relay_session_id = Some(sid);
        } else {
            lobby.relay_session_id = None;
        }
        /* Match start clears ready so a return-to-lobby rematch must re-confirm. */
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        let n = player_count(lobby);
        let mut slots = Vec::new();
        for (i, s) in lobby.slots.iter().enumerate() {
            if let Some(slot) = s {
                slots.push(slot_json(i, slot));
            }
        }
        let mut spectators = Vec::new();
        for (i, s) in lobby.spectators.iter().enumerate() {
            if let Some(slot) = s {
                spectators.push(slot_json(spectator_seat(i), slot));
            }
        }
        /* The gallery launches with the match. It runs the same simulation
         * from the same start, so it has to be told to start at the same
         * moment and with the same caps -- it simply never contributes a row.
         * player_count stays the PLAYER count: it is what sizes the peers'
         * rollback slot_count, and a spectator counted there would be a seat
         * every player waits on forever. */
        let members = lobby.member_ids();
        let mut launch = json!({
            "op": "launch",
            "ok": true,
            "lobby_id": lobby.lobby_id,
            "session_id": lobby.session_id,
            "host_endpoint": lobby.host_endpoint,
            "guest_endpoint": lobby.guest_endpoint,
            "player_count": n,
            "max_slots": lobby.max_slots,
            "slots": slots,
            "spectators": spectators,
            "spectator_count": spectators.len(),
            "spectator_slot_base": SPECTATOR_SLOT_BASE,
            /* Where the gallery starts in the RELAY's slot space, which is a
             * different namespace from the lobby seat index above. A spectator
             * sends as spectator_relay_base + its gallery index; every player
             * seat is below it. */
            "spectator_relay_base": lobby.max_slots + usize::from(host_spectates),
            /* The host is in the gallery but runs the match from session
             * slot 0 with its pad muted; players sit at lobby seat + 1. Every
             * peer derives its session slot from this, so it is said once. */
            "host_spectates": host_spectates,
            "transport": if relay_endpoint.is_some() { "sfu" } else { "ice_p2p" },
        });
        if let Some(caps) = &lobby.match_caps {
            launch["match_caps"] = caps.clone();
        }
        if let Some(ep) = &relay_endpoint {
            launch["relay_endpoint"] = json!(ep);
        }
        StartOut::Ok {
            msg: launch.to_string(),
            members,
            game_name: lobby.game_name.clone(),
        }
    };
    match outcome {
        StartOut::Err(code) => {
            if let Some(ep_sid) = relay_endpoint.as_ref().map(|_| sid) {
                state.input_relay.close_session(ep_sid).await;
            }
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": code, "ok": false }).to_string(),
            )
            .await;
        }
        StartOut::Ok {
            msg,
            members,
            game_name,
        } => {
            /* `members` is everyone who gets the launch; the metric wants
             * players. They stopped being the same number when the gallery
             * started launching with the match. */
            for m in members {
                send_to(hub, &m, msg.clone()).await;
            }
            metrics::ws_lobby_started(&game_name, n);
        }
    }
    Ok(())
}

/// A system line in the lobby chat ("X has joined."): no sender, `system`
/// set, sent to everyone currently seated. Not stored, like every chat line.
async fn chat_system(hub: &WsLobbyHub, lobby_id: &str, text: &str) {
    let (fwd, targets) = {
        let g = hub.inner.lock().await;
        let Some(lobby) = g.lobbies.get(lobby_id) else {
            return;
        };
        let fwd = json!({
            "op": "chat",
            "lobby_id": lobby_id,
            "from_player_id": "",
            "from": "",
            "system": true,
            "text": text,
        })
        .to_string();
        let targets: Vec<String> = lobby.everyone().map(|s| s.player_id.clone()).collect();
        (fwd, targets)
    };
    for t in targets {
        send_to(hub, &t, fwd.clone()).await;
    }
}

/// Lobby chat. One line from a seated client (player or spectator), echoed to
/// EVERYONE seated including the sender: the room's order is this server's
/// order, so no client appends its own line locally. Not stored — a late
/// joiner starts from the first line after they arrive.
const CHAT_MAX_CHARS: usize = 240;

async fn handle_chat(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let text = msg.text.unwrap_or_default();
    /* One line, no control characters, capped — a chat box is not a channel
     * for anything a client cannot render as a line of text. */
    let text: String = text
        .chars()
        .filter(|c| !c.is_control())
        .take(CHAT_MAX_CHARS)
        .collect();
    let text = text.trim();
    if text.is_empty() {
        return Ok(());
    }
    let (fwd, targets) = {
        let g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "not_in_lobby", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get(&lid) else {
            return Ok(());
        };
        let Some(sender) = lobby.everyone().find(|s| s.player_id == player_id) else {
            return Ok(());
        };
        let fwd = json!({
            "op": "chat",
            "lobby_id": lid,
            "from_player_id": player_id,
            "from": sender.display_name,
            "text": text,
        })
        .to_string();
        let targets: Vec<String> = lobby.everyone().map(|s| s.player_id.clone()).collect();
        (fwd, targets)
    };
    for t in targets {
        send_to(hub, &t, fwd.clone()).await;
    }
    Ok(())
}

async fn handle_signal(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let (fwd, targets) = {
        let g = hub.inner.lock().await;
        let lid = msg
            .lobby_id
            .clone()
            .or_else(|| g.clients.get(player_id).and_then(|c| c.lobby_id.clone()))
            .ok_or_else(|| "no lobby".to_string())?;
        let Some(lobby) = g.lobbies.get(&lid) else {
            return Ok(());
        };
        let fwd = json!({
            "op": "signal",
            "lobby_id": lid,
            "from_player_id": player_id,
            "type": msg.r#type.unwrap_or(0),
            "flag": msg.flag.unwrap_or(0),
            "text": msg.text.unwrap_or_default(),
        })
        .to_string();
        let to = msg.to_player_id.unwrap_or_default();
        let targets: Vec<String> = lobby
            .everyone()
            .filter(|s| s.player_id != player_id)
            .filter(|s| to.is_empty() || s.player_id == to)
            .map(|s| s.player_id.clone())
            .collect();
        (fwd, targets)
    };
    if !targets.is_empty() {
        metrics::ws_signal_relayed();
    }
    for t in targets {
        send_to(hub, &t, fwd.clone()).await;
    }
    Ok(())
}

/// Unused helper kept for future structured Value replies.
#[allow(dead_code)]
fn err_json(code: &str) -> Value {
    json!({ "op": "error", "code": code, "ok": false })
}

#[cfg(test)]
mod geoip_tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn hdr(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", v.parse().unwrap());
        h
    }

    fn peer() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000)
    }

    #[test]
    fn the_header_is_ignored_unless_trusted() {
        /* The whole point of the flag. Anyone reaching the server directly can
         * set this header, so believing it by default would let a client pick
         * its own country. */
        assert_eq!(client_ip_for(&hdr("8.8.8.8"), &peer(), false), "127.0.0.1");
    }

    #[test]
    fn a_trusted_proxy_reveals_the_client() {
        /* And the reason to turn it on: behind a proxy every peer otherwise
         * arrives as loopback, which is private, so nobody gets a flag. */
        assert_eq!(client_ip_for(&hdr("8.8.8.8"), &peer(), true), "8.8.8.8");
    }

    #[test]
    fn the_leftmost_entry_is_the_client() {
        /* Proxies append, so the original client is first and the rest are
         * hops. Taking the last would attribute every player to the proxy. */
        assert_eq!(
            client_ip_for(&hdr("8.8.8.8, 10.0.0.1, 10.0.0.2"), &peer(), true),
            "8.8.8.8"
        );
    }

    #[test]
    fn a_missing_or_empty_header_falls_back_to_the_peer() {
        assert_eq!(client_ip_for(&HeaderMap::new(), &peer(), true), "127.0.0.1");
        assert_eq!(client_ip_for(&hdr("   "), &peer(), true), "127.0.0.1");
        assert_eq!(client_ip_for(&hdr(" , 10.0.0.1"), &peer(), true), "127.0.0.1");
    }

    #[test]
    fn a_deployment_that_installs_nothing_still_gets_flags() {
        /* The whole point of the built-in table. This is the DEFAULT
         * configuration -- no GEOIP_DB_PATH, nothing downloaded -- and it has
         * to answer, or flags remain a per-deployment setup step. */
        let hub = WsLobbyHub::with_geoip(None);
        assert_eq!(hub.country_for("8.8.8.8"), "US");
    }

    #[test]
    fn private_and_malformed_addresses_still_have_no_country() {
        /* The built-in table must not change this: a LAN peer has no country,
         * and neither does a string that is not an address. */
        let hub = WsLobbyHub::with_geoip(None);
        assert_eq!(hub.country_for("127.0.0.1"), "");
        assert_eq!(hub.country_for("192.168.1.4"), "");
        assert_eq!(hub.country_for("10.0.0.7"), "");
        assert_eq!(hub.country_for("::1"), "");
        assert_eq!(hub.country_for("not-an-ip"), "");
        assert_eq!(hub.country_for(""), "");
    }

    #[test]
    fn a_bad_database_path_falls_back_instead_of_failing() {
        /* A mistyped GEOIP_DB_PATH used to mean no flags at all. It now costs
         * accuracy, not the feature -- and the server still starts. */
        let hub = WsLobbyHub::with_geoip(Some("/nonexistent/GeoLite2-Country.mmdb"));
        assert_eq!(hub.country_for("8.8.8.8"), "US");
        assert_eq!(hub.country_for("127.0.0.1"), "");
    }
}

#[cfg(test)]
mod spectator_tests {
    use super::*;

    fn slot(name: &str) -> Slot {
        Slot {
            player_id: name.to_string(),
            display_name: name.to_string(),
            ready: false,
            bios_offer: None,
            mod_offer: None,
            memcard_offer: None,
            country: String::new(),
            ice_path: None,
            ice_path_at: None,
        }
    }

    fn lobby(players: usize, spectators: usize) -> Lobby {
        Lobby {
            lobby_id: "L".into(),
            name: "L".into(),
            game_name: "G".into(),
            game_version: String::new(),
            disc_fp: String::new(),
            host_player_id: "host".into(),
            host_bind: String::new(),
            host_endpoint: String::new(),
            lan_endpoints: Vec::new(),
            guest_endpoint: String::new(),
            password_hash: None,
            password_salt: None,
            max_slots: players,
            session_id: 1,
            slots: vec![None; players],
            allow_spectators: spectators > 0,
            spectators: vec![None; spectators],
            match_caps: None,
            relay_session_id: None,
            started: false,
        }
    }

    #[test]
    fn seat_indices_do_not_collide() {
        /* The two tables share one integer namespace. Player seats must stay
         * where they always were, or every existing client's `slot` changes
         * meaning. */
        assert!(!is_spectator_seat(0));
        assert!(!is_spectator_seat(MAX_SLOTS - 1));
        assert!(is_spectator_seat(spectator_seat(0)));
        assert!(spectator_seat(0) > MAX_SLOTS);
    }

    #[test]
    fn seat_addresses_both_tables() {
        let mut l = lobby(2, 4);
        *l.seat_mut(1).unwrap() = Some(slot("p1"));
        *l.seat_mut(spectator_seat(2)).unwrap() = Some(slot("s2"));

        assert_eq!(
            l.seat(1).unwrap().as_ref().map(|s| s.player_id.as_str()),
            Some("p1")
        );
        assert_eq!(
            l.seat(spectator_seat(2))
                .unwrap()
                .as_ref()
                .map(|s| s.player_id.as_str()),
            Some("s2")
        );
        /* Out of range in either table is None, not a panic and not a
         * wrap-around into the other one. */
        assert!(l.seat(2).is_none());
        assert!(l.seat(spectator_seat(4)).is_none());
    }

    #[test]
    fn counts_keep_the_tables_apart() {
        let mut l = lobby(2, 4);
        *l.seat_mut(0).unwrap() = Some(slot("host"));
        *l.seat_mut(spectator_seat(0)).unwrap() = Some(slot("watcher"));

        /* player_count is what sizes the peers' rollback slot_count. A
         * spectator counted here is a seat every player waits on forever. */
        assert_eq!(player_count(&l), 1);
        assert_eq!(l.spectator_count(), 1);
        assert_eq!(l.everyone().count(), 2);
        assert_eq!(l.member_ids(), vec!["host".to_string(), "watcher".to_string()]);
    }

    #[test]
    fn seat_of_finds_either_table() {
        let mut l = lobby(2, 4);
        *l.seat_mut(1).unwrap() = Some(slot("p"));
        *l.seat_mut(spectator_seat(3)).unwrap() = Some(slot("s"));
        assert_eq!(l.seat_of("p"), Some(1));
        assert_eq!(l.seat_of("s"), Some(spectator_seat(3)));
        assert_eq!(l.seat_of("nobody"), None);
    }

    #[test]
    fn a_lobby_without_spectators_has_no_gallery_seats() {
        /* The toggle is off: there is nowhere for a joiner to overflow to, so
         * a full room is still full. */
        let l = lobby(2, 0);
        assert!(!l.allow_spectators);
        assert!(l.seat(spectator_seat(0)).is_none());
        assert_eq!(l.spectator_count(), 0);
    }

    #[test]
    fn ready_is_a_player_property() {
        let mut l = lobby(2, 4);
        *l.seat_mut(0).unwrap() = Some(slot("host"));
        *l.seat_mut(1).unwrap() = Some(slot("p2"));
        *l.seat_mut(spectator_seat(0)).unwrap() = Some(slot("watcher"));
        for s in l.slots.iter_mut().flatten() {
            s.ready = true;
        }
        /* The gallery never pressed Ready, and the match is still startable. */
        let all_ready = l.slots.iter().flatten().all(|s| s.ready) && player_count(&l) >= 2;
        assert!(all_ready);
    }
}

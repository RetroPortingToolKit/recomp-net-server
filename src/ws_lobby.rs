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
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::moderation;
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
    /// Built by the server for a matched pair. Its host is a sentinel nobody
    /// holds, so it has no host to inherit a departure and no business in the
    /// browser -- both of which need saying rather than inferring from an
    /// empty `host_player_id`.
    automatch: bool,
}

struct ClientMeta {
    player_id: String,
    display_name: String,
    /// The signed-in account, when there is one.
    ///
    /// Attached BESIDE the connection's ephemeral `player_id`, never replacing
    /// it. That is the whole compatibility trick: every lobby, seat, signal and
    /// relay path still keys on the id it always did, and only the code that
    /// cares about identity -- naming, and later reports and bans -- looks
    /// here. `None` is a guest, which is exactly the behaviour this server has
    /// always had.
    account: Option<crate::identity::Player>,
    /// TCP source IP as seen by the lobby (LAN vs WAN / hairpin signal).
    peer_ip: String,
    /// ISO 3166-1 alpha-2 from GeoIP on peer_ip; "" when unknown / private.
    country: String,
    /// The title this client is browsing for, from its `list` requests.
    /// Scopes the players-online list and the per-game server chat.
    game_name: String,
    lobby_id: Option<String>,
    /// Password-ok join waiting on missing mods (not seated).
    pending_mod_lobby: Option<String>,
    /// Round trip to the relay, as this client measured it with a UDP probe;
    /// -1 until it reports one. Kept on the CONNECTION, not the ticket, so a
    /// client that probes before it queues does not have to probe again.
    probe_rtt_ms: i32,
    tx: broadcast::Sender<String>,
}

/// One chat line the server relayed, kept just long enough to be reportable.
///
/// Chat is not persisted (see `handle_chat`), and this does not change that.
/// These live in a bounded in-memory ring and are forgotten as they scroll
/// off; a line reaches the database only if somebody reports it, and then only
/// that line and a few before it.
///
/// The ring exists so a report can name a MESSAGE and the server can write
/// down what it actually relayed. A report that carried its own copy of the
/// text would let a reporter fabricate one, which is the difference between a
/// moderation record and an accusation.
#[derive(Clone)]
struct ChatLine {
    id: String,
    /// Account of the sender. Empty for a signed-out connection, which is why
    /// such a line cannot be reported: there is nobody to attribute it to.
    from_account: String,
    from_name: String,
    text: String,
    /// "lobby" | "server"
    scope: String,
    /// Empty for server chat.
    lobby_id: String,
    game_name: String,
}

/// How many relayed lines stay reportable.
///
/// Generous enough that somebody can read a line, decide, and still find it --
/// which takes longer than people assume during an argument -- and bounded so
/// a busy server cannot be made to hold chat indefinitely. At 240 chars a line
/// the whole ring is a few hundred KB.
const CHAT_RING_MAX: usize = 1000;

/// Preceding lines stored with a report.
const CHAT_CONTEXT_LINES: usize = 6;

struct HubInner {
    clients: HashMap<String, ClientMeta>,
    lobbies: HashMap<String, Lobby>,
    next_session: u32,
    /// Recently relayed chat, oldest first. See `ChatLine`.
    chat_ring: VecDeque<ChatLine>,
    /// Source of message ids. Process-local and never reused within a run; a
    /// restart empties the ring, so an id from a previous process simply finds
    /// nothing and the report is refused rather than misattributed.
    next_chat_id: u64,
}

#[derive(Clone, Default)]
pub struct WsLobbyHub {
    inner: Arc<Mutex<HubInner>>,
    /// GeoIP country reader (None = flags off). Shared, read-only.
    geoip: Arc<Option<maxminddb::Reader<Vec<u8>>>>,
}

impl HubInner {
    /// Record a relayed line and hand back its id, which goes out with the
    /// message so a client can later report exactly this one.
    fn remember_chat(
        &mut self,
        from_account: &str,
        from_name: &str,
        text: &str,
        scope: &str,
        lobby_id: &str,
        game_name: &str,
    ) -> String {
        let id = format!("{:x}", self.next_chat_id);
        self.next_chat_id += 1;
        self.chat_ring.push_back(ChatLine {
            id: id.clone(),
            from_account: from_account.to_string(),
            from_name: from_name.to_string(),
            text: text.to_string(),
            scope: scope.to_string(),
            lobby_id: lobby_id.to_string(),
            game_name: game_name.to_string(),
        });
        while self.chat_ring.len() > CHAT_RING_MAX {
            self.chat_ring.pop_front();
        }
        id
    }

    /// The reported line, plus the lines before it from the same room.
    ///
    /// Same room only: pulling context across rooms would put unrelated
    /// people's chat into a moderation record about someone else, which is
    /// both useless and a privacy cost with nothing to show for it.
    fn chat_with_context(&self, message_id: &str) -> Option<(ChatLine, String)> {
        let at = self.chat_ring.iter().position(|l| l.id == message_id)?;
        let line = self.chat_ring[at].clone();
        let start = at.saturating_sub(CHAT_CONTEXT_LINES);
        let context = self
            .chat_ring
            .iter()
            .take(at)
            .skip(start)
            .filter(|l| l.scope == line.scope && l.lobby_id == line.lobby_id)
            .map(|l| format!("{}: {}", l.from_name, l.text))
            .collect::<Vec<_>>()
            .join("\n");
        Some((line, context))
    }
}

impl Default for HubInner {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
            lobbies: HashMap::new(),
            chat_ring: VecDeque::new(),
            next_chat_id: 1,
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
    /// Our own session JWT, from POST /auth/discord/poll. OPTIONAL, and its
    /// absence is the whole compatibility story: a client that has never heard
    /// of Discord simply does not send it and is a guest, exactly as before.
    #[serde(default)]
    session: Option<String>,
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
    /* ---- automatch ---- */
    /// Titles this ticket will accept, in the client's preference order. A
    /// launcher that knows one game sends one entry; the server must not
    /// assume that.
    #[serde(default)]
    titles: Option<Vec<InTitle>>,
    /// Which queue, when the client already picked one.
    #[serde(default)]
    ruleset_id: Option<String>,
    /// The client asserting it will boot vanilla -- its own verdict, kept as
    /// a first gate. It is no longer the only thing the server has: see
    /// `mod_exempt`, which carries the evidence the verdict was computed from
    /// so the server can apply the allowlist itself.
    #[serde(default)]
    mods_enabled: Option<bool>,
    /// Every cosmetic exemption the ticket relies on, as `id@version#sha256`.
    ///
    /// A mod that only changes what a machine draws may be run by one player
    /// alone; one that touches the simulation may not. Which is which is the
    /// ruleset's call, so the client declares what it is relying on and the
    /// server checks it against `match_caps.mod_cosmetic_allow`. Absent or
    /// empty means "none claimed", which is what every client older than this
    /// field sends and is exactly as safe as before.
    #[serde(default)]
    mod_exempt: Option<Vec<String>>,
    /* `desync_report` only. The client sends mod_exempt as a STRING here
     * rather than the array the ticket uses, because a report is one flat row
     * and the ';'-joined form is what goes in the column. */
    #[serde(default)]
    tick: Option<i64>,
    #[serde(default)]
    partition: Option<String>,
    #[serde(default)]
    mine: Option<String>,
    #[serde(default)]
    theirs: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    mod_exempt_text: Option<String>,
    /* `chat_report`: which line, why, and an optional sentence for a human.
     * The TEXT is never taken from the client -- the server looks up what it
     * relayed under this id. */
    #[serde(default)]
    mid: Option<String>,
    /* Several messages at once: harassment is usually a burst, and making
     * somebody file six reports to describe one incident produces six rows
     * that each look minor. `mid` remains accepted so an older client still
     * reports. */
    #[serde(default)]
    mids: Option<Vec<String>>,
    #[serde(default)]
    game: Option<String>,
    #[serde(default)]
    platform: Option<String>,
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    lobby: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    note: Option<String>,
    /// Echoed on `automatch_accept` so a late answer to a lapsed offer is
    /// recognisably late rather than applied to the next one.
    #[serde(default)]
    match_id: Option<String>,
    /// Round trip to the relay in milliseconds, measured by the client with a
    /// UDP probe. Accepted on `automatch_rtt` and on `automatch_queue`.
    #[serde(default)]
    rtt_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct InTitle {
    #[serde(default)]
    game_name: String,
    #[serde(default)]
    game_version: String,
    #[serde(default)]
    disc_fp: String,
    #[serde(default)]
    ruleset_id: String,
    #[serde(default)]
    max_slots: Option<usize>,
}

/* ---- client-supplied display strings ------------------------------------
 * Every name a client sends is rebroadcast to its peers -- `display_name`
 * becomes chat's `from` and the seat/players-online rows, `name` is the room
 * title in `lobby_list`, `game_name` scopes both. They used to be assigned
 * raw, at three separate sites (`hello`, `create`, `join`), while a chat LINE
 * three functions away was capped, control-stripped and filtered. One gate,
 * applied once at the deserialization boundary, is what stops the next
 * handler from forgetting: see sanitize_in_place below. */

/// The name gate lives in `names` so the Discord login path shares it rather
/// than growing a second copy. Re-exported here under the names this file has
/// always used.
fn sanitize_name(s: Option<String>) -> Option<String> {
    crate::names::sanitize(s)
}

/// A name that trips the word list is REFUSED, not masked. See `names`.
fn name_is_refused(name: &str) -> bool {
    crate::names::is_refused(name)
}

/// A password is VALIDATED, never rewritten.
///
/// Sanitizing one is worse than refusing it: silently dropping a character
/// leaves the host with a password that is not the one they typed, and both
/// sides then disagree about a secret. Nothing here is ever shown to a peer
/// (only `has_password`, and the value itself is salted and hashed), so the
/// filter has no business in it -- this only rejects what no honest client
/// can produce.
const PASSWORD_MAX_BYTES: usize = 128;

fn password_is_invalid(pw: &str) -> bool {
    pw.len() > PASSWORD_MAX_BYTES || pw.chars().any(|c| c.is_control())
}

impl InMsg {
    /// The gate: every client-supplied string is cleaned or refused HERE, once,
    /// before any handler runs.
    ///
    /// `Some(code)` refuses the whole message -- the caller answers with that
    /// error and dispatches nothing, so a bad name cannot ride in on a
    /// `create` or a `join` only to be refused after the room exists.
    fn sanitize_in_place(&mut self) -> Option<&'static str> {
        self.display_name = sanitize_name(self.display_name.take());
        self.name = sanitize_name(self.name.take());
        /* A title is a SCOPING KEY, not prose: clients filter the lobby list
         * and the server chat by string equality on it. Masking or refusing it
         * would silently split one game into two sets of rooms that cannot see
         * each other, so it gets the hygiene pass and nothing more. */
        self.game_name = sanitize_name(self.game_name.take());

        if matches!(self.display_name.as_deref(), Some(n) if name_is_refused(n)) {
            return Some("name_rejected");
        }
        if matches!(self.name.as_deref(), Some(n) if name_is_refused(n)) {
            return Some("lobby_name_rejected");
        }
        if matches!(self.password.as_deref(), Some(pw) if password_is_invalid(pw)) {
            return Some("password_invalid");
        }
        None
    }
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

/// `account` is the opaque, stable id behind the seat ("" for a guest),
/// resolved by the caller -- every call site already holds the hub lock, and
/// looking it up there beats copying it into five seating sites that would
/// each have to be kept in step.
/// The opaque account id behind a connection, or "" for a guest.
fn account_of(g: &HubInner, player_id: &str) -> String {
    g.clients
        .get(player_id)
        .and_then(|c| c.account.as_ref())
        .map(|a| a.id.to_string())
        .unwrap_or_default()
}

fn slot_json(i: usize, slot: &Slot, account: &str) -> Value {
    let mut row = json!({
        "slot": i,
        "player_id": slot.player_id,
        /* Stable across the peer's reconnect and rename, unlike either name
         * beside it -- this is what a client-side block list can key on. */
        "account": account,
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
    /// First 8 chars of the connection id, so a client can find its own
    /// row (a display name is not unique across the hub) without the hub
    /// publishing whole ids to every browser.
    tag: String,
    /// The signed-in ACCOUNT behind this connection, as an opaque id -- empty
    /// for a guest.
    ///
    /// This is `players.id`, the server's own row key, and deliberately NOT
    /// the Discord snowflake, which is never published to other players. It is
    /// also not the handle: 002_discord_identity.sql is explicit that
    /// netplay_handle is "never an identity, is not unique" and
    /// discord_username is "not our key".
    ///
    /// It exists because a client-side ignore/block list needs something that
    /// survives a reconnect AND a rename. Keyed on a name, such a list blocks
    /// whoever renames into it and frees whoever renames out -- which is worse
    /// than not having one. Opaque, stable, and useless for finding somebody
    /// off this server, which is the whole set of properties wanted.
    account: String,
    /// The title this client is browsing for ("" until its first `list`).
    game_name: String,
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
            /* An automatch room is not joinable and must not be shopped for.
             * It is only created at both-accept, so it never exists while
             * unjoinable -- this is the belt to that braces, so a future
             * rematch hold cannot leak one into the browser. */
            if l.automatch {
                return false;
            }
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
        .filter(|c| match filter_game {
            Some(g) if !g.is_empty() => c.game_name == g,
            _ => true,
        })
        .map(|c| {
            let lobby = c.lobby_id.as_ref().and_then(|id| hub.lobbies.get(id));
            OnlinePlayerRow {
                display_name: c.display_name.clone(),
                country: c.country.clone(),
                lobby_id: lobby.map(|l| l.lobby_id.clone()).unwrap_or_default(),
                lobby_name: lobby.map(|l| l.name.clone()).unwrap_or_default(),
                hosting: lobby.is_some_and(|l| l.host_player_id == c.player_id),
                tag: c.player_id.chars().take(8).collect(),
                account: c.account.as_ref().map(|a| a.id.to_string()).unwrap_or_default(),
                game_name: c.game_name.clone(),
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

/// The list as ONE PARTICULAR CLIENT should see it.
///
/// A `list` request carries the caller's title and is filtered by it, but the
/// two PUSHED paths -- the 1 Hz tick and the broadcast on every state change
/// -- used to build one unfiltered payload and send that same blob to
/// everyone. So a client that asked a filtered question got the right answer
/// and had it overwritten a second later by a list containing every player and
/// room on the server, whatever game they were playing. The filtering looked
/// broken because the last word always came from the unfiltered path.
///
/// The scope is the client's own, resolved the same way chat resolves it
/// (`game_scope_for`), so the players-online list, the room list and the chat
/// audience all agree about what "here" means. A client that has not announced
/// a title yet still sees everything -- it has not told us what to filter by,
/// and showing it nothing would make a browser look empty rather than
/// unfiltered.
fn lobby_list_json_for(g: &HubInner, player_id: &str) -> String {
    let scope = game_scope_for(g, player_id);
    if scope.is_empty() {
        return lobby_list_json(g);
    }
    lobby_list_json_filtered(g, Some(&scope), None)
}

async fn broadcast_list(hub: &WsLobbyHub) {
    let g = hub.inner.lock().await;
    let ids: Vec<String> = g.clients.keys().cloned().collect();
    /* Built per SCOPE rather than per client: everyone playing one title gets
     * a byte-identical list, and a server with three titles on it does three
     * serializations instead of one per connection. */
    let mut by_scope: HashMap<String, String> = HashMap::new();
    for id in ids {
        let Some(c) = g.clients.get(&id) else { continue };
        let scope = game_scope_for(&g, &id);
        let payload = by_scope
            .entry(scope.clone())
            .or_insert_with(|| {
                if scope.is_empty() {
                    lobby_list_json(&g)
                } else {
                    lobby_list_json_filtered(&g, Some(&scope), None)
                }
            })
            .clone();
        let _ = c.tx.send(payload);
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
                slots.push(slot_json(i, slot, &account_of(&g, &slot.player_id)));
            }
        }
        let mut spectators = Vec::new();
        for (i, s) in l.spectators.iter().enumerate() {
            if let Some(slot) = s {
                spectators.push(slot_json(spectator_seat(i), slot,
                                          &account_of(&g, &slot.player_id)));
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
        /* An automatch room's host is a sentinel, so nobody's departure is
         * the host's. Left alone it would survive as a one-seat room forever;
         * it dies when it can no longer be a match. */
        let automatch_emptied = lobby.automatch && player_count(lobby) <= 2;
        if lobby.host_player_id == player_id || automatch_emptied {
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
                /* A connection starts as a guest and may be upgraded by a
                 * `hello` carrying a session. It is never downgraded. */
                account: None,
                country: hub.country_for(&peer_ip),
                peer_ip: peer_ip.clone(),
                game_name: String::new(),
                lobby_id: None,
                pending_mod_lobby: None,
                probe_rtt_ms: -1,
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
                lobby_list_json_for(&g, &player_tick)
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
    /* A ticket must not outlive its socket. Left behind it would block that
     * ACCOUNT from ever queueing again (one ticket per account) and would keep
     * being offered matches nobody is listening for. A pair already on offer
     * settles as a dodge: the other player is sitting in front of a countdown
     * either way, and closing the window is not a cheaper way to decline. */
    if let Some(p) = state.automatch.remove_player(&player_id).await {
        settle_declined(&state, &p, &player_id, "timeout").await;
    }
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
    let mut msg: InMsg = serde_json::from_str(text).map_err(|e| e.to_string())?;
    if let Some(code) = msg.sanitize_in_place() {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": code, "ok": false }).to_string(),
        )
        .await;
        return Ok(());
    }
    match msg.op.as_str() {
        "hello" => {
            /* `hello` is the identity message, not a handshake: a client
             * re-sends it when the player renames, and everything that shows
             * a name has to follow. Before this, a rename reached the hub
             * only on the next reconnect, so the players-online list and the
             * seat table both kept the name the player first typed. */
            let hello_game = msg.game_name.clone().filter(|s| !s.is_empty());
            let mut hello_name = msg.display_name.clone().filter(|s| !s.is_empty());

            /* A session upgrades this connection from guest to account. The
             * server then OWNS the name: an account's handle is the thing a
             * report or a ban hangs off, so it must not be whatever the client
             * felt like sending. A guest keeps naming itself, as always. */
            if let Some(tok) = msg.session.as_deref().filter(|s| !s.is_empty()) {
                match crate::auth::verify_session_token(&state.config, tok)
                    .ok()
                    .and_then(|c| uuid::Uuid::parse_str(&c.sub).ok())
                {
                    Some(uuid) => match crate::identity::load_player(&state.pool, &uuid).await {
                        Ok(Some(player)) => {
                            hello_name = Some(player.handle.clone());
                            let mut g = hub.inner.lock().await;
                            if let Some(c) = g.clients.get_mut(player_id) {
                                c.account = Some(player);
                            }
                        }
                        /* A valid token for a row that is gone: treat as a
                         * guest rather than half-authenticating. */
                        _ => {}
                    },
                    None => {
                        send_to(
                            hub,
                            player_id,
                            json!({ "op": "error", "code": "session_invalid", "ok": false })
                                .to_string(),
                        )
                        .await;
                    }
                }
            }
            let (accepted_name, renamed_in) = {
                let mut g = hub.inner.lock().await;
                apply_identity(&mut g, player_id, hello_name, hello_game)
            };
            send_to(
                hub,
                player_id,
                /* The accepted name, which is not always the requested one
                 * (see the dedupe above). */
                json!({ "op": "hello_ok", "ok": true, "display_name": accepted_name })
                    .to_string(),
            )
            .await;
            if let Some(lid) = renamed_in {
                emit_lobby_update(hub, &lid).await;
            }
        }
        "list" => {
            /* The title a client lists for is the title it is playing: it
             * scopes what "players online" and the server chat mean to it. */
            if let Some(g) = msg.game_name.as_deref().filter(|s| !s.is_empty()) {
                let mut guard = hub.inner.lock().await;
                if let Some(c) = guard.clients.get_mut(player_id) {
                    c.game_name = g.to_string();
                }
            }
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
        "server_chat" => handle_server_chat(hub, player_id, msg).await?,
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
        "automatch_rulesets" => handle_automatch_rulesets(state, player_id, msg).await?,
        "automatch_queue" => handle_automatch_queue(state, player_id, msg).await?,
        "automatch_cancel" => handle_automatch_cancel(state, player_id).await?,
        "automatch_rtt" => handle_automatch_rtt(state, player_id, msg).await?,
        "desync_report" => handle_desync_report(state, player_id, msg).await?,
        "chat_report" => handle_chat_report(state, player_id, msg).await?,
        "automatch_accept" => handle_automatch_accept(state, player_id, msg).await?,
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
    /* Kept for the client's own per-game scope after the lobby takes it. */
    let host_game_name = game_name.clone();
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
                automatch: false,
            },
        );
        if let Some(c) = g.clients.get_mut(player_id) {
            c.lobby_id = Some(lobby_id.clone());
            /* Hosting a room for a title says which title this client plays
             * as plainly as listing for it does. */
            c.game_name = host_game_name;
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
    let lobby_game = g
        .lobbies
        .get(lobby_id)
        .map(|l| l.game_name.clone())
        .unwrap_or_default();
    if let Some(c) = g.clients.get_mut(player_id) {
        c.lobby_id = Some(lobby_id.to_string());
        c.display_name = seated.5.clone();
        /* Sitting in a room for a title says which title this client plays. */
        if !lobby_game.is_empty() {
            c.game_name = lobby_game;
        }
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

/// `start` from a player: check they are the host, then run the shared path.
///
/// The check lives HERE and the work lives in `start_lobby`, because automatch
/// rooms are started by the server and a second copy of the transport
/// decision, the LAN-advertise heuristic and the launch broadcast is exactly
/// how the two would drift apart.
async fn handle_start(state: &AppState, player_id: &str, msg: InMsg) -> Result<(), String> {
    let hub = &state.ws_lobby;
    let lid = {
        let g = hub.inner.lock().await;
        let Some(lid) = g.clients.get(player_id).and_then(|c| c.lobby_id.clone()) else {
            send_to(hub, player_id, am_err("not_in_lobby")).await;
            return Ok(());
        };
        let Some(lobby) = g.lobbies.get(&lid) else {
            send_to(hub, player_id, am_err("gone")).await;
            return Ok(());
        };
        if lobby.host_player_id != player_id {
            send_to(hub, player_id, am_err("not_host")).await;
            return Ok(());
        }
        lid
    };
    if let Err(code) = start_lobby(state, &lid, msg.match_caps, Some(player_id)).await {
        send_to(hub, player_id, am_err(code)).await;
    }
    Ok(())
}

/// Open the relay and launch a lobby.
///
/// `initiator` is the host that pressed Play, or None when the server is the
/// host. Errors are RETURNED rather than sent: a server-started match has no
/// player to address a `not_host` to, and the caller knows who (if anyone) is
/// waiting on an answer.
async fn start_lobby(
    state: &AppState,
    lid: &str,
    raw_caps: Option<Value>,
    initiator: Option<&str>,
) -> Result<(), &'static str> {
    enum StartOut {
        Err(&'static str),
        Ok {
            msg: String,
            members: Vec<String>,
            game_name: String,
        },
    }
    let hub = &state.ws_lobby;
    let fresh_caps = sanitize_match_caps(raw_caps);
    let lid = lid.to_string();

    // Phase 1: validate + allocate session_id (hold lobby lock briefly).
    let prepared = 'prep: {
        let mut g = hub.inner.lock().await;
        let Some(lobby) = g.lobbies.get(&lid) else {
            break 'prep Err("gone");
        };
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
        /* Only a player can be in the gallery and still run the match. The
         * server has no seat, so a server-started room never spectates. */
        let host_spectates = initiator
            .and_then(|p| lobby.seat_of(p))
            .is_some_and(is_spectator_seat);
        let max_slots_for_relay = lobby.max_slots + usize::from(host_spectates);
        /* Fresh session_id per match so rematch UDP HELLO/BYE cannot be
         * confused with packets from the previous delay-sync session. */
        let sid = g.next_session;
        g.next_session = g.next_session.saturating_add(1);
        Ok((
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
        Err(code) => return Err(code),
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
                return Err("relay_unavailable");
            }
        }
    } else {
        None
    };

    // Phase 3: commit lobby state + build launch payload.
    let outcome = 'out: {
        let mut g = hub.inner.lock().await;
        /* Snapshot the accounts BEFORE the mutable borrow below: the seat rows
         * are serialised while `lobby` is held mutably, and the ids live on
         * the hub's client map. One pass, no borrow argument. */
        let acct_map: HashMap<String, String> = g
            .clients
            .values()
            .filter_map(|c| {
                c.account.as_ref().map(|a| (c.player_id.clone(), a.id.to_string()))
            })
            .collect();
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
                slots.push(slot_json(i, slot, acct_map.get(&slot.player_id)
                                                  .map(String::as_str).unwrap_or("")));
            }
        }
        let mut spectators = Vec::new();
        for (i, s) in lobby.spectators.iter().enumerate() {
            if let Some(slot) = s {
                spectators.push(slot_json(spectator_seat(i), slot,
                                          acct_map.get(&slot.player_id)
                                              .map(String::as_str).unwrap_or("")));
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
            return Err(code);
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


/* ===================== automatch ===================== */

use crate::automatch::{self, MatchKey, Pending, Ticket};

fn am_err(code: &str) -> String {
    json!({ "op": "error", "code": code, "ok": false }).to_string()
}

/// Turn a client's ticket request into match keys, or say which gate refused.
///
/// Every gate here is one `join` would apply later. Refusing at queue time is
/// the whole point: a pair that fails at seating has already spent both
/// players' accept-gate attention.
fn build_keys(
    rulesets: &automatch::Rulesets,
    titles: &[InTitle],
) -> Result<Vec<MatchKey>, &'static str> {
    let mut keys = Vec::new();
    let mut first_err: Option<&'static str> = None;
    for t in titles {
        let game_name = t.game_name.trim();
        if game_name.is_empty() {
            first_err.get_or_insert("unknown_ruleset");
            continue;
        }
        let Some(rs) = rulesets.resolve(game_name, t.ruleset_id.trim()) else {
            first_err.get_or_insert("unknown_ruleset");
            continue;
        };
        /* A wildcard fingerprint is fine for a human picking a room out of a
         * list; in a queue it silently pairs a Track-01-only dump against a
         * full multi-track cue, which is the case the fingerprint exists to
         * catch. */
        let disc_fp = normalize_disc_fp(Some(t.disc_fp.clone()));
        if disc_fp.is_empty() {
            first_err.get_or_insert("need_disc_fp");
            continue;
        }
        let game_version = normalize_game_version(Some(t.game_version.clone()));
        if !rs.game_version.is_empty() && rs.game_version != game_version {
            first_err.get_or_insert("version_not_pooled");
            continue;
        }
        if t.max_slots.unwrap_or(2) != 2 {
            first_err.get_or_insert("slots_not_pooled");
            continue;
        }
        let key = MatchKey {
            game_name: game_name.to_string(),
            game_version,
            disc_fp,
            ruleset_id: rs.id.clone(),
            max_slots: rs.max_slots,
        };
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    if keys.is_empty() {
        return Err(first_err.unwrap_or("unknown_ruleset"));
    }
    Ok(keys)
}

/// A player reporting somebody's chat line.
///
/// The client names a MESSAGE; the server writes down what it relayed under
/// that id, from its own ring. Nothing the reporter sends becomes the evidence
/// — they choose a category and may add a sentence, and that is all. A report
/// carrying its own copy of the text would let anyone manufacture a message
/// and have somebody sanctioned for it.
///
/// Both ends need an account. The reporter's, because an anonymous report is
/// unanswerable and unratelimitable; the accused's, because a sanction has to
/// attach to something a reconnect does not erase. A line from a signed-out
/// guest is refused with `cannot_report` rather than stored against nobody.
async fn handle_chat_report(
    state: &AppState,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let hub = &state.ws_lobby;
    /* `mids` is the current shape; `mid` is what an older client sends. */
    let mut ids: Vec<String> = msg.mids.clone().unwrap_or_default();
    if ids.is_empty() {
        if let Some(one) = msg.mid.clone() {
            ids.push(one);
        }
    }
    ids.retain(|s| !s.is_empty());
    ids.truncate(16);
    if ids.is_empty() {
        send_to(hub, player_id, am_err("bad_report")).await;
        return Ok(());
    }
    let mid = ids[0].clone();

    let (reporter, found, others) = {
        let g = hub.inner.lock().await;
        let reporter = g
            .clients
            .get(player_id)
            .and_then(|c| c.account.as_ref())
            .map(|a| a.id.to_string());
        /* Resolve every id. Ones that have scrolled away are simply absent
         * from the transcript rather than failing the report: a reporter who
         * selected six lines and waited too long for one of them should still
         * get the other five on the record. */
        let others: Vec<(String, String)> = ids
            .iter()
            .filter_map(|m| g.chat_with_context(m).map(|(l, _)| (l.from_name, l.text)))
            .collect();
        (reporter, g.chat_with_context(&mid), others)
    };

    let Some(reporter_id) = reporter else {
        send_to(hub, player_id, am_err("need_account")).await;
        return Ok(());
    };
    /* Gone from the ring, or never in it. Says so plainly rather than
     * pretending to file: somebody who reported a line and was told nothing
     * has no way to know it did not happen. */
    let Some((line, context)) = found else {
        send_to(hub, player_id, am_err("message_expired")).await;
        return Ok(());
    };
    if line.from_account.is_empty() {
        send_to(hub, player_id, am_err("cannot_report")).await;
        return Ok(());
    }
    if line.from_account == reporter_id {
        /* Reporting yourself is not meaningful and is an easy way to put rows
         * in the queue. Refused quietly. */
        send_to(hub, player_id, am_err("cannot_report")).await;
        return Ok(());
    }

    let transcript = others
        .iter()
        .map(|(who, what)| format!("{who}: {what}"))
        .collect::<Vec<_>>()
        .join("\n");

    let report = moderation::ChatReport {
        reporter_id: reporter_id.clone(),
        accused_id: line.from_account.clone(),
        message_id: line.id.clone(),
        message_text: line.text.clone(),
        accused_name: line.from_name.clone(),
        context,
        transcript,
        message_count: others.len() as i64,
        /* Location fields prefer the SERVER's own knowledge and fall back to
         * what the client said. The client's copy matters because a LAN or
         * direct session has no server-side record at all -- but where the
         * server does know, its answer is the one that cannot be edited. */
        scope: if line.scope.is_empty() {
            msg.scope.clone().unwrap_or_default()
        } else {
            line.scope.clone()
        },
        lobby_id: if line.lobby_id.is_empty() {
            msg.lobby.clone().unwrap_or_default()
        } else {
            line.lobby_id.clone()
        },
        game_name: if line.game_name.is_empty() {
            msg.game.clone().unwrap_or_default()
        } else {
            line.game_name.clone()
        },
        game_version: msg.game_version.clone().unwrap_or_default(),
        platform: msg.platform.clone().unwrap_or_default(),
        server: msg.server.clone().unwrap_or_default(),
        reason: msg.reason.clone().unwrap_or_default(),
        note: msg.note.clone().unwrap_or_default(),
    };

    let code = match moderation::record(
        &state.pool,
        &state.config.chat_report_dump_dir,
        &report,
    )
    .await
    {
        /* A duplicate answers OK on purpose. The reporter did what they meant
         * to; that a row already existed is the server's business, not a
         * failure to hand back to somebody who just reported abuse. */
        moderation::Submitted::Ok | moderation::Submitted::Duplicate => None,
        moderation::Submitted::RateLimited => Some("rate_limited"),
        moderation::Submitted::Failed => Some("report_failed"),
    };
    match code {
        Some(c) => send_to(hub, player_id, am_err(c)).await,
        None => {
            send_to(
                hub,
                player_id,
                json!({ "op": "chat_report_ok", "ok": true, "mid": mid }).to_string(),
            )
            .await
        }
    }
    Ok(())
}

/// One peer reporting that the two simulations diverged.
///
/// Stored, never judged. A fork means two peers disagreed at a tick -- version
/// skew produces it, and so does a genuine emulation bug, from two entirely
/// honest players. Which side moved is not answerable from one disagreement at
/// all; it needs many matches against many DIFFERENT opponents, which is the
/// one thing this server can do that neither client can. So both digests are
/// stored, both peers report independently, and nothing here decides anything.
/// See `migrations/005_desync_reports.sql` and AUTOMATCH.md §15.
///
/// Requires a signed-in account for the same reason automatch does: a signal a
/// reconnect erases is not a signal. A report from an anonymous connection is
/// dropped silently -- it is a diagnostic, and refusing it loudly would give a
/// client something to handle on a path that runs while a match is falling
/// apart.
async fn handle_desync_report(
    state: &AppState,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let hub = &state.ws_lobby;
    let (account, game_name) = {
        let g = hub.inner.lock().await;
        let Some(c) = g.clients.get(player_id) else {
            return Ok(());
        };
        (
            c.account.as_ref().map(|a| a.id.to_string()),
            c.game_name.clone(),
        )
    };
    let Some(account_id) = account else {
        return Ok(());
    };

    let report = automatch::DesyncReport {
        lobby_id: msg.lobby_id.clone().unwrap_or_default(),
        game_name,
        game_version: msg.game_version.clone().unwrap_or_default(),
        disc_fp: msg.disc_fp.clone().unwrap_or_default(),
        tick: msg.tick.unwrap_or(0),
        partition: msg.partition.clone().unwrap_or_default(),
        digest_mine: msg.mine.clone().unwrap_or_default(),
        digest_theirs: msg.theirs.clone().unwrap_or_default(),
        role: msg.role.clone().unwrap_or_default(),
        mod_exempt: msg.mod_exempt_text.clone().unwrap_or_default(),
    };
    /* Deliberately info!, not warn!. A fork is a fact about a session, not a
     * fault and not an accusation, and a log level that reads as an alarm is
     * how a row like this ends up treated as proof of something. */
    info!(
        account = %account_id,
        lobby = %report.lobby_id,
        tick = report.tick,
        partition = %report.partition,
        mine = %report.digest_mine,
        theirs = %report.digest_theirs,
        role = %report.role,
        "desync reported (two peers differed; this is not an attribution)"
    );
    automatch::record_desync(&state.pool, &account_id, &report).await;
    Ok(())
}

async fn handle_automatch_rulesets(state: &AppState, player_id: &str, msg: InMsg) -> Result<(), String> {
    let hub = &state.ws_lobby;
    let game = match msg.game_name.filter(|g| !g.is_empty()) {
        Some(g) => g,
        None => {
            let g = hub.inner.lock().await;
            g.clients.get(player_id).map(|c| c.game_name.clone()).unwrap_or_default()
        }
    };
    let mut payload = automatch::rulesets_json(&state.automatch_rulesets, &game);
    /* Here as well as on `automatch_queued`, because THIS is the message a
     * launcher reads before it draws the queue button -- probing now means
     * the ticket carries a measurement from its first millisecond. */
    payload["probe"] = json!({
        "endpoint": state.input_relay.advertise_endpoint(),
        "magic": state.config.protocol_magic,
        "type": automatch::PROBE_PKT_TYPE,
    });
    let payload = payload.to_string();
    send_to(hub, player_id, payload).await;
    Ok(())
}

async fn handle_automatch_queue(state: &AppState, player_id: &str, msg: InMsg) -> Result<(), String> {
    let hub = &state.ws_lobby;
    if state.automatch_rulesets.is_empty() {
        send_to(hub, player_id, am_err("automatch_off")).await;
        return Ok(());
    }

    /* The account, not the connection. Automatch is the one surface that
     * requires a sign-in, because the accept gate's cost has to survive a
     * reconnect and a `Uuid::new_v4()` per socket does not. */
    let (account, handle, username, country, in_lobby, known_rtt) = {
        let g = hub.inner.lock().await;
        let Some(c) = g.clients.get(player_id) else {
            return Ok(());
        };
        (
            c.account.as_ref().map(|a| a.id.to_string()),
            c.account.as_ref().map(|a| a.handle.clone()).unwrap_or_else(|| c.display_name.clone()),
            c.account.as_ref().map(|a| a.discord_username.clone()).unwrap_or_default(),
            c.country.clone(),
            c.lobby_id.is_some(),
            c.probe_rtt_ms,
        )
    };
    let Some(account_id) = account else {
        send_to(hub, player_id, am_err("need_account")).await;
        return Ok(());
    };
    if in_lobby {
        send_to(hub, player_id, am_err("already_in_lobby")).await;
        return Ok(());
    }
    /* Per ACCOUNT, not per connection: two clients on one login must not get
     * two rolls of the dice. */
    if state.automatch.is_queued_account(&account_id).await {
        send_to(hub, player_id, am_err("already_queued")).await;
        return Ok(());
    }
    if msg.mods_enabled.unwrap_or(false) {
        send_to(hub, player_id, am_err("mods_not_pooled")).await;
        return Ok(());
    }

    let titles = msg.titles.clone().unwrap_or_default();
    /* A client that named no titles but did name a ruleset is the
     * single-title launcher; build the one entry it meant. */
    let titles = if titles.is_empty() {
        let g = hub.inner.lock().await;
        let game = g.clients.get(player_id).map(|c| c.game_name.clone()).unwrap_or_default();
        drop(g);
        vec![InTitle {
            game_name: game,
            game_version: msg.game_version.clone().unwrap_or_default(),
            disc_fp: msg.disc_fp.clone().unwrap_or_default(),
            ruleset_id: msg.ruleset_id.clone().unwrap_or_default(),
            max_slots: None,
        }]
    } else {
        titles
    };

    let keys = match build_keys(&state.automatch_rulesets, &titles) {
        Ok(k) => k,
        Err(code) => {
            send_to(hub, player_id, am_err(code)).await;
            return Ok(());
        }
    };

    /* The cosmetic-exemption gate, and the point at which the server stops
     * taking the client's word for which of its mods are harmless.
     *
     * `mods_enabled` above is still the client's own verdict and still
     * refused when true. This is the other half: the ticket declares the
     * exemptions it is RELYING on, and each one is checked against the
     * allowlist of the ruleset being queued into. A mod the ruleset has not
     * approved is refused here however the client classified it, so shipping
     * a modified client no longer widens what a player may run.
     *
     * Checked against EVERY ruleset in the ticket, not just the first. A
     * ticket enters a bucket for each of its keys and may be paired under any
     * of them, so approval by one permissive ruleset must not carry a mod
     * into a strict pool listed beside it. Refusing the whole ticket rather
     * than dropping the titles that disapprove is the conservative choice: a
     * dropped title would silently queue the player for less than they asked
     * for, and this way the message names what to turn off.
     */
    let exempt: Vec<String> = msg.mod_exempt.clone().unwrap_or_default();
    if !exempt.is_empty() {
        for t in &titles {
            let Some(rs) = state
                .automatch_rulesets
                .resolve(&t.game_name, &t.ruleset_id)
            else {
                continue; // build_keys already refused anything unresolvable
            };
            if let Some(bad) = automatch::unapproved_exemption(&rs.match_caps, &exempt) {
                warn!(
                    account = %account_id,
                    game = %t.game_name,
                    ruleset = %rs.id,
                    claim = %bad,
                    "automatch: refused a mod exemption the ruleset does not approve"
                );
                send_to(
                    hub,
                    player_id,
                    json!({
                        "op": "error",
                        "code": "mod_not_approved",
                        "ok": false,
                        "ruleset_id": rs.id,
                        "mod": bad,
                    })
                    .to_string(),
                )
                .await;
                return Ok(());
            }
        }
        /* Logged even when every claim passes. What a player was running is
         * the first question after any dispute about a match, and a claim
         * that was accepted is exactly the record that makes a later lie
         * visible as a change. */
        info!(
            account = %account_id,
            claims = %exempt.join(";"),
            "automatch: accepted declared mod exemptions"
        );
    }

    let cool = automatch::cooldown_secs(
        &state.pool,
        &account_id,
        &state.config.automatch_dodge_cooldowns,
    )
    .await;
    if cool > 0 {
        send_to(
            hub,
            player_id,
            json!({ "op": "error", "code": "cooldown", "ok": false, "retry_secs": cool })
                .to_string(),
        )
        .await;
        return Ok(());
    }

    if state.automatch.waiting().await >= state.config.automatch_queue_max {
        send_to(hub, player_id, am_err("queue_full")).await;
        return Ok(());
    }

    let ticket = Ticket {
        player_id: player_id.to_string(),
        account_id: account_id.clone(),
        handle,
        username,
        country,
        keys: keys.clone(),
        host_bind: msg.host_bind.clone().unwrap_or_else(|| "0.0.0.0:7777".into()),
        guest_bind: msg.guest_bind.clone().unwrap_or_else(|| "0.0.0.0:7778".into()),
        queued_at: std::time::Instant::now(),
        /* A value on the queue message wins over whatever the connection
         * already had -- it is the fresher measurement. Still -1 when the
         * client has not probed, and the pairing filter passes on unknown
         * rather than refusing every pair over a number nobody took. */
        rtt_ms: match msg.rtt_ms {
            Some(v) => automatch::sanitize_rtt(v),
            None => known_rtt,
        },
    };
    state.automatch.push(ticket).await;

    let mut rows = Vec::new();
    for k in &keys {
        rows.push(json!({
            "game_name": k.game_name,
            "ruleset_id": k.ruleset_id,
            "pool": state.automatch.pool_for(k, &account_id).await,
        }));
    }
    /* Where to probe, and with what. A client that has not measured yet gets
     * the address in the same breath as being told it is queued, so the first
     * `automatch_rtt` can arrive seconds later and still qualify the pair. */
    send_to(
        hub,
        player_id,
        json!({
            "op": "automatch_queued", "ok": true, "titles": rows,
            "probe": {
                "endpoint": state.input_relay.advertise_endpoint(),
                "magic": state.config.protocol_magic,
                "type": automatch::PROBE_PKT_TYPE,
            },
        })
        .to_string(),
    )
    .await;
    info!(%player_id, keys = keys.len(), "automatch queued");
    metrics::automatch_queued();
    /* Pair now rather than at the next tick: with two people waiting, a
     * one-second delay is the whole experience. */
    run_pairing(state).await;
    Ok(())
}

/// A client reporting what its UDP probe measured.
///
/// Sendable any time: before queueing (it lands on the connection and the next
/// ticket inherits it) or while queued (it updates the waiting ticket, so a
/// client can re-probe as conditions change).
async fn handle_automatch_rtt(state: &AppState, player_id: &str, msg: InMsg) -> Result<(), String> {
    let hub = &state.ws_lobby;
    let rtt = automatch::sanitize_rtt(msg.rtt_ms.unwrap_or(-1));
    {
        let mut g = hub.inner.lock().await;
        if let Some(c) = g.clients.get_mut(player_id) {
            c.probe_rtt_ms = rtt;
        }
    }
    state.automatch.set_rtt(player_id, rtt).await;
    send_to(
        hub,
        player_id,
        json!({ "op": "automatch_rtt_ok", "ok": true, "rtt_ms": rtt }).to_string(),
    )
    .await;
    Ok(())
}

async fn handle_automatch_cancel(state: &AppState, player_id: &str) -> Result<(), String> {
    let hub = &state.ws_lobby;
    /* Leaving while a pair is on offer IS a decline: the other side is
     * sitting in front of a countdown either way. */
    if let Some(p) = state.automatch.remove_player(player_id).await {
        settle_declined(state, &p, player_id, "timeout").await;
    }
    send_to(
        hub,
        player_id,
        json!({ "op": "automatch_cancelled", "ok": true, "reason": "cancelled" }).to_string(),
    )
    .await;
    Ok(())
}

async fn handle_automatch_accept(state: &AppState, player_id: &str, msg: InMsg) -> Result<(), String> {
    let hub = &state.ws_lobby;
    let accept = msg.accept.unwrap_or(false);
    send_to(
        hub,
        player_id,
        json!({ "op": "automatch_accept_ok", "ok": true, "accept": accept }).to_string(),
    )
    .await;

    let Some(p) = state.automatch.answer(player_id, accept).await else {
        /* Either not our offer, or the peer has not answered yet. Both are
         * ordinary; the accept_ok above already told the client we heard. */
        return Ok(());
    };
    if p.both_yes() {
        form_match(state, p).await;
    } else {
        let decliner = if p.a_accept == Some(false) { &p.a } else { &p.b };
        settle_declined(state, &p, &decliner.player_id.clone(), "decline").await;
    }
    Ok(())
}

/// One side said no (or vanished). Charge them, and put the other side back at
/// the front of the queue.
async fn settle_declined(state: &AppState, p: &Pending, loser_player_id: &str, kind: &str) {
    let hub = &state.ws_lobby;
    let (loser, other) = if p.a.player_id == loser_player_id {
        (&p.a, &p.b)
    } else {
        (&p.b, &p.a)
    };
    automatch::record_strike(&state.pool, &loser.account_id, kind, &p.key.game_name).await;
    metrics::automatch_dodge();
    send_to(
        hub,
        &loser.player_id,
        json!({
            "op": "automatch_cancelled", "ok": true, "reason": "declined",
            "cooldown_secs": automatch::cooldown_secs(
                &state.pool, &loser.account_id, &state.config.automatch_dodge_cooldowns).await,
        })
        .to_string(),
    )
    .await;

    let reason = if kind == "decline" { "peer_declined" } else { "peer_timeout" };
    /* Only requeue somebody who is still connected and still wants it. */
    let still_here = {
        let g = hub.inner.lock().await;
        g.clients.contains_key(&other.player_id)
    };
    if still_here {
        state.automatch.requeue_front(other.clone()).await;
        send_to(
            hub,
            &other.player_id,
            json!({ "op": "automatch_requeue", "ok": true, "reason": reason, "queued": true })
                .to_string(),
        )
        .await;
    }
}

/// Offer a formed pair to both sides.
async fn offer_pair(state: &AppState, p: &Pending) {
    let hub = &state.ws_lobby;
    let rs = state
        .automatch_rulesets
        .resolve(&p.key.game_name, &p.key.ruleset_id);
    let label = rs.map(|r| r.label.clone()).unwrap_or_default();
    let est = if p.a.rtt_ms >= 0 && p.b.rtt_ms >= 0 {
        p.a.rtt_ms + p.b.rtt_ms
    } else {
        -1
    };
    /* The caps this match will actually run, floor included. Computed here so
     * the accept gate shows the delay the player is agreeing to rather than
     * the ruleset's advertised one -- being told "delay 2" and then playing
     * at 6 is the kind of surprise that reads as a bug. */
    let (caps, floor) = match rs {
        Some(r) => {
            let f = automatch::delay_floor(&r.match_caps, p.a.rtt_ms, p.b.rtt_ms, r.frame_ms);
            (Some(automatch::caps_with_floor(&r.match_caps, f)), Some(f))
        }
        None => (None, None),
    };
    automatch::record_pairing(
        &state.pool,
        &p.match_id,
        &p.key,
        &p.a.account_id,
        &p.b.account_id,
        est,
    )
    .await;

    for (me, them) in [(&p.a, &p.b), (&p.b, &p.a)] {
        let mut m = json!({
            "op": "automatch_found",
            "ok": true,
            "match_id": p.match_id,
            "game_name": p.key.game_name,
            "game_version": p.key.game_version,
            "ruleset_id": p.key.ruleset_id,
            "ruleset_label": label,
            /* The opponent as the launcher draws them. The snowflake is NOT
             * here and never is: the handle is what other players see, and
             * the @username is only the disambiguator for two of them. */
            "opponent": {
                "handle": them.handle,
                "discord_username": them.username,
                "country": them.country,
            },
            "est_rtt_ms": est,
            "accept_secs": state.config.automatch_accept_secs,
        });
        if let Some(f) = floor {
            m["input_delay"] = json!(f.input_delay);
            m["input_prediction"] = json!(f.input_prediction);
            /* 0 means nothing was measured and the ruleset stands as written,
             * which a launcher can say plainly instead of implying a floor it
             * did not compute. */
            m["frames_needed"] = json!(f.needed);
        }
        if let Some(c) = &caps {
            m["match_caps"] = c.clone();
        }
        send_to(hub, &me.player_id, m.to_string()).await;
    }
    info!(
        match_id = %p.match_id, game = %p.key.game_name,
        rtt_a = p.a.rtt_ms, rtt_b = p.b.rtt_ms,
        delay = floor.map(|f| f.input_delay).unwrap_or(0),
        prediction = floor.map(|f| f.input_prediction).unwrap_or(0),
        "automatch pair offered"
    );
    metrics::automatch_paired();
}

/// Both accepted. Build the room, seat them, and hand off to the start path.
async fn form_match(state: &AppState, p: Pending) {
    let hub = &state.ws_lobby;
    /* Note the pairing HERE, where a match actually happens, and not where the
     * offer was sent.
     *
     * It used to be recorded in offer_pair, which meant a DECLINED offer
     * counted as "these two just played" -- so declining once put the pair
     * under the avoid-last-opponent cooldown for five minutes and the two
     * players could not be matched again until one of them had waited out the
     * 100 seconds that makes the filter give up. Declining is supposed to cost
     * the decliner a dodge strike and nothing else; it is not supposed to
     * remove an opponent from their pool. In a pool of two it removed the only
     * opponent there was.
     *
     * The filter's purpose is "you two just played, have someone else for a
     * while", and an offer nobody accepted is not that. */
    state.automatch.note_pairing(&p.a.account_id, &p.b.account_id).await;
    let Some(rs) = state
        .automatch_rulesets
        .resolve(&p.key.game_name, &p.key.ruleset_id)
        .cloned()
    else {
        return;
    };

    let floor = automatch::delay_floor(&rs.match_caps, p.a.rtt_ms, p.b.rtt_ms, rs.frame_ms);
    let floored_caps = automatch::caps_with_floor(&rs.match_caps, floor);

    let lobby_id = Uuid::new_v4().to_string();
    let session_id;
    {
        let mut g = hub.inner.lock().await;
        if g.lobbies.len() >= MAX_LOBBIES {
            drop(g);
            for t in [&p.a, &p.b] {
                send_to(
                    hub,
                    &t.player_id,
                    json!({ "op": "automatch_requeue", "ok": true,
                            "reason": "lobby_limit", "queued": false })
                        .to_string(),
                )
                .await;
            }
            return;
        }
        /* Either side may have dropped between the offer and the second
         * accept. Better to unwind here than to open a relay session for a
         * room one player will never reach. */
        if !g.clients.contains_key(&p.a.player_id) || !g.clients.contains_key(&p.b.player_id) {
            drop(g);
            let gone = {
                let gg = hub.inner.lock().await;
                if gg.clients.contains_key(&p.a.player_id) { p.a.player_id.clone() } else { p.b.player_id.clone() }
            };
            let survivor = if gone == p.a.player_id { &p.b } else { &p.a };
            let still = {
                let gg = hub.inner.lock().await;
                gg.clients.contains_key(&survivor.player_id)
            };
            if still {
                state.automatch.requeue_front(survivor.clone()).await;
                send_to(
                    hub,
                    &survivor.player_id,
                    json!({ "op": "automatch_requeue", "ok": true,
                            "reason": "peer_left", "queued": true })
                        .to_string(),
                )
                .await;
            }
            return;
        }

        session_id = g.next_session;
        g.next_session = g.next_session.saturating_add(1);

        let mut slots: Vec<Option<Slot>> = vec![None; rs.max_slots];
        for (i, t) in [(&0usize, &p.a), (&1usize, &p.b)] {
            let country = g
                .clients
                .get(&t.player_id)
                .map(|c| c.country.clone())
                .unwrap_or_default();
            slots[*i] = Some(Slot {
                player_id: t.player_id.clone(),
                display_name: t.handle.clone(),
                ready: true,
                bios_offer: None,
                mod_offer: None,
                memcard_offer: None,
                country,
                ice_path: None,
                ice_path_at: None,
            });
        }

        let host_endpoint = rewrite_endpoint(
            &p.a.host_bind,
            &g.clients.get(&p.a.player_id).map(|c| c.peer_ip.clone()).unwrap_or_default(),
        );
        let guest_endpoint = rewrite_endpoint(
            &p.b.guest_bind,
            &g.clients.get(&p.b.player_id).map(|c| c.peer_ip.clone()).unwrap_or_default(),
        );

        g.lobbies.insert(
            lobby_id.clone(),
            Lobby {
                lobby_id: lobby_id.clone(),
                name: rs.label.clone(),
                game_name: p.key.game_name.clone(),
                game_version: p.key.game_version.clone(),
                disc_fp: p.key.disc_fp.clone(),
                /* THE sentinel. No connection can hold "", so `start`,
                 * `kick`, `set_match_caps`, `close` and `move` all answer
                 * not_host through the checks that already exist, and both
                 * clients correctly see is_host = false. Saying "the server
                 * is the host" by giving the role to nobody beats handing it
                 * to a player and then trying to take pieces of it back. */
                host_player_id: String::new(),
                host_bind: p.a.host_bind.clone(),
                host_endpoint,
                lan_endpoints: Vec::new(),
                guest_endpoint,
                password_hash: None,
                password_salt: None,
                max_slots: rs.max_slots,
                session_id,
                slots,
                allow_spectators: false,
                spectators: Vec::new(),
                /* The floor is part of the match, not advice about it: the
                 * room stores what it will run, so `launch` carries it and a
                 * peer applies it at boot like any other cap. */
                match_caps: Some(floored_caps.clone()),
                relay_session_id: None,
                started: false,
                automatch: true,
            },
        );
        for (i, t) in [(0usize, &p.a), (1usize, &p.b)] {
            if let Some(c) = g.clients.get_mut(&t.player_id) {
                c.lobby_id = Some(lobby_id.clone());
                c.pending_mod_lobby = None;
            }
            let _ = i;
        }
    }

    /* Seated exactly as a human-hosted room seats, so the client's waiting
     * room, caps application and launch path are the ones that already ship. */
    for (i, t) in [(0usize, &p.a), (1usize, &p.b)] {
        let (host_endpoint, guest_endpoint) = {
            let g = hub.inner.lock().await;
            g.lobbies
                .get(&lobby_id)
                .map(|l| (l.host_endpoint.clone(), l.guest_endpoint.clone()))
                .unwrap_or_default()
        };
        let mut joined = json!({
            "op": "joined",
            "ok": true,
            "lobby_id": lobby_id,
            "session_id": session_id,
            "local_slot": i,
            "spectator": false,
            "spectator_slot_base": SPECTATOR_SLOT_BASE,
            "host_endpoint": host_endpoint,
            "guest_endpoint": guest_endpoint,
            "automatch": true,
        });
        joined["match_caps"] = floored_caps.clone();
        send_to(hub, &t.player_id, joined.to_string()).await;
    }
    emit_lobby_update(hub, &lobby_id).await;
    info!(
        match_id = %p.match_id, %lobby_id,
        delay = floor.input_delay, prediction = floor.input_prediction,
        needed = floor.needed, "automatch match formed"
    );

    /* The settle is not cosmetic: both clients get a beat to apply caps, and
     * a peer that drops between accept and launch has somewhere to be
     * noticed. */
    let delay = state.config.automatch_start_delay_secs;
    let st = state.clone();
    let lid = lobby_id.clone();
    let match_id = p.match_id.clone();
    tokio::spawn(async move {
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
        }
        match start_lobby(&st, &lid, None, None).await {
            Ok(()) => automatch::mark_launched(&st.pool, &match_id).await,
            Err(code) => {
                warn!(lobby_id = %lid, code, "automatch start failed");
                let note = json!({ "op": "error", "code": code, "ok": false }).to_string();
                let members = {
                    let g = st.ws_lobby.inner.lock().await;
                    g.lobbies.get(&lid).map(|l| l.member_ids()).unwrap_or_default()
                };
                for m in members {
                    send_to(&st.ws_lobby, &m, note.clone()).await;
                }
                destroy_lobby(&st, &lid).await;
            }
        }
    });
}

/// Form what pairs can be formed and offer them.
pub async fn run_pairing(state: &AppState) {
    let offers = state
        .automatch
        .pair(state.config.automatch_rematch_cooldown_secs)
        .await;
    for p in offers {
        offer_pair(state, &p).await;
    }
}

/// The 1 Hz driver: lapse dead offers, pair, then tell everyone waiting where
/// they stand.
pub async fn automatch_tick(state: &AppState) {
    for p in state
        .automatch
        .take_expired(state.config.automatch_accept_secs)
        .await
    {
        /* Whoever did not answer pays. If neither did, both do -- there is no
         * innocent party in an offer two people ignored. */
        let a_bad = p.a_accept != Some(true);
        let b_bad = p.b_accept != Some(true);
        if a_bad && b_bad {
            for t in [&p.a, &p.b] {
                automatch::record_strike(&state.pool, &t.account_id, "timeout", &p.key.game_name)
                    .await;
                send_to(
                    &state.ws_lobby,
                    &t.player_id,
                    json!({ "op": "automatch_cancelled", "ok": true, "reason": "timeout" })
                        .to_string(),
                )
                .await;
            }
            metrics::automatch_dodge();
        } else {
            let loser = if a_bad { p.a.player_id.clone() } else { p.b.player_id.clone() };
            settle_declined(state, &p, &loser, "timeout").await;
        }
    }

    run_pairing(state).await;

    for (player_id, waited, rtt, pools) in state.automatch.status_rows().await {
        let rows: Vec<Value> = pools
            .into_iter()
            .map(|(k, n)| json!({ "game_name": k.game_name, "ruleset_id": k.ruleset_id, "pool": n }))
            .collect();
        send_to(
            &state.ws_lobby,
            &player_id,
            json!({
                "op": "automatch_status", "ok": true,
                "queued_secs": waited, "est_rtt_ms": rtt, "titles": rows,
            })
            .to_string(),
        )
        .await;
    }
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
    /* Profanity and slurs are masked here, before any peer sees the line
     * (see chat_filter.rs); clients mask again on arrival, which is what
     * covers LAN rooms and older servers. */
    let text = crate::chat_filter::apply(text);
    let text = text.as_str();
    let (fwd, targets) = {
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
        let Some(lobby) = g.lobbies.get(&lid) else {
            return Ok(());
        };
        let Some(sender) = lobby.everyone().find(|s| s.player_id == player_id) else {
            return Ok(());
        };
        /* The seat row carries the display name; the account comes from the
         * connection behind it, which is where identity lives. */
        let from_account = {
            g.clients.get(player_id)
                .and_then(|c| c.account.as_ref())
                .map(|a| a.id.to_string())
                .unwrap_or_default()
        };
        let from_name = sender.display_name.clone();
        let game_name = g
            .clients
            .get(player_id)
            .map(|c| c.game_name.clone())
            .unwrap_or_default();
        let targets: Vec<String> = lobby.everyone().map(|s| s.player_id.clone()).collect();
        /* The id goes out WITH the line, so a client reporting it names a
         * message this server relayed rather than describing one. */
        let mid = g.remember_chat(&from_account, &from_name, text, "lobby", &lid, &game_name);
        let fwd = json!({
            "op": "chat",
            "lobby_id": lid,
            "mid": mid,
            "from_player_id": player_id,
            "from_account": from_account,
            "from": from_name,
            "text": text,
        })
        .to_string();
        (fwd, targets)
    };
    for t in targets {
        send_to(hub, &t, fwd.clone()).await;
    }
    Ok(())
}

/// Apply an identity (`hello`) to a connected client: the hub-wide row, and
/// -- when it is seated -- the seat everyone in its room reads, which is
/// deduplicated within that room exactly as a join is. Returns the accepted
/// name (not always the requested one) and the lobby that needs a
/// `lobby_update`, if any.
fn apply_identity(
    g: &mut HubInner,
    player_id: &str,
    name: Option<String>,
    game: Option<String>,
) -> (String, Option<String>) {
    let mut renamed_in = None;
    if let Some(c) = g.clients.get_mut(player_id) {
        if let Some(n) = name.clone() {
            c.display_name = n;
        }
        /* The title comes in on the FIRST message a client sends, not only
         * on `list`: server chat and the players-online scope must not
         * depend on having browsed first. */
        if let Some(gn) = game {
            c.game_name = gn;
        }
    }
    if let Some(n) = name {
        let lid = g.clients.get(player_id).and_then(|c| c.lobby_id.clone());
        if let Some(lid) = lid {
            if let Some(unique) = g
                .lobbies
                .get(&lid)
                .map(|l| unique_display_name(l, &n, Some(player_id)))
            {
                if let Some(lobby) = g.lobbies.get_mut(&lid) {
                    if let Some(slot) = lobby.everyone_mut().find(|s| s.player_id == player_id) {
                        if slot.display_name != unique {
                            slot.display_name = unique.clone();
                            renamed_in = Some(lid.clone());
                        }
                    }
                }
                /* The hub row follows the seat, exactly as it does on join,
                 * so one player never reads as two different names. */
                if let Some(c) = g.clients.get_mut(player_id) {
                    c.display_name = unique;
                }
            }
        }
    }
    let accepted = g
        .clients
        .get(player_id)
        .map(|c| c.display_name.clone())
        .unwrap_or_default();
    (accepted, renamed_in)
}

/// The title a client counts as playing, for the players-online list and
/// for server chat. It is the TITLE ONLY and never the version: two builds
/// of one game are one audience, even when the lobby list hides one from
/// the other. Learned from `hello`, from `list`, from creating or joining a
/// room, and from a `server_chat` line that carries it; the room the client
/// sits in is the last word when none of those arrived.
fn game_scope_for(g: &HubInner, player_id: &str) -> String {
    let Some(c) = g.clients.get(player_id) else {
        return String::new();
    };
    if !c.game_name.is_empty() {
        return c.game_name.clone();
    }
    c.lobby_id
        .as_ref()
        .and_then(|id| g.lobbies.get(id))
        .map(|l| l.game_name.clone())
        .unwrap_or_default()
}

/// Per-game chat outside any room: every client browsing for the same
/// title hears it, seated or not. Same shape and same filter as lobby
/// chat; no history, so a newcomer sees only what is said after arriving.
async fn handle_server_chat(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    let text = msg.text.unwrap_or_default();
    let text: String = text
        .chars()
        .filter(|c| !c.is_control())
        .take(CHAT_MAX_CHARS)
        .collect();
    let text = text.trim();
    if text.is_empty() {
        return Ok(());
    }
    let text = crate::chat_filter::apply(text);
    let (fwd, targets) = {
        let mut g = hub.inner.lock().await;
        /* A line may carry its own title. Taking it here is what stops a
         * client that has not browsed yet from being turned away with
         * `no_game` -- the first thing it does may well be to say hello in
         * the chat. */
        if let Some(gn) = msg.game_name.as_deref().filter(|s| !s.is_empty()) {
            if let Some(c) = g.clients.get_mut(player_id) {
                c.game_name = gn.to_string();
            }
        }
        let scope = game_scope_for(&g, player_id);
        if scope.is_empty() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "no_game", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        /* Remember what we resolved (it may have come from the room), so the
         * players-online filter and the next line agree with this one. */
        if let Some(c) = g.clients.get_mut(player_id) {
            if c.game_name.is_empty() {
                c.game_name = scope.clone();
            }
        }
        let Some(sender) = g.clients.get(player_id) else {
            return Ok(());
        };
        /* The opaque account id, so a muted player stays muted across their
         * reconnect and their rename. Empty for a guest, whose lines can only
         * be muted for as long as the connection lasts -- and, for the same
         * reason, cannot be reported: there is nobody to attribute them to. */
        let from_account = sender
            .account
            .as_ref()
            .map(|a| a.id.to_string())
            .unwrap_or_default();
        let from_name = sender.display_name.clone();
        let country = sender.country.clone();
        /* Audience: same title, any version. */
        let targets: Vec<String> = g
            .clients
            .values()
            .filter(|c| game_scope_for(&g, &c.player_id) == scope)
            .map(|c| c.player_id.clone())
            .collect();
        let mid = g.remember_chat(&from_account, &from_name, &text, "server", "", &scope);
        let fwd = json!({
            "op": "server_chat",
            "game_name": scope,
            "mid": mid,
            "from_player_id": player_id,
            "from_account": from_account,
            "from": from_name,
            "country": country,
            "text": text,
        })
        .to_string();
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
mod game_scope_tests {
    use super::*;

    fn hub_inner() -> HubInner {
        HubInner::default()
    }

    fn client(g: &mut HubInner, id: &str, game: &str, lobby: Option<&str>) {
        let (tx, _rx) = broadcast::channel(4);
        g.clients.insert(
            id.to_string(),
            ClientMeta {
                player_id: id.to_string(),
                display_name: id.to_string(),
                account: None,
                peer_ip: "127.0.0.1".into(),
                country: String::new(),
                game_name: game.to_string(),
                lobby_id: lobby.map(str::to_string),
                pending_mod_lobby: None,
                probe_rtt_ms: -1,
                tx,
            },
        );
    }

    fn seat(g: &mut HubInner, lobby_id: &str, index: usize, player_id: &str, name: &str) {
        let l = g.lobbies.get_mut(lobby_id).unwrap();
        l.slots[index] = Some(Slot {
            player_id: player_id.into(),
            display_name: name.into(),
            ready: false,
            bios_offer: None,
            mod_offer: None,
            memcard_offer: None,
            country: String::new(),
            ice_path: None,
            ice_path_at: None,
        });
    }

    fn seat_name(g: &HubInner, lobby_id: &str, index: usize) -> String {
        g.lobbies[lobby_id].slots[index]
            .as_ref()
            .map(|s| s.display_name.clone())
            .unwrap_or_default()
    }

    fn lobby(g: &mut HubInner, id: &str, game: &str) {
        g.lobbies.insert(
            id.to_string(),
            Lobby {
                lobby_id: id.into(),
                name: id.into(),
                game_name: game.into(),
                game_version: "9.9.9".into(),
                disc_fp: String::new(),
                host_player_id: String::new(),
                host_bind: String::new(),
                host_endpoint: String::new(),
                lan_endpoints: Vec::new(),
                guest_endpoint: String::new(),
                password_hash: None,
                password_salt: None,
                max_slots: 2,
                session_id: 1,
                slots: vec![None; 2],
                allow_spectators: false,
                spectators: Vec::new(),
                match_caps: None,
                relay_session_id: None,
                started: false,
                automatch: false,
            },
        );
    }

    #[test]
    fn the_recorded_title_is_the_scope() {
        let mut g = hub_inner();
        client(&mut g, "a", "Crash Bash", None);
        assert_eq!(game_scope_for(&g, "a"), "Crash Bash");
    }

    #[test]
    fn a_client_that_never_said_its_title_falls_back_to_its_room() {
        /* This is the case that answered `no_game`: a client seated in a
         * room, chatting before it ever sent a `list`. */
        let mut g = hub_inner();
        lobby(&mut g, "L", "Gundam Wing");
        client(&mut g, "a", "", Some("L"));
        assert_eq!(game_scope_for(&g, "a"), "Gundam Wing");
    }

    #[test]
    fn nothing_anywhere_is_still_no_scope() {
        let mut g = hub_inner();
        client(&mut g, "a", "", None);
        assert_eq!(game_scope_for(&g, "a"), "");
        assert_eq!(game_scope_for(&g, "nobody"), "");
    }

    #[test]
    fn the_scope_is_the_title_and_never_the_version() {
        /* Two builds of one game are one audience. The lobby list may hide
         * one from the other (strict version filtering), but the chat and
         * the players-online list must not split along that line. */
        let mut g = hub_inner();
        lobby(&mut g, "old", "Crash Bash");
        lobby(&mut g, "new", "Crash Bash");
        g.lobbies.get_mut("old").unwrap().game_version = "0.1.0".into();
        g.lobbies.get_mut("new").unwrap().game_version = "0.2.0".into();
        client(&mut g, "a", "", Some("old"));
        client(&mut g, "b", "", Some("new"));
        client(&mut g, "c", "Crash Bash", None);
        assert_eq!(game_scope_for(&g, "a"), game_scope_for(&g, "b"));
        assert_eq!(game_scope_for(&g, "a"), game_scope_for(&g, "c"));
    }

    #[test]
    fn a_rename_reaches_the_seat_and_the_hub_row() {
        /* The bug this covers: a rename only ever reached the server in the
         * first hello, so the players-online list and the seat both kept the
         * name the player first typed until the next reconnect. */
        let mut g = hub_inner();
        lobby(&mut g, "L", "Crash Bash");
        client(&mut g, "a", "Crash Bash", Some("L"));
        seat(&mut g, "L", 0, "a", "Alex");

        let (accepted, renamed) = apply_identity(&mut g, "a", Some("Zephyr".into()), None);
        assert_eq!(accepted, "Zephyr");
        assert_eq!(renamed.as_deref(), Some("L"));
        assert_eq!(seat_name(&g, "L", 0), "Zephyr");
        assert_eq!(g.clients["a"].display_name, "Zephyr");
    }

    #[test]
    fn renaming_onto_a_name_the_room_has_deduplicates() {
        let mut g = hub_inner();
        lobby(&mut g, "L", "Crash Bash");
        client(&mut g, "a", "Crash Bash", Some("L"));
        client(&mut g, "b", "Crash Bash", Some("L"));
        seat(&mut g, "L", 0, "a", "Alex");
        seat(&mut g, "L", 1, "b", "Marisa");

        let (accepted, renamed) = apply_identity(&mut g, "a", Some("Marisa".into()), None);
        assert_eq!(accepted, "Marisa (2)");
        assert_eq!(renamed.as_deref(), Some("L"));
        assert_eq!(seat_name(&g, "L", 0), "Marisa (2)");
        /* The hub row matches the seat, or one player reads as two names. */
        assert_eq!(g.clients["a"].display_name, "Marisa (2)");
        assert_eq!(seat_name(&g, "L", 1), "Marisa");
    }

    #[test]
    fn renaming_while_unseated_still_updates_the_hub_row() {
        let mut g = hub_inner();
        client(&mut g, "a", "Crash Bash", None);
        let (accepted, renamed) = apply_identity(&mut g, "a", Some("Zephyr".into()), None);
        assert_eq!(accepted, "Zephyr");
        assert!(renamed.is_none());
        assert_eq!(g.clients["a"].display_name, "Zephyr");
    }

    #[test]
    fn a_hello_that_renames_nothing_moves_nothing() {
        /* Re-sending the same name must not spam the room with updates. */
        let mut g = hub_inner();
        lobby(&mut g, "L", "Crash Bash");
        client(&mut g, "a", "Crash Bash", Some("L"));
        seat(&mut g, "L", 0, "a", "Alex");
        let (accepted, renamed) = apply_identity(&mut g, "a", Some("Alex".into()), None);
        assert_eq!(accepted, "Alex");
        assert!(renamed.is_none());
    }

    #[test]
    fn a_hello_carrying_only_a_title_leaves_the_name_alone() {
        let mut g = hub_inner();
        client(&mut g, "a", "", None);
        g.clients.get_mut("a").unwrap().display_name = "Alex".into();
        let (accepted, _) = apply_identity(&mut g, "a", None, Some("Crash Bash".into()));
        assert_eq!(accepted, "Alex");
        assert_eq!(g.clients["a"].game_name, "Crash Bash");
    }

    #[test]
    fn different_titles_do_not_share_a_room() {
        let mut g = hub_inner();
        client(&mut g, "a", "Crash Bash", None);
        client(&mut g, "b", "Gundam Wing", None);
        assert_ne!(game_scope_for(&g, "a"), game_scope_for(&g, "b"));
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
            automatch: false,
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

#[cfg(test)]
mod name_tests {
    use super::*;

    fn msg(json: &str) -> InMsg {
        serde_json::from_str(json).expect("parse")
    }

    #[test]
    fn control_characters_never_reach_a_peer() {
        /* The gap this closes: a name went to every peer as chat's `from`
         * while a chat LINE was control-stripped three functions away. */
        let mut m = msg(r#"{"op":"hello","display_name":"Ma\nri\tsa "}"#);
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(m.display_name.as_deref(), Some("Marisa"));
    }

    #[test]
    fn a_name_is_capped_in_characters_and_in_bytes() {
        let long = "a".repeat(200);
        let mut m = msg(&format!(r#"{{"op":"hello","display_name":"{long}"}}"#));
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(
            m.display_name.as_deref().unwrap().chars().count(),
            crate::names::NAME_MAX_CHARS
        );

        /* 32 four-byte characters is 128 bytes -- over the 64-byte field every
         * client stores this in, so the BYTE cap has to bind first. */
        let wide = "\u{1F600}".repeat(64);
        let mut m = msg(&format!(r#"{{"op":"hello","display_name":"{wide}"}}"#));
        assert_eq!(m.sanitize_in_place(), None);
        let got = m.display_name.as_deref().unwrap();
        assert!(got.len() <= crate::names::NAME_MAX_BYTES, "{} bytes", got.len());
        /* Cut on a character boundary, so no client truncates mid-sequence. */
        assert!(got.chars().all(|c| c == '\u{1F600}'));
    }

    #[test]
    fn a_name_that_trims_to_nothing_is_none() {
        let mut m = msg(r#"{"op":"hello","display_name":"  \t "}"#);
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(m.display_name, None);
    }

    #[test]
    fn a_profane_name_is_refused_not_masked() {
        let mut m = msg(r#"{"op":"hello","display_name":"fuck"}"#);
        assert_eq!(m.sanitize_in_place(), Some("name_rejected"));
        /* Refused at the boundary, so the caller answers `name_rejected` and
         * dispatches nothing: `hello`, `create` and `join` all carry a name,
         * and none of them may apply this one. */
        let mut m = msg(r#"{"op":"create","display_name":"sh1t","name":"Room"}"#);
        assert_eq!(m.sanitize_in_place(), Some("name_rejected"));
        let mut m = msg(r#"{"op":"join","display_name":"f.u.c.k"}"#);
        assert_eq!(m.sanitize_in_place(), Some("name_rejected"));
    }

    #[test]
    fn an_ordinary_name_passes_untouched() {
        let mut m =
            msg(r#"{"op":"hello","display_name":"Scunthorpe","game_name":"Crash Bash"}"#);
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(m.display_name.as_deref(), Some("Scunthorpe"));
        assert_eq!(m.game_name.as_deref(), Some("Crash Bash"));
    }

    #[test]
    fn a_room_title_is_refused_but_a_game_title_is_a_key() {
        /* A room title sits in the lobby browser in front of everyone
         * shopping for a game, so it is refused like a player name and the
         * host is asked to rename the room. */
        let mut m = msg(r#"{"op":"create","name":"fuck lobby","game_name":"Crash Bash"}"#);
        assert_eq!(m.sanitize_in_place(), Some("lobby_name_rejected"));

        /* A scoping key: clients match the lobby list and the server chat on
         * string equality, so touching it would split one game in two. */
        let mut m = msg(r#"{"op":"create","name":"Reimu's Room","game_name":"Crash Bash"}"#);
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(m.name.as_deref(), Some("Reimu's Room"));
        assert_eq!(m.game_name.as_deref(), Some("Crash Bash"));
    }

    #[test]
    fn a_password_is_validated_never_rewritten() {
        /* Dropping a character would leave the host holding a password that
         * is not the one they typed, so anything unusable is refused whole. */
        let mut m = msg(r#"{"op":"create","password":"hunter2 !@#$%^&*()"}"#);
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(m.password.as_deref(), Some("hunter2 !@#$%^&*()"));

        /* Never filtered: a password is never shown to anyone, and it is
         * salted and hashed before it is stored. */
        let mut m = msg(r#"{"op":"create","password":"fuck"}"#);
        assert_eq!(m.sanitize_in_place(), None);
        assert_eq!(m.password.as_deref(), Some("fuck"));

        let mut m = msg(r#"{"op":"join","password":"a\u0009b"}"#);
        assert_eq!(m.sanitize_in_place(), Some("password_invalid"));

        let long = "a".repeat(PASSWORD_MAX_BYTES + 1);
        let mut m = msg(&format!(r#"{{"op":"join","password":"{long}"}}"#));
        assert_eq!(m.sanitize_in_place(), Some("password_invalid"));
    }
}

#[cfg(test)]
mod list_scope_tests {
    use super::*;

    fn client(g: &mut HubInner, id: &str, name: &str, game: &str) {
        let (tx, _rx) = broadcast::channel(8);
        g.clients.insert(
            id.to_string(),
            ClientMeta {
                player_id: id.to_string(),
                display_name: name.to_string(),
                account: None,
                peer_ip: "127.0.0.1".into(),
                country: String::new(),
                game_name: game.to_string(),
                lobby_id: None,
                pending_mod_lobby: None,
                probe_rtt_ms: -1,
                tx,
            },
        );
    }

    /// The bug this exists to prevent: `list` was filtered by title, but the
    /// PUSHED list (the 1 Hz tick and the broadcast on every change) was not.
    /// A client asked a filtered question, got the right answer, and had it
    /// overwritten a second later by every player on the server. It looked
    /// like the filter did not work, when in fact the last word came from a
    /// path that never filtered at all -- which is why this asserts on the
    /// PUSHED payload and not on the request path.
    #[test]
    fn a_pushed_list_carries_only_the_recipients_own_title() {
        let mut g = HubInner::default();
        client(&mut g, "p1", "GundamPlayer", "Gundam Wing Endless Duel");
        client(&mut g, "p2", "YugiohPlayer", "Yu-Gi-Oh! Forbidden Memories");

        let seen = lobby_list_json_for(&g, "p1");
        assert!(seen.contains("GundamPlayer"));
        assert!(
            !seen.contains("YugiohPlayer"),
            "another title's player leaked into a pushed list: {seen}"
        );

        /* And symmetrically, so this is a scope rule rather than one title
         * happening to sort first. */
        let seen = lobby_list_json_for(&g, "p2");
        assert!(seen.contains("YugiohPlayer"));
        assert!(!seen.contains("GundamPlayer"));
    }

    /// A client that has not said what it is playing has not told us what to
    /// filter by. Showing it nothing would make the browser look empty rather
    /// than unfiltered, which is the worse failure of the two.
    #[test]
    fn a_client_with_no_title_yet_still_sees_everyone() {
        let mut g = HubInner::default();
        client(&mut g, "p1", "GundamPlayer", "Gundam Wing Endless Duel");
        client(&mut g, "p2", "Browsing", "");

        let seen = lobby_list_json_for(&g, "p2");
        assert!(seen.contains("GundamPlayer"));
        assert!(seen.contains("Browsing"));
    }
}

#[cfg(test)]
mod chat_ring_tests {
    use super::*;

    fn ring() -> HubInner {
        HubInner::default()
    }

    #[test]
    fn a_reported_id_resolves_to_the_servers_own_text() {
        // The point of the ring: a report names a message, and the server
        // supplies the words. Nothing the reporter sends becomes evidence.
        let mut g = ring();
        let mid = g.remember_chat("acc1", "Alice", "the line", "lobby", "L1", "G");
        let (line, _ctx) = g.chat_with_context(&mid).expect("should resolve");
        assert_eq!(line.text, "the line");
        assert_eq!(line.from_account, "acc1");
        assert_eq!(line.from_name, "Alice");
    }

    #[test]
    fn ids_are_not_reused_within_a_run() {
        let mut g = ring();
        let a = g.remember_chat("acc1", "A", "one", "lobby", "L1", "G");
        let b = g.remember_chat("acc1", "A", "two", "lobby", "L1", "G");
        assert_ne!(a, b);
        assert_eq!(g.chat_with_context(&a).unwrap().0.text, "one");
        assert_eq!(g.chat_with_context(&b).unwrap().0.text, "two");
    }

    #[test]
    fn an_unknown_id_resolves_to_nothing() {
        // What a report of a line that has scrolled away must hit: refused,
        // never guessed at.
        let g = ring();
        assert!(g.chat_with_context("nope").is_none());
    }

    #[test]
    fn context_is_the_preceding_lines_of_the_same_room() {
        let mut g = ring();
        g.remember_chat("a", "Alice", "first", "lobby", "L1", "G");
        // Another room entirely: must not leak into a record about L1.
        g.remember_chat("c", "Carol", "elsewhere", "lobby", "L2", "G");
        g.remember_chat("b", "Bob", "second", "lobby", "L1", "G");
        let mid = g.remember_chat("a", "Alice", "reported", "lobby", "L1", "G");

        let (_line, ctx) = g.chat_with_context(&mid).unwrap();
        assert!(ctx.contains("Alice: first"));
        assert!(ctx.contains("Bob: second"));
        assert!(
            !ctx.contains("elsewhere"),
            "another room's chat must not reach a moderation record: {ctx}"
        );
        assert!(
            !ctx.contains("reported"),
            "context is what came BEFORE, not the line itself"
        );
    }

    #[test]
    fn server_chat_and_lobby_chat_do_not_share_context() {
        let mut g = ring();
        g.remember_chat("a", "Alice", "in a room", "lobby", "L1", "G");
        let mid = g.remember_chat("b", "Bob", "in the server channel", "server", "", "G");
        let (_l, ctx) = g.chat_with_context(&mid).unwrap();
        assert!(!ctx.contains("in a room"), "{ctx}");
    }

    #[test]
    fn the_ring_is_bounded_and_old_lines_stop_being_reportable() {
        // Chat is not persisted, and a report is the only thing that promotes
        // a line out of memory. That has to stay true however busy a server
        // gets, so the oldest ids must genuinely go.
        let mut g = ring();
        let first = g.remember_chat("a", "A", "oldest", "lobby", "L1", "G");
        for i in 0..CHAT_RING_MAX {
            g.remember_chat("a", "A", &format!("line {i}"), "lobby", "L1", "G");
        }
        assert_eq!(g.chat_ring.len(), CHAT_RING_MAX);
        assert!(
            g.chat_with_context(&first).is_none(),
            "the oldest line should have aged out of the ring"
        );
    }
}

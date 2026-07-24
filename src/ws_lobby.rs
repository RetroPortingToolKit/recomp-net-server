//! WebSocket lobby protocol for recomp-net hosts (MotK / psxrecomp client).
//!
//! Wire format matches the client contract documented in `docs/WS_LOBBY.md`
//! (JSON text frames, `"op"` field).

use axum::extract::connect_info::ConnectInfo;
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
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::metrics;
use crate::AppState;

const MAX_SLOTS: usize = 5;
const MAX_LOBBIES: usize = 64;

#[derive(Clone)]
struct Slot {
    player_id: String,
    display_name: String,
    ready: bool,
}

#[derive(Clone)]
struct Lobby {
    lobby_id: String,
    name: String,
    game_name: String,
    /// Release / build pin (semver or tag). Peers must match to join.
    game_version: String,
    host_player_id: String,
    #[allow(dead_code)]
    host_bind: String,
    host_endpoint: String,
    guest_endpoint: String,
    password_hash: Option<[u8; 32]>,
    password_salt: Option<[u8; 16]>,
    max_slots: usize,
    session_id: u32,
    slots: Vec<Option<Slot>>,
    /* Host-authoritative sim-affecting settings (opaque JSON object). */
    match_caps: Option<Value>,
}

struct ClientMeta {
    #[allow(dead_code)]
    player_id: String,
    display_name: String,
    #[allow(dead_code)]
    peer_ip: String,
    lobby_id: Option<String>,
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

    pub async fn client_count(&self) -> usize {
        self.inner.lock().await.clients.len()
    }

    pub async fn lobby_count(&self) -> usize {
        self.inner.lock().await.lobbies.len()
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
}

pub fn ws_router() -> Router<AppState> {
    Router::new()
        .route("/", get(ws_upgrade))
        .route("/ws", get(ws_upgrade))
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let peer_ip = addr.ip().to_string();
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
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    max_slots: Option<u32>,
    #[serde(default)]
    host_bind: Option<String>,
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
    /// Host sim settings blob (aspect, turbo_loads, bios_hle, input_delay, …).
    #[serde(default)]
    match_caps: Option<Value>,
    #[serde(default)]
    slot: Option<usize>,
    /// Host slot move: source index (paired with `to_slot`).
    #[serde(default)]
    from_slot: Option<usize>,
    /// Host slot move: destination index (paired with `from_slot` or `slot`).
    #[serde(default)]
    to_slot: Option<usize>,
}

fn sanitize_match_caps(caps: Option<Value>) -> Option<Value> {
    let Some(v) = caps else {
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

#[derive(Serialize)]
struct LobbyListRow<'a> {
    lobby_id: &'a str,
    name: &'a str,
    game_name: &'a str,
    game_version: &'a str,
    player_count: usize,
    max_slots: usize,
    has_password: bool,
}

/// Normalize empty / missing version to `"dev"` (local builds).
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
        })
        .collect();
    json!({ "op": "lobby_list", "lobbies": rows }).to_string()
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

fn player_count(lobby: &Lobby) -> usize {
    lobby.slots.iter().filter(|s| s.is_some()).count()
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
                slots.push(json!({
                    "slot": i,
                    "player_id": slot.player_id,
                    "display_name": slot.display_name,
                    "ready": slot.ready,
                }));
            }
        }
        let all_ready = l.slots.iter().flatten().all(|s| s.ready)
            && player_count(l) >= 2;
        let mut msg = json!({
            "op": "lobby_update",
            "lobby_id": l.lobby_id,
            "session_id": l.session_id,
            "host_endpoint": l.host_endpoint,
            "guest_endpoint": l.guest_endpoint,
            "player_count": player_count(l),
            "max_slots": l.max_slots,
            "host_player_id": l.host_player_id,
            "all_ready": all_ready,
            "slots": slots,
        });
        if let Some(caps) = &l.match_caps {
            msg["match_caps"] = caps.clone();
        }
        let msg = msg.to_string();
        let members: Vec<String> = l
            .slots
            .iter()
            .filter_map(|s| s.as_ref().map(|x| x.player_id.clone()))
            .collect();
        (msg, members)
    };
    for m in members {
        send_to(hub, &m, msg.clone()).await;
    }
}

async fn destroy_lobby(hub: &WsLobbyHub, lobby_id: &str) {
    let members = {
        let mut g = hub.inner.lock().await;
        let Some(l) = g.lobbies.remove(lobby_id) else {
            return;
        };
        metrics::ws_lobby_destroyed();
        let members: Vec<String> = l
            .slots
            .iter()
            .filter_map(|s| s.as_ref().map(|x| x.player_id.clone()))
            .collect();
        for m in &members {
            if let Some(c) = g.clients.get_mut(m) {
                c.lobby_id = None;
            }
        }
        members
    };
    let note = json!({ "op": "lobby_closed", "lobby_id": lobby_id, "ok": true }).to_string();
    for m in members {
        send_to(hub, &m, note.clone()).await;
    }
    broadcast_list(hub).await;
}

async fn client_leave(hub: &WsLobbyHub, player_id: &str) {
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
            Some((lid, true))
        } else {
            if let Some(lobby) = g.lobbies.get_mut(&lid) {
                for s in lobby.slots.iter_mut() {
                    if s.as_ref().map(|x| x.player_id.as_str()) == Some(player_id) {
                        *s = None;
                    }
                }
                lobby.guest_endpoint.clear();
            }
            Some((lid, false))
        }
    };
    if let Some((lid, is_host)) = action {
        if is_host {
            destroy_lobby(hub, &lid).await;
        } else {
            emit_lobby_update(hub, &lid).await;
            broadcast_list(hub).await;
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
                peer_ip: peer_ip.clone(),
                lobby_id: None,
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
                if let Err(e) = handle_text(&hub, &player_id, &peer_ip, &text).await {
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
    client_leave(&hub, &player_id).await;
    {
        let mut g = hub.inner.lock().await;
        g.clients.remove(&player_id);
    }
    info!(%player_id, "ws lobby client disconnected");
    metrics::ws_disconnected();
}

async fn handle_text(
    hub: &WsLobbyHub,
    player_id: &str,
    peer_ip: &str,
    text: &str,
) -> Result<(), String> {
    let msg: InMsg = serde_json::from_str(text).map_err(|e| e.to_string())?;
    match msg.op.as_str() {
        "hello" => {
            if let Some(name) = msg.display_name.filter(|s| !s.is_empty()) {
                let mut g = hub.inner.lock().await;
                if let Some(c) = g.clients.get_mut(player_id) {
                    c.display_name = name;
                }
            }
            send_to(hub, player_id, json!({ "op": "hello_ok", "ok": true }).to_string()).await;
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
        "set_match_caps" => handle_set_match_caps(hub, player_id, msg).await?,
        "start" => handle_start(hub, player_id, msg).await?,
        "leave" => {
            client_leave(hub, player_id).await;
            send_to(hub, player_id, json!({ "op": "left", "ok": true }).to_string()).await;
        }
        "kick" => handle_kick(hub, player_id, msg).await?,
        "move" => handle_move(hub, player_id, msg).await?,
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
                destroy_lobby(hub, &lid).await;
            }
        }
        "signal" => handle_signal(hub, player_id, msg).await?,
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

async fn handle_get_turn_credentials(
    hub: &WsLobbyHub,
    player_id: &str,
) -> Result<(), String> {
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
    let host_bind = msg
        .host_bind
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0.0.0.0:7777".into());
    let max_slots = msg
        .max_slots
        .unwrap_or(2)
        .clamp(2, MAX_SLOTS as u32) as usize;

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
        });

        let match_caps = sanitize_match_caps(msg.match_caps);
        g.lobbies.insert(
            lobby_id.clone(),
            Lobby {
                lobby_id: lobby_id.clone(),
                name,
                game_name,
                game_version,
                host_player_id: player_id.to_string(),
                host_bind,
                host_endpoint: host_endpoint.clone(),
                guest_endpoint: String::new(),
                password_hash,
                password_salt,
                max_slots,
                session_id,
                slots,
                match_caps: match_caps.clone(),
            },
        );
        if let Some(c) = g.clients.get_mut(player_id) {
            c.lobby_id = Some(lobby_id.clone());
        }
        (lobby_id, session_id, host_endpoint, display_name, match_caps)
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
    let lobby_id = msg.lobby_id.filter(|s| !s.is_empty()).ok_or("missing lobby_id")?;
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

    let outcome = {
        let mut g = hub.inner.lock().await;
        if !g.lobbies.contains_key(&lobby_id) {
            SeatResult::Err("gone")
        } else {
            let (game_name, game_version) = {
                let lobby = g.lobbies.get(&lobby_id).unwrap();
                (lobby.game_name.clone(), lobby.game_version.clone())
            };
            if let Some(ref want) = join_game_name {
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
        SeatResult::Ok {
            slot,
            session_id,
            host_endpoint,
            guest_endpoint,
            match_caps,
        } => {
            let mut joined = json!({
                "op": "joined",
                "ok": true,
                "lobby_id": lobby_id,
                "session_id": session_id,
                "local_slot": slot,
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
            Ok(())
        }
    }
}

enum SeatResult {
    Err(&'static str),
    Ok {
        slot: usize,
        session_id: u32,
        host_endpoint: String,
        guest_endpoint: String,
        match_caps: Option<Value>,
    },
}

fn seat_joiner_locked(
    g: &mut HubInner,
    lobby_id: &str,
    player_id: &str,
    peer_ip: &str,
    guest_bind: &str,
    password: Option<&str>,
) -> SeatResult {
    let display_name = g
        .clients
        .get(player_id)
        .map(|c| c.display_name.clone())
        .unwrap_or_else(|| "Guest".into());

    let seated = {
        let lobby = match g.lobbies.get_mut(lobby_id) {
            Some(l) => l,
            None => return SeatResult::Err("gone"),
        };
        let pw_err = if let (Some(hash), Some(salt)) = (lobby.password_hash, lobby.password_salt)
        {
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
        if player_count(lobby) >= lobby.max_slots || lobby.slots.iter().all(|s| s.is_some()) {
            return SeatResult::Err("full");
        }
        let slot = lobby.slots.iter().position(|s| s.is_none()).unwrap();
        lobby.slots[slot] = Some(Slot {
            player_id: player_id.to_string(),
            display_name,
            ready: false,
        });
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        lobby.guest_endpoint = rewrite_endpoint(guest_bind, peer_ip);
        (
            slot,
            lobby.session_id,
            lobby.host_endpoint.clone(),
            lobby.guest_endpoint.clone(),
            lobby.match_caps.clone(),
        )
    };
    if let Some(c) = g.clients.get_mut(player_id) {
        c.lobby_id = Some(lobby_id.to_string());
    }
    SeatResult::Ok {
        slot: seated.0,
        session_id: seated.1,
        host_endpoint: seated.2,
        guest_endpoint: seated.3,
        match_caps: seated.4,
    }
}

async fn handle_kick(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let slot = msg.slot.ok_or("missing slot")?;
    struct KickOk {
        lid: String,
        victim: String,
    }
    let outcome: Option<KickOk> = {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g
            .clients
            .get(player_id)
            .and_then(|c| c.lobby_id.clone())
        else {
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
        if slot >= lobby.slots.len() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        let Some(victim) = lobby.slots[slot].as_ref().map(|s| s.player_id.clone()) else {
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
        lobby.slots[slot] = None;
        lobby.guest_endpoint.clear();
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        if let Some(c) = g.clients.get_mut(&victim) {
            c.lobby_id = None;
        }
        Some(KickOk { lid, victim })
    };
    if let Some(KickOk { lid, victim }) = outcome {
        send_to(
            hub,
            &victim,
            json!({ "op": "kicked", "ok": true, "lobby_id": lid }).to_string(),
        )
        .await;
        emit_lobby_update(hub, &lid).await;
        broadcast_list(hub).await;
    }
    Ok(())
}

/// Host-only: swap (or move into an empty) seat. Broadcasts `lobby_update`.
async fn handle_move(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
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
        let Some(lid) = g
            .clients
            .get(player_id)
            .and_then(|c| c.lobby_id.clone())
        else {
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
        if from >= lobby.slots.len() || to >= lobby.slots.len() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "bad_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        if lobby.slots[from].is_none() {
            drop(g);
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": "empty_slot", "ok": false }).to_string(),
            )
            .await;
            return Ok(());
        }
        lobby.slots.swap(from, to);
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        lid
    };
    emit_lobby_update(hub, &lid).await;
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
        let Some(lid) = g
            .clients
            .get(player_id)
            .and_then(|c| c.lobby_id.clone())
        else {
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

async fn handle_set_ready(
    hub: &WsLobbyHub,
    player_id: &str,
    msg: InMsg,
) -> Result<(), String> {
    let ready = msg.ready.unwrap_or(true);
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
                    for s in lobby.slots.iter_mut().flatten() {
                        if s.player_id == player_id {
                            s.ready = ready;
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

async fn handle_start(hub: &WsLobbyHub, player_id: &str, msg: InMsg) -> Result<(), String> {
    enum StartOut {
        Err(&'static str),
        Ok { msg: String, members: Vec<String> },
    }
    let fresh_caps = sanitize_match_caps(msg.match_caps);
    let outcome = 'out: {
        let mut g = hub.inner.lock().await;
        let Some(lid) = g
            .clients
            .get(player_id)
            .and_then(|c| c.lobby_id.clone())
        else {
            break 'out StartOut::Err("not_in_lobby");
        };
        {
            let Some(lobby) = g.lobbies.get(&lid) else {
                break 'out StartOut::Err("gone");
            };
            if lobby.host_player_id != player_id {
                break 'out StartOut::Err("not_host");
            }
            let n = player_count(lobby);
            if n < 2 {
                break 'out StartOut::Err("need_players");
            }
            /* Host Start Lobby is the launch authority. Ready flags remain for
             * lobby_update / UI, but must not block start when the client has
             * no Ready toggle (and rematch clears ready on soft-return). */
            if lobby.host_endpoint.is_empty() || lobby.guest_endpoint.is_empty() {
                /* Guest never completed join endpoint rewrite — refuse rather
                 * than launch into a HELLO hang (host peer would be empty). */
                break 'out StartOut::Err("missing_endpoints");
            }
        }
        /* Fresh session_id per match so rematch UDP HELLO/BYE cannot be
         * confused with packets from the previous delay-sync session. */
        let sid = g.next_session;
        g.next_session = g.next_session.saturating_add(1);
        let Some(lobby) = g.lobbies.get_mut(&lid) else {
            break 'out StartOut::Err("gone");
        };
        if let Some(caps) = fresh_caps {
            lobby.match_caps = Some(caps);
        }
        lobby.session_id = sid;
        /* Match start clears ready so a return-to-lobby rematch must re-confirm. */
        for s in lobby.slots.iter_mut().flatten() {
            s.ready = false;
        }
        let n = player_count(lobby);
        let mut slots = Vec::new();
        for (i, s) in lobby.slots.iter().enumerate() {
            if let Some(slot) = s {
                slots.push(json!({
                    "slot": i,
                    "player_id": slot.player_id,
                    "display_name": slot.display_name,
                    "ready": slot.ready,
                }));
            }
        }
        let members: Vec<String> = lobby
            .slots
            .iter()
            .filter_map(|s| s.as_ref().map(|x| x.player_id.clone()))
            .collect();
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
        });
        if let Some(caps) = &lobby.match_caps {
            launch["match_caps"] = caps.clone();
        }
        StartOut::Ok {
            msg: launch.to_string(),
            members,
        }
    };
    match outcome {
        StartOut::Err(code) => {
            send_to(
                hub,
                player_id,
                json!({ "op": "error", "code": code, "ok": false }).to_string(),
            )
            .await;
        }
        StartOut::Ok { msg, members } => {
            for m in members {
                send_to(hub, &m, msg.clone()).await;
            }
            metrics::ws_lobby_started();
        }
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
            .slots
            .iter()
            .filter_map(|s| s.as_ref())
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

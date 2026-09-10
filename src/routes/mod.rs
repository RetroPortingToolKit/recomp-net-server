//! HTTP routes for lobby, players, ICE signal relay, and TURN credentials.

use crate::metrics;
use crate::players;
use crate::rooms::{RnetBootstrap, Room, RoomError, RoomRegistry};
use crate::signal::SignalEnvelope;
use crate::turn_credentials;
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub fn api_router() -> Router<AppState> {
    Router::new()
        .route("/v1/players", post(create_player))
        .route("/v1/games", get(list_games))
        .route("/v1/rooms", get(list_rooms).post(create_room))
        .route("/v1/rooms/{room_id}", get(get_room))
        .route("/v1/rooms/{room_id}/join", post(join_room))
        .route("/v1/rooms/{room_id}/leave", post(leave_room))
        .route("/v1/rooms/{room_id}/ready", post(set_ready))
        .route("/v1/rooms/{room_id}/heartbeat", post(heartbeat))
        .route("/v1/rooms/{room_id}/start", post(mark_running))
        .route("/v1/rooms/{room_id}/signal", post(post_signal))
        .route("/v1/rooms/{room_id}/signals", get(get_signals))
        .route("/v1/turn-credentials", get(turn_creds))
        /* Discord login. Three endpoints, no cookies and no browser session on
         * our side: the pairing code carries the whole flow. */
        .route("/auth/discord/start", post(discord_start))
        .route("/auth/discord/callback", get(discord_callback))
        .route("/auth/discord/poll", post(discord_poll))
        /* The browserless path: a device trades its stored key for a session.
         * This is what a handheld or a console does at startup, and what a PC
         * does on every later launch instead of signing in again. */
        .route("/auth/challenge", post(auth_challenge))
        .route("/auth/session", post(session_from_secret))
        .route("/auth/secret/revoke", post(revoke_secret))
        .route("/auth/handle", post(set_handle))
}

#[derive(Deserialize)]
struct ChallengeReq {
    /* Read by nobody, on purpose. The nonce does not depend on who is asking,
     * and looking the player up here would turn this into an oracle for
     * whether an account exists. The field stays because it documents what a
     * client sends and because a later rate-limit will want it. */
    #[allow(dead_code)]
    player_id: String,
}

#[derive(Serialize)]
struct ChallengeIssued {
    nonce: String,
    /// Seconds. The device has this long to answer before the nonce is dead.
    expires_in: u64,
}

/// Step one of every device authentication: get a nonce to prove against.
///
/// Deliberately unauthenticated and deliberately uninformative — it answers
/// the same way for a real player id and a made-up one, so it is not a way to
/// ask whether an account exists.
async fn auth_challenge(
    State(state): State<AppState>,
    Json(_req): Json<ChallengeReq>,
) -> Json<ChallengeIssued> {
    Json(ChallengeIssued {
        nonce: state.discord_challenges.issue().await,
        expires_in: 60,
    })
}

#[derive(Deserialize)]
struct ProofReq {
    player_id: String,
    nonce: String,
    /// HMAC-SHA256 over the nonce, keyed by SHA-256 of the device's key. The
    /// key itself is never sent.
    proof: String,
    /// Revoke every key this player holds, not just the proving one. The "I
    /// lost a device and cannot remember which key was on it" case.
    #[serde(default)]
    all: bool,
}

fn parse_player(id: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(id).map_err(|_| ApiError::new(StatusCode::FORBIDDEN, "invalid_secret"))
}

/// Spend the nonce first, whatever happens next. A nonce that survived a
/// failed proof would let an attacker grind guesses against one challenge.
async fn spend_nonce(state: &AppState, nonce: &str) -> Result<(), ApiError> {
    if state.discord_challenges.take(nonce).await {
        Ok(())
    } else {
        Err(ApiError::new(StatusCode::FORBIDDEN, "bad_nonce"))
    }
}

#[derive(Serialize)]
struct SessionIssued {
    session: String,
    player_id: String,
    handle: String,
    discord_username: String,
}

/// Proof in, short-lived session out. This is what a browserless device does
/// at startup, and what a PC does on later launches instead of signing in
/// again. The key stays on the device.
async fn session_from_secret(
    State(state): State<AppState>,
    Json(req): Json<ProofReq>,
) -> Result<Json<SessionIssued>, ApiError> {
    let uuid = parse_player(&req.player_id)?;
    spend_nonce(&state, &req.nonce).await?;
    let player = crate::secrets::verify_proof(&state.pool, &uuid, &req.nonce, &req.proof)
        .await
        .map_err(|_| ApiError::new(StatusCode::FORBIDDEN, "invalid_secret"))?;
    let session = crate::auth::issue_session_token(&state.config, &player.id)
        .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(Json(SessionIssued {
        session,
        player_id: player.id.to_string(),
        handle: player.handle,
        discord_username: player.discord_username,
    }))
}

#[derive(Deserialize)]
struct HandleReq {
    player_id: String,
    nonce: String,
    proof: String,
    handle: String,
}

/// Change the presentational handle, proved by a device key.
///
/// Refused when the name trips the word list -- and here refusing is right,
/// because the player typed this one and can type another. That is the mirror
/// of `identity::default_handle_for`, which must NOT refuse, since a Discord
/// name is not something the player can fix from inside the game.
async fn set_handle(
    State(state): State<AppState>,
    Json(req): Json<HandleReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let uuid = parse_player(&req.player_id)?;
    spend_nonce(&state, &req.nonce).await?;
    crate::secrets::verify_proof(&state.pool, &uuid, &req.nonce, &req.proof)
        .await
        .map_err(|_| ApiError::new(StatusCode::FORBIDDEN, "invalid_secret"))?;
    let handle = crate::identity::set_handle(&state.pool, uuid, &req.handle)
        .await
        .map_err(|_| ApiError::new(StatusCode::CONFLICT, "handle_rejected"))?;
    Ok(Json(serde_json::json!({ "handle": handle })))
}

/// Retire a key, proved by that key, so a device can always sign itself out
/// without a browser or a session first.
async fn revoke_secret(
    State(state): State<AppState>,
    Json(req): Json<ProofReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let uuid = parse_player(&req.player_id)?;
    spend_nonce(&state, &req.nonce).await?;
    if req.all {
        /* Prove first: "forget every device" must not be something an
         * onlooker can trigger with a player id alone. */
        crate::secrets::verify_proof(&state.pool, &uuid, &req.nonce, &req.proof)
            .await
            .map_err(|_| ApiError::new(StatusCode::FORBIDDEN, "invalid_secret"))?;
        let n = crate::secrets::revoke_all(&state.pool, &uuid)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        return Ok(Json(serde_json::json!({ "revoked": n })));
    }
    let ok = crate::secrets::revoke_by_proof(&state.pool, &uuid, &req.nonce, &req.proof)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(Json(serde_json::json!({ "revoked": if ok { 1 } else { 0 } })))
}

// ---- Discord login ---------------------------------------------------------

#[derive(Serialize)]
struct DiscordStart {
    /// Opaque pairing code. The launcher keeps it and polls with it; it is also
    /// the OAuth `state`, which is how the callback finds this login.
    code: String,
    /// The URL the launcher opens in the player's browser.
    url: String,
}

/// Begin a login. 503 when the operator has not configured Discord, which is
/// also how a launcher discovers that this server does not offer logins.
async fn discord_start(State(state): State<AppState>) -> Result<Json<DiscordStart>, ApiError> {
    let code = state.discord_logins.begin().await;
    let url = crate::discord_auth::authorize_url(&state.config, &code)
        .map_err(|e| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok(Json(DiscordStart { code, url }))
}

#[derive(Deserialize)]
struct DiscordCallback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

/// Where Discord sends the player's browser. The launcher is not here — it is
/// polling — so this returns a small page for a human to read and close.
async fn discord_callback(
    State(state): State<AppState>,
    Query(q): Query<DiscordCallback>,
) -> axum::response::Html<String> {
    let pairing = q.state.unwrap_or_default();
    if let Some(err) = q.error {
        /* The player pressed Cancel, or Discord refused. Park it so the
         * launcher stops polling instead of spinning until the TTL. */
        state.discord_logins_finish_err(&pairing, &err).await;
        return page("Login cancelled", "You can close this window and try again.");
    }
    let Some(code) = q.code else {
        state
            .discord_logins_finish_err(&pairing, "no_code")
            .await;
        return page("Login failed", "Discord did not return a code.");
    };
    match crate::discord_auth::on_callback(
        &state.config,
        &state.pool,
        &state.http,
        &state.discord_logins,
        &pairing,
        &code,
    )
    .await
    {
        Ok(done) => page(
            "Signed in",
            &format!("Welcome, {}. You can close this window and return to the game.",
                     html_escape(&done.handle)),
        ),
        Err(e) if e == "not_in_guild" => page(
            "Not a member",
            "This server is for members of our Discord only. Join it, then sign in again.",
        ),
        Err(e) => {
            /* The operator needs the whole chain, and gets it in the log. The
             * player gets the short code in front of it, which is enough to
             * quote in a bug report and gives away nothing. A login that fails
             * with neither is the hardest kind of problem to act on -- this
             * one reached "Something went wrong" with no log line at all. */
            tracing::warn!(error = %e, "discord login failed");
            let code = e.split(':').next().unwrap_or("unknown");
            page(
                "Login failed",
                &format!(
                    "Sign-in could not be completed ({}). Close this window and                      try again — if it keeps happening, the server operator can                      see the reason in the lobby server log.",
                    html_escape(code)
                ),
            )
        }
    }
}

/// The launcher asks "is it done yet?". 202 = still waiting.
#[derive(Deserialize)]
struct DiscordPollReq {
    code: String,
}

async fn discord_poll(
    State(state): State<AppState>,
    Json(req): Json<DiscordPollReq>,
) -> Result<Json<crate::discord_auth::Completed>, ApiError> {
    match state.discord_logins.take(&req.code).await {
        Some(Ok(done)) => Ok(Json(done)),
        Some(Err(e)) => Err(ApiError::new(StatusCode::FORBIDDEN, e)),
        None => Err(ApiError::new(StatusCode::ACCEPTED, "pending")),
    }
}

/// The callback's replies are read by a person in a browser, not by the game,
/// so they are a page rather than JSON.
fn page(title: &str, body: &str) -> axum::response::Html<String> {
    axum::response::Html(format!(
        "<!doctype html><meta charset=utf-8><title>{t}</title>\
         <body style=\"font:16px/1.5 system-ui;margin:4rem auto;max-width:32rem;\
         color:#e8e8ea;background:#0a0d16\"><h1 style=\"font-size:1.3rem\">{t}</h1>\
         <p>{b}</p></body>",
        t = html_escape(title),
        b = html_escape(body)
    ))
}

/// The handle comes from a Discord display name, i.e. from a person, so it is
/// escaped before it goes into a page. Everything else here is a literal.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[derive(Serialize)]
struct PlayerCreated {
    player_id: Uuid,
    api_token: String,
}

async fn create_player(State(state): State<AppState>) -> Result<Json<PlayerCreated>, ApiError> {
    let (player_id, api_token) = players::create_player(&state.pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    metrics::http_player_created();
    Ok(Json(PlayerCreated {
        player_id,
        api_token,
    }))
}

#[derive(Serialize)]
struct GamesResponse {
    mode: &'static str,
    games: Vec<String>,
}

async fn list_games(State(state): State<AppState>) -> Json<GamesResponse> {
    if state.config.game_allowlist.is_empty() {
        Json(GamesResponse {
            mode: "open",
            games: vec![],
        })
    } else {
        Json(GamesResponse {
            mode: "allowlist",
            games: state.config.game_allowlist.clone(),
        })
    }
}

#[derive(Deserialize)]
struct ListRoomsQuery {
    game_id: String,
    client_version: Option<String>,
}

async fn list_rooms(
    State(state): State<AppState>,
    Query(q): Query<ListRoomsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let rooms = state.rooms.lock().await;
    let list = rooms.list_public(&q.game_id, q.client_version.as_deref());
    Ok(Json(serde_json::json!({ "rooms": list })))
}

#[derive(Deserialize)]
struct CreateRoomBody {
    game_id: String,
    client_version: String,
    #[serde(default)]
    display_name: String,
    slot_count: Option<u8>,
    input_delay: Option<u8>,
    #[serde(default)]
    is_private: bool,
}

async fn create_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateRoomBody>,
) -> Result<(StatusCode, Json<RoomView>), ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    if !state.config.game_allowed(&body.game_id) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "game_id not allowed",
        ));
    }
    if body.game_id.is_empty() || body.client_version.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "game_id and client_version are required",
        ));
    }

    let slot_count = body.slot_count.unwrap_or(state.config.default_slot_count);
    let input_delay = body.input_delay.unwrap_or(state.config.default_input_delay);
    let display_name = if body.display_name.trim().is_empty() {
        "lobby".to_string()
    } else {
        body.display_name.trim().to_string()
    };

    let mut rooms = state.rooms.lock().await;
    let room = rooms
        .create(
            player_id,
            body.game_id,
            body.client_version,
            display_name,
            slot_count,
            input_delay,
            state.config.protocol_magic,
            body.is_private,
        )
        .map_err(ApiError::from)?;
    metrics::http_room_created();

    Ok((
        StatusCode::CREATED,
        Json(RoomView::from_room(&room, player_id)),
    ))
}

async fn get_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
) -> Result<Json<RoomView>, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    let rooms = state.rooms.lock().await;
    let room = rooms
        .get(&room_id)
        .ok_or(ApiError::from(RoomError::NotFound))?;
    if !room.members.iter().any(|m| m.player_id == player_id) {
        return Err(ApiError::from(RoomError::NotMember));
    }
    Ok(Json(RoomView::from_room(room, player_id)))
}

#[derive(Deserialize)]
struct JoinBody {
    game_id: String,
    client_version: String,
    join_code: Option<String>,
}

async fn join_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
    Json(body): Json<JoinBody>,
) -> Result<Json<RoomView>, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    let mut rooms = state.rooms.lock().await;
    let room = match rooms.join(
        &room_id,
        player_id,
        &body.game_id,
        &body.client_version,
        body.join_code.as_deref(),
    ) {
        Ok(room) => {
            metrics::http_room_join_ok();
            room
        }
        Err(e) => {
            metrics::http_room_join_fail(metrics::room_error_code(&e));
            return Err(ApiError::from(e));
        }
    };
    Ok(Json(RoomView::from_room(&room, player_id)))
}

async fn leave_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    {
        let mut rooms = state.rooms.lock().await;
        rooms.leave(&room_id, player_id).map_err(ApiError::from)?;
    }
    state.signals.lock().await.clear_player(player_id);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ReadyBody {
    ready: bool,
}

async fn set_ready(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
    Json(body): Json<ReadyBody>,
) -> Result<Json<RoomView>, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    let mut rooms = state.rooms.lock().await;
    let room = rooms
        .set_ready(&room_id, player_id, body.ready)
        .map_err(ApiError::from)?;
    Ok(Json(RoomView::from_room(&room, player_id)))
}

async fn heartbeat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    let mut rooms = state.rooms.lock().await;
    rooms
        .heartbeat(&room_id, player_id)
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn mark_running(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
) -> Result<Json<RoomView>, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    let mut rooms = state.rooms.lock().await;
    let room = rooms
        .mark_running(&room_id, player_id)
        .map_err(ApiError::from)?;
    metrics::http_room_started(&room.game_id, room.members.len());
    Ok(Json(RoomView::from_room(&room, player_id)))
}

#[derive(Deserialize)]
struct SignalBody {
    target_player_id: Option<Uuid>,
    #[serde(rename = "type")]
    signal_type: u8,
    #[serde(default)]
    flag: u8,
    text: String,
}

async fn post_signal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
    Json(body): Json<SignalBody>,
) -> Result<StatusCode, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    if body.text.len() > 2047 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "signal text exceeds 2047 characters",
        ));
    }

    let targets = {
        let rooms = state.rooms.lock().await;
        let room = rooms
            .get(&room_id)
            .ok_or(ApiError::from(RoomError::NotFound))?;
        if !room.members.iter().any(|m| m.player_id == player_id) {
            return Err(ApiError::from(RoomError::NotMember));
        }
        match body.target_player_id {
            Some(t) => {
                if !room.members.iter().any(|m| m.player_id == t) {
                    return Err(ApiError::new(StatusCode::BAD_REQUEST, "target not in room"));
                }
                vec![t]
            }
            None => room
                .members
                .iter()
                .map(|m| m.player_id)
                .filter(|id| *id != player_id)
                .collect(),
        }
    };

    let envelope = SignalEnvelope {
        from_player_id: player_id,
        room_id,
        signal_type: body.signal_type,
        flag: body.flag,
        text: body.text,
        created_at: Utc::now(),
    };

    let mut signals = state.signals.lock().await;
    for t in targets {
        signals.push_to(t, envelope.clone());
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn get_signals(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    {
        let rooms = state.rooms.lock().await;
        let room = rooms
            .get(&room_id)
            .ok_or(ApiError::from(RoomError::NotFound))?;
        if !room.members.iter().any(|m| m.player_id == player_id) {
            return Err(ApiError::from(RoomError::NotMember));
        }
    }
    let mut signals = state.signals.lock().await;
    let drained = signals.drain(player_id);
    let (for_room, other): (Vec<_>, Vec<_>) =
        drained.into_iter().partition(|s| s.room_id == room_id);
    for s in other {
        signals.push_to(player_id, s);
    }
    Ok(Json(serde_json::json!({ "signals": for_room })))
}

#[derive(Serialize)]
struct TurnCredsResponse {
    stun_host: String,
    stun_port: u16,
    turn_host: String,
    turn_port: u16,
    turns_port: u16,
    realm: String,
    username: String,
    password: String,
    ttl_secs: u64,
}

async fn turn_creds(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TurnCredsResponse>, ApiError> {
    let player_id = require_auth(&state, &headers).await?;
    let cfg = turn_credentials::require_config()
        .map_err(|e| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    let (username, password) = turn_credentials::issue_credentials(&cfg, &player_id)
        .map_err(|e| ApiError::internal(e.to_string()))?;
    metrics::http_turn_credentials_issued();
    Ok(Json(TurnCredsResponse {
        stun_host: cfg.stun_host,
        stun_port: cfg.stun_port,
        turn_host: cfg.turn_host,
        turn_port: cfg.turn_port,
        turns_port: cfg.turns_port,
        realm: cfg.realm,
        username,
        password,
        ttl_secs: cfg.ttl_secs,
    }))
}

#[derive(Serialize)]
struct RoomView {
    room_id: Uuid,
    game_id: String,
    client_version: String,
    display_name: String,
    slot_count: u8,
    input_delay: u8,
    is_private: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    join_code: Option<String>,
    status: crate::rooms::RoomStatus,
    host_player_id: Uuid,
    members: Vec<crate::rooms::Member>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rnet: Option<RnetBootstrap>,
}

impl RoomView {
    fn from_room(room: &Room, viewer: Uuid) -> Self {
        let join_code = if room.host_player_id == viewer {
            room.join_code.clone()
        } else {
            None
        };
        Self {
            room_id: room.room_id,
            game_id: room.game_id.clone(),
            client_version: room.client_version.clone(),
            display_name: room.display_name.clone(),
            slot_count: room.slot_count,
            input_delay: room.input_delay,
            is_private: room.is_private,
            join_code,
            status: room.status,
            host_player_id: room.host_player_id,
            members: room.members.clone(),
            rnet: RoomRegistry::bootstrap_for(room, viewer),
        }
    }
}

async fn require_auth(state: &AppState, headers: &HeaderMap) -> Result<Uuid, ApiError> {
    let player_id = headers
        .get("X-Player-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing X-Player-Id"))?;

    let auth = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing Authorization"))?;

    let token = auth
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "expected Bearer token"))?;

    players::require_player(&state.pool, &player_id, token)
        .await
        .map_err(|_| ApiError::new(StatusCode::UNAUTHORIZED, "invalid credentials"))?;

    Ok(player_id)
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

impl From<RoomError> for ApiError {
    fn from(e: RoomError) -> Self {
        let status = match e {
            RoomError::NotFound => StatusCode::NOT_FOUND,
            RoomError::Full => StatusCode::CONFLICT,
            RoomError::NotJoinable => StatusCode::CONFLICT,
            RoomError::BadJoinCode => StatusCode::FORBIDDEN,
            RoomError::GameMismatch | RoomError::VersionMismatch => StatusCode::CONFLICT,
            RoomError::AlreadyJoined => StatusCode::CONFLICT,
            RoomError::NotMember => StatusCode::FORBIDDEN,
            RoomError::NotHost => StatusCode::FORBIDDEN,
            RoomError::BadSlotCount | RoomError::GameNotAllowed => StatusCode::BAD_REQUEST,
        };
        Self::new(status, e.to_string())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

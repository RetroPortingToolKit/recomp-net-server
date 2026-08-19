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

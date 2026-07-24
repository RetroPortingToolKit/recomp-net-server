//! In-memory game-filtered lobbies and RNetConfig bootstrap.

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomStatus {
    Open,
    Starting,
    Running,
    Closed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Member {
    pub player_id: Uuid,
    pub local_slot: u8,
    pub ready: bool,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RnetBootstrap {
    pub session_id: u32,
    pub protocol_magic: u32,
    pub slot_count: u8,
    pub input_delay: u8,
    pub local_slot: u8,
    pub you_are_sim_authority: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Room {
    pub room_id: Uuid,
    pub game_id: String,
    pub client_version: String,
    pub display_name: String,
    pub slot_count: u8,
    pub input_delay: u8,
    pub protocol_magic: u32,
    pub session_id: Option<u32>,
    pub is_private: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_code: Option<String>,
    pub status: RoomStatus,
    pub host_player_id: Uuid,
    pub members: Vec<Member>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoomSummary {
    pub room_id: Uuid,
    pub game_id: String,
    pub client_version: String,
    pub display_name: String,
    pub slot_count: u8,
    pub member_count: usize,
    pub input_delay: u8,
    pub is_private: bool,
    pub status: RoomStatus,
}

#[derive(Debug, Error)]
pub enum RoomError {
    #[error("room not found")]
    NotFound,
    #[error("room is full")]
    Full,
    #[error("room is not joinable")]
    NotJoinable,
    #[error("invalid join code")]
    BadJoinCode,
    #[error("game_id mismatch")]
    GameMismatch,
    #[error("client_version mismatch")]
    VersionMismatch,
    #[error("player already in room")]
    AlreadyJoined,
    #[error("player not in room")]
    NotMember,
    #[error("not room host")]
    NotHost,
    #[error("invalid slot_count")]
    BadSlotCount,
    #[error("game_id not allowed")]
    GameNotAllowed,
}

#[derive(Default)]
pub struct RoomRegistry {
    rooms: HashMap<Uuid, Room>,
}

impl RoomRegistry {
    pub fn create(
        &mut self,
        host: Uuid,
        game_id: String,
        client_version: String,
        display_name: String,
        slot_count: u8,
        input_delay: u8,
        protocol_magic: u32,
        is_private: bool,
    ) -> Result<Room, RoomError> {
        if !(2..=5).contains(&slot_count) {
            return Err(RoomError::BadSlotCount);
        }
        let now = Utc::now();
        let room_id = Uuid::new_v4();
        let join_code = if is_private {
            Some(generate_join_code())
        } else {
            None
        };
        let room = Room {
            room_id,
            game_id,
            client_version,
            display_name,
            slot_count,
            input_delay,
            protocol_magic,
            session_id: None,
            is_private,
            join_code,
            status: RoomStatus::Open,
            host_player_id: host,
            members: vec![Member {
                player_id: host,
                local_slot: 0,
                ready: false,
                last_heartbeat: now,
            }],
            created_at: now,
            updated_at: now,
        };
        self.rooms.insert(room_id, room.clone());
        Ok(room)
    }

    pub fn get(&self, room_id: &Uuid) -> Option<&Room> {
        self.rooms.get(room_id)
    }

    pub fn list_public(&self, game_id: &str, client_version: Option<&str>) -> Vec<RoomSummary> {
        self.rooms
            .values()
            .filter(|r| {
                r.status == RoomStatus::Open
                    && !r.is_private
                    && r.game_id == game_id
                    && client_version
                        .map(|v| v == r.client_version)
                        .unwrap_or(true)
                    && r.members.len() < r.slot_count as usize
            })
            .map(RoomSummary::from)
            .collect()
    }

    pub fn join(
        &mut self,
        room_id: &Uuid,
        player_id: Uuid,
        game_id: &str,
        client_version: &str,
        join_code: Option<&str>,
    ) -> Result<Room, RoomError> {
        let room = self.rooms.get_mut(room_id).ok_or(RoomError::NotFound)?;
        if room.status != RoomStatus::Open {
            return Err(RoomError::NotJoinable);
        }
        if room.game_id != game_id {
            return Err(RoomError::GameMismatch);
        }
        if room.client_version != client_version {
            return Err(RoomError::VersionMismatch);
        }
        if room.is_private {
            match (&room.join_code, join_code) {
                (Some(code), Some(provided)) if code == provided => {}
                _ => return Err(RoomError::BadJoinCode),
            }
        }
        if room.members.iter().any(|m| m.player_id == player_id) {
            return Err(RoomError::AlreadyJoined);
        }
        if room.members.len() >= room.slot_count as usize {
            return Err(RoomError::Full);
        }

        let used: Vec<u8> = room.members.iter().map(|m| m.local_slot).collect();
        let local_slot = (0..room.slot_count)
            .find(|s| !used.contains(s))
            .ok_or(RoomError::Full)?;

        let now = Utc::now();
        room.members.push(Member {
            player_id,
            local_slot,
            ready: false,
            last_heartbeat: now,
        });
        room.updated_at = now;
        Ok(room.clone())
    }

    pub fn leave(&mut self, room_id: &Uuid, player_id: Uuid) -> Result<Option<Room>, RoomError> {
        let room = self.rooms.get_mut(room_id).ok_or(RoomError::NotFound)?;
        if !room.members.iter().any(|m| m.player_id == player_id) {
            return Err(RoomError::NotMember);
        }

        // Host leave dissolves the room.
        if room.host_player_id == player_id {
            room.status = RoomStatus::Closed;
            let closed = room.clone();
            self.rooms.remove(room_id);
            return Ok(Some(closed));
        }

        room.members.retain(|m| m.player_id != player_id);
        room.updated_at = Utc::now();
        // Leaving resets ready barrier.
        for m in &mut room.members {
            m.ready = false;
        }
        if room.status == RoomStatus::Starting {
            room.status = RoomStatus::Open;
            room.session_id = None;
        }
        Ok(Some(room.clone()))
    }

    pub fn set_ready(
        &mut self,
        room_id: &Uuid,
        player_id: Uuid,
        ready: bool,
    ) -> Result<Room, RoomError> {
        let room = self.rooms.get_mut(room_id).ok_or(RoomError::NotFound)?;
        if room.status != RoomStatus::Open && room.status != RoomStatus::Starting {
            return Err(RoomError::NotJoinable);
        }
        let member = room
            .members
            .iter_mut()
            .find(|m| m.player_id == player_id)
            .ok_or(RoomError::NotMember)?;
        member.ready = ready;
        member.last_heartbeat = Utc::now();
        room.updated_at = Utc::now();

        let all_ready = room.members.len() == room.slot_count as usize
            && room.members.iter().all(|m| m.ready);

        if all_ready {
            if room.session_id.is_none() {
                room.session_id = Some(rand::thread_rng().next_u32());
            }
            room.status = RoomStatus::Starting;
        } else if room.status == RoomStatus::Starting {
            room.status = RoomStatus::Open;
            room.session_id = None;
        }

        Ok(room.clone())
    }

    pub fn heartbeat(&mut self, room_id: &Uuid, player_id: Uuid) -> Result<(), RoomError> {
        let room = self.rooms.get_mut(room_id).ok_or(RoomError::NotFound)?;
        let member = room
            .members
            .iter_mut()
            .find(|m| m.player_id == player_id)
            .ok_or(RoomError::NotMember)?;
        member.last_heartbeat = Utc::now();
        room.updated_at = Utc::now();
        Ok(())
    }

    pub fn mark_running(&mut self, room_id: &Uuid, player_id: Uuid) -> Result<Room, RoomError> {
        let room = self.rooms.get_mut(room_id).ok_or(RoomError::NotFound)?;
        if room.host_player_id != player_id {
            return Err(RoomError::NotHost);
        }
        if room.status != RoomStatus::Starting {
            return Err(RoomError::NotJoinable);
        }
        room.status = RoomStatus::Running;
        room.updated_at = Utc::now();
        Ok(room.clone())
    }

    pub fn bootstrap_for(room: &Room, player_id: Uuid) -> Option<RnetBootstrap> {
        let session_id = room.session_id?;
        let member = room.members.iter().find(|m| m.player_id == player_id)?;
        Some(RnetBootstrap {
            session_id,
            protocol_magic: room.protocol_magic,
            slot_count: room.slot_count,
            input_delay: room.input_delay,
            local_slot: member.local_slot,
            you_are_sim_authority: member.local_slot == 0,
        })
    }

    pub fn purge_stale(&mut self, heartbeat_timeout_secs: u64, idle_secs: u64) {
        let now = Utc::now();
        let hb = chrono::Duration::seconds(heartbeat_timeout_secs as i64);
        let idle = chrono::Duration::seconds(idle_secs as i64);

        let mut remove = Vec::new();
        for (id, room) in self.rooms.iter_mut() {
            room.members
                .retain(|m| now.signed_duration_since(m.last_heartbeat) <= hb);
            if room.members.is_empty()
                || now.signed_duration_since(room.updated_at) > idle
                || !room.members.iter().any(|m| m.player_id == room.host_player_id)
            {
                remove.push(*id);
            }
        }
        for id in remove {
            self.rooms.remove(&id);
        }
    }

    pub fn member_ids(&self, room_id: &Uuid) -> Vec<Uuid> {
        self.rooms
            .get(room_id)
            .map(|r| r.members.iter().map(|m| m.player_id).collect())
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.rooms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }

    /// Live room counts keyed by `game_id` (for `/stats` only).
    pub fn counts_by_game(&self) -> BTreeMap<String, usize> {
        let mut out = BTreeMap::new();
        for room in self.rooms.values() {
            *out.entry(room.game_id.clone()).or_insert(0) += 1;
        }
        out
    }
}

impl From<&Room> for RoomSummary {
    fn from(r: &Room) -> Self {
        Self {
            room_id: r.room_id,
            game_id: r.game_id.clone(),
            client_version: r.client_version.clone(),
            display_name: r.display_name.clone(),
            slot_count: r.slot_count,
            member_count: r.members.len(),
            input_delay: r.input_delay,
            is_private: r.is_private,
            status: r.status,
        }
    }
}

fn generate_join_code() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::thread_rng();
    (0..6)
        .map(|_| {
            let i = (rng.next_u32() as usize) % ALPHABET.len();
            ALPHABET[i] as char
        })
        .collect()
}

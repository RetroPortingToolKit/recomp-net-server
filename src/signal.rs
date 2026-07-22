//! Per-player ICE signal mailboxes (recomp-net `RNetSignal` envelopes).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;

const MAX_QUEUED_PER_PLAYER: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalEnvelope {
    pub from_player_id: Uuid,
    pub room_id: Uuid,
    /// Mirrors `RNetSignalType`.
    #[serde(rename = "type")]
    pub signal_type: u8,
    pub flag: u8,
    pub text: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Default)]
pub struct SignalStore {
    /// player_id → inbound queue
    queues: HashMap<Uuid, VecDeque<SignalEnvelope>>,
}

impl SignalStore {
    pub fn push_to(&mut self, to: Uuid, msg: SignalEnvelope) {
        let q = self.queues.entry(to).or_default();
        if q.len() >= MAX_QUEUED_PER_PLAYER {
            q.pop_front();
        }
        q.push_back(msg);
    }

    pub fn drain(&mut self, player_id: Uuid) -> Vec<SignalEnvelope> {
        self.queues.remove(&player_id).map(|q| q.into()).unwrap_or_default()
    }

    pub fn clear_player(&mut self, player_id: Uuid) {
        self.queues.remove(&player_id);
    }
}

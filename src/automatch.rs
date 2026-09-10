//! Automatch retention.
//!
//! The queue, the accept gate and the pairing loop live in a later change;
//! what is here is the part that outlives a process — the two tables from
//! `004_automatch.sql` and the sweep that keeps them from growing without
//! bound.
//!
//! # Why a knob at all
//!
//! `automatch_pairings` records who played whom. Keeping that indefinitely is
//! a choice an operator should make deliberately rather than inherit from a
//! default, so it has a dial and the dial is documented in `docs/PRIVACY.md`
//! next to everything else this server retains.
//!
//! # Why the cutoff is a maximum, not the setting
//!
//! `AUTOMATCH_RETENTION_DAYS=0` means "prune a row as soon as it stops
//! affecting a decision" — it does NOT mean "delete everything now". A strike
//! inside the 24 h window is still raising somebody's cooldown, and a pairing
//! inside the rematch window is still steering the next pair away from a
//! repeat. Deleting either would silently undo enforcement that is currently
//! running, so the cutoff each table uses is the LONGER of the operator's
//! retention and the window the feature itself still reads.

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use std::collections::HashMap;
use tracing::{debug, info, warn};


/* ===================== rulesets ===================== */

/// The serialized ceiling `set_match_caps` already enforces on a host's blob.
/// A ruleset is the same kind of object and gets the same limit, so one path
/// cannot smuggle in something the other would refuse.
const MAX_CAPS_BYTES: usize = 4096;

/// One queue type: a stored `match_caps` blob with an id and a label.
///
/// Nothing here is parsed for meaning. The server does not know what
/// `input_delay` does; it knows that both players agreed to this exact object
/// by queueing into it and that it is what `launch` will carry.
#[derive(Debug, Clone)]
pub struct Ruleset {
    pub id: String,
    pub label: String,
    pub game_name: String,
    /// Pin. Empty = any release may queue into this ruleset.
    pub game_version: String,
    pub max_slots: usize,
    pub match_caps: Value,
    /// A short human line for the launcher's picker, derived once at load.
    pub caps_summary: String,
    /// Frame time for this title's delay arithmetic. See `DEFAULT_FRAME_MS`.
    pub frame_ms: f64,
}

#[derive(Debug, Deserialize)]
struct RulesetFile {
    #[serde(default)]
    ruleset: Vec<RulesetEntry>,
}

#[derive(Debug, Deserialize)]
struct RulesetEntry {
    id: String,
    #[serde(default)]
    label: String,
    game_name: String,
    #[serde(default)]
    game_version: String,
    #[serde(default = "two")]
    max_slots: usize,
    #[serde(default)]
    match_caps: Value,
    /// Optional. A 50 Hz title may say `frame_ms = 20.0`; omitted is 60 Hz.
    #[serde(default)]
    frame_ms: Option<f64>,
}

fn two() -> usize {
    2
}

/// Every ruleset a deployment offers, indexed for the two lookups that
/// actually happen: "what can this title queue for?" and "is this id real?".
#[derive(Debug, Clone, Default)]
pub struct Rulesets {
    by_game: HashMap<String, Vec<Ruleset>>,
}

impl Rulesets {
    pub fn is_empty(&self) -> bool {
        self.by_game.is_empty()
    }

    pub fn for_game(&self, game_name: &str) -> &[Ruleset] {
        self.by_game.get(game_name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Resolve a queue request. An empty id means "the first one", which is
    /// what a launcher sends when the server offered exactly one and it did
    /// not draw a picker.
    pub fn resolve(&self, game_name: &str, id: &str) -> Option<&Ruleset> {
        let list = self.for_game(game_name);
        if id.is_empty() {
            return list.first();
        }
        list.iter().find(|r| r.id == id)
    }

    /// Load from disk. A missing file is not an error -- it is how a
    /// deployment says "no automatch", and the caller reports that once at
    /// startup rather than failing to boot over an optional feature.
    pub fn load(path: &str) -> Self {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) => {
                debug!(path, error = %e, "no automatch rulesets; automatch is off");
                return Self::default();
            }
        };
        let parsed: RulesetFile = match toml::from_str(&text) {
            Ok(f) => f,
            Err(e) => {
                /* Loud, and then off. A malformed ruleset file must not take
                 * the lobby down -- every other client on this server has
                 * nothing to do with automatch. */
                warn!(path, error = %e, "automatch rulesets failed to parse; automatch is off");
                return Self::default();
            }
        };

        let mut by_game: HashMap<String, Vec<Ruleset>> = HashMap::new();
        let mut seen: Vec<(String, String)> = Vec::new();
        for e in parsed.ruleset {
            if let Err(why) = validate(&e, &seen) {
                warn!(id = %e.id, game = %e.game_name, why, "automatch ruleset refused");
                continue;
            }
            seen.push((e.game_name.clone(), e.id.clone()));
            let label = if e.label.is_empty() { e.id.clone() } else { e.label.clone() };
            let caps_summary = summarize(&e.match_caps);
            by_game.entry(e.game_name.clone()).or_default().push(Ruleset {
                id: e.id,
                label,
                game_name: e.game_name,
                game_version: e.game_version,
                max_slots: e.max_slots,
                match_caps: e.match_caps,
                caps_summary,
                frame_ms: e.frame_ms.filter(|f| *f > 0.0).unwrap_or(DEFAULT_FRAME_MS),
            });
        }
        if by_game.is_empty() {
            warn!(path, "automatch rulesets loaded but none were usable; automatch is off");
        } else {
            let n: usize = by_game.values().map(|v| v.len()).sum();
            info!(path, rulesets = n, games = by_game.len(), "automatch rulesets loaded");
        }
        Self { by_game }
    }
}

/// The load-time refusals from `docs/AUTOMATCH.md` §4, each one a thing that
/// would otherwise fail later and further away.
fn validate(e: &RulesetEntry, seen: &[(String, String)]) -> Result<(), &'static str> {
    if e.id.trim().is_empty() {
        return Err("empty id");
    }
    if e.game_name.trim().is_empty() {
        return Err("empty game_name");
    }
    if seen.iter().any(|(g, i)| *g == e.game_name && *i == e.id) {
        return Err("duplicate (game_name, id)");
    }
    /* v1 is 2p. A 4-seat ruleset would pair two players into a room that
     * never fills and never starts. */
    if e.max_slots != 2 {
        return Err("max_slots must be 2 in v1");
    }
    if !e.match_caps.is_null() && !e.match_caps.is_object() {
        return Err("match_caps must be a table");
    }
    /* Vanilla only: the seat gate compares a required plan against a joiner's
     * catalog, and there is no host to run the transfer prompt. */
    if e.match_caps.get("mods").is_some() {
        return Err("match_caps.mods is not supported in v1 (vanilla only)");
    }
    if e.match_caps.to_string().len() > MAX_CAPS_BYTES {
        return Err("match_caps too large");
    }
    Ok(())
}

/// A one-line human summary for the launcher's picker. Derived from the caps
/// rather than authored, so it cannot drift from what the match will run.
fn summarize(caps: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    /* An authored delay is a floor the link can raise, so it is advertised as
     * a minimum. A ruleset that authors NONE is saying "negotiate it", and
     * printing the internal default 2 there would advertise a number the
     * match will almost never run -- the accept gate then shows 6 and reads
     * as a bug. Say which of the two it is. */
    match caps.get("input_delay").and_then(|v| v.as_i64()) {
        Some(d) => parts.push(format!("Delay {d}+")),
        None => parts.push("Delay auto".into()),
    }
    match caps.get("rollback").and_then(|v| v.as_bool()) {
        Some(true) => parts.push("Rollback on".into()),
        Some(false) => parts.push("Rollback off".into()),
        None => {}
    }
    if caps.get("turbo_loads").and_then(|v| v.as_bool()) == Some(true) {
        parts.push("Turbo loads".into());
    }
    parts.join(" - ")
}

/// The `automatch_rulesets_ok` payload for one title.
pub fn rulesets_json(rs: &Rulesets, game_name: &str) -> Value {
    let rows: Vec<Value> = rs
        .for_game(game_name)
        .iter()
        .map(|r| {
            json!({
                "id": r.id,
                "label": r.label,
                "caps_summary": r.caps_summary,
                "game_version": r.game_version,
                "max_slots": r.max_slots,
                "match_caps": r.match_caps,
            })
        })
        .collect();
    json!({ "op": "automatch_rulesets_ok", "ok": true, "rulesets": rows })
}



/* ===================== delay floors ===================== */

/// One frame, in milliseconds. 60 Hz.
///
/// A PAL title runs 50 Hz, where a frame is LONGER, so 60 Hz maths asks for
/// more frames than a 50 Hz game needs. That is the safe direction: too much
/// delay plays badly, too little stalls the sim. A ruleset may override it.
pub const DEFAULT_FRAME_MS: f64 = 1000.0 / 60.0;

/// Frames added on top of the latency the measurement saw.
///
/// A single RTT sample is not the worst case: jitter, a retransmit, and the
/// two peers' frame boundaries not lining up all cost fractions of a frame
/// that the arithmetic below does not model. One frame is the cheapest
/// insurance against a floor that is right on average and stalls in practice.
pub const JITTER_MARGIN_FRAMES: u32 = 1;

/// Launcher clamps (`recomp_launcher.h`): delay 2..20, prediction 2..16.
pub const MIN_DELAY: u32 = 2;
pub const MAX_DELAY: u32 = 20;
pub const MIN_PREDICTION: u32 = 2;
pub const MAX_PREDICTION: u32 = 16;

/// What the pair's latency demands, in frames.
///
/// # Where this comes from
///
/// `recomp-net/docs/architecture.md`, "Delay-sync admission": local input is
/// stored at wire tick `T + D`, and admission at tick `T` needs every remote
/// slot's row for `T + D`. The peer produced that row `D` frames earlier, so
/// the input has exactly `D` frames of wall time to make a ONE-WAY trip.
///
/// Online always runs through the SFU, so that trip is peer → relay → peer.
/// Each `rtt_ms` is a client's own round trip to the relay, i.e. twice its
/// one-way distance, so:
///
/// ```text
///   one_way(A→B) = rtt_a/2 + rtt_b/2 = (rtt_a + rtt_b) / 2
///   frames       = ceil(one_way / frame_ms) + JITTER_MARGIN_FRAMES
/// ```
pub fn frames_needed(rtt_a: i32, rtt_b: i32, frame_ms: f64) -> u32 {
    if rtt_a < 0 || rtt_b < 0 || frame_ms <= 0.0 {
        return 0;
    }
    let one_way = (rtt_a as f64 + rtt_b as f64) / 2.0;
    (one_way / frame_ms).ceil() as u32 + JITTER_MARGIN_FRAMES
}

/* ── The hosted-lobby rules ──────────────────────────────────────────────────
 *
 * Automatch negotiates delay the same way a human-hosted room does, rather
 * than by its own arithmetic. That is a deliberate choice and worth stating:
 * the pure one-way calculation above is CORRECT and still too optimistic in
 * practice. The launcher's tables (recomp-ui launcher_imgui.cpp,
 * np_rb_delay_frames_from_rtt_ms / np_rb_prediction_frames_from_rtt_ms) were
 * moved up twice off measured soaks -- a TURN WAN link whose lobby RTT said
 * D=3 actually needed D=5-6 once transit and jitter were counted, and the
 * session spent its first minute invent-storming until arrival-driven
 * auto-delay caught up. A queued player has no host to notice that and nudge
 * the slider, so automatch has MORE reason to start at the settled number,
 * not less.
 *
 * The tables are keyed on a ROUND TRIP between the two peers. Here that path
 * is A -> relay -> B, whose round trip is rtt_a + rtt_b (each rtt is one
 * peer's own round trip to the relay, so each contributes half of it twice).
 *
 * Keep these in step with the launcher's. They are duplicated rather than
 * shared because one is Rust on a server and the other is C++ in a launcher,
 * and a wire field carrying the table would put the client in charge of its
 * own handicap.
 */

/// Rollback D, from the pair's round-trip time. WAN-aware tiers, floor 3.
pub fn hosted_rollback_delay(rtt_ms: i32) -> u32 {
    let rtt = rtt_ms.max(0);
    let d = if rtt < 50 {
        3
    } else if rtt < 80 {
        4
    } else if rtt < 120 {
        6
    } else if rtt < 160 {
        7
    } else if rtt < 200 {
        8
    } else if rtt < 260 {
        9
    } else {
        10
    };
    d.clamp(3, 12)
}

/// Invent runway: P = 4 + D, matching the launcher exactly.
pub fn hosted_rollback_prediction(delay: u32) -> u32 {
    (4 + delay.max(2)).clamp(6, MAX_PREDICTION)
}

/// Delay-only D. No runway to hide latency in, so the pad is larger: one-way
/// frames (ceil(rtt / 33)) plus three for ICE/TURN variance and scheduling.
pub fn hosted_delay_only(rtt_ms: i32) -> u32 {
    let rtt = rtt_ms.max(0);
    let one_way_frames = ((rtt + 32) / 33).max(1) as u32;
    (one_way_frames + 3).clamp(3, MAX_DELAY)
}

/// The delay and prediction a match should actually run.
///
/// A FLOOR, never a ceiling: the ruleset author picked a baseline and this can
/// only raise it. A measurement that says "you could get away with less" is
/// not a reason to overrule what the queue advertised, and both players agreed
/// to the advertised numbers by queueing.
///
/// Rollback splits the work: `D` covers what it was authored to cover and the
/// prediction runway `P` absorbs the rest, which is the whole reason to run
/// rollback on a long link. Without rollback there is nowhere else to put it,
/// so `D` carries all of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayFloor {
    pub input_delay: u32,
    pub input_prediction: u32,
    /// Frames the link needed. 0 when nothing was measured.
    pub needed: u32,
}

pub fn delay_floor(caps: &Value, rtt_a: i32, rtt_b: i32, frame_ms: f64) -> DelayFloor {
    let base_d = caps
        .get("input_delay")
        .and_then(|v| v.as_u64())
        .unwrap_or(MIN_DELAY as u64) as u32;
    let base_p = caps
        .get("input_prediction")
        .and_then(|v| v.as_u64())
        .unwrap_or(MIN_PREDICTION as u64) as u32;
    let rollback = caps.get("rollback").and_then(|v| v.as_bool()).unwrap_or(false);

    let needed = frames_needed(rtt_a, rtt_b, frame_ms);
    /* Nothing measured: leave the ruleset exactly as authored. Raising a
     * floor off a number nobody took would be inventing the reason for it. */
    if needed == 0 {
        return DelayFloor {
            input_delay: base_d.clamp(MIN_DELAY, MAX_DELAY),
            input_prediction: base_p.clamp(MIN_PREDICTION, MAX_PREDICTION),
            needed: 0,
        };
    }

    /* The two peers' round trip through the relay -- what the hosted tables
     * are keyed on. */
    let pair_rtt = rtt_a.saturating_add(rtt_b);

    let (d, p) = if rollback {
        /* The tier table decides D, and the runway follows it. Still a FLOOR:
         * a ruleset that authored a higher delay keeps it, because both
         * players agreed to the advertised number by queueing and a good
         * connection is not a reason to overrule them. */
        let d = hosted_rollback_delay(pair_rtt).max(base_d);
        let p = hosted_rollback_prediction(d).max(base_p);
        /* A link longer than D + P still cannot be papered over; D takes the
         * shortfall. P is then recomputed from the raised D rather than left
         * behind, so the P = 4 + D invariant the launcher maintains holds
         * here too -- a runway sized for a shorter delay than the one being
         * run is the mismatch that invents into a stall. */
        let shortfall = needed.saturating_sub(d + p);
        if shortfall == 0 {
            (d, p)
        } else {
            let d2 = d + shortfall;
            (d2, hosted_rollback_prediction(d2).max(base_p))
        }
    } else {
        (hosted_delay_only(pair_rtt).max(needed).max(base_d), base_p)
    };

    DelayFloor {
        input_delay: d.clamp(MIN_DELAY, MAX_DELAY),
        input_prediction: p.clamp(MIN_PREDICTION, MAX_PREDICTION),
        needed,
    }
}

/// Ruleset caps with the floor applied. Returned rather than mutated so the
/// stored ruleset stays what the operator wrote.
pub fn caps_with_floor(caps: &Value, floor: DelayFloor) -> Value {
    let mut out = caps.clone();
    if !out.is_object() {
        out = json!({});
    }
    out["input_delay"] = json!(floor.input_delay);
    if out.get("rollback").and_then(|v| v.as_bool()).unwrap_or(false) {
        out["input_prediction"] = json!(floor.input_prediction);
    }
    out
}

/// A client-reported round trip, made safe to store.
///
/// Client-reported, and treated as such (`docs/LOBBY.md` trust table). The
/// grief case is reporting HIGH to force delay on an opponent; reporting low
/// only stalls the liar's own sim, which is its own answer. The clamp bounds
/// the first and a sane ceiling keeps one bad number out of the arithmetic.
pub fn sanitize_rtt(raw: i64) -> i32 {
    if raw < 0 {
        return -1;
    }
    raw.min(MAX_REPORTED_RTT_MS as i64) as i32
}

/// Past this, the link cannot host a match a person would want to play; the
/// number is kept only so the pairing filter can say "no" with it.
pub const MAX_REPORTED_RTT_MS: u32 = 2000;

/// The relay packet type a client probes with, published to clients so the
/// number lives in one place rather than in two that can disagree.
pub const PROBE_PKT_TYPE: u16 = 200;

/// How long a freshly queued ticket is held back to let its probe land.
///
/// The documented path is to probe BEFORE queueing and put `rtt_ms` on the
/// queue message, and a client that does never waits here at all. This is for
/// the client that queues first and measures second: without it, two such
/// clients pair in the same millisecond they queue and the match runs at the
/// ruleset's authored delay with both measurements arriving too late to have
/// mattered. Pairing on "unknown" when the number is one second away is worse
/// than pairing one second later.
pub const PROBE_GRACE_SECS: u64 = 3;

/* ===================== the queue ===================== */

use std::time::Instant;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

/// The join gate, as a value.
///
/// `join` can refuse on game, release, disc fingerprint and seat count, so a
/// pairing that ignored any of them would produce a room the second player
/// cannot sit down in. Equality on this whole tuple is the pairing predicate:
/// no fuzzy matching, no "close enough".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MatchKey {
    pub game_name: String,
    pub game_version: String,
    pub disc_fp: String,
    pub ruleset_id: String,
    pub max_slots: usize,
}

/// One player waiting.
///
/// `account_id` is the identity; `player_id` is only where to send messages.
/// The distinction is the point: a reconnect gets a new connection id and must
/// not get a new dodge record with it.
#[derive(Debug, Clone)]
pub struct Ticket {
    pub player_id: String,
    pub account_id: String,
    pub handle: String,
    pub username: String,
    pub country: String,
    /// The client's preference order. Standalone recomp-ui sends one entry.
    pub keys: Vec<MatchKey>,
    pub host_bind: String,
    pub guest_bind: String,
    pub queued_at: Instant,
    /// Round trip to the relay, or -1 when unmeasured. See `rtt_ok`.
    pub rtt_ms: i32,
}

impl Ticket {
    pub fn waited(&self) -> u64 {
        self.queued_at.elapsed().as_secs()
    }
}

/// A pair on offer, waiting for two answers.
#[derive(Debug, Clone)]
pub struct Pending {
    pub match_id: String,
    pub key: MatchKey,
    pub a: Ticket,
    pub b: Ticket,
    pub a_accept: Option<bool>,
    pub b_accept: Option<bool>,
    pub offered_at: Instant,
}

impl Pending {
    fn side(&self, player_id: &str) -> Option<bool> {
        if self.a.player_id == player_id {
            Some(true)
        } else if self.b.player_id == player_id {
            Some(false)
        } else {
            None
        }
    }
    fn answered(&self) -> bool {
        self.a_accept.is_some() && self.b_accept.is_some()
    }
    pub fn both_yes(&self) -> bool {
        self.a_accept == Some(true) && self.b_accept == Some(true)
    }
}

/// Everything in flight. Small on purpose: a linear scan over a pool this size
/// is not worth an index, and the ordering the scan relies on (oldest first) is
/// easier to see in a Vec than to maintain in a map.
#[derive(Debug, Default)]
pub struct QueueInner {
    tickets: Vec<Ticket>,
    pending: Vec<Pending>,
    /// Recent opponents, for the soft avoid-last-opponent filter. Mirrors what
    /// `automatch_pairings` holds so the filter costs no query per candidate.
    recent: Vec<(String, String, Instant)>,
}

#[derive(Debug, Clone, Default)]
pub struct Queue(std::sync::Arc<AsyncMutex<QueueInner>>);

/// What a queue attempt refused on. The `&'static str` is the wire `code`.
pub type QueueError = &'static str;

impl Queue {
    pub async fn is_queued_account(&self, account_id: &str) -> bool {
        let g = self.0.lock().await;
        g.tickets.iter().any(|t| t.account_id == account_id)
            || g.pending.iter().any(|p| {
                p.a.account_id == account_id || p.b.account_id == account_id
            })
    }

    /// Tickets waiting. Named for what it counts rather than `len`, because
    /// a queue's length and a collection's length answer different questions.
    pub async fn waiting(&self) -> usize {
        self.0.lock().await.tickets.len()
    }

    /// How many tickets sit in the same bucket -- the "3 waiting" the launcher
    /// shows. Counts others, not the asker.
    pub async fn pool_for(&self, key: &MatchKey, excluding: &str) -> usize {
        let g = self.0.lock().await;
        g.tickets
            .iter()
            .filter(|t| t.account_id != excluding && t.keys.contains(key))
            .count()
    }

    pub async fn push(&self, t: Ticket) {
        self.0.lock().await.tickets.push(t);
    }

    /// Drop this connection from everything. Returns a pending it was half of,
    /// so the caller can tell the other side rather than leaving them waiting
    /// on somebody who closed the tab.
    pub async fn remove_player(&self, player_id: &str) -> Option<Pending> {
        let mut g = self.0.lock().await;
        g.tickets.retain(|t| t.player_id != player_id);
        let idx = g.pending.iter().position(|p| p.side(player_id).is_some())?;
        Some(g.pending.remove(idx))
    }

    /// Update a waiting ticket's measured RTT. A client probes while queued,
    /// so the number can arrive after the ticket did.
    pub async fn set_rtt(&self, player_id: &str, rtt_ms: i32) -> bool {
        let mut g = self.0.lock().await;
        if let Some(t) = g.tickets.iter_mut().find(|t| t.player_id == player_id) {
            t.rtt_ms = rtt_ms;
            return true;
        }
        false
    }

    pub async fn ticket_of(&self, player_id: &str) -> Option<Ticket> {
        let g = self.0.lock().await;
        g.tickets.iter().find(|t| t.player_id == player_id).cloned()
    }

    /// Put a ticket back at the FRONT. An accepter who lost their pair did
    /// nothing wrong; making them serve a second full wait would be a penalty
    /// for somebody else's decline.
    pub async fn requeue_front(&self, mut t: Ticket) {
        let mut g = self.0.lock().await;
        t.queued_at = Instant::now();
        g.tickets.insert(0, t);
    }

    /// Record an answer. Returns the pending once both sides have spoken.
    pub async fn answer(&self, player_id: &str, accept: bool) -> Option<Pending> {
        let mut g = self.0.lock().await;
        let idx = g.pending.iter().position(|p| p.side(player_id).is_some())?;
        {
            let p = &mut g.pending[idx];
            match p.side(player_id) {
                Some(true) => p.a_accept = Some(accept),
                Some(false) => p.b_accept = Some(accept),
                None => return None,
            }
            if !p.answered() {
                return None;
            }
        }
        Some(g.pending.remove(idx))
    }

    /// Pendings whose deadline has passed. Removed here so the caller can
    /// charge the side that never answered.
    pub async fn take_expired(&self, accept_secs: u64) -> Vec<Pending> {
        let mut g = self.0.lock().await;
        let mut out = Vec::new();
        let mut i = 0;
        while i < g.pending.len() {
            if g.pending[i].offered_at.elapsed().as_secs() >= accept_secs {
                out.push(g.pending.remove(i));
            } else {
                i += 1;
            }
        }
        out
    }

    /// Note that two accounts just played, for the avoid-last-opponent filter.
    pub async fn note_pairing(&self, a: &str, b: &str) {
        let mut g = self.0.lock().await;
        g.recent.push((a.to_string(), b.to_string(), Instant::now()));
    }

    /// Form pairs. Returns the offers to send; the caller owns the messaging
    /// so this never holds the queue lock and the hub lock at once.
    pub async fn pair(&self, rematch_cooldown_secs: u64) -> Vec<Pending> {
        let mut g = self.0.lock().await;
        g.recent
            .retain(|(_, _, at)| at.elapsed().as_secs() < rematch_cooldown_secs);

        let mut offers = Vec::new();
        loop {
            let Some((i, j, key)) = find_pair(&g, rematch_cooldown_secs) else {
                break;
            };
            /* Remove the higher index first so the lower one stays valid. */
            let (hi, lo) = if i > j { (i, j) } else { (j, i) };
            let t_hi = g.tickets.remove(hi);
            let t_lo = g.tickets.remove(lo);
            /* Slot 0 to the older ticket: deterministic, and it shows up in
             * the log, which "whichever the scan reached first" does not. */
            let (a, b) = if t_lo.queued_at <= t_hi.queued_at {
                (t_lo, t_hi)
            } else {
                (t_hi, t_lo)
            };
            let p = Pending {
                match_id: Uuid::new_v4().to_string(),
                key,
                a,
                b,
                a_accept: None,
                b_accept: None,
                offered_at: Instant::now(),
            };
            g.pending.push(p.clone());
            offers.push(p);
        }
        offers
    }

    /// Everyone waiting, for the 1 Hz status push.
    pub async fn status_rows(&self) -> Vec<(String, u64, i32, Vec<(MatchKey, usize)>)> {
        let g = self.0.lock().await;
        g.tickets
            .iter()
            .map(|t| {
                let pools = t
                    .keys
                    .iter()
                    .map(|k| {
                        let n = g
                            .tickets
                            .iter()
                            .filter(|o| o.account_id != t.account_id && o.keys.contains(k))
                            .count();
                        (k.clone(), n)
                    })
                    .collect();
                (t.player_id.clone(), t.waited(), t.rtt_ms, pools)
            })
            .collect()
    }
}

/// The first pairable two tickets, oldest-first, honouring each ticket's own
/// preference order.
fn find_pair(g: &QueueInner, rematch_cooldown_secs: u64) -> Option<(usize, usize, MatchKey)> {
    for i in 0..g.tickets.len() {
        let a = &g.tickets[i];
        for key in &a.keys {
            for j in 0..g.tickets.len() {
                if i == j {
                    continue;
                }
                let b = &g.tickets[j];
                if !b.keys.contains(key) {
                    continue;
                }
                /* Hard: never the same account, whatever else is true. */
                if a.account_id == b.account_id {
                    continue;
                }
                /* Both sides must be past the measuring window, or the
                 * pair is qualified on a number that was about to arrive. */
                if !measurable(a) || !measurable(b) {
                    continue;
                }
                if !rtt_ok(a, b) {
                    continue;
                }
                if !rematch_ok(g, a, b, rematch_cooldown_secs) {
                    continue;
                }
                return Some((i, j, key.clone()));
            }
        }
    }
    None
}

/// Has this ticket either measured, or had its chance to?
fn measurable(t: &Ticket) -> bool {
    t.rtt_ms >= 0 || t.waited() >= PROBE_GRACE_SECS
}

/// Combined round trip, widening with the older ticket's age.
///
/// An unmeasured RTT passes. That is deliberate rather than optimistic: v1
/// does not probe, so refusing on a number nobody measured would mean refusing
/// every pair. When the probe lands this becomes a real filter with no other
/// change.
fn rtt_ok(a: &Ticket, b: &Ticket) -> bool {
    if a.rtt_ms < 0 || b.rtt_ms < 0 {
        return true;
    }
    let waited = a.waited().max(b.waited());
    let steps = waited / RTT_WIDEN_SECS;
    if steps >= RTT_UNLIMITED_AFTER_STEPS {
        return true;
    }
    let ceiling = RTT_START_MS + steps * RTT_STEP_MS;
    (a.rtt_ms as u64 + b.rtt_ms as u64) <= ceiling
}

pub const RTT_START_MS: u64 = 120;
pub const RTT_STEP_MS: u64 = 60;
pub const RTT_WIDEN_SECS: u64 = 20;
/// After this many widen steps the ceiling is gone. A queue that never matches
/// is worse than a match a player can decline with their eyes open.
pub const RTT_UNLIMITED_AFTER_STEPS: u64 = 5;

/// Soft: prefer a new opponent, but not at the cost of not matching at all.
fn rematch_ok(g: &QueueInner, a: &Ticket, b: &Ticket, cooldown: u64) -> bool {
    /* Once the RTT filter has given up, so does this one -- otherwise a pool
     * of exactly two people stops working after their first match. */
    if a.waited().max(b.waited()) >= RTT_WIDEN_SECS * RTT_UNLIMITED_AFTER_STEPS {
        return true;
    }
    !g.recent.iter().any(|(x, y, at)| {
        at.elapsed().as_secs() < cooldown
            && ((x == &a.account_id && y == &b.account_id)
                || (x == &b.account_id && y == &a.account_id))
    })
}

/* ===================== strikes ===================== */

/// Record a dodge. Best-effort: a database that will not take the row must not
/// stop the queue from carrying on.
pub async fn record_strike(pool: &SqlitePool, account_id: &str, kind: &str, game_name: &str) {
    let r = sqlx::query(
        "INSERT INTO automatch_strikes (id, player_id, kind, game_name) VALUES (?, ?, ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(account_id)
    .bind(kind)
    .bind(game_name)
    .execute(pool)
    .await;
    if let Err(e) = r {
        warn!(error = %e, "automatch strike not recorded");
    }
}

/// Seconds this account must wait before queueing again, or 0.
pub async fn cooldown_secs(pool: &SqlitePool, account_id: &str, ladder: &[u64]) -> u64 {
    if ladder.is_empty() {
        return 0;
    }
    /* Strikes in the window, and how long ago the last one was: the ladder
     * picks the length, the last strike says how much of it is left. */
    let row: Option<(i64, Option<i64>)> = sqlx::query_as(
        "SELECT COUNT(*), CAST(strftime('%s','now') - strftime('%s', MAX(created_at)) AS INTEGER)          FROM automatch_strikes          WHERE player_id = ? AND created_at >= datetime('now', ?)",
    )
    .bind(account_id)
    .bind(format!("-{STRIKE_WINDOW_SECS} seconds"))
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    let Some((count, since)) = row else { return 0 };
    if count <= 0 {
        return 0;
    }
    let idx = (count as usize - 1).min(ladder.len() - 1);
    let penalty = ladder[idx];
    let elapsed = since.unwrap_or(0).max(0) as u64;
    penalty.saturating_sub(elapsed)
}

/// Note a formed pair. Also best-effort, for the same reason.
pub async fn record_pairing(
    pool: &SqlitePool,
    match_id: &str,
    key: &MatchKey,
    a: &str,
    b: &str,
    est_rtt_ms: i32,
) {
    let r = sqlx::query(
        "INSERT INTO automatch_pairings          (match_id, game_name, ruleset_id, player_a, player_b, est_rtt_ms)          VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(match_id)
    .bind(&key.game_name)
    .bind(&key.ruleset_id)
    .bind(a)
    .bind(b)
    .bind(if est_rtt_ms >= 0 { Some(est_rtt_ms) } else { None })
    .execute(pool)
    .await;
    if let Err(e) = r {
        warn!(error = %e, "automatch pairing not recorded");
    }
}

/// Mark a pairing as having actually reached `launch`, which is what tells a
/// dead accept gate apart from a match that ran.
pub async fn mark_launched(pool: &SqlitePool, match_id: &str) {
    let _ = sqlx::query("UPDATE automatch_pairings SET launched_at = datetime('now') WHERE match_id = ?")
        .bind(match_id)
        .execute(pool)
        .await;
}

/// How far back a cooldown counts strikes. The ladder in `docs/AUTOMATCH.md`
/// §8 is "strikes in the last 24 h", so a strike older than this cannot raise
/// anyone's cooldown and is history rather than enforcement.
pub const STRIKE_WINDOW_SECS: u64 = 24 * 60 * 60;

/// How often the sweep runs. Daily: nothing here is urgent, and a prune that
/// races the queue for the write lock is worse than a prune that is a few
/// hours late.
pub const SWEEP_INTERVAL_SECS: u64 = 24 * 60 * 60;

/// Rows deleted by one sweep, for the log line and for tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pruned {
    pub strikes: u64,
    pub pairings: u64,
}

impl Pruned {
    pub fn total(&self) -> u64 {
        self.strikes + self.pairings
    }
}

/// Seconds of history a table keeps: the operator's retention, floored by the
/// window the feature still reads from it.
///
/// Separated out and tested because it is the one place a wrong answer is
/// invisible — an over-long cutoff merely keeps rows, but an over-short one
/// deletes a strike that is currently holding a cooldown, and the symptom is
/// a dodger who is no longer on cooldown, which nobody reports as a bug.
pub fn cutoff_secs(retention_days: u64, functional_window_secs: u64) -> u64 {
    let retention = retention_days.saturating_mul(24 * 60 * 60);
    retention.max(functional_window_secs)
}

/// Delete rows past their cutoff. Idempotent, and safe to run on a database
/// where automatch has never been used.
pub async fn prune(
    pool: &SqlitePool,
    retention_days: u64,
    rematch_cooldown_secs: u64,
) -> Result<Pruned> {
    let strike_cut = cutoff_secs(retention_days, STRIKE_WINDOW_SECS);
    let pairing_cut = cutoff_secs(retention_days, rematch_cooldown_secs);

    /* `datetime('now', '-N seconds')` rather than arithmetic in Rust: the
     * columns are SQLite datetime text written by `datetime('now')`, so the
     * comparison has to be made by the same clock that wrote them. */
    let strikes = sqlx::query("DELETE FROM automatch_strikes WHERE created_at < datetime('now', ?)")
        .bind(format!("-{strike_cut} seconds"))
        .execute(pool)
        .await?
        .rows_affected();

    let pairings = sqlx::query("DELETE FROM automatch_pairings WHERE paired_at < datetime('now', ?)")
        .bind(format!("-{pairing_cut} seconds"))
        .execute(pool)
        .await?
        .rows_affected();

    Ok(Pruned { strikes, pairings })
}

/// Run the sweep now, then daily. The immediate first pass matters: a server
/// that is restarted more often than once a day would otherwise never prune.
pub fn spawn_sweep(pool: SqlitePool, retention_days: u64, rematch_cooldown_secs: u64) {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(SWEEP_INTERVAL_SECS));
        loop {
            interval.tick().await;
            match prune(&pool, retention_days, rematch_cooldown_secs).await {
                Ok(p) if p.total() > 0 => {
                    info!(
                        strikes = p.strikes,
                        pairings = p.pairings,
                        retention_days,
                        "automatch retention sweep"
                    );
                }
                Ok(_) => debug!(retention_days, "automatch retention sweep: nothing to prune"),
                /* A failed sweep is not fatal and must not take the lobby with
                 * it: the tables grow until the next pass. */
                Err(e) => info!(error = %e, "automatch retention sweep failed"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;


    /* ---- rulesets ---- */

    fn load_str(body: &str) -> Rulesets {
        let dir = std::env::temp_dir().join(format!("am-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("r.toml");
        std::fs::write(&path, body).unwrap();
        Rulesets::load(path.to_str().unwrap())
    }

    #[test]
    fn a_missing_file_is_automatch_off_not_a_failure() {
        /* Absent config means "this deployment does not run automatch", which
         * must not be a reason the lobby fails to serve everyone else. */
        assert!(Rulesets::load("/nonexistent/nope.toml").is_empty());
    }

    #[test]
    fn a_ruleset_carrying_mods_is_refused() {
        /* v1 is vanilla: there is no host to run the transfer prompt, so a
         * required plan could only ever fail at seating. */
        let rs = load_str(
            r#"
            [[ruleset]]
            id = "standard"
            game_name = "G"
            [ruleset.match_caps]
            mods = [{ id = "psx.foo", ver = "1.0.0" }]
            "#,
        );
        assert!(rs.is_empty(), "a mod plan must not load");
    }

    #[test]
    fn a_party_sized_ruleset_is_refused() {
        /* Four seats would pair two players into a room that never fills. */
        let rs = load_str("[[ruleset]]\nid='r'\ngame_name='G'\nmax_slots=4\n");
        assert!(rs.is_empty());
    }

    #[test]
    fn a_duplicate_id_for_one_game_keeps_only_the_first() {
        let rs = load_str(
            "[[ruleset]]\nid='r'\ngame_name='G'\nlabel='first'\n\
             [[ruleset]]\nid='r'\ngame_name='G'\nlabel='second'\n",
        );
        assert_eq!(rs.for_game("G").len(), 1);
        assert_eq!(rs.for_game("G")[0].label, "first");
    }

    #[test]
    fn the_same_id_under_two_games_is_not_a_duplicate() {
        let rs = load_str(
            "[[ruleset]]\nid='standard'\ngame_name='A'\n\
             [[ruleset]]\nid='standard'\ngame_name='B'\n",
        );
        assert_eq!(rs.for_game("A").len(), 1);
        assert_eq!(rs.for_game("B").len(), 1);
    }

    #[test]
    fn an_empty_id_resolves_to_the_only_queue() {
        /* What a launcher sends when the server offered one and it drew no
         * picker. */
        let rs = load_str("[[ruleset]]\nid='standard'\ngame_name='G'\n");
        assert_eq!(rs.resolve("G", "").map(|r| r.id.as_str()), Some("standard"));
        assert!(rs.resolve("G", "nope").is_none());
        assert!(rs.resolve("Other", "").is_none());
    }

    #[test]
    fn a_label_defaults_to_the_id_rather_than_being_blank() {
        let rs = load_str("[[ruleset]]\nid='standard'\ngame_name='G'\n");
        assert_eq!(rs.for_game("G")[0].label, "standard");
    }

    #[test]
    fn the_caps_summary_is_derived_from_the_caps() {
        /* Derived, not authored, so the picker cannot describe settings the
         * match will not run. The delay reads "2+" because an authored delay
         * is a floor the link can raise, never the number the match runs. */
        let rs = load_str(
            "[[ruleset]]\nid='r'\ngame_name='G'\n[ruleset.match_caps]\n\
             input_delay=2\nrollback=true\nturbo_loads=true\n",
        );
        assert_eq!(rs.for_game("G")[0].caps_summary, "Delay 2+ - Rollback on - Turbo loads");
    }

    /* ---- pairing ---- */

    fn key(game: &str) -> MatchKey {
        MatchKey {
            game_name: game.into(),
            game_version: "1.0.0".into(),
            disc_fp: "a".repeat(64),
            ruleset_id: "standard".into(),
            max_slots: 2,
        }
    }

    fn ticket(player: &str, account: &str, keys: Vec<MatchKey>) -> Ticket {
        Ticket {
            player_id: player.into(),
            account_id: account.into(),
            handle: player.into(),
            username: String::new(),
            country: String::new(),
            keys,
            host_bind: "0.0.0.0:7777".into(),
            guest_bind: "0.0.0.0:7778".into(),
            queued_at: Instant::now(),
            /* Measured, because that is the path a real client takes: probe
             * the relay, then queue carrying the number. A test that wants
             * the unmeasured case sets this to -1 and says so. */
            rtt_ms: 20,
        }
    }

    #[tokio::test]
    async fn two_compatible_tickets_pair() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        let offers = q.pair(300).await;
        assert_eq!(offers.len(), 1);
        assert_eq!(q.waiting().await, 0, "paired tickets leave the queue");
    }

    #[tokio::test]
    async fn a_different_disc_never_pairs() {
        /* The gate `join` would apply later, applied here instead. */
        let mut k2 = key("G");
        k2.disc_fp = "b".repeat(64);
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![k2])).await;
        assert!(q.pair(300).await.is_empty());
        assert_eq!(q.waiting().await, 2);
    }

    #[tokio::test]
    async fn a_different_release_never_pairs() {
        let mut k2 = key("G");
        k2.game_version = "1.1.0".into();
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![k2])).await;
        assert!(q.pair(300).await.is_empty());
    }

    #[tokio::test]
    async fn one_account_on_two_connections_never_pairs_with_itself() {
        /* The hard filter. Two clients on one login is a support case, not a
         * way to play yourself. */
        let q = Queue::default();
        q.push(ticket("p1", "same", vec![key("G")])).await;
        q.push(ticket("p2", "same", vec![key("G")])).await;
        assert!(q.pair(300).await.is_empty());
    }

    #[tokio::test]
    async fn a_multi_title_ticket_pairs_on_the_title_they_share() {
        /* The Retro Launcher shape: several titles offered, one in common. */
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("A"), key("B")])).await;
        q.push(ticket("p2", "a2", vec![key("B")])).await;
        let offers = q.pair(300).await;
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].key.game_name, "B");
    }

    #[tokio::test]
    async fn preference_order_decides_when_both_titles_are_shared() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("A"), key("B")])).await;
        q.push(ticket("p2", "a2", vec![key("B"), key("A")])).await;
        let offers = q.pair(300).await;
        /* The older ticket's order wins; it is the one that has been waiting. */
        assert_eq!(offers[0].key.game_name, "A");
    }

    #[tokio::test]
    async fn slot_zero_goes_to_the_older_ticket() {
        let q = Queue::default();
        let mut older = ticket("p1", "a1", vec![key("G")]);
        older.queued_at = Instant::now() - std::time::Duration::from_secs(30);
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        q.push(older).await;
        let offers = q.pair(300).await;
        assert_eq!(offers[0].a.player_id, "p1", "the older ticket is slot 0");
    }

    #[tokio::test]
    async fn a_just_played_pair_is_skipped_while_the_window_holds() {
        let q = Queue::default();
        q.note_pairing("a1", "a2").await;
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        assert!(q.pair(300).await.is_empty(), "avoid the last opponent");
    }

    #[tokio::test]
    async fn the_rematch_filter_gives_way_rather_than_emptying_a_two_person_pool() {
        /* Soft, and it has to be: held hard, a pool of exactly two people
         * stops working after their first match. */
        let q = Queue::default();
        q.note_pairing("a1", "a2").await;
        let old = std::time::Duration::from_secs(RTT_WIDEN_SECS * RTT_UNLIMITED_AFTER_STEPS + 1);
        let mut t1 = ticket("p1", "a1", vec![key("G")]);
        let mut t2 = ticket("p2", "a2", vec![key("G")]);
        t1.queued_at = Instant::now() - old;
        t2.queued_at = Instant::now() - old;
        q.push(t1).await;
        q.push(t2).await;
        assert_eq!(q.pair(300).await.len(), 1, "a long wait beats the preference");
    }

    #[tokio::test]
    async fn an_accepter_who_loses_their_pair_goes_to_the_front() {
        let q = Queue::default();
        q.push(ticket("p_waiting", "a3", vec![key("G")])).await;
        q.requeue_front(ticket("p_jilted", "a1", vec![key("G")])).await;
        let rows = q.status_rows().await;
        assert_eq!(rows[0].0, "p_jilted");
    }

    #[tokio::test]
    async fn an_offer_needs_both_answers_before_it_resolves() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        q.pair(300).await;
        assert!(q.answer("p1", true).await.is_none(), "one answer is not enough");
        let done = q.answer("p2", true).await.expect("both answered");
        assert!(done.both_yes());
    }

    #[tokio::test]
    async fn a_decline_resolves_the_offer_as_not_both_yes() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        q.pair(300).await;
        q.answer("p1", true).await;
        let done = q.answer("p2", false).await.expect("resolved");
        assert!(!done.both_yes());
    }

    #[tokio::test]
    async fn a_queued_account_is_recognised_while_its_offer_is_open() {
        /* One ticket per account has to keep holding while the pair is at the
         * accept gate, or a decline could be answered from a second queue. */
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        q.pair(300).await;
        assert!(q.is_queued_account("a1").await);
    }

    #[tokio::test]
    async fn a_dropped_connection_leaves_nothing_behind() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        q.pair(300).await;
        let orphan = q.remove_player("p1").await.expect("was half of an offer");
        assert_eq!(orphan.b.player_id, "p2");
        assert!(!q.is_queued_account("a1").await);
    }

    #[tokio::test]
    async fn an_unanswered_offer_lapses() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        q.push(ticket("p2", "a2", vec![key("G")])).await;
        q.pair(300).await;
        assert!(q.take_expired(3600).await.is_empty(), "not yet");
        assert_eq!(q.take_expired(0).await.len(), 1, "deadline passed");
    }


    /* ---- delay floors ---- */

    #[test]
    fn frames_needed_follows_the_one_way_trip_through_the_relay() {
        /* Each rtt is a peer's round trip to the relay, so peer-to-peer
         * one-way is (a + b) / 2. 40 + 40 -> 40ms one way -> 3 frames at
         * 16.67, plus the jitter frame. */
        assert_eq!(frames_needed(40, 40, DEFAULT_FRAME_MS), 4);
        /* A LAN pair still gets the margin frame and nothing more. */
        assert_eq!(frames_needed(0, 0, DEFAULT_FRAME_MS), 1);
    }

    #[test]
    fn an_unmeasured_side_needs_nothing() {
        /* Not "needs zero frames" -- "we did not measure", which the caller
         * distinguishes by leaving the ruleset alone. */
        assert_eq!(frames_needed(-1, 40, DEFAULT_FRAME_MS), 0);
        assert_eq!(frames_needed(40, -1, DEFAULT_FRAME_MS), 0);
    }

    #[test]
    fn an_unmeasured_pair_runs_the_ruleset_exactly_as_written() {
        let caps = json!({"input_delay": 2, "rollback": true, "input_prediction": 6});
        let f = delay_floor(&caps, -1, -1, DEFAULT_FRAME_MS);
        assert_eq!((f.input_delay, f.input_prediction, f.needed), (2, 6, 0));
    }

    #[test]
    fn a_short_link_does_not_lower_what_the_ruleset_asked_for() {
        /* A floor, not a target. The queue advertised delay 4 and both
         * players agreed to it; a good connection is not a reason to overrule
         * that. */
        let caps = json!({"input_delay": 4, "rollback": false});
        let f = delay_floor(&caps, 5, 5, DEFAULT_FRAME_MS);
        assert_eq!(f.input_delay, 4);
    }

    #[test]
    fn without_rollback_the_delay_carries_all_of_it() {
        /* Delay-sync has no runway, so D carries the link on its own, by the
         * launcher's own delay-only rule: ceil(rtt/33) one-way frames plus a
         * three-frame pad for ICE/TURN variance. 120 + 120 -> pair rtt 240 ->
         * ceil(240/33) = 8, + 3 = 11. That is deliberately ABOVE the pure
         * one-way arithmetic's 9: the pad is what the soaks said was missing.
         */
        let caps = json!({"input_delay": 2, "rollback": false});
        let f = delay_floor(&caps, 120, 120, DEFAULT_FRAME_MS);
        assert_eq!(f.input_delay, 11);
        assert_eq!(f.needed, 9, "the raw requirement is still reported as-is");
        assert_eq!(f.input_delay, hosted_delay_only(240));
    }

    #[test]
    fn with_rollback_the_tiers_set_the_delay_and_the_runway_follows() {
        /* Automatch negotiates like a hosted room rather than by its own
         * arithmetic. The launcher's tier table was moved up twice off
         * measured soaks precisely because "keep D low, let P absorb it" spent
         * the first minute of a WAN session invent-storming -- so D moves.
         *
         * 120 + 120 -> pair rtt 240 -> the >=200 tier -> D = 9, P = 4 + 9. */
        let caps = json!({"input_delay": 2, "rollback": true, "input_prediction": 2});
        let f = delay_floor(&caps, 120, 120, DEFAULT_FRAME_MS);
        assert_eq!(f.input_delay, 9);
        assert_eq!(f.input_prediction, 13);
        assert_eq!(f.input_delay, hosted_rollback_delay(240));
        assert_eq!(f.input_prediction, hosted_rollback_prediction(9));
    }

    #[test]
    fn the_tiers_match_the_launcher_table_exactly() {
        /* These numbers are the launcher's (np_rb_delay_frames_from_rtt_ms),
         * and the two copies drifting apart would give a queued player a
         * different handicap from a hosted one on the same link. */
        for (rtt, want) in [(0, 3), (19, 3), (49, 3), (50, 4), (79, 4), (80, 6),
                            (119, 6), (120, 7), (159, 7), (160, 8), (199, 8),
                            (200, 9), (259, 9), (260, 10), (5000, 10)] {
            assert_eq!(hosted_rollback_delay(rtt), want, "rtt {rtt}");
        }
        /* P = 4 + D, clamped 6..16 -- also the launcher's. */
        assert_eq!(hosted_rollback_prediction(2), 6);
        assert_eq!(hosted_rollback_prediction(3), 7);
        assert_eq!(hosted_rollback_prediction(10), 14);
        assert_eq!(hosted_rollback_prediction(20), MAX_PREDICTION);
    }

    #[test]
    fn a_local_pair_still_gets_the_hosted_floor_of_three() {
        /* The tables floor at 3 even on a LAN-grade link, which is above the
         * pure arithmetic's answer. Automatch does not get to be more
         * optimistic than a hosted room on the same connection. */
        let caps = json!({"input_delay": 2, "rollback": true, "input_prediction": 2});
        let f = delay_floor(&caps, 1, 1, DEFAULT_FRAME_MS);
        assert_eq!(f.input_delay, 3);
    }

    #[test]
    fn a_link_longer_than_the_runway_pushes_the_delay_back_up() {
        /* The tiers top out at D = 10, so on a link far past them D + P is
         * still short of what the trip needs. There is nowhere left to put
         * the latency but D, and pretending otherwise would stall the sim.
         *
         * 400 + 400 -> pair rtt 800 -> tier D = 10, P = 14, while the raw
         * requirement is 25 frames. D absorbs the 1-frame shortfall and P is
         * recomputed from the raised D. */
        let caps = json!({"input_delay": 2, "rollback": true, "input_prediction": 2});
        let f = delay_floor(&caps, 400, 400, DEFAULT_FRAME_MS);
        assert!(f.input_delay > hosted_rollback_delay(800),
                "delay took the shortfall: {}", f.input_delay);
        assert_eq!(f.input_prediction, hosted_rollback_prediction(f.input_delay),
                   "the runway is sized for the delay actually being run");
        assert!(f.input_delay + f.input_prediction >= f.needed.min(MAX_DELAY + MAX_PREDICTION));
    }

    #[test]
    fn the_floor_never_escapes_the_launcher_clamps() {
        let caps = json!({"input_delay": 2, "rollback": false});
        let f = delay_floor(&caps, 2000, 2000, DEFAULT_FRAME_MS);
        assert!(f.input_delay <= MAX_DELAY);
        assert!(f.input_prediction >= MIN_PREDICTION && f.input_prediction <= MAX_PREDICTION);
    }

    #[test]
    fn a_fifty_hertz_title_needs_fewer_frames_for_the_same_link() {
        /* A PAL frame is longer, so the same milliseconds fit in fewer of
         * them. 60 Hz maths over-asks, which is the safe direction. */
        let ntsc = frames_needed(100, 100, DEFAULT_FRAME_MS);
        let pal = frames_needed(100, 100, 20.0);
        assert!(pal < ntsc, "{pal} should be under {ntsc}");
    }

    #[test]
    fn caps_with_floor_leaves_prediction_alone_without_rollback() {
        /* A delay-sync match has no runway; writing one in would publish a
         * setting the match does not have. */
        let caps = json!({"input_delay": 2, "rollback": false});
        let out = caps_with_floor(&caps, delay_floor(&caps, 120, 120, DEFAULT_FRAME_MS));
        assert_eq!(out["input_delay"], json!(11));
        assert!(out.get("input_prediction").is_none());
    }

    #[test]
    fn caps_with_floor_keeps_every_other_setting() {
        let caps = json!({"input_delay": 2, "rollback": false, "language": "en", "v": 1});
        let out = caps_with_floor(&caps, delay_floor(&caps, 40, 40, DEFAULT_FRAME_MS));
        assert_eq!(out["language"], json!("en"));
        assert_eq!(out["v"], json!(1));
    }

    #[test]
    fn a_reported_rtt_is_clamped_rather_than_trusted() {
        assert_eq!(sanitize_rtt(37), 37);
        assert_eq!(sanitize_rtt(-5), -1);
        assert_eq!(sanitize_rtt(9_999_999), MAX_REPORTED_RTT_MS as i32);
    }

    #[tokio::test]
    async fn a_measured_pair_that_is_too_far_apart_does_not_pair_yet() {
        /* The qualifying filter, with real numbers in it. */
        let q = Queue::default();
        let mut a = ticket("p1", "a1", vec![key("G")]);
        let mut b = ticket("p2", "a2", vec![key("G")]);
        a.rtt_ms = 300;
        b.rtt_ms = 300;
        q.push(a).await;
        q.push(b).await;
        assert!(q.pair(300).await.is_empty(), "600ms combined is over the opening ceiling");
    }

    #[tokio::test]
    async fn a_measured_pair_that_is_close_pairs_immediately() {
        let q = Queue::default();
        let mut a = ticket("p1", "a1", vec![key("G")]);
        let mut b = ticket("p2", "a2", vec![key("G")]);
        a.rtt_ms = 30;
        b.rtt_ms = 40;
        q.push(a).await;
        q.push(b).await;
        assert_eq!(q.pair(300).await.len(), 1);
    }

    #[tokio::test]
    async fn a_distant_pair_matches_once_the_window_has_widened() {
        /* Waiting buys reach. A queue that never matches is worse than a
         * match the player can decline having seen the estimate. */
        let q = Queue::default();
        let old = std::time::Duration::from_secs(RTT_WIDEN_SECS * RTT_UNLIMITED_AFTER_STEPS + 1);
        let mut a = ticket("p1", "a1", vec![key("G")]);
        let mut b = ticket("p2", "a2", vec![key("G")]);
        a.rtt_ms = 300;
        b.rtt_ms = 300;
        a.queued_at = Instant::now() - old;
        b.queued_at = Instant::now() - old;
        q.push(a).await;
        q.push(b).await;
        assert_eq!(q.pair(300).await.len(), 1);
    }

    #[tokio::test]
    async fn an_unmeasured_ticket_waits_out_the_grace_before_it_can_pair() {
        /* Otherwise two clients that queue and then probe pair in the same
         * millisecond they queued, and both measurements land too late to
         * have qualified anything. */
        let q = Queue::default();
        let mut a = ticket("p1", "a1", vec![key("G")]);
        let mut b = ticket("p2", "a2", vec![key("G")]);
        a.rtt_ms = -1;
        b.rtt_ms = -1;
        q.push(a).await;
        q.push(b).await;
        assert!(q.pair(300).await.is_empty(), "still measuring");

        /* The probe lands. No further waiting: the number is what the grace
         * was for. */
        q.set_rtt("p1", 25).await;
        q.set_rtt("p2", 25).await;
        assert_eq!(q.pair(300).await.len(), 1);
    }

    #[tokio::test]
    async fn a_client_that_never_probes_still_gets_a_match() {
        /* The grace is a delay, not a requirement. A client with no probe
         * support must not sit in the queue forever. */
        let q = Queue::default();
        let past = std::time::Duration::from_secs(PROBE_GRACE_SECS + 1);
        for (p_, a_) in [("p1", "a1"), ("p2", "a2")] {
            let mut t = ticket(p_, a_, vec![key("G")]);
            t.rtt_ms = -1;
            t.queued_at = Instant::now() - past;
            q.push(t).await;
        }
        assert_eq!(q.pair(300).await.len(), 1);
    }

    #[tokio::test]
    async fn a_probe_that_arrives_after_the_ticket_still_counts() {
        let q = Queue::default();
        q.push(ticket("p1", "a1", vec![key("G")])).await;
        assert!(q.set_rtt("p1", 42).await);
        assert_eq!(q.status_rows().await[0].2, 42);
    }

    /* ---- cooldown ladder ---- */

    async fn db() -> SqlitePool {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO players (id, api_token_hash) VALUES ('a1','x')")
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    async fn a_clean_account_has_no_cooldown() {
        let pool = db().await;
        assert_eq!(cooldown_secs(&pool, "a1", &[60, 300, 900]).await, 0);
    }

    #[tokio::test]
    async fn the_ladder_escalates_with_each_strike() {
        let pool = db().await;
        record_strike(&pool, "a1", "decline", "G").await;
        let one = cooldown_secs(&pool, "a1", &[60, 300, 900]).await;
        record_strike(&pool, "a1", "decline", "G").await;
        let two = cooldown_secs(&pool, "a1", &[60, 300, 900]).await;
        assert!(one > 0 && two > one, "{one} then {two}");
    }

    #[tokio::test]
    async fn an_empty_ladder_records_the_dodge_and_charges_nothing() {
        /* A deployment may want the record before it wants the penalty. */
        let pool = db().await;
        record_strike(&pool, "a1", "decline", "G").await;
        assert_eq!(cooldown_secs(&pool, "a1", &[]).await, 0);
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM automatch_strikes")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 1, "still recorded");
    }

    #[tokio::test]
    async fn a_strike_outside_the_window_no_longer_costs_anything() {
        let pool = db().await;
        sqlx::query(
            "INSERT INTO automatch_strikes (id, player_id, kind, created_at) \
             VALUES ('old','a1','decline', datetime('now','-40 hours'))",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(cooldown_secs(&pool, "a1", &[60, 300, 900]).await, 0);
    }

    #[test]
    fn zero_retention_still_keeps_what_is_being_enforced() {
        /* The point of the floor: 0 means "as soon as it stops mattering",
         * not "now". A strike inside the 24 h window is holding a cooldown. */
        assert_eq!(cutoff_secs(0, STRIKE_WINDOW_SECS), STRIKE_WINDOW_SECS);
        assert_eq!(cutoff_secs(0, 300), 300);
    }

    #[test]
    fn a_longer_retention_wins_over_the_functional_window() {
        assert_eq!(cutoff_secs(30, STRIKE_WINDOW_SECS), 30 * 86_400);
        assert_eq!(cutoff_secs(1, 300), 86_400);
    }

    #[test]
    fn a_day_of_retention_is_exactly_the_strike_window() {
        /* Neither value wins by accident: they are equal, and the result must
         * be that value rather than a doubled or zeroed one. */
        assert_eq!(cutoff_secs(1, STRIKE_WINDOW_SECS), STRIKE_WINDOW_SECS);
    }

    #[test]
    fn an_absurd_retention_does_not_overflow_into_a_short_cutoff() {
        /* saturating_mul, so a fat-fingered env var keeps everything rather
         * than wrapping to a cutoff that deletes everything. */
        let c = cutoff_secs(u64::MAX, STRIKE_WINDOW_SECS);
        assert!(c >= STRIKE_WINDOW_SECS);
        assert_eq!(c, u64::MAX);
    }

    #[tokio::test]
    async fn prune_is_safe_on_a_database_nobody_has_queued_on() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let p = prune(&pool, 30, 300).await.unwrap();
        assert_eq!(p, Pruned::default());
    }

    #[tokio::test]
    async fn old_rows_go_and_rows_still_being_read_stay() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO players (id, api_token_hash) VALUES ('p1', 'x'), ('p2', 'y')")
            .execute(&pool)
            .await
            .unwrap();

        /* One strike inside the 24 h window, one well outside it. */
        sqlx::query(
            "INSERT INTO automatch_strikes (id, player_id, kind, created_at) VALUES \
             ('s_new', 'p1', 'decline', datetime('now', '-1 hours')), \
             ('s_old', 'p1', 'timeout', datetime('now', '-40 days'))",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO automatch_pairings (match_id, game_name, ruleset_id, player_a, player_b, paired_at) \
             VALUES ('m_new', 'g', 'standard', 'p1', 'p2', datetime('now', '-1 minutes')), \
                    ('m_old', 'g', 'standard', 'p1', 'p2', datetime('now', '-40 days'))",
        )
        .execute(&pool)
        .await
        .unwrap();

        let p = prune(&pool, 30, 300).await.unwrap();
        assert_eq!(p, Pruned { strikes: 1, pairings: 1 });

        let (s,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM automatch_strikes")
            .fetch_one(&pool)
            .await
            .unwrap();
        let (m,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM automatch_pairings")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!((s, m), (1, 1), "the recent rows are still being read");
    }

    #[tokio::test]
    async fn zero_retention_does_not_drop_a_cooldown_that_is_still_running() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO players (id, api_token_hash) VALUES ('p1', 'x')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO automatch_strikes (id, player_id, kind, created_at) VALUES \
             ('s', 'p1', 'decline', datetime('now', '-2 hours'))",
        )
        .execute(&pool)
        .await
        .unwrap();

        let p = prune(&pool, 0, 300).await.unwrap();
        assert_eq!(p.strikes, 0, "a 2h-old strike is inside the 24h window");
    }
}

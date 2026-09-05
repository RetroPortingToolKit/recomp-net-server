//! UDP delay-sync input relay (star topology).
//!
//! Each peer dials one public UDP endpoint (via `rnet_session_start_lan` with
//! the relay as `peer`). The relay:
//!   1. Validates the recomp-net common header (magic + session_id)
//!   2. Learns `(session_id, local_slot) → (peer, local_dst)` from packets
//!   3. Forwards the raw datagram to every *other* registered seat, pinning
//!      the UDP source IP to that seat's dialed local address (`IP_PKTINFO`)
//!
//! Pad bytes are never interpreted. This is enough for 2P NAT traversal and
//! for 3–4P matches where a full mesh is not available.

use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::metrics;
use crate::udp_pktinfo::{self, RecvSas};

/// Eight player seats plus a four-seat gallery.
///
/// Spectators bind slots ABOVE the session's player count: binding is what
/// puts a peer on the fan-out list, so a spectator has to hold a slot to
/// receive anything. What it must not do is send, and that is enforced in
/// `recv_loop` rather than by withholding a binding.
const MAX_PLAYER_SLOTS: usize = 8;
const MAX_SPECTATOR_SLOTS: usize = 4;
const MAX_SLOTS: usize = MAX_PLAYER_SLOTS + MAX_SPECTATOR_SLOTS;
const MIN_PACKET: usize = 14; // magic(4)+type(2)+session(4)+body(≥0)+checksum(4)
const HEADER_LEN: usize = 10;
const RNET_PKT_START: u16 = 3;
const RNET_PKT_DELAY_SYNC: u16 = 5;
const SESSION_IDLE: Duration = Duration::from_secs(120);
/// Recent UDP + at least two bound seats ⇒ pads are actually flowing.
const SESSION_ACTIVE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct InputRelay {
    inner: Arc<Mutex<RelayInner>>,
    advertise_host: String,
    /// Same-LAN / split-horizon alternate (may be empty).
    lan_host: String,
    advertise_port: u16,
    enabled: bool,
}

struct RelayInner {
    sessions: HashMap<u32, RelaySession>,
}

impl RelayInner {
    fn publish_gauges(&self) {
        metrics::set_input_relay_sessions(self.sessions.len());
        metrics::set_input_relay_sessions_active(self.active_count());
    }

    fn active_count(&self) -> usize {
        self.sessions
            .values()
            .filter(|s| {
                s.last_rx.elapsed() < SESSION_ACTIVE
                    && s.slots.iter().filter(|b| b.is_some()).count() >= 2
            })
            .count()
    }
}

#[derive(Clone, Copy, Debug)]
struct SlotBinding {
    peer: SocketAddr,
    /// Local IPv4 this peer dialed (from IP_PKTINFO on recv).
    local_dst: Option<Ipv4Addr>,
}

struct RelaySession {
    /// Bindable seats: players plus spectators. Packets naming a slot at or
    /// beyond this are rejected as before.
    slot_count: u8,
    /// Seats whose packets are forwarded. Slots in `player_slots..slot_count`
    /// are the gallery: they bind, they receive, and nothing they send is
    /// ever passed on.
    player_slots: u8,
    /// Per-slot last-seen binding (None = not yet registered).
    slots: [Option<SlotBinding>; MAX_SLOTS],
    last_rx: Instant,
}

impl InputRelay {
    /// Bind the UDP socket and spawn the recv loop. Returns a handle used by
    /// the lobby to open/close sessions. When `INPUT_RELAY_ENABLED=0`, the
    /// relay is a no-op stub (open_session fails).
    pub async fn start(config: &Config) -> Result<Self> {
        let enabled = config.input_relay_enabled;
        let advertise_host = config.input_relay_advertise_host.clone();
        let lan_host = config.input_relay_lan_host.trim().to_string();
        let advertise_port = config.input_relay_advertise_port;
        let fallback_source = udp_pktinfo::parse_ipv4_host(&lan_host)
            .or_else(|| udp_pktinfo::parse_ipv4_host(&advertise_host));

        let inner = Arc::new(Mutex::new(RelayInner {
            sessions: HashMap::new(),
        }));

        if !enabled {
            info!("input relay disabled (INPUT_RELAY_ENABLED=0)");
            return Ok(Self {
                inner,
                advertise_host,
                lan_host,
                advertise_port,
                enabled: false,
            });
        }

        let bind = &config.input_relay_bind;
        let sock = udp_pktinfo::bind_udp_with_pktinfo(bind)
            .with_context(|| format!("input relay bind {bind}"))?;
        let local = sock.local_addr().context("input relay local_addr")?;
        info!(
            %local,
            advertise = %format!("{}:{}", advertise_host, advertise_port),
            lan = %if lan_host.is_empty() {
                "(unset)".to_string()
            } else {
                format!("{lan_host}:{advertise_port}")
            },
            ?fallback_source,
            "input relay listening (IP_PKTINFO source pin)"
        );

        let loop_inner = inner.clone();
        let magic = config.protocol_magic;
        let fallback = fallback_source;
        tokio::spawn(async move {
            if let Err(e) = recv_loop(sock, loop_inner, magic, fallback).await {
                warn!(error = %e, "input relay recv loop exited");
            }
        });

        let janitor_inner = inner.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            loop {
                tick.tick().await;
                let mut g = janitor_inner.lock().await;
                let before = g.sessions.len();
                g.sessions.retain(|_, s| s.last_rx.elapsed() < SESSION_IDLE);
                let dropped = before.saturating_sub(g.sessions.len());
                if dropped > 0 {
                    debug!(dropped, "purged idle input-relay sessions");
                }
                g.publish_gauges();
            }
        });

        Ok(Self {
            inner,
            advertise_host,
            lan_host,
            advertise_port,
            enabled: true,
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Public / DNS advertise `host:port` string clients should dial.
    pub fn advertise_endpoint(&self) -> String {
        format!("{}:{}", self.advertise_host, self.advertise_port)
    }

    /// Same-LAN alternate when `INPUT_RELAY_LAN_HOST` is set.
    pub fn lan_advertise_endpoint(&self) -> Option<String> {
        let h = self.lan_host.trim();
        if h.is_empty() {
            None
        } else {
            Some(format!("{h}:{}", self.advertise_port))
        }
    }

    /// Pick public vs LAN advertise. `prefer_lan` when every seated WS peer is
    /// on a private/loopback address (split-horizon / no hairpin).
    pub fn pick_advertise_endpoint(&self, prefer_lan: bool) -> String {
        if prefer_lan {
            if let Some(lan) = self.lan_advertise_endpoint() {
                return lan;
            }
        }
        self.advertise_endpoint()
    }

    /// Register a match session. Idempotent if the same session_id is reopened
    /// (rematch allocates a fresh session_id from the lobby).
    pub async fn open_session(
        &self,
        session_id: u32,
        player_slots: u8,
        spectator_slots: u8,
    ) -> Result<()> {
        if !self.enabled {
            bail!("input relay disabled");
        }
        if session_id == 0 {
            bail!("invalid session_id");
        }
        let players = player_slots.clamp(2, MAX_PLAYER_SLOTS as u8);
        let spectators = spectator_slots.min(MAX_SPECTATOR_SLOTS as u8);
        let slots = players.saturating_add(spectators).min(MAX_SLOTS as u8);
        let mut g = self.inner.lock().await;
        g.sessions.insert(
            session_id,
            RelaySession {
                slot_count: slots,
                player_slots: players,
                slots: [None; MAX_SLOTS],
                last_rx: Instant::now(),
            },
        );
        metrics::input_relay_session_opened();
        g.publish_gauges();
        debug!(
            session_id,
            slot_count = slots,
            player_slots = players,
            "input relay session opened"
        );
        Ok(())
    }

    pub async fn close_session(&self, session_id: u32) {
        if session_id == 0 {
            return;
        }
        let mut g = self.inner.lock().await;
        if g.sessions.remove(&session_id).is_some() {
            metrics::input_relay_session_closed();
            g.publish_gauges();
            debug!(session_id, "input relay session closed");
        }
    }

    /// `(allocated sessions, sessions with recent two-seat traffic)`.
    pub async fn session_counts(&self) -> (usize, usize) {
        let g = self.inner.lock().await;
        (g.sessions.len(), g.active_count())
    }
}

fn read_u16_le(buf: &[u8], off: usize) -> Option<u16> {
    Some(u16::from_le_bytes([*buf.get(off)?, *buf.get(off + 1)?]))
}

fn read_u32_le(buf: &[u8], off: usize) -> Option<u32> {
    Some(u32::from_le_bytes([
        *buf.get(off)?,
        *buf.get(off + 1)?,
        *buf.get(off + 2)?,
        *buf.get(off + 3)?,
    ]))
}

fn packet_local_slot(pkt_type: u16, buf: &[u8]) -> Option<u8> {
    // START and DELAY_SYNC have no local_slot at body[0].
    if pkt_type == RNET_PKT_START || pkt_type == RNET_PKT_DELAY_SYNC {
        return None;
    }
    buf.get(HEADER_LEN).copied()
}

fn bind_slot(slot: &mut Option<SlotBinding>, peer: SocketAddr, local_dst: Option<Ipv4Addr>) {
    match slot {
        Some(b) => {
            b.peer = peer;
            if local_dst.is_some() {
                b.local_dst = local_dst;
            }
        }
        None => {
            *slot = Some(SlotBinding { peer, local_dst });
        }
    }
}

async fn recv_one(sock: &UdpSocket, buf: &mut [u8]) -> io::Result<RecvSas> {
    loop {
        sock.readable().await?;
        match sock.try_io(Interest::READABLE, || udp_pktinfo::try_recv_sas(sock, buf)) {
            Ok(v) => return Ok(v),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
}

async fn send_one(
    sock: &UdpSocket,
    buf: &[u8],
    peer: SocketAddr,
    source: Option<Ipv4Addr>,
) -> io::Result<()> {
    if let Some(src) = source {
        loop {
            sock.writable().await?;
            match sock.try_io(Interest::WRITABLE, || {
                udp_pktinfo::try_send_sas(sock, buf, peer, src)
            }) {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => {
                    // Fall back if pktinfo send fails (e.g. address not local).
                    warn!(%peer, %src, error = %e, "IP_PKTINFO send failed; falling back to send_to");
                    sock.send_to(buf, peer).await?;
                    return Ok(());
                }
            }
        }
    }
    sock.send_to(buf, peer).await?;
    Ok(())
}

async fn recv_loop(
    sock: UdpSocket,
    inner: Arc<Mutex<RelayInner>>,
    magic: u32,
    fallback_source: Option<Ipv4Addr>,
) -> Result<()> {
    let mut buf = vec![0u8; 2048];
    loop {
        let RecvSas {
            n,
            peer: src,
            local_dst,
        } = recv_one(&sock, &mut buf).await?;
        if n < MIN_PACKET {
            metrics::input_relay_drop("short");
            continue;
        }
        let pkt = &buf[..n];
        let Some(pkt_magic) = read_u32_le(pkt, 0) else {
            continue;
        };
        if pkt_magic != magic {
            metrics::input_relay_drop("magic");
            continue;
        }
        let Some(pkt_type) = read_u16_le(pkt, 4) else {
            continue;
        };
        let Some(session_id) = read_u32_le(pkt, 6) else {
            continue;
        };

        // Collect fan-out targets under the lock, then send without holding it.
        let targets: Vec<SlotBinding> = {
            let mut g = inner.lock().await;
            let Some(sess) = g.sessions.get_mut(&session_id) else {
                metrics::input_relay_drop("unknown_session");
                continue;
            };
            sess.last_rx = Instant::now();

            let sender_slot = match packet_local_slot(pkt_type, pkt) {
                Some(slot) => {
                    if slot as usize >= sess.slot_count as usize || slot as usize >= MAX_SLOTS {
                        metrics::input_relay_drop("bad_slot");
                        continue;
                    }
                    bind_slot(&mut sess.slots[slot as usize], src, local_dst);
                    Some(slot)
                }
                None => {
                    // START / DELAY_SYNC: map by source address if known.
                    sess.slots.iter_mut().enumerate().find_map(|(i, a)| {
                        if a.as_ref().map(|b| b.peer) == Some(src) {
                            bind_slot(a, src, local_dst);
                            Some(i as u8)
                        } else {
                            None
                        }
                    })
                }
            };

            if sender_slot.is_none() && pkt_type == RNET_PKT_START {
                // Slot 0 authority — bind this source as seat 0 if empty.
                if sess.slots[0].is_none() {
                    bind_slot(&mut sess.slots[0], src, local_dst);
                }
            }

            /* The gallery is read-only, and this is where that is true.
             *
             * The binding above already happened, which is deliberate: a
             * spectator has to be on the fan-out list to receive the match,
             * and the list is built from bindings. What it does not get is
             * this: its packet stops here and reaches nobody. Enforcing it at
             * the relay rather than in the client is the whole point --
             * "spectators cannot affect the game" then holds against a
             * spectator running a patched build, which a client-side check
             * cannot promise. */
            if sender_is_spectator(sender_slot, sess.player_slots) {
                metrics::input_relay_drop("spectator");
                continue;
            }

            sess.slots
                .iter()
                .flatten()
                .copied()
                .filter(|b| b.peer != src)
                .collect()
        };

        if targets.is_empty() {
            metrics::input_relay_forwarded(0);
            continue;
        }
        let mut sent = 0u64;
        for dst in targets {
            let source = dst.local_dst.or(fallback_source);
            if send_one(&sock, pkt, dst.peer, source).await.is_ok() {
                sent += 1;
            }
        }
        metrics::input_relay_forwarded(sent);
    }
}

/// Is this packet from the gallery?
///
/// Seats at or beyond `player_slots` are spectators. An unbound sender
/// (`None`) is NOT treated as one: it has not been placed in the session yet,
/// and the existing START/first-packet binding paths above decide what it is.
/// Answering "spectator" for an unknown sender would silently mute a player
/// whose first packet arrived out of order.
fn sender_is_spectator(sender_slot: Option<u8>, player_slots: u8) -> bool {
    sender_slot.is_some_and(|slot| slot >= player_slots)
}

/// Whether `start` should open the lobby UDP SFU.
///
/// Decision is made in `ws_lobby` from seated count + waiting-room ICE path
/// reports. This helper remains for call-site clarity / tests.
pub fn wants_input_relay(use_sfu: bool) -> bool {
    use_sfu
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gallery_fits_beside_a_full_room() {
        /* The point of raising the ceiling: eight players AND four watchers,
         * not four plus four. */
        assert_eq!(MAX_SLOTS, MAX_PLAYER_SLOTS + MAX_SPECTATOR_SLOTS);
        assert!(MAX_PLAYER_SLOTS >= 8);
        assert_eq!(MAX_SPECTATOR_SLOTS, 4);
    }

    #[test]
    fn player_seats_are_forwarded() {
        for slot in 0..4u8 {
            assert!(!sender_is_spectator(Some(slot), 4), "slot {slot}");
        }
    }

    #[test]
    fn spectator_seats_are_muted() {
        /* Seat 4 in a four-player session is the first gallery seat. */
        for slot in 4..8u8 {
            assert!(sender_is_spectator(Some(slot), 4), "slot {slot}");
        }
    }

    #[test]
    fn unbound_sender_is_not_muted() {
        /* A packet we could not attribute to a seat is not evidence of a
         * spectator. Muting it would drop a player's first packet whenever it
         * arrived before the binding that names it. */
        assert!(!sender_is_spectator(None, 2));
    }

    #[test]
    fn a_session_with_no_gallery_mutes_nobody_in_range() {
        /* player_slots == slot_count: every bindable seat is a player, and
         * out-of-range seats are already rejected before this check. */
        for slot in 0..8u8 {
            assert!(!sender_is_spectator(Some(slot), 8), "slot {slot}");
        }
    }
}

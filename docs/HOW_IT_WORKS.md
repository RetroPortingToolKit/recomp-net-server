# How recomp-net-server works

This is the open-source lobby / signaling control plane for hosts that use the
`recomp-net` delay-sync library. It is a **sibling** of `recomp-net` (separate
repo) and does not ship inside that library crate/tree.

## What this server is (and is not)

| This server | Not this server |
|-------------|-----------------|
| HTTP `/v1` rooms + ICE signal relay | Guest console simulation |
| WebSocket JSON lobby for MotK / psxrecomp | Interpreting pad bits / sim state |
| Slot / `session_id` / endpoint handoff | Rollback or prediction |
| Host-relay negotiation (who relays, proof it is reachable) | Relaying or storing gameplay pad streams |
| Optional TURN credential minting | |

After WebSocket `start`, the other seats dial **one player's own relay hub**
(`transport=host`): the server decides whether that is allowed (the ask, an
advertised endpoint, every guest's `path_report`) and tells everyone where it
is, but no match datagram ever touches this process. Sim authority remains pad
**slot 0** (session host); guests may rearrange among seats 1..N−1.

## Two surfaces, one process

```text
                    ┌─────────────────────────────┐
  MotK / psxrecomp  │  WebSocket  /  or /ws       │  JSON ops: welcome,
  lobby UI ────────►│  (docs/WS_LOBBY.md)         │  create, join, list…
                    ├─────────────────────────────┤
  SNES / HTTP       │  HTTP /v1/*                 │  players, rooms,
  clients ─────────►│  (docs/LOBBY.md)            │  heartbeat, signals
                    ├─────────────────────────────┤
                    │  (no match relay)           │  see note below
                    │  SQLite (players / auth)    │
                    └─────────────────────────────┘
                              │
                              ▼ handoff
                    host_endpoint / guest_endpoint
                    (+ relay_host_slot)
                    session_id / slots
                              │
                     ┌────────┴────────┐
                     ▼                 ▼
              peer ↔ peer      peers → host's hub
            (LAN / ICE)         (host relay)
```

> **The server relays nothing.** There is no UDP input relay (SFU): the code
> is gone and `INPUT_RELAY_ENABLED` is refused at startup. A match starts only
> as a host relay (`transport: "host"`): one player's own hub carries it, and
> every other seat has proven in the waiting room that it reaches that
> endpoint. Automatch negotiates which player relays (AUTOMATCH.md §1). TURN
> credentials are unaffected: TURN is a separate coturn deployment, not this
> process.

Both surfaces share `BIND_ADDR` (default `0.0.0.0:8765` for MotK local dev).

## WebSocket lobby flow (MotK)

1. Client opens `ws://host:8765` (or `PSX_NET_LOBBY_URL`).
2. Server assigns `player_id` (`welcome`).
3. Host `create`s a lobby (optional salted password hash); guests `list` / `join`.
4. Server rewrites bind addresses (`0.0.0.0` → peer TCP IP) into
   `host_endpoint` / `guest_endpoint`.
5. Both sides receive slot map + endpoints (`created` / `joined` /
   `lobby_update`). On `start`, the server checks the host-relay proof and
   launches `transport=host`; `host_endpoint` is the relay player's own
   advertised address (empty when the room asked for `relay_via: "ice"`,
   where guests reach the host over ICE signalled via `signal` and no
   endpoint is required).
6. Clients start `recomp-net` LAN sessions with peer = that hub. The
   WebSocket stays up for list / ICE `signal`.

## HTTP `/v1` flow

See [LOBBY.md](LOBBY.md). Used by hosts that prefer REST room create/join and
push ICE envelopes through `/v1/rooms/.../signals`. Same trust boundary: server
authoritative for membership and `session_id`, not for sim state.

## ICE / TURN (optional)

LAN UDP works without Coturn. For NAT traversal, peers use ICE (libjuice);
this server relays signaling and can mint TURN credentials. Coturn runs
beside the lobby — configure it per [COTURN.md](COTURN.md). The secret that
must stay in lockstep is `COTURN_STATIC_AUTH_SECRET` ↔ `static-auth-secret`.

## Usage metrics

| Endpoint | Use |
|----------|-----|
| `GET /stats` | JSON: live WS/HTTP lobby counts, in-match vs waiting, by-game breakdown, process totals |
| `GET /stats/ui` | Small HTML dashboard that polls `/stats` |
| `GET /metrics` | Prometheus (`recomp_*` counters/gauges + HTTP request metrics) |

Aggregates only — no display names, IPs, or ICE payloads. Details:
[PRIVACY.md](PRIVACY.md).

## Configuration

- Copy [`.env.example`](../.env.example) → `.env` for local runs.
- Secrets and auth: [SECURITY.md](SECURITY.md).
- Coturn / TURNS / nginx TLS: [COTURN.md](COTURN.md).
- Privacy of collected fields: [PRIVACY.md](PRIVACY.md).

## Running for MotK

```bash
cp .env.example .env   # BIND_ADDR=0.0.0.0:8765
cargo run
# optional: cargo run -- --debug
```

Clients default to `ws://netplay.retcomm.net:8765`. For a local
server: `PSX_NET_LOBBY_URL=ws://127.0.0.1:8765` (or `SNES_NET_LOBBY_URL`).
(default matches local MotK).

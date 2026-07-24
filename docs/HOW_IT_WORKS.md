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
| Optional UDP input relay (star fan-out) | Storing gameplay pad streams |
| Optional TURN credential minting | |

After lobby handoff, peers normally talk **directly** (LAN UDP or ICE) via
`recomp-net`. For 3+ seats, clients default to **host-as-relay** (the game host
fans out datagrams). When the host opts in with `match_caps.force_input_relay`,
`start` opens a UDP star relay on this server: clients dial one public endpoint
and the server forwards opaque delay-sync datagrams (magic + `session_id`
checked; pad bytes are never interpreted).

## Two surfaces, one process

```text
                    ┌─────────────────────────────┐
  MotK / psxrecomp  │  WebSocket  /  or /ws       │  JSON ops: welcome,
  lobby UI ────────►│  (docs/WS_LOBBY.md)         │  create, join, list…
                    ├─────────────────────────────┤
  SNES / HTTP       │  HTTP /v1/*                 │  players, rooms,
  clients ─────────►│  (docs/LOBBY.md)            │  heartbeat, signals
                    ├─────────────────────────────┤
                    │  UDP input relay (optional) │  fan-out delay-sync
                    │  SQLite (players / auth)    │
                    └─────────────────────────────┘
                              │
                              ▼ handoff
                    host_endpoint / guest_endpoint
                    (+ relay_endpoint when relaying)
                    session_id / slots
                              │
                     ┌────────┴────────┐
                     ▼                 ▼
              peer ↔ peer      peers → relay → peers
            (LAN / ICE)         (star UDP)
```

Both surfaces share `BIND_ADDR` (default `0.0.0.0:8765` for MotK local dev).

## WebSocket lobby flow (MotK)

1. Client opens `ws://host:8765` (or `PSX_NET_LOBBY_URL`).
2. Server assigns `player_id` (`welcome`).
3. Host `create`s a lobby (optional salted password hash); guests `list` / `join`.
4. Server rewrites bind addresses (`0.0.0.0` → peer TCP IP) into
   `host_endpoint` / `guest_endpoint`.
5. Both sides receive slot map + endpoints (`created` / `joined` /
   `lobby_update`). On `start`, if input relay is selected, both endpoints
   (and `relay_endpoint`) are rewritten to the public relay address.
6. Clients start `recomp-net` LAN sessions (peer = other endpoint, or the
   relay). The WebSocket stays up for list / ICE `signal`; pad fan-out uses
   the separate UDP relay when enabled.

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
| `GET /stats` | JSON: live WS/HTTP lobby counts, by-game breakdown, process totals |
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

Clients default to `ws://netplay.technicallycomputers.ca:8765`. For a local
server: `PSX_NET_LOBBY_URL=ws://127.0.0.1:8765` (or `SNES_NET_LOBBY_URL`).
(default matches local MotK).

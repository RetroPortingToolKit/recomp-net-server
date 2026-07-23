# How recomp-net-server works

This is the open-source lobby / signaling control plane for hosts that use the
`recomp-net` delay-sync library. It is a **sibling** of `recomp-net` (separate
repo) and does not ship inside that library crate/tree.

## What this server is (and is not)

| This server | Not this server |
|-------------|-----------------|
| HTTP `/v1` rooms + ICE signal relay | Guest console simulation |
| WebSocket JSON lobby for MotK / psxrecomp | Delay-sync INPUT exchange |
| Slot / `session_id` / endpoint handoff | Rollback or prediction |
| Optional TURN credential minting | Storing gameplay pad streams |

After lobby handoff, peers talk **directly** (LAN UDP or ICE) via `recomp-net`.
The server never sees or forwards gameplay inputs.

## Two surfaces, one process

```text
                    ┌─────────────────────────────┐
  MotK / psxrecomp  │  WebSocket  /  or /ws       │  JSON ops: welcome,
  lobby UI ────────►│  (docs/WS_LOBBY.md)         │  create, join, list…
                    ├─────────────────────────────┤
  SNES / HTTP       │  HTTP /v1/*                 │  players, rooms,
  clients ─────────►│  (docs/LOBBY.md)            │  heartbeat, signals
                    ├─────────────────────────────┤
                    │  SQLite (players / auth)    │
                    └─────────────────────────────┘
                              │
                              ▼ handoff only
                    host_endpoint / guest_endpoint
                    session_id / slots
                              │
                              ▼
                    peers ←── recomp-net ──→ peers
                         (LAN UDP or ICE)
```

Both surfaces share `BIND_ADDR` (default `0.0.0.0:8765` for MotK local dev).

## WebSocket lobby flow (MotK)

1. Client opens `ws://host:8765` (or `PSX_NET_LOBBY_URL`).
2. Server assigns `player_id` (`welcome`).
3. Host `create`s a lobby (optional salted password hash); guests `list` / `join`.
4. Server rewrites bind addresses (`0.0.0.0` → peer TCP IP) into
   `host_endpoint` / `guest_endpoint`.
5. Both sides receive slot map + endpoints (`created` / `joined` /
   `lobby_update`) and start a local `recomp-net` LAN (or ICE) session.
6. Lobby connection can stay up for list updates / ICE `signal` relay; it is
   not on the input path.

## HTTP `/v1` flow

See [LOBBY.md](LOBBY.md). Used by hosts that prefer REST room create/join and
push ICE envelopes through `/v1/rooms/.../signals`. Same trust boundary: server
authoritative for membership and `session_id`, not for sim state.

## ICE / TURN (optional)

LAN UDP works without Coturn. For NAT traversal, peers use ICE (libjuice);
this server relays signaling and can mint TURN credentials. Coturn runs
beside the lobby — configure it per [COTURN.md](COTURN.md). The secret that
must stay in lockstep is `COTURN_STATIC_AUTH_SECRET` ↔ `static-auth-secret`.

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

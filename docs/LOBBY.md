# Lobby API (v1)

HTTP contract for game-filtered lobbies and ICE signaling for **recomp-net**
hosts.

Related: [SECURITY.md](SECURITY.md). Peers use the open `recomp-net` library for
delay-sync INPUT exchange after this server hands out `RNetConfig` fields and
(optionally) relays ICE signals.

## Trust boundaries

| Signal | Trust | Use |
|--------|--------|-----|
| `game_id` | Client-reported | Room filter key; validate against allowlist when configured. |
| `client_version` | Client-reported | Must match across peers in a room. |
| `udp_endpoint` / `lan_endpoint` | Client-reported | Optional LAN helpers; ICE path preferred online. |
| ICE SDP / candidates | Client-reported | Relayed 1:1; not validated as legitimate libjuice output. |
| Match/lobby membership | Server | Authoritative for slots and `session_id`. |

## Environment (lobby)

| Variable | Meaning |
|----------|---------|
| `DATABASE_URL` | SQLite URL; if unset, defaults to `sqlite:recomp-net-server.db?mode=rwc`. |
| `LOBBY_DEFAULT_INPUT_DELAY` | Default delay `D` for new rooms (default `2`). |
| `LOBBY_DEFAULT_SLOT_COUNT` | Default slots (default `2`; recomp-net transport is 2p-first). |
| `LOBBY_ROOM_IDLE_SECS` | Evict empty/idle rooms (default `600`). |
| `LOBBY_HEARTBEAT_TIMEOUT_SECS` | Drop members without heartbeat (default `30`). |
| `LOBBY_PROTOCOL_MAGIC` | Hex or decimal `protocol_magic` (default `0x524E4554` / `"RNET"`). |
| `LOBBY_GAME_ALLOWLIST` | Comma-separated `game_id` values; empty = allow any non-empty id (dev). |

Coturn variables match BattleShip-Server’s TURN credential shape; see `.env.example`.

## Game identity

Recommended `game_id` forms (stable, filterable):

```text
snes:sha256:<64-hex>          # preferred for SNES recomp ROM content hash
snes:crc32:<8-hex>            # weaker; only if SHA unavailable
psx:sha256:<64-hex>           # reserved for a future psxrecomp host
```

Optionally append build pins in `client_version`, e.g. `snesrecomp-smw/0.1.0+lle`.

Peers in one room must share the same `game_id` and `client_version`.

## Identity

1. `POST /v1/players` — create an anonymous player. Response includes `player_id`
   and `api_token` (show once; stored hashed server-side).
2. Authenticated requests send:
   - Header `X-Player-Id: <uuid>`
   - Header `Authorization: Bearer <api_token>`

## Endpoints

### `GET /health`

Liveness + auth/db configuration flags.

### `POST /v1/players`

**Response:** `{ "player_id": "uuid", "api_token": "hex..." }`

### `GET /v1/games`

Lists allowlisted games when `LOBBY_GAME_ALLOWLIST` is set; otherwise returns
`{ "mode": "open", "games": [] }` (dev).

### `POST /v1/rooms`

Create a lobby.

**Body:**

```json
{
  "game_id": "snes:sha256:...",
  "client_version": "snesrecomp-smw/0.1.0",
  "display_name": "evening smash",
  "slot_count": 2,
  "input_delay": 2,
  "is_private": false
}
```

**Response:** room summary including `room_id`, `join_code` (for private rooms),
and creator membership with `local_slot = 0` (sim authority).

### `GET /v1/rooms?game_id=...`

List **public**, joinable rooms for a `game_id` (and optional `client_version`).

### `POST /v1/rooms/{room_id}/join`

Join by id (public) or with `{ "join_code": "..." }` for private rooms.

Assigns the next free `local_slot`. Rejects mismatched `game_id` /
`client_version` / full rooms.

### `POST /v1/rooms/{room_id}/leave`

Leave the room. Host leaving may dissolve the room (current policy).

### `POST /v1/rooms/{room_id}/ready`

Body: `{ "ready": true }`. When all members are ready, room status becomes
`starting` and the response includes **`rnet`** bootstrap fields for
`rnet_session_create`:

```json
{
  "status": "starting",
  "rnet": {
    "session_id": 1234567890,
    "protocol_magic": 1380869460,
    "slot_count": 2,
    "input_delay": 2,
    "local_slot": 0,
    "you_are_sim_authority": true
  }
}
```

### `GET /v1/rooms/{room_id}`

Poll room state, members, ready flags, and `rnet` once starting/running.

### `POST /v1/rooms/{room_id}/heartbeat`

Keep membership alive while in lobby or connecting.

### `POST /v1/rooms/{room_id}/signal`

Relay a recomp-net ICE signal to peers (or a specific `target_player_id`).

**Body:**

```json
{
  "target_player_id": null,
  "type": 1,
  "flag": 0,
  "text": "v=0..."
}
```

`type` / `flag` / `text` mirror `RNetSignal` in recomp-net (`docs/signaling.md`).

### `GET /v1/rooms/{room_id}/signals`

Drain inbound signals for the calling player (polling mailbox). Future: WebSocket
upgrade on the same path family.

### `GET /v1/turn-credentials`

When coturn is configured, returns STUN/TURN hosts/ports plus ephemeral
username/password for libjuice (`RNetIceConfig`).

## Mapping to recomp-net

After `status == "starting"`:

1. Build `RNetConfig` from `rnet.*` (`local_slot` differs per peer).
2. Implement `RNetHostVTable.on_signal` → `POST .../signal`.
3. Poll `GET .../signals` → `rnet_session_push_signal`.
4. `rnet_session_start_ice` (or LAN if both peers exchange endpoints out of band).
5. Host loop: `pump` → `try_admit` → one sim step → `advance`.

## Non-goals (v1)

- Running the SNES/PSX sim on the server
- Rollback / state sync mid-session
- True N>2 mesh (rooms may advertise up to 4 slots later; transport is 2p-first)
- Publishing this repository

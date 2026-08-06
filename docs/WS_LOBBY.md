# WebSocket lobby protocol (MotK / psxrecomp)

JSON-over-WebSocket contract implemented by this server (`src/ws_lobby.rs`) and
consumed by the MotK / psxrecomp lobby client (`psx_lobby_client`).

Default listen URL: `ws://127.0.0.1:8765`  
Clients override with env `PSX_NET_LOBBY_URL`.

This document is the **client-facing wire format**. The open `recomp-net`
library does **not** include a lobby server — run this project (or another
compatible implementation) separately.

## Transport

- One WebSocket connection per player (`/` or `/ws` on the same listener as HTTP `/v1`).
- Text frames carry a single JSON object per message.
- Every message has a string `"op"` field.

## Identity

On connect the server assigns a UUID `player_id` and replies:

```json
{ "op": "welcome", "player_id": "...", "ok": true }
```

Client may then send:

```json
{ "op": "hello", "display_name": "Alex" }
```

## Listing

Client → server:

```json
{ "op": "list" }
```

Server → clients (on request, on change, and ~1 Hz while anyone is connected):

```json
{
  "op": "lobby_list",
  "lobbies": [
    {
      "lobby_id": "...",
      "name": "Friday Fights",
      "game_name": "Star Wars: Masters of Teras Kasi",
      "game_version": "0.1.0",
      "player_count": 1,
      "max_slots": 2,
      "has_password": true,
      "host_endpoint": "203.0.113.10:7777",
      "lan_endpoints": ["192.168.1.42:7777"]
    }
  ]
}
```

Passwords are never listed — only `has_password`.
`host_endpoint` is the public/STUN UDP address for list latency.
`lan_endpoints` is deprecated for privacy: current clients discover same-LAN
hosts via a local UDP broadcast beacon (`RNETBC1`) keyed by `lobby_id`, and
omit private IPs from the hub. The field remains accepted/echoed for older
clients. At create, `host_endpoint` is the TCP-peer rewrite of `host_bind`;
the host should follow up with `set_host_endpoint` after STUN.

## Set host endpoint (STUN + LAN advertise)

Host-only, while in a lobby (before launch / input-relay rewrite):

```json
{
  "op": "set_host_endpoint",
  "host_endpoint": "203.0.113.10:54321"
}
```

Success: `{ "op": "host_endpoint_ok", "ok": true }` plus `lobby_update` and a
list broadcast. Rejects `0.0.0.0`, port `0`, and updates after the server has
opened an input-relay session (`relay_locked`). `lan_endpoints` are capped at
4 and filtered to RFC1918 only.

`list` may optionally include `game_name` and/or `game_version` to filter
the response (exact match). Broadcast / periodic list pushes remain
unfiltered; clients should filter locally to their own title + release.

## Create

```json
{
  "op": "create",
  "name": "Friday Fights",
  "game_name": "Star Wars: Masters of Teras Kasi",
  "game_version": "0.1.0",
  "password": "optional",
  "max_slots": 2,
  "host_bind": "0.0.0.0:7777",
  "display_name": "Host",
  "match_caps": {
    "v": 1,
    "aspect_num": 4,
    "aspect_den": 3,
    "turbo_loads": true,
    "bios_hle": true,
    "fast_boot": false,
    "auto_skip_fmv": false,
    "input_delay": 2,
    "language": "en"
  }
}
```

`game_version` is the release / build pin for this title (semver or tag).
Omitted or empty → stored as `"dev"` (local/dev builds). Peers must match
exactly to join.

`match_caps` is optional. When present it must be a JSON object (≤2048
bytes serialized). The server stores it opaquely and echoes it on
`created` / `joined` / `lobby_update` / `launch` so guests can apply the
host’s sim-affecting settings at boot. Present-only settings (renderer,
fullscreen, filters) stay local.

Success:

```json
{
  "op": "created",
  "ok": true,
  "lobby_id": "...",
  "session_id": 1,
  "local_slot": 0,
  "host_endpoint": "127.0.0.1:7777",
  "slots": [{ "slot": 0, "player_id": "...", "display_name": "Host" }]
}
```

`host_endpoint` is the address peers should use as the LAN peer
(server rewrites `0.0.0.0` / `*` to the connecting client’s TCP peer IP when possible).

## Join

```json
{
  "op": "join",
  "lobby_id": "...",
  "password": "optional",
  "guest_bind": "0.0.0.0:7778",
  "display_name": "Guest",
  "game_name": "Star Wars: Masters of Teras Kasi",
  "game_version": "0.1.0"
}
```

`game_version` is normalized like create (`""` → `"dev"`). When present,
`game_name` must also match the lobby.

Outcomes:

| `op` / fields | Meaning |
|---------------|---------|
| `joined` + `ok:true` | In room; includes `local_slot`, `session_id`, endpoints |
| `error` + `code:"need_password"` | Lobby is locked; retry with password |
| `error` + `code:"bad_password"` | Wrong password |
| `error` + `code:"version_mismatch"` | Guest `game_version` ≠ lobby |
| `error` + `code:"game_mismatch"` | Guest `game_name` ≠ lobby |
| `error` + `code:"full"` / `"gone"` | Cannot join |

On successful join every member receives `lobby_update` with the new slot map,
peer endpoints, and ready flags. Membership changes clear everyone’s ready so
players must re-confirm.

## Lobby room (`lobby_update`)

Server → members (after join, leave, set_ready, etc.):

```json
{
  "op": "lobby_update",
  "lobby_id": "...",
  "session_id": 1,
  "host_endpoint": "127.0.0.1:7777",
  "guest_endpoint": "127.0.0.1:7778",
  "player_count": 2,
  "max_slots": 2,
  "host_player_id": "...",
  "all_ready": false,
  "slots": [
    { "slot": 0, "player_id": "...", "display_name": "Host", "ready": true },
    { "slot": 1, "player_id": "...", "display_name": "Guest", "ready": false }
  ]
}
```

## Ready / start / launch

Client → server (any member in a lobby):

```json
{ "op": "set_ready", "ready": true }
```

Host may update caps while in the room (broadcasts `lobby_update`):

```json
{ "op": "set_match_caps", "match_caps": { "v": 1, "…": "…" } }
```

Errors: `not_in_lobby`, `not_host`, `gone`, `bad_match_caps`.

Client → server (host only; requires `player_count >= 2`). Online start always
opens the lobby UDP SFU star (`INPUT_RELAY_*`) and rewrites
`host_endpoint` / `guest_endpoint` / `relay_endpoint` to the advertise address.
Peers dial the SFU only — no host-as-relay and no guest↔guest mesh on the
WebSocket path. `match_caps.force_input_relay` is retained for older clients
but does not gate relay open. Ready flags are informational only — host Play
is the launch authority:

```json
{ "op": "start", "match_caps": { "v": 1, "…": "…" } }
```

Optional `match_caps` on `start` overwrites the lobby’s stored blob so launch
freezes the host’s latest settings. Errors: `not_in_lobby`, `not_host`,
`need_players`, `relay_unavailable`.

On success the server:

1. Allocates a **new** `session_id` (monotonic) for this match — rematch after
   return-to-lobby must not reuse the previous UDP session id (stale HELLO/BYE).
2. Opens a UDP SFU session and sets `host_endpoint` / `guest_endpoint` (and
   `relay_endpoint`) to the advertised relay address
   (`INPUT_RELAY_ADVERTISE_HOST`:`INPUT_RELAY_ADVERTISE_PORT`). The host
   defaults from `PUBLIC_HOST` / `LOBBY_PUBLIC_HOST`, otherwise startup
   STUN-discovers this machine’s public IPv4 (never `127.0.0.1` unless
   `INPUT_RELAY_ALLOW_LOOPBACK=1`). When **every** seated member’s WebSocket
   TCP peer IP is a *direct* RFC1918/loopback address (not the LAN gateway /
   hairpin source) and `INPUT_RELAY_LAN_HOST` is set, launch uses that LAN
   host instead. Peers that dial the public DNS and NAT-hairpin often appear
   as the router (`.1`); those keep the public advertise. Override the
   gateway with `INPUT_RELAY_LAN_GATEWAY` if it is not `<LAN>/24` → `.1`.
   MotK clients may also rewrite the relay host to a private WebSocket peer.
   On Linux the SFU uses `IP_PKTINFO` so forwarded datagrams are sourced from
   the local address each peer dialed (avoids dual-NIC wrong-source drops).
3. Clears every slot’s `ready` (clients auto-ready again for rematch).
4. Broadcasts to **all** members:

```json
{
  "op": "launch",
  "ok": true,
  "lobby_id": "...",
  "session_id": 2,
  "host_endpoint": "…",
  "guest_endpoint": "…",
  "relay_endpoint": "public.example:8777",
  "player_count": 2,
  "max_slots": 2,
  "slots": [ … ],
  "match_caps": { "v": 1, "…": "…" }
}
```

`relay_endpoint` is present only when the server opened an input-relay
session. Each client then starts delay-sync with the LAN endpoints from the
message (local bind from create/join; peer = the other endpoint, or the
relay when `relay_endpoint` / force-relay is set). Clients must refuse to
boot netplay when the peer endpoint is empty. Guests apply `match_caps`
(when present) before booting so both peers share sim-affecting settings.

## Leave / close / kick

```json
{ "op": "leave" }
```

Host may remove a guest (not the host player / not self):

```json
{ "op": "kick", "slot": 1 }
```

Errors: `not_host`, `bad_slot`, `empty_slot`, `cannot_kick`.

The kicked player receives `{ "op": "kicked", "ok": true, "lobby_id": "…" }`.
Remaining members get `lobby_update` (ready flags cleared, like guest leave).

Host may swap (or move into an empty) **guest** seat. Slot 0 is the session
host / sim authority and stays pinned; rearranging is only among seats
1..max_slots−1. Clears ready flags and broadcasts `lobby_update` so every
peer refreshes the member table and `local_slot`:

```json
{ "op": "move", "from_slot": 1, "to_slot": 2 }
```

`slot` is accepted as an alias for `from_slot`. Errors: `not_in_lobby`,
`not_host`, `gone`, `bad_slot`, `empty_slot`, `host_slot_fixed`.

Host disconnect or `{ "op": "close" }` destroys the lobby and notifies members
with `lobby_closed`.

## Signal relay

```json
{
  "op": "signal",
  "lobby_id": "...",
  "to_player_id": "",
  "type": 1,
  "flag": 0,
  "text": "..."
}
```

Server forwards to the other member(s). Used for ICE (`RNetSignal`);
LAN delay-sync does not require it.

## TURN credentials (ICE)

WS lobby sessions only receive `player_id` on `welcome` (no HTTP Bearer
token), so snesrecomp mints Coturn credentials over the socket instead of
`GET /v1/turn-credentials`.

Client → server:

```json
{ "op": "get_turn_credentials" }
```

Server → client (Coturn configured):

```json
{
  "op": "turn_credentials",
  "ok": true,
  "stun_host": "coturn.example.com",
  "stun_port": 3478,
  "turn_host": "coturn.example.com",
  "turn_port": 3478,
  "turns_port": 5349,
  "realm": "recomp-net",
  "username": "<expiry>:<player_id>",
  "password": "<base64 HMAC>",
  "ttl_secs": 86400
}
```

Server → client when Coturn env is missing or mint fails:

```json
{ "op": "turn_credentials", "ok": false, "error": "coturn_unconfigured" }
```

Same HMAC mint as HTTP (`docs/COTURN.md`). Clients should request after
`welcome` / before ICE gather; ICE still prefers host/srflx over relay.

## Keepalive

Either side may send `{ "op": "ping" }`; reply is `{ "op": "pong" }`.

## Related

- Architecture: [HOW_IT_WORKS.md](HOW_IT_WORKS.md)
- HTTP `/v1` rooms API: [LOBBY.md](LOBBY.md)
- Privacy: [PRIVACY.md](PRIVACY.md)

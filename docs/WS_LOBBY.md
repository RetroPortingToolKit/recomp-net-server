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
      "lan_endpoints": ["192.168.1.42:7777"],
      "host_country": "JP",
      "allow_spectators": true,
      "max_spectators": 4,
      "spectator_count": 1
    }
  ],
  "players": [
    { "display_name": "Marisa", "country": "JP", "lobby_id": "...",
      "lobby_name": "Friday Fights", "hosting": true, "tag": "3f9a1c02" },
    { "display_name": "Reimu", "country": "DE", "lobby_id": "",
      "lobby_name": "", "hosting": false, "tag": "b71e40d9" }
  ]
}
```

`tag` is the first 8 characters of that connection's player id: a client
finds its own row by comparing with its id, since display names are not
unique across the hub. Each row also carries `game_name`, the title that
client last listed for; a filtered `list` (with `game_name`) returns only
the players of that title, and the unfiltered broadcast carries everyone,
for the client to filter.

### Server chat (per game)

```json
{ "op": "server_chat", "text": "anyone up for a set?" }
```

Relayed, after the profanity filter, to every client whose last `list`
named the same `game_name` as the sender -- seated in a room or not:

```json
{ "op": "server_chat", "game_name": "…", "from_player_id": "…",
  "from": "Marisa", "country": "JP", "text": "anyone up for a set?" }
```

No history is kept. A client that has not listed for a title yet gets
`error` / `no_game`.

`allow_spectators` / `max_spectators` / `spectator_count` describe the
lobby's gallery (a browser shows "No" or "1/4"). `players` is everyone
connected to the hub, seated or browsing, sorted by name; `lobby_id` is
empty for a player who is only browsing. Both are additive: older clients
ignore them.

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
  "disc_fp": "0123…64 hex chars…abcd",
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

`disc_fp` is an optional lowercase hex SHA-256 of the mounted disc TOC
(geometry fingerprint from the client). Omitted / empty / invalid → stored
empty (legacy). When either the lobby or the joiner has a non-empty
fingerprint, both must match or join fails with `disc_mismatch`. This
catches Track-01-only dumps vs full multi-track cues even when data-track
hashes agree.

`match_caps` is optional. When present it must be a JSON object (≤4096
bytes serialized). The server stores it opaquely and echoes it on
`created` / `joined` / `lobby_update` / `launch` so guests can apply the
host’s sim-affecting settings at boot. Present-only settings (renderer,
fullscreen, filters) stay local. Optional `mods` is the host’s required
package list (`[{id,ver,n,f,b,size},…]`); the match runs those packages
and enabled features, not vanilla.

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
  "game_version": "0.1.0",
  "disc_fp": "0123…64 hex chars…abcd",
  "mod_offer": { "v": 1, "pkgs": [ { "id": "psx.foo", "ver": "1.0.0" } ] }
}
```

`game_version` is normalized like create (`""` → `"dev"`). When present,
`game_name` must also match the lobby. `mod_offer` is the guest’s installed
package catalog (≤2048 bytes). After password / game / disc checks, the
server compares it to `match_caps.mods` **before seating**.

Outcomes:

| `op` / fields | Meaning |
|---------------|---------|
| `joined` + `ok:true` | In room; includes `local_slot`, `session_id`, endpoints |
| `need_mods` + `ok:false` | Password ok, not seated; install missing packages then rejoin |
| `error` + `code:"need_password"` | Lobby is locked; retry with password |
| `error` + `code:"bad_password"` | Wrong password |
| `error` + `code:"version_mismatch"` | Guest `game_version` ≠ lobby |
| `error` + `code:"game_mismatch"` | Guest `game_name` ≠ lobby |
| `error` + `code:"disc_mismatch"` | Guest `disc_fp` ≠ lobby (or one side empty) |
| `error` + `code:"full"` / `"gone"` | Cannot join |

On successful join every member receives `lobby_update` with the new slot map,
peer endpoints, and ready flags. Membership changes clear everyone’s ready so
players must re-confirm.

## Missing mods (pre-join transfer)

When `match_caps.mods` lists packages the guest’s `mod_offer` does not contain,
the server does **not** seat the player. It stores a password-ok grant
(`pending_mod_lobby`) and replies:

```json
{
  "op": "need_mods",
  "ok": false,
  "code": "need_mods",
  "lobby_id": "...",
  "host_player_id": "...",
  "mods": [ { "id": "psx.foo", "ver": "1.0.0", "n": "Foo", "f": "wide", "b": true, "size": 0 } ],
  "can_transfer": true
}
```

The client prompts before fully joining. Accept starts a **host↔guest ICE
data path** (same libjuice stack as waiting-room RTT). Package bytes never
cross the lobby WebSocket — the VPS only relays tiny SDP/candidate JSON.
LAN/direct matches have no transfer path (join vanilla). There is no
lobby-imposed archive size cap; STORE zip32 still cannot exceed 4 GiB per
file/archive.

```json
{ "op": "mod_xfer_start", "lobby_id": "..." }
```

Server → lobby host:

```json
{ "op": "mod_xfer_pull", "from_player_id": "…guest…", "lobby_id": "…", "mods": [ … ] }
```

ICE signaling (pending guest ↔ host only; **not** seated `signal`). Host is
ICE-controlling. Relayed only if the sender is the lobby host or a client
with `pending_mod_lobby` for that room:

```json
{
  "op": "mod_signal",
  "lobby_id": "…",
  "to_player_id": "…peer…",
  "type": 1,
  "flag": 0,
  "text": "…"
}
```

Abort / export failure (still WS; no file bytes):

```json
{ "op": "mod_xfer_fail", "to_player_id": "…guest…", "error": "export failed" }
{ "op": "mod_xfer_cancel" }
```

Guest installs received `.psxmod` zips, then sends `join` again with an updated
`mod_offer`. Cancel clears `pending_mod_lobby` (not seated). If ICE cannot
hole-punch, juice may fall back to TURN (same Coturn as waiting-room ICE) —
that is last-resort connectivity, not lobby-VPS file relay.

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

### Country flags (GeoIP)

With `GEOIP_DB_PATH` pointing at a MaxMind Country database (`GeoLite2-Country.mmdb`
or GeoIP2), every client's country is resolved once, at connect, from its TCP
source address, and published as ISO 3166-1 alpha-2: `"country"` on each
member row of `lobby_update` / `launch`, and `"host_country"` on each
`lobby_list` row. The field is omitted / empty when the database is not
configured, the address is private or loopback, or the lookup has no answer.
Clients draw it as a flag before the name. Nothing else depends on it.

**Flags work with nothing installed.** A country table built from the Regional
Internet Registries' published delegation records is committed at
`data/ip_country.bin` and compiled into the binary, so every deployment
resolves the same address to the same country with no account, no licence key
and no download. `GEOIP_DB_PATH` is now an *accuracy upgrade*, not a
prerequisite: where MaxMind has a record it wins, and where it does not the
built-in table still answers, so adding it can only improve coverage.

The trade is that RIR records give the country a range was ALLOCATED to rather
than where it is used today, so a multinational holder or a re-routed block can
report the registrant's country. Regenerate the table with
`python3 tools/gen_ip_country.py`; quarterly is plenty.

**No flags? The startup log says which state you are in.** `using the built-in
RIR country table`, `GeoIP database not loaded` (with the error, and the
built-in table still answering), or `GeoIP country database loaded; flags on`.
Run at `RUST_LOG=debug` and each connect logs why a lookup came back empty.

**Behind a reverse proxy, set `TRUST_PROXY_HEADER=1`.** The country is
resolved from the TCP source address, which behind a proxy is the proxy --
usually loopback, which is private, so *every* player is flagless even with a
working database. With this set, the left-most `X-Forwarded-For` entry is used
instead. It is off by default and must stay off unless a proxy really is in
front: the header is trivially spoofable by anyone reaching the server
directly, and trusting it would let a client choose its own flag.

### Lobby chat

Relayed lines pass through the profanity / slur filter first
(`src/chat_filter.rs`; word list `data/chat_filter_words.txt`, a copy of
recomp-net's `data/chat_filter_words.txt` -- keep them identical). Matches
become one `*` per character; matching folds case, Latin diacritics,
full-width ASCII and leetspeak, and tolerates repeated or spaced-out
letters. `CHAT_FILTER=0` disables it; `CHAT_FILTER_EXTRA_PATH` appends a
file of extra entries in the same format. Clients run the same filter on
every line they display, so a LAN room without a server is filtered too.

Any seated member (player or spectator) may send a line; the server echoes it
to **everyone seated, sender included**, so the room's order is the server's
order and no client appends its own line. Lines are not stored — a late joiner
sees only what arrives after them. Text is trimmed, control characters are
dropped, and it is capped at 240 characters; an empty line is ignored.

```json
{ "op": "chat", "text": "gg last time, ready when you are" }
```

Server → members:

```json
{ "op": "chat", "lobby_id": "…", "from_player_id": "…", "from": "Marisa",
  "text": "gg last time, ready when you are" }
```

Errors: `not_in_lobby`.

### Host in the gallery

If the host seated itself in the gallery, `start` still succeeds (two seated
*players* are required). The launch message carries `"host_spectates": true`,
the relay is opened with one extra player slot, and `spectator_relay_base`
moves up by one: the host runs the match from session slot 0 with a muted pad
and every player seat is its lobby seat + 1 in session terms.

The server also emits **system lines** on membership changes, to everyone
seated, with no sender and `"system": true`: `"<name> has joined."`,
`"<name> has joined as a spectator."`, `"<name> has left."`,
`"<name> was kicked."`.

```json
{ "op": "chat", "lobby_id": "…", "from_player_id": "", "from": "",
  "system": true, "text": "Marisa has joined." }
```

Client → server (host only; requires seated `player_count >= 2`). Online MotK/BPE
lobbies **always** open the lobby UDP SFU (`transport=sfu`, §108). Waiting-room
ICE `path_report` is telemetry only and does **not** select `ice_p2p`.
`match_caps.force_turn` is a client delay-floor hint, not a transport switch.
Ready flags are informational only — host Play is the launch authority:

```json
{ "op": "start", "match_caps": { "v": 1, "…": "…" } }
```

Optional `match_caps` on `start` overwrites the lobby’s stored blob so launch
freezes the host’s latest settings. Errors: `not_in_lobby`, `not_host`,
`need_players`, `relay_unavailable` (SFU required but relay not configured).

On success the server:

1. Allocates a **new** `session_id` (monotonic) for this match — rematch after
   return-to-lobby must not reuse the previous UDP session id (stale HELLO/BYE).
2. **SFU:** opens a UDP SFU session and sets `host_endpoint` /
   `guest_endpoint` / `relay_endpoint` to the advertised relay address
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
  "transport": "sfu",
  "player_count": 2,
  "max_slots": 2,
  "slots": [ … ],
  "match_caps": { "v": 1, "…": "…" }
}
```

`transport` is `"sfu"`. `relay_endpoint` is present on every successful online
start. Each client starts netplay with LAN transport to the relay. Guests apply
`match_caps` (when present) before booting so both peers share sim-affecting
settings. Direct IP / LAN file lobbies (no MotK WS seat) stay on local UDP and
do not use this path.

## Path report (waiting-room ICE, telemetry)

While seated (2 players), ICE-capable clients may still report the selected
candidate type from the waiting-room RTT probe (delay hints / diagnostics).
This no longer affects match transport:

```json
{ "op": "path_report", "path": "direct" }
```

`path` is `direct` | `relay` | `fail` (aliases: `host`/`srflx`/`prflx` →
`direct`, `failed`/`none` → `fail`). Success: `{ "op": "path_report_ok",
"ok": true, "path": "direct" }`. Join/leave/kick clears stored paths.

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

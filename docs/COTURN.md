# Coturn for ICE / TURN

`recomp-net-server` can mint short-lived TURN credentials for
`GET /v1/turn-credentials` (libjuice / `RNetIceConfig`). Coturn itself is a
**separate** process: this repo does not ship or start it.

The big takeaway: **`COTURN_STATIC_AUTH_SECRET` in the server `.env` must be
identical to coturn’s `static-auth-secret`.** If they drift, credential minting
still returns values, but TURN allocation fails at the edge.

## What the lobby server expects

| Server env | Coturn side | Notes |
|------------|-------------|--------|
| `COTURN_STATIC_AUTH_SECRET` | `static-auth-secret=…` with `use-auth-secret` | Shared HMAC secret (REST / time-limited credentials). **Must match exactly.** |
| `COTURN_REALM` | `realm=…` | Same realm string clients and coturn use. |
| `COTURN_HOST` (or `COTURN_STUN_HOST` / `COTURN_TURN_HOST`) | Public hostname clients reach | Usually the DNS name on your certs (e.g. `coturn.example.com`). |
| `COTURN_STUN_PORT` / `COTURN_TURN_PORT` | `listening-port` (default `3478`) | UDP/TCP STUN/TURN. |
| `COTURN_TURNS_PORT` | `tls-listening-port` (default `5349`) | TLS/DTLS TURN (`turns:`). |

Credential algorithm: HMAC-SHA1 over `expiry:player_id` with the static auth
secret, password base64-encoded — coturn’s standard `use-auth-secret` REST
shape. See `src/turn_credentials.rs`.

You do **not** need coturn’s long-lived `user=name:password` lines for the
HTTP minting path; `lt-cred-mech` + `use-auth-secret` is enough. A static
`user=` entry is optional for manual testing outside the lobby.

## TLS and a reverse proxy

For secure, globally reachable ICE:

1. Terminate **lobby WebSocket / HTTP** with a reverse proxy (nginx, Caddy,
   etc.) using a valid public certificate (`wss://` / `https://`).
2. Give **coturn** its own valid certificate material via `cert=` / `pkey=`
   (Let’s Encrypt or copies synced into coturn’s read path). TURNS on
   `5349` needs a cert browsers and libjuice peers will trust.
3. Publish a stable DNS name for TURN (`COTURN_HOST`) that matches the cert
   SAN. Point `COTURN_STUN_HOST` / `COTURN_TURN_HOST` at that name when they
   differ from an internal bind address.

Coturn should listen on the public TURN/TURNS ports (or be DNAT’d there).
Do not put TURN UDP relay traffic through an HTTP reverse proxy; nginx fronts
the **lobby**, while coturn speaks STUN/TURN directly (with TLS on 5349).

Open the firewall for:

- `3478/udp` and `3478/tcp` (STUN/TURN)
- `5349/udp` and `5349/tcp` (TURNS / DTLS)
- Your `min-port`–`max-port` UDP relay range (e.g. `64000–65000`)

## Example `turnserver.conf` (obfuscated)

Illustrative production-shaped config. Replace secrets, IPs, and paths; do not
copy secrets into git.

```conf
listening-port=3478
tls-listening-port=5349
listening-ip=192.168.66.3
listening-ip=::
# public/private mapping when coturn is behind NAT
external-ip=216.154.76.149/192.168.66.3
relay-ip=192.168.66.3
min-port=64000
max-port=65000
fingerprint
lt-cred-mech
use-auth-secret
static-auth-secret=<same-value-as-COTURN_STATIC_AUTH_SECRET>
# optional long-lived user for manual tests; lobby minting uses auth-secret
user=netplay:<secret-pass-shared>
realm=coturn.retcomm.net
cert=/etc/turnserver/certs/fullchain.pem
pkey=/etc/turnserver/certs/privkey.pem
allow-loopback-peers
# Harden against abusing TURN as an open relay into RFC1918:
denied-peer-ip=10.0.0.0-10.255.255.255
denied-peer-ip=172.16.0.0-172.16.255.255
denied-peer-ip=192.168.0.0-192.168.255.255
cli-ip=127.0.0.1
cli-port=5766
cli-password=<secret-cli-pass>
log-file=/var/log/turnserver.log
```

### Settings that matter for recomp-net

| Directive | Why |
|-----------|-----|
| `use-auth-secret` + `static-auth-secret` | Required for lobby-minted time-limited credentials. |
| `lt-cred-mech` | Long-term credential mechanism coturn expects with auth-secret. |
| `realm` | Must match `COTURN_REALM` in `.env`. |
| `external-ip=PUBLIC/PRIVATE` | Correct candidate rewriting when the daemon binds a LAN IP. |
| `relay-ip` | Interface used for relayed media; usually the LAN bind IP. |
| `min-port` / `max-port` | UDP relay pool; must be reachable from the internet. |
| `cert` / `pkey` | Enables TURNS on `tls-listening-port` for encrypted relay. |
| `denied-peer-ip=…` | Blocks relaying into private ranges (recommended on public hosts). |
| `fingerprint` | Common for WebRTC-style ICE; keep enabled unless you know you need it off. |

`allow-loopback-peers` is useful for same-host bring-up; tighten
`allowed-peer-ip` / drop loopback allowance on a hostile public edge if you
do not need it.

## CreatePermission 403 (Forbidden IP)

libjuice may log:

```text
Got TURN CreatePermission error response, code=403
```

With the example `denied-peer-ip=` ranges above, that is **expected** whenever
ICE asks TURN to open a permission toward a **private** peer candidate
(RFC1918 / link-local). Coturn is refusing to be used as a relay into your
LAN — that is the point of those directives.

What to do:

| Situation | Action |
|-----------|--------|
| CGNAT / internet (host candidates RFC1918-only at first) | 403s for private **host** candidates are noise. recomp-net still runs automatic `force_relay` fallback so both sides gather `typ relay`; CreatePermission then targets public relay addresses. |
| Internet peers (srflx/public remotes) | Same: host-candidate 403s are noise; relay/srflx permissions should succeed. |
| Same-LAN with hardened `denied-peer-ip` | Prefer **host** ICE (or LAN transport). Force TURN / `force_relay` is a poor fit if coturn will not CreatePermission into your LAN. |
| Lab coturn on a trusted LAN | You may omit or narrow `denied-peer-ip` for that subnet. **Do not** remove all RFC1918 denials on a hostile public edge unless you accept the SSRF-style relay risk. |
| Auth / secret mismatch | Failures look different (Allocate 401/438, etc.). See secret checklist below. |

`force_relay` / Force TURN is the reliable path for hard CGNAT. Automatic
fallback (without Force TURN from the start) restarts ICE once onto relay
after host/srflx stalls.

## Matching `.env` on recomp-net-server

```bash
# Must equal turnserver.conf static-auth-secret
COTURN_STATIC_AUTH_SECRET=<same-secret>

COTURN_REALM=coturn.retcomm.net
COTURN_HOST=coturn.retcomm.net
# Optional overrides (default to COTURN_HOST / standard ports):
# COTURN_STUN_HOST=coturn.retcomm.net
# COTURN_TURN_HOST=coturn.retcomm.net
# COTURN_STUN_PORT=3478
# COTURN_TURN_PORT=3478
# COTURN_TURNS_PORT=5349
# COTURN_CREDENTIAL_TTL_SECS=86400
```

After both sides share the secret and realm, clients can mint credentials via:

- HTTP `GET /v1/turn-credentials` (Bearer auth — [LOBBY.md](LOBBY.md))
- WebSocket `{ "op": "get_turn_credentials" }` for snesrecomp WS lobby
  sessions that only have `player_id` ([WS_LOBBY.md](WS_LOBBY.md))

Both return hosts/ports plus ephemeral `username` / `password` for
`RNetIceConfig`.

## Checklist

1. Coturn running with `use-auth-secret` and a strong `static-auth-secret`.
2. Same secret in `COTURN_STATIC_AUTH_SECRET`; realm strings match.
3. Valid TLS certs on coturn for TURNS; nginx (or similar) terminates TLS for
   the lobby `wss://` / `https://` endpoint.
4. Public `external-ip` mapping and open relay port range.
5. `COTURN_HOST` is the name clients actually resolve and trust on the cert.
6. Rotate the auth secret on both coturn and the lobby if it ever leaks
   ([SECURITY.md](SECURITY.md)).

## Related

- [LOBBY.md](LOBBY.md) — `GET /v1/turn-credentials` and ICE signal relay
- [SECURITY.md](SECURITY.md) — secret handling
- [HOW_IT_WORKS.md](HOW_IT_WORKS.md) — where TURN sits in the handoff path
- [`.env.example`](../.env.example) — variable names

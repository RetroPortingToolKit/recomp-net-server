# Privacy

This document describes what **recomp-net-server** processes when players use
the lobby / signaling control plane. It is not a substitute for a product-level
privacy policy for a shipped game, but it documents the server’s data handling
for operators and clients.

## What we process

| Data | Purpose | Retention |
|------|---------|-----------|
| Display names | Show in lobby list / slot map | In-memory for connection/lobby lifetime; HTTP player rows may persist in SQLite until deleted |
| Lobby metadata (name, game name, slot counts, session id) | Matchmaking / room listing | In-memory while the lobby exists |
| Lobby passwords | Restrict join | **Never stored in plaintext.** Salted SHA-256 hashes only; discarded when the lobby is destroyed |
| Client TCP peer IP | Rewrite `0.0.0.0` / `*` bind addresses into usable LAN/WAN endpoints | Ephemeral (in-memory with the WebSocket client); not written to long-term logs by default |
| Connection duration / presence | Heartbeats, idle room eviction, list updates | Ephemeral |
| Optional JWT / player ids (HTTP `/v1`) | Auth and room membership | Per [SECURITY.md](SECURITY.md) and SQLite migrations |
| ICE signal envelopes (`text` / SDP-like payloads) | Relay only | Not persisted; forwarded then discarded |

## What we do **not** process

- Interpreted pad / sim state (relay forwards opaque `recomp-net` datagrams
  when input relay is enabled; bytes are not decoded or stored)
- Disc / ROM contents
- Screenshots, audio, or video from the guest
- Long-term plaintext lobby passwords

When input relay is active, ephemeral UDP datagrams for a match traverse the
server process for fan-out only and are not persisted.

## Logging

- Default: structured `tracing` at `info` (connect/disconnect, startup).
- `--debug` / `RUST_LOG`: more verbose request and lobby diagnostics. Operators
  should treat debug logs as potentially containing display names and IPs and
  configure log retention accordingly.
- This server does not write lobby passwords to logs.

## Metrics / usage

Operators can review aggregate usage without scraping application logs:

| Endpoint | Purpose |
|----------|---------|
| `GET /stats` | JSON snapshot: live client/lobby/room counts, live counts by `game_name` / `game_id`, per-game match starts, process-lifetime totals |
| `GET /stats/ui` | Small browser page that polls `/stats` |
| `GET /metrics` | Prometheus text exposition (HTTP request metrics + recomp_* counters/gauges) |

What metrics include:

- Counts of connects, lobby/room creates, joins, join failures (by result code),
  match starts, TURN credential mints, and ICE signal relays
- Match starts, seated-player sums, and live in-match counts labelled by
  **surface** (`ws` / `http`) and **game** — the title being played, nothing
  about who is playing it
- Gauges for currently connected WS clients, open WS lobbies, **WS matches**
  (lobbies that have `start`ed), open HTTP rooms, **HTTP rooms running**,
  allocated input-relay sessions, and **SFU-active** sessions (recent UDP from
  at least two seats)

What metrics intentionally omit:

- Display names, player ids, peer IPs, lobby passwords, and ICE SDP payloads
- Lobby names, game versions, and disc fingerprints — no metric label carries them
- Unbounded label values: the `game` label is normalized (lowercased, reduced to
  `[a-z0-9._-]`, truncated to 48 chars) and capped. With `LOBBY_GAME_ALLOWLIST`
  set, only allowlisted titles get their own label; otherwise the first
  `METRICS_GAME_LABEL_LIMIT` (default 64) distinct games do and everything past
  the cap folds into `other`. Empty / unnamed games become `unknown`. Other
  breakdowns (open lobbies, waiting rooms) stay `/stats`-only.

Process-lifetime totals reset on restart unless scraped into an external
time-series store (e.g. Prometheus + Grafana).

## Operator responsibilities

- Run over TLS at the reverse-proxy edge for public deployments.
- Do not commit `.env` secrets (see [SECURITY.md](SECURITY.md)).
- If you front this service for a commercial title, publish an end-user privacy
  notice that covers your hosting region, retention, and contact channels.

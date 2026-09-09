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
| Discord account link (`discord_id`, cached `@username` / display name / avatar hash, `netplay_handle`) | Identity for naming, reports and bans | Persisted in SQLite indefinitely; names refreshed on every sign-in. No deletion endpoint yet |
| Per-device netplay secrets | Browserless sign-in on handhelds / consoles | **Never stored in plaintext.** SHA-256 hashes only, plus a player-set label and timestamps; revoked rows are kept, marked, not deleted |
| Automatch queue tickets (opted-in titles, `game_version`, disc fingerprint, UDP binds, relay RTT estimate) | Pair two players into a match | In-memory for the ticket's life; discarded on pair, cancel or disconnect |
| Automatch accept-gate strikes | Dodge cooldown | Persisted, keyed on the account; only the last 24 h affects a cooldown; pruned per `AUTOMATCH_RETENTION_DAYS` |
| Automatch pairings (who was paired with whom, when, RTT estimate, whether it launched) | Avoid-last-opponent filter; operator support questions | Persisted; only the last `AUTOMATCH_REMATCH_COOLDOWN_SECS` affects matching; pruned per `AUTOMATCH_RETENTION_DAYS` |

## Accounts (Discord sign-in)

Sign-in is **optional**. A player who never signs in is a guest, names itself
locally, and is processed exactly as this server processed everyone before
accounts existed. Automatch is the one surface that requires an account
([AUTOMATCH.md](AUTOMATCH.md) §2).

- **Only the snowflake is identity.** `discord_id` is immutable and is the key.
  Both Discord names are mutable and a released `@username` can be claimed by
  somebody else, so neither is ever used as a key
  ([`identity.rs`](../src/identity.rs)).
- **The snowflake is never published to other players.** It does not appear in
  `lobby_list`, `lobby_update`, `launch`, or `automatch_found`. What other
  players see is `netplay_handle`, with the `@username` as a disambiguator.
- **Email is deliberately not requested.** The OAuth scope does not ask for it,
  so the server never holds one.
- Cached Discord names and avatar hash are refreshed on each sign-in and kept
  for display and for moderation audit — a stale copy in a moderation queue is
  worse than useless.
- `netplay_handle` is player-editable and presentational. Changing it does not
  change identity, and does not shed a strike, a ban, or a report.
- **Account deletion is not implemented yet.** There is no endpoint, and an
  operator's only route today is SQL. Stated rather than left to be
  discovered: when it lands it must remove the `players` row, its
  `player_secrets`, and its `automatch_strikes` / `automatch_pairings`
  together, because a half-deleted account is still an identifiable one.

## What we do **not** process

- Interpreted pad / sim state (relay forwards opaque `recomp-net` datagrams
  when input relay is enabled; bytes are not decoded or stored)
- Disc / ROM contents
- Screenshots, audio, or video from the guest
- Long-term plaintext lobby passwords
- Discord email addresses (never requested) or any Discord data beyond the
  snowflake, the two names and the avatar hash
- Match results, scores, or ratings. The sim runs on the clients; the server
  does not observe who won and records nothing that claims to

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
- Automatch queue depth, pairs formed, accepts / declines / timeouts, and
  time-to-pair, labelled by **game** and **ruleset** — never by player
- Gauges for currently connected WS clients, open WS lobbies, **WS matches**
  (lobbies that have `start`ed), open HTTP rooms, **HTTP rooms running**,
  allocated input-relay sessions, and **SFU-active** sessions (recent UDP from
  at least two seats)

What metrics intentionally omit:

- Display names, player ids, peer IPs, lobby passwords, and ICE SDP payloads
- Discord snowflakes, `@usernames`, avatars, and netplay handles
- Lobby names, game versions, and disc fingerprints — no metric label carries them
- Unbounded label values: the `game` label is normalized (lowercased, reduced to
  `[a-z0-9._-]`, truncated to 48 chars) and capped. With `LOBBY_GAME_ALLOWLIST`
  set, only allowlisted titles get their own label; otherwise the first
  `METRICS_GAME_LABEL_LIMIT` (default 64) distinct games do and everything past
  the cap folds into `other`. Empty / unnamed games become `unknown`. Other
  breakdowns (open lobbies, waiting rooms) stay `/stats`-only.

Process-lifetime totals reset on restart unless scraped into an external
time-series store (e.g. Prometheus + Grafana).

## Retention

Most of what this server handles is in-memory and dies with the connection or
the lobby. What outlives it:

| Table | Holds | Pruning |
|-------|-------|---------|
| `players` | Account link and cached Discord names | Indefinite. No deletion endpoint exists yet (see above); SQL is the only route. |
| `player_secrets` | Per-device sign-in key hashes | Until revoked; revoked rows are marked and kept so "when did that device stop working?" stays answerable |
| `automatch_strikes` | Accept-gate declines / timeouts | `AUTOMATCH_RETENTION_DAYS` (default 30). Only the last 24 h ever affects a cooldown, so the remainder is operator history, not enforcement |
| `automatch_pairings` | Who was paired with whom | `AUTOMATCH_RETENTION_DAYS` (default 30). Only `AUTOMATCH_REMATCH_COOLDOWN_SECS` (default 300) of it affects matching |

Set `AUTOMATCH_RETENTION_DAYS=0` to prune automatch rows as soon as they stop
affecting a decision. The two automatch tables record who played whom; keeping
them indefinitely is a choice an operator should make deliberately rather than
inherit from a default.

## Operator responsibilities

- Run over TLS at the reverse-proxy edge for public deployments.
- Do not commit `.env` secrets (see [SECURITY.md](SECURITY.md)).
- If you front this service for a commercial title, publish an end-user privacy
  notice that covers your hosting region, retention, and contact channels.

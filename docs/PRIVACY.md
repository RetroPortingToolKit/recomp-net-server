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

- Gameplay pad / input streams (those go peer-to-peer via `recomp-net`)
- Disc / ROM contents
- Screenshots, audio, or video from the guest
- Long-term plaintext lobby passwords

## Logging

- Default: structured `tracing` at `info` (connect/disconnect, startup).
- `--debug` / `RUST_LOG`: more verbose request and lobby diagnostics. Operators
  should treat debug logs as potentially containing display names and IPs and
  configure log retention accordingly.
- This server does not write lobby passwords to logs.

## Operator responsibilities

- Run over TLS at the reverse-proxy edge for public deployments.
- Do not commit `.env` secrets (see [SECURITY.md](SECURITY.md)).
- If you front this service for a commercial title, publish an end-user privacy
  notice that covers your hosting region, retention, and contact channels.

# recomp-net-server

Open-source lobby / signaling control plane for hosts that use the
[`recomp-net`](https://github.com/TechnicallyComputers/recomp-net) delay-sync
library. Run your own matchmaking server, or point clients at a community
instance.

It is **not** part of the `recomp-net` library tree — peers still exchange
gameplay inputs over `recomp-net` (LAN UDP or ICE) after lobby handoff.

## Role

This server owns:

- WebSocket JSON lobby for MotK / psxrecomp / SNES hosts ([docs/WS_LOBBY.md](docs/WS_LOBBY.md))
- HTTP `/v1` game-filtered rooms ([docs/LOBBY.md](docs/LOBBY.md))
- Slot and `session_id` / endpoint handoff
- ICE signal relay and optional short-lived TURN credentials

It does **not** run the guest sim and never sees gameplay inputs.

Architecture: [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md)  
Privacy: [docs/PRIVACY.md](docs/PRIVACY.md)  
Secrets: [docs/SECURITY.md](docs/SECURITY.md)

## Quick start (local)

Clients often default to `ws://127.0.0.1:8765` for local bring-up.

```bash
cp .env.example .env
# BIND_ADDR=0.0.0.0:8765 is the MotK-friendly default

cargo run
# optional: cargo run -- --debug
```

Override the client with `PSX_NET_LOBBY_URL` or `SNES_NET_LOBBY_URL` if needed.
Leave JWT secrets empty for local no-auth HTTP `/v1` mode, or set them for auth
testing.

## Self-hosting

1. Copy [`.env.example`](.env.example) → `.env` and set `BIND_ADDR`.
2. Optional auth: set `REQUIRE_AUTH=true` and `JWT_SECRET_CURRENT` (see
   [docs/SECURITY.md](docs/SECURITY.md)).
3. Optional Coturn: set `COTURN_*` for `GET /v1/turn-credentials`.
4. Put TLS at a reverse proxy in front of public deployments.
5. Point game clients at `ws://your-host:8765` (or `wss://…` behind TLS).

Never commit `.env` — it is gitignored.

Public reference lobby (when available):
`ws://netplay.technicallycomputers.ca:8765`.

## License

MIT — see [LICENSE](LICENSE).

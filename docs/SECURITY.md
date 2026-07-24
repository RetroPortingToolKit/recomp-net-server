# Security, secrets, and client authentication

This document describes how **recomp-net-server** handles secrets, optional
client authentication, and the limits of “validating” game clients.

## Threat model

1. **Anything shipped in a game binary can be extracted.** Shared secrets,
   static API keys, or obfuscated strings are **hints**, not proofs of a
   legitimate client. Assume compromise and design for **revocation** and
   **rotation**.
2. **Server-side enforcement** is what matters: rate limits, abuse detection,
   short-lived credentials, and lobby/session integrity checks on the server.
3. **Open source does not weaken the threat model.** Attackers can already
   replay API calls or patch a client; server-side checks and short-lived
   credentials remain the real controls.

## Configuration and secrets (no defaults in source)

All sensitive values are read from the **process environment** (or your
platform’s secrets injector).

| Variable | Secret? | Purpose |
|----------|---------|---------|
| `BIND_ADDR` | No | `host:port` to listen on (e.g. `0.0.0.0:8091`). |
| `DATABASE_URL` | Often | Database connection string; never commit. |
| `REQUIRE_AUTH` | No | If `1`/`true`, server refuses to start unless JWT signing secrets are set. |
| `JWT_SECRET_CURRENT` | **Yes** | HMAC key for signing **short-lived** session/player tokens. |
| `JWT_SECRET_PREVIOUS` | **Yes** | Optional second key still accepted while rotating `JWT_SECRET_CURRENT`. |
| `COTURN_STATIC_AUTH_SECRET` | **Yes** | Shared secret for coturn `use-auth-secret` / REST HMAC passwords. **Must match** coturn `static-auth-secret` exactly (see [COTURN.md](COTURN.md)). |

Rules implemented in code:

- There is **no** hard-coded signing key, API key, or password in the repository.
- If `REQUIRE_AUTH` is enabled and `JWT_SECRET_CURRENT` is missing, startup **fails fast**.

## Short-lived tokens and key rotation

- Issued JWTs should use a **short expiration** (e.g. 15–60 minutes for session
  tokens; tune to your launcher flow). Long-lived bearer tokens in binaries
  are a liability.
- **Rotation:** set a new `JWT_SECRET_CURRENT` and move the old value to
  `JWT_SECRET_PREVIOUS`. The server accepts signatures from **either** key
  until all old tokens expire; then clear `JWT_SECRET_PREVIOUS`.

## Client software validation (defense in depth)

- Use tokens for **session continuity** and **revocation**, not as DRM.
- Combine with: TLS in production, per-IP rate limits, anomaly flags, and
  server-authoritative lobby membership.
- Game identity (`game_id`) is client-reported; enforce an allowlist and
  matching `client_version` / build pins so peers do not join mismatched sims.

## Repository hygiene

- Never commit `.env`, `*.pem`, or production `DATABASE_URL`.
- Keep CI logs free of printed secrets and internal stack traces where possible.
- Rotate Coturn / JWT secrets if they were ever shared outside a trusted host.
  When rotating Coturn, update **both** `COTURN_STATIC_AUTH_SECRET` and the
  daemon’s `static-auth-secret` in the same change window.

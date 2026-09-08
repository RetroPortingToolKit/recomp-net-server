-- Long-lived per-device netplay keys.
--
-- The problem this solves: the Discord login needs a browser, and the devices
-- people want to play on increasingly do not have one. A player signs in once
-- on a PC, and the secret issued there travels with a build they install on a
-- handheld or a console, which then authenticates with no browser at all.
--
-- One row PER DEVICE, not one per player. A single shared key would mean
-- revoking a lost handheld also signs the player out of their desktop, which
-- is the kind of thing that makes people never revoke anything. Separate rows
-- make "forget that one device" a real, cheap action.
--
-- Only the SHA-256 hash is stored, exactly as `players.api_token_hash` does:
-- the plaintext is shown once, at issue, and is unrecoverable afterwards. A
-- database leak yields no usable key.
CREATE TABLE IF NOT EXISTS player_secrets (
    id           TEXT PRIMARY KEY NOT NULL,
    player_id    TEXT NOT NULL REFERENCES players(id),
    secret_hash  TEXT NOT NULL,
    -- Free text the player sees when deciding what to revoke ("Steam Deck").
    label        TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    last_used_at TEXT,
    -- Set rather than deleted, so a revoked key stays visible in a moderation
    -- or support question ("when did that device stop working?").
    revoked_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_player_secrets_player ON player_secrets (player_id);
-- The lookup the auth path actually makes: hash first, then check it is live.
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_secrets_hash ON player_secrets (secret_hash);

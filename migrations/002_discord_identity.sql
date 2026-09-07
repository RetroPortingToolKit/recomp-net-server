-- Discord-linked identity.
--
-- `discord_id` is the ONLY stable key. It is Discord's snowflake, immutable
-- for the life of the account. BOTH Discord names are mutable by the user,
-- and a released @username can later be claimed by somebody else -- so keying
-- identity on either name would hand out an impersonation route and a way to
-- inherit (or shed) a ban by renaming. The names below are cached copies,
-- refreshed on every login, kept for display and for moderation audit.
--
-- Two names, two jobs, and they are not interchangeable:
--
--   discord_username     Discord's unique @handle. Unique ON DISCORD, not
--                        here, and not our key. Shown as the disambiguator
--                        when two players present the same handle.
--   discord_global_name  Discord's display name. NOT unique, even on Discord.
--
--   netplay_handle       What other players see in the seat table. Arbitrary
--                        and player-editable, defaulted from the Discord names
--                        on first link. Presentational only: it is never an
--                        identity, is not unique, and is deduplicated within a
--                        room the way a display name always was.
ALTER TABLE players ADD COLUMN discord_id TEXT;
ALTER TABLE players ADD COLUMN discord_username TEXT;
ALTER TABLE players ADD COLUMN discord_global_name TEXT;
ALTER TABLE players ADD COLUMN discord_avatar TEXT;
ALTER TABLE players ADD COLUMN netplay_handle TEXT;
ALTER TABLE players ADD COLUMN linked_at TEXT;
ALTER TABLE players ADD COLUMN last_seen_at TEXT;

-- One player row per Discord account. Partial, so the pre-Discord anonymous
-- rows (discord_id NULL) are unaffected and keep working.
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_discord_id
    ON players (discord_id) WHERE discord_id IS NOT NULL;

-- Simulation-state fork reports.
--
-- WHAT THIS TABLE IS, and what it is not.
--
-- In rollback netcode both peers digest the same simulation every tick, so any
-- divergence is visible by construction: the whole class of cheating that
-- alters the simulation cannot hide from it. That detection already existed in
-- the client, named down to the subsystem, and was written to a local log and
-- discarded. This table is where it goes instead.
--
-- A row is NOT an accusation, and no query over it should be written as though
-- it were. A row says two peers disagreed at a tick. Version skew says that. A
-- genuine emulation bug says that -- this project has found several, and each
-- would have produced these rows from two entirely honest players. Which side
-- MOVED is not knowable from a two-peer disagreement at all; it is only ever
-- answerable by looking across many matches against many DIFFERENT opponents,
-- which is the one thing a server can do that neither client can.
--
-- So: both peers report independently, both digests are stored, and the server
-- stores rather than judges. There is deliberately no verdict column, no score
-- and no flag. Adding one later is a decision to make on evidence that does
-- not exist yet; see docs/AUTOMATCH.md §5.2 and §15.
--
-- Like every other table here it records participation and facts the clients
-- reported, never a result the server did not observe.

CREATE TABLE IF NOT EXISTS desync_reports (
    id          TEXT PRIMARY KEY NOT NULL,
    -- The account, not the connection: a signal that a reconnect erases is not
    -- a signal. Matches the reasoning in 004_automatch.sql.
    player_id   TEXT NOT NULL REFERENCES players(id),
    -- Empty when the fork happened outside a server lobby (a LAN or direct
    -- match that still had a lobby connection open). Kept rather than dropped:
    -- the row is still evidence about the account.
    lobby_id    TEXT NOT NULL DEFAULT '',
    game_name   TEXT NOT NULL DEFAULT '',
    game_version TEXT NOT NULL DEFAULT '',
    -- The ROM this build was recompiled from. Two peers on different dumps is
    -- an ordinary, innocent cause of a fork and needs to be separable from the
    -- rest at a glance.
    disc_fp     TEXT NOT NULL DEFAULT '',
    -- Where and what. `partition` is the subsystem whose digest moved first
    -- ('wram', 'apu', 'ppu', 'post', 'other'); "the state differs" is not a
    -- diagnosis, "the APU differs and everything else matches" is.
    tick        INTEGER NOT NULL,
    partition   TEXT NOT NULL DEFAULT '',
    -- Hex strings, not integers. They are opaque 32-bit identities that are
    -- only ever compared for equality, and a reader that decides to render one
    -- as a float has destroyed the only thing the column is for.
    digest_mine   TEXT NOT NULL DEFAULT '',
    digest_theirs TEXT NOT NULL DEFAULT '',
    -- 'host' | 'guest'. Both peers report the same fork from opposite sides,
    -- so this is what lets the two rows be recognised as one event.
    role        TEXT NOT NULL DEFAULT '',
    -- The cosmetic exemptions this peer was running (`id@version#sha256`,
    -- ';'-separated). A fork under an unapproved exemption and a fork under
    -- none are different facts, and the row is worth little without which.
    mod_exempt  TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- The only two questions this table is meant to answer.
--
-- "Show me this account's forks" -- which is the one that matters, and only
-- becomes meaningful across many opponents.
CREATE INDEX IF NOT EXISTS idx_desync_reports_player
    ON desync_reports (player_id, created_at);

-- "Did both sides report the same event?" Two rows for one lobby, at the same
-- tick, from opposite roles, is a corroborated fork. One row is a peer whose
-- opponent did not report -- which is itself informative, and is exactly why
-- the pair has to be findable.
CREATE INDEX IF NOT EXISTS idx_desync_reports_lobby
    ON desync_reports (lobby_id, tick);

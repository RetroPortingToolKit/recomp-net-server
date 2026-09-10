-- Automatch: the two tables the accept gate needs.
--
-- Both key on `players.id` -- the account row behind a Discord snowflake --
-- and NOT on the connection's ephemeral uuid. That is the whole reason
-- automatch requires a sign-in: a dodge cost that reconnecting erases is not
-- a cost, and every other lobby path here is keyed on a `Uuid::new_v4()` that
-- lasts exactly as long as one WebSocket.
--
-- Neither table records a RESULT. The sim runs on the clients; this server
-- never observes who won, and a stored winner it could not observe would be a
-- verdict it invented. See docs/AUTOMATCH.md §13.

-- Accept-gate dodges. Only the last 24 h can raise a cooldown; older rows are
-- operator history and are pruned per AUTOMATCH_RETENTION_DAYS.
CREATE TABLE IF NOT EXISTS automatch_strikes (
    id         TEXT PRIMARY KEY NOT NULL,
    player_id  TEXT NOT NULL REFERENCES players(id),
    -- 'decline' (the client said no) | 'timeout' (it never answered).
    kind       TEXT NOT NULL,
    game_name  TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- The lookup the cooldown actually makes: this player, recently.
CREATE INDEX IF NOT EXISTS idx_automatch_strikes_player
    ON automatch_strikes (player_id, created_at);

-- Who was paired with whom. Feeds the avoid-last-opponent filter, and answers
-- an operator's "what happened at 19:04". Participation only.
CREATE TABLE IF NOT EXISTS automatch_pairings (
    match_id    TEXT PRIMARY KEY NOT NULL,
    game_name   TEXT NOT NULL,
    ruleset_id  TEXT NOT NULL,
    player_a    TEXT NOT NULL REFERENCES players(id),
    player_b    TEXT NOT NULL REFERENCES players(id),
    est_rtt_ms  INTEGER,
    paired_at   TEXT NOT NULL DEFAULT (datetime('now')),
    -- Set when the pair reached `launch`. NULL means it died at the accept
    -- gate, which is a different thing from a match that ended early and is
    -- worth being able to tell apart later.
    launched_at TEXT
);

-- Two indexes, not one: a pairing is symmetric to a human but not to SQLite,
-- and the filter asks "was I in this?" from either side.
CREATE INDEX IF NOT EXISTS idx_automatch_pairings_a
    ON automatch_pairings (player_a, paired_at);
CREATE INDEX IF NOT EXISTS idx_automatch_pairings_b
    ON automatch_pairings (player_b, paired_at);

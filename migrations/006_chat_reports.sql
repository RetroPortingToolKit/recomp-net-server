-- Player-submitted reports of chat messages, for moderation.
--
-- THIS TABLE IS THE CONTRACT. There is deliberately no HTTP API and no bot in
-- this repo: the brief was a simple server-side thing to build monitoring,
-- reporting and notifications on later, and a schema is a more stable thing to
-- build against than an endpoint invented before anyone knows what the tool
-- needs. A bot reads these rows and writes back `status`, `reviewed_by`,
-- `reviewed_at` and `resolution`. Adding an endpoint later changes nothing
-- here.
--
-- WHAT IS STORED, AND WHY IT IS THE SERVER'S COPY
--
-- The server already relays every chat line, so it has the text itself. The
-- report therefore carries a MESSAGE ID, and the server writes down what it
-- relayed -- not what the reporter says was said. That distinction is the
-- whole difference between a moderation record and an accusation: a reporter
-- who could supply the text could fabricate a message and get somebody
-- sanctioned for it.
--
-- Chat is otherwise NOT persisted anywhere (see handle_chat: "Not stored, like
-- every chat line"), and this does not change that. Lines live in a small
-- in-memory ring and are written here only when somebody reports one. A report
-- promotes a handful of lines out of that ring; everything else is still
-- forgotten when it scrolls off.
--
-- `context` is the lines immediately before the reported one, from the same
-- room. It is kept because moderation without it is mostly guesswork -- a slur
-- quoted while reporting it and a slur aimed at somebody read identically in
-- isolation -- and it necessarily contains messages from people who were not
-- reported. That is a real privacy cost, taken deliberately, bounded to a few
-- lines, and subject to the same retention as the row.

CREATE TABLE IF NOT EXISTS chat_reports (
    id            TEXT PRIMARY KEY NOT NULL,

    -- Who reported, and who they reported. Accounts, not connections: a
    -- moderation record keyed to something a reconnect erases is not a record.
    -- See identity.rs -- `account` is what moderation reads.
    reporter_id   TEXT NOT NULL REFERENCES players(id),
    accused_id    TEXT NOT NULL REFERENCES players(id),

    -- The server's own id for the relayed line, so two reports of the same
    -- message are recognisably the same message.
    message_id    TEXT NOT NULL,
    -- What the SERVER relayed, already run through chat_filter. Not the
    -- reporter's transcription of it.
    message_text  TEXT NOT NULL,
    -- The display name the accused was using at the time, which is not
    -- necessarily the one they are using when somebody reads this.
    accused_name  TEXT NOT NULL DEFAULT '',
    -- Preceding lines from the same room, newest last, '\n'-separated and
    -- prefixed with their sender. See the note above on why, and its cost.
    context       TEXT NOT NULL DEFAULT '',

    -- Where it happened, as the CLIENT reported it. One queue serves every
    -- title on every console, so a row has to carry its own context rather
    -- than have it inferred: a moderator reading the queue does not otherwise
    -- know whether they are looking at a fighting game lobby or a racing one,
    -- and a LAN or direct session has no server-side record at all.
    --
    -- `scope` is 'lobby' or 'server' (the per-game channel outside any room);
    -- `lobby_id` is empty for the latter.
    scope         TEXT NOT NULL DEFAULT 'lobby',
    lobby_id      TEXT NOT NULL DEFAULT '',
    game_name     TEXT NOT NULL DEFAULT '',
    game_version  TEXT NOT NULL DEFAULT '',
    -- The emulated machine ('snes', 'psx', …). METADATA ONLY: nothing may name
    -- a file or a directory after it. See chat_report_dump_dir in config.rs.
    platform      TEXT NOT NULL DEFAULT '',
    -- Which lobby server the session ran against. Empty for LAN/direct, which
    -- is itself worth knowing -- those sessions have no other server record.
    server        TEXT NOT NULL DEFAULT '',
    -- How many messages this report covered. The first is in `message_text`
    -- for indexing and at-a-glance reading; the full transcript is in
    -- `transcript` and in the dump file named by `dump_path`.
    message_count INTEGER NOT NULL DEFAULT 1,
    transcript    TEXT NOT NULL DEFAULT '',
    -- Relative path of the transcript dump, or empty when dumps are disabled.
    dump_path     TEXT NOT NULL DEFAULT '',

    -- Why, from a fixed list the client offers, plus an optional free-text
    -- note. The category is what a bot can count and alert on; the note is
    -- what a human reads.
    reason        TEXT NOT NULL DEFAULT 'other',
    note          TEXT NOT NULL DEFAULT '',

    -- The moderation workflow, for whatever tool comes later.
    -- 'open' -> 'reviewed' | 'actioned' | 'dismissed'. Nothing in this server
    -- ever changes it: the server records, it does not adjudicate.
    status        TEXT NOT NULL DEFAULT 'open',
    reviewed_by   TEXT NOT NULL DEFAULT '',
    reviewed_at   TEXT,
    resolution    TEXT NOT NULL DEFAULT '',

    created_at    TEXT NOT NULL DEFAULT (datetime('now')),

    -- One report per person per message. A second attempt is not an error
    -- worth surfacing -- the reporter did what they meant to -- it just must
    -- not make the queue look busier than it is, or let one person inflate a
    -- count against somebody by clicking repeatedly.
    UNIQUE (reporter_id, message_id)
);

-- The queue: what a bot polls.
CREATE INDEX IF NOT EXISTS idx_chat_reports_status
    ON chat_reports (status, created_at);

-- "What has been reported about this account?" -- the question a moderator
-- asks once a name comes up, and the one where a pattern lives. Note that
-- several reports are not proof of anything on their own: a popular player
-- attracts them, and a coordinated group can manufacture them. That is a
-- reason to show the count with the rows, never instead of them.
CREATE INDEX IF NOT EXISTS idx_chat_reports_accused
    ON chat_reports (accused_id, created_at);

-- "Is this person reporting everybody?" Report spam is its own abuse, and the
-- table has to be able to answer for its own reporters.
CREATE INDEX IF NOT EXISTS idx_chat_reports_reporter
    ON chat_reports (reporter_id, created_at);

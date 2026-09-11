# Chat reports

Players report chat lines; the server records them. **Cross-platform**: one
queue for every title on every console, one server path, and the frame itself
is built by `recomp-net` (`recomp_net/chat_report.h`) rather than by each
console's lobby client — those are written once per framework
(`snes_lobby_client.c`, `psx_lobby_client.c`, one per console after them) and a
rule about what may be reported must not exist in five versions.

A lobby client's whole involvement is saying where it is and calling
`queue_send`.

There is no bot, no notifier and no HTTP API here — the brief was a simple
server-side thing to develop those against later, and the **table is the
contract**, because a schema is a more stable thing to build on than an
endpoint invented before anyone knows what the tool needs.

## The one design decision that matters

**The server stores its own copy of the message, not the reporter's.**

Chat already passes through this server on its way between players, so it has
the words. Every relayed line is given an id and kept in a small in-memory
ring; a report names that id, and the server writes down what *it* relayed.

The alternative — letting the report carry the text — would mean anyone could
compose a message, attribute it to somebody, and have them sanctioned for words
they never typed. That is the difference between a moderation record and an
accusation, and it is why the client-side API has no text parameter at all.

The same reasoning that runs through the netplay work in this repo: take
evidence from the party that has it, not a verdict from the party with an
interest in it.

## What is retained

Chat is **not** persisted (`handle_chat`: "Not stored, like every chat line"),
and this does not change that. Lines live in a ring of the last
`CHAT_RING_MAX` (1000) relayed messages and are forgotten as they scroll off. A
line reaches the database only when somebody reports it.

A report stores the reported line plus up to `CHAT_CONTEXT_LINES` (6) preceding
lines **from the same room**. Context is kept because moderation without it is
mostly guesswork — a slur quoted while reporting it and a slur aimed at
somebody read identically in isolation — and it necessarily contains messages
from people who were not reported. That is a real privacy cost, taken
deliberately, bounded, same-room only, and subject to the same retention as the
row.

A line whose sender was not signed in cannot be reported: there is nobody to
attribute it to. Same for your own lines.

## The wire

Client → server:

```json
{ "op": "chat_report", "v": 1,
  "mids": ["3f", "40", "41"],
  "reason": "harassment", "note": "…",
  "game": "…", "game_version": "…", "platform": "snes",
  "server": "wss://…", "lobby": "…", "scope": "lobby" }
```

**Several messages per report**, because harassment is usually a burst rather
than a line, and making somebody file six reports to describe one incident
produces six rows that each look minor. Ids that have scrolled out of the ring
are simply absent from the transcript rather than failing the report. A single
`"mid"` is still accepted, which is what an older client sends.

**Metadata says where it happened.** The server prefers its own knowledge and
falls back to the client's — the client's copy matters because a LAN or direct
session has no server-side record at all, and where the server does know, its
answer is the one that cannot be edited.

Server → client: `{"op":"chat_report_ok","ok":true,"mid":"3f"}`, or an error
code:

| code | meaning |
|---|---|
| `bad_report` | no usable message id in `mids`/`mid` |
| `need_account` | the reporter is not signed in |
| `message_expired` | the line has scrolled out of the ring, or never existed |
| `cannot_report` | the sender was a guest, or it is your own line |
| `rate_limited` | over `REPORTS_PER_HOUR` (20) for this account |
| `report_failed` | the write failed |

Relayed chat now carries a `mid` field. Older clients ignore it; older servers
omit it, and a line without one simply cannot be reported.

Categories: `harassment`, `hate_speech`, `sexual_content`, `spam`, `threats`,
`cheating_claim`, `other`. An unrecognised one is stored as `other` rather than
refused — a client from a later version offering a finer category should not
have its report thrown away over a label.

## Transcript dumps

Each report also writes a plain-text transcript beside its row, under
`CHAT_REPORT_DUMP_DIR` (default `data/reports`, empty disables it). The file
carries the metadata header, the context and every reported line.

**Named `<yyyy>/<mm>/<report-id>.txt`, and nothing else.** Not the game, not
the platform, not a player:

- one queue spans every title on every console, and naming files after the
  console would fragment the evidence along a line that has nothing to do with
  moderation;
- a directory listing named after games or players discloses who has been
  reported to anyone who can see the folder.

Which game, which console and which account are **inside** the file, where they
belong. `a_dump_is_named_by_date_and_id_only` asserts both halves of that.

The database row is the record; the dump is a convenience for reading with a
text editor instead of SQL. A dump that cannot be written costs the path, never
the report.

## The table

`chat_reports`, from `migrations/006_chat_reports.sql` — read that file, the
columns are documented there. The parts a bot cares about:

| column | |
|---|---|
| `status` | `open` → `reviewed` \| `actioned` \| `dismissed`. **Nothing in this server ever changes it.** The bot owns this column. |
| `reviewed_by`, `reviewed_at`, `resolution` | Also the bot's, untouched by the server. |
| `reporter_id`, `accused_id` | Accounts (`players.id`), not connections — a record keyed to something a reconnect erases is not a record. |
| `message_text`, `context`, `transcript`, `accused_name` | The server's copy, at the time. `message_text` is the first reported line (indexed); `transcript` is all of them. |
| `game_name`, `game_version`, `platform`, `server`, `lobby_id`, `scope` | Where it happened. `platform` is metadata only — never used to name a file. |
| `dump_path` | Relative path of the transcript, or empty when dumps are off. |
| `reason`, `note` | Category for counting; note for a human to read. |

Three indexes, for the three questions worth asking:

- `(status, created_at)` — the queue to poll.
- `(accused_id, created_at)` — what has been reported about this account.
- `(reporter_id, created_at)` — **is this person reporting everybody?** Report
  spam is its own abuse and the table has to answer for its own reporters.

`UNIQUE (reporter_id, message_id)`: one report per person per message. A repeat
answers OK — the reporter did what they meant to — but adds no row, so nobody
can inflate a count by clicking. Several *different* people reporting one line
does create several rows, because that is the signal and collapsing it would
erase it.

## Cautions for whoever writes the bot

**A report count is not evidence.** A popular player attracts reports. A
coordinated group can manufacture them. Show counts *with* the rows, never
instead of them — the same reason the desync table (§15 of `AUTOMATCH.md`) has
no verdict column.

**Check the reporter as well as the accused.** The `reporter_id` index exists
for that and it is not an afterthought; a report queue with no view of who is
filing becomes a harassment tool.

**Reported text is hostile input.** It reached the database through
`chat_filter`, but it is still whatever somebody typed. Anything that renders
it — a Discord message, a web page — escapes it.

**Rows age out** on the ordinary retention sweep. If moderation needs a longer
memory than telemetry, that is a deliberate policy change to make here, with a
reason: keeping records of what people said for longer than everything else is
a decision, not a default.

## Not built

- No notifications, no auto-action, no appeals flow.
- No admin HTTP surface. A bot reads the SQLite file directly; if that stops
  being convenient, add an endpoint then, against the schema above.
- Nothing reports **voice or netplay-internal** chat, because neither exists.
- The **client UI** is not wired on any console. `*_lobby_report_chat(mids,
  count, reason, note)` exists for SNES and PSX and the chat rings carry
  `mid`, but no launcher chat panel offers a Report control yet — so the
  server side has no callers. That control belongs in recomp-ui, once, for the
  same reason the frame builder does.
- Consoles other than SNES and PSX have no lobby client yet, so nothing to
  wire.

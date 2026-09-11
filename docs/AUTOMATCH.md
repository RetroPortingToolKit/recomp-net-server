# Automatch (v1)

Server-run pairing for two players who want a match and do not care which
lobby it happens in. An addendum to [WS_LOBBY.md](WS_LOBBY.md): every message
here rides the same WebSocket, and once a pair is formed the match runs on the
**existing** `joined` / `lobby_update` / `launch` path with no new client
machinery behind it.

Requires a signed-in account ([`002_discord_identity.sql`](../migrations/002_discord_identity.sql)).
That is the point of the feature, not a side condition — see
[Why this needs the account system](#why-this-needs-the-account-system).

---

## 1. What automatch is, mechanically

Online matches **always** run through the UDP SFU (`transport=sfu`, WS_LOBBY
§start; a start with the relay unconfigured fails `relay_unavailable`). So
"host" is not a network position — it is slot 0, which is sim authority and,
today, the owner of `match_caps`. Nothing about hosting requires a
reachable peer.

That makes automatch a small feature rather than a second netplay stack:

> **The server creates a lobby on two players' behalf and is its host.**

No new transport, no new session type, no peer-reachability selection, and no
new boot path. The client gains a queue panel and an accept modal; everything
downstream of `joined` is the code that already ships.

## 2. Decisions of record

Settled 2026-09-09 (Alex). Recorded here so the reasons are not re-litigated
in a later diff.

| # | Decision | v1 |
|---|----------|-----|
| 1 | Account required to queue | **Yes.** Guests are refused `need_account`. |
| 2 | Who owns `match_caps` | **The server**, from a named ruleset. Multiple queue types with different rulesets are planned; one (`standard`) ships. |
| 3 | Mods | **Vanilla only.** A ruleset carrying a mod plan is a config error. |
| 4 | Rating | **None.** Not recorded, not computed, not displayed. |
| 5 | Party sizes | **2p only.** `max_slots` 3..8 is refused. |
| 6 | Multi-title tickets | **Wire-supported from day one**, filtered client-side. Standalone recomp-ui opts in exactly one title (the running game); Retro Launcher will opt in several. |

Requiring an account (1) does not break the compatibility promise in
WS_LOBBY.md → "What is deliberately not gateable". `list` and `join` stay open
to guests, as promised. `automatch_queue` is an op no older client sends, so
there is no client that could regress. It also holds independently of
`DISCORD_REQUIRED`, which stays off.

## 3. The ticket, and the match key

A `join` can fail today on `game_name`, `game_version` (exact, `""` → `"dev"`),
`disc_fp`, `full`, and `need_mods`. Automatch must never pair two clients that
would fail one of those, so **the queue key is the join gate**:

```
(game_name, game_version, disc_fp, ruleset_id, max_slots)
```

Equality bucket. No fuzzy matching, no "close enough". Three consequences,
each deliberate:

- **`disc_fp` is required.** In `join`, an empty fingerprint means "legacy
  host, no check" — a wildcard. A wildcard in a queue silently pairs a
  Track-01-only dump against a full multi-track cue, which is exactly the case
  the fingerprint was added to catch. Automatch is a new op, so requiring a
  real 64-hex fingerprint costs no compatibility. Empty → `need_disc_fp`.
- **`game_version` exact-match makes tagged releases a prerequisite.** Dev
  builds all pool together as `"dev"`; a shipped port pools per release. A
  title with no tagged release has a queue of developers, which is correct and
  worth knowing before wondering why nobody is in it.
- **Mods are absent from the key**, because in v1 the required plan is always
  empty. See §5.

### Wire

```json
{
  "op": "automatch_queue",
  "titles": [
    {
      "game_name": "Star Wars: Masters of Teras Kasi",
      "game_version": "1.2.0",
      "disc_fp": "0123…64 hex chars…abcd",
      "ruleset_id": "standard",
      "max_slots": 2
    }
  ],
  "mods_enabled": false,
  "host_bind": "0.0.0.0:7777",
  "guest_bind": "0.0.0.0:7778"
}
```

`titles` is an **ordered preference list**, 1..8 entries. The server must not
assume one: standalone recomp-ui sends the running game and nothing else, and
a launcher that knows about several titles sends several. Which titles are in
the list is decided entirely client-side — the server neither knows nor cares
what the player has installed but chose not to queue for.

**Both binds are carried, because the role is unknown at queue time.** With a
human host you know whether you are host or guest before you bind a port; here
the server assigns slot 0 after pairing. So the client prepares both under the
usual policy (`launcher_udp_port.*`: host prefers 7777.., guest prefers
7778..) and learns which one it got from `local_slot` in `joined`.

`mods_enabled` is an assertion, not a catalog: see §5.

Replies:

```json
{ "op": "automatch_queued", "ok": true, "ticket_id": "…",
  "titles": [ { "game_name": "…", "ruleset_id": "standard", "pool": 3 } ] }
```

`pool` is how many other tickets currently sit in that bucket — the number the
UI shows as "3 waiting". Pushed again on every `automatch_status` (§7).

Errors (`{ "op": "error", "ok": false, "code": … }`):

| `code` | Meaning |
|--------|---------|
| `need_account` | Guest connection. Sign in first. |
| `automatch_off` | This deployment has no rulesets loaded. |
| `already_queued` | This **account** already holds a ticket (see §8). |
| `already_in_lobby` | Leave the room first. |
| `unknown_ruleset` | No such `ruleset_id` for that `game_name`. |
| `need_disc_fp` | Missing or malformed `disc_fp`. |
| `version_not_pooled` | Ruleset pins a `game_version` and this is not it. |
| `mods_not_pooled` | `mods_enabled` was true. |
| `slots_not_pooled` | `max_slots` != 2 in v1. |
| `queue_full` | `AUTOMATCH_QUEUE_MAX` reached. |
| `cooldown` | Dodge cooldown active; `retry_secs` is on the error. |

## 4. Rulesets are server-owned

An automatch lobby has no human host, so `match_caps` cannot be "the host's
latest settings". The queue names a `ruleset_id`; the server writes the caps.
Both players consented to those caps by queueing into that ruleset.

The rejected alternative — copy whichever queued player's caps — means an
opponent silently changes your sim settings, and it makes two matches from the
same queue incomparable, which would poison any later work that wants to treat
a queue as a level playing field.

A ruleset **is** a stored `match_caps` blob with an id and a label, so nothing
new has to be validated: the same object shape, the same ≤4096-byte ceiling as
`set_match_caps`, echoed on `launch` the same way.

`AUTOMATCH_RULESETS_PATH`, default `data/automatch_rulesets.toml`:

```toml
[[ruleset]]
id         = "standard"
label      = "Standard"
game_name  = "Star Wars: Masters of Teras Kasi"
# Optional. When set, only this release may queue into this ruleset.
game_version = "1.2.0"
max_slots  = 2

[ruleset.match_caps]
v                = 1
input_delay      = 2
rollback         = true
input_prediction = 6
turbo_loads      = true
bios_hle         = true
fast_boot        = false
auto_skip_fmv    = false
language         = "en"
```

Refused at load, loudly, with the server still starting and automatch off:

- a `match_caps.mods` key (v1 is vanilla-only, §5),
- `max_slots` other than 2,
- a duplicate `(game_name, id)`,
- caps that are not a JSON object, or serialize over 4096 bytes.

Adding a title to the pool is therefore a deployment edit, like
`LOBBY_GAME_ALLOWLIST`. That is deliberate: an automatch pool is a curated
thing, and a client that could define its own would be defining its opponent's
sim settings.

Clients discover what exists rather than shipping a copy:

```json
{ "op": "automatch_rulesets", "game_name": "…" }
```
```json
{ "op": "automatch_rulesets_ok", "ok": true,
  "rulesets": [ { "id": "standard", "label": "Standard",
                  "game_version": "1.2.0", "max_slots": 2,
                  "match_caps": { "v": 1, "…": "…" } } ] }
```

The caps come back so the queue panel can show what it is about to sign the
player up for ("Delay 2 · Rollback on") without a per-title table in the UI.

## 5. Vanilla only, and why it is a desync question

The seat gate compares the lobby's **required** plan (`match_caps.mods`)
against the joiner's **installed** catalog (`mod_offer`). A v1 ruleset has no
required plan, so nothing is ever missing and the `need_mods` transfer flow —
which is an interactive host↔guest ICE prompt with no host to prompt — never
has to run.

The trap is the other direction. A player with sim-affecting mod features
**enabled locally** boots a different sim from a vanilla opponent. That is a
desync, not a fairness complaint. So:

- the ticket asserts `mods_enabled: false`;
- the server refuses `true` with `mods_not_pooled`;
- **the launcher is where this is actually enforced** — the Queue button is
  disabled, with the reason shown, while any sim-affecting mod feature is on.
  The server cannot check this and must not pretend to: it is taking the
  client's word, and the assertion exists so a modified client is making a
  deliberate false statement rather than exploiting an omission.

Identical-plan pooling (both sides carrying the same required plan) is the v2
shape. It needs the plan digest in the match key and a way to reach the
transfer flow without a host.

## 6. Pairing

Index tickets by match key; each bucket is FIFO by enqueue time. On every
enqueue, cancel, and 1 Hz tick, walk the new/aging ticket's keys **in the
client's preference order** and take the oldest ticket in that bucket which
passes the filters. O(keys) per ticket, and at this pool size a linear scan
inside a bucket is not worth improving on.

**Hard filters** — never relaxed:

- same match key (§3),
- not the same account,
- neither account blocks the other (hook for a later block list; the predicate
  exists in v1 even if the table is empty),
- both connections still live and unseated.

**Soft filters** — widen with ticket age, because every knob fragments an
already-small pool:

| Filter | Start | Widens |
|--------|-------|--------|
| Combined RTT to relay | `AUTOMATCH_RTT_START_MS` (120) | +`AUTOMATCH_RTT_STEP_MS` (60) every `AUTOMATCH_RTT_WIDEN_SECS` (20), to `AUTOMATCH_RTT_MAX_MS` (400), then unlimited |
| Avoid last opponent | on | off after `AUTOMATCH_REMATCH_COOLDOWN_SECS` (300) or once RTT is unlimited |

### The floor under D and P

The measurement is not only a filter. `recomp-net/docs/architecture.md`
("Delay-sync admission") stores local input at wire tick `T + D` and admits
tick `T` only when every remote slot has its row for `T + D` — so the input has
exactly `D` frames of wall time to make a **one-way** trip. Online is always
peer → relay → peer, and each `rtt_ms` is a peer's own round trip to the relay,
so:

```text
  one_way(A→B) = rtt_a/2 + rtt_b/2 = (rtt_a + rtt_b) / 2
  frames       = ceil(one_way / frame_ms) + 1     # the +1 is jitter margin
```

That number becomes a **floor**, never a ceiling:

- **Rollback off** — there is nowhere else to put the latency, so `D` carries
  all of it.
- **Rollback on** — `D` stays where the ruleset put it and the prediction
  runway `P` absorbs the remainder, which is the whole reason to run rollback
  on a long link. Past `D + 16` the runway is clamped out and `D` takes the
  shortfall; a link longer than that cannot be papered over.
- **Nothing measured** — the ruleset runs exactly as authored. Raising a floor
  off a number nobody took would be inventing the reason for it.
- **A short link never lowers what the ruleset asked for.** Both players agreed
  to the advertised delay by queueing; a good connection is not a reason to
  overrule it.

`frame_ms` defaults to 60 Hz and a ruleset may override it (a 50 Hz title sets
`frame_ms = 20.0`). The default errs safe: a PAL frame is longer, so 60 Hz
maths asks for *more* frames than a 50 Hz game needs, and too much delay plays
badly where too little stalls the sim.

The floor is published on `automatch_found` as `input_delay`,
`input_prediction` and `frames_needed`, so the accept gate shows the delay the
player is agreeing to rather than the ruleset's advertised one — being told
"delay 2" and then playing at 6 reads as a bug. The room stores the floored
caps, so `launch` carries them and a peer applies them at boot like any other
cap.

**Slot 0 goes to the older ticket.** Under the SFU it carries no mechanical
advantage, but the rule is deterministic and it shows up in the log, which
"whichever the map iterated first" does not. Revisit if slot 0 ever turns out
to matter.

### Latency: the shortcut this architecture hands you

Because every online match goes through one relay, the only latency that
matters is **each peer → relay**, not peer ↔ peer. Match quality is therefore
predictable from each ticket on its own, before any pairing, with no pairwise
probing at all — `rtt_a + rtt_b` is the estimate.

**Measured, as of this version, by a UDP probe against the relay itself.**
The relay answers packet type `200` with type `201` — the same 14 bytes back,
nonce included — before any session lookup, precisely so a client can measure
the path while it is sitting in a queue with no session to belong to. Same size
in and out, so it is not an amplifier; it is still a reflector, so the length is
exact rather than a maximum and replies are capped at 8 per source per second.

The client times the round trip and reports it (`automatch_rtt`, or `rtt_ms` on
`automatch_queue`). Where to probe is published on both `automatch_rulesets_ok`
and `automatch_queued` as `probe: { endpoint, magic, type }`.

`rtt_ms` is **client-reported** and belongs in LOBBY.md's trust table as such.
It is clamped to `MAX_REPORTED_RTT_MS` (2000). The grief case is reporting HIGH
to force delay on an opponent; reporting low only stalls the liar's own sim,
which is its own answer.

A ticket that has not measured yet is held out of pairing for
`PROBE_GRACE_SECS` (3). A client that probes *before* queueing never waits at
all; this is for one that queues first and measures second, where pairing
immediately would qualify the match on a number that was one second away. The
grace is a delay, not a requirement: a client that never probes still matches
once it lapses, and the filter passes on unknown.

With one VPS in one region a cross-globe pair is bad no matter what the
algorithm does. Show the estimate in the accept modal and let the widening
window pair them anyway — a queue that never matches is worse than a match a
player can decline with their eyes open.

## 7. Queue status

Pushed to each queued client on change and at most 1 Hz:

```json
{ "op": "automatch_status", "ok": true, "ticket_id": "…",
  "queued_secs": 47, "est_rtt_ms": 38,
  "titles": [ { "game_name": "…", "ruleset_id": "standard", "pool": 3 } ] }
```

Two UI consequences matter more than the algorithm does, in a pool this size:

1. **Show the population.** "3 waiting" is the difference between waiting and
   wondering whether the feature is broken.
2. **Stay queued while doing something else.** A queued player must still be
   able to browse the lobby list, read server chat, and talk. Nothing in this
   protocol seats or blocks a client until it accepts, and the client must not
   impose a modal spinner that takes that away.

## 8. The accept gate

Never drop two players straight into a match.

```
automatch_queue  →  automatch_queued
      ↓ (server pairs)
automatch_found (both)  ──15s──►  timeout
      ↓ automatch_accept
automatch_accept_ok  →  wait for peer
      ↓ (both accepted)
joined + lobby_update (both)  →  launch
```

```json
{ "op": "automatch_found", "ok": true, "match_id": "…",
  "game_name": "…", "game_version": "1.2.0",
  "ruleset_id": "standard", "ruleset_label": "Standard",
  "match_caps": { "v": 1, "…": "…" },
  "opponent": { "handle": "Marisa", "discord_username": "marisa",
                "avatar": "…", "country": "JP" },
  "est_rtt_ms": 74, "accept_secs": 15 }
```

`discord_id` is **never** on the wire. The snowflake is the identity key; the
handle and `@username` are the display pair, exactly as
[`identity.rs`](../src/identity.rs) sets out, and `@username` is here only as
the disambiguator for two players showing the same handle.

```json
{ "op": "automatch_accept", "match_id": "…", "accept": true }
```

Outcomes:

| Message | When |
|---------|------|
| `automatch_accept_ok` | Your answer landed; waiting on the peer. |
| `joined` → `lobby_update` → `launch` | Both accepted. Existing path, unchanged. |
| `automatch_requeue` + `reason` (`peer_declined` / `peer_timeout` / `peer_left`) | You accepted, they did not. **Back to the front of the queue**, ticket intact. |
| `automatch_cancelled` + `reason: "declined"` + `cooldown_secs` | You declined or timed out. |

An accepter who loses the pair goes to the *front* of their bucket, not the
back. They did nothing wrong and should not be punished with a second full
wait.

### Dodge cost, and the one place to be honest about it

A gate is only real if declining costs something, **and a cost a reconnect
erases is not a cost**. Every lobby path today keys on a `Uuid::new_v4()`
minted per connection, so this is the first feature that structurally needs a
stable key.

`004_automatch.sql`:

```sql
-- Accept-gate dodges, keyed on the account (players.id), because a cost that
-- a reconnect erases is not a cost.
CREATE TABLE IF NOT EXISTS automatch_strikes (
    id         TEXT PRIMARY KEY NOT NULL,
    player_id  TEXT NOT NULL REFERENCES players(id),
    kind       TEXT NOT NULL,          -- 'decline' | 'timeout'
    game_name  TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_automatch_strikes_player
    ON automatch_strikes (player_id, created_at);

-- Pairings, for the avoid-last-opponent filter and for operators answering
-- "what happened at 19:04". Participation only: NO result, NO rating.
CREATE TABLE IF NOT EXISTS automatch_pairings (
    match_id    TEXT PRIMARY KEY NOT NULL,
    game_name   TEXT NOT NULL,
    ruleset_id  TEXT NOT NULL,
    player_a    TEXT NOT NULL REFERENCES players(id),
    player_b    TEXT NOT NULL REFERENCES players(id),
    est_rtt_ms  INTEGER,
    paired_at   TEXT NOT NULL DEFAULT (datetime('now')),
    -- Set when the pair reached `launch`. NULL = it died at the accept gate.
    launched_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_automatch_pairings_a ON automatch_pairings (player_a, paired_at);
CREATE INDEX IF NOT EXISTS idx_automatch_pairings_b ON automatch_pairings (player_b, paired_at);
```

Ladder from `AUTOMATCH_DODGE_COOLDOWNS`, counting strikes in the last 24 h; a
clean 24 h drops the count back to zero.

**The default is a flat `60`**, not a ladder. Escalation is the right shape for
a populated queue, where a repeat dodger costs a stream of other people their
match. It is the wrong shape for the pools these servers have today: the
original `60,300,900` put a player who declined three times in an afternoon on
a fifteen-minute lockout from a queue that might hold two people, which
punishes the one still trying to play harder than it deters anything. The
ladder mechanism is unchanged and a busier deployment should set one.

Both tables are pruned on a daily sweep at `AUTOMATCH_RETENTION_DAYS` (default
30; `0` prunes a row as soon as it stops affecting a decision). Only the last
24 h of strikes and the last `AUTOMATCH_REMATCH_COOLDOWN_SECS` of pairings ever
change an outcome — the rest is operator history, and `automatch_pairings`
records who played whom, so keeping it should be a deliberate choice rather
than an inherited default. There is no account-deletion endpoint in the
server today; when one lands it must clear both of these tables with the
`players` row. See [PRIVACY.md](PRIVACY.md).

**Early disconnects are not penalized in v1.** A rage quit and a crashed
process and a dropped uplink are the same event from the relay's side, and the
server cannot tell them apart. Penalizing on a signal that cannot distinguish
them would be inventing a verdict the server never observed. The accept-gate
dodge, by contrast, is unambiguous: the client sent a decline, or it did not
answer a message it was watching for. That is why it is the only thing v1
charges for.

## 9. What the server does on both-accept

1. Refuse if either connection has since seated or dropped → the survivor gets
   `automatch_requeue`.
2. Create a `Lobby` with the ruleset's caps, `max_slots = 2`,
   `name` = the ruleset label, no password, `allow_spectators = false`.
3. **`host_player_id` is a sentinel no connection can hold** (`""`). Every
   host-gated op — `start`, `kick`, `set_match_caps`, `close`, `move` — then
   answers `not_host` through the checks that already exist, and both clients
   correctly see `is_host = false`. This is the cheapest correct way to say
   "the server is the host": don't hand the role to a player and then try to
   take pieces of it back.
4. Seat both (older ticket → slot 0), from the ticket's `host_bind` /
   `guest_bind` as the assigned role requires.
5. Emit `joined` to each and `lobby_update` to both — byte-identical in shape
   to a human-hosted room.
6. After `AUTOMATCH_START_DELAY_SECS` (default 3), run the start path. The
   settle is not cosmetic: it gives both clients a beat to apply caps and gives
   a peer that drops between accept and launch somewhere to be noticed.

Implementation notes, so this does not grow a parallel copy of the start path:

- **Factor `start_lobby(state, lobby_id) -> Result<…>` out of
  [`handle_start`](../src/ws_lobby.rs).** The `host_player_id != player_id`
  check and the `not_in_lobby` lookup stay in the handler; everything from
  `need_players` onward — the SFU decision, the LAN-advertise heuristic, the
  fresh `session_id`, clearing ready, the `launch` broadcast — is shared. Two
  copies of the transport decision is how the two paths start behaving
  differently.
- **Teardown has no host to key on.** `client_leave` destroys a lobby only
  when the leaver is `host_player_id`; with the sentinel, nobody is, and a
  departure would leave a one-seat automatch room alive forever. Automatch
  lobbies are destroyed when seated players drop below 2.
- **Hide them from `lobby_list`.** Creating the lobby only at both-accept
  means it never exists while unjoinable, so the browser needs no filter — but
  add the `automatch` marker on `Lobby` anyway and skip those rows at the
  `LobbyListRow` build site, so a future rematch-hold cannot leak one.
- **`MAX_LOBBIES` is 64 and automatch rooms draw from the same pool.** A
  server that is full of rooms cannot start a pairing; the pair gets
  `automatch_requeue` with `reason: "lobby_limit"`, not a silent drop.
- **Metrics** ([`metrics.rs`](../src/metrics.rs)): queued gauge per
  `(game_name, ruleset_id)`, pairs formed, accept / decline / timeout counts,
  and time-to-pair. Wait time is the number that tells an operator whether the
  pool is alive.

## 10. Rematch (v1.1, spec'd now)

After a soft-return the pair is still seated in a room whose host is a
sentinel, so neither can press Play. Left there, v1 destroys the room and the
players go back to the browser — which throws away the scarcest thing in a
small pool, an opponent you already found.

Spec'd here so the client state machine is not retrofitted later:

```json
{ "op": "automatch_rematch", "vote": true }
```
```json
{ "op": "automatch_rematch_state", "ok": true, "you": true, "peer": false,
  "deadline_secs": 15 }
```

Both yes → the server runs the start path again. The existing code already
allocates a fresh `session_id` per start, which is the thing rematch actually
needs. Either no, or the deadline passing, destroys the room.

## 11. Client surface (recomp-ui)

Append-only additions to `RecompLauncherCNetplayCallbacks`
(recomp-ui [`src/recomp_launcher.h`](https://github.com/mstan/recomp-ui/blob/master/src/recomp_launcher.h)),
guarded by a `RECOMP_LAUNCHER_HAS_AUTOMATCH` define so a runner and a UI can
be updated in either order — the same pattern the account callbacks use:

```c
/* 1 when the CONFIGURED server offers automatch (it has rulesets loaded).
 * Draw no queue affordance when this says no. */
int  (*automatch_available)(void* ctx);
int  (*automatch_ruleset_count)(void* ctx);
int  (*automatch_ruleset_get)(void* ctx, int index,
                              RecompLauncherCNetplayRuleset* out);
/* 0 = accepted. <0 and automatch_error() says why (need_account, cooldown,
 * mods_not_pooled, …). The title list is built by the CALLER: standalone
 * recomp-ui passes the running game and nothing else. */
int  (*automatch_queue)(void* ctx, const char* ruleset_id);
int  (*automatch_cancel)(void* ctx);
/* RecompLauncherCAutomatchState: IDLE / QUEUED / FOUND / ACCEPTED / FAILED */
int  (*automatch_state)(void* ctx);
int  (*automatch_queued_secs)(void* ctx);
int  (*automatch_pool)(void* ctx);
int  (*automatch_found_get)(void* ctx, RecompLauncherCNetplayFound* out);
int  (*automatch_accept)(void* ctx, int accept);
const char* (*automatch_error)(void* ctx);
```

These have **landed** in recomp-ui, along with
`RecompLauncherCNetplayRuleset` / `RecompLauncherCNetplayFound` and the
`RECOMP_LAUNCHER_AUTOMATCH_*` state enum; the header is the ABI truth and
carries the per-field notes. The launcher draws the ⚡ Automatch button in
online netplay (LAN keeps Join Direct), the queue-type picker when a server
offers more than one ruleset, the elapsed clock and queue population while
waiting, and the accept gate. Everything after both-accept is the existing
`launch_pending` → `fill_launch` → boot path, untouched.

No backend implements the callbacks yet, so the button is gated off
`automatch_available` and sits disabled with the reason in its tooltip rather
than being offered and then found not to work.

Two things the launcher owns and the server cannot:

- **The opt-in title list.** Standalone recomp-ui runs one game and opts in
  that one. Retro Launcher will present the installed set with per-title
  checkboxes and pass several. The protocol is ready for both; the UI is what
  differs.
- **The mods gate (§5).** Queue is unavailable, with the reason on screen,
  while any sim-affecting mod feature is enabled.

## 12. Compatibility

Additive in the same way the Discord login was:

- Every op here is new. A client that has never heard of automatch connects,
  lists, joins, chats and hosts exactly as before.
- `004` only adds tables. No column on `players` changes.
- With no `automatch_rulesets.toml`, automatch is off and the server is
  byte-identical to one built before this document — `automatch_available`
  answers no and the UI draws nothing.
- Human-hosted rooms are untouched: same `create`, same `join`, same host
  authority, same `start`.

## 13. Non-goals (v1)

- Rating, matchmaking rating, win/loss records, leaderboards. The server does
  not observe who won — the sim runs on the clients — and a recorded winner it
  cannot observe would be a synthesized result.
- Party sizes above 2. Group-fill changes pairing and makes the accept gate
  N-way.
- Modded pools.
- Cross-region relay selection. One relay, one estimate.
- Penalizing early disconnects (§8).
- Spectators in an automatch room.

## 14. Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `AUTOMATCH_RULESETS_PATH` | `data/automatch_rulesets.toml` | Ruleset definitions. Absent or empty → automatch off, and `automatch_available` says no. |
| `AUTOMATCH_QUEUE_MAX` | `256` | Live tickets before `queue_full`. |
| `AUTOMATCH_ACCEPT_SECS` | `15` | Accept-gate deadline. |
| `AUTOMATCH_START_DELAY_SECS` | `3` | Settle between both-accept and the start path (§9). |
| `AUTOMATCH_RTT_START_MS` | `120` | Opening combined-RTT ceiling. |
| `AUTOMATCH_RTT_STEP_MS` | `60` | Widen step. |
| `AUTOMATCH_RTT_WIDEN_SECS` | `20` | Seconds between widen steps. |
| `AUTOMATCH_RTT_MAX_MS` | `400` | Last ceiling before the filter goes unlimited. |
| `AUTOMATCH_REMATCH_COOLDOWN_SECS` | `300` | Avoid-last-opponent window. |
| `AUTOMATCH_DODGE_COOLDOWNS` | `60` | Cooldown ladder, seconds, by strikes in 24 h. Flat by default; see §8. |
| `AUTOMATCH_RETENTION_DAYS` | `30` | Prune `automatch_strikes` / `automatch_pairings`. `0` = prune once a row stops mattering. |

There is no `AUTOMATCH_ENABLED`. Automatch is on when rulesets load and off
when they do not, so there is one place to look and no state where the flag and
the config disagree.

**Implemented today: `AUTOMATCH_RETENTION_DAYS` and
`AUTOMATCH_REMATCH_COOLDOWN_SECS`** (`src/automatch.rs`, `004_automatch.sql`) —
the two tables and the sweep that prunes them, which is the part that outlives
a process and therefore could not wait for the queue. The rest of this table is
read by nothing yet: setting one changes no behaviour until the queue lands.

## Related

- [WS_LOBBY.md](WS_LOBBY.md) — the protocol this extends
- [LOBBY.md](LOBBY.md) — HTTP `/v1` rooms API
- [HOW_IT_WORKS.md](HOW_IT_WORKS.md) — architecture
- [PRIVACY.md](PRIVACY.md) — what is retained; §8's two tables are new rows for it

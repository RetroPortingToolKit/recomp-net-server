//! Player-submitted chat reports.
//!
//! The server records; it never adjudicates. Rows land in `chat_reports`
//! (`006_chat_reports.sql`), which is the contract a moderation bot is meant
//! to be written against — there is deliberately no HTTP API here, because a
//! schema is a more stable thing to build on than an endpoint invented before
//! anyone knows what the tool needs.
//!
//! The text stored is the SERVER's copy of the line it relayed, looked up by a
//! message id, not anything the reporter supplied. A reporter who could supply
//! the text could fabricate a message and get somebody sanctioned for it.

use sqlx::SqlitePool;
use tracing::{info, warn};

/// Reasons the client may offer. Anything else is stored as `other` rather
/// than rejected: the category exists so a bot can count and alert, and a
/// client from a later version offering a finer one should not have its report
/// thrown away over a label.
pub const REASONS: &[&str] = &[
    "harassment",
    "hate_speech",
    "sexual_content",
    "spam",
    "threats",
    "cheating_claim",
    "other",
];

pub fn normalize_reason(reason: &str) -> String {
    let r = reason.trim().to_ascii_lowercase();
    if REASONS.contains(&r.as_str()) {
        r
    } else {
        "other".to_string()
    }
}

/// Everything one report records. Assembled by the caller from the server's
/// own chat ring plus the reporter's category and note.
#[derive(Debug, Clone, Default)]
pub struct ChatReport {
    pub reporter_id: String,
    pub accused_id: String,
    /// The first reported message. Indexed, and what the uniqueness constraint
    /// keys on, so reporting the same burst twice is still one row.
    pub message_id: String,
    pub message_text: String,
    pub accused_name: String,
    pub context: String,
    /// Every reported line, oldest first, "sender: text". One report may cover
    /// a burst -- harassment usually is one -- and making somebody file six
    /// reports to describe one incident produces six rows that each look
    /// minor.
    pub transcript: String,
    pub message_count: i64,
    pub scope: String,
    pub lobby_id: String,
    pub game_name: String,
    pub game_version: String,
    /// Emulated machine. Metadata only; never used to name a file.
    pub platform: String,
    pub server: String,
    pub reason: String,
    pub note: String,
}

/// Free-text the reporter controls. Bounded hard: a note is a sentence for a
/// human, not a channel.
pub const NOTE_MAX_CHARS: usize = 500;

fn clamp(s: &str, max: usize) -> String {
    s.trim().chars().take(max).collect()
}

/// How many reports one account may file in an hour.
///
/// Report spam is its own abuse — a group filing in concert can bury a queue
/// or manufacture a count against somebody — and the limit is what stops the
/// table being a harassment tool in its own right. Generous enough that a bad
/// session genuinely worth reporting several times is not cut off.
pub const REPORTS_PER_HOUR: i64 = 20;

pub async fn recent_report_count(pool: &SqlitePool, reporter_id: &str) -> i64 {
    sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM chat_reports \
         WHERE reporter_id = ? AND created_at >= datetime('now', '-1 hours')",
    )
    .bind(reporter_id)
    .fetch_one(pool)
    .await
    .map(|(n,)| n)
    .unwrap_or(0)
}

/// Outcome of a submission, so the caller can answer the client precisely.
#[derive(Debug, PartialEq, Eq)]
pub enum Submitted {
    Ok,
    /// Already reported by this account. Not an error worth showing as one —
    /// they did what they meant to — but it must not add a row, or one person
    /// could inflate a count by clicking.
    Duplicate,
    RateLimited,
    Failed,
}

/// Write the transcript beside the row, and return the path stored with it.
///
/// FILE NAMING. `<yyyy>/<mm>/<report-id>.txt`, and nothing else. Not the game,
/// not the platform, not the player. One moderation queue spans every title on
/// every console and is read by one person; naming files after the console
/// would fragment the evidence along a line that has nothing to do with
/// moderation, and naming them after a game or a player would make a directory
/// listing disclose who has been reported to anyone who can see the folder.
/// Which game, which console and which account are all INSIDE the file.
///
/// Best effort: a dump that cannot be written costs the path, never the row.
/// The database copy in `transcript` is the record; the file is a convenience
/// for whoever is reading with a text editor rather than SQL.
async fn write_dump(dir: &str, id: &str, r: &ChatReport) -> String {
    if dir.is_empty() {
        return String::new();
    }
    let now = chrono::Utc::now();
    let rel = format!("{}/{}/{id}.txt", now.format("%Y"), now.format("%m"));
    let full = std::path::Path::new(dir).join(&rel);
    if let Some(parent) = full.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            warn!(error = %e, "chat report dump directory not created");
            return String::new();
        }
    }
    let body = format!(
        "report:   {id}\nfiled:    {}\nreporter: {}\naccused:  {} ({})\n\
         game:     {} {}\nplatform: {}\nserver:   {}\nscope:    {}\n\
         lobby:    {}\nreason:   {}\nnote:     {}\n\n\
         --- context (preceding lines, same room) ---\n{}\n\n\
         --- reported ({} message(s)) ---\n{}\n",
        now.to_rfc3339(),
        r.reporter_id,
        r.accused_name,
        r.accused_id,
        r.game_name,
        r.game_version,
        r.platform,
        r.server,
        r.scope,
        r.lobby_id,
        normalize_reason(&r.reason),
        r.note,
        r.context,
        r.message_count,
        r.transcript,
    );
    match tokio::fs::write(&full, body).await {
        Ok(()) => rel,
        Err(e) => {
            warn!(error = %e, "chat report dump not written");
            String::new()
        }
    }
}

pub async fn record(pool: &SqlitePool, dump_dir: &str, r: &ChatReport) -> Submitted {
    if recent_report_count(pool, &r.reporter_id).await >= REPORTS_PER_HOUR {
        warn!(reporter = %r.reporter_id, "chat report rate limit reached");
        return Submitted::RateLimited;
    }
    let id = uuid::Uuid::new_v4().to_string();
    let dump_path = write_dump(dump_dir, &id, r).await;
    let res = sqlx::query(
        "INSERT INTO chat_reports (id, reporter_id, accused_id, message_id, \
         message_text, accused_name, context, transcript, message_count, \
         scope, lobby_id, game_name, game_version, platform, server, \
         dump_path, reason, note) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(&r.reporter_id)
    .bind(&r.accused_id)
    .bind(clamp(&r.message_id, 64))
    .bind(clamp(&r.message_text, 512))
    .bind(clamp(&r.accused_name, 64))
    .bind(clamp(&r.context, 2048))
    .bind(clamp(&r.transcript, 8192))
    .bind(r.message_count.clamp(0, 64))
    .bind(clamp(&r.scope, 16))
    .bind(clamp(&r.lobby_id, 64))
    .bind(clamp(&r.game_name, 128))
    .bind(clamp(&r.game_version, 64))
    .bind(clamp(&r.platform, 32))
    .bind(clamp(&r.server, 256))
    .bind(&dump_path)
    .bind(normalize_reason(&r.reason))
    .bind(clamp(&r.note, NOTE_MAX_CHARS))
    .execute(pool)
    .await;

    match res {
        Ok(_) => {
            /* info!, and without the message text. The queue is where the
             * content belongs; a log line is read by whoever is tailing the
             * server for an unrelated reason, and reported chat is not
             * something to spray across it. */
            info!(
                reporter = %r.reporter_id,
                accused = %r.accused_id,
                reason = %normalize_reason(&r.reason),
                scope = %r.scope,
                "chat report filed"
            );
            Submitted::Ok
        }
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => Submitted::Duplicate,
        Err(e) => {
            warn!(error = %e, "chat report not recorded");
            Submitted::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> SqlitePool {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        for id in ["r1", "r2", "acc"] {
            sqlx::query("INSERT INTO players (id, api_token_hash) VALUES (?, 'x')")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool
    }

    fn rep(reporter: &str, mid: &str) -> ChatReport {
        ChatReport {
            reporter_id: reporter.into(),
            accused_id: "acc".into(),
            message_id: mid.into(),
            message_text: "the reported line".into(),
            accused_name: "Someone".into(),
            context: "a: hello\nb: hi".into(),
            transcript: "Someone: the reported line".into(),
            message_count: 1,
            scope: "lobby".into(),
            lobby_id: "L1".into(),
            game_name: "G".into(),
            game_version: "1.0".into(),
            platform: "snes".into(),
            server: "wss://example".into(),
            reason: "harassment".into(),
            note: "please look".into(),
        }
    }

    #[tokio::test]
    async fn a_report_stores_the_servers_copy_and_opens() {
        let pool = db().await;
        assert_eq!(record(&pool, "", &rep("r1", "m1")).await, Submitted::Ok);
        let (text, status, reason): (String, String, String) = sqlx::query_as(
            "SELECT message_text, status, reason FROM chat_reports",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(text, "the reported line");
        // 'open' is what a bot polls for; nothing in this server moves it.
        assert_eq!(status, "open");
        assert_eq!(reason, "harassment");
    }

    #[tokio::test]
    async fn reporting_the_same_message_twice_adds_one_row() {
        // Otherwise one person clicking repeatedly inflates a count against
        // somebody, and the queue looks busier than it is.
        let pool = db().await;
        assert_eq!(record(&pool, "", &rep("r1", "m1")).await, Submitted::Ok);
        assert_eq!(record(&pool, "", &rep("r1", "m1")).await, Submitted::Duplicate);
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chat_reports")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn two_people_may_report_the_same_message() {
        // The uniqueness is per reporter, not per message: several people
        // reporting one line is the signal, and collapsing it would erase it.
        let pool = db().await;
        assert_eq!(record(&pool, "", &rep("r1", "m1")).await, Submitted::Ok);
        assert_eq!(record(&pool, "", &rep("r2", "m1")).await, Submitted::Ok);
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chat_reports")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn report_spam_is_capped() {
        let pool = db().await;
        for i in 0..REPORTS_PER_HOUR {
            assert_eq!(
                record(&pool, "", &rep("r1", &format!("m{i}"))).await,
                Submitted::Ok,
                "report {i} should have been accepted"
            );
        }
        assert_eq!(
            record(&pool, "", &rep("r1", "over")).await,
            Submitted::RateLimited
        );
        // The cap is per reporter: someone else is unaffected by it.
        assert_eq!(record(&pool, "", &rep("r2", "other")).await, Submitted::Ok);
    }

    #[tokio::test]
    async fn a_hostile_note_is_truncated_not_rejected() {
        // A note is free text from a client. Bounded, but never a reason to
        // lose the report it came attached to.
        let pool = db().await;
        let mut r = rep("r1", "m1");
        r.note = "x".repeat(50_000);
        r.reason = "not-a-real-reason".into();
        assert_eq!(record(&pool, "", &r).await, Submitted::Ok);
        let (note, reason): (String, String) =
            sqlx::query_as("SELECT note, reason FROM chat_reports")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(note.chars().count(), NOTE_MAX_CHARS);
        // An unknown category is stored as `other`, never dropped.
        assert_eq!(reason, "other");
    }

    #[tokio::test]
    async fn a_dump_is_named_by_date_and_id_only() {
        // The naming rule, asserted rather than left to a comment: one queue
        // spans every title and console, and a file named after the game, the
        // platform or a player would fragment the evidence and make a folder
        // listing disclose who has been reported.
        let pool = db().await;
        let dir = std::env::temp_dir().join(format!("rep-{}", uuid::Uuid::new_v4()));
        let dir_s = dir.to_string_lossy().to_string();
        assert_eq!(record(&pool, &dir_s, &rep("r1", "m1")).await, Submitted::Ok);

        let (path,): (String,) = sqlx::query_as("SELECT dump_path FROM chat_reports")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!path.is_empty(), "a dump path should have been stored");
        for banned in ["snes", "psx", "G", "Someone", "acc", "r1"] {
            assert!(
                !path.contains(banned),
                "dump path {path} must not be named after {banned}"
            );
        }
        // It is <yyyy>/<mm>/<uuid>.txt and nothing else.
        assert!(path.ends_with(".txt"), "{path}");
        assert_eq!(path.split('/').count(), 3, "{path}");

        // The metadata lives INSIDE the file, which is the other half of the
        // rule: neutral outside, complete inside.
        let body = tokio::fs::read_to_string(dir.join(&path)).await.unwrap();
        assert!(body.contains("platform: snes"), "{body}");
        assert!(body.contains("game:     G"), "{body}");
        assert!(body.contains("the reported line"), "{body}");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn dumps_disabled_still_files_the_report() {
        // The row is the record; the file is a convenience. Losing the second
        // must never cost the first.
        let pool = db().await;
        assert_eq!(record(&pool, "", &rep("r1", "m1")).await, Submitted::Ok);
        let (path, n): (String, i64) =
            sqlx::query_as("SELECT dump_path, message_count FROM chat_reports")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(path, "");
        assert_eq!(n, 1);
    }

    #[test]
    fn every_offered_reason_survives_normalization() {
        // A category the client can offer but the server silently rewrites is
        // a category no bot can ever count.
        for r in REASONS {
            assert_eq!(&normalize_reason(r), r);
        }
        assert_eq!(normalize_reason("  HARASSMENT "), "harassment");
    }
}

//! Automatch retention.
//!
//! The queue, the accept gate and the pairing loop live in a later change;
//! what is here is the part that outlives a process — the two tables from
//! `004_automatch.sql` and the sweep that keeps them from growing without
//! bound.
//!
//! # Why a knob at all
//!
//! `automatch_pairings` records who played whom. Keeping that indefinitely is
//! a choice an operator should make deliberately rather than inherit from a
//! default, so it has a dial and the dial is documented in `docs/PRIVACY.md`
//! next to everything else this server retains.
//!
//! # Why the cutoff is a maximum, not the setting
//!
//! `AUTOMATCH_RETENTION_DAYS=0` means "prune a row as soon as it stops
//! affecting a decision" — it does NOT mean "delete everything now". A strike
//! inside the 24 h window is still raising somebody's cooldown, and a pairing
//! inside the rematch window is still steering the next pair away from a
//! repeat. Deleting either would silently undo enforcement that is currently
//! running, so the cutoff each table uses is the LONGER of the operator's
//! retention and the window the feature itself still reads.

use anyhow::Result;
use sqlx::SqlitePool;
use tracing::{debug, info};

/// How far back a cooldown counts strikes. The ladder in `docs/AUTOMATCH.md`
/// §8 is "strikes in the last 24 h", so a strike older than this cannot raise
/// anyone's cooldown and is history rather than enforcement.
pub const STRIKE_WINDOW_SECS: u64 = 24 * 60 * 60;

/// How often the sweep runs. Daily: nothing here is urgent, and a prune that
/// races the queue for the write lock is worse than a prune that is a few
/// hours late.
pub const SWEEP_INTERVAL_SECS: u64 = 24 * 60 * 60;

/// Rows deleted by one sweep, for the log line and for tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pruned {
    pub strikes: u64,
    pub pairings: u64,
}

impl Pruned {
    pub fn total(&self) -> u64 {
        self.strikes + self.pairings
    }
}

/// Seconds of history a table keeps: the operator's retention, floored by the
/// window the feature still reads from it.
///
/// Separated out and tested because it is the one place a wrong answer is
/// invisible — an over-long cutoff merely keeps rows, but an over-short one
/// deletes a strike that is currently holding a cooldown, and the symptom is
/// a dodger who is no longer on cooldown, which nobody reports as a bug.
pub fn cutoff_secs(retention_days: u64, functional_window_secs: u64) -> u64 {
    let retention = retention_days.saturating_mul(24 * 60 * 60);
    retention.max(functional_window_secs)
}

/// Delete rows past their cutoff. Idempotent, and safe to run on a database
/// where automatch has never been used.
pub async fn prune(
    pool: &SqlitePool,
    retention_days: u64,
    rematch_cooldown_secs: u64,
) -> Result<Pruned> {
    let strike_cut = cutoff_secs(retention_days, STRIKE_WINDOW_SECS);
    let pairing_cut = cutoff_secs(retention_days, rematch_cooldown_secs);

    /* `datetime('now', '-N seconds')` rather than arithmetic in Rust: the
     * columns are SQLite datetime text written by `datetime('now')`, so the
     * comparison has to be made by the same clock that wrote them. */
    let strikes = sqlx::query("DELETE FROM automatch_strikes WHERE created_at < datetime('now', ?)")
        .bind(format!("-{strike_cut} seconds"))
        .execute(pool)
        .await?
        .rows_affected();

    let pairings = sqlx::query("DELETE FROM automatch_pairings WHERE paired_at < datetime('now', ?)")
        .bind(format!("-{pairing_cut} seconds"))
        .execute(pool)
        .await?
        .rows_affected();

    Ok(Pruned { strikes, pairings })
}

/// Run the sweep now, then daily. The immediate first pass matters: a server
/// that is restarted more often than once a day would otherwise never prune.
pub fn spawn_sweep(pool: SqlitePool, retention_days: u64, rematch_cooldown_secs: u64) {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(SWEEP_INTERVAL_SECS));
        loop {
            interval.tick().await;
            match prune(&pool, retention_days, rematch_cooldown_secs).await {
                Ok(p) if p.total() > 0 => {
                    info!(
                        strikes = p.strikes,
                        pairings = p.pairings,
                        retention_days,
                        "automatch retention sweep"
                    );
                }
                Ok(_) => debug!(retention_days, "automatch retention sweep: nothing to prune"),
                /* A failed sweep is not fatal and must not take the lobby with
                 * it: the tables grow until the next pass. */
                Err(e) => info!(error = %e, "automatch retention sweep failed"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_retention_still_keeps_what_is_being_enforced() {
        /* The point of the floor: 0 means "as soon as it stops mattering",
         * not "now". A strike inside the 24 h window is holding a cooldown. */
        assert_eq!(cutoff_secs(0, STRIKE_WINDOW_SECS), STRIKE_WINDOW_SECS);
        assert_eq!(cutoff_secs(0, 300), 300);
    }

    #[test]
    fn a_longer_retention_wins_over_the_functional_window() {
        assert_eq!(cutoff_secs(30, STRIKE_WINDOW_SECS), 30 * 86_400);
        assert_eq!(cutoff_secs(1, 300), 86_400);
    }

    #[test]
    fn a_day_of_retention_is_exactly_the_strike_window() {
        /* Neither value wins by accident: they are equal, and the result must
         * be that value rather than a doubled or zeroed one. */
        assert_eq!(cutoff_secs(1, STRIKE_WINDOW_SECS), STRIKE_WINDOW_SECS);
    }

    #[test]
    fn an_absurd_retention_does_not_overflow_into_a_short_cutoff() {
        /* saturating_mul, so a fat-fingered env var keeps everything rather
         * than wrapping to a cutoff that deletes everything. */
        let c = cutoff_secs(u64::MAX, STRIKE_WINDOW_SECS);
        assert!(c >= STRIKE_WINDOW_SECS);
        assert_eq!(c, u64::MAX);
    }

    #[tokio::test]
    async fn prune_is_safe_on_a_database_nobody_has_queued_on() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let p = prune(&pool, 30, 300).await.unwrap();
        assert_eq!(p, Pruned::default());
    }

    #[tokio::test]
    async fn old_rows_go_and_rows_still_being_read_stay() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO players (id, api_token_hash) VALUES ('p1', 'x'), ('p2', 'y')")
            .execute(&pool)
            .await
            .unwrap();

        /* One strike inside the 24 h window, one well outside it. */
        sqlx::query(
            "INSERT INTO automatch_strikes (id, player_id, kind, created_at) VALUES \
             ('s_new', 'p1', 'decline', datetime('now', '-1 hours')), \
             ('s_old', 'p1', 'timeout', datetime('now', '-40 days'))",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO automatch_pairings (match_id, game_name, ruleset_id, player_a, player_b, paired_at) \
             VALUES ('m_new', 'g', 'standard', 'p1', 'p2', datetime('now', '-1 minutes')), \
                    ('m_old', 'g', 'standard', 'p1', 'p2', datetime('now', '-40 days'))",
        )
        .execute(&pool)
        .await
        .unwrap();

        let p = prune(&pool, 30, 300).await.unwrap();
        assert_eq!(p, Pruned { strikes: 1, pairings: 1 });

        let (s,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM automatch_strikes")
            .fetch_one(&pool)
            .await
            .unwrap();
        let (m,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM automatch_pairings")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!((s, m), (1, 1), "the recent rows are still being read");
    }

    #[tokio::test]
    async fn zero_retention_does_not_drop_a_cooldown_that_is_still_running() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO players (id, api_token_hash) VALUES ('p1', 'x')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO automatch_strikes (id, player_id, kind, created_at) VALUES \
             ('s', 'p1', 'decline', datetime('now', '-2 hours'))",
        )
        .execute(&pool)
        .await
        .unwrap();

        let p = prune(&pool, 0, 300).await.unwrap();
        assert_eq!(p.strikes, 0, "a 2h-old strike is inside the 24h window");
    }
}

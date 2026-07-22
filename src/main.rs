//! recomp-net-server — lobby / signaling control plane for recomp-net hosts.
//!
//! Configuration and secrets: see `docs/SECURITY.md` and `src/config.rs`.
//!
//! Run with `--debug` for request tracing (tower-http) and structured lobby logs.
//! Use `RUST_LOG` to override levels (e.g. `RUST_LOG=warn`).

use anyhow::Context;
use axum::{routing::get, Json, Router};
use recomp_net_server::config::Config;
use recomp_net_server::rooms::RoomRegistry;
use recomp_net_server::routes;
use recomp_net_server::signal::SignalStore;
use recomp_net_server::AppState;
use serde::Serialize;
use sqlx::sqlite::SqlitePoolOptions;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    require_auth: bool,
    jwt_configured: bool,
    database_configured: bool,
    turn_configured: bool,
}

fn parse_cli_debug_flag() -> bool {
    std::env::args().any(|a| a == "--debug")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let debug_cli = parse_cli_debug_flag();

    let default_filter = if debug_cli {
        "info,tower_http=trace,recomp_net_server=debug"
    } else {
        "info"
    };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        default_filter
            .parse()
            .expect("embedded default EnvFilter directives are valid")
    });

    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let config = Config::from_env().context("invalid configuration")?;
    let db_url = config.effective_database_url();
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .with_context(|| format!("failed to connect database {db_url}"))?;

    let migr_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
    sqlx::migrate::Migrator::new(migr_path)
        .await
        .context("migrations load")?
        .run(&pool)
        .await
        .context("migrations run")?;

    let state = AppState {
        pool: pool.clone(),
        config: Arc::new(config.clone()),
        rooms: Arc::new(Mutex::new(RoomRegistry::default())),
        signals: Arc::new(Mutex::new(SignalStore::default())),
        ws_lobby: recomp_net_server::ws_lobby::WsLobbyHub::new(),
        debug: debug_cli,
    };

    let janitor = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut rooms = janitor.rooms.lock().await;
            rooms.purge_stale(
                janitor.config.heartbeat_timeout_secs,
                janitor.config.room_idle_secs,
            );
        }
    });

    let turn_configured = recomp_net_server::turn_credentials::TurnCredentialConfig::from_env()
        .is_some();

    info!(
        bind = %config.bind_addr,
        require_auth = config.require_auth,
        jwt_configured = config.jwt_secret_current.is_some(),
        database = %db_url,
        turn_configured,
        allowlist_len = config.game_allowlist.len(),
        debug = debug_cli,
        "starting recomp-net-server"
    );

    let cfg_clone = config.clone();
    let mut app = Router::new()
        .route(
            "/health",
            get(move || async move {
                Json(HealthResponse {
                    status: "ok",
                    require_auth: cfg_clone.require_auth,
                    jwt_configured: cfg_clone.jwt_secret_current.is_some(),
                    database_configured: true,
                    turn_configured,
                })
            }),
        )
        .merge(routes::api_router())
        .merge(recomp_net_server::ws_lobby::ws_router())
        .with_state(state);

    if debug_cli {
        app = app.layer(TraceLayer::new_for_http());
    }

    let addr: SocketAddr = config
        .bind_addr
        .parse()
        .with_context(|| format!("invalid BIND_ADDR: {}", config.bind_addr))?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    info!(%addr, "listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("server error")?;

    Ok(())
}

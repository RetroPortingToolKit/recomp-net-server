//! recomp-net-server — lobby / signaling control plane for recomp-net hosts.
//!
//! Configuration and secrets: see `docs/SECURITY.md` and `src/config.rs`.
//!
//! Run with `--debug` for request tracing (tower-http) and structured lobby logs.
//! Use `RUST_LOG` to override levels (e.g. `RUST_LOG=warn`).
//!
//! Usage: `GET /stats` (JSON), `GET /stats/ui` (browser), `GET /metrics` (Prometheus).

use anyhow::Context;
use axum::response::Html;
use axum::{routing::get, Json, Router};
use axum_prometheus::PrometheusMetricLayer;
use recomp_net_server::config::Config;
use recomp_net_server::metrics::{self, StatsSnapshot};
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

async fn refresh_gauges(state: &AppState) -> (usize, usize, usize) {
    let ws_clients = state.ws_lobby.client_count().await;
    let ws_lobbies = state.ws_lobby.lobby_count().await;
    let http_rooms = state.rooms.lock().await.len();
    metrics::set_gauges(ws_clients, ws_lobbies, http_rooms);
    (ws_clients, ws_lobbies, http_rooms)
}

async fn stats_handler(axum::extract::State(state): axum::extract::State<AppState>) -> Json<StatsSnapshot> {
    let (ws_clients, ws_lobbies, http_rooms) = refresh_gauges(&state).await;
    let ws_lobbies_by_game = state.ws_lobby.counts_by_game().await;
    let http_rooms_by_game = state.rooms.lock().await.counts_by_game();
    Json(StatsSnapshot {
        ws_clients,
        ws_lobbies,
        ws_lobbies_by_game,
        http_rooms,
        http_rooms_by_game,
        totals: metrics::totals(),
    })
}

async fn stats_ui() -> Html<&'static str> {
    Html(STATS_UI_HTML)
}

const STATS_UI_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>recomp-net-server usage</title>
  <style>
    :root { color-scheme: light dark; font-family: ui-sans-serif, system-ui, sans-serif; }
    body { margin: 1.5rem; line-height: 1.4; max-width: 52rem; }
    h1 { font-size: 1.25rem; margin: 0 0 0.25rem; }
    .sub { opacity: 0.7; font-size: 0.9rem; margin-bottom: 1.25rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(9rem, 1fr)); gap: 0.75rem; }
    .card { border: 1px solid color-mix(in srgb, CanvasText 18%, transparent); border-radius: 0.5rem; padding: 0.75rem 0.9rem; }
    .card .n { font-size: 1.75rem; font-variant-numeric: tabular-nums; font-weight: 650; }
    .card .l { opacity: 0.7; font-size: 0.8rem; }
    h2 { font-size: 1rem; margin: 1.5rem 0 0.5rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
    th, td { text-align: left; padding: 0.35rem 0.4rem; border-bottom: 1px solid color-mix(in srgb, CanvasText 12%, transparent); }
    th { opacity: 0.7; font-weight: 550; }
    pre { font-size: 0.8rem; overflow: auto; }
    .err { color: tomato; }
  </style>
</head>
<body>
  <h1>recomp-net-server</h1>
  <p class="sub">Live usage · auto-refresh 2s · <a href="/stats">/stats</a> · <a href="/metrics">/metrics</a></p>
  <div id="live" class="grid"></div>
  <h2>By game (live)</h2>
  <table><thead><tr><th>Surface</th><th>Game</th><th>Count</th></tr></thead><tbody id="games"></tbody></table>
  <h2>Process totals</h2>
  <div id="totals" class="grid"></div>
  <p class="sub" id="updated"></p>
  <script>
    const liveEl = document.getElementById('live');
    const gamesEl = document.getElementById('games');
    const totalsEl = document.getElementById('totals');
    const updatedEl = document.getElementById('updated');
    function card(label, n) {
      return `<div class="card"><div class="n">${n}</div><div class="l">${label}</div></div>`;
    }
    function rows(surface, map) {
      return Object.entries(map || {}).map(([g, n]) =>
        `<tr><td>${surface}</td><td>${g}</td><td>${n}</td></tr>`).join('');
    }
    async function tick() {
      try {
        const r = await fetch('/stats');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const s = await r.json();
        liveEl.innerHTML =
          card('WS clients', s.ws_clients) +
          card('WS lobbies', s.ws_lobbies) +
          card('HTTP rooms', s.http_rooms);
        const g = rows('ws', s.ws_lobbies_by_game) + rows('http', s.http_rooms_by_game);
        gamesEl.innerHTML = g || '<tr><td colspan="3">none</td></tr>';
        const t = s.totals || {};
        totalsEl.innerHTML = [
          ['WS connects', t.ws_connects],
          ['WS creates', t.ws_lobby_creates],
          ['WS joins', t.ws_lobby_joins],
          ['WS join fails', t.ws_lobby_join_failures],
          ['WS starts', t.ws_lobby_starts],
          ['HTTP creates', t.http_room_creates],
          ['HTTP joins', t.http_room_joins],
          ['HTTP starts', t.http_room_starts],
          ['TURN creds', t.http_turn_credentials],
        ].map(([l, n]) => card(l, n ?? 0)).join('');
        updatedEl.textContent = 'Updated ' + new Date().toLocaleTimeString();
      } catch (e) {
        updatedEl.innerHTML = '<span class="err">' + e + '</span>';
      }
    }
    tick();
    setInterval(tick, 2000);
  </script>
</body>
</html>
"#;

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

    let mut config = Config::from_env().context("invalid configuration")?;
    config
        .resolve_input_relay_advertise()
        .context("input relay advertise host")?;
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

    let input_relay = recomp_net_server::input_relay::InputRelay::start(&config)
        .await
        .context("input relay")?;

    let state = AppState {
        pool: pool.clone(),
        config: Arc::new(config.clone()),
        rooms: Arc::new(Mutex::new(RoomRegistry::default())),
        signals: Arc::new(Mutex::new(SignalStore::default())),
        ws_lobby: recomp_net_server::ws_lobby::WsLobbyHub::new(),
        input_relay,
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

    metrics::describe();
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();

    info!(
        bind = %config.bind_addr,
        require_auth = config.require_auth,
        jwt_configured = config.jwt_secret_current.is_some(),
        database = %db_url,
        turn_configured,
        input_relay = config.input_relay_enabled,
        input_relay_bind = %config.input_relay_bind,
        input_relay_advertise = %format!(
            "{}:{}",
            config.input_relay_advertise_host, config.input_relay_advertise_port
        ),
        input_relay_lan = %if config.input_relay_lan_host.is_empty() {
            "(unset)".to_string()
        } else {
            format!(
                "{}:{}",
                config.input_relay_lan_host, config.input_relay_advertise_port
            )
        },
        input_relay_lan_gateway = %config
            .effective_input_relay_lan_gateway()
            .unwrap_or_else(|| "(unset)".to_string()),
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
        .route("/stats", get(stats_handler))
        .route("/stats/ui", get(stats_ui))
        .route(
            "/metrics",
            get({
                let metric_handle = metric_handle.clone();
                move |axum::extract::State(st): axum::extract::State<AppState>| {
                    let handle = metric_handle.clone();
                    async move {
                        refresh_gauges(&st).await;
                        handle.render()
                    }
                }
            }),
        )
        .merge(routes::api_router())
        .merge(recomp_net_server::ws_lobby::ws_router())
        .layer(prometheus_layer)
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
    info!("usage: GET /stats  GET /stats/ui  GET /metrics");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("server error")?;

    Ok(())
}

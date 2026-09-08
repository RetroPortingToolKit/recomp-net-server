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
use std::collections::BTreeMap;
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

async fn refresh_gauges(state: &AppState) -> LiveCounts {
    let ws_clients = state.ws_lobby.client_count().await;
    let ws_lobbies = state.ws_lobby.lobby_count().await;
    let ws_matches = state.ws_lobby.match_count().await;
    let ws_lobbies_by_game = state.ws_lobby.counts_by_game().await;
    let ws_matches_by_game = state.ws_lobby.match_counts_by_game().await;
    let rooms = state.rooms.lock().await;
    let http_rooms = rooms.len();
    let http_rooms_running = rooms.running_count();
    let http_rooms_by_game = rooms.counts_by_game();
    let http_rooms_running_by_game = rooms.running_counts_by_game();
    drop(rooms);
    let (input_relay_sessions, input_relay_sessions_active) =
        state.input_relay.session_counts().await;
    metrics::set_gauges(
        ws_clients,
        ws_lobbies,
        ws_matches,
        http_rooms,
        http_rooms_running,
    );
    /* Live "what is being played right now", on the same bounded game label
     * set as recomp_match_starts_total. */
    metrics::set_matches_by_game(metrics::SURFACE_WS, &ws_matches_by_game);
    metrics::set_matches_by_game(metrics::SURFACE_HTTP, &http_rooms_running_by_game);
    metrics::set_input_relay_sessions(input_relay_sessions);
    metrics::set_input_relay_sessions_active(input_relay_sessions_active);
    LiveCounts {
        ws_clients,
        ws_lobbies,
        ws_matches,
        ws_lobbies_by_game,
        ws_matches_by_game,
        http_rooms,
        http_rooms_running,
        http_rooms_by_game,
        http_rooms_running_by_game,
        input_relay_sessions,
        input_relay_sessions_active,
    }
}

struct LiveCounts {
    ws_clients: usize,
    ws_lobbies: usize,
    ws_matches: usize,
    ws_lobbies_by_game: BTreeMap<String, usize>,
    ws_matches_by_game: BTreeMap<String, usize>,
    http_rooms: usize,
    http_rooms_running: usize,
    http_rooms_by_game: BTreeMap<String, usize>,
    http_rooms_running_by_game: BTreeMap<String, usize>,
    input_relay_sessions: usize,
    input_relay_sessions_active: usize,
}

async fn stats_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<StatsSnapshot> {
    let live = refresh_gauges(&state).await;
    Json(StatsSnapshot {
        ws_clients: live.ws_clients,
        ws_lobbies: live.ws_lobbies,
        ws_lobbies_waiting: live.ws_lobbies.saturating_sub(live.ws_matches),
        ws_matches: live.ws_matches,
        ws_lobbies_by_game: live.ws_lobbies_by_game,
        ws_matches_by_game: live.ws_matches_by_game,
        http_rooms: live.http_rooms,
        http_rooms_running: live.http_rooms_running,
        http_rooms_by_game: live.http_rooms_by_game,
        http_rooms_running_by_game: live.http_rooms_running_by_game,
        ws_match_starts_by_game: metrics::match_starts_by_game(metrics::SURFACE_WS),
        http_match_starts_by_game: metrics::match_starts_by_game(metrics::SURFACE_HTTP),
        input_relay_sessions: live.input_relay_sessions,
        input_relay_sessions_active: live.input_relay_sessions_active,
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
  <h2>Match starts by game (since restart)</h2>
  <table><thead><tr><th>Surface</th><th>Game</th><th>Starts</th><th>Players</th><th>Avg seats</th></tr></thead><tbody id="starts"></tbody></table>
  <h2>Process totals</h2>
  <div id="totals" class="grid"></div>
  <p class="sub" id="updated"></p>
  <script>
    const liveEl = document.getElementById('live');
    const gamesEl = document.getElementById('games');
    const startsEl = document.getElementById('starts');
    const totalsEl = document.getElementById('totals');
    const updatedEl = document.getElementById('updated');
    function card(label, n) {
      return `<div class="card"><div class="n">${n}</div><div class="l">${label}</div></div>`;
    }
    function rows(surface, map) {
      return Object.entries(map || {}).map(([g, n]) =>
        `<tr><td>${surface}</td><td>${g}</td><td>${n}</td></tr>`).join('');
    }
    function startRows(surface, map) {
      return Object.entries(map || {})
        .sort((a, b) => (b[1].starts || 0) - (a[1].starts || 0))
        .map(([g, t]) => {
          const starts = t.starts || 0, players = t.players || 0;
          const avg = starts ? (players / starts).toFixed(1) : '0.0';
          return `<tr><td>${surface}</td><td>${g}</td><td>${starts}</td><td>${players}</td><td>${avg}</td></tr>`;
        }).join('');
    }
    async function tick() {
      try {
        const r = await fetch('/stats');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const s = await r.json();
        liveEl.innerHTML =
          card('WS clients', s.ws_clients) +
          card('WS waiting', s.ws_lobbies_waiting ?? Math.max(0, (s.ws_lobbies||0) - (s.ws_matches||0))) +
          card('WS matches', s.ws_matches ?? 0) +
          card('HTTP rooms', s.http_rooms) +
          card('HTTP running', s.http_rooms_running ?? 0) +
          card('SFU live', s.input_relay_sessions_active ?? 0);
        const g = rows('ws', s.ws_lobbies_by_game) +
          rows('ws-match', s.ws_matches_by_game) +
          rows('http', s.http_rooms_by_game) +
          rows('http-running', s.http_rooms_running_by_game);
        gamesEl.innerHTML = g || '<tr><td colspan="3">none</td></tr>';
        const st = startRows('ws', s.ws_match_starts_by_game) +
          startRows('http', s.http_match_starts_by_game);
        startsEl.innerHTML = st || '<tr><td colspan="5">none</td></tr>';
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

    /* Migrations are embedded at compile time, not read from disk. Reading them
     * from CARGO_MANIFEST_DIR only worked while every deployment was also a
     * build tree; a binary bundled on one machine and extracted on another
     * would have looked for the build machine's absolute path and failed to
     * start. sqlx::migrate! bakes migrations/ into the executable, so the
     * bundle is self-contained. */
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("migrations run")?;

    let input_relay = recomp_net_server::input_relay::InputRelay::start(&config)
        .await
        .context("input relay")?;

    recomp_net_server::chat_filter::init(
        config.chat_filter_enabled,
        config.chat_filter_extra_path.as_deref(),
    );

    let state = AppState {
        discord_logins: recomp_net_server::discord_auth::LoginStore::default(),
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .context("http client")?,
        pool: pool.clone(),
        config: Arc::new(config.clone()),
        rooms: Arc::new(Mutex::new(RoomRegistry::default())),
        signals: Arc::new(Mutex::new(SignalStore::default())),
        ws_lobby: recomp_net_server::ws_lobby::WsLobbyHub::with_geoip(
            config.geoip_db_path.as_deref(),
        ),
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

    let turn_configured =
        recomp_net_server::turn_credentials::TurnCredentialConfig::from_env().is_some();

    metrics::describe();
    metrics::init_game_labels(&config.game_allowlist, config.metrics_game_label_limit);
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

use mw_mission::api::build_router;
use mw_mission::db::Database;
use mw_mission::engine::MissionEngine;
use std::env;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

struct ServerConfig {
    addr: SocketAddr,
    db_path: String,
    reap_interval: Duration,
    cron_interval: Duration,
}

impl ServerConfig {
    fn from_env_and_args() -> Result<Self, String> {
        let mut addr_raw =
            env::var("MW_MISSION_ADDR").unwrap_or_else(|_| "127.0.0.1:4410".to_string());
        let mut db_path =
            env::var("MW_MISSION_DB").unwrap_or_else(|_| "./mw-mission.db".to_string());
        let mut reap_secs = env::var("MW_MISSION_REAP_INTERVAL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(5);
        let mut cron_secs = env::var("MW_MISSION_CRON_INTERVAL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(15);

        let mut args = env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--addr" => {
                    addr_raw = args
                        .next()
                        .ok_or_else(|| "--addr requires a value".to_string())?;
                }
                "--db" => {
                    db_path = args
                        .next()
                        .ok_or_else(|| "--db requires a value".to_string())?;
                }
                "--reap-interval-secs" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--reap-interval-secs requires a value".to_string())?;
                    reap_secs = raw
                        .parse::<u64>()
                        .map_err(|_| format!("invalid --reap-interval-secs: {raw}"))?;
                }
                "--cron-interval-secs" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--cron-interval-secs requires a value".to_string())?;
                    cron_secs = raw
                        .parse::<u64>()
                        .map_err(|_| format!("invalid --cron-interval-secs: {raw}"))?;
                }
                other => {
                    return Err(format!("unknown argument: {other}"));
                }
            }
        }

        let addr: SocketAddr = addr_raw
            .parse()
            .map_err(|err| format!("invalid MW_MISSION_ADDR '{addr_raw}': {err}"))?;

        Ok(Self {
            addr,
            db_path,
            reap_interval: Duration::from_secs(reap_secs.max(1)),
            cron_interval: Duration::from_secs(cron_secs.max(1)),
        })
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = match ServerConfig::from_env_and_args() {
        Ok(parsed) => parsed,
        Err(message) => {
            error!("{message}");
            std::process::exit(2);
        }
    };

    let database = match Database::open(&config.db_path) {
        Ok(opened) => opened,
        Err(err) => {
            error!("failed to open SQLite database '{}': {err}", config.db_path);
            std::process::exit(1);
        }
    };

    let engine = MissionEngine::new(database);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let reaper_handle = spawn_zombie_reaper(
        engine.clone(),
        config.reap_interval,
        shutdown_rx.clone(),
    );
    let cron_handle = spawn_cron_dispatcher(
        engine.clone(),
        config.cron_interval,
        shutdown_rx,
    );

    let router = build_router(engine);
    let listener = match TcpListener::bind(config.addr).await {
        Ok(bound) => bound,
        Err(err) => {
            error!("failed to bind {}: {err}", config.addr);
            std::process::exit(1);
        }
    };

    info!(
        addr = %config.addr,
        db = %config.db_path,
        reap_interval_secs = config.reap_interval.as_secs(),
        cron_interval_secs = config.cron_interval.as_secs(),
        "mw-mission broker listening"
    );

    let serve_outcome = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    let _ = shutdown_tx.send(true);
    let _ = reaper_handle.await;
    let _ = cron_handle.await;

    if let Err(err) = serve_outcome {
        error!("HTTP server exited with error: {err}");
        std::process::exit(1);
    }

    info!("mw-mission broker shut down cleanly");
}

fn spawn_zombie_reaper(
    engine: MissionEngine,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let engine_clone = engine.clone();
                    match tokio::task::spawn_blocking(move || engine_clone.reap_expired_leases()).await {
                        Ok(Ok(report)) if report.reaped_count > 0 => {
                            warn!(
                                reaped = report.reaped_count,
                                requeued = report.requeued_job_ids.len(),
                                dlq = report.dlq_job_ids.len(),
                                "reaped expired worker leases"
                            );
                        }
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => error!("zombie reaper failed: {err}"),
                        Err(join_err) => error!("zombie reaper task panicked: {join_err}"),
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

fn spawn_cron_dispatcher(
    engine: MissionEngine,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let engine_clone = engine.clone();
                    match tokio::task::spawn_blocking(move || engine_clone.dispatch_due_crons()).await {
                        Ok(Ok(report)) if !report.dispatched_job_ids.is_empty() => {
                            info!(
                                dispatched = report.dispatched_job_ids.len(),
                                "dispatched due cron jobs"
                            );
                        }
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => error!("cron dispatcher failed: {err}"),
                        Err(join_err) => error!("cron dispatcher task panicked: {join_err}"),
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!("failed to install Ctrl+C handler: {err}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => error!("failed to install SIGTERM handler: {err}"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

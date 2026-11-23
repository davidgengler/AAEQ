//! Headless mode: runs AAEQ's EQ switching with no window.
//!
//! In the desktop app the window's redraw loop does two jobs besides drawing:
//! it asks the worker to check the device once a second, and it reconnects
//! when the device goes away. Here a timer loop does those jobs instead, so
//! nothing has to be drawn. Everything that decides and applies the EQ (track
//! reading, genre lookup, preset resolution) is the same worker the desktop
//! app uses.

use super::{AaeqApp, AppCommand, AppResponse};
use aaeq_core::RulesIndex;
use aaeq_persistence::{AppSettingsRepository, MappingRepository};
use anyhow::{bail, Result};
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{Instant, MissedTickBehavior};

/// How often to check the device while connected (same as the desktop app)
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How long to wait between reconnect attempts (same as the desktop app)
const RECONNECT_INTERVAL: Duration = Duration::from_secs(5);

/// Connection state as the supervisor sees it
enum Link {
    Connected,
    /// A connect attempt has been sent and hasn't been answered yet
    Connecting,
    /// Not connected; try again at this time
    RetryAt(Instant),
}

/// Run AAEQ with no window until SIGTERM or Ctrl+C.
///
/// Connects to the device saved in the database and always retries if it's
/// unreachable, including at startup (the desktop app leaves a failed first
/// connect to the user, but nobody is watching here).
pub async fn run_headless(pool: SqlitePool) -> Result<()> {
    let settings = AppSettingsRepository::new(pool.clone());

    let Some(host) = settings.get_last_connected_host().await? else {
        bail!("No device saved in the database. Connect to the WiiM once from the desktop app, then copy the database here.");
    };

    // Rules for the active profile, loaded the same way as the desktop app
    let profile_id = settings.get_active_profile_id().await.ok().flatten().unwrap_or(1);
    let mappings = MappingRepository::new(pool.clone()).list_by_profile(profile_id).await?;
    let rules = RulesIndex::from_mappings(mappings);
    tracing::info!(
        "Loaded {} song rules, {} album rules, {} genre rules for profile {}",
        rules.song_rules.len(), rules.album_rules.len(), rules.genre_rules.len(), profile_id
    );
    let rules_index = Arc::new(RwLock::new(rules));

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (response_tx, mut response_rx) = mpsc::unbounded_channel();
    let worker = tokio::spawn(AaeqApp::async_worker(pool, rules_index, command_rx, response_tx));

    tracing::info!("Running headless; connecting to {}", host);
    let _ = command_tx.send(AppCommand::ConnectDevice(host.clone()));
    let mut link = Link::Connecting;

    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = tick.tick() => match link {
                Link::Connected => {
                    let _ = command_tx.send(AppCommand::Poll);
                }
                Link::RetryAt(when) if Instant::now() >= when => {
                    tracing::info!("Reconnecting to {}", host);
                    let _ = command_tx.send(AppCommand::ConnectDevice(host.clone()));
                    link = Link::Connecting;
                }
                _ => {}
            },

            // Every response must be read, even ones we ignore: the channel is
            // unbounded, so unread track updates would pile up in memory.
            response = response_rx.recv() => {
                let Some(response) = response else {
                    bail!("AAEQ worker stopped unexpectedly");
                };
                match response {
                    AppResponse::Connected(..) => {
                        link = Link::Connected;
                        let _ = command_tx.send(AppCommand::RefreshPresets);
                    }
                    // A failed connect attempt (the worker reports it as an error dialog)
                    AppResponse::ErrorDialog(info) if matches!(link, Link::Connecting) => {
                        tracing::warn!("{}; retrying in {}s", info.message, RECONNECT_INTERVAL.as_secs());
                        link = Link::RetryAt(Instant::now() + RECONNECT_INTERVAL);
                    }
                    AppResponse::Disconnected(reason) => {
                        tracing::warn!("{}; retrying in {}s", reason, RECONNECT_INTERVAL.as_secs());
                        link = Link::RetryAt(Instant::now() + RECONNECT_INTERVAL);
                    }
                    AppResponse::ErrorDialog(info) => tracing::warn!("{}", info.message),
                    AppResponse::Error(message) => tracing::warn!("{}", message),
                    // Track changes and applied presets are already logged by the worker
                    _ => {}
                }
            }

            _ = &mut shutdown => {
                tracing::info!("Shutting down");
                break;
            }
        }
    }

    worker.abort();
    Ok(())
}

/// Resolves on SIGTERM (docker stop) or Ctrl+C
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

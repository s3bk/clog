use std::{path::{Path, PathBuf}, sync::Arc, time::Duration};

use anyhow::Error;
use axum::{extract::{Request, State, WebSocketUpgrade}, response::IntoResponse, routing::get, Router};
use clog_collector::{init_log, LogCollector, LogOptions};
use clog_core::RequestEntry;
use tokio::{spawn, time::sleep, signal};
use tower_http::services::ServeDir;
use clog_ws_server::handle_ws;

struct App {
    log: LogCollector
}

#[axum::debug_handler]
async fn ws_handler(
    state: State<Arc<App>>,
    ws: WebSocketUpgrade
) -> impl IntoResponse {
    ws.on_upgrade(move |ws| handle_ws(ws, state.log.clone()))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (collector, log_tx) = init_log(LogOptions {
        data_dir: Some(PathBuf::from("blocks")),
        read_old: true
    }).await?;
    let state = Arc::new(App { log: collector.clone() });
    /*
    let data = test_data();
    spawn(async move {
        let mut data = data.into_iter();
        for r in data.by_ref().take(2) {
            log_tx.send(r).await.unwrap();
        }
        for r in data.take(10) {
            sleep(Duration::from_millis(1000)).await;
            log_tx.send(r).await.unwrap();
        }
    });
     */

    let routes = Router::new()
        .route("/ws", get(ws_handler))
        .fallback(file_and_error_handler)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, routes)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    collector.flush().await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn file_and_error_handler(req: Request) -> impl IntoResponse {
    use tower::util::ServiceExt;
    ServeDir::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("../viewer")).oneshot(req).await.unwrap()
}

fn test_data() -> Vec<RequestEntry> {
    use std::io::BufRead;

    let file = std::fs::File::open("../../artisan/user.log").unwrap();
    let mut entries = vec![];

    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    while let Ok(n) = reader.read_line(&mut line) {
        if n == 0 {
            break;
        }
        if let Ok(entry) = serde_json::from_str::<RequestEntry>(&line) {
            entries.push(entry);
        }
        line.clear();
    }
    entries
}

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Error;
use axum::{extract::{ws::WebSocket, Request, State, WebSocketUpgrade}, response::{IntoResponse, Response}, routing::get, Router};
use clog_core::{collector::{init_log, LogCollector}, RequestEntry};
use tokio::{spawn, time::sleep};
use tower_http::services::ServeDir;
use ws_server::handle_ws;

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
    let (collector, log_tx) = init_log();
    let state = Arc::new(App { log: collector });

    let data = test_data();
    spawn(async move {
        for r in data {
            sleep(Duration::from_secs(1)).await;
            log_tx.send(r).await.unwrap();
        }
    });

    let routes = Router::new()
        .route("/ws", get(ws_handler))
        .fallback(file_and_error_handler)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, routes).await?;
    Ok(())
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

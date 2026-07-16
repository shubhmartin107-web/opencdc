use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use opencdc_mcp::state::AppState;

const CORS_HEADERS: &str = "\
Access-Control-Allow-Origin: *\r\n\
Access-Control-Allow-Methods: GET, OPTIONS\r\n\
Access-Control-Allow-Headers: Content-Type, Authorization\r\n";

fn json_error(code: &str, message: &str) -> String {
    serde_json::to_string(&json!({"error": code, "message": message})).unwrap_or_default()
}

pub async fn serve_health(port: u16, state: Arc<AppState>) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind health endpoint on {}: {}", addr, e);
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _peer_addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    handle_health_request(&mut stream, &state).await;
                });
            }
            Err(e) => {
                tracing::warn!("Health endpoint accept error: {}", e);
            }
        }
    }
}

async fn handle_health_request(stream: &mut tokio::net::TcpStream, state: &AppState) {
    let mut buf = [0u8; 2048];
    let n = match stream.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let request = String::from_utf8_lossy(&buf[..n]);

    // Handle CORS preflight
    if request.starts_with("OPTIONS ") {
        let response = format!(
            "HTTP/1.1 204 No Content\r\n\
             {CORS_HEADERS}\
             Content-Length: 0\r\n\
             Connection: close\r\n\
             \r\n"
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    let (status_code, status_text, body, content_type) = if request.starts_with("GET /health ") || request.starts_with("GET /health\r\n") {
        let health = state.health_status().await;
        let body = serde_json::to_string_pretty(&health).unwrap_or_default();
        if health.status == "healthy" {
            (200, "OK", body, "application/json")
        } else {
            (503, "Service Unavailable", body, "application/json")
        }
    } else if request.starts_with("GET /metrics ") || request.starts_with("GET /metrics\r\n") {
        let health = state.health_status().await;
        let metrics = format!(
            "# HELP opencdc_events_received Total events received\n\
             # TYPE opencdc_events_received counter\n\
             opencdc_events_received {}\n\
             # HELP opencdc_events_sent Total events sent to sinks\n\
             # TYPE opencdc_events_sent counter\n\
             opencdc_events_sent {}\n\
             # HELP opencdc_errors Total errors\n\
             # TYPE opencdc_errors counter\n\
             opencdc_errors {}\n\
             # HELP opencdc_connectors_running Currently running connectors\n\
             # TYPE opencdc_connectors_running gauge\n\
             opencdc_connectors_running {}\n\
             # HELP opencdc_uptime_seconds Server uptime in seconds\n\
             # TYPE opencdc_uptime_seconds gauge\n\
             opencdc_uptime_seconds {}\n",
            health.total_events_received,
            health.total_events_sent,
            health.total_errors,
            health.running_connectors,
            health.uptime_seconds,
        );
        (200, "OK", metrics, "text/plain; charset=utf-8")
    } else {
        let body = json_error("not_found", "the requested endpoint does not exist");
        (404, "Not Found", body, "application/json")
    };

    let response = format!(
        "HTTP/1.1 {status_code} {status_text}\r\n\
         {CORS_HEADERS}\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        body.len(),
        body,
    );

    if let Err(e) = stream.write_all(response.as_bytes()).await {
        tracing::warn!("health endpoint write error: {}", e);
    }
    if let Err(e) = stream.shutdown().await {
        tracing::warn!("health endpoint shutdown error: {}", e);
    }
}

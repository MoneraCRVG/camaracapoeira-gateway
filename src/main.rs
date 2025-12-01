use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use futures_util::StreamExt; // Needed for payload iteration and stream mapping
use reqwest::Client;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Configuration constants
const LISTEN_PORT: u16 = 3021;
const PRIMARY_PORT: u16 = 3020;
const BACKUP_PORT: u16 = 3022;
const SERVICE_NAME: &str = "camaracapoeira-org-br-fullstack";
const TIMEOUT_DURATION: Duration = Duration::from_secs(3); // Fast failover

// Shared state to track which port we should target
struct AppState {
    http_client: Client,
    // true = Primary (3020), false = Backup (3022)
    is_primary_active: AtomicBool,
}

// Helper to run system commands
fn run_system_command(action: &str) {
    log::info!("Executing system command: sudo systemctl {} {}", action, SERVICE_NAME);
    
    let status = Command::new("sudo")
        .arg("systemctl")
        .arg(action)
        .arg(SERVICE_NAME)
        .status();

    match status {
        Ok(s) => log::info!("Command finished with: {}", s),
        Err(e) => log::error!("Failed to execute command: {}", e),
    }
}

// Background task to check health every 1 minute
async fn health_monitor(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        log::info!("Background Monitor: Checking health of Primary Port {}", PRIMARY_PORT);

        // Try to connect to Primary Port
        let check_url = format!("http://localhost:{}/", PRIMARY_PORT);
        let resp = state.http_client.get(&check_url)
            .timeout(Duration::from_secs(2))
            .send()
            .await;

        match resp {
            Ok(response) if response.status().is_success() => {
                // Primary is ALIVE
                if !state.is_primary_active.load(Ordering::Relaxed) {
                    log::info!("Primary Port {} is back online. Stopping backup service...", PRIMARY_PORT);
                    
                    // 1. Stop the backup service
                    run_system_command("stop");
                    
                    // 2. Switch state back to Primary
                    state.is_primary_active.store(true, Ordering::Relaxed);
                } else {
                    log::debug!("Primary is healthy and already active.");
                }
            }
            _ => {
                // Primary is DOWN
                log::warn!("Primary Port {} check failed in background monitor.", PRIMARY_PORT);
                // If we aren't already in backup mode, we probably should be.
                if state.is_primary_active.load(Ordering::Relaxed) {
                     log::warn!("Switching to Backup mode via Monitor.");
                     run_system_command("start");
                     state.is_primary_active.store(false, Ordering::Relaxed);
                }
            }
        }
    }
}

async fn forward_request(
    req: HttpRequest,
    mut payload: web::Payload,
    state: web::Data<Arc<AppState>>,
) -> Result<HttpResponse, Error> {
    // 1. Determine target port based on current state
    let is_primary = state.is_primary_active.load(Ordering::Relaxed);
    let target_port = if is_primary { PRIMARY_PORT } else { BACKUP_PORT };
    
    // 2. Construct the URL
    let path = req.uri().path();
    let query = req.uri().query().map(|q| format!("?{}", q)).unwrap_or_default();
    let url = format!("http://localhost:{}{}{}", target_port, path, query);

    log::info!("Proxying request to: {}", url);

    // 3. Convert Method (Actix http 0.2 -> Reqwest http 1.0)
    let method_str = req.method().as_str();
    let reqwest_method = reqwest::Method::from_bytes(method_str.as_bytes())
        .unwrap_or(reqwest::Method::GET);

    // 4. Prepare the Request Builder
    let mut request_builder = state.http_client
        .request(reqwest_method.clone(), &url)
        .timeout(TIMEOUT_DURATION);

    // 5. Convert Headers (Actix -> Reqwest)
    for (key, value) in req.headers() {
        if key.as_str() != "host" {
            // We must convert keys and values manually due to version mismatch
            if let Ok(h_name) = reqwest::header::HeaderName::from_bytes(key.as_str().as_bytes()) {
                if let Ok(h_val) = reqwest::header::HeaderValue::from_bytes(value.as_bytes()) {
                    request_builder = request_builder.header(h_name, h_val);
                }
            }
        }
    }

    // 6. Buffer the Body
    // Actix Payload is !Send (thread-local), but Reqwest needs Send. 
    // We buffer it here. This also allows us to REUSE the body for the failover retry.
    let mut body_bytes = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        body_bytes.extend_from_slice(&chunk);
    }
    let body_bytes = body_bytes.freeze(); // Immutable Bytes

    // Attach body to request
    request_builder = request_builder.body(body_bytes.clone());

    // 7. Execute Request
    let res = request_builder.send().await;

    match res {
        Ok(upstream_response) => {
            // Success! Convert response back to Actix format
            
            // Convert Status Code (Reqwest -> Actix)
            let status_u16 = upstream_response.status().as_u16();
            let actix_status = actix_web::http::StatusCode::from_u16(status_u16)
                .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);

            let mut client_resp = HttpResponse::build(actix_status);

            // Convert Headers (Reqwest -> Actix)
            for (key, value) in upstream_response.headers() {
                if let Ok(h_name) = actix_web::http::header::HeaderName::from_bytes(key.as_str().as_bytes()) {
                    if let Ok(h_val) = actix_web::http::header::HeaderValue::from_bytes(value.as_bytes()) {
                        client_resp.insert_header((h_name, h_val));
                    }
                }
            }

            // Stream response body back
            // Map Reqwest chunks to Actix Results
            let stream = upstream_response.bytes_stream().map(|res| {
                res.map_err(|e| actix_web::error::ErrorBadGateway(e))
            });

            Ok(client_resp.streaming(stream))
        }
        Err(_) if is_primary => {
            // 8. FAILOVER LOGIC
            log::error!("Primary Port {} failed responding. Triggering FAILOVER.", PRIMARY_PORT);

            // A. Run start command
            run_system_command("start");

            // B. Update Global State to Backup
            state.is_primary_active.store(false, Ordering::Relaxed);

            // C. Redirect THIS request to Backup Port immediately
            let backup_url = format!("http://localhost:{}{}{}", BACKUP_PORT, path, query);
            log::info!("Retrying request on Backup: {}", backup_url);

            // Prepare Retry Request
            let mut retry_builder = state.http_client
                .request(reqwest_method, &backup_url);
            
            // Copy headers again for retry
            for (key, value) in req.headers() {
                if key.as_str() != "host" {
                    if let Ok(h_name) = reqwest::header::HeaderName::from_bytes(key.as_str().as_bytes()) {
                        if let Ok(h_val) = reqwest::header::HeaderValue::from_bytes(value.as_bytes()) {
                            retry_builder = retry_builder.header(h_name, h_val);
                        }
                    }
                }
            }

            // Attach the SAME body we buffered earlier
            retry_builder = retry_builder.body(body_bytes);

            let backup_res = retry_builder.send().await;

            match backup_res {
                Ok(upstream_response) => {
                     // Convert Status Code
                    let status_u16 = upstream_response.status().as_u16();
                    let actix_status = actix_web::http::StatusCode::from_u16(status_u16)
                        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);

                    let mut client_resp = HttpResponse::build(actix_status);

                    // Convert Headers
                    for (key, value) in upstream_response.headers() {
                        if let Ok(h_name) = actix_web::http::header::HeaderName::from_bytes(key.as_str().as_bytes()) {
                            if let Ok(h_val) = actix_web::http::header::HeaderValue::from_bytes(value.as_bytes()) {
                                client_resp.insert_header((h_name, h_val));
                            }
                        }
                    }
                    
                    let stream = upstream_response.bytes_stream().map(|res| {
                        res.map_err(|e| actix_web::error::ErrorBadGateway(e))
                    });

                    Ok(client_resp.streaming(stream))
                },
                Err(e) => {
                    log::error!("Backup failed: {}", e);
                    Ok(HttpResponse::BadGateway().body("Both Primary and Backup failed."))
                }
            }
        }
        Err(e) => {
            log::error!("Proxy error: {}", e);
            Ok(HttpResponse::BadGateway().body(format!("Gateway Error: {}", e)))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logger
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Create shared state
    let state = Arc::new(AppState {
        http_client: Client::builder().build().unwrap(),
        is_primary_active: AtomicBool::new(true), // Start assuming primary is up
    });

    // Spawn the background monitor
    let monitor_state = state.clone();
    tokio::spawn(async move {
        health_monitor(monitor_state).await;
    });

    log::info!("Gateway server starting at http://localhost:{}", LISTEN_PORT);
    log::info!("Primary: {}, Backup: {}", PRIMARY_PORT, BACKUP_PORT);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .default_service(web::to(forward_request))
    })
    .bind(("127.0.0.1", LISTEN_PORT))?
    .run()
    .await
}
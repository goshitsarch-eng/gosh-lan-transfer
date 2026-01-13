// SPDX-License-Identifier: MIT
// gosh-lan-transfer - HTTP client for sending file transfers
//
// The client explicitly resolves hostnames and attempts all IPs.
// This ensures reliable connections over LAN, Tailscale, and VPNs.

use crate::error::{EngineError, EngineResult};
use crate::events::EventHandler;
use crate::protocol::{
    EngineEvent, TransferApprovalStatus, TransferDecision, TransferFile, TransferProgress,
    TransferRequest, TransferResponse,
};
use crate::types::{NetworkInterface, ResolveResult};
use futures::StreamExt;
use reqwest::{Body, Client};
use std::{
    net::ToSocketAddrs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs::File,
    time::{sleep, Instant},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

/// Client for sending files to a peer
pub struct TransferClient {
    http_client: Client,
    event_handler: Arc<dyn EventHandler>,
}

impl TransferClient {
    /// Create a new transfer client with the given event handler
    pub fn new(event_handler: Arc<dyn EventHandler>) -> Self {
        let http_client = Client::builder()
            // No global timeout - large file transfers can take a long time
            // Use read_timeout to detect stalled connections instead
            .read_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            event_handler,
        }
    }

    /// Resolve a hostname or IP to all available addresses
    pub fn resolve_address(address: &str) -> ResolveResult {
        // First, check if it's already an IP address
        if let Ok(ip) = address.parse::<std::net::IpAddr>() {
            return ResolveResult {
                hostname: address.to_string(),
                ips: vec![ip.to_string()],
                success: true,
                error: None,
            };
        }

        // Attempt DNS resolution
        let addr_with_port = format!("{}:0", address);
        match addr_with_port.to_socket_addrs() {
            Ok(addrs) => {
                let ips: Vec<String> = addrs.map(|a| a.ip().to_string()).collect();

                if ips.is_empty() {
                    ResolveResult {
                        hostname: address.to_string(),
                        ips: Vec::new(),
                        success: false,
                        error: Some("No IP addresses found".to_string()),
                    }
                } else {
                    tracing::info!("Resolved {} to {:?}", address, ips);
                    ResolveResult {
                        hostname: address.to_string(),
                        ips,
                        success: true,
                        error: None,
                    }
                }
            }
            Err(e) => ResolveResult {
                hostname: address.to_string(),
                ips: Vec::new(),
                success: false,
                error: Some(format!("DNS resolution failed: {}", e)),
            },
        }
    }

    /// Resolve a hostname or IP, returning an error if resolution fails
    pub fn resolve_address_or_err(address: &str) -> EngineResult<Vec<String>> {
        let result = Self::resolve_address(address);
        if result.success {
            Ok(result.ips)
        } else {
            Err(EngineError::DnsResolution(
                result.error.unwrap_or_else(|| format!("Failed to resolve {}", address)),
            ))
        }
    }

    /// Check if a peer is reachable by hitting the /health endpoint
    pub async fn check_peer(&self, address: &str, port: u16) -> EngineResult<bool> {
        let url = format!("http://{}:{}/health", address, port);

        match self.http_client.get(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(true)
                } else {
                    Err(EngineError::Network(format!(
                        "Peer returned status {}",
                        response.status()
                    )))
                }
            }
            Err(e) => {
                if e.is_connect() {
                    Err(EngineError::ConnectionRefused(format!(
                        "Cannot connect to {}:{} - {}",
                        address, port, e
                    )))
                } else if e.is_timeout() {
                    Err(EngineError::Network(format!(
                        "Connection timed out to {}:{}",
                        address, port
                    )))
                } else {
                    Err(EngineError::Network(format!("Request failed: {}", e)))
                }
            }
        }
    }

    /// Get peer info
    pub async fn get_peer_info(&self, address: &str, port: u16) -> EngineResult<serde_json::Value> {
        let url = format!("http://{}:{}/info", address, port);

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| EngineError::Network(format!("Failed to get peer info: {}", e)))?;

        response
            .json()
            .await
            .map_err(|e| EngineError::Serialization(format!("Failed to parse peer info: {}", e)))
    }

    /// Initiate a transfer request to a peer
    pub async fn request_transfer(
        &self,
        address: &str,
        port: u16,
        transfer_id: &str,
        files: Vec<TransferFile>,
        sender_name: Option<String>,
    ) -> EngineResult<TransferResponse> {
        let total_size: u64 = files.iter().map(|f| f.size).sum();

        let request = TransferRequest {
            transfer_id: transfer_id.to_string(),
            sender_name,
            files,
            total_size,
        };

        let url = format!("http://{}:{}/transfer", address, port);

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    EngineError::ConnectionRefused(format!(
                        "Cannot connect to {}:{} - {}",
                        address, port, e
                    ))
                } else {
                    EngineError::Network(format!("Transfer request failed: {}", e))
                }
            })?;

        let transfer_response: TransferResponse = response
            .json()
            .await
            .map_err(|e| EngineError::Serialization(format!("Failed to parse response: {}", e)))?;

        Ok(transfer_response)
    }

    async fn wait_for_approval(
        &self,
        address: &str,
        port: u16,
        transfer_id: &str,
    ) -> EngineResult<TransferApprovalStatus> {
        let url = format!(
            "http://{}:{}/transfer/status?transfer_id={}",
            address, port, transfer_id
        );
        let timeout = Duration::from_secs(120);
        let poll_interval = Duration::from_millis(500);
        let started = Instant::now();

        loop {
            let response = self
                .http_client
                .get(&url)
                .send()
                .await
                .map_err(|e| EngineError::Network(format!("Failed to check transfer status: {}", e)))?;

            if !response.status().is_success() {
                return Err(EngineError::Network(format!(
                    "Status check failed: {}",
                    response.status()
                )));
            }

            let status: TransferApprovalStatus = response
                .json()
                .await
                .map_err(|e| EngineError::Serialization(format!("Failed to parse status: {}", e)))?;

            match status.status {
                TransferDecision::Pending => {
                    if started.elapsed() > timeout {
                        return Err(EngineError::TransferTimeout);
                    }
                    sleep(poll_interval).await;
                }
                TransferDecision::Accepted => return Ok(status),
                TransferDecision::Rejected => return Err(EngineError::TransferRejected),
                TransferDecision::NotFound => {
                    return Err(EngineError::TransferNotFound(transfer_id.to_string()))
                }
            }
        }
    }

    /// Send a file to a peer (after transfer is accepted)
    pub async fn send_file(
        &self,
        address: &str,
        port: u16,
        transfer_id: &str,
        token: &str,
        file_id: &str,
        file_path: &Path,
        total_transfer_size: u64,
        bytes_sent_so_far: Arc<AtomicU64>,
        transfer_start_time: Instant,
    ) -> EngineResult<()> {
        let url = format!(
            "http://{}:{}/chunk?transfer_id={}&file_id={}&token={}",
            address, port, transfer_id, file_id, token
        );

        // Open and read the file
        let file = File::open(file_path)
            .await
            .map_err(|e| EngineError::FileIo(format!("Failed to open file: {}", e)))?;

        let metadata = file
            .metadata()
            .await
            .map_err(|e| EngineError::FileIo(format!("Failed to get file metadata: {}", e)))?;

        let file_size = metadata.len();

        // Create progress-tracking stream
        let event_handler = self.event_handler.clone();
        let transfer_id_owned = transfer_id.to_string();
        let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();
        let last_update = Arc::new(AtomicU64::new(0));

        let stream = ReaderStream::new(file).inspect({
            let event_handler = event_handler.clone();
            let transfer_id = transfer_id_owned.clone();
            let file_name = file_name.clone();
            let bytes_sent = bytes_sent_so_far.clone();
            let last_update = last_update.clone();
            let start_time = transfer_start_time;

            move |chunk_result| {
                if let Ok(chunk) = chunk_result {
                    let new_total = bytes_sent.fetch_add(chunk.len() as u64, Ordering::SeqCst)
                        + chunk.len() as u64;
                    let last = last_update.load(Ordering::SeqCst);

                    // Throttle updates to every 32KB to avoid flooding
                    if new_total - last >= 32768 || new_total == total_transfer_size {
                        last_update.store(new_total, Ordering::SeqCst);

                        // Calculate speed based on elapsed time
                        let elapsed_secs = start_time.elapsed().as_secs_f64();
                        let speed_bps = if elapsed_secs > 0.0 {
                            (new_total as f64 / elapsed_secs) as u64
                        } else {
                            0
                        };

                        event_handler.on_event(EngineEvent::TransferProgress(TransferProgress {
                            transfer_id: transfer_id.clone(),
                            current_file: Some(file_name.clone()),
                            bytes_transferred: new_total,
                            total_bytes: total_transfer_size,
                            speed_bps,
                        }));
                    }
                }
            }
        });

        // Send the file
        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", file_size)
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .map_err(|e| EngineError::Network(format!("Failed to send file: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(EngineError::Network(format!(
                "Server returned error: {}",
                error_text
            )));
        }

        // Send final progress update for this file
        let final_bytes = bytes_sent_so_far.load(Ordering::SeqCst);
        let elapsed_secs = transfer_start_time.elapsed().as_secs_f64();
        let speed_bps = if elapsed_secs > 0.0 {
            (final_bytes as f64 / elapsed_secs) as u64
        } else {
            0
        };
        self.event_handler.on_event(EngineEvent::TransferProgress(TransferProgress {
            transfer_id: transfer_id_owned,
            current_file: Some(file_name),
            bytes_transferred: final_bytes,
            total_bytes: total_transfer_size,
            speed_bps,
        }));

        Ok(())
    }

    /// Send multiple files to a peer
    pub async fn send_files(
        &self,
        address: &str,
        port: u16,
        file_paths: Vec<PathBuf>,
        sender_name: Option<String>,
    ) -> EngineResult<()> {
        let transfer_id = Uuid::new_v4().to_string();

        // Build file list with metadata
        let mut files = Vec::new();
        for path in &file_paths {
            let metadata = tokio::fs::metadata(path)
                .await
                .map_err(|e| EngineError::FileIo(format!("Failed to get file info: {}", e)))?;

            let name = path
                .file_name()
                .ok_or_else(|| EngineError::FileIo("Invalid file path".to_string()))?
                .to_string_lossy()
                .to_string();

            let mime_type = mime_guess::from_path(path)
                .first()
                .map(|m| m.to_string());

            files.push(TransferFile {
                id: Uuid::new_v4().to_string(),
                name,
                size: metadata.len(),
                mime_type,
            });
        }

        // Request transfer
        let response = self
            .request_transfer(address, port, &transfer_id, files.clone(), sender_name)
            .await?;

        let token = if response.accepted {
            response
                .token
                .ok_or_else(|| EngineError::Network("No token received".to_string()))?
        } else {
            let status = self
                .wait_for_approval(address, port, &transfer_id)
                .await?;
            status
                .token
                .ok_or_else(|| EngineError::Network("No token received".to_string()))?
        };

        // Calculate total transfer size
        let total_transfer_size: u64 = files.iter().map(|f| f.size).sum();
        let bytes_sent_so_far = Arc::new(AtomicU64::new(0));
        let transfer_start_time = Instant::now();

        // Send each file
        for (file, path) in files.iter().zip(file_paths.iter()) {
            self.send_file(
                address,
                port,
                &transfer_id,
                &token,
                &file.id,
                path,
                total_transfer_size,
                bytes_sent_so_far.clone(),
                transfer_start_time,
            )
            .await?;

            tracing::info!("Sent file: {}", file.name);
        }

        // Emit completion event
        self.event_handler.on_event(EngineEvent::TransferComplete {
            transfer_id,
        });

        Ok(())
    }
}

/// Get all network interfaces with their IP addresses
pub fn get_network_interfaces() -> Vec<NetworkInterface> {
    let mut interfaces = Vec::new();

    if let Ok(addrs) = local_ip_address::list_afinet_netifas() {
        for (name, ip) in addrs {
            let is_loopback = ip.is_loopback();
            interfaces.push(NetworkInterface {
                name,
                ip: ip.to_string(),
                is_loopback,
            });
        }
    }

    interfaces
}

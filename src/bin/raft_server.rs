use std::{sync::Arc};
use tokio::sync::mpsc;
use tonic::transport::Server;

use rust_raft::raft::core::RaftConfig;
use rust_raft::raft::proto::{LogEntry, raft_server::RaftServer};
use rust_raft::raft::{Peer, RaftService};
use tracing::info;
use tracing_appender::rolling;

#[tokio::main]
async fn main() {
    let filename = format!("log_{}.log", chrono::Local::now().format("%Y%m%d_%H%M%S"));
    let file_appender = rolling::never("logs", &filename);
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(file_writer).with_ansi(false).init();

    info!("Starting Raft server...");
    start_cluster().await;
}

async fn start_one(config: Arc<RaftConfig>) -> Result<(), Box<dyn std::error::Error>> {
    // Create channel for state machine application
    let (apply_tx, mut apply_rx) = mpsc::unbounded_channel::<LogEntry>();
    // Spawn state machine applier task
    let node_id = config.node_id;
    tokio::spawn(async move {
        while let Some(entry) = apply_rx.recv().await {
            info!(
                "[Node {}] [StateMachine] Applying log entry: term={}, command={:?}",
                node_id, entry.term, entry.command
            );
            // TODO: Apply to actual state machine
        }
    });

    // Create Raft service
    let raft_service = RaftService::new(
        &config.peers,
        apply_tx,
        &config,
    );
    // Start gRPC server
    info!("[Node {}] gRPC server listening...", config.node_id);
    Server::builder()
        .add_service(RaftServer::new(raft_service))
        .serve(config.bind_addr)
        .await?;
    Ok(())
}

async fn start_cluster() {
    let peers = (0..3)
        .map(|id| Arc::new(Peer::new(id, format!("http://127.0.0.1:{}", 50000 + id))))
        .collect::<Vec<_>>();
    let peers = Arc::new(peers);
    let mut handles = vec![];
    for peer in peers.iter() {
        let peers_clone = peers.clone();
        let config = RaftConfig {
            node_id: peer.id,
            bind_addr: peer.addr[7..].parse().unwrap(),
            peers: peers_clone,
            auto_election: true,
            election_timeout_ms: (150, 850),
            heartbeat_interval_ms: 100,
        };
        let handle = tokio::spawn(async move {
            start_one(Arc::new(config)).await.unwrap();
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

use std::{
    sync::{Arc},
};

use tokio::sync::{mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{test_util::check::LogChecker, raft::{ApplyMsg, Peer, RaftService, core::RaftConfig, storage::FileStorage}};


#[derive(Clone)]
pub struct RaftServer {
    pub id: u32,
    pub service: Arc<RaftService>,
    pub checker: Arc<LogChecker>,
    apply_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ApplyMsg>>>,
    applier_cancel_token: CancellationToken,
}

impl RaftServer {
    /// Create a Raft server node.
    ///
    /// RaftService owns the Raft protocol logic; RaftServer owns state machine application.
    pub fn new(peers: &Arc<Vec<Arc<Peer>>>, config: &Arc<RaftConfig>, checker: Arc<LogChecker>, storage_path: &str) -> Self {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel::<ApplyMsg>();

        // 创建持久化存储
        let storage = Box::new(FileStorage::new(storage_path).expect("Failed to create storage"));

        let service = Arc::new(RaftService::new(peers, apply_tx, config, storage));

        let applier_cancel_token = CancellationToken::new();

        let server = Self {
            id: config.node_id,
            service,
            checker,
            apply_rx: Arc::new(tokio::sync::Mutex::new(apply_rx)),
            applier_cancel_token,
            // state_machine,
            // pending_requests,
        };

        server.applier();
        server
    }

    fn applier(&self) {
        let id = self.id;
        let apply_rx = self.apply_rx.clone();
        let checker = self.checker.clone();
        let cancel_token = self.applier_cancel_token.clone();

        tokio::spawn(async move {
            let mut rx = apply_rx.lock().await;
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("[Node {}] Applier task cancelled", id);
                        break;
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if let Err(s) = checker.append_log(id as usize, msg).await {
                                    error!("{s}");
                                    panic!("{s}")
                                }
                            }
                            None => {
                                info!("[Node {}] Apply channel closed", id);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn shutdown(&self) {
        info!("[Node {}] Shutting down server", self.id);
        self.applier_cancel_token.cancel();
        self.service.shutdown().await;
    }
}
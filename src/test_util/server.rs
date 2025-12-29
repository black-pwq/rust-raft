use std::{
    sync::{Arc},
};

use tokio::sync::{mpsc};
use tracing::{error};

use crate::{test_util::check::LogChecker, raft::{ApplyMsg, Peer, RaftService, core::RaftConfig, storage::FileStorage}};


/// A full Raft server node: RaftService + in-memory state machine + committed log tracking.
///
/// Notes:
/// - No persistence and no snapshots.
/// - State machine application happens when entries become committed and are applied.
#[derive(Clone)]
pub struct RaftServer {
    pub id: u32,
    pub service: Arc<RaftService>,
    pub checker: Arc<LogChecker>,
    apply_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ApplyMsg>>>,
    // pub state_machine: Arc<Mutex<HashMap<String, String>>>,
    // pending_requests: Arc<Mutex<HashMap<u32, oneshot::Sender<Result<(), String>>>>>,
}

impl RaftServer {
    /// Create a Raft server node.
    ///
    /// RaftService owns the Raft protocol logic; RaftServer owns state machine application.
    pub fn new(peers: &Arc<Vec<Arc<Peer>>>, config: &Arc<RaftConfig>, checker: Arc<LogChecker>) -> Self {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel::<ApplyMsg>();
        // let state_machine = Arc::new(Mutex::new(HashMap::<String, String>::new()));
        // let pending_requests = Arc::new(Mutex::new(HashMap::<u32, oneshot::Sender<Result<(), String>>>::new()));

        // 为每个节点创建单独的持久化文件
        let storage_path = format!("logs/raft_node_{}.json", config.node_id);
        let storage = Box::new(FileStorage::new(&storage_path).expect("Failed to create storage"));

        let service = Arc::new(RaftService::new(peers, apply_tx, config, storage));

        let server = Self {
            id: config.node_id,
            service,
            checker,
            apply_rx: Arc::new(tokio::sync::Mutex::new(apply_rx)),
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

        tokio::spawn(async move {
            let mut rx = apply_rx.lock().await;
            while let Some(msg) = rx.recv().await {
                if let Err(s) =  checker.append_log(id as usize, msg).await {
                    error!("{s}");
                    panic!("{s}")
                }
            }
        });
    }

    // fn register_pending_request(&self, log_index: u32) -> oneshot::Receiver<Result<(), String>> {
    //     let (tx, rx) = oneshot::channel();
    //     let mut pending = self.pending_requests.lock().unwrap();
    //     pending.insert(log_index, tx);
    //     rx
    // }

    // Handle a client request by proposing it and waiting until it is applied.
    //
    // Returns `Err((msg, leader_id))` when this node is not the leader.
    // 
    // pub async fn handle_client_request(
    //     &self,
    //     command: Command,
    //     timeout: Duration,
    // ) -> Result<(), (String, Option<u32>)> {
    //     let log_index = match self.service.propose_command(command) {
    //         Ok(index) => index,
    //         Err(leader_id) => {
    //             return Err(("Not the leader".to_string(), leader_id));
    //         }
    //     };

    //     let rx = self.register_pending_request(log_index);

    //     // Speed up replication.
    //     self.service.broadcast_heartbeat().await;

    //     match tokio::time::timeout(timeout, rx).await {
    //         Ok(Ok(Ok(()))) => Ok(()),
    //         Ok(Ok(Err(err))) => Err((err, None)),
    //         Ok(Err(_)) => Err(("Internal error: notification channel closed".to_string(), None)),
    //         Err(_) => {
    //             // Timeout: cleanup pending request.
    //             self.pending_requests.lock().unwrap().remove(&log_index);
    //             Err(("Request timeout".to_string(), None))
    //         }
    //     }
    // }

    // Read a key from the local state machine (best-effort, local view).
    
    // pub fn get(&self, key: &str) -> Option<String> {
    //     self.state_machine.lock().unwrap().get(key).cloned()
    // }
}

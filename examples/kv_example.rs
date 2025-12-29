/// 使用 KvServer 的完整示例
///
/// 这个示例展示如何：
/// 1. 启动一个 3 节点的 Raft 集群
/// 2. 执行 Set/Get 操作
/// 3. 优雅关闭
///
/// 日志存放在 logs/latest-kv_example.log 中
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use rust_raft::raft::{
    ApplyMsg, Command, Peer, RaftService,
    core::RaftConfig,
    proto::{
        AppendEntriesArgsPb, AppendEntriesReplyPb, RequestVoteArgsPb, RequestVoteReplyPb,
        raft_server::Raft,
    },
    storage::FileStorage,
};

use tokio::{
    net::TcpListener,
    time::{Duration, sleep},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

use rust_raft::raft::proto::raft_server::RaftServer as GrpcRaftServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting KV Server example with 3 nodes");

    // 绑定监听端口（先绑定以便并行测试）
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for i in 0..3 {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", 9000 + i)).await?;
        let addr = listener.local_addr()?;
        listeners.push(listener);
        addrs.push(addr);
    }

    let peers: Vec<Arc<Peer>> = addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| Arc::new(Peer::new(i as u32, format!("http://{}", addr))))
        .collect();
    let peers = Arc::new(peers);

    // 创建服务器并启动 gRPC 服务
    let mut servers = Vec::new();
    let mut server_handles = Vec::new();

    for (i, (addr, listener)) in addrs.iter().zip(listeners.into_iter()).enumerate() {
        let config = Arc::new(RaftConfig {
            node_id: i as u32,
            bind_addr: *addr,
            peers: peers.clone(),
            auto_election: true,
            election_timeout_ms: (150, 300),
            heartbeat_interval_ms: 50,
        });

        let storage_path = format!("logs/kv_server_{}.json", i);
        // 清理旧的持久化文件
        let _ = std::fs::remove_file(&storage_path);

        let server = KvServer::new(&peers, &config, &storage_path);
        servers.push(server.clone());

        // 启动 gRPC 服务器
        let incoming = TcpListenerStream::new(listener);
        let grpc_server = Server::builder()
            .add_service(GrpcRaftServer::new(server))
            .serve_with_incoming(incoming);

        let handle = tokio::spawn(async move {
            if let Err(e) = grpc_server.await {
                eprintln!("gRPC server error: {}", e);
            }
        });
        server_handles.push(handle);

        info!("Started server {} on {}", i, addr);
        sleep(Duration::from_millis(50)).await;
    }

    info!("All servers started. Waiting for leader election...");
    sleep(Duration::from_secs(2)).await;

    // 找到 leader
    let leader_id = servers
        .iter()
        .position(|s| s.get_state().1)
        .expect("No leader found");
    info!("Leader is server {}", leader_id);

    // 执行一些操作
    info!("=== Executing Set operations ===");
    match servers[leader_id]
        .set("key1".to_string(), "value1".to_string())
        .await
    {
        Ok(result) => info!("Set key1: {}", result),
        Err(e) => info!("Set key1 failed: {}", e),
    }

    match servers[leader_id]
        .set("key2".to_string(), "value2".to_string())
        .await
    {
        Ok(result) => info!("Set key2: {}", result),
        Err(e) => info!("Set key2 failed: {}", e),
    }

    sleep(Duration::from_millis(500)).await;

    info!("=== Executing Get operations ===");
    match servers[leader_id].get("key1".to_string()).await {
        Ok(value) => info!("Get key1: {}", value),
        Err(e) => info!("Get key1 failed: {}", e),
    }

    match servers[leader_id].get("key2".to_string()).await {
        Ok(value) => info!("Get key2: {}", value),
        Err(e) => info!("Get key2 failed: {}", e),
    }

    match servers[leader_id].get("key3".to_string()).await {
        Ok(value) => info!("Get key3: {}", value),
        Err(e) => info!("Get key3 failed: {}", e),
    }

    // 验证所有服务器的状态一致
    sleep(Duration::from_millis(500)).await;
    info!("=== Verifying state machine consistency ===");
    assert!(
        servers
            .iter()
            .all(|s| s.get_local("key1") == Some("value1".to_string()))
    );
    assert!(
        servers
            .iter()
            .all(|s| s.get_local("key2") == Some("value2".to_string()))
    );
	info!("All Good!");

    // 优雅关闭
    info!("=== Shutting down servers ===");
    for server in servers {
        server.shutdown().await;
    }

    // 终止 gRPC 服务器
    for handle in server_handles {
        handle.abort();
    }

    sleep(Duration::from_millis(100)).await;

    info!("Example completed successfully!");
    Ok(())
}

/// 用于演示的 KV Raft 服务器：Raft 协议 + KV 状态机 + 客户端请求处理
#[derive(Clone)]
pub struct KvServer {
    pub id: u32,
    pub service: Arc<RaftService>,
    /// KV 状态机
    state_machine: Arc<Mutex<HashMap<String, String>>>,
    /// 等待日志应用的客户端请求
    pending_requests: Arc<Mutex<HashMap<u32, oneshot::Sender<Result<String, String>>>>>,
    apply_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ApplyMsg>>>,
    applier_cancel_token: CancellationToken,
}

impl KvServer {
    /// 创建一个新的 KV 服务器
    pub fn new(peers: &Arc<Vec<Arc<Peer>>>, config: &Arc<RaftConfig>, storage_path: &str) -> Self {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel::<ApplyMsg>();
        let state_machine = Arc::new(Mutex::new(HashMap::<String, String>::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::<
            u32,
            oneshot::Sender<Result<String, String>>,
        >::new()));

        // 创建持久化存储
        let storage = Box::new(FileStorage::new(storage_path).expect("Failed to create storage"));

        let service = Arc::new(RaftService::new(peers, apply_tx, config, storage));

        let applier_cancel_token = CancellationToken::new();

        let server = Self {
            id: config.node_id,
            service,
            state_machine,
            pending_requests,
            apply_rx: Arc::new(tokio::sync::Mutex::new(apply_rx)),
            applier_cancel_token: applier_cancel_token.clone(),
        };

        // 启动状态机应用任务
        server.start_applier();
        server
    }

    /// 启动状态机应用任务
    fn start_applier(&self) {
        let id = self.id;
        let apply_rx = self.apply_rx.clone();
        let state_machine = self.state_machine.clone();
        let pending_requests = self.pending_requests.clone();
        let cancel_token = self.applier_cancel_token.clone();

        tokio::spawn(async move {
            let mut rx = apply_rx.lock().await;
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("[KvServer {}] Applier task cancelled", id);
                        break;
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(ApplyMsg::Command { index, command, .. }) => {
                                info!("[KvServer {}] Applying command at index {}: {:?}", id, index, command);

                                // 应用到状态机
                                let result = {
                                    let mut sm = state_machine.lock().unwrap();
                                    match command {
                                        Command::Get { key } => {
                                            let value = sm.get(&key).cloned();
                                            Ok(value.unwrap_or_else(|| format!("Key '{}' not found", key)))
                                        }
                                        Command::Set { key, value } => {
                                            sm.insert(key.clone(), value.clone());
                                            Ok(format!("OK"))
                                        }
                                    }
                                };

                                // 通知等待的客户端请求
                                if let Some(sender) = pending_requests.lock().unwrap().remove(&index) {
                                    let _ = sender.send(result);
                                }
                            }
                            Some(ApplyMsg::Snapshot { .. }) => {
                                // 快照支持（暂未实现）
                                info!("[KvServer {}] Snapshot message received (not implemented)", id);
                            }
                            None => {
                                info!("[KvServer {}] Apply channel closed", id);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    /// 注册一个等待日志应用的请求
    fn register_pending_request(
        &self,
        log_index: u32,
    ) -> oneshot::Receiver<Result<String, String>> {
        let (tx, rx) = oneshot::channel();
        let mut pending = self.pending_requests.lock().unwrap();
        pending.insert(log_index, tx);
        rx
    }

    /// 获取键的值（只读操作）
    ///
    /// 注意：这是一个本地读取，可能读到旧数据（非线性一致性）
    /// 如果需要线性一致性读取，应该通过 Raft 共识
    pub fn get_local(&self, key: &str) -> Option<String> {
        self.state_machine.lock().unwrap().get(key).cloned()
    }

    /// 线性一致性 Get（通过 Raft 共识）
    pub async fn get(&self, key: String) -> Result<String, String> {
        let command = Command::Get { key };
        self.execute_command(command).await
    }

    /// Set 操作（通过 Raft 共识）
    pub async fn set(&self, key: String, value: String) -> Result<String, String> {
        let command = Command::Set { key, value };
        self.execute_command(command).await
    }

    /// 执行一个命令（通过 Raft 共识）
    async fn execute_command(&self, command: Command) -> Result<String, String> {
        // 提议命令到 Raft
        let (log_index, _term) = match self.service.propose_command(command.clone()) {
            Ok((index, term)) => (index, term),
            Err(leader_id) => {
                return Err(format!(
                    "Not the leader. Redirect to leader: {:?}",
                    leader_id
                ));
            }
        };

        info!(
            "[KvServer {}] Proposed command {:?} at index {}",
            self.id, command, log_index
        );

        // 注册等待应用的请求
        let rx = self.register_pending_request(log_index);

        // 等待命令被应用（超时 5 秒）
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("Internal error: notification channel closed".to_string()),
            Err(_) => {
                // 超时：清理等待请求
                self.pending_requests.lock().unwrap().remove(&log_index);
                Err("Request timeout".to_string())
            }
        }
    }

    /// 获取当前节点状态
    pub fn get_state(&self) -> (u32, bool) {
        self.service.get_state()
    }

    /// 优雅关闭服务器
    pub async fn shutdown(&self) {
        info!("[KvServer {}] Shutting down", self.id);

        // 取消 applier 任务
        self.applier_cancel_token.cancel();

        // 关闭 Raft 服务
        self.service.shutdown().await;

        // 通知所有等待的请求
        let mut pending = self.pending_requests.lock().unwrap();
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err("Server shutting down".to_string()));
        }

        info!("[KvServer {}] Shutdown complete", self.id);
    }
}

/// 实现 Raft trait 以响应 gRPC RPC
#[tonic::async_trait]
impl Raft for KvServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgsPb>,
    ) -> Result<Response<RequestVoteReplyPb>, Status> {
        let args_pb = request.into_inner();
        let args = args_pb
            .try_into()
            .map_err(|_| Status::invalid_argument("Invalid RequestVoteArgs"))?;
        let reply = self.service.handle_request_vote(args);
        Ok(Response::new(reply.into()))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgsPb>,
    ) -> Result<Response<AppendEntriesReplyPb>, Status> {
        let args_pb = request.into_inner();
        let args = args_pb
            .try_into()
            .map_err(|_| Status::invalid_argument("Invalid AppendEntriesArgs"))?;
        let reply = self.service.handle_append_entries(args);
        Ok(Response::new(reply.into()))
    }
}

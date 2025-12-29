pub mod check;
pub mod logger;
pub mod network;
pub mod server;

use network::{Network, NetworkFilteredService};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    time::{Duration, Instant, sleep},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{client::Grpc, transport::Server};
use tracing::{debug, error, info, warn};

use crate::{
    raft::{
        ApplyMsg, Command, Peer, RaftService, core::RaftConfig,
        proto::raft_server::RaftServer as GrpcRaftServer,
    },
    tpanic,
};

use crate::test_util::server::RaftServer;
/// Test harness for Raft cluster (integration tests)
pub struct TestHarness {
    /// Number of servers in the cluster
    pub n_servers: usize,
    /// Raft nodes for each server
    pub servers: Vec<Arc<RaftServer>>,
    pub checker: Arc<check::LogChecker>,
    /// Simulated network between raft servers.
    pub network: Arc<Mutex<Network>>,
    /// Server tasks (for cleanup)
    pub server_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Peer endpoints (http://ip:port) indexed by server id
    pub peer_endpoints: Vec<String>,
    /// Unique test ID to avoid file conflicts in parallel tests
    test_id: String,
}

impl TestHarness {
    /// Create a new test harness with `n_servers`.
    pub async fn new(n_servers: usize) -> Self {
        // 生成唯一的测试ID以避免并行测试的文件冲突
        let test_id = format!("{}_{}", std::process::id(), rand::random::<u64>());
        info!("Creating test harness with {} servers (test_id: {})", n_servers, test_id);

        // 在开始前清理可能存在的持久化文件
        for i in 0..n_servers {
            let storage_path = format!("logs/raft_test_{}_{}.json", test_id, i);
            if let Err(e) = std::fs::remove_file(&storage_path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to remove old storage file {}: {}", storage_path, e);
                }
            }
        }

        // Bind listeners on ephemeral ports first so tests can run in parallel.
        let mut listeners = Vec::with_capacity(n_servers);
        let mut bind_addrs = Vec::with_capacity(n_servers);
        for _ in 0..n_servers {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let bind_addr = listener.local_addr().unwrap();
            listeners.push(listener);
            bind_addrs.push(bind_addr);
        }

        let peer_endpoints: Vec<String> = bind_addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                // tonic requires full HTTP URI.
                info!("Server {} will listen on {}", i, addr);
                format!("http://{}", addr)
            })
            .collect();

        let all_peers: Vec<Arc<Peer>> = peer_endpoints
            .iter()
            .enumerate()
            .map(|(i, endpoint)| Arc::new(Peer::new(i as u32, endpoint.clone())))
            .collect();
        let all_peers = Arc::new(all_peers);

        // Initialize network - fully connected by default.
        let network = Arc::new(Mutex::new(Network::new(n_servers)));

        let checker = Arc::new(check::LogChecker::new(n_servers));
        let mut servers = Vec::new();
        let mut server_handles = Vec::new();

        for (i, (bind_addr, listener)) in bind_addrs
            .into_iter()
            .zip(listeners.into_iter())
            .enumerate()
        {
            let config = Arc::new(RaftConfig {
                node_id: i as u32,
                bind_addr,
                peers: all_peers.clone(),
                auto_election: true,
                election_timeout_ms: (150, 750),
                heartbeat_interval_ms: 100,
            });

            let storage_path = format!("logs/raft_test_{}_{}.json", test_id, i);
            let server = Arc::new(RaftServer::new(&all_peers, &config, checker.clone(), &storage_path));
            servers.push(server.clone());

            let filtered_service = NetworkFilteredService::new(server.clone(), i, network.clone());

            let incoming = TcpListenerStream::new(listener);
            let server = Server::builder()
                .add_service(GrpcRaftServer::new(filtered_service))
                .serve_with_incoming(incoming);

            let handle = tokio::spawn(async move {
                server.await.unwrap();
            });
            server_handles.push(handle);

            // Give server a moment to start accepting connections.
            sleep(Duration::from_millis(30)).await;
        }

        Self {
            n_servers,
            servers,
            checker,
            network,
            server_handles,
            peer_endpoints,
            test_id,
        }
    }

    /// Get endpoint for server id.
    pub fn endpoint(&self, server_id: usize) -> String {
        self.peer_endpoints[server_id].clone()
    }

    /// Check if server i is connected to the network
    pub fn is_connected(&self, i: usize) -> bool {
        self.network.lock().unwrap().is_node_connected(i)
    }

    pub fn disconnect_all(&self, i: usize) {
        info!("disconnecting server {} from network", i);
        self.network.lock().unwrap().disconnect_all(i);
    }

    pub fn connect_one(&self, i: usize) {
        info!("connecting server {} to network", i);
        self.network.lock().unwrap().connect_one(i);
    }

    /// Disable links both directions between two groups.
    pub fn partition(&self, group_a: &[usize], group_b: &[usize]) {
        info!("partitioning {:?} <-> {:?}", group_a, group_b);
        self.network.lock().unwrap().partition(group_a, group_b);
    }

    /// Configure latency (with optional jitter) for a directed link.
    pub fn set_delay(&self, from: usize, to: usize, base_delay: Duration, jitter: Duration) {
        self.network
            .lock()
            .unwrap()
            .set_delay(from, to, base_delay, jitter);
    }

    /// Configure latency (with optional jitter) for both directions.
    pub fn set_delay_bidir(&self, a: usize, b: usize, base_delay: Duration, jitter: Duration) {
        self.network
            .lock()
            .unwrap()
            .set_delay_bidir(a, b, base_delay, jitter);
    }

    /// Configure drop probability for a directed link.
    pub fn set_drop(&self, from: usize, to: usize, drop_prob: f64) {
        self.network.lock().unwrap().set_drop(from, to, drop_prob);
    }

    /// Configure drop probability for both directions.
    pub fn set_drop_bidir(&self, a: usize, b: usize, drop_prob: f64) {
        self.network.lock().unwrap().set_drop_bidir(a, b, drop_prob);
    }

    /// 检查是否所有在线服务器的任期一致。
    ///
    /// 返回 Some(term) 如果一致，否则返回 None（包含无服务器在线的情况）。
    pub fn check_terms(&self) -> Option<u32> {
        info!("checking terms...");
        let mut iter = self
            .servers
            .iter()
            .filter(|s| self.is_connected(s.id as usize))
            .map(|s| s.service.get_state().0);
        let term = iter.next()?;
        if iter.all(|x| x == term) {
            Some(term)
        } else {
            None
        }
    }

    /// 对于所有在线的服务器，检查是否存在且仅存在一个 leader。
    ///
    /// 返回 leader 的服务器 id 如果存在，否则返回错误信息。
    pub async fn check_one_leader(&self) -> Result<usize, String> {
        info!("checking for leader...");
        for _ in 0..10 {
            sleep(Duration::from_millis(500)).await;

            // term -> [server ids who think they are leader]
            let leaders = self
                .servers
                .iter()
                .enumerate()
                .filter(|(i, s)| self.is_connected(*i) && s.service.get_state().1)
                .fold(HashMap::<u32, Vec<usize>>::new(), |mut map, (i, s)| {
                    map.entry(s.service.get_state().0)
                        .or_default()
                        .push(i);
                    map
                });

            // 选出任期最高的 leader
            let mut highest_term_leader: Option<(u32, usize)> = None;
            for (&term, leaders_in_term) in leaders.iter() {
                if leaders_in_term.len() > 1 {
                    return Err(format!(
                        "Multiple leaders {:?} found in term {}",
                        leaders_in_term, term
                    ));
                }
                match &highest_term_leader {
                    Some((highest_term, _)) if term > *highest_term => {
                        highest_term_leader = Some((term, leaders_in_term[0]));
                    }
                    None => {
                        highest_term_leader = Some((term, leaders_in_term[0]));
                    }
                    _ => {}
                }
            }

            if let Some((term, leader_id)) = highest_term_leader {
                info!("found leader {} in term {}", leader_id, term);
                return Ok(leader_id);
            }
        }
        Err("expected one leader, but found none".to_string())
    }

    /// 对于所有在线的服务器，检查是否没有 leader。
    ///
    /// 返回错误信息，如果发现有服务器认为自己是 leader。
    pub fn check_no_leader(&self) -> Result<(), String> {
        info!("checking that no leader exists...");
        if let Some((i, _)) = self
            .servers
            .iter()
            .enumerate()
            .filter(|(i, _)| self.is_connected(*i))
            .find(|(_, s)| s.service.get_state().1)
        {
            return Err(format!(
                "Expected no leader, but server {} thinks it is leader",
                i
            ));
        }
        info!("Confirmed: no leader exists");
        Ok(())
    }

    /// 在给定的 index 上，检查有多少服务器提交了日志，以及它们的日志内容是否一致。
    ///
    /// 返回 (n_committed, Some(ApplyMsg)) 如果有服务器提交了日志，否则返回 (0, None)。
    ///
    /// 抛出错误如果发现日志不一致。
    pub async fn n_committed(&self, index: u32) -> Result<(usize, Option<ApplyMsg>), String> {
        let mut expected: Option<(usize, ApplyMsg)> = None;
        let mut n_committed = 0usize;

        for (server_id, log_mutex) in self.checker.logs.iter().enumerate() {
            let log = log_mutex.lock().await;
            let Some(msg) = log.get(&index) else {
                continue;
            };

            n_committed += 1;
            match &expected {
                Some((expected_server_id, expected_msg)) if msg != expected_msg => {
                    return Err(format!(
                        "LOG INCONSISTENCY at index {}: server {} has {:?} but server {} has {:?}",
                        index, server_id, msg, expected_server_id, expected_msg
                    ));
                }
                Some(_) => {}
                None => expected = Some((server_id, msg.clone())),
            }
        }
        Ok((n_committed, expected.map(|(_, msg)| msg)))
    }

    /// 做一次完整的共识过程。
    ///
    /// 在10s内不断尝试从在线服务器中找到一个 leader，并向它提交命令 `cmd`，然后等待命令被 `expected_servers` 个服务器提交。
    /// 如果 leader 出于某种原因无法提交命令（例如网络分区），则会重试，直到超时。
    ///
    /// 返回提交日志的 index 如果成功，否则返回错误信息。
    pub async fn one(
        &self,
        cmd: Command,
        expected_servers: usize,
        retry: bool,
    ) -> Result<u32, String> {
        let t0 = Instant::now();
        let mut index = None;
        while t0.elapsed() < Duration::from_secs(10) {
            // 找到某个 leader，并发送命令
            for server_id in 0..self.n_servers {
                let server_id = (server_id + 1) % self.n_servers;
                if self.is_connected(server_id) {
                    info!("Submitting command {:?} to server {}", cmd, server_id);
                    if let Ok((ndx, _)) =
                        self.servers[server_id].service.propose_command(cmd.clone())
                    {
                        index = Some(ndx);
                        break;
                    }
                }
            }
            if let Some(ndx) = index {
                // 等待共识达成
                let t1 = Instant::now();

                while t1.elapsed() < Duration::from_secs(2) {
                    // debug!("checking commitment of command {:?} at index {}", cmd, ndx);
                    if let Ok((n, cmd1)) = self.n_committed(ndx).await
                        && n >= expected_servers
                        && let Some(ApplyMsg::Command { command, .. }) = cmd1
                        && command == cmd
                    {
                        info!("agreement of {:?} reached", cmd);
                        return Ok(ndx);
                    }
                    sleep(Duration::from_millis(20)).await;
                }
                if !retry {
                    break;
                }
            } else {
                // 若没有找到 leader，则等待一会儿重试
                sleep(Duration::from_millis(50)).await;
            }
        }
        Err(format!("agreement of {:?} failed", cmd))
    }

    pub async fn check_no_agreement(&self, index: u32) -> Result<(), String> {
        let (n, _) = self.n_committed(index).await?;
        if n > 0 {
            Err(format!(
                "Expected no agreement at index {}, but found {} servers commit it",
                index, n
            ))
        } else {
            Ok(())
        }
    }

    pub async fn wait(
        &self,
        index: u32,
        expected_servers: usize,
        start_term: Option<u32>,
    ) -> Result<ApplyMsg, String> {
        let mut to = Duration::from_millis(10);
        for _ in 0..30 {
            if let Ok((n, msg)) = self.n_committed(index).await
                && n >= expected_servers
                && let Some(msg) = msg
            {
                return Ok(msg);
            }
            sleep(to).await;
            to = std::cmp::min(to * 2, Duration::from_secs(1));
            // 检查 term 没有变化
            if let Some(start_term) = start_term
                && let Some((id, term)) = self
                    .servers
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (i, s.service.get_state().0))
                    .find(|(_, term)| *term != start_term)
            {
                return Err(format!(
                    "Term changed from {} to {} on server {}",
                    start_term, term, id,
                ));
            }
        }

        Err(format!("Timeout waiting for agreement at index {}", index))
    }

    pub async fn cleanup(self) {
        for srv in self.servers {
            srv.shutdown().await;
        }
        for handle in self.server_handles {
            handle.abort();
        }
        sleep(Duration::from_millis(50)).await;

        // 清理持久化文件
        for i in 0..self.n_servers {
            let storage_path = format!("logs/raft_test_{}_{}.json", self.test_id, i);
            if let Err(e) = std::fs::remove_file(&storage_path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to remove storage file {}: {}", storage_path, e);
                }
            } else {
                info!("Removed storage file: {}", storage_path);
            }
        }
    }

    pub fn log_network(&self) {
        let mut connected = Vec::new();
        let mut disconnected = Vec::new();

        for i in 0..self.n_servers {
            if self.is_connected(i) {
                connected.push(i);
            } else {
                disconnected.push(i);
            }
        }

        info!(
            "Network status: connected: {:?}, disconnected: {:?}",
            connected, disconnected
        );
    }
}

/// 为了测试方便
impl Command {
    pub fn get<K>(key: K) -> Self
    where
        K: ToString,
    {
        let key = key.to_string();
        Command::Get { key }
    }
}

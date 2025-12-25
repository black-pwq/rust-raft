use rand::RngCore;
use rust_raft::raft::{
    client::Peer,
    core::RaftService,
    proto::{
        raft_server::Raft,
        raft_server::RaftServer,
        AppendEntriesArgs,
        AppendEntriesReply,
        ClientReply,
        ClientRequest,
        LogEntry,
        RequestVoteArgs,
        RequestVoteReply,
    },
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, Once},
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, time::sleep};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::{error, info, warn};
use tracing_appender::rolling;

use rust_raft::raft::core::RaftConfig;

/// Global logger initialization (once per test process)
static INIT_LOGGER: Once = Once::new();

/// Initialize the logger for tests - writes to logs directory
pub fn init_test_logger() {
    INIT_LOGGER.call_once(|| {
        let mut rng = rand::thread_rng();
        let suffix: u64 = rng.next_u64();
        let filename = format!(
            "test_{}_pid{}_{}.log",
            chrono::Local::now().format("%Y%m%d_%H%M%S_%3f"),
            std::process::id(),
            suffix
        );
        let file_appender = rolling::never("logs", &filename);
        let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_writer(file_writer)
            .with_ansi(false)
            .init();

        // Keep the guard alive for the entire test run
        std::mem::forget(guard);

        println!("Test logs will be written to: logs/{}", filename);
    });
}

/// Test harness for Raft cluster (integration tests)
pub struct TestHarness {
    /// Test name for logging
    pub test_name: String,
    /// Number of servers in the cluster
    pub n_servers: usize,
    /// Raft services for each server
    pub services: Vec<Arc<RaftService>>,
    /// Network connectivity state (true = connected, false = disconnected)
    pub network: Arc<Mutex<HashMap<(usize, usize), bool>>>,
    /// Server tasks (for cleanup)
    pub server_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Apply channel receivers for each server
    pub apply_rxs: Vec<mpsc::UnboundedReceiver<LogEntry>>,
    /// Peer endpoints (http://ip:port) indexed by server id
    pub peer_endpoints: Vec<String>,
}

impl TestHarness {
    /// Create a new test harness with `n_servers`.
    pub async fn new(n_servers: usize, start_servers: bool) -> Self {
        init_test_logger();

        info!("Creating test harness with {} servers", n_servers);

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

        // Initialize network - all connected by default.
        let mut network_map = HashMap::new();
        for i in 0..n_servers {
            for j in 0..n_servers {
                if i != j {
                    network_map.insert((i, j), true);
                }
            }
        }
        let network = Arc::new(Mutex::new(network_map));

        let mut services = Vec::new();
        let mut server_handles = Vec::new();
        let mut apply_rxs = Vec::new();

        for (i, (bind_addr, listener)) in bind_addrs
            .into_iter()
            .zip(listeners.into_iter())
            .enumerate()
        {
            let (apply_tx, apply_rx) = mpsc::unbounded_channel();
            apply_rxs.push(apply_rx);

            let config = Arc::new(RaftConfig {
                node_id: i as u32,
                bind_addr,
                peers: all_peers.clone(),
                auto_election: true,
                election_timeout_ms: (150, 750),
                heartbeat_interval_ms: 100,
            });

            let service = Arc::new(RaftService::new(&all_peers, apply_tx, &config));
            services.push(service.clone());

            if start_servers {
                let filtered_service =
                    NetworkFilteredService::new(service.clone(), i, network.clone());

                let incoming = TcpListenerStream::new(listener);
                let server = Server::builder()
                    .add_service(RaftServer::new(filtered_service))
                    .serve_with_incoming(incoming);

                let handle = tokio::spawn(async move {
                    server.await.unwrap();
                });
                server_handles.push(handle);

                // Give server a moment to start accepting connections.
                sleep(Duration::from_millis(30)).await;
            }
        }

        Self {
            test_name: String::new(),
            n_servers,
            services,
            network,
            server_handles,
            apply_rxs,
            peer_endpoints,
        }
    }

    pub fn begin(&mut self, test_name: &str, description: &str) {
        self.test_name = test_name.to_string();
        info!("╔══════════════════════════════════════════════════════════════");
        info!("║ TEST: {}", test_name);
        info!("║ {}", description);
        info!("║ Servers: {}", self.n_servers);
        info!("╚══════════════════════════════════════════════════════════════");
    }

    pub fn log(&self, message: &str) {
        if !self.test_name.is_empty() {
            info!("[{}] {}", self.test_name, message);
        } else {
            info!("{}", message);
        }
    }

    /// Get endpoint for server id.
    pub fn endpoint(&self, server_id: usize) -> String {
        self.peer_endpoints[server_id].clone()
    }

    /// Get state of server i (term, leader_id if it thinks it's leader)
    pub fn get_state(&self, i: usize) -> Option<(u32, Option<usize>)> {
        let service = &self.services[i];
        let core = service.get_core();
        let is_leader = matches!(core.state, rust_raft::raft::core::NodeState::Leader { .. });
        Some((core.current_term, if is_leader { Some(i) } else { None }))
    }

    /// Check if server i is connected to the network
    pub fn is_connected(&self, i: usize) -> bool {
        let network = self.network.lock().unwrap();
        for j in 0..self.n_servers {
            if i != j && *network.get(&(i, j)).unwrap_or(&false) {
                return true;
            }
        }
        false
    }

    pub fn disconnect_all(&self, i: usize) {
        self.log(&format!("disconnecting server {} from network", i));
        let mut network = self.network.lock().unwrap();
        for j in 0..self.n_servers {
            if i != j {
                network.insert((i, j), false);
                network.insert((j, i), false);
            }
        }
    }

    pub fn connect_one(&self, i: usize) {
        self.log(&format!("connecting server {} to network", i));
        let mut network = self.network.lock().unwrap();
        for j in 0..self.n_servers {
            if i != j {
                network.insert((i, j), true);
                network.insert((j, i), true);
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

        self.log(&format!(
            "Network: connected={:?}, disconnected={:?}",
            connected, disconnected
        ));
    }

    pub fn log_leader(&self, leader: usize) {
        if let Some((term, _)) = self.get_state(leader) {
            self.log(&format!("Leader: server {} (term {})", leader, term));
        }
    }

    pub fn log_pass(&self) {
        info!("✓ [{}] PASSED", self.test_name);
    }

    /// Check that all servers agree on the term
    pub fn check_terms(&self) -> u32 {
        self.log("checking terms...");
        let mut term = None;
        for i in 0..self.n_servers {
            if self.is_connected(i) {
                if let Some((t, _)) = self.get_state(i) {
                    if term.is_none() {
                        term = Some(t);
                    } else if term != Some(t) {
                        error!("[{}] Servers don't agree on term", self.test_name);
                        panic!("Servers don't agree on term");
                    }
                }
            }
        }
        let t = term.expect("No term found");
        self.log(&format!("all servers agree on term {}", t));
        t
    }

    /// Check that exactly one leader exists
    pub async fn check_one_leader(&self) -> usize {
        self.log("checking for leader...");
        for _ in 0..10 {
            sleep(Duration::from_millis(500)).await;

            let mut leaders: HashMap<u32, Vec<usize>> = HashMap::new();
            for i in 0..self.n_servers {
                if self.is_connected(i) {
                    if let Some((term, leader_id)) = self.get_state(i) {
                        if leader_id == Some(i) {
                            leaders.entry(term).or_default().push(i);
                        }
                    }
                }
            }

            let mut last_term_with_leader = None;
            for (&term, leaders_in_term) in leaders.iter() {
                if leaders_in_term.len() > 1 {
                    error!(
                        "[{}] Term {} has {} leaders: {:?}",
                        self.test_name,
                        term,
                        leaders_in_term.len(),
                        leaders_in_term
                    );
                    panic!(
                        "Term {} has {} leaders: {:?}",
                        term,
                        leaders_in_term.len(),
                        leaders_in_term
                    );
                }
                last_term_with_leader = Some((term, leaders_in_term[0]));
            }

            if let Some((term, leader)) = last_term_with_leader {
                self.log(&format!("leader {} elected in term {}", leader, term));
                return leader;
            }
        }

        error!("[{}] Expected one leader, got none", self.test_name);
        panic!("Expected one leader, got none");
    }

    /// Check that no leader exists
    pub fn check_no_leader(&self) {
        self.log("checking that no leader exists...");
        for i in 0..self.n_servers {
            if self.is_connected(i) {
                if let Some((_, Some(leader_id))) = self.get_state(i) {
                    if leader_id == i {
                        error!(
                            "[{}] Expected no leader, but server {} thinks it is leader",
                            self.test_name, i
                        );
                        panic!(
                            "Expected no leader, but server {} thinks it is leader",
                            i
                        );
                    }
                }
            }
        }
        info!("[{}] Confirmed: no leader exists", self.test_name);
    }

    pub async fn cleanup(self) {
        for handle in self.server_handles {
            handle.abort();
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Network-filtered service that respects the test harness network state
pub struct NetworkFilteredService {
    inner: Arc<RaftService>,
    server_id: usize,
    network: Arc<Mutex<HashMap<(usize, usize), bool>>>,
}

impl NetworkFilteredService {
    pub fn new(
        inner: Arc<RaftService>,
        server_id: usize,
        network: Arc<Mutex<HashMap<(usize, usize), bool>>>,
    ) -> Self {
        Self {
            inner,
            server_id,
            network,
        }
    }

    fn can_receive_from(&self, peer_id: u32) -> bool {
        let network = self.network.lock().unwrap();
        *network
            .get(&(peer_id as usize, self.server_id))
            .unwrap_or(&false)
    }
}

#[tonic::async_trait]
impl Raft for NetworkFilteredService {
    async fn request_vote(
        &self,
        request: tonic::Request<RequestVoteArgs>,
    ) -> Result<tonic::Response<RequestVoteReply>, tonic::Status> {
        let args = request.into_inner();
        let peer_id = args.candidate_id;

        if !self.can_receive_from(peer_id) {
            return Err(tonic::Status::unavailable("Network partition"));
        }

        let reply = self.inner.handle_request_vote(args);
        Ok(tonic::Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesArgs>,
    ) -> Result<tonic::Response<AppendEntriesReply>, tonic::Status> {
        let args = request.into_inner();
        let peer_id = args.leader_id;

        if !self.can_receive_from(peer_id) {
            return Err(tonic::Status::unavailable("Network partition"));
        }

        let reply = self.inner.handle_append_entries(args);
        Ok(tonic::Response::new(reply))
    }

    async fn client_command(
        &self,
        request: tonic::Request<ClientRequest>,
    ) -> Result<tonic::Response<ClientReply>, tonic::Status> {
        // Delegate to the real RaftService RPC implementation.
        <RaftService as Raft>::client_command(&*self.inner, request).await
    }
}

/// Raft Leader Election Tests (3A)
/// Based on MIT 6.5840 Raft implementation tests
use rand::Rng;
use rust_raft::raft::{
    client::Peer,
    core::{RaftConfig, RaftService},
    proto::raft_server::RaftServer,
};
use tracing_appender::rolling;
use tracing::{info, warn, error};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, Once},
    time::Duration,
};
use tokio::{sync::mpsc, time::sleep};
use tonic::transport::Server;

/// Global logger initialization
static INIT_LOGGER: Once = Once::new();

/// Initialize the logger for tests - writes to logs directory
fn init_test_logger() {
    INIT_LOGGER.call_once(|| {
        let filename = format!("test_{}.log", chrono::Local::now().format("%Y%m%d_%H%M%S"));
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

/// Election timeout as defined in MIT 6.5840 tests
const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

/// Test harness for Raft cluster
struct TestHarness {
    /// Test name for logging
    test_name: String,
    /// Number of servers in the cluster
    n_servers: usize,
    /// Raft services for each server
    services: Vec<Arc<RaftService>>,
    /// Network connectivity state (true = connected, false = disconnected)
    network: Arc<Mutex<HashMap<(usize, usize), bool>>>,
    /// Server tasks (for cleanup)
    server_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Apply channel receivers for each server
    apply_rxs: Vec<mpsc::UnboundedReceiver<rust_raft::raft::proto::LogEntry>>,
}

impl TestHarness {
    /// Create a new test harness with n servers
    async fn new(n_servers: usize, start_servers: bool) -> Self {
        // Initialize logger
        init_test_logger();
        
        info!("Creating test harness with {} servers", n_servers);
        
        let mut services = Vec::new();
        let mut server_handles = Vec::new();
        let mut apply_rxs = Vec::new();
        let mut all_peers = Vec::new();

        // First, create all peer addresses (must be full HTTP URIs for tonic)
        for i in 0..n_servers {
            let addr = format!("http://127.0.0.1:{}", 50051 + i);
            all_peers.push(Arc::new(Peer::new(i as u32, addr)));
        }
        let all_peers = Arc::new(all_peers);

        // Initialize network - all connected by default
        let mut network = HashMap::new();
        for i in 0..n_servers {
            for j in 0..n_servers {
                if i != j {
                    network.insert((i, j), true);
                }
            }
        }
        let network = Arc::new(Mutex::new(network));

        // Create services and servers
        for i in 0..n_servers {
            let (apply_tx, apply_rx) = mpsc::unbounded_channel();
            apply_rxs.push(apply_rx);

            let bind_addr: SocketAddr = format!("127.0.0.1:{}", 50051 + i).parse().unwrap();

            let config = Arc::new(RaftConfig {
                node_id: i as u32,
                bind_addr,
                peers: all_peers.clone(),
                auto_election: true,  // Enable auto election for tests
                election_timeout_ms: (150, 750),
                heartbeat_interval_ms: 100,
            });

            let service = Arc::new(RaftService::new(&all_peers, apply_tx, &config));
            services.push(service.clone());

            if start_servers {
                // Wrap service with network filter
                let filtered_service =
                    NetworkFilteredService::new(service.clone(), i, network.clone());
                let server = Server::builder()
                    .add_service(RaftServer::new(filtered_service))
                    .serve(bind_addr);

                let handle = tokio::spawn(async move {
                    server.await.unwrap();
                });
                server_handles.push(handle);

                // Give server time to start
                sleep(Duration::from_millis(50)).await;
            }
        }

        Self {
            test_name: String::new(),
            n_servers,
            services,
            network,
            server_handles,
            apply_rxs,
        }
    }

    /// Check that exactly one leader exists
    async fn check_one_leader(&self) -> usize {
        self.log("checking for leader...");
        for _ in 0..10 {
            sleep(Duration::from_millis(500)).await;

            let mut leaders = HashMap::new();
            for i in 0..self.n_servers {
                if self.is_connected(i) {
                    if let Some((term, leader_id)) = self.get_state(i) {
                        if leader_id == Some(i) {
                            leaders.entry(term).or_insert(Vec::new()).push(i);
                        }
                    }
                }
            }

            let mut last_term_with_leader = None;
            for (&term, leaders_in_term) in leaders.iter() {
                if leaders_in_term.len() > 1 {
                    error!(
                        "[{}] Term {} has {} leaders: {:?}",
                        self.test_name, term, leaders_in_term.len(), leaders_in_term
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
    fn check_no_leader(&self) {
        self.log("checking that no leader exists...");
        for i in 0..self.n_servers {
            if self.is_connected(i) {
                if let Some((_, Some(leader_id))) = self.get_state(i) {
                    if leader_id == i {
                        error!("[{}] Expected no leader, but server {} thinks it is leader", self.test_name, i);
                        panic!("Expected no leader, but server {} thinks it is leader", i);
                    }
                }
            }
        }
        info!("[{}] Confirmed: no leader exists", self.test_name);
    }

    /// Check that all servers agree on the term
    fn check_terms(&self) -> u32 {
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

    /// Get state of server i (term, leader_id if it thinks it's leader)
    fn get_state(&self, i: usize) -> Option<(u32, Option<usize>)> {
        let service = &self.services[i];
        // Access the core through the service
        // Note: This requires adding a getter method to RaftService
        let core = service.get_core();
        let is_leader = matches!(core.state, rust_raft::raft::core::NodeState::Leader { .. });
        Some((core.current_term, if is_leader { Some(i) } else { None }))
    }

    /// Check if server i is connected to the network
    fn is_connected(&self, i: usize) -> bool {
        let network = self.network.lock().unwrap();
        // A server is connected if it can communicate with at least one other server
        for j in 0..self.n_servers {
            if i != j && *network.get(&(i, j)).unwrap_or(&false) {
                return true;
            }
        }
        false
    }

    /// Disconnect server i from all other servers
    fn disconnect_all(&self, i: usize) {
        self.log(&format!("disconnecting server {} from network", i));
        let mut network = self.network.lock().unwrap();
        for j in 0..self.n_servers {
            if i != j {
                network.insert((i, j), false);
                network.insert((j, i), false);
            }
        }
    }

    /// Connect server i to all other servers
    fn connect_one(&self, i: usize) {
        self.log(&format!("connecting server {} to network", i));
        let mut network = self.network.lock().unwrap();
        for j in 0..self.n_servers {
            if i != j {
                network.insert((i, j), true);
                network.insert((j, i), true);
            }
        }
    }

    /// Begin a test
    fn begin(&mut self, test_name: &str, description: &str) {
        self.test_name = test_name.to_string();
        info!("╔══════════════════════════════════════════════════════════════");
        info!("║ TEST: {}", test_name);
        info!("║ {}", description);
        info!("║ Servers: {}", self.n_servers);
        info!("╚══════════════════════════════════════════════════════════════");
    }

    /// Log a message with test context
    fn log(&self, message: &str) {
        if !self.test_name.is_empty() {
            info!("[{}] {}", self.test_name, message);
        } else {
            info!("{}", message);
        }
    }

    /// Log network connectivity state
    fn log_network(&self) {
        let mut connected = Vec::new();
        let mut disconnected = Vec::new();
        
        for i in 0..self.n_servers {
            if self.is_connected(i) {
                connected.push(i);
            } else {
                disconnected.push(i);
            }
        }
        
        self.log(&format!("Network: connected={:?}, disconnected={:?}", connected, disconnected));
    }

    /// Log current leader information
    fn log_leader(&self, leader: usize) {
        if let Some((term, _)) = self.get_state(leader) {
            self.log(&format!("Leader: server {} (term {})", leader, term));
        }
    }

    /// Log test result
    fn log_pass(&self) {
        info!("✓ [{}] PASSED", self.test_name);
    }

    /// Clean up resources
    async fn cleanup(self) {
        for handle in self.server_handles {
            handle.abort();
        }
        // Give time for cleanup
        sleep(Duration::from_millis(100)).await;
    }
}

/// Network-filtered service that respects the test harness network state
struct NetworkFilteredService {
    inner: Arc<RaftService>,
    server_id: usize,
    network: Arc<Mutex<HashMap<(usize, usize), bool>>>,
}

impl NetworkFilteredService {
    fn new(
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

    fn is_connected_to(&self, _peer_id: u32) -> bool {
        // For now, we'll implement basic connectivity check
        // In a full implementation, this would check the network state
        true
    }
}

#[tonic::async_trait]
impl rust_raft::raft::proto::raft_server::Raft for NetworkFilteredService {
    async fn request_vote(
        &self,
        request: tonic::Request<rust_raft::raft::proto::RequestVoteArgs>,
    ) -> Result<tonic::Response<rust_raft::raft::proto::RequestVoteReply>, tonic::Status> {
        let args = request.into_inner();
        let peer_id = args.candidate_id;

        // Check if this server can receive from the peer
        let network = self.network.lock().unwrap();
        let connected = network
            .get(&(peer_id as usize, self.server_id))
            .unwrap_or(&false);

        if !connected {
            return Err(tonic::Status::unavailable("Network partition"));
        }

        let reply = self.inner.handle_request_vote(args);
        Ok(tonic::Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<rust_raft::raft::proto::AppendEntriesArgs>,
    ) -> Result<tonic::Response<rust_raft::raft::proto::AppendEntriesReply>, tonic::Status> {
        let args = request.into_inner();
        let peer_id = args.leader_id;

        // Check if this server can receive from the peer
        let network = self.network.lock().unwrap();
        let connected = network
            .get(&(peer_id as usize, self.server_id))
            .unwrap_or(&false);

        if !connected {
            return Err(tonic::Status::unavailable("Network partition"));
        }

        let reply = self.inner.handle_append_entries(args);
        Ok(tonic::Response::new(reply))
    }
}

// ============================================================================
// Test Cases
// ============================================================================

#[tokio::test]
async fn test_initial_election_3a() {
    let servers = 3;
    let mut ts = TestHarness::new(servers, true).await;

    ts.begin("TestInitialElection3A", "Test (3A): initial election");

    // Give time for election
    ts.log("waiting for initial election...");
    sleep(Duration::from_millis(1000)).await;

    // Is a leader elected?
    let leader1 = ts.check_one_leader().await;
    ts.log_leader(leader1);

    // Sleep a bit to avoid racing with followers learning of the election
    ts.log("checking term agreement...");
    sleep(Duration::from_millis(50)).await;
    let term1 = ts.check_terms();
    assert!(term1 >= 1, "Term is {}, but should be at least 1", term1);

    // Does the leader+term stay the same if there is no network failure?
    ts.log("waiting 2 election timeouts to check stability...");
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    let term2 = ts.check_terms();
    if term1 != term2 {
        warn!("Term changed from {} to {} even though there were no failures", term1, term2);
    }

    // There should still be a leader
    let leader2 = ts.check_one_leader().await;
    ts.log_leader(leader2);

    ts.log_pass();
    ts.cleanup().await;
}

#[tokio::test]
async fn test_re_election_3a() {
    let servers = 3;
    let mut ts = TestHarness::new(servers, true).await;

    ts.begin("TestReElection3A", "Test (3A): election after network failure");

    // Give time for initial election
    ts.log("waiting for initial election...");
    sleep(Duration::from_millis(1000)).await;

    let leader1 = ts.check_one_leader().await;
    ts.log_leader(leader1);

    // If the leader disconnects, a new one should be elected
    ts.log("testing leader failure and re-election...");
    ts.disconnect_all(leader1);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    let leader2 = ts.check_one_leader().await;
    ts.log_leader(leader2);

    // If the old leader rejoins, that shouldn't disturb the new leader
    ts.log("testing old leader rejoining...");
    ts.connect_one(leader1);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    let leader3 = ts.check_one_leader().await;
    ts.log_leader(leader3);

    // If there's no quorum, no new leader should be elected
    ts.log("testing loss of quorum...");
    ts.disconnect_all(leader3);
    ts.disconnect_all((leader3 + 1) % servers);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;

    // Check that the one connected server does not think it is the leader
    ts.check_no_leader();
    ts.log("confirmed: no leader without quorum");

    // If a quorum arises, it should elect a leader
    ts.log("restoring quorum...");
    ts.connect_one((leader3 + 1) % servers);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    ts.check_one_leader().await;

    // Re-join of last node shouldn't prevent leader from existing
    ts.log("re-joining last node...");
    ts.connect_one(leader3);
    ts.log_network();
    sleep(Duration::from_millis(500)).await;
    ts.check_one_leader().await;

    ts.log_pass();
    ts.cleanup().await;
}

#[tokio::test]
async fn test_many_elections_3a() {
    let servers = 7;
    let mut ts = TestHarness::new(servers, true).await;

    ts.begin("TestManyElections3A", "Test (3A): multiple elections");

    // Give time for initial election
    ts.log("waiting for initial election...");
    sleep(Duration::from_millis(1000)).await;
    ts.check_one_leader().await;

    let iters = 10;
    for ii in 1..iters {
        ts.log(&format!("iteration {}/{}", ii, iters - 1));

        // Disconnect three random nodes
        let mut rng = rand::thread_rng();
        let i1 = rng.gen_range(0..servers);
        let i2 = rng.gen_range(0..servers);
        let i3 = rng.gen_range(0..servers);

        ts.log(&format!("disconnecting servers [{}, {}, {}]", i1, i2, i3));
        ts.disconnect_all(i1);
        ts.disconnect_all(i2);
        ts.disconnect_all(i3);
        ts.log_network();

        // Either the current leader should still be alive,
        // or the remaining four should elect a new one
        sleep(2 * RAFT_ELECTION_TIMEOUT).await;
        ts.check_one_leader().await;

        ts.log(&format!("reconnecting servers [{}, {}, {}]", i1, i2, i3));
        ts.connect_one(i1);
        ts.connect_one(i2);
        ts.connect_one(i3);
        ts.log_network();

        sleep(Duration::from_millis(500)).await;
    }

    ts.log("final check...");
    ts.check_one_leader().await;

    ts.log_pass();
    ts.cleanup().await;
}

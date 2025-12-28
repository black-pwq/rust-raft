use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::Rng;
use tokio::time::sleep;

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
    proto::{
        AppendEntriesArgsPb, AppendEntriesReplyPb, ClientReply, ClientRequest, RequestVoteArgsPb,
        RequestVoteReplyPb, raft_server::Raft,
    },
};

use crate::test_util::server::RaftServer;

#[derive(Clone, Copy, Debug)]
pub struct LinkConfig {
    pub enabled: bool,
    pub base_delay: Duration,
    pub jitter: Duration,
    pub drop_prob: f64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_delay: Duration::from_millis(0),
            jitter: Duration::from_millis(0),
            drop_prob: 0.0,
        }
    }
}

impl LinkConfig {
    fn should_drop(&self, rng: &mut impl Rng) -> bool {
        let p = self.drop_prob.clamp(0.0, 1.0);
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        rng.gen_bool(p)
    }

    fn sample_delay(&self, rng: &mut impl Rng) -> Duration {
        let jitter_ns_u128 = self.jitter.as_nanos();
        let jitter_ns = std::cmp::min(jitter_ns_u128, u128::from(u64::MAX)) as u64;

        let extra_ns = if jitter_ns == 0 {
            0
        } else {
            rng.gen_range(0..=jitter_ns)
        };

        self.base_delay + Duration::from_nanos(extra_ns)
    }
}

/// A tiny directed network model for tests.
///
/// Stores an N×N matrix of per-link behavior, replacing the old
/// `HashMap<(from,to), bool>`.
pub struct Network {
    n: usize,
    links: Vec<Vec<LinkConfig>>,
    /// Tracks which nodes have been explicitly disconnected via disconnect_all.
    /// These nodes can only be reconnected by calling connect_one on themselves.
    disconnected: Vec<bool>,
}

impl Network {
    /// Creates a fully-connected network with default link configuration.
    /// Self-links (i -> i) are disabled.
    pub fn new(n: usize) -> Self {
        let mut links = vec![vec![LinkConfig::default(); n]; n];
        for i in 0..n {
            links[i][i].enabled = false;
        }
        Self {
            n,
            links,
            disconnected: vec![false; n],
        }
    }

    pub fn n(&self) -> usize {
        self.n
    }

    pub fn link(&self, from: usize, to: usize) -> LinkConfig {
        if from >= self.n || to >= self.n {
            return LinkConfig {
                enabled: false,
                ..Default::default()
            };
        }
        self.links[from][to]
    }

    pub fn is_node_connected(&self, node: usize) -> bool {
        if node >= self.n {
            return false;
        }
        // If the node is marked as disconnected, it's not connected
        if self.disconnected[node] {
            return false;
        }
        (0..self.n).any(|j| j != node && self.links[node][j].enabled)
    }

    pub fn disconnect_all(&mut self, node: usize) {
        if node >= self.n {
            return;
        }
        // Mark this node as explicitly disconnected
        self.disconnected[node] = true;
        for j in 0..self.n {
            if j == node {
                continue;
            }
            self.links[node][j].enabled = false;
            self.links[j][node].enabled = false;
        }
    }

    pub fn connect_one(&mut self, node: usize) {
        if node >= self.n {
            return;
        }
        // Clear the disconnected flag for this node
        self.disconnected[node] = false;
        for j in 0..self.n {
            if j == node {
                continue;
            }
            // Only reconnect if the other node is not disconnected
            if !self.disconnected[j] {
                self.links[node][j].enabled = true;
                self.links[j][node].enabled = true;
            }
        }
    }

    pub fn partition(&mut self, group_a: &[usize], group_b: &[usize]) {
        for &a in group_a {
            if a >= self.n {
                continue;
            }
            for &b in group_b {
                if b >= self.n || a == b {
                    continue;
                }
                self.links[a][b].enabled = false;
                self.links[b][a].enabled = false;
            }
        }
    }

    pub fn set_delay(&mut self, from: usize, to: usize, base_delay: Duration, jitter: Duration) {
        if from >= self.n || to >= self.n || from == to {
            return;
        }
        self.links[from][to].base_delay = base_delay;
        self.links[from][to].jitter = jitter;
    }

    pub fn set_delay_bidir(
        &mut self,
        a: usize,
        b: usize,
        base_delay: Duration,
        jitter: Duration,
    ) {
        self.set_delay(a, b, base_delay, jitter);
        self.set_delay(b, a, base_delay, jitter);
    }

    pub fn set_drop(&mut self, from: usize, to: usize, drop_prob: f64) {
        if from >= self.n || to >= self.n || from == to {
            return;
        }
        self.links[from][to].drop_prob = drop_prob;
    }

    pub fn set_drop_bidir(&mut self, a: usize, b: usize, drop_prob: f64) {
        self.set_drop(a, b, drop_prob);
        self.set_drop(b, a, drop_prob);
    }
}

/// Network-filtered service that respects the test harness network state
pub struct NetworkFilteredService {
    inner: Arc<RaftServer>,
    server_id: usize,
    network: Arc<Mutex<Network>>,
}

impl NetworkFilteredService {
    pub fn new(
        inner: Arc<RaftServer>,
        server_id: usize,
        network: Arc<Mutex<Network>>,
    ) -> Self {
        Self {
            inner,
            server_id,
            network,
        }
    }

    async fn apply_link_behavior(&self, peer_id: u32) -> Result<(), tonic::Status> {
        let from = peer_id as usize;
        let to: usize = self.server_id;

        // Copy config out while holding the lock; never hold across await.
        let cfg = {
            let net = self.network.lock().unwrap();
            net.link(from, to)
        };

        if !cfg.enabled {
            return Err(tonic::Status::unavailable("Network partition"));
        }

        let (should_drop, delay) = {
            let mut rng = rand::thread_rng();
            (cfg.should_drop(&mut rng), cfg.sample_delay(&mut rng))
        };

        if should_drop {
            return Err(tonic::Status::unavailable("Network drop"));
        }
        if delay != Duration::from_millis(0) {
            sleep(delay).await;
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl Raft for NetworkFilteredService {
    async fn request_vote(
        &self,
        request: tonic::Request<RequestVoteArgsPb>,
    ) -> Result<tonic::Response<RequestVoteReplyPb>, tonic::Status> {
        let args_pb = request.into_inner();
        let args: RequestVoteArgs = args_pb.into();
        self.apply_link_behavior(args.candidate_id).await?;

        let reply = self.inner.service.handle_request_vote(args);
        let reply_pb: RequestVoteReplyPb = reply.into();
        Ok(tonic::Response::new(reply_pb))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesArgsPb>,
    ) -> Result<tonic::Response<AppendEntriesReplyPb>, tonic::Status> {
        let args_pb = request.into_inner();
        let args: AppendEntriesArgs = args_pb.try_into()
            .map_err(|_| tonic::Status::invalid_argument("Invalid AppendEntries arguments"))?;
        self.apply_link_behavior(args.leader_id).await?;

        let reply = self.inner.service.handle_append_entries(args);
        let reply_pb: AppendEntriesReplyPb = reply.into();
        Ok(tonic::Response::new(reply_pb))
    }

    // async fn client_command(
    //     &self,
    //     request: tonic::Request<ClientRequest>,
    // ) -> Result<tonic::Response<ClientReply>, tonic::Status> {
    //     // Delegate to the real RaftServer RPC implementation.
    //     <RaftServer as Raft>::client_command(&*self.inner, request).await
    // }
}

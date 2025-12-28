use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};
use tracing::{error, info};

use crate::raft::{
    client::Peer,
    proto::{kv_command, KvCommand, KvGet, KvSet},
};

/// KV存储命令
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u32,
    pub command: Option<Command>,
}

/// Domain model for AppendEntries RPC arguments
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesArgs {
    pub term: u32,
    pub leader_id: u32,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u32,
}

/// Domain model for AppendEntries RPC reply
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: u32,
    pub success: bool,
}

/// Domain model for RequestVote RPC arguments
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteArgs {
    pub term: u32,
    pub candidate_id: u32,
    pub last_log_index: u32,
    pub last_log_term: u32,
}

/// Domain model for RequestVote RPC reply
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: u32,
    pub vote_granted: bool,
}

/// Message sent from Raft core to the service/server layer when something becomes
/// committed and should be applied.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyMsg {
    /// A committed log entry to be applied to the state machine.
    Command {
        index: u32,
        term: u32,
        command: Command,
    },
    /// Snapshot installation/application (placeholder for future work).
    Snapshot {},
}

impl From<Command> for KvCommand {
    fn from(value: Command) -> Self {
        match value {
            Command::Get { key } => KvCommand {
                cmd: Some(kv_command::Cmd::Get(KvGet { key })),
            },
            Command::Set { key, value } => KvCommand {
                cmd: Some(kv_command::Cmd::Set(KvSet { key, value })),
            },
        }
    }
}

impl TryFrom<KvCommand> for Command {
    type Error = ();

    fn try_from(value: KvCommand) -> Result<Self, Self::Error> {
        match value.cmd {
            Some(kv_command::Cmd::Get(get)) => Ok(Command::Get { key: get.key }),
            Some(kv_command::Cmd::Set(set)) => Ok(Command::Set {
                key: set.key,
                value: set.value,
            }),
            None => Err(()),
        }
    }
}

/// Raft service implementation
#[derive(Clone)]
pub struct RaftService {
    core: Arc<Mutex<RaftCore>>,
    apply_tx: mpsc::UnboundedSender<ApplyMsg>,
    peers: Arc<Vec<Arc<Peer>>>,
    config: Arc<RaftConfig>,
}

/// Core Raft state based on Figure 2
#[derive(Debug, Clone)]
pub struct RaftCore {
    // Persistent state on all servers (should be persisted before responding to RPCs)
    /// Latest term server has seen (initialized to 0, increases monotonically)
    pub current_term: u32,
    /// CandidateId that received vote in current term (or None)
    pub voted_for: Option<u32>,
    /// Log entries; each entry contains command and term when received by leader
    pub log: Vec<LogEntry>,

    // Volatile state on all servers
    /// Index of highest log entry known to be committed (initialized to 0)
    pub commit_index: u32,
    /// Index of highest log entry applied to state machine (initialized to 0)
    pub last_applied: u32,

    // Additional fields
    /// Current node state (Follower/Candidate/Leader)
    pub state: NodeState,
    /// This server's ID
    pub id: u32,
    /// 最近已知的leader ID（用于客户端重定向）
    pub last_known_leader: Option<u32>,
    /// 最后一次收到心跳的时间
    pub last_heartbeat: Instant,
}

/// Raft node states from Figure 2
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate {
        votes: usize,
    },
    Leader {
        // Volatile state on leaders (reinitialized after election)
        /// For each server, index of next log entry to send (initialized to leader last log index + 1)
        next_index: Vec<u32>,
        /// For each server, index of highest log entry known to be replicated (initialized to 0)
        match_index: Vec<u32>,
    },
}
/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's unique identifier.
    pub node_id: u32,
    /// Address to bind the gRPC server.
    pub bind_addr: SocketAddr,
    /// List of peer nodes in the cluster.
    pub peers: Arc<Vec<Arc<Peer>>>,
    /// Whether to automatically start an election on startup.
    pub auto_election: bool,
    /// Election timeout range in milliseconds.
    pub election_timeout_ms: (u64, u64),
    /// Heartbeat interval in milliseconds (for leaders).
    pub heartbeat_interval_ms: u64,
}

impl RaftCore {
    pub fn new(id: u32) -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                command: None,
            }],
            commit_index: 0,
            last_applied: 0,
            state: NodeState::Follower,
            id,
            last_known_leader: None,
            last_heartbeat: Instant::now(),
        }
    }

    fn last_log_term(&self) -> u32 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    fn last_log_index(&self) -> u32 {
        self.log.len() as u32 - 1
    }

    /// 为指定的 peer_id 构造 AppendEntriesArgs
    /// # Arguments
    /// * `peer_id` - 目标 peer 的 ID
    /// # Returns
    /// Some(args) 如果当前节点是 Leader
    /// None 如果当前节点不是 Leader
    /// # Notes
    /// 不会改变节点状态
    fn append_entries_args(&self, peer_id: u32) -> Option<AppendEntriesArgs> {
        if let NodeState::Leader {
            next_index,
            match_index: _,
        } = &self.state
        {
            let next_index = next_index[peer_id as usize];
            let prev_log_index = next_index - 1;
            let prev_log_term = self.log[prev_log_index as usize].term;
            let entries = self.log[next_index as usize..].to_vec();
            let args = AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };
            Some(args)
        } else {
            None
        }
    }

    fn request_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }

    fn on_receive_term(&mut self, term: u32) {
        if term > self.current_term {
            info!(
                "[Node {}] Discovered higher term {} from peer ?, reverting to follower",
                self.id, term
            );
            self.current_term = term;
            self.state = NodeState::Follower;
            self.voted_for = None;
        }
    }

    /// 将已提交但未应用的日志条目应用到状态机
    ///
    /// # Notes
    /// 两个地方会调用它：
    /// - 收到 AppendEntriesReply （由 Leader 发出 AE RPC 后收到回复时）
    /// - 收到 AppendEntries （由 Follower 收到 AE RPC 时进行处理）
    fn apply_logs(
        &mut self,
        sender: &mpsc::UnboundedSender<ApplyMsg>,
    ) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = self.log[self.last_applied as usize].clone();
            info!(
                "[Node {}] Applying log entry at index {}: {:?}",
                self.id, self.last_applied, entry
            );
            // Send to state machine applier.
            // Note: index 0 is a dummy entry; real entries should carry a command.
            if let Some(cmd) = entry.command {
                if let Err(err) = sender.send(ApplyMsg::Command {
                    index: self.last_applied,
                    term: entry.term,
                    command: cmd,
                }) {
                    error!(
                        "[Node {}] Failed to send apply msg to state machine applier: {}",
                        self.id, err
                    );
                }
            }
        }
    }
}

impl RaftService {
    pub fn new(
        peers: &Arc<Vec<Arc<Peer>>>,
        apply_tx: mpsc::UnboundedSender<ApplyMsg>,
        config: &Arc<RaftConfig>,
    ) -> Self {
        // Initialize RaftCore
        let service = Self {
            core: Arc::new(Mutex::new(RaftCore::new(config.node_id))),
            apply_tx,
            peers: peers.clone(),
            config: config.clone(),
        };

        // Spawn election timer task
        let svc = service.clone();
        tokio::spawn(async move {
            loop {
                let timeout = svc.config.election_timeout_ms.0
                    + (rand::random::<u64>()
                        % (svc.config.election_timeout_ms.1 - svc.config.election_timeout_ms.0));
                let timeout = Duration::from_millis(timeout);

                // Check if election timeout has passed without receiving heartbeat
                let (is_leader, elapsed) = {
                    let core = svc.core.lock().unwrap();
                    (matches!(core.state, NodeState::Leader { .. }), core.last_heartbeat.elapsed())
                };

                if !is_leader && elapsed >= timeout {
                    info!(
                        "[Node {}] Election timeout triggered after {:?}",
                        svc.config.node_id, elapsed
                    );
                    svc.start_election().await;
                    // Reset heartbeat timer
                    svc.core.lock().unwrap().last_heartbeat = Instant::now();
                } else if elapsed < timeout {
                    tokio::time::sleep(timeout - elapsed).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(svc.config.heartbeat_interval_ms)).await;
                }
            }
        });

        service
    }

    /// 返回当前节点的 (任期, 是否为Leader)
    pub fn get_state(&self) -> (u32, bool) {
        let core = self.core.lock().unwrap();
        let is_leader = matches!(core.state, NodeState::Leader { .. });
        (core.current_term, is_leader)
    }

    /// Start an election by converting to candidate, bumping term, and requesting votes from peers.
    pub async fn start_election(&self) {
        let rv_args = self.request_vote_args();

        info!(
            "[Node {}] Starting election for term {} ({} peers)",
            rv_args.candidate_id,
            rv_args.term,
            self.peers.len()
        );

        for peer in self
            .peers
            .iter()
            .filter(|p| p.id != self.core.lock().unwrap().id)
        {
            let peer = peer.clone();
            let args = rv_args.clone();
            let svc = self.clone();
            let candidate_id = args.candidate_id;

            tokio::spawn(async move {
                match peer.send_request_vote(args).await {
                    Ok(reply) => {
                        svc.on_receive_rv_reply(&reply);
                    }
                    Err(err) => {
                        info!(
                            "[Node {}] Failed to request vote from peer {}: {}",
                            candidate_id, peer.id, err
                        );
                    }
                }
            });
        }
    }

    /// 并发地对所有 peers 发送心跳
    ///
    /// 内部只会在 Leader 状态下发送心跳
    /// # Notes
    /// * RPC - 会调用 AE RPC
    /// * 状态机 - 可能会与底层状态机交互（因为多数节点可能复制了日志）
    pub async fn broadcast_heartbeat(&self) {
        for peer in self
            .peers
            .iter()
            .filter(|p| p.id != self.core.lock().unwrap().id)
        {
            if let Some(args) = self.append_entries_args(peer.id) {
                let peer = peer.clone();
                let svc = self.clone();

                tokio::spawn(async move {
                    match peer.send_append_entries(args.clone()).await {
                        Ok(reply) => {
                            svc.on_receive_ae_reply(&args, &reply, peer.id);
                        }
                        Err(err) => {
                            info!(
                                "[Node {}] Failed heartbeat to peer {}: {}",
                                args.leader_id, peer.id, err
                            );
                        }
                    }
                });
            }
        }
    }

    fn append_entries_args(&self, peer_id: u32) -> Option<AppendEntriesArgs> {
        let node = self.core.lock().unwrap();
        node.append_entries_args(peer_id)
    }

    fn request_vote_args(&self) -> RequestVoteArgs {
        let mut node = self.core.lock().unwrap();
        node.state = NodeState::Candidate { votes: 0 }; // exclude vote from self
        node.current_term += 1;
        node.voted_for = Some(node.id);
        node.request_vote_args()
    }

    fn on_receive_ae_reply(
        &self,
        args: &AppendEntriesArgs,
        reply: &AppendEntriesReply,
        peer_id: u32,
    ) {
        let mut node = self.core.lock().unwrap();
        node.on_receive_term(reply.term);
        if node.current_term == args.term
            && let NodeState::Leader {
                next_index,
                match_index,
            } = &mut node.state
        {
            let peer_id = peer_id as usize;
            if reply.success {
                let new_match = args.prev_log_index + args.entries.len() as u32;
                match_index[peer_id] = std::cmp::max(match_index[peer_id], new_match);
                next_index[peer_id] = match_index[peer_id] + 1;
                // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
                // and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
                let mut indices: Vec<u32> = match_index.iter().copied().collect();
                indices.push(node.last_log_index());
                indices.sort_unstable();
                let majority_index = indices[indices.len() / 2];
                if majority_index > node.commit_index
                    && node.log[majority_index as usize].term == node.current_term
                {
                    node.commit_index = majority_index;
                    info!(
                        "[Node {}] Advanced commit_index to {}",
                        node.id, node.commit_index
                    );
                }
                node.apply_logs(&self.apply_tx);
            } else if args.term >= reply.term {
                // 如果AE被拒绝了，有可能
                // 1. 它的任期比那时我的任期更大
                // 2. prev_log_index它没有，或者它的prev_log_term和我的对不上（这是我们正在考虑的）
                // TODO: quickly find next_index
                next_index[peer_id] = args.prev_log_index;
            }
        }
    }

    fn on_receive_rv_reply(&self, reply: &RequestVoteReply) {
        let mut node = self.core.lock().unwrap();
        node.on_receive_term(reply.term);
        let node_id = node.id;
        // 如果支持的不是此任期的自己，不能说明投票给自己（投给的是过去的自己），或者
        // 如果此时自己已经不是候选者了（不再接收投票）
        if reply.term == node.current_term
            && reply.vote_granted
            && let NodeState::Candidate { votes, .. } = &mut node.state
        {
            *votes += 1;
            info!(
                "[Node {}] Received vote from peer ? (total votes = {})",
                node_id, votes
            );
            if *votes >= self.peers.len() / 2 {
                // c --receives votes from majority of servers--> l
                node.state = NodeState::Leader {
                    next_index: vec![node.log.len() as u32; self.peers.len()],
                    match_index: vec![0; self.peers.len()],
                };
                // start heartbeat
                let svc = self.clone();
                tokio::spawn(async move {
                    while matches!(svc.core.lock().unwrap().state, NodeState::Leader { .. }) {
                        svc.broadcast_heartbeat().await;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }
        }
    }

    pub fn handle_request_vote(&self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut node = self.core.lock().unwrap();
        node.on_receive_term(args.term);

        info!(
            "[Node {}] Received RequestVote from candidate {} for term {}",
            node.id, args.candidate_id, args.term
        );

        let term = node.current_term;
        let vote_granted = false;
        // Reply false if term < currentTerm (§5.1)
        if args.term < node.current_term {
            info!(
                "[Node {}] Rejecting vote: candidate term {} < current term {}",
                node.id, args.term, node.current_term
            );
            return RequestVoteReply { term, vote_granted };
        }

        // Check if we can grant vote
        let can_vote = node.voted_for.is_none() || node.voted_for == Some(args.candidate_id);

        // Check if candidate's log is at least as up-to-date as receiver's log (§5.4)
        // If logs have different terms, the one with later term is more up-to-date
        // If logs have same last term, the longer log is more up-to-date
        let log_is_up_to_date = args.last_log_term > node.last_log_term()
            || (args.last_log_term == node.last_log_term()
                && args.last_log_index >= node.last_log_index());

        let vote_granted = can_vote && log_is_up_to_date;

        if vote_granted {
            info!(
                "[Node {}] Granting vote to candidate {}",
                node.id, args.candidate_id
            );
            node.voted_for = Some(args.candidate_id);
        } else {
            info!(
                "[Node {}] Denying vote to candidate {} (can_vote={}, log_up_to_date={})",
                node.id, args.candidate_id, can_vote, log_is_up_to_date
            );
        }
        RequestVoteReply { term, vote_granted }
    }

    pub fn handle_append_entries(&self, mut args: AppendEntriesArgs) -> AppendEntriesReply {
        let mut node = self.core.lock().unwrap();
        
        // 更新心跳时间
        node.last_heartbeat = Instant::now();
        
        node.on_receive_term(args.term);

        info!(
            "[Node {}] Received AE {:?} from leader {} for term {}",
            node.id, args.entries, args.leader_id, args.term
        );

        let term = node.current_term;
        let success = false;

        // Reply false if term < currentTerm (§5.1)
        if args.term < node.current_term {
            info!(
                "[Node {}] Rejecting AppendEntries: leader term {} < current term {}",
                node.id, args.term, node.current_term
            );
            return AppendEntriesReply { term, success };
        }

        // 更新已知的leader（仅在term检查通过后）
        node.last_known_leader = Some(args.leader_id);

        // Reply false if log doesn't contain an entry at prev_log_index
        // whose term matches prev_log_term (§5.3)
        if args.prev_log_index >= node.log.len() as u32 {
            info!(
                "[Node {}] Rejecting AppendEntries: prev_log_index {} >= log length {}",
                node.id,
                args.prev_log_index,
                node.log.len()
            );
            return AppendEntriesReply { term, success };
        }

        let prev_log_term = node.log[args.prev_log_index as usize].term;
        if prev_log_term != args.prev_log_term {
            info!(
                "[Node {}] Rejecting AppendEntries: term mismatch at index {} (expected {}, got {})",
                node.id, args.prev_log_index, args.prev_log_term, prev_log_term
            );
            return AppendEntriesReply { term, success };
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it (§5.3)
        let success = true;
        let start_index = args.prev_log_index as usize + 1;

        args.entries
            .iter()
            .enumerate()
            .find_map(|(i, entry)| match node.log.get(start_index + i) {
                // 如果有值且有冲突，返回冲突点
                Some(existing) if existing.term != entry.term => Some(i),
                // 如果有值但没冲突，继续查找
                Some(_) => None,
                // 如果没有值，返回追加点
                None => Some(i),
            })
            .inspect(|idx| {
                // 裁去冲突部分
                node.log.truncate(start_index + *idx);
                // 追加仍没有的部分 NOTE: SIDE AFFECT
                node.log.extend(args.entries.drain(*idx..));
            });

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if args.leader_commit > node.commit_index {
            node.commit_index = std::cmp::min(args.leader_commit, node.last_log_index());
            info!(
                "[Node {}] Updated commit_index to {}",
                node.id, node.commit_index
            );
            node.apply_logs(&self.apply_tx);
        }

        AppendEntriesReply { term, success }
    }

    /// Propose a new command to the Raft cluster.
    /// Returns (index, term) if successful, or Err with the last known leader ID.
    pub fn propose_command(&self, command: Command) -> Result<(u32, u32), Option<u32>> {
        let mut node = self.core.lock().unwrap();
        
        // 检查是否是leader
        if !matches!(node.state, NodeState::Leader { .. }) {
            // 不是leader，返回最近已知的leader
            return Err(node.last_known_leader);
        }
        
        let term = node.current_term;
        
        // 创建新的日志条目
        let entry = LogEntry {
            term,
            command: Some(command.into()),
        };
        
        // 将条目追加到日志中
        node.log.push(entry);
        let log_index = node.last_log_index();
        
        info!(
            "[Node {}] Proposed command at index {} for term {}",
            node.id, log_index, term
        );
        
        Ok((log_index, term))
    }
}

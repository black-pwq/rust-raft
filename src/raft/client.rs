use std::{sync::{Arc, Mutex}, time::Duration};
use tokio::time::timeout;
use tonic::{Request, Status};
use crate::raft::proto::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply, raft_client::RaftClient};

#[derive(Clone)]
pub struct Peer {
    pub id: u32,
    pub addr: String,
    // Cached connection for this peer
    client: Arc<Mutex<Option<RaftClient<tonic::transport::Channel>>>>,
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("id", &self.id)
            .field("addr", &self.addr)
            .finish()
    }
}

impl Peer {
    pub fn new(id: u32, addr: String) -> Self {
        Self {
            id,
            addr,
            client: Arc::new(Mutex::new(None)),
        }
    }

    /// Get or establish connection to this peer
    async fn get_or_connect(&self) -> Result<RaftClient<tonic::transport::Channel>, Status> {
        // First, check for an existing cached client while holding the lock.
        {
            let conn = self.client.lock().unwrap();
            if let Some(client) = conn.as_ref() {
                return Ok(client.clone());
            }
        }

        // No cached client; establish a new connection without holding the lock.
        let client = RaftClient::connect(self.addr.clone())
            .await
            .map_err(|e| Status::unknown(format!("Failed to connect to {}: {}", self.addr, e)))?;

        // Store the new client, but handle the case where another task may have raced us.
        let mut conn = self.client.lock().unwrap();
        if let Some(existing) = conn.as_ref() {
            Ok(existing.clone())
        } else {
            *conn = Some(client.clone());
            Ok(client)
        }
    }

    /// Send RequestVote RPC to this peer
    pub async fn send_request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, Status> {
        let mut client = self.get_or_connect().await?;
        match timeout(Duration::from_secs(1), client.request_vote(Request::new(args))).await {
            Ok(Ok(response)) => {
                let reply: RequestVoteReply = response.into_inner();
                Ok(reply)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Status::deadline_exceeded("RequestVote RPC timed out")),
        }
    }

    /// Send AppendEntries RPC to this peer
    pub async fn send_append_entries(
        &self,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, Status> {
        let mut client = self.get_or_connect().await?;
        
        match timeout(Duration::from_secs(1), client.append_entries(Request::new(args))).await {
            Ok(Ok(response)) => {
                let reply: AppendEntriesReply = response.into_inner();
                Ok(reply)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Status::deadline_exceeded("AppendEntries RPC timed out")),
        }
    }
}
pub mod client;
pub mod core;
use tonic::{Request, Response, Status};

// Include generated proto code
pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply, raft_server::Raft,
};

// Re-export key types for consumers of the crate.
pub use client::Peer;
pub use core::RaftService;

#[tonic::async_trait]
impl Raft for RaftService {
    /// RequestVote RPC implementation (Figure 2)
    /// Invoked by candidates to gather votes
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let args = request.into_inner();
        let reply = self.handle_request_vote(args);
        Ok(Response::new(reply))
    }

    /// AppendEntries RPC implementation (Figure 2)
    /// Invoked by leader to replicate log entries; also used as heartbeat
    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let args = request.into_inner();
        let reply = self.handle_append_entries(args);
        Ok(Response::new(reply))
    }
}

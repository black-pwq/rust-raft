pub mod client;
pub mod core;
pub mod mapper;
pub mod storage;
use tonic::{Request, Response, Status};

// Include generated proto code
pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    AppendEntriesArgsPb, AppendEntriesReplyPb,
    RequestVoteArgsPb, RequestVoteReplyPb, raft_server::Raft,
};

// Re-export key types for consumers of the crate.
pub use client::Peer;
pub use core::{Command, RaftService, ApplyMsg, AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
pub use storage::{Storage, FileStorage, PersistentState};

#[tonic::async_trait]
impl Raft for RaftService {
    /// RequestVote RPC implementation (Figure 2)
    /// Invoked by candidates to gather votes
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgsPb>,
    ) -> Result<Response<RequestVoteReplyPb>, Status> {
        let args_pb = request.into_inner();
        let args: RequestVoteArgs = args_pb.into();
        let reply = self.handle_request_vote(args);
        let reply_pb: RequestVoteReplyPb = reply.into();
        Ok(Response::new(reply_pb))
    }

    /// AppendEntries RPC implementation (Figure 2)
    /// Invoked by leader to replicate log entries; also used as heartbeat
    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgsPb>,
    ) -> Result<Response<AppendEntriesReplyPb>, Status> {
        let args_pb = request.into_inner();
        let args: AppendEntriesArgs = args_pb.try_into()
            .map_err(|_| Status::invalid_argument("Invalid AppendEntries arguments"))?;
        let reply = self.handle_append_entries(args);
        let reply_pb: AppendEntriesReplyPb = reply.into();
        Ok(Response::new(reply_pb))
    }

    // ClientCommand RPC implementation
    // Invoked by clients to submit commands to the cluster

    // async fn client_command(
    //     &self,
    //     request: Request<ClientRequest>,
    // ) -> Result<Response<ClientReply>, Status> {
    //     let req = request.into_inner();
    //     let timeout = tokio::time::Duration::from_secs(5); // 5秒超时
        
    //     // 构造命令
    //     let command = match CommandType::try_from(req.command_type) {
    //         Ok(CommandType::Get) => Command::Get { key: req.key },
    //         Ok(CommandType::Set) => Command::Set { key: req.key, value: req.value },
    //         Err(_) => {
    //             return Ok(Response::new(ClientReply {
    //                 success: false,
    //                 error: "Invalid command type".to_string(),
    //                 leader_id: 0,
    //                 value: String::new(),
    //             }));
    //         }
    //     };
        
    //     match self.handle_client_request(command, timeout).await {
    //         Ok(()) => Ok(Response::new(ClientReply {
    //             success: true,
    //             error: String::new(),
    //             leader_id: 0,
    //             value: String::new(), // TODO: 从状态机获取实际值
    //         })),
    //         Err((error, leader_id)) => Ok(Response::new(ClientReply {
    //             success: false,
    //             error,
    //             leader_id: leader_id.unwrap_or(0),
    //             value: String::new(),
    //         })),
    //     }
    // }
}

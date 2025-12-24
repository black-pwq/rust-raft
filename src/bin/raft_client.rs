use rust_raft::raft::proto::{raft_client::RaftClient, RequestVoteArgs};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    // Parse arguments: candidate_id port
    let candidate_id = if args.len() > 1 {
        args[1].parse().unwrap_or(1)
    } else {
        1
    };
    
    let port = if args.len() > 2 {
        args[2].parse().unwrap_or(50051)
    } else {
        50051
    };
    
    let addr = format!("http://[::1]:{}", port);
    println!("Connecting to Raft node at {}", addr);
    
    let mut client = RaftClient::connect(addr).await?;
    
    // Send RequestVote RPC
    let request = tonic::Request::new(RequestVoteArgs {
        term: 1,
        candidate_id,
        last_log_index: 0,
        last_log_term: 0,
    });
    
    println!("Sending RequestVote from candidate {}...", candidate_id);
    let response = client.request_vote(request).await?;
    let reply = response.into_inner();
    
    println!("\nRequestVote Response:");
    println!("  Term: {}", reply.term);
    println!("  Vote Granted: {}", reply.vote_granted);
    
    Ok(())
}

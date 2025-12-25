use rust_raft::raft::proto::{raft_client::RaftClient, ClientRequest, CommandType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 3 {
        println!("Usage:");
        println!("  {} get <port> <key>              - Get value for key", args[0]);
        println!("  {} set <port> <key> <value>      - Set key to value", args[0]);
        return Ok(());
    }
    
    let command = &args[1];
    let port: u16 = args[2].parse().unwrap_or(50051);
    
    let addr = format!("http://[::1]:{}", port);
    println!("Connecting to Raft node at {}", addr);
    
    let mut client = RaftClient::connect(addr).await?;
    
    match command.as_str() {
        "get" => {
            if args.len() < 4 {
                println!("Error: key required for GET command");
                return Ok(());
            }
            
            let key = &args[3];
            
            let request = tonic::Request::new(ClientRequest {
                command_type: CommandType::Get as i32,
                key: key.clone(),
                value: String::new(),
            });
            
            println!("Getting key: '{}'...", key);
            let response = client.client_command(request).await?;
            let reply = response.into_inner();
            
            println!("\nGET Response:");
            println!("  Success: {}", reply.success);
            if reply.success {
                println!("  Value: {}", reply.value);
            } else {
                println!("  Error: {}", reply.error);
                if reply.leader_id != 0 {
                    println!("  Leader ID: {}", reply.leader_id);
                    println!("  Try connecting to port {} instead.", 50051 + reply.leader_id - 1);
                }
            }
        }
        "set" => {
            if args.len() < 5 {
                println!("Error: key and value required for SET command");
                return Ok(());
            }
            
            let key = &args[3];
            let value = args[4..].join(" ");
            
            let request = tonic::Request::new(ClientRequest {
                command_type: CommandType::Set as i32,
                key: key.clone(),
                value: value.clone(),
            });
            
            println!("Setting '{}' = '{}'...", key, value);
            let response = client.client_command(request).await?;
            let reply = response.into_inner();
            
            println!("\nSET Response:");
            println!("  Success: {}", reply.success);
            if !reply.success {
                println!("  Error: {}", reply.error);
                if reply.leader_id != 0 {
                    println!("  Leader ID: {}", reply.leader_id);
                    println!("  Try connecting to port {} instead.", 50051 + reply.leader_id - 1);
                }
            } else {
                println!("  Command successfully applied!");
            }
        }
        _ => {
            println!("Unknown command: {}", command);
            println!("Use 'get' or 'set'");
        }
    }
    
    Ok(())
}

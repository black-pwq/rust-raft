mod common;

use common::TestHarness;
use rust_raft::raft::proto::{raft_client::RaftClient, ClientRequest, CommandType};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_client_set_on_leader_succeeds() {
    let servers = 3;
    let mut ts = TestHarness::new(servers, true).await;

    ts.begin(
        "TestClientSetLeader",
        "Client SET should succeed when sent to leader",
    );

    // Wait for election
    sleep(Duration::from_millis(1000)).await;
    let leader = ts.check_one_leader().await;
    ts.log_leader(leader);

    let mut client = RaftClient::connect(ts.endpoint(leader)).await.unwrap();

    let reply = client
        .client_command(ClientRequest {
            command_type: CommandType::Set as i32,
            key: "k1".to_string(),
            value: "v1".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(reply.success, "expected success, got error='{}'", reply.error);

    ts.log_pass();
    ts.cleanup().await;
}

#[tokio::test]
async fn test_client_set_on_follower_redirects() {
    let servers = 3;
    let mut ts = TestHarness::new(servers, true).await;

    ts.begin(
        "TestClientSetFollowerRedirect",
        "Client SET sent to follower should return Not the leader + leader_id",
    );

    // Wait for election and some heartbeats so followers learn leader_id.
    sleep(Duration::from_millis(1200)).await;
    let leader = ts.check_one_leader().await;
    ts.log_leader(leader);

    let follower = (leader + 1) % servers;
    let mut client = RaftClient::connect(ts.endpoint(follower)).await.unwrap();

    let reply = client
        .client_command(ClientRequest {
            command_type: CommandType::Set as i32,
            key: "k2".to_string(),
            value: "v2".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(!reply.success, "expected failure on follower");
    assert!(
        reply.error.contains("Not the leader"),
        "unexpected error='{}'",
        reply.error
    );
    assert_eq!(reply.leader_id as usize, leader);

    ts.log_pass();
    ts.cleanup().await;
}

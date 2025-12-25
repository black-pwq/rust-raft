/// Raft Leader Election Tests (3A)
/// Based on MIT 6.5840 Raft implementation tests
mod common;

use common::TestHarness;
use rand::Rng;
use tracing::warn;
use std::time::Duration;
use tokio::time::sleep;

/// Election timeout as defined in MIT 6.5840 tests
const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

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

/// Raft Leader Election Tests (3A)
/// Based on MIT 6.5840 Raft implementation tests

use rust_raft::{test_util::TestHarness, tpanic};
use rand::Rng;
use tracing::{debug, error, info, instrument, warn};
use std::time::Duration;
use tokio::time::sleep;


const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

#[tokio::test]
#[instrument]
async fn test_initial_election_3a() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3A): initial election =====");

    // Is a leader elected?
    ts.check_one_leader().await?;

    // Sleep a bit to avoid racing with followers learning of the election
    info!("checking term agreement...");
    sleep(Duration::from_millis(50)).await;
    let term1 = ts.check_terms().ok_or("No terms found after initial election")?;

    // Does the leader+term stay the same if there is no network failure?
    info!("waiting 2 election timeouts to check stability...");
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    let term2 = ts.check_terms().ok_or("No terms found after waiting 2 election timeouts")?;
    if term1 != term2 {
        warn!("Term changed from {} to {} even though there were no failures", term1, term2);
    }

    // There should still be a leader
    let _ = ts.check_one_leader().await?;

    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_re_election_3a() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3A): election after network failure =====");

    let leader1 = ts.check_one_leader().await?;
    // If the leader disconnects, a new one should be elected
    debug!("testing leader failure and re-election...");
    ts.disconnect_all(leader1);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    let _ = ts.check_one_leader().await?;
    // If the old leader rejoins, that shouldn't disturb the new leader
    debug!("testing old leader rejoining...");
    ts.connect_one(leader1);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    let leader3 = ts.check_one_leader().await?;
    info!("Leader: {}", leader3);

    // If there's no quorum, no new leader should be elected
    debug!("testing loss of quorum...");
    ts.disconnect_all(leader3);
    ts.disconnect_all((leader3 + 1) % servers);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;

    // Check that the one connected server does not think it is the leader
    ts.check_no_leader()?;
    info!("confirmed: no leader without quorum");

    // If a quorum arises, it should elect a leader
    info!("restoring quorum...");
    ts.connect_one((leader3 + 1) % servers);
    ts.log_network();
    sleep(2 * RAFT_ELECTION_TIMEOUT).await;
    ts.check_one_leader().await?;

    // Re-join of last node shouldn't prevent leader from existing
    info!("re-joining last node...");
    ts.connect_one(leader3);
    ts.log_network();
    sleep(Duration::from_millis(500)).await;
    ts.check_one_leader().await?;

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_many_elections_3a() -> Result<(), String> {
    let servers = 7;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3A): multiple elections =====");

    // Give time for initial election
    info!("waiting for initial election...");
    sleep(Duration::from_millis(1000)).await;
    ts.check_one_leader().await?;

    let iters = 10;
    for ii in 1..iters {
        info!("iteration {}/{}", ii, iters - 1);

        // Disconnect three random nodes
        let mut rng = rand::thread_rng();
        let i1 = rng.gen_range(0..servers);
        let i2 = rng.gen_range(0..servers);
        let i3 = rng.gen_range(0..servers);

        info!("disconnecting servers [{}, {}, {}]", i1, i2, i3);
        ts.disconnect_all(i1);
        ts.disconnect_all(i2);
        ts.disconnect_all(i3);
        ts.log_network();

        // Either the current leader should still be alive,
        // or the remaining four should elect a new one
        sleep(2 * RAFT_ELECTION_TIMEOUT).await;
        ts.check_one_leader().await?;

        info!("reconnecting servers [{}, {}, {}]", i1, i2, i3);
        ts.connect_one(i1);
        ts.connect_one(i2);
        ts.connect_one(i3);
        ts.log_network();

        sleep(Duration::from_millis(500)).await;
    }

    info!("final check...");
    ts.check_one_leader().await?;

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

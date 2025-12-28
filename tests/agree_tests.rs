use tokio::time::Duration;

use rust_raft::{
    raft::{ApplyMsg, Command},
    tassert, tassert_eq,
    test_util::TestHarness,
    tpanic,
};
use tokio::time::sleep;
use tracing::{info, instrument};

#[tokio::test]
#[instrument]
async fn test_basic_agree_3b() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;
    info!("===== Test (3B): basic agreement =====");

    // 等待选举产生
    sleep(Duration::from_millis(1000)).await;

    // 尝试提交索引为1, 2, 3的三个命令
    for i in 1..=3 {
        // 在没有提交命令之前，日志应该是空的
        match ts.n_committed(i).await {
            Ok((n, _)) => tassert_eq!(
                n,
                0,
                "在没有提交命令之前，日志不应该在索引 {i} 处存在，但实际有 {n} 个服务器提交了该日志",
            ),
            Err(e) => tpanic!("{e}"),
        }
        // 检查完毕后，提交一个命令
        let cmd = Command::get(100 + i);
        match ts.one(cmd, servers, false).await {
            Ok(index) => tassert_eq!(index, i, "提交的命令索引应该是 {i}，但实际是 {index}"),
            Err(e) => tpanic!("{e}"),
        }
    }

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_rpc_bytes_3b() -> Result<(), String> {
    // TODO: 需要实现 RPC 字节计数功能
    // 参考 Go 实现的 BytesTotal() 方法
    info!("===== Test (3B): RPC byte count =====");
    info!("===== SKIPPED (RPC counting not implemented) =====");
    Ok(())
}
#[tokio::test]
#[instrument]
async fn test_follower_failure_3b() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): agreement with failures =====");

    if let Err(e) = ts.one(Command::get(101), servers, false).await {
        tpanic!("{e}: 在没有节点失败的情况下，日志应该可以被所有节点共识。");
    }

    info!("===== 让一个 follower 离开网络 =====");
    // 让一个 follower 离开网络
    let leader = ts.check_one_leader().await?;
    ts.disconnect_all((leader + 1) % servers);

    // 提交一个命令，这个命令仍然应该被大多数节点共识
    if let Err(e) = ts.one(Command::get(102), servers - 1, false).await {
        tpanic!("{e}: 在一个 follower 离开网络的情况下，日志仍然应该可以被大多数节点共识。");
    }
    // 等待一个选举超时（有可能刚才的 leader 挂了）
    sleep(Duration::from_millis(1000)).await;
    // 再提交一个命令
    if let Err(e) = ts.one(Command::get(103), servers - 1, false).await {
        tpanic!("{e}: 在一个 follower 离开网络的情况下，日志仍然应该可以被大多数节点共识。");
    }

    info!("===== 让所有 follower 离开网络，只留下 leader =====");
    // 让其他所有 follower 离开网络，只留下 leader
    let leader = ts.check_one_leader().await?;
    ts.disconnect_all(leader);
    // 提交一个命令
    match ts.servers[leader]
        .service
        .propose_command(Command::get(104))
    {
        Ok((ndx, _)) => tassert_eq!(
            ndx,
            4,
            "在只有 leader 存活的情况下，日志仍然应该被提交，且索引为 4，但实际是 {}",
            ndx
        ),
        Err(_) => tpanic!("leader {leader} 拒绝了命令提交，但只有 leader 存活时，其身份应该不变"),
    }

    // 等待两个选举超时，随后检查 104 的确没有被共识
    sleep(Duration::from_millis(2000)).await;
    if let Err(e) = ts.check_no_agreement(4).await {
        tpanic!("{e}: 在只有 leader {leader} 存活的情况下，日志不应该被其他服务器提交");
    }
    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_leader_failure_3b() -> Result<(), String> {
    let servers: usize = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): test failure of leaders =====");

    ts.one(Command::get(101), servers, false).await?;
    info!("===== 让第一个 leader 离开网络 =====");
    let leader1 = ts.check_one_leader().await?;
    ts.disconnect_all(leader1);

    // 剩余的 followers 应该选出新的 leader
    ts.one(Command::get(102), servers - 1, false).await?;
    sleep(Duration::from_millis(1000)).await;
    ts.one(Command::get(103), servers - 1, false).await?;

    info!("===== 让第二个 leader 也离开网络 =====");
    let leader2 = ts.check_one_leader().await?;
    ts.disconnect_all(leader2);

    // 向所有服务器提交命令
    for i in 0..servers {
        let _ = ts.servers[i].service.propose_command(Command::get(104));
    }

    sleep(Duration::from_millis(2000)).await;

    // 检查命令 104 没有被提交
    ts.check_no_agreement(4).await?;

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}
#[tokio::test]
#[instrument]
async fn test_fail_agree_3b() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): agreement after follower reconnects =====");

    ts.one(Command::get(101), servers, false).await?;

    info!("===== 让一个 follower 离开网络 =====");
    let leader = ts.check_one_leader().await?;
    ts.disconnect_all((leader + 1) % servers);

    // leader 和剩余的 follower 应该能够达成共识
    ts.one(Command::get(102), servers - 1, false).await?;
    ts.one(Command::get(103), servers - 1, false).await?;
    sleep(Duration::from_millis(1000)).await;
    ts.one(Command::get(104), servers - 1, false).await?;
    ts.one(Command::get(105), servers - 1, false).await?;

    info!("===== 重新连接断开的 follower =====");
    ts.connect_one((leader + 1) % servers);

    // 完整的服务器集合应该保留之前的共识，并能够在新命令上达成共识
    ts.one(Command::get(106), servers, true).await?;
    sleep(Duration::from_millis(1000)).await;
    ts.one(Command::get(107), servers, true).await?;

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}
#[tokio::test]
#[instrument]
async fn test_fail_no_agree_3b() -> Result<(), String> {
    let servers = 5;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): no agreement if too many followers disconnect =====");

    ts.one(Command::get(10), servers, false).await?;

    info!("===== 让 3 个 follower 离开网络（5个节点中） =====");
    let leader = ts.check_one_leader().await?;
    ts.disconnect_all((leader + 1) % servers);
    ts.disconnect_all((leader + 2) % servers);
    ts.disconnect_all((leader + 3) % servers);

    // 提交一个命令，但由于没有多数派，不应该被提交
    match ts.servers[leader].service.propose_command(Command::get(20)) {
        Ok((index, _)) => {
            tassert_eq!(index, 2, "提交的命令索引应该是 2");

            sleep(Duration::from_millis(2000)).await;

            let (n, _) = ts.n_committed(index).await?;
            tassert_eq!(
                n,
                0,
                "没有多数派时，不应该有服务器提交日志，但有 {n} 个服务器提交了",
            );
        }
        Err(_) => tpanic!("leader 应该接受命令，即使无法提交"),
    }

    info!("===== 修复网络分区 =====");
    ts.connect_one((leader + 1) % servers);
    ts.connect_one((leader + 2) % servers);
    ts.connect_one((leader + 3) % servers);

    // 断开连接的多数派可能已经选出了自己的 leader，可能忘记了索引 2
    let leader2 = ts.check_one_leader().await?;
    match ts.servers[leader2]
        .service
        .propose_command(Command::get(30))
    {
        Ok((index2, _)) => tassert!(
            index2 == 2 || index2 == 3,
            "新命令的索引应该在 [2, 3] 范围内，但实际是 {}",
            index2
        ),
        Err(_) => tpanic!("新 leader 应该接受命令"),
    }

    ts.one(Command::get(1000), servers, true).await?;

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_concurrent_starts_3b() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): concurrent Start()s =====");

    let mut success = false;

    'outer: for attempt in 0..5 {
        if attempt > 0 {
            // 给方案一些时间稳定下来
            sleep(Duration::from_secs(3)).await;
        }

        let leader = ts.check_one_leader().await?;
        info!("检查并发命令提交（尝试 {}）", attempt);

        let (_, term) = match ts.servers[leader as usize]
            .service
            .propose_command(Command::get(1))
        {
            Ok((idx, t)) => (idx, t),
            Err(_) => {
                info!("leader {} 已经不再是 leader", leader);
                continue 'outer;
            }
        };

        let iters = 5;
        let (tx, mut rx) = tokio::sync::mpsc::channel(iters);

        // 并发提交命令
        let mut handles = vec![];
        for i in 0..iters {
            let server = ts.servers[leader as usize].clone();
            let tx = tx.clone();
            let handle = tokio::spawn(async move {
                let result = server.service.propose_command(Command::get(100 + i));
                // 如果仍然是 leader，且任期与之前一样，则记录日志索引
                if let Ok((index, term1)) = result
                    && term1 == term
                {
                    let _ = tx.send(index).await;
                }
            });
            handles.push(handle);
        }
        drop(tx); // 关闭发送端

        // 等待所有任务完成
        for handle in handles {
            let _ = handle.await;
        }

        // 检查 term 是否改变
        for j in 0..servers {
            let (t, _) = ts.servers[j].service.get_state();
            if t != term {
                info!("服务器 {} 的 term 从 {} 变为 {}", j, term, t);
                break 'outer;
            }
        }

        // 等待所有索引的命令被提交
        let mut cmds = vec![];
        while let Some(index) = rx.recv().await {
            match ts.wait(index, servers, Some(term)).await {
                Ok(ApplyMsg::Command { command, .. }) => cmds.push(command),
                Ok(ApplyMsg::Snapshot {}) => {} // TODO: snapshot
                Err(_) => continue 'outer,
            }
        }

        // 检查所有并发提交的命令都在 cmds 中
        for i in 0..iters {
            let x = 100 + i;
            if !cmds.contains(&Command::get(x)) {
                info!("命令 {} 在 {:?} 中缺失", x, cmds);
                break 'outer;
            }
        }
        success = true;
        break;
    }

    tassert!(success, "在 5 次尝试后无法在并发提交下达成共识");
    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_rejoin_3b() -> Result<(), String> {
    let servers = 3;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): rejoin of partitioned leader =====");

    ts.one(Command::get(101), servers, true).await?;

    info!("===== leader 网络故障 =====");
    let leader1 = ts.check_one_leader().await?;
    ts.disconnect_all(leader1);

    // 让旧 leader 尝试在一些条目上达成共识
    ts.servers[leader1]
        .service
        .propose_command(Command::get(102))
        .ok();
    ts.servers[leader1]
        .service
        .propose_command(Command::get(103))
        .ok();
    ts.servers[leader1]
        .service
        .propose_command(Command::get(104))
        .ok();
    info!("向服务器 {} 提交了命令 [102, 103, 104]", leader1);

    // 新 leader 提交，索引=2 也会被提交
    ts.one(Command::get(103), 2, true).await?;

    info!("===== 新 leader 网络故障 =====");
    let leader2 = ts.check_one_leader().await?;
    ts.disconnect_all(leader2);

    info!("===== 旧 leader 重新连接 =====");
    ts.connect_one(leader1);

    ts.one(Command::get(104), 2, true).await?;

    info!("===== 所有服务器重新连接 =====");
    ts.connect_one(leader2);

    ts.one(Command::get(105), servers, true).await?;

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_backup_3b() -> Result<(), String> {
    let servers = 5;
    let ts = TestHarness::new(servers).await;

    info!("===== Test (3B): leader backs up quickly over incorrect follower logs =====");

    ts.one(Command::get(rand::random::<u32>()), servers, true).await?;

    // 将 leader 和一个 follower 放在一个分区
    let leader1 = ts.check_one_leader().await?;
    ts.disconnect_all((leader1 + 2) % servers);
    ts.disconnect_all((leader1 + 3) % servers);
    ts.disconnect_all((leader1 + 4) % servers);

    info!("向 leader {} 提交 50 个不会被提交的命令", leader1);
    // 提交很多不会被提交的命令
    for _ in 0..50 {
        ts.servers[leader1]
            .service
            .propose_command(Command::get(rand::random::<u32>()))
            .ok();
    }

    sleep(Duration::from_millis(500)).await;

    ts.disconnect_all((leader1 + 0) % servers);
    ts.disconnect_all((leader1 + 1) % servers);

    // 允许另一个分区恢复
    ts.connect_one((leader1 + 2) % servers);
    ts.connect_one((leader1 + 3) % servers);
    ts.connect_one((leader1 + 4) % servers);

    info!("向新分区提交 50 个成功的命令");
    // 向新组提交很多成功的命令
    for i in 0..50 {
        ts.one(Command::get(rand::random::<u32>()), 3, true).await.expect(&format!("向新分区提交的第 {i} 个命令失败"));
    }

    // 现在另一个分区的 leader 和一个 follower
    let leader2 = ts.check_one_leader().await?;
    let mut other = (leader1 + 2) % servers;
    if leader2 == other {
        other = (leader2 + 1) % servers;
    }
    ts.disconnect_all(other);

    info!("向 leader {} 提交 50 个不会被提交的命令", leader2);
    // 提交更多不会被提交的命令
    for _ in 0..50 {
        ts.servers[leader2]
            .service
            .propose_command(Command::get(rand::random::<u32>()))
            .ok();
    }

    sleep(Duration::from_millis(500)).await;

    // 让原来的 leader 重新上线
    for i in 0..servers {
        ts.disconnect_all(i);
    }
    ts.connect_one((leader1 + 0) % servers);
    ts.connect_one((leader1 + 1) % servers);
    ts.connect_one(other);

    info!("向新组提交 50 个成功的命令");
    // 向新组提交很多成功的命令
    for i in 0..50 {
        ts.one(Command::get(rand::random::<u32>()), 3, true).await.expect(&format!("向第二个新组提交的第 {i} 个命令失败"));
    }

    // 现在所有人都连接
    for i in 0..servers {
        ts.connect_one(i);
    }

    ts.one(Command::get(rand::random::<u32>()), servers, true).await.expect("向所有连通服务器提交命令失败");

    info!("===== PASS =====");
    ts.cleanup().await;
    Ok(())
}

#[tokio::test]
#[instrument]
async fn test_count_3b() -> Result<(), String> {
    // TODO: 需要实现 RPC 计数功能
    // 需要 BytesTotal() 或类似的 RPC 统计方法
    info!("===== Test (3B): RPC counts aren't too high =====");
    info!("===== TODO (requires RPC counting implementation) =====");
    Ok(())
}

use std::{collections::HashMap, sync::Arc};

use crate::raft::ApplyMsg;
use tokio::sync::Mutex;

pub struct LogChecker {
	/// committed logs of all nodes
	pub logs: Arc<Vec<Mutex<HashMap<u32, ApplyMsg>>>>,
	/// number of nodes
	n: usize,
}

impl LogChecker {
	pub fn new(n: usize) -> Self {
		let mut logs = Vec::with_capacity(n);
		for _ in 0..n {
			logs.push(Mutex::new(HashMap::new()));
		}
		Self {
			logs: Arc::new(logs),
			n,
		}
	}
	pub async fn append_log(&self, node: usize, msg: ApplyMsg) -> Result<(), String> {
		match msg {
			ApplyMsg::Command { index, .. } => {
				// 其他节点不应该提交了此日志
				self.check_log(node, &msg).await?;
				// 如果没有问题，则尝试添加此日志
				// 但首先要检查是否已经添加了前一索引的日志
				let mut log = self.logs[node].lock().await;
				if index > 1 && !log.contains_key(&(index - 1)) {
					return Err(format!(
						"LOG APPLY OUT OF ORDER: node {} missing previous log at index {} before appending {:?}",
						node, index - 1, msg
					));
				}
				log.insert(index, msg);
			}
			ApplyMsg::Snapshot {  } => {
				// TODO
			}
		}
		Ok(())
	}

	/// 只检查其他节点是否存在的一致的日志（不考虑不存在的日志）
	pub async fn check_log(&self, node_id: usize, msg: &ApplyMsg) -> Result<(), String> {
		match msg {
			ApplyMsg::Command { index, .. } => {
				for (i, log_mutex) in self.logs.iter().enumerate() {
					if i == node_id {
						continue;
					}
		
					let log = log_mutex.lock().await;
					if log.contains_key(index) && &log[index] != msg {
						return Err(format!(
							"LOG INCONSISTENCY: index {}: node {} has {:?} while node {} has expected {:?}",
							index, i, log[index], node_id, msg
						));
					}
				}
			},
			ApplyMsg::Snapshot {  } => {
				// TODO
			}
		};

		Ok(())
	}

	pub async fn get_committed(&self, node_id: usize, index: u32) -> Option<ApplyMsg> {
		let log = self.logs[node_id].lock().await;
		log.get(&index).cloned()
	}
}
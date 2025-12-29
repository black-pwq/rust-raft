use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, Read};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

use super::core::LogEntry;

/// 需要持久化的Raft状态（根据论文Figure 2）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    /// 当前任期
    pub current_term: u32,
    /// 当前任期投票给的候选者ID
    pub voted_for: Option<u32>,
    /// 日志条目
    pub log: Vec<LogEntry>,
}

/// 持久化存储的trait，用于测试和扩展
pub trait Storage: Send + Sync + std::fmt::Debug {
    /// 保存持久化状态
    fn persist(&mut self, state: &PersistentState) -> io::Result<()>;
    
    /// 读取持久化状态，如果不存在则返回None
    fn restore(&self) -> io::Result<Option<PersistentState>>;
}

/// 基于文件系统的简单持久化实现
#[derive(Debug)]
pub struct FileStorage {
    path: PathBuf,
}

impl FileStorage {
    /// 创建新的文件存储
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // 确保父目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        Ok(Self { path })
    }
}

impl Storage for FileStorage {
    fn persist(&mut self, state: &PersistentState) -> io::Result<()> {
        // 序列化为JSON
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        // 写入临时文件，然后原子性地重命名（确保crash safety）
        let temp_path = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        drop(file);
        
        // 原子性重命名
        fs::rename(temp_path, &self.path)?;
        
        Ok(())
    }
    
    fn restore(&self) -> io::Result<Option<PersistentState>> {
        // 如果文件不存在，返回None
        if !self.path.exists() {
            return Ok(None);
        }
        
        let mut file = File::open(&self.path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        // 反序列化
        let state = serde_json::from_str(&contents)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        
        Ok(Some(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::core::{LogEntry};
    
    #[test]
    fn test_file_storage() {
        let temp_dir = std::env::temp_dir();
        let test_path = temp_dir.join(format!("raft_test_{}.json", std::process::id()));
        
        // 清理可能存在的旧文件
        let _ = fs::remove_file(&test_path);
        
        let mut storage = FileStorage::new(&test_path).unwrap();
        
        // 持久化状态
        let state = PersistentState {
            current_term: 3,
            voted_for: None,
            log: vec![LogEntry { term: 0, command: None }],
        };
        storage.persist(&state).unwrap();
        
        // 创建新的存储实例来恢复（模拟重启）
        let storage2 = FileStorage::new(&test_path).unwrap();
        let restored = storage2.restore().unwrap().unwrap();
        assert_eq!(restored.current_term, 3);
        assert_eq!(restored.voted_for, None);
        assert_eq!(restored.log.len(), 1);
        
        // 清理
        fs::remove_file(&test_path).unwrap();
    }
}

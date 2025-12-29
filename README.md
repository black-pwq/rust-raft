# Rust Raft

一个基于 Rust 实现的 Raft 分布式共识协议库，遵循 [Raft 论文](https://raft.github.io/raft.pdf) 的设计。

## 目录

- [架构设计](#架构设计)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
- [测试](#测试)
- [示例应用](#示例应用)
- [项目结构](#项目结构)
- [限制](#限制)
- [参考资料](#参考资料)

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                  Application (KV Server)                    │
│                   ApplyMsg Channel ↕                        │
├─────────────────────────────────────────────────────────────┤
│                      Raft Core                              │
│    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│    │   Election   │  │ Log Replicat │  │  Persistence │     │
│    │    Timer     │  │    -ion      │  │              │     │
│    └──────────────┘  └──────────────┘  └──────────────┘     │
│         ↕                  ↕                  ↕             │
├─────────────────────────────────────────────────────────────┤
│                      gRPC (Tonic)                           │
│         RequestVote RPC  |  AppendEntries RPC               │
└─────────────────────────────────────────────────────────────┘
                           ↕
                    Network (TCP)
```

### 核心组件

#### 1. **RaftCore** - Raft 协议核心状态

位于 [`src/raft/core.rs`](src/raft/core.rs)，包含：

- **持久化状态**（需在响应 RPC 前持久化）：
  - `current_term`: 当前任期号
  - `voted_for`: 当前任期投票给的候选者
  - `log[]`: 日志条目数组
  
- **易失性状态**：
  - `commit_index`: 已知已提交的最高日志索引
  - `last_applied`: 已应用到状态机的最高日志索引
  - `state`: 节点状态（Follower/Candidate/Leader）

- **Leader 状态**（选举后重新初始化）：
  - `next_index[]`: 每个服务器下一个要发送的日志索引
  - `match_index[]`: 每个服务器已复制的最高日志索引

#### 2. **RaftService** - Raft 服务层

管理 Raft 协议的运行时：

- **长期运行的任务**：
  - **Election Timer**: 监控心跳超时，触发选举
  - **Replicators**: 每个 peer 一个，负责日志复制和心跳
  
- **RPC 处理**：
  - `handle_request_vote()`: 处理投票请求
  - `handle_append_entries()`: 处理日志追加/心跳
  
- **客户端接口**：
  - `propose_command()`: 提议新命令
  - `get_state()`: 获取节点状态

#### 3. **Storage Layer** - 持久化层

位于 [`src/raft/storage.rs`](src/raft/storage.rs)，基于 Trait 的可扩展设计：

```rust
pub trait Storage: Send + Sync + Debug {
    fn persist(&mut self, state: &PersistentState) -> io::Result<()>;
    fn restore(&self) -> io::Result<Option<PersistentState>>;
}
```

**实现**：
- `FileStorage`: 基于 JSON 文件的持久化（使用原子性写入保证 crash-safety）
- `MemoryStorage`: 用于测试的内存存储

**持久化时机**：
- 任期变更（`current_term` 增加）
- 投票（`voted_for` 设置）
- 日志变更（追加、截断）

#### 4. **Proto 定义** - gRPC 接口

位于 [`proto/raft.proto`](proto/raft.proto)，定义了：

```protobuf
service Raft {
  rpc RequestVote(RequestVoteArgsPb) returns (RequestVoteReplyPb);
  rpc AppendEntries(AppendEntriesArgsPb) returns (AppendEntriesReplyPb);
}
```

**Mapper 层** ([`src/raft/mapper.rs`](src/raft/mapper.rs))：
- 使用 `From`/`TryFrom` traits 实现 Protobuf ↔ Domain Model 转换
- 分离网络层和业务逻辑

### 关键流程

#### Leader 选举

1. Follower 超时 → 转为 Candidate
2. 增加 `current_term`，投票给自己，**持久化**
3. 并行发送 `RequestVote` RPC 给所有 peers
4. 收到多数票 → 转为 Leader
5. 立即发送心跳建立权威

#### 日志复制

1. Leader 收到客户端命令
2. 追加到本地日志，**持久化**
3. Replicators 并行发送 `AppendEntries` RPC
4. 收到多数确认 → 更新 `commit_index`
5. 通过 `ApplyMsg` channel 通知状态机应用

#### Crash 恢复

1. 节点启动时从 Storage 恢复持久化状态
2. 恢复 `current_term`、`voted_for`、`log[]`
3. 易失性状态重新初始化
4. 重新参与 Raft 协议

## 技术栈

### 核心依赖

| 库 | 版本 | 用途 |
|---|---|---|
| `tokio` | 1.0 | 异步运行时，任务调度 |
| `tonic` | 0.14 | gRPC 框架 |
| `prost` | 0.14 | Protobuf 序列化 |
| `serde` + `serde_json` | 1.0 | 持久化序列化 |
| `tracing` | * | 结构化日志 |
| `tokio-util` | 0.7 | CancellationToken（优雅关闭）|

### 构建工具

- `tonic-prost-build`: Protobuf 代码生成
- `build.rs`: 编译时生成 Rust 代码

## 快速开始

### 前置要求

- Rust 1.75+ (支持 `let-else` 语法)
- Protocol Buffers 编译器（如果修改 proto）

### 编译

```bash
cargo build --release
```

### 运行 KV 服务器示例

```bash
cargo run --example kv_example
```

这将启动一个 3 节点的 Raft 集群，执行一些 KV 操作，然后关闭。具体见[示例应用](#示例应用)


## 测试

项目包含完整的集成测试套件：

```bash
# 运行所有测试
cargo test

# 运行选举测试
cargo test election

# 运行共识测试
cargo test agree

# 单线程运行（避免日志交错）
cargo test -- --test-threads=1
```

### 测试覆盖

#### 选举测试 (`tests/election_tests.rs`)

- ✅ **初始选举**: 网络正常时能选出唯一 leader
- ✅ **重新选举**: Leader 失败后能重新选举
- ✅ **多次选举**: 多次分区和恢复后仍能维持一个 leader

#### 共识测试 (`tests/agree_tests.rs`)

- ✅ **基本共识**: 简单场景下达成共识
- ✅ **并发提交**: 多个客户端并发提交
- ✅ **失败容忍**: 少数节点失败时仍能达成共识
- ✅ **Leader 失败**: Leader 失败后继续达成共识
- ✅ **Follower 失败**: Follower 失败后仍能达成共识
- ✅ **网络分区**: 分区后恢复能达成共识
- ✅ **日志备份**: 测试 Raft 的日志修复机制

### 测试工具

位于 [`src/test_util/`](src/test_util/)：

- `TestHarness`: 测试集群管理
- `Network`: 模拟网络分区、延迟、丢包
- `LogChecker`: 验证日志一致性

## 示例应用

### KV 存储服务器

位于 [`examples/kv_example.rs`](examples/kv_example.rs)，这是一个简单的 KV 存储示例。

#### 架构

```rust
KvServer {
    service: RaftService,           // Raft 协议核心
    state_machine: HashMap,         // KV 状态机
    pending_requests: HashMap,      // 等待应用的请求
    applier_task: Task,             // 后台应用任务
}
```

#### 运行流程

1. **启动 3 个节点**，每个绑定不同端口
2. **自动选举** leader（约 500ms）
3. **执行操作**：
   - Set key1=value1
   - Set key2=value2
   - Get key1, key2, key3
4. **验证一致性**：检查所有节点状态机一致
5. **关闭子任务**：
   - 取消 applier 任务
   - 关闭 Raft 服务（停止 replicators 和 election timer）
   - 终止 gRPC 服务器

#### 查看运行结果

```bash
# 运行示例
cargo run --example kv_example

# 查看完整日志
tail -f logs/latest-kv_example.log
# 或者仅测试日志
awk '/kv_example/ {print}' logs/latest-kv_example.log

# 查看持久化文件
cat logs/kv_server_0.json
```

日志中会显示：
- Leader 选举过程
- 命令提议和复制
- 日志应用
- 所有节点状态一致性验证

## 项目结构

```
rust-raft/
├── proto/                      # Protobuf 定义
│   └── raft.proto             # Raft RPC 接口定义
├── src/
│   ├── lib.rs                 # 库入口
│   ├── raft.rs                # Raft 模块入口
│   ├── raft/
│   │   ├── core.rs           # Raft 核心逻辑（RaftCore, RaftService）
│   │   ├── client.rs         # Peer 客户端（gRPC）
│   │   ├── mapper.rs         # Proto ↔ Domain Model 转换
│   │   └── storage.rs        # 持久化层（Storage trait, FileStorage）
│   └── test_util/            # 测试工具
│       ├── check.rs          # 日志一致性检查
│       ├── logger.rs         # 日志初始化
│       ├── network.rs        # 网络模拟
│       └── server.rs         # 测试用 Raft 服务器
├── tests/                     # 集成测试
│   ├── election_tests.rs     # 选举测试
│   └── agree_tests.rs        # 共识测试
├── examples/                  # 示例应用
│   ├── kv_example.rs         # KV 服务器示例（包含 KvServer 定义）
│   └── README.md             # 示例文档
├── logs/                      # 日志和持久化文件
├── build.rs                   # 构建脚本（生成 proto 代码）
├── Cargo.toml                 # 依赖配置
└── README.md                 # 本文件
```

## 限制

- 暂不支持日志压缩和快照（论文 §7）
- 持久化使用 JSON（未来可改为 bincode）

## 参考资料

- [Raft 论文](https://raft.github.io/raft.pdf) - In Search of an Understandable Consensus Algorithm
- [MIT 6.5840](https://pdos.csail.mit.edu/6.824/) - 分布式系统课程
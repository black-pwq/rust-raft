use crate::raft::Command;
/// Mapper module for converting between protobuf network transfer objects and domain models
/// 
/// This module provides conversions between:
/// - Protobuf types (with Pb suffix) used for network serialization
/// - Domain model types (without Pb suffix) used internally

use crate::raft::core::{AppendEntriesArgs, AppendEntriesReply, LogEntry, RequestVoteArgs, RequestVoteReply};
use crate::raft::proto::{AppendEntriesArgsPb, AppendEntriesReplyPb, KvCommand, KvGet, KvSet, LogEntryPb, RequestVoteArgsPb, RequestVoteReplyPb, kv_command};

impl From<Command> for KvCommand {
    fn from(value: Command) -> Self {
        match value {
            Command::Get { key } => KvCommand {
                cmd: Some(kv_command::Cmd::Get(KvGet { key })),
            },
            Command::Set { key, value } => KvCommand {
                cmd: Some(kv_command::Cmd::Set(KvSet { key, value })),
            },
        }
    }
}

impl TryFrom<KvCommand> for Command {
    type Error = ();

    fn try_from(value: KvCommand) -> Result<Self, Self::Error> {
        match value.cmd {
            Some(kv_command::Cmd::Get(get)) => Ok(Command::Get { key: get.key }),
            Some(kv_command::Cmd::Set(set)) => Ok(Command::Set {
                key: set.key,
                value: set.value,
            }),
            None => Err(()),
        }
    }
}

// ============================================================================
// LogEntry conversions
// ============================================================================

impl From<LogEntry> for LogEntryPb {
    fn from(entry: LogEntry) -> Self {
        LogEntryPb {
            term: entry.term,
            command: entry.command.map(|cmd| cmd.into()),
        }
    }
}

impl TryFrom<LogEntryPb> for LogEntry {
    type Error = ();

    fn try_from(entry: LogEntryPb) -> Result<Self, Self::Error> {
        Ok(LogEntry {
            term: entry.term,
            command: match entry.command {
                Some(cmd) => Some(cmd.try_into()?),
                None => None,
            },
        })
    }
}

// ============================================================================
// AppendEntriesArgs conversions
// ============================================================================

impl From<AppendEntriesArgs> for AppendEntriesArgsPb {
    fn from(args: AppendEntriesArgs) -> Self {
        AppendEntriesArgsPb {
            term: args.term,
            leader_id: args.leader_id,
            prev_log_index: args.prev_log_index,
            prev_log_term: args.prev_log_term,
            entries: args.entries.into_iter().map(|e| e.into()).collect(),
            leader_commit: args.leader_commit,
        }
    }
}

impl TryFrom<AppendEntriesArgsPb> for AppendEntriesArgs {
    type Error = ();

    fn try_from(args: AppendEntriesArgsPb) -> Result<Self, Self::Error> {
        let entries: Result<Vec<LogEntry>, _> = args.entries.into_iter().map(|e| e.try_into()).collect();
        Ok(AppendEntriesArgs {
            term: args.term,
            leader_id: args.leader_id,
            prev_log_index: args.prev_log_index,
            prev_log_term: args.prev_log_term,
            entries: entries?,
            leader_commit: args.leader_commit,
        })
    }
}

// ============================================================================
// AppendEntriesReply conversions
// ============================================================================

impl From<AppendEntriesReply> for AppendEntriesReplyPb {
    fn from(reply: AppendEntriesReply) -> Self {
        AppendEntriesReplyPb {
            term: reply.term,
            success: reply.success,
        }
    }
}

impl From<AppendEntriesReplyPb> for AppendEntriesReply {
    fn from(reply: AppendEntriesReplyPb) -> Self {
        AppendEntriesReply {
            term: reply.term,
            success: reply.success,
        }
    }
}

// ============================================================================
// RequestVoteArgs conversions
// ============================================================================

impl From<RequestVoteArgs> for RequestVoteArgsPb {
    fn from(args: RequestVoteArgs) -> Self {
        RequestVoteArgsPb {
            term: args.term,
            candidate_id: args.candidate_id,
            last_log_index: args.last_log_index,
            last_log_term: args.last_log_term,
        }
    }
}

impl From<RequestVoteArgsPb> for RequestVoteArgs {
    fn from(args: RequestVoteArgsPb) -> Self {
        RequestVoteArgs {
            term: args.term,
            candidate_id: args.candidate_id,
            last_log_index: args.last_log_index,
            last_log_term: args.last_log_term,
        }
    }
}

// ============================================================================
// RequestVoteReply conversions
// ============================================================================

impl From<RequestVoteReply> for RequestVoteReplyPb {
    fn from(reply: RequestVoteReply) -> Self {
        RequestVoteReplyPb {
            term: reply.term,
            vote_granted: reply.vote_granted,
        }
    }
}

impl From<RequestVoteReplyPb> for RequestVoteReply {
    fn from(reply: RequestVoteReplyPb) -> Self {
        RequestVoteReply {
            term: reply.term,
            vote_granted: reply.vote_granted,
        }
    }
}

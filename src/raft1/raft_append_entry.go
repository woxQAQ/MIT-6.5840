package raft

import (
	"encoding/json"
	"time"

	"6.5840/raftapi"
)

type AppendEntriesArgs struct {
	// leader's term
	Term int
	// so follower can redirect clients
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of PrevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat;
	Entries []LogEntry
	// leader's commit index
	LeaderCommit int
}

func (s AppendEntriesArgs) String() string {
	res, _ := json.Marshal(s)
	return string(res)
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching
	// PrevLogIndex and PrevLogTerm
	Success bool
	// 冲突的 term
	ConflictTerm int
	// Follower 在 ConflictTerm 的第一个索引
	ConflictIndex int
}

func (s AppendEntriesReply) String() string {
	res, _ := json.Marshal(s)
	return string(res)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fail := func(reason ...string) {
		if len(reason) != 0 {
			dPrintfRaft(rf, "append entry failed due to %s", reason[0])
		}
		reply.Term, reply.Success = rf.currentTerm, false
	}

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		fail("requester term lag")
		return
	} else if args.Term >= rf.currentTerm {
		rf.switchRole(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.lastReceivedTime = time.Now()
	}

	// 检查 PrevLogIndex 是否超出范围
	if args.PrevLogIndex > len(rf.log)-1 {
		// 返回冲突信息：follower 日志太短
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		fail("PrevLogIndex too large")
		return
	}

	// 检查 term 是否匹配
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 返回冲突信息：term 不匹配
		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm

		// 找到 conflictTerm 的第一个索引
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		fail("log term mismatch")
		return
	}

	// 追加日志
	rf.log = rf.log[:args.PrevLogIndex+1]
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.applyLog()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	// 批量准备所有消息
	msgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msgs = append(msgs, raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		})
	}
	// 更新 lastApplied 后释放锁
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	// 在锁外批量发送
	for _, msg := range msgs {
		rf.applych <- msg
	}
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	dPrintfRaft(rf, "Sending Append Entries %v to peer %v", args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		dPrintfRaft(rf, "failed to send appendentries rpc to peer %d", server)
	} else {
		dPrintfRaft(rf, "receive Append Entries reply from peer %d, reply: %v", server, reply)
	}
	return ok
}

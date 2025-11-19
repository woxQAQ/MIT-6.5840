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
}

func (s AppendEntriesReply) String() string {
	res, _ := json.Marshal(s)
	return string(res)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fail := func() {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		fail()
		return
	} else if args.Term >= rf.currentTerm {
		rf.switchRole(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.lastReceivedTime = time.Now()
	}
	checkArgsMatch := func() bool {
		if args.PrevLogIndex > len(rf.log)-1 {
			return false
		}
		return args.PrevLogIndex < 0 || rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
	}

	index := args.PrevLogIndex
	if !checkArgsMatch() {
		fail()
		return
	}

	rf.log = rf.log[:index+1]
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
	msg := []raftapi.ApplyMsg{}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg = append(msg, raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		})
	}
	rf.mu.Unlock()
	for _, m := range msg {
		rf.applych <- m
		rf.mu.Lock()
		rf.lastApplied = m.CommandIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	dPrintfRaft(rf, "Sending Append Entries %v to peer %v", args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	dPrintfRaft(rf, "receive from peer %d, reply: %v", server, reply)
	return ok
}

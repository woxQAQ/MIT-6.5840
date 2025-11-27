package raft

import (
	"6.5840/raftapi"
)

func (rf *Raft) checkLogMatch(index, term int) bool {
	return index <= len(rf.log)-1 && rf.log[index].Term == term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	fail := func() {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		fail()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.switchRole(Follower)

	index := args.PrevLogIndex
	if !rf.checkLogMatch(index, args.PrevLogTerm) {
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
	rf.RLock()
	msg := []raftapi.ApplyMsg{}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg = append(msg, raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		})
	}
	rf.RUnlock()
	for _, m := range msg {
		rf.applych <- m
		rf.Lock()
		rf.lastApplied = m.CommandIndex
		rf.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	dPrintfRaft(rf, "Sending Append Entries %v to peer %v", args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	dPrintfRaft(rf, "receive from peer %d, reply: %v", server, reply)
	return ok
}

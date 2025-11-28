package raft

import (
	"6.5840/raftapi"
)

func (rf *Raft) checkLogMatch(index, term int) bool {
	return index <= rf.getLastLogIndex() && rf.logs[index].Term == term
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
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.switchRole(Follower)
	rf.electionTicker.Reset(randomElectionTimeout())

	index := args.PrevLogIndex
	if !rf.checkLogMatch(index, args.PrevLogTerm) {
		fail()
		lastLogIndex := rf.getLastLogIndex()
		if lastLogIndex < args.PrevLogIndex {
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = -1
		} else {
			index := args.PrevLogIndex
			for index >= 0 && rf.logs[index].Term == args.PrevLogTerm {
				index--
			}
			reply.ConflictIndex = index + 1
			reply.ConflictTerm = args.PrevLogTerm
		}
		return
	}

	rf.logs = rf.logs[:index+1]
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		// TODO: apply log
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
			Command:      rf.logs[i].Command,
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

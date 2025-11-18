package raft

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

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching
	// PrevLogIndex and PrevLogTerm
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.switchRole(Follower)

	reply.Term = rf.currentTerm
	reply.Success = true
	//TODO: implement append entries
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

package raft

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.Lock()
	defer rf.Unlock()
	fail := func() {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	termDelay := args.Term < rf.currentTerm
	hasVotedForOthers := args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId

	if termDelay || hasVotedForOthers {
		fail()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.switchRole(Follower)
		rf.persist()
	}

	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fail()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTicker.Reset(randomElectionTimeout())
}

// NOTE: unsafe without lock
func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastIndex := rf.getLastLogIndex()
	return term > rf.logs[lastIndex].Term || (term == rf.logs[lastIndex].Term && index >= lastIndex)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
//
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

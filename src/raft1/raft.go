package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	sync.RWMutex                     // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *tester.Persister   // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	applych      chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role raftState

	// persistent state on all servers
	//
	// latest term server has seen
	// initialized to 0 on first boot,
	// increases monotonically
	currentTerm int
	// candidateId that received vote
	// in current term (or null if none)
	votedFor int
	// logs entries; each entry contains command for state machine,
	// and term when entry was received by leader
	// first index is 1
	logs []LogEntry

	// volatile state on all servers
	//
	// index of highest log entry known to be committed
	// initialized to 0, increases monotonically
	commitIndex int
	// index of highest log entry applied to state machine
	// initialized to 0, increases monotonically
	lastApplied int

	// volatile state on leaders
	//
	// for each server, index of the next log entry
	// to send to that server
	// initialized to leader last log index + 1
	nextIndex []int
	// for each server, index of highest log entry
	// known to be replicated on server
	// initialized to 0, increases monotonically
	matchIndex []int

	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.RLock()
	defer rf.RUnlock()

	term = rf.currentTerm
	isleader = (rf.role == Leader)
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed.
// the second return value is the current
// term.
// the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).
	rf.Lock()
	defer rf.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, entry)
	rf.persist()

	return len(rf.logs) - 1, rf.currentTerm, rf.role == Leader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartBeat() {
	sendAppendEntriesToPeer := func(peer int) {
		rf.RLock()
		if rf.role != Leader {
			rf.RUnlock()
			return
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		var prevLogTerm int
		if prevLogIndex >= 0 && prevLogIndex < len(rf.logs) {
			prevLogTerm = rf.logs[prevLogIndex].Term
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      slices.Clone(rf.logs[rf.nextIndex[peer]:]),
			LeaderCommit: rf.commitIndex,
		}
		rf.RUnlock()
		reply := AppendEntriesReply{}
		if !rf.sendAppendEntries(peer, &args, &reply) {
			return
		}
		rf.Lock()
		defer rf.Unlock()
		if rf.role != Leader || args.Term != rf.currentTerm {
			return
		}
		handleConflictTerm := func() {
			index := rf.nextIndex[peer] - 1
			for index >= 0 && rf.logs[index].Term > reply.ConflictTerm {
				index--
			}
			if index >= 0 && rf.logs[index].Term == reply.ConflictTerm {
				rf.nextIndex[peer] = index + 1
				return
			}
		}
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.switchRole(Follower)
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					handleConflictTerm()
				}
			}
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// Find the highest index that is committed by majority
		newCommitIndex := rf.commitIndex
		for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
			count := 1 // leader counts
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				newCommitIndex = i
				break
			}
		}

		if newCommitIndex > rf.commitIndex {
			if rf.checkLogMatch(newCommitIndex, rf.currentTerm) {
				rf.commitIndex = newCommitIndex
				go rf.applyLog()
			}
		}
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go sendAppendEntriesToPeer(i)
	}
}

func (rf *Raft) switchRole(role raftState) {
	if rf.role == role {
		return
	}
	switch role {
	case Leader:
		// Initialize leader state
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.heartbeatTicker.Reset(heartbeatTimeout())
		rf.electionTicker.Stop()
	case Follower:
		rf.electionTicker.Reset(randomElectionTimeout())
		rf.heartbeatTicker.Stop()
	case Candidate:
	}
	rf.role = role
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTicker.C:
			rf.Lock()
			rf.switchRole(Candidate)
			rf.currentTerm++
			rf.persist()
			rf.election()
			rf.electionTicker.Reset(randomElectionTimeout())
			rf.Unlock()
		case <-rf.heartbeatTicker.C:
			rf.Lock()
			if rf.role == Leader {
				// dPrintfRaft(rf, "heartbeat comes")
				go rf.heartBeat()
				rf.heartbeatTicker.Reset(heartbeatTimeout())
			}
			rf.Unlock()
		}
	}
}

func (rf *Raft) election() {
	rf.votedFor = rf.me
	rf.persist()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.logs[rf.getLastLogIndex()].Term,
	}
	// number of votes received
	voted := 1
	sendVoteReq2Peer := func(peer int) {
		reply := &RequestVoteReply{}
		if rf.sendRequestVote(peer, args, reply) {
			rf.Lock()
			defer rf.Unlock()
			// fast fallback when leader is elected
			if rf.role == Leader {
				return
			}
			// NOTE: it's impossible to receive
			// vote granted from a higher term
			if rf.currentTerm == args.Term && rf.role == Candidate && reply.VoteGranted {
				voted += 1
				// check majority
				if voted > len(rf.peers)/2 {
					rf.switchRole(Leader)
					rf.heartBeat()
				}
			} else if reply.Term > rf.currentTerm {
				// someone else becomes leader
				rf.currentTerm = reply.Term
				rf.switchRole(Follower)
				rf.votedFor = -1
				rf.persist()
			}
		}
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go sendVoteReq2Peer(peer)
	}
}

// the service or tester wants to create a Raft server.
//
// the ports of all the Raft servers
// (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
//
// persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any.
//
// applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		applych:         applyCh,
		logs:            []LogEntry{{Term: 0}}, // log index starts from 1
		matchIndex:      make([]int, len(peers)),
		nextIndex:       make([]int, len(peers)),
		votedFor:        -1,
		role:            Follower,
		electionTicker:  time.NewTicker(randomElectionTimeout()),
		heartbeatTicker: time.NewTicker(heartbeatTimeout()),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLogIndex()+1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	// go func() {
	// 	if Debug {
	// 		timer := time.NewTicker(time.Second)
	// 		for range timer.C {
	// 			DPrintf("Raft %d is in %s role\n", rf.me, rf.role)
	// 		}
	// 	}
	// }()

	return rf
}

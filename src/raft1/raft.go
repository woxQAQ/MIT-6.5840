package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Command any
	Term    int
}

type raftState int

func (s raftState) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return ""
	}
}

const (
	Leader raftState = iota
	Follower
	Candidate

	_HEART_BEAT_PER_SEC_  = 8
	_HEART_BEAT_INTERVAL_ = time.Second / _HEART_BEAT_PER_SEC_
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applych   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role             raftState
	lastReceivedTime time.Time
	heartbeatCh      chan struct{}

	// persistent state on all servers
	//
	// latest term server has seen
	// initialized to 0 on first boot,
	// increases monotonically
	currentTerm int
	// candidateId that received vote
	// in current term (or null if none)
	votedFor int
	// log entries; each entry contains command for state machine,
	// and term when entry was received by leader
	// first index is 1
	log []LogEntry

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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	entry := LogEntry{
		Command: command,
		Term:    term,
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, entry)

	return len(rf.log) - 1, term, isLeader
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
	select {
	case <-rf.heartbeatCh:
		return
	default:
		_, isleader := rf.GetState()
		if !isleader {
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[peer] - 1
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					Entries:      slices.Clone(rf.log[rf.nextIndex[peer]:]),
					LeaderCommit: rf.commitIndex,
				}
				if prevLogIndex >= 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if !rf.sendAppendEntries(peer, &args, &reply) {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.lastReceivedTime = time.Now()
				if rf.role != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchRole(Follower)
					return
				}
				if !reply.Success {
					if rf.nextIndex[peer] > 0 {
						rf.nextIndex[peer]--
					}
					return
				}
				rf.nextIndex[peer] = len(rf.log)
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)

				for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
					count := 1
					if rf.log[n].Term != rf.currentTerm {
						break
					}
					for i := range len(rf.peers) {
						if i == rf.me {
							continue
						}
						if rf.matchIndex[i] >= n {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = n
						go rf.applyLog()
						break
					}
				}
			}(i)
		}
		time.Sleep(_HEART_BEAT_INTERVAL_)
	}
}

func (rf *Raft) switchRole(role raftState) {
	switch role {
	case Leader:
		rf.heartbeatCh = make(chan struct{})
		for i := range rf.peers {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.log)
		}
		go func() {
			for !rf.killed() {
				rf.heartBeat()
			}
		}()
		// dPrintfRaft(rf, "switch to leader")
		// go rf.heartBeat()
	case Follower:
		if rf.role == Leader {
			close(rf.heartbeatCh)
		}
	case Candidate:
	}
	rf.role = role
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		ms := 600 + rand.Int63()%300

		rf.mu.Lock()
		lastTime := rf.lastReceivedTime
		isLeader := rf.role == Leader
		rf.mu.Unlock()
		if !isLeader && time.Since(lastTime) > time.Duration(ms)*time.Millisecond {
			rf.election()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms = 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.switchRole(Candidate)
	rf.mu.Unlock()
	// rf.lastReceivedTime = time.Now()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	vote := 1

	dPrintfRaft(rf, "Start Election")
	sendVoteReq2Peer := func(peer int) {
		reply := RequestVoteReply{}
		if rf.sendRequestVote(peer, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role == Candidate && reply.VoteGranted {
				vote += 1
				if vote > len(rf.peers)/2 {
					rf.switchRole(Leader)
					return
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.switchRole(Follower)
				rf.votedFor = -1
				return
			}
		}
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go sendVoteReq2Peer(peer)
		time.Sleep(10 * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applych = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.log = []LogEntry{{Term: 0}} // log index starts from 1
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.votedFor = -1
	rf.role = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

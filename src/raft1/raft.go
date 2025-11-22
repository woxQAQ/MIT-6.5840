package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
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

	_HEART_BEAT_PER_SEC_  = 10
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
	// 优化：为每个 follower 创建长期运行的 worker，避免频繁创建 goroutine
	replicateTickers []*time.Ticker

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

	// 优化：缓存每个 term 的第一个索引，用于快速查找
	// key: term, value: first index with this term
	termToFirstIndex map[int]int

	// 优化：sync.Pool 用于复用切片，减少 GC 压力
	applyMsgPool sync.Pool
	entriesPool  sync.Pool
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

	// 记录新 term 的第一个索引
	if _, ok := rf.termToFirstIndex[term]; !ok {
		rf.termToFirstIndex[term] = len(rf.log)
	}

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
				rf.doAppendEntries(peer)
			}(i)
		}
	}
}

func (rf *Raft) leaderLoop() {
	// 初始化 replicateTickers
	rf.mu.Lock()
	rf.replicateTickers = make([]*time.Ticker, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.replicateTickers[i] = time.NewTicker(_HEART_BEAT_INTERVAL_)
		// 为每个 follower 启动 long-running worker
		go func(peer int, ticker *time.Ticker) {
			defer ticker.Stop()
			for !rf.killed() {
				select {
				case <-ticker.C:
					rf.doAppendEntries(peer)
				case <-rf.heartbeatCh:
					return
				}
			}
		}(i, rf.replicateTickers[i])
	}
	rf.mu.Unlock()

	// leaderLoop 只需要等待退出信号
	<-rf.heartbeatCh

	// 清理 tickers（worker 会自己停止自己的 ticker）
	rf.mu.Lock()
	rf.replicateTickers = nil
	rf.mu.Unlock()
}

func (rf *Raft) doAppendEntries(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1

	// 边界检查：确保 nextIndex 不会越界
	if prevLogIndex < 0 {
		prevLogIndex = 0
		rf.nextIndex[peer] = 1
	}
	if rf.nextIndex[peer] > len(rf.log) {
		rf.nextIndex[peer] = len(rf.log)
	}

	// 从 pool 获取 entries 切片
	entries := rf.entriesPool.Get().([]LogEntry)
	entries = entries[:0] // 重置切片

	// 复制需要的日志条目
	needed := rf.log[rf.nextIndex[peer]:]
	if cap(entries) < len(needed) {
		// 容量不够，重新分配
		entries = make([]LogEntry, len(needed))
		copy(entries, needed)
	} else {
		// 容量足够，扩展切片
		entries = entries[:len(needed)]
		copy(entries, needed)
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	if prevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)

	// RPC 完成后，将 entries 切片放回 pool
	// 注意：要先做重置，然后放回
	entries = entries[:0]
	rf.entriesPool.Put(entries)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchRole(Follower)
		return
	}
	if rf.role != Leader {
		return
	}
	if !reply.Success {
		// 使用冲突信息快速回退 nextIndex
		rf.nextIndex[peer] = rf.findConflictIndex(reply.ConflictTerm, reply.ConflictIndex)
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		return
	}
	rf.nextIndex[peer] = len(rf.log)
	rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)

	newMatch := rf.matchIndex[peer]
	// 如果新的 matchIndex 没有超过当前的 commitIndex，不可能提交新日志
	if newMatch <= rf.commitIndex {
		return
	}

	// 只检查从 commitIndex+1 到 newMatch 的区间
	start := rf.commitIndex + 1
	end := newMatch
	if end >= len(rf.log) {
		end = len(rf.log) - 1
	}

	for n := start; n <= end; n++ {
		// 只有当前 term 的日志才能被提交（Raft 论文 5.4.2）
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 1
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
		}
	}
}

// 找到冲突的索引
// 使用 termToFirstIndex 缓存，O(1) 查找
// 如果 leader 有 ConflictTerm，返回 leader 中该 term 的第一个索引
// 否则直接返回 ConflictIndex
func (rf *Raft) findConflictIndex(conflictTerm int, conflictIndex int) int {
	if conflictTerm > 0 {
		// 使用缓存快速查找
		if idx, ok := rf.termToFirstIndex[conflictTerm]; ok {
			return idx
		}
	}
	// 如果 leader 没有这个 term，直接返回 conflictIndex
	return conflictIndex
}

func (rf *Raft) switchRole(role raftState) {
	switch role {
	case Leader:
		rf.heartbeatCh = make(chan struct{})
		for i := range rf.peers {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.log)
		}
		go rf.leaderLoop()
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
	// rf.lastReceivedTime = time.Now()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	vote := 1
	var wg sync.WaitGroup

	dPrintfRaft(rf, "Start Election")
	rf.mu.Unlock()

	// 并行发送 vote 请求，而不是按顺序发送
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 如果已经不再是 candidate，直接返回
				if rf.role != Candidate {
					return
				}

				if reply.VoteGranted {
					vote++
					if vote > len(rf.peers)/2 {
						rf.switchRole(Leader)
					}
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchRole(Follower)
					rf.votedFor = -1
				}
			}
		}(peer)
	}

	// 等待一段时间让投票完成，但不要无限等待
	// 这可以避免某个 peer 无响应时阻塞整个选举
	go func() {
		time.Sleep(200 * time.Millisecond)
		wg.Wait()
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

	// 初始化 termToFirstIndex 缓存
	rf.termToFirstIndex = make(map[int]int)
	rf.termToFirstIndex[0] = 0 // term 0 从索引 0 开始

	// 初始化 replicateTickers（在成为 leader 时实际创建）
	rf.replicateTickers = nil

	// 初始化 sync.Pool
	rf.applyMsgPool = sync.Pool{
		New: func() interface{} {
			return make([]raftapi.ApplyMsg, 0, 128)
		},
	}
	rf.entriesPool = sync.Pool{
		New: func() interface{} {
			return make([]LogEntry, 0, 128)
		},
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

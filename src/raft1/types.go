package raft

// Raft type definition

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
)

// RPC Definitions
// Append Entries RPC
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
	return marshalJson(s)
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching
	// PrevLogIndex and PrevLogTerm
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (s AppendEntriesReply) String() string {
	return marshalJson(s)
}

// RequestVote RPC
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// Current term of candidate
	Term int
	// Candidate requesting vote
	CandidateId int
	// Index of candidate's last log entry
	LastLogIndex int
	// Term of candidate's last log entry
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Current term, for candidate to update itself
	Term int
	// True means candidate received vote
	VoteGranted bool
}

func (s RequestVoteArgs) String() string {
	return marshalJson(s)
}

func (s RequestVoteReply) String() string {
	return marshalJson(s)
}

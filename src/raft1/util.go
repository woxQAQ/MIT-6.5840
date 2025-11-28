package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

const (
	_HEART_BEAT_PER_SEC_  = 8
	_HEART_BEAT_INTERVAL_ = time.Second / _HEART_BEAT_PER_SEC_
	_ELECTION_TIMEOUT_    = 600
)

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func dPrintfRaft(rf *Raft, format string, a ...any) {
	format = fmt.Sprintf("[Server=%d,role=%s,term=%d]", rf.me, rf.role, rf.currentTerm) + " " + format
	DPrintf(format, a...)
}

func randomElectionTimeout() time.Duration {
	return time.Duration(_ELECTION_TIMEOUT_+rand.Int63()%300) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	return _HEART_BEAT_INTERVAL_
}

func marshalJson(data any) string {
	res, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}
	return string(res)
}

package raft

import (
	"fmt"
	"log"
)

// Debugging
var (
	Debug = false
	PPROF = false

	Server = -1
)

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func dPrintfRaft(rf *Raft, format string, a ...any) {
	if Server != -1 && rf.me != Server {
		return
	}
	format = fmt.Sprintf("[Server=%d,role=%s,term=%d]", rf.me, rf.role, rf.currentTerm) + " " + format
	DPrintf(format, a...)
}

package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func dPrintfRaft(rf *Raft, format string, a ...any) {
	format = fmt.Sprintf("[Server=%d,role=%s,term=%d]", rf.me, rf.role, rf.currentTerm) + " " + format
	DPrintf(format, a...)
}

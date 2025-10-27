package mr

import "time"

type taskStatus int

const (
	taskStatusRunning taskStatus = iota
	taskStatusPending
	taskStatusFinish
)

// +---------+      											+---------+
// |				 |    allocate to worker			|					|
// | pending |		----------------->			| running |
// |				 |		<-----------------		  |         |
// +---------+		 worker timeout   			+---------+

type task struct {
	filename  string
	id        int
	startTime time.Time
	taskStatus
}

type controlMessage[T any] struct {
	data T
	ok   chan struct{}
}

func newControlMessage[T any](v T) controlMessage[T] {
	return controlMessage[T]{
		ok:   make(chan struct{}),
		data: v,
	}
}

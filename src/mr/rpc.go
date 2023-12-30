package mr

import (
	"os"
	"strconv"
)

type EmptyArgs struct{}

type EmptyReply struct{}

type GetMapTaskReply struct {
	TaskId   int
	NoTasks  bool
	Filename string
	NReduce  int
}

type GetReduceTaskReply struct {
	TaskId      int
	NoTasks     bool
	ReduceIndex int
}

type CompleteTaskArgs struct {
	TaskId int
}

type ShouldExitReply struct {
	ShouldExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

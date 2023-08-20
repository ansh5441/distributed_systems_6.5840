package mr

import (
	"os"
	"strconv"
)

type Args struct {
	Query         string
	JobDoneType   string
	FileName      string
	MapTaskNum    int
	ReduceTaskNum int
}

type Reply struct {
	FileName      string
	FileNames     []string
	Command       string
	NReduce       int
	MapTaskNum    int
	ReduceTaskNum int
	Done          bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskReq struct {
}

type GetTaskResp struct {
	Id       int
	Filename string
	TaskType int
	NReduce  int
	NMap     int
}

type FinishNoticeReq struct {
	Id                   int
	TaskType             int
	IntermediateFilename []string
}

type FinishNoticeResp struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

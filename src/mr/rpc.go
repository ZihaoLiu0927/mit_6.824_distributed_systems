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

type RequestType int

const (
	RequestForJob RequestType = 0
	ReportFinish  RequestType = 1
)

type JobType int

const (
	Map    JobType = 0
	Reduce JobType = 1
)

type ReportInfo struct {
	JobTp   JobType
	JobId   int
	JobName string
}

type RequestArgs struct {
	ReqType RequestType
	Info    ReportInfo
}

type RequestReply struct {
	File    string
	JobType JobType
	JobId   int
	NReduce int
	MapRes  []int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

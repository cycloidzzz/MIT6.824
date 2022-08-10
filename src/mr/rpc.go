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

// Add your RPC definitions here.
// Mapper request
type MapperTaskRequest struct {
	CompletedMapperTaskId int
}

// Mapper response
type MapperTaskResponse struct {
	MapperTaskName string
	MapperTaskId   int
	NumReduceTask  int
	ShouldExit     bool
}

type ReducerTaskRequest struct {
	CompletedReducerTaskId int
}

type ReducerTaskResponse struct {
	ReducerTaskId int
	NumMapperTask int
	ShouldExit    bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

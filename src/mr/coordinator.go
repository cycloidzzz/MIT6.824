package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int32

const (
	kTaskIdle       TaskStatus = 0
	kTaskInProgress TaskStatus = 1
	kTaskComplete   TaskStatus = 2
)

type Coordinator struct {
	// Your definitions here.
	// map tasks
	MapTaskMutex    sync.Mutex
	CurMapTask      int
	NumMapTask      int
	NumCompleteTask int
	MapTaskList     []string

	// reduce tasks
	ReduceTaskMutex sync.Mutex
	CurReduceTask   int
	NumReduceTask   int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Mapper(args *MapperTaskRequest, reply *MapperTaskResponse) error {
	c.MapTaskMutex.Lock()
	defer c.MapTaskMutex.Unlock()
	if c.CurMapTask == c.NumMapTask {
		// TODO: (cycloidz) Notify the waiting reducer to get to work.
		reply.ShouldExit = true
	} else {
		curTask := c.CurMapTask
		c.CurMapTask += 1

		mapperTaskName := c.MapTaskList[curTask]

		reply.MapperTaskName = mapperTaskName
		reply.MapperTaskId = curTask
		reply.NumReduceTask = c.NumReduceTask
		reply.ShouldExit = false
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

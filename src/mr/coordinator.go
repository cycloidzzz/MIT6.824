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

type JobStatusType int32

const (
	kJobInit        JobStatusType = 0
	kJobMapPhase    JobStatusType = 1
	kJobReducePhase JobStatusType = 2
	kJobComplete    JobStatusType = 3
)

type Coordinator struct {
	// Your definitions here.
	//JobMutex  sync.Mutex
	JobCond   *sync.Cond
	JobStatus JobStatusType

	// map tasks
	MapTaskMutex       sync.Mutex
	CurMapTask         int
	NumMapTask         int
	NumCompleteMapTask int
	MapTaskList        []string

	// reduce tasks
	ReduceTaskMutex       sync.Mutex
	CurReduceTask         int
	NumReduceTask         int
	NumCompleteReduceTask int
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
	c.JobCond.L.Lock()
	defer c.JobCond.L.Unlock()

	// FIXME: (cycloidz) fault tolerance ...

	// FIXME: (cycloiodz) should deduplicate with map or something else ...
	if args.CompletedMapperTaskId != -1 {
		c.NumCompleteMapTask += 1
	}

	if c.CurMapTask == c.NumMapTask {
		// fmt.Println("The current Completed Map Task = ", c.NumCompleteMapTask)
		if c.NumCompleteMapTask == c.NumMapTask {
			c.JobStatus = kJobReducePhase
			// fmt.Println("The Coordinator is now in Reduce Phase.")
			// Broadcast the waiting reducer to get to work.
			c.JobCond.Broadcast()
		}
		reply.ShouldExit = true

	} else {
		curTask := c.CurMapTask
		c.CurMapTask += 1

		if curTask == 0 {
			c.JobStatus = kJobMapPhase
		}

		mapperTaskName := c.MapTaskList[curTask]

		reply.MapperTaskName = mapperTaskName
		reply.MapperTaskId = curTask
		reply.NumReduceTask = c.NumReduceTask
		reply.ShouldExit = false
	}
	return nil
}

func (c *Coordinator) Reducer(args *ReducerTaskRequest, reply *ReducerTaskResponse) error {
	c.JobCond.L.Lock()
	defer c.JobCond.L.Unlock()

	for c.JobStatus != kJobReducePhase {
		c.JobCond.Wait()
	}
	// fmt.Println("Now the reducer can run now.")

	// FIXME: (cycloidz) deduplication and fault tolerance
	if args.CompletedReducerTaskId != -1 {
		c.NumCompleteReduceTask += 1
	}

	if c.CurReduceTask == c.NumReduceTask {

		if c.NumCompleteReduceTask == c.NumReduceTask {
			c.JobStatus = kJobComplete
		}

		reply.ShouldExit = true
	} else {

		curTask := c.CurReduceTask
		c.CurReduceTask += 1

		reply.ReducerTaskId = curTask
		reply.NumMapperTask = c.NumMapTask
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
	{
		c.JobCond.L.Lock()
		defer c.JobCond.L.Unlock()

		if c.JobStatus == kJobComplete {
			ret = true
		}
	}

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
	// Set up Job Status
	c.JobStatus = kJobInit
	c.JobCond = sync.NewCond(&sync.Mutex{})

	// Set up mapper tasks.  c.CurMapTask = 0
	c.NumMapTask = len(files)
	c.NumCompleteMapTask = 0

	for _, file := range files {
		c.MapTaskList = append(c.MapTaskList, file)
	}

	// Set up Reduce tasks.
	c.CurReduceTask = 0
	c.NumReduceTask = nReduce
	c.NumCompleteReduceTask = 0

	c.server()
	return &c
}

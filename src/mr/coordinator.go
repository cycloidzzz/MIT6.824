package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatusType int32

const (
	kTaskInit       TaskStatusType = 0
	kTaskInProgress TaskStatusType = 1
	kTaskFailed     TaskStatusType = 2
	kTaskComplete   TaskStatusType = 3
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

	// Fault detection
	MapTaskIds       []int
	MapTaskStatus    []TaskStatusType
	MapTaskInitTimes []time.Time

	// reduce tasks
	ReduceTaskMutex       sync.Mutex
	CurReduceTask         int
	NumReduceTask         int
	NumCompleteReduceTask int

	// Reduce Task fault detectin
	ReduceTaskIds       []int
	ReduceTaskStatus    []TaskStatusType
	ReduceTaskInitTimes []time.Time
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

	//fmt.Println("Called by Mapper worker.")

	// FIXME: (cycloidz) fault tolerance ...

	// FIXME: (cycloiodz) should deduplicate with map or something else ...
	if args.CompletedMapperTaskId != -1 {
		taskId := args.CompletedMapperTaskId
		if c.MapTaskStatus[taskId] == kTaskInProgress {
			c.NumCompleteMapTask += 1
			c.MapTaskStatus[taskId] = kTaskComplete

			if c.NumCompleteMapTask == c.NumMapTask {
				c.JobStatus = kJobReducePhase
				c.JobCond.Broadcast()
			}
		}
	}

	for len(c.MapTaskIds) == 0 &&
		c.JobStatus == kJobMapPhase {
		c.JobCond.Wait()
	}

	if len(c.MapTaskIds) > 0 {

		curTask := c.MapTaskIds[0]
		c.MapTaskIds = append([]int{}, c.MapTaskIds[1:]...)

		//fmt.Println("Current index: ", curTask)
		c.MapTaskStatus[curTask] = kTaskInProgress
		// firing the clock.
		c.MapTaskInitTimes[curTask] = time.Now()

		mapperTaskName := c.MapTaskList[curTask]
		reply.MapperTaskName = mapperTaskName
		reply.MapperTaskId = curTask
		reply.NumReduceTask = c.NumReduceTask
		reply.ShouldExit = false
	} else {
		//fmt.Println("now the Mapper Should Exit!")
		reply.ShouldExit = true
	}

	//if c.CurMapTask == c.NumMapTask {
	// //fmt.Println("The current Completed Map Task = ", c.NumCompleteMapTask)
	//	if c.NumCompleteMapTask == c.NumMapTask {
	//		c.JobStatus = kJobReducePhase
	// //fmt.Println("The Coordinator is now in Reduce Phase.")
	// Broadcast the waiting reducer to get to work.
	//		c.JobCond.Broadcast()
	//}
	//	reply.ShouldExit = true

	//} else {

	//		curTask := c.CurMapTask
	//		if len(c.MapTaskIds) > 0 {
	//			curTask = c.MapTaskIds[0]
	//			c.MapTaskIds = append([]int{}, c.MapTaskIds[1:]...)
	//		} else {
	//			c.CurMapTask += 1
	//		}
	//
	//
	//		//fmt.Println("Current index: ", curTask)
	//		c.MapTaskStatus[curTask] = kTaskInProgress
	//		// firing the clock.
	//		c.MapTaskInitTimes[curTask] = time.Now()
	//		mapperTaskName := c.MapTaskList[curTask]
	//		reply.MapperTaskName = mapperTaskName
	//		reply.MapperTaskId = curTask
	//		reply.NumReduceTask = c.NumReduceTask
	//		reply.ShouldExit = false
	//	}
	return nil
}

func (c *Coordinator) Reducer(args *ReducerTaskRequest, reply *ReducerTaskResponse) error {
	c.JobCond.L.Lock()
	defer c.JobCond.L.Unlock()

	for c.JobStatus == kJobMapPhase {
		c.JobCond.Wait()
	}
	// //fmt.Println("Now the reducer can run now.")

	// FIXME: (cycloidz) deduplication and fault tolerance
	if args.CompletedReducerTaskId != -1 {
		taskId := args.CompletedReducerTaskId
		if c.ReduceTaskStatus[taskId] == kTaskInProgress {
			c.ReduceTaskStatus[taskId] = kTaskComplete
			c.NumCompleteReduceTask += 1
			if c.NumCompleteReduceTask == c.NumReduceTask {
				//fmt.Println("THe job is complete!")
				c.JobStatus = kJobComplete
				//c.JonCond.Broadcast()
			}
		}
	}

	for len(c.ReduceTaskIds) == 0 &&
		c.JobStatus == kJobReducePhase {
		c.JobCond.Wait()
	}

	if len(c.ReduceTaskIds) > 0 {
		curTask := c.ReduceTaskIds[0]
		c.ReduceTaskIds = append([]int{}, c.ReduceTaskIds[1:]...)
		//fmt.Println("current index", curTask)

		c.ReduceTaskStatus[curTask] = kTaskInProgress
		c.ReduceTaskInitTimes[curTask] = time.Now()

		reply.ReducerTaskId = curTask
		reply.NumMapperTask = c.NumMapTask
		reply.ShouldExit = false

	} else {

		//fmt.Println("Now the reducer exiting!")
		reply.ShouldExit = true
	}

	//	if c.CurReduceTask == c.NumReduceTask {
	//
	//		if c.NumCompleteReduceTask == c.NumReduceTask {
	//			c.JobStatus = kJobComplete
	//		}
	//
	//		reply.ShouldExit = true
	//	} else {
	//
	//		curTask := c.CurReduceTask
	//		if len(c.MapTaskIds) > 0 {
	//			curTask = c.ReduceTaskIds[0]
	//			c.ReduceTaskIds = append([]int{}, c.ReduceTaskIds[1:]...)
	//		} else {
	//			c.CurReduceTask += 1
	//		}
	//		//fmt.Println("current index", curTask)
	//
	//		c.ReduceTaskStatus[curTask] = kTaskInProgress
	//		c.ReduceTaskInitTimes[curTask] = time.Now()
	//
	//		reply.ReducerTaskId = curTask
	//		reply.NumMapperTask = c.NumMapTask
	//		reply.ShouldExit = false
	//
	//	}
	return nil
}

func (c *Coordinator) detectFailTask() {
	go func() {
		for true {
			time.Sleep(time.Second)
			c.JobCond.L.Lock()

			nowTime := time.Now()
			if c.JobStatus == kJobComplete {
				c.JobCond.L.Unlock()
				//fmt.Println("Fault detector is now exiting.")
				break
			} else if c.JobStatus == kJobMapPhase {
				// Map task fault detection.
				for i := 0; i < c.NumMapTask; i++ {
					if c.MapTaskStatus[i] == kTaskInProgress &&
						nowTime.Sub(c.MapTaskInitTimes[i]) > 10*time.Second {
						//fmt.Println("Fail map task", i)
						// Mark as Fail task, require reinitialize.
						// TODO: directly mark as init, instead of fail?
						c.MapTaskStatus[i] = kTaskFailed
						// Enqueue into the fail task list.
						c.MapTaskIds = append(c.MapTaskIds, i)
						c.JobCond.Broadcast()
					}
				}
			} else if c.JobStatus == kJobReducePhase {
				// Reduce task fault detection.
				for i := 0; i < c.NumReduceTask; i++ {
					if c.ReduceTaskStatus[i] == kTaskInProgress &&
						nowTime.Sub(c.ReduceTaskInitTimes[i]) > 10*time.Second {
						//fmt.Println("Fail reducer task", i)
						// Mark as Fail task, require reinitialize.
						c.ReduceTaskStatus[i] = kTaskFailed
						// Enqueue into the fail task list.
						c.ReduceTaskIds = append(c.ReduceTaskIds, i)
						c.JobCond.Broadcast()
					}
				}
			}
			c.JobCond.L.Unlock()
		}
	}()
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
	c.JobCond.L.Lock()
	defer c.JobCond.L.Unlock()

	if c.JobStatus == kJobComplete {
		//fmt.Println("Now the Coordinator should exit")
		ret = true
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
	c.JobStatus = kJobMapPhase
	c.JobCond = sync.NewCond(&sync.Mutex{})

	// Set up mapper tasks.
	c.CurMapTask = 0
	c.NumMapTask = len(files)
	c.NumCompleteMapTask = 0

	for index, file := range files {
		c.MapTaskList = append(c.MapTaskList, file)
		c.MapTaskIds = append(c.MapTaskIds, index)
	}

	// Set up fail detector for Map task.
	c.MapTaskStatus = make([]TaskStatusType, c.NumMapTask)
	c.MapTaskInitTimes = make([]time.Time, c.NumMapTask)

	// Set up Reduce tasks.
	c.CurReduceTask = 0
	c.NumReduceTask = nReduce
	c.NumCompleteReduceTask = 0

	for i := 0; i < nReduce; i++ {
		c.ReduceTaskIds = append(c.ReduceTaskIds, i)
	}

	// Set up fail detector for Reduce task.
	c.ReduceTaskStatus = make([]TaskStatusType, c.NumReduceTask)
	c.ReduceTaskInitTimes = make([]time.Time, c.NumReduceTask)

	c.server()
	c.detectFailTask()

	return &c
}

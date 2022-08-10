package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		lastMapperId := -1
		for true {
			reply := CallMapper(lastMapperId)
			if reply.ShouldExit {
				break
			}
			filename := reply.MapperTaskName
			taskId := reply.MapperTaskId
			numReduce := reply.NumReduceTask

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Cannot open %v", filename)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Cannot read %v", filename)
			}
			file.Close()

			kvArray := mapf(filename, string(content))

			outNames := make([]string, numReduce)
			outFiles := make([]*os.File, numReduce)
			reducerBuffers := make([][]KeyValue, numReduce)
			for i := 0; i < numReduce; i++ {
				outNames[i] = fmt.Sprint("mr-", taskId, "-", i)
				ofile, err := os.Create(outNames[i])
				if err != nil {
					log.Fatalf("Cannot create file %v", outNames[i])
				}
				outFiles[i] = ofile
			}

			// Strat our mapping job.
			for _, kv := range kvArray {
				reduceIndex := ihash(kv.Key) % numReduce
				reducerBuffers[reduceIndex] = append(reducerBuffers[reduceIndex], kv)
			}

			// Write to the corresponding intermediate files.
			for i := 0; i < numReduce; i++ {
				encoder := json.NewEncoder(outFiles[i])
				for _, kv := range reducerBuffers[i] {
					err := encoder.Encode(&kv)
					if err != nil {
						log.Fatalf("Cannot encode %v", kv)
					}
				}
			}

			// Close the open files.
			for i := 0; i < numReduce; i++ {
				outFiles[i].Close()
			}

			lastMapperId = taskId
		}
		// fmt.Println("Mapper Worker is now exiting.")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		lastTaskId := -1

		for true {
			reply := CallReducer(lastTaskId)

			if reply.ShouldExit {
				break
			}

			// Start reducer task
			reducerId := reply.ReducerTaskId
			numMapTask := reply.NumMapperTask

			var reducerBuffer []KeyValue

			for i := 0; i < numMapTask; i++ {
				ifilename := fmt.Sprint("mr-", i, "-", reducerId)
				ifile, err := os.Open(ifilename)
				if err != nil {
					log.Fatalf("Reducer: cannot open file %v", ifilename)
				}

				decoder := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					reducerBuffer = append(reducerBuffer, kv)
				}
				ifile.Close()

				// remove intermediate file w.r.t to this worker
				err = os.Remove(ifilename)
				if err != nil {
					log.Fatalf("Reducer Worker: cannot remove file %v", ifilename)
				}
			}

			sort.Sort(ByKey(reducerBuffer))

			ofilename := fmt.Sprint("mr-out-", reducerId)
			ofile, err := os.Create(ofilename)
			if err != nil {
				log.Fatalf("Cannot create file %v", ofilename)
			}

			startIndex := 0
			for startIndex < len(reducerBuffer) {
				endIndex := startIndex + 1
				for endIndex < len(reducerBuffer) &&
					reducerBuffer[startIndex].Key == reducerBuffer[endIndex].Key {
					endIndex += 1
				}

				values := []string{}
				for i := startIndex; i < endIndex; i++ {
					values = append(values, reducerBuffer[i].Value)
				}
				output := reducef(reducerBuffer[startIndex].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", reducerBuffer[startIndex].Key, output)

				startIndex = endIndex
			}
			ofile.Close()

			lastTaskId = reducerId
		}
		// fmt.Println("Reducer Worker is now exiting.")
	}()

	wg.Wait()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallMapper(lastMapperTaskId int) MapperTaskResponse {
	mapperRequest := MapperTaskRequest{}
	mapperRequest.CompletedMapperTaskId = lastMapperTaskId

	mapperReply := MapperTaskResponse{}

	ok := call("Coordinator.Mapper", &mapperRequest, &mapperReply)
	if !ok {
		log.Fatal("Call to Mapper fail!")
	}

	return mapperReply
}

func CallReducer(lastReducerTaskId int) ReducerTaskResponse {
	reducerRequest := ReducerTaskRequest{}
	reducerReply := ReducerTaskResponse{}

	reducerRequest.CompletedReducerTaskId = lastReducerTaskId

	ok := call("Coordinator.Reducer", &reducerRequest, &reducerReply)

	if !ok {
		log.Fatal("Call to Reducer fail!")
	}

	return reducerReply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type kvArray []KeyValue

func (a kvArray) Len() int           { return len(a) }
func (a kvArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a kvArray) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (kvarray kvArray) splitIntoBuckets(nReduce int) []kvArray {
	buckets := make([]kvArray, nReduce)
	for _, kv := range kvarray {
		reducerIdx := ihash(kv.Key) % nReduce
		buckets[reducerIdx] = append(buckets[reducerIdx], kv)
	}
	return buckets
}

func (kvarray kvArray) writeToFile(filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.FileMode(0666))
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	err = enc.Encode(kvarray)
	return err
}

func readKVArrayFromFile(filename string) (kvArray, error) {
	var kvarray kvArray
	file, err := os.Open(filename)
	if err != nil {
		return kvarray, err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	err = dec.Decode(&kvarray)
	return kvarray, err
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var wlogger = log.New(os.Stdout, "Worker: ", 0)

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var emptyArgs EmptyArgs
	var emptyReply EmptyReply
	pid := os.Getpid()

	for true {
		wlogger.SetPrefix(fmt.Sprintf("Worker[pid=%v]: ", pid))

		// Map task
		{
			var reply GetMapTaskReply
			wlogger.Printf("Getting the map task\n")
			call("Coordinator.GetMapTask", &emptyArgs, &reply)
			if !reply.NoTasks {
				taskId := reply.TaskId
				wlogger.SetPrefix(fmt.Sprintf("Worker[pid=%v task=%v]: ", pid, taskId))
				wlogger.Printf("Executing map task for file '%s'\n", reply.Filename)
				bytes, err := os.ReadFile(reply.Filename)
				if err != nil {
					wlogger.Fatal(err)
				}

				contents := string(bytes)
				kvarray := kvArray(mapf(reply.Filename, contents))
				buckets := kvarray.splitIntoBuckets(reply.NReduce)
				for reduceIdx, bucket := range buckets {
					outfile := filenameForMapResult(reduceIdx, taskId)
					err := bucket.writeToFile(outfile)
					if err != nil {
						wlogger.Fatal(err)
					}
				}

				wlogger.Printf("Completing map task for file '%s'\n", reply.Filename)
				call("Coordinator.CompleteMapTask", &CompleteTaskArgs{taskId}, &emptyReply)
				continue
			}
		}

		// Reduce task
		{
			wlogger.Printf("Getting the reduce task\n")
			var reply GetReduceTaskReply
			call("Coordinator.GetReduceTask", &emptyArgs, &reply)
			if !reply.NoTasks {
				// Refactor this block of code (too much logic right now)
				taskId := reply.TaskId
				wlogger.SetPrefix(fmt.Sprintf("Worker[pid=%v task=%v]", pid, taskId))
				files, err := findFilesWithMapResults(reply.ReduceIndex)
				if err != nil {
					wlogger.Fatal(err)
				}
				var kvarray kvArray
				for _, file := range files {
					arr, err := readKVArrayFromFile(file)
					if err != nil {
						wlogger.Fatal(err)
					}
					kvarray = append(kvarray, arr...)
				}

				sort.Sort(kvarray)

				oname := fmt.Sprintf("mr-out-%v", reply.ReduceIndex)
				ofile, _ := os.Create(oname)

				var wg sync.WaitGroup
				i := 0
				for i < len(kvarray) {
					j := i + 1
					for j < len(kvarray) && kvarray[j].Key == kvarray[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kvarray[k].Value)
					}
					wg.Add(1)

					go func(key string, values []string) {
						defer wg.Done()
						output := reducef(key, values)
						// this is the correct format for each line of Reduce output.
						fmt.Fprintf(ofile, "%v %v\n", key, output)
					}(kvarray[i].Key, values)

					i = j
				}

				wg.Wait()
				call("Coordinator.CompleteReduceTask", &CompleteTaskArgs{taskId}, &emptyReply)
				continue
			}
		}

		// Checking for exit
		{
			wlogger.Printf("Checking if worker should exit\n")
			var reply ShouldExitReply
			call("Coordinator.ShouldExit", &emptyArgs, &reply)
			if reply.ShouldExit {
				break
			} else {
				dur := 1500 * time.Millisecond
				wlogger.Printf("No available tasks, waiting for %v\n", dur.String())
				time.Sleep(dur)
				continue
			}
		}
	}

	wlogger.Printf("Exiting\n")
}

func filenameForMapResult(reduceIdx int, taskId int) string {
	return fmt.Sprintf("./map-inter-%d-%d", reduceIdx, taskId)
}

func findFilesWithMapResults(reduceIdx int) ([]string, error) {
	return filepath.Glob(fmt.Sprintf("./map-inter-%v-*", reduceIdx))
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	log.Println(err)
	return false
}

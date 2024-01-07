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

type taskState int

const (
	taskStateIdle taskState = iota
	taskStateProgress
	taskStateComplete
)

const workerTimeout = 10 * time.Second

type mapTask struct {
	State    taskState
	TaskId   int
	Filename string
}

type reduceTask struct {
	State       taskState
	TaskId      int
	ReduceIndex int
}

type Coordinator struct {
	mu          sync.Mutex // Mutex for operations on slices (MapTasks, ReduceTasks)
	MapTasks    []mapTask
	ReduceTasks []reduceTask
	NReduce     int
	IdIncrement *safeCounter
}

var clogger = DLogger{log.New(os.Stdout, "Coordinator: ", 0)}

func (c *Coordinator) GetMapTask(_ *EmptyArgs, reply *GetMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.findMapTask(func(t mapTask) bool { return t.State == taskStateIdle })
	if idx == -1 {
		reply.NoTasks = true
		return nil
	}
	task := c.MapTasks[idx]
	clogger.DPrintf("Giving map-task[id=%v] to a worker\n", task.TaskId)
	*reply = GetMapTaskReply{
		NoTasks:  false,
		TaskId:   task.TaskId,
		Filename: task.Filename,
		NReduce:  c.NReduce,
	}
	c.MapTasks[idx].State = taskStateProgress

	go func(c *Coordinator, taskId int) {
		time.Sleep(workerTimeout)
		c.mu.Lock()
		defer c.mu.Unlock()
		complete := c.mapTaskComplete(taskId)
		if !complete {
			clogger.DPrintf("Worker didn't finish map-task[id=%v] in %v. Setting it's state to idle", taskId, workerTimeout.String())
			c.resetMapTask(taskId)
		}
	}(c, task.TaskId)

	return nil
}

func (c *Coordinator) GetReduceTask(_ *EmptyArgs, reply *GetReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.findReduceTask(func(t reduceTask) bool { return t.State == taskStateIdle })
	if idx == -1 {
		reply.NoTasks = true
		return nil
	}
	task := c.ReduceTasks[idx]
	clogger.DPrintf("Giving reduce-task[id=%v] to a worker\n", task.TaskId)
	*reply = GetReduceTaskReply{
		NoTasks:     false,
		TaskId:      task.TaskId,
		ReduceIndex: task.ReduceIndex,
	}
	c.ReduceTasks[idx].State = taskStateProgress

	go func(c *Coordinator, taskId int) {
		time.Sleep(workerTimeout)
		c.mu.Lock()
		defer c.mu.Unlock()
		complete := c.reduceTaskComplete(taskId)
		if !complete {
			clogger.DPrintf("Worker didn't finish reduce-task[id=%v] in %v. Setting it's state to idle", taskId, workerTimeout.String())
			c.resetReduceTask(taskId)
		}
	}(c, task.TaskId)

	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteTaskArgs, _ *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.findMapTask(func(t mapTask) bool { return t.TaskId == args.TaskId && t.State == taskStateProgress })
	if idx == -1 {
		return nil
	}
	task := c.MapTasks[idx]
	clogger.DPrintf("map-task[id=%v] complete\n", task.TaskId)
	c.MapTasks[idx].State = taskStateComplete

	allComplete := -1 == c.findMapTask(func(t mapTask) bool { return t.State != taskStateComplete })

	if allComplete {
		clogger.DPrintf("All map-tasks complete, adding %v reduce-tasks\n", c.NReduce)
		for i := 0; i < c.NReduce; i++ {
			c.addReduceTask(i)
		}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteTaskArgs, _ *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.findReduceTask(func(t reduceTask) bool { return t.TaskId == args.TaskId && t.State == taskStateProgress })
	if idx == -1 {
		return nil
	}
	task := c.ReduceTasks[idx]
	clogger.DPrintf("reduce-task[id=%v] complete\n", task.TaskId)
	c.ReduceTasks[idx].State = taskStateComplete

	files, err := findFilesWithMapResults(task.ReduceIndex)
	if err != nil {
		clogger.DPrintf("Couldn't find intermediate files for reduce-index=%v", task.ReduceIndex)
		return nil
	}

	clogger.DPrintf("Deleting files for reduce-index=%v %v", task.ReduceIndex, files)
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			clogger.DPrintf("Couldn't remove file for reduce-index=%v with filename=%v", task.ReduceIndex, file)
		}
	}
	return nil
}

func (c *Coordinator) ShouldExit(_ *EmptyArgs, reply *ShouldExitReply) error {
	reply.ShouldExit = c.Done()
	return nil
}

func (c *Coordinator) addMapTask(filename string) {
	c.MapTasks = append(c.MapTasks, mapTask{
		State: taskStateIdle,
		TaskId: c.IdIncrement.Value(),
		Filename: filename,
	})
	c.IdIncrement.Increment()
}

func (c *Coordinator) addReduceTask(reduceIndex int) {
	c.ReduceTasks = append(c.ReduceTasks, reduceTask{
		State:       taskStateIdle,
		TaskId:      c.IdIncrement.Value(),
		ReduceIndex: reduceIndex,
	})
	c.IdIncrement.Increment()
}

func (c *Coordinator) findMapTask(pred func(task mapTask) bool) int {
	for idx, task := range c.MapTasks {
		if pred(task) {
			return idx
		}
	}
	return -1
}

func (c *Coordinator) findReduceTask(pred func(task reduceTask) bool) int {
	for idx, task := range c.ReduceTasks {
		if pred(task) {
			return idx
		}
	}
	return -1
}

func (c *Coordinator) mapTaskComplete(taskId int) bool {
	idx := c.findMapTask(func(t mapTask) bool { return t.TaskId == taskId })
	if idx == -1 {
		return false
	}
	return c.MapTasks[idx].State == taskStateComplete
}

func (c *Coordinator) resetMapTask(taskId int) {
	idx := c.findMapTask(func(t mapTask) bool { return t.TaskId == taskId })
	if idx == -1 {
		return
	}
	c.MapTasks[idx].State = taskStateIdle
}

func (c *Coordinator) reduceTaskComplete(taskId int) bool {
	idx := c.findReduceTask(func(t reduceTask) bool { return t.TaskId == taskId })
	if idx == -1 {
		return false
	}
	return c.ReduceTasks[idx].State == taskStateComplete
}

func (c *Coordinator) resetReduceTask(taskId int) {
	idx := c.findReduceTask(func(t reduceTask) bool { return t.TaskId == taskId })
	if idx == -1 {
		return
	}
	c.ReduceTasks[idx].State = taskStateIdle
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		clogger.Fatal("listen error:", e)
	}
	clogger.DPrintf("http.Serve\n")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.MapTasks {
		if task.State != taskStateComplete {
			return false
		}
	}
	for _, task := range c.ReduceTasks {
		if task.State != taskStateComplete {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	clogger.DPrintf("Init\n")
	
	idIncrement := makeSafeCounter(1)
	c := Coordinator{
		MapTasks:    make([]mapTask, 0),
		ReduceTasks: make([]reduceTask, 0),
		NReduce:     nReduce,
		IdIncrement: &idIncrement,
	}

	clogger.DPrintf("Starting with %v map-tasks\n", len(files))
	for _, file := range files {
		c.addMapTask(file)
	}

	c.server()
	return &c
}

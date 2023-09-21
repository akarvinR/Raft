package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

/*
Coordinator -> has 2 type of tasks [reduce/worker] ->

For each worker, output the files to 10 temp files [map tasks] and pass them to the coordinator.

Once, a worker had done the job, it will send a RPC to the coordinator to notify it, and ask for a new task. If no tasks are done, the coordinator will send a different call signal "none" []

Once, a map task is done, the worker will send all filesnames to the coordinator.
*/
type NReduce struct {
	NReduce int
}
type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	files       []string
	nReduce     int
	tmpWorkerID int32
	tmpTaskID   int32

	tasksCompleted  map[int32]TaskOutput //taskID -> TaskOutput
	tasksAvailable  chan Task
	tasksInProgress map[int32]Task //taskID -> Task
	workersTaskMap  map[int32]Task //workerID -> Task
	workersAliveMap map[int32]int  //workerID -> aliveCount
}

func (c *Coordinator) GetTask(args *WorkerDetails, reply *Task) error {

	if len(c.tasksCompleted) == c.nReduce+len(c.files) {
		reply.TaskType = "over"
		return nil
	}

	if args.WorkerID == -1 {
		atomic.AddInt32(&c.tmpWorkerID, 1)
		reply.WorkerID = atomic.LoadInt32(&c.tmpWorkerID)
	} else {
		reply.WorkerID = args.WorkerID
	}
	task := <-c.tasksAvailable
	reply.TaskID = task.TaskID
	reply.FileNames = task.FileNames
	reply.TaskType = task.TaskType

	c.mu.Lock()
	c.workersAliveMap[reply.WorkerID] = 1
	c.workersTaskMap[reply.WorkerID] = task
	c.tasksInProgress[task.TaskID] = task
	c.mu.Unlock()

	return nil
}

func (c *Coordinator) addReduceTasks() {

	reduceTasks := make([]Task, c.nReduce)
	for _, value := range c.tasksCompleted {
		for i := 0; i < c.nReduce; i++ {
			reduceTasks[i].FileNames = append(reduceTasks[i].FileNames, value.OutputFileNames[i])
		}
	}
	for i := 0; i < c.nReduce; i++ {
		c.tmpTaskID++
		reduceTasks[i].TaskType = "reduce"
		reduceTasks[i].TaskID = c.tmpTaskID
		c.tasksAvailable <- reduceTasks[i]
	}
}

func (c *Coordinator) addMapTasks() {
	for i := 0; i < len(c.files); i++ {
		c.tmpTaskID++

		mapTask := Task{TaskID: c.tmpTaskID, TaskType: "map", FileNames: []string{c.files[i]}}
		c.tasksAvailable <- mapTask
		// print(mapTask)
	}

}
func (c *Coordinator) TaskCompleted(args *TaskOutput, reply *struct{}) error {
	// print(args.TaskID)
	// print(args.TaskID)

	c.mu.Lock()

	task := c.tasksInProgress[args.TaskID]
	delete(c.tasksInProgress, task.TaskID)
	delete(c.workersTaskMap, task.WorkerID)
	c.tasksCompleted[task.TaskID] = *args

	// print(args.outputFileNames)
	if len(c.tasksCompleted) == len(c.files) {
		c.addReduceTasks()
	}

	c.mu.Unlock()

	return nil

}

func (c *Coordinator) removeWorker(args WorkerDetails, reply struct{}) {
	//remove tasks from the tasksinprogress and put it in taskstobedone
	task := c.workersTaskMap[args.WorkerID]
	c.tasksAvailable <- task

	c.mu.Lock()

	delete(c.tasksInProgress, task.TaskID)

	delete(c.workersTaskMap, args.WorkerID)
	delete(c.workersAliveMap, args.WorkerID)

	c.mu.Unlock()

}
func (c *Coordinator) waitForHeartBeat(args *WorkerDetails, reply struct{}) {
	c.mu.Lock()
	tmpCount := c.workersAliveMap[args.WorkerID]
	c.mu.Unlock()
	time.Sleep(5 * time.Second)
	if tmpCount == c.workersAliveMap[args.WorkerID] {
		delete(c.workersAliveMap, args.WorkerID)
		c.removeWorker(*args, reply)
	}

}
func (c *Coordinator) HeartBeat(args *WorkerDetails, reply *struct{}) error {
	//increament alive count
	//sleepfor10seconds
	//go waitforheartbeat//10 seconds
	//sendreply

	//return
	c.mu.Lock()
	c.workersAliveMap[args.WorkerID]++
	c.mu.Unlock()
	time.Sleep(10 * time.Second)
	go c.waitForHeartBeat(args, *reply)
	return nil

}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetNreduce(args *NReduce, reply *NReduce) error {

	reply.NReduce = c.nReduce

	return nil
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
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

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := (len(c.tasksCompleted) == c.nReduce+len(c.files))

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, tasksAvailable: make(chan Task, nReduce+len(files)), tmpWorkerID: 0, tmpTaskID: 0, tasksCompleted: make(map[int32]TaskOutput), tasksInProgress: make(map[int32]Task), workersTaskMap: make(map[int32]Task), workersAliveMap: make(map[int32]int)}
	c.addMapTasks()
	// Your code here.

	c.server()
	return &c
}

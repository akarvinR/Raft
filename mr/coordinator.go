package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
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
	mu                sync.Mutex
	files             []string
	nReduce           int
	tmpWorkerID       int32
	tmpTaskID         int32
	tasksCompletedCnt int32
	tasksCompleted    sync.Map //map[int32]TaskOutput //taskID -> TaskOutput
	tasksAvailable    chan Task
	tasksInProgress   sync.Map // map[int32]Task //taskID -> Task
	workersTaskMap    sync.Map // map[int32]Task //workerID -> Task
	workersHeartBeat  sync.Map //map[int32]int  //workerID -> aliveCount
}

func (c *Coordinator) GetTask(args *WorkerDetails, reply *Task) error {
	// print("task request")
	if int(atomic.LoadInt32(&c.tasksCompletedCnt)) == c.nReduce+len(c.files) {
		reply.TaskType = "over"
		return nil
	}
	heartBeatChannel, workerfound := c.workersHeartBeat.Load(args.WorkerID)
	if !workerfound {
		atomic.AddInt32(&c.tmpWorkerID, 1)
		reply.WorkerID = atomic.LoadInt32(&c.tmpWorkerID)
		args.WorkerID = reply.WorkerID
		heartBeatChannel = make(chan int)
		c.workersHeartBeat.Store(reply.WorkerID, heartBeatChannel)
	} else {
		reply.WorkerID = args.WorkerID
	}
	task := <-c.tasksAvailable
	reply.TaskID = task.TaskID
	reply.FileNames = task.FileNames
	reply.TaskType = task.TaskType
	// print(reply.TaskID)

	// c.workersAliveMap[reply.WorkerID] = 1
	// c.workersTaskMap[reply.WorkerID] = task
	// c.tasksInProgress[task.TaskID] = task

	// c.workersAliveMap.Store(reply.WorkerID, 1

	go c.waitForHeartBeat(args, struct{}{})

	c.workersTaskMap.Store(reply.WorkerID, task)
	c.tasksInProgress.Store(task.TaskID, task)
	go c.redoTask(task.TaskID)
	return nil
}
func (c *Coordinator) redoTask(taskID int32) {
	time.Sleep(15 * time.Second)
	// print("Redoing Task")
	if val, found := c.tasksInProgress.Load(taskID); found {
		task := val.(Task)
		// print("Task found in progress")
		c.removeWorker(WorkerDetails{WorkerID: task.WorkerID}, struct{}{})
	}

}
func (c *Coordinator) addReduceTasks() {

	reduceTasks := make([]Task, c.nReduce)

	c.tasksCompleted.Range(func(key, value interface{}) bool {
		for i := 0; i < c.nReduce; i++ {
			reduceTasks[i].FileNames = append(reduceTasks[i].FileNames, value.(TaskOutput).OutputFileNames[i])
		}
		return true
	})

	// for _, value := range c.tasksCompleted {
	// 	for i := 0; i < c.nReduce; i++ {
	// 		reduceTasks[i].FileNames = append(reduceTasks[i].FileNames, value.OutputFileNames[i])
	// 	}
	// }
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

	ttask, found := c.tasksInProgress.Load(args.TaskID) //[args.TaskID]
	if !found {
		return nil
	}
	task := ttask.(Task)
	// delete(c.tasksInProgress, task.TaskID)
	// delete(c.workersTaskMap, task.WorkerID)
	// c.tasksCompleted[task.TaskID] = *args

	if task.TaskType == "reduce" {
		os.Rename(args.OutputFileNames[0], "mr-out-"+strconv.Itoa(int(task.TaskID)))
	}

	c.tasksInProgress.Delete(task.TaskID)
	c.workersTaskMap.Delete(task.WorkerID)
	c.tasksCompleted.Store(task.TaskID, *args)
	atomic.AddInt32(&c.tasksCompletedCnt, 1)

	// print(args.outputFileNames)
	if int(atomic.LoadInt32(&c.tasksCompletedCnt)) == len(c.files) {
		c.addReduceTasks()
	}

	return nil

}

func (c *Coordinator) removeWorker(args WorkerDetails, reply struct{}) {
	//remove tasks from the tasksinprogress and put it in taskstobedone
	// task := c.workersTaskMap[args.WorkerID]

	tmptask, _ := c.workersTaskMap.Load(args.WorkerID)
	task, _ := tmptask.(Task)
	c.tasksAvailable <- task

	// delete(c.tasksInProgress, task.TaskID)
	// delete(c.workersTaskMap, args.WorkerID)
	// delete(c.workersAliveMap, args.WorkerID)
	// print("deleting worker")
	// print(args.WorkerID)
	c.tasksInProgress.Delete(task.TaskID)
	c.workersTaskMap.Delete(args.WorkerID)
	c.workersHeartBeat.Delete(args.WorkerID)

}

func (c *Coordinator) SendHeartBeat(args *WorkerDetails, reply *WorkerDetails) error {
	// print(args.WorkerID)
	heartBeatChannel, workerfound := c.workersHeartBeat.Load(args.WorkerID)
	// print(workerfound)
	if workerfound {
		// print("hello")

		heartBeatChannel.(chan int) <- 1
	} else {
		// print("Worker not found")
		// reply.WorkerID = -1
	}
	return nil
}
func (c *Coordinator) waitForHeartBeat(args *WorkerDetails, reply struct{}) {

	// tmpCount := c.workersAliveMap[args.WorkerID]
	for {

		heartBeatChannel, workerfound := c.workersHeartBeat.Load(args.WorkerID)
		// print(workerfound)
		if !workerfound {
			return
		}
		interval := 15 * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			//for every second
			// print("run")
			select {
			case <-ticker.C:
				//after 10 seconds

				select {
				case <-heartBeatChannel.(chan int):
					// print("heart beat detected")
					ticker.Stop()
					ticker = time.NewTicker(interval)
					// tmpCount, _ := c.workersAliveMap.Load(args.WorkerID)
				default:
					// print("worker dead")
					c.removeWorker(*args, reply)
					return

				}
			default:
				select {
				case <-heartBeatChannel.(chan int):
					// print("heart beat detected")
					ticker.Stop()
					ticker = time.NewTicker(interval)
				default:
					continue
				}

			}
		}

	}
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

	ret := (int(atomic.LoadInt32(&c.tasksCompletedCnt)) == c.nReduce+len(c.files))
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, tasksAvailable: make(chan Task, nReduce+len(files)), tmpWorkerID: 0, tmpTaskID: 0} // tasksCompleted: make(map[int32]TaskOutput), tasksInProgress: make(map[int32]Task), workersTaskMap: make(map[int32]Task), workersAliveMap: make(map[int32]int)}
	c.addMapTasks()
	// Your code here.

	c.server()
	return &c
}

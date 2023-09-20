package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

/*
Coordinator -> has 2 type of tasks [reduce/worker] ->

For each worker, output the files to 10 temp files [map tasks] and pass them to the coordinator.

Once, a worker had done the job, it will send a RPC to the coordinator to notify it, and ask for a new task. If no tasks are done, the coordinator will send a different call signal "none" []

Once, a map task is done, the worker will send all filesnames to the coordinator.
*/
type Task struct {
	workerID int
	taskType string
	fileName string
	state    string
}

type CompletedTask struct {
	task           Task
	outputFileName string
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	mapTasks    Task
	reduceTasks chan Task
	tmpWorkerID int //TODO MAKE ATOMIC
	workers     map[int]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args struct{}, reply *Task) error {

	//if mapworkers are completed
	task := <-c.tasks
	reply.fileName = task.fileName
	reply.taskType = task.taskType
	reply.workerID = c.tmpWorkerID
	//clean all workers.


	//if only reduce works are left
	//
	return nil
}
func (c *Coordinator) taskCompleted(args struct{}, reply struct{}) {
	//put tasksinprogress to taskscompleted.
	//remove worker from workers, and from reduce jobs.


}

func (c *Coordinator) removeWorker(args struct{}, reply struct{}) {
	//remove tasks from the tasksinprogress and put it in taskstobedone
	//

}
func (c *Coordinator) waitForHeartBeat(args struct{}, reply struct{})  {
	//store alive count
	//sleepfor5seconds
	//if alivenotincreamend
	//killworker

}
func (c *Coordinator) heartBeat(args struct{}, reply struct{}) error {
	//increament alive count
	//sleepfor10seconds
	//go waitforheartbeat//10 seconds
	//sendreply

	//return

}

// rpc done task
func (c *Coordinator) DoneTask(args *Task, reply *struct{}) error {
	// Your code here.
}
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, tasks: make(chan Task, 10), tmpWorkerID: 0}

	// Your code here.

	c.server()
	return &c
}

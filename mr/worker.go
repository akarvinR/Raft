package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var taskDone chan bool
	var task Task
	var workerDetails WorkerDetails
	var cancelWorker chan bool
	sendHeartBeat := func() {
		interval := time.Second * 8
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-taskDone:
				return
			case <-cancelWorker:
				return
			default:
				select {
				case <-ticker.C:
					call("Coordinator.SendHeartBeat", &workerDetails, &workerDetails)

				default:
					continue
				}
			}
		}
	}
	NReduce := NReduce{}

	call("Coordinator.GetNreduce", &NReduce, &NReduce)
	workerDetails.WorkerID = -1
	for {
		// time.Sleep(2 * time.Millisecond)
		select {
		case <-cancelWorker:
			//donothing
			// print("worker cancelled")
		default:
			call("Coordinator.GetTask", &workerDetails, &task)
			workerDetails.WorkerID = task.WorkerID
			go sendHeartBeat()
			if task.TaskType == "over" {
				taskDone <- true
				break
			} else if task.TaskType == "map" {
				//

				intermediate := []KeyValue{}
				filename := task.FileNames[0]
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
				buckets := make([][]KeyValue, NReduce.NReduce)
				for _, kv := range intermediate {
					bucket := ihash(kv.Key) % NReduce.NReduce
					buckets[bucket] = append(buckets[bucket], kv)
				}
				fileNames := make([]string, NReduce.NReduce)
				for i := 0; i < NReduce.NReduce; i++ {
					// oname := "mr-temp-" + fmt.Sprint(task.TaskID) + "-" + fmt.Sprint(i)
					//Create oname be a tempfile using io.TempFile
					// oname := fmt.Sprintf("mr-temp-%v-%v", task.TaskID, i)
					ofile, _ := os.CreateTemp("", "temp-")
					fileNames[i] = ofile.Name()

					// ofile, _ := os.Create(oname)
					for _, kv := range buckets[i] {
						fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
					}
					// os.Rename(ofile.Name(), "mr-"+fmt.Sprint(task.TaskID)+"-"+fmt.Sprint(i))
					ofile.Close()
				}

				taskOutput := TaskOutput{TaskID: task.TaskID, OutputFileNames: fileNames}
				// print((taskOutput.TaskID))
				var reply struct{}
				call("Coordinator.TaskCompleted", &taskOutput, &reply)
				// print("task done")

				// taskDone <- true

			} else if task.TaskType == "reduce" {

				taskOutput := TaskOutput{TaskID: task.TaskID, OutputFileNames: task.FileNames}
				output := []KeyValue{}
				for _, filePath := range task.FileNames {
					file, err := os.Open(filePath)
					if err != nil {
						fmt.Printf("Error opening file %s: %v\n", filePath, err)
						continue
					}
					defer file.Close()

					scanner := bufio.NewScanner(file)

					// Process each line in the file
					for scanner.Scan() {
						line := scanner.Text()
						parts := strings.Fields(line)
						if len(parts) != 2 {
							fmt.Printf("Skipping invalid line: %s\n", line)
							continue
						}

						key := parts[0]
						value := parts[1]

						// Append the value to the map's slice
						output = append(output, KeyValue{key, value})
					}

					if err := scanner.Err(); err != nil {
						fmt.Printf("Error scanning file %s: %v\n", filePath, err)
					}
				}

				sort.Sort(ByKey(output))
				// oname := "mr-out-" + strings.Split(task.FileNames[0], "-")[3]

				ofile, _ := os.CreateTemp("", "reduce-")

				oname := ofile.Name()

				taskOutput.OutputFileNames = []string{oname}

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(output) {
					j := i + 1
					for j < len(output) && output[j].Key == output[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, output[k].Value)
					}
					outputx := reducef(output[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", output[i].Key, outputx)

					i = j
				}

				ofile.Close()
				var reply struct{}
				call("Coordinator.TaskCompleted", &taskOutput, &reply)
				// taskDone <- true
			}

		}

	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	fmt.Println(err)
	return false
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
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

var mapF func(string, string) []KeyValue
var reduceF func(string, []string) string

func openFile(filename string) *os.File {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Worker:", err.Error())
	}
	return file
}

func createTempFile(taskType TaskType) *os.File {
	pattern := ""
	pid := os.Getpid()
	if taskType == Map {
		pattern = fmt.Sprintf("map-%d-*", pid)
	} else {
		pattern = fmt.Sprintf("reduce-%d-*", pid)
	}
	file, err := os.CreateTemp(".", pattern)
	if err != nil {
		log.Fatal("Worker:", err.Error())
	}
	return file
}

func readFile(file *os.File) []byte {
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("Worker:", err.Error())
	}
	return content
}

func renameFile(oldName string, newName string) {
	if err := os.Rename(oldName, newName); err != nil {
		log.Fatal("Worker:", err.Error())
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapProcess(task Task, nReduce int) {
	taskNum := task.TaskNum
	iname := task.FileName
	ifile := openFile(iname)
	content := readFile(ifile)
	ifile.Close()
	tnames := make([]string, nReduce)
	tfiles := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		tfile := createTempFile(task.TaskType)
		defer tfile.Close()
		tnames[i] = tfile.Name()
		tfiles[i] = json.NewEncoder(tfile)
	}
	kva := mapF(iname, string(content))
	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % nReduce
		tfiles[reduceNum].Encode(&kv)
	}
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", taskNum, i)
		renameFile(tnames[i], oname)
	}
}

func ReduceProcess(task Task, nMap int) {
	taskNum := task.TaskNum
	intermediate := []KeyValue{}
	tfile := createTempFile(task.TaskType)
	defer tfile.Close()
	for i := 0; i < nMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, taskNum)
		ifile := openFile(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	length := len(intermediate)
	output := []KeyValue{}
	for i < length {
		j := i + 1
		for j < length && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output = append(output, KeyValue{intermediate[i].Key, reduceF(intermediate[i].Key, values)})
		i = j
	}
	for _, kv := range output {
		fmt.Fprintf(tfile, "%v %v\n", kv.Key, kv.Value)
	}
	oname := fmt.Sprintf("mr-out-%d", taskNum)
	renameFile(tfile.Name(), oname)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	mapF = mapf
	reduceF = reducef

	for {
		task, nTask := CallGetTask()
		switch task.TaskType {
		case Map:
			mapProcess(task, nTask)
		case Reduce:
			ReduceProcess(task, nTask)
		case Done:
			os.Exit(0)
		default:
			log.Fatalf("Worker: Invalid Task. task No = %d, task Type = %d", task.TaskNum, task.TaskType)
		}
		CallFinishTask(task.TaskNum, task.TaskType)
	}
}

func CallGetTask() (task Task, nTask int) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	if !call("Coordinator.GetTask", &args, &reply) {
		log.Fatal("Worker: GetTask Call failed!")
	}
	return reply.Task, reply.NTask
}

func CallFinishTask(taskNum int, taskType TaskType) {
	args := FinishTaskArgs{taskNum, taskType}
	reply := FinishTaskReply{}
	if !call("Coordinator.FinishTask", &args, &reply) {
		log.Fatal("Worker: FinishTask Call failed!")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("Worker: dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Worker:", err)
	return false
}

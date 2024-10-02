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

type TaskType int
type TaskState int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

const (
	IDLE       TaskState = 0
	INPROGRESS TaskState = 1
	COMPLETED  TaskState = 2
)

type Task struct {
	TaskNum  int
	TaskType TaskType
	FileName string
}

type TaskMetaData struct {
	task      *Task
	state     TaskState
	startTime time.Time
}

type Coordinator struct {
	stage     int
	tasks     chan Task
	dones     chan bool
	metaTable []TaskMetaData
	nMap      int
	nReduce   int
	inputs    []string
	latch     sync.RWMutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for {
		task, ok := <-c.tasks
		if !ok {
			reply.Task.TaskType = Done
			return nil
		}
		taskNum := task.TaskNum
		c.latch.Lock()
		if task.TaskType == TaskType(c.stage) && c.metaTable[taskNum].state == IDLE {
			c.metaTable[taskNum].state = INPROGRESS
			c.metaTable[taskNum].startTime = time.Now()
			c.latch.Unlock()
			reply.Task = task
			break
		}
		c.latch.Unlock()
	}
	if reply.Task.TaskType == Map {
		reply.NTask = c.nReduce
	} else {
		reply.NTask = c.nMap
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.latch.Lock()
	defer c.latch.Unlock()
	if c.stage == int(Done) {
		return nil
	}
	taskNum := args.TaskNum
	table := c.metaTable
	if args.TaskType == TaskType(c.stage) && (table[taskNum].state != COMPLETED) {
		table[taskNum].state = COMPLETED
		c.dones <- true
	}
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
		log.Fatal("Coordinator: listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.latch.RLock()
	defer c.latch.RUnlock()
	return c.stage == int(Done)
}

func (c *Coordinator) execute(stage int) {
	nTask := 0
	for {
		c.latch.Lock()
		c.stage = stage
		switch TaskType(stage) {
		case Map:
			nTask = c.nMap
		case Reduce:
			nTask = c.nReduce
		case Done:
			nTask = 0
		default:
			log.Fatal("Coordinator: Invalid Stage")
		}
		c.metaTable = make([]TaskMetaData, nTask)
		tasks := make([]*Task, nTask)
		for i := 0; i < nTask; i++ {
			ifileName := ""
			if stage == int(Map) {
				ifileName = c.inputs[i]
			}
			task := Task{i, TaskType(stage), ifileName}
			tasks[i] = &task
			c.metaTable[i] = TaskMetaData{&task, IDLE, time.Time{}}
		}
		c.latch.Unlock()
		if nTask == 0 {
			close(c.tasks)
			close(c.dones)
			break
		}
		go func(tasks []*Task) {
			for _, task := range tasks {
				c.tasks <- *task
			}
		}(tasks)
		for i := 0; i < nTask; i++ {
			<-c.dones
		}
		stage++
	}
}

func (c *Coordinator) timeOutDetection() {
	for {
		time.Sleep(time.Second)
		c.latch.Lock()
		if c.stage == int(Done) {
			c.latch.Unlock()
			break
		}
		table := c.metaTable
		timeOutTasks := []*Task{}
		for i, item := range table {
			if item.state == INPROGRESS && time.Since(item.startTime).Seconds() > 10 {
				timeOutTasks = append(timeOutTasks, item.task)
				table[i].state = IDLE
			}
		}
		c.latch.Unlock()
		for _, task := range timeOutTasks {
			c.tasks <- *task
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	c := &Coordinator{
		stage:   int(Map),
		nMap:    nMap,
		nReduce: nReduce,
		inputs:  files,
		tasks:   make(chan Task, max(nMap, nReduce)),
		dones:   make(chan bool),
	}
	go c.execute(int(Map))
	go c.timeOutDetection()
	c.server()
	return c
}

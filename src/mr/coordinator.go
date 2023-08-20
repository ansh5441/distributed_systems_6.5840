package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// create a lock for task read and write
var taskLock = sync.Mutex{}

// Task states for map and reduce tasks
const (
	Pending = iota
	InProgress
	Completed
)

type MapTask struct {
	FileName  string // file name of the map task
	State     int    // 0: pending, 1: in progress, 2: completed
	Num       int    // task number
	StartTime int64  // time when the task was assigned

	//
}

type ReduceTask struct {
	FileNames []string // file name of the reduce task
	State     int      // 0: pending, 1: in progress, 2: completed
	StartTime int64    // time when the task was assigned
	Num       int      // task number
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	nReduce     int
}

func (c *Coordinator) RPCHandler(args *Args, reply *Reply) error {
	switch args.Query {
	case "map done":
		taskLock.Lock()

		log.Println("Lock in map done")
		c.MapTasks[args.MapTaskNum].State = Completed
		// Add reduce tasks
		for i := 0; i < c.nReduce; i++ {
			filename := fmt.Sprintf("mr-%d-%d.txt", args.MapTaskNum, i)
			if len(c.ReduceTasks) <= i {
				c.ReduceTasks = append(c.ReduceTasks, ReduceTask{[]string{filename}, Pending, 0, i})
			} else {
				c.ReduceTasks[i].FileNames = append(c.ReduceTasks[i].FileNames, filename)
			}
		}
		taskLock.Unlock()
		log.Printf("Map done: %v", args.FileName)
	case "reduce done":
		taskLock.Lock()
		log.Println("Lock in reduce done")
		c.ReduceTasks[args.ReduceTaskNum].State = Completed
		taskLock.Unlock()
		log.Println("Reduce done: ", args.ReduceTaskNum)
	case "give me a job":
		// assign a map task if there is any
		taskLock.Lock()
		log.Println("Lock in give me a job")
		for i, task := range c.MapTasks {
			if task.State == Pending {
				reply.FileName = task.FileName
				reply.Command = "map"
				reply.NReduce = c.nReduce
				reply.MapTaskNum = i
				task.State = InProgress
				task.StartTime = time.Now().Unix()
				log.Println("Map task ", i, " assigned")
				taskLock.Unlock()
				return nil
			}
		}
		taskLock.Unlock()
		if !c.AllMapTasksDone() {
			reply.Command = "wait"
			log.Println("No map task available, wait")
			return nil
		}
		taskLock.Lock()
		// assign a reduce task if there is any
		for _, task := range c.ReduceTasks {
			if task.State == Pending {
				reply.FileNames = task.FileNames
				reply.Command = "reduce"
				reply.ReduceTaskNum = task.Num
				task.State = InProgress
				task.StartTime = time.Now().Unix()
				log.Printf("Reduce task %d assigned", task.Num)
				taskLock.Unlock()
				return nil
			}
		}
		taskLock.Unlock()
		// no task left
		reply.FileName = "done"
		reply.Done = true
	default:
		log.Println("Wrong query")
	}
	return nil
}

// check if all map tasks are done
func (c *Coordinator) AllMapTasksDone() bool {
	taskLock.Lock()
	log.Println("Lock in all map tasks done")
	defer taskLock.Unlock()
	for _, task := range c.MapTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) PeriodicHealthCheck() {

	// check if any task is in progress for more than 10 seconds
	// if so, mark it as pending
	log.Println("\n\nPeriodic health check")
	taskLock.Lock()
	log.Println("Lock in periodic health check")
	defer taskLock.Unlock()
	for i, task := range c.MapTasks {
		log.Println("Map task ", i, " state: ", task.State)
		if task.State == InProgress && time.Now().Unix()-task.StartTime > 10 {
			task.State = Pending
			log.Println("Map task ", i, " timed out")
		}
	}
	for i, task := range c.ReduceTasks {
		log.Println("Reduce task ", i, " state: ", task.State)
		if task.State == InProgress && time.Now().Unix()-task.StartTime > 10 {
			task.State = Pending
			log.Println("Reduce task ", i, " timed out")
		}
	}

}

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	taskLock.Lock()
	log.Println("Lock in done")
	defer taskLock.Unlock()
	for _, task := range c.MapTasks {
		if task.State != Completed {
			return false
		}
	}
	for _, task := range c.ReduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	for i, file := range files {
		c.MapTasks = append(c.MapTasks, MapTask{file, Pending, i, 0})
	}
	// run periodic health check
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.PeriodicHealthCheck()
		}
	}()

	c.server()

	return &c
}

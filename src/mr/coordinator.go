package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskQueue    []string
	reduceTaskQueue []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RPCHandler(args *RealArgs, reply *RealReply) error {
	if args.Query == "give me a job" {
		if len(c.mapTaskQueue) > 0 {
			reply.Result = c.mapTaskQueue[0]
			c.mapTaskQueue = c.mapTaskQueue[1:]
		} else if len(c.reduceTaskQueue) > 0 {
			reply.Result = c.reduceTaskQueue[0]
			c.reduceTaskQueue = c.reduceTaskQueue[1:]
		} else {
			reply.Result = "no job"
		}
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
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTaskQueue = append(c.mapTaskQueue, files...)
	fmt.Println(c.mapTaskQueue)

	c.server()
	return &c
}

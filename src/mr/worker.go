package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	isDone := false
	for !isDone {
		time.Sleep(time.Millisecond * 100)
		reply, err := CallReal()
		if err != nil {
			fmt.Println("error in calling coordinator")
			isDone = true
		}
		switch reply.Command {
		case "wait":
			fmt.Println("waiting for a job")
		case "done":
			fmt.Println("all jobs are done")
			isDone = true
		case "map":
			executeMap(mapf, reply.FileName, reply.NReduce, reply.MapTaskNum)
		}
	}
}

func CallReal() (RealReply, error) {
	args := RealArgs{}
	args.Query = "give me a job"
	reply := RealReply{}
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		return reply, nil
	}
	fmt.Println("error in calling coordinator")
	return RealReply{}, errors.New("error in calling coordinator")

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

func executeMap(mapf func(string, string) []KeyValue, filename string, nReduce int, mapTaskNum int) {
	// Open input file
	file, err := os.Open(fmt.Sprintf("../main/%v", filename))
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	// Call map function
	kva := mapf(filename, string(content))
	// Create intermediate files
	filesList := []*json.Encoder{}
	for r := 0; r < nReduce; r++ {
		intermediateFilename := fmt.Sprintf("../main/mr-tmp/mr-%d-%d.txt", mapTaskNum, r)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilename)
		}
		defer intermediateFile.Close()
		filesList = append(filesList, json.NewEncoder(intermediateFile))
	}
	// Write intermediate key value pairs to intermediate files
	for _, kv := range kva {
		fmt.Println(kv.Key)
		fmt.Println(kv.Value)
		reduceNum := ihash(kv.Key) % nReduce
		enc := *filesList[reduceNum]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("error in writing to intermediate file - key: %v, reduce num: %d", kv.Key, reduceNum)
		}
	}
}

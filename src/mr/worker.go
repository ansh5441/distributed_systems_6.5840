package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallReal(mapf, reducef)

}

func CallReal(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// declare an argument structure.
	args := RealArgs{}

	// fill in the argument(s).
	args.Query = "give me a job"

	// declare a reply structure.
	reply := RealReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.RPCHandler" tells the
	// receiving server that we'd like to call
	// the RPCHandler() method of struct Coordinator.
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		if reply.Command == "map" {

			fmt.Println(reply.FileName)
			filename := reply.FileName
			// ../main/filename
			file, err := os.Open(fmt.Sprintf("../main/%v", filename))
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			defer file.Close()
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			kva := mapf(filename, string(content))

			// enc := json.NewEncoder(file)
			filesList := []*json.Encoder{}
			for r := 0; r < reply.NReduce; r++ {
				intermediateFilename := fmt.Sprintf("../main/mr-tmp/mr-%d-%d.txt", reply.MapTaskNum, r)
				intermediateFile, err := os.Create(intermediateFilename)
				if err != nil {
					log.Fatalf("cannot open %v", intermediateFilename)
				}
				defer intermediateFile.Close()
				filesList = append(filesList, json.NewEncoder(intermediateFile))
			}
			for _, kv := range kva {
				fmt.Println(kv.Key)
				fmt.Println(kv.Value)
				reduceNum := ihash(kv.Key) % reply.NReduce
				enc := *filesList[reduceNum]
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("error in writing to intermediate file - key: %v, reduce num: %d", kv.Key, reduceNum)
				}
			}
		}
	} else {
		fmt.Printf("call failed!\n")
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

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
	"sort"
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
		time.Sleep(time.Second)
		reply, err := CallNeedJob()
		if err != nil {
			log.Println("error in calling coordinator")
			isDone = true
		}
		switch reply.Command {
		case "wait":
			log.Println("waiting for a job")
		case "done":
			log.Println("all jobs are done. exiting")
			isDone = true
		case "map":
			success, err := executeMap(mapf, reply.FileName, reply.NReduce, reply.MapTaskNum)
			if err != nil {
				log.Println("error in executing map")
			}
			if success {
				log.Printf("map task %d done", reply.MapTaskNum)
				CallMapSuccess(reply.MapTaskNum)
			}
		case "reduce":
			success, err := executeReduce(reducef, reply.FileNames, reply.ReduceTaskNum)
			if err != nil {
				log.Println("error in executing map")
			}
			if success {
				log.Printf("reduce task %d done", reply.ReduceTaskNum)
			}
		default:
			log.Println("unknown command")

		}
	}
}

func CallNeedJob() (Reply, error) {
	args := Args{}
	args.Query = "give me a job"
	reply := Reply{}
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		return reply, nil
	}
	log.Println("error in calling coordinator")
	return Reply{}, errors.New("error in calling coordinator")
}

func CallMapSuccess(taskNum int) (Reply, error) {
	args := Args{}
	args.Query = "map done"
	args.MapTaskNum = taskNum
	reply := Reply{}
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		return reply, nil
	}
	log.Println("error in calling coordinator")
	return Reply{}, errors.New("error in calling coordinator")
}

func CallReduceSuccess(reduceTaskNum int) (Reply, error) {
	args := Args{}
	args.Query = "reduce done"
	args.ReduceTaskNum = reduceTaskNum
	reply := Reply{}
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if ok {
		return reply, nil
	}
	log.Println("error in calling coordinator")
	return Reply{}, errors.New("error in calling coordinator")
}

func executeReduce(reducef func(string, []string) string, fileNames []string, reduceTaskNum int) (bool, error) {
	// Read intermediate files
	kva := []KeyValue{}
	for _, filename := range fileNames {
		jsonFile, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v", filename)
			return false, err
		}
		defer jsonFile.Close()
		dec := json.NewDecoder(jsonFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Printf("error in decoding json file %v", filename)
				return false, err
			}
			kva = append(kva, kv)
		}

		// Call reduce function
		oname := fmt.Sprintf("../main/mr-out-%d", reduceTaskNum)
		ofile, _ := os.Create(oname)
		defer ofile.Close()

		// sort key value pairs by key
		sort.Slice(kva, func(i, j int) bool {
			return kva[i].Key < kva[j].Key
		})

		// Collect values for each key
		for i := 0; i < len(kva); {
			values := []string{}
			// find the end point for the key
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			// collect all values for the key
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			// Call reduce function
			output := reducef(kva[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
	}

	// Tell coordinator that reduce task is done
	CallReduceSuccess(reduceTaskNum)
	return true, nil
}

func executeMap(mapf func(string, string) []KeyValue, filename string, nReduce int, mapTaskNum int) (bool, error) {
	// Open input file
	file, err := os.Open(fmt.Sprintf("../main/%v", filename))
	if err != nil {
		log.Printf("cannot open %v", filename)
		return false, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return false, err
	}
	// Call map function
	kva := mapf(filename, string(content))
	// Create intermediate files
	filesList := []*json.Encoder{}
	for r := 0; r < nReduce; r++ {
		intermediateFilename := fmt.Sprintf("../main/mr-tmp/mr-%d-%d.txt", mapTaskNum, r)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Printf("cannot open %v", intermediateFilename)
			return false, err
		}
		defer intermediateFile.Close()
		filesList = append(filesList, json.NewEncoder(intermediateFile))
	}
	// Write intermediate key value pairs to intermediate files
	for _, kv := range kva {
		reduceNum := ihash(kv.Key) % nReduce
		enc := *filesList[reduceNum]
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("error in writing to intermediate file - key: %v, reduce num: %d", kv.Key, reduceNum)
			return false, err
		}
	}
	return true, nil
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

	log.Println(err)
	return false
}

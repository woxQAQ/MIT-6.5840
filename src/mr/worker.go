package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// port from mrsequential.go
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type (
	mapfunc    func(string, string) []KeyValue
	reducefunc func(string, []string) string
)

// main/mrworker.go calls this function.
func Worker(mapf mapfunc, reducef reducefunc) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		resp := doHeartbeat()
		log.Printf("Worker: receive heartbeat from coordinator %v\n", resp)
		switch resp.JobType {
		case JobTypeMap:
			doMapTask(mapf, resp)
		case JobTypeReduce:
			doReduceTask(reducef, resp)
		case JobTypeWait:
			time.Sleep(3 * time.Second)
		case JobTypeDone:
			log.Printf("Worker: receive done signal from coordinator")
			return
		default:
			log.Fatalf("error: job type unavailable %v", resp.JobType)
		}
	}
}

func intermediateFileName(mapSeq, reduceSeq int) string {
	return "mr-" + strconv.Itoa(mapSeq) + "-" + strconv.Itoa(reduceSeq)
}

func doMapTask(mapf mapfunc, resp HeartbeatResponse) {
	file, err := os.Open(resp.Filename)
	if err != nil {
		log.Printf("maptask: failed to open file: %s, error: %v", resp.Filename, err)
		return
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("maptask: failed to read file: %s, error: %v", resp.Filename, err)
		return
	}

	res := mapf(resp.Filename, string(content))

	buckets := make([][]KeyValue, resp.NReduce)
	for i := range buckets {
		buckets[i] = make([]KeyValue, 0)
	}
	for _, v := range res {
		idx := ihash(v.Key) % resp.NReduce
		buckets[idx] = append(buckets[idx], v)
	}

	for i := range buckets {
		filename := intermediateFileName(resp.MapTaskSeq, i)
		temp, err := os.CreateTemp("dist", filename+"*")
		if err != nil {
			log.Printf("maptask: failed to create temp dir, error: %v", err)
			return
		}
		enc := json.NewEncoder(temp)

		// err = enc.Encode(buckets[i])
		for _, kv := range buckets[i] {
			err = enc.Encode(kv)
			if err != nil {
				temp.Close()
				return
			}
		}
		os.Rename(temp.Name(), filepath.Join("dist", filename))
		temp.Close()
	}
	doFinish(resp.JobType, resp.MapTaskSeq)
}

func doReduceTask(reducef reducefunc, resp HeartbeatResponse) {
	input := []KeyValue{}
	for i := range resp.NMap {
		filename := intermediateFileName(i, resp.ReduceTaskSeq)
		file, err := os.Open(filepath.Join("dist", filename))
		if err != nil {
			log.Printf("reducetask: failed to open file: %s, error: %v", filename, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var v KeyValue
			err = dec.Decode(&v)
			if err != nil {
				break
			}
			input = append(input, v)
		}
		file.Close()
	}
	oname := "mr-out-" + strconv.Itoa(resp.ReduceTaskSeq)
	ofile, _ := os.CreateTemp("dist", oname+"*")
	sort.Sort(ByKey(input))
	i := 0
	for i < len(input) {
		j := i + 1
		for j < len(input) && input[j].Key == input[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, input[k].Value)
		}
		output := reducef(input[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", input[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
	for i := 0; i < resp.NMap; i++ {
		iname := intermediateFileName(i, resp.ReduceTaskSeq)
		err := os.Remove(filepath.Join("dist", iname))
		if err != nil {
			log.Printf("reducetask: failed to remove file: %s, error: %v", iname, err)
		}
	}
	doFinish(resp.JobType, resp.ReduceTaskSeq)
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
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

	log.Println(err)
	return false
}

func doHeartbeat() HeartbeatResponse {
	resp := HeartbeatResponse{}
	call("Coordinator.HeartBeat", &HeartbeatRequest{}, &resp)
	return resp
}

func doFinish(t JobType, seq int) {
	req := FinishRequest{
		JobType: t,
		Seq:     seq,
	}
	call("Coordinator.FinishWork", &req, &FinishResponse{})
}

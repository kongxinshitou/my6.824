package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Map(mapf func(string, string) []KeyValue, files []string, taskId int, nReduce int, worker string) []string {
	var intermediate []KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	var resFiles []string
	var encoders []*json.Encoder
	for i := 0; i < nReduce; i++ {
		filename := "/home/mit6824/6824/src/tmp_file/" + worker + "Map" + strconv.Itoa(taskId) + strconv.Itoa(i)
		file, err := os.Create(filename)
		if err != nil {
			panic("创建文件失败")
		}
		resFiles = append(resFiles, filename)
		encoders = append(encoders, json.NewEncoder(file))
		defer file.Close()
	}
	for _, kv := range intermediate {
		encoderIdx := ihash(kv.Key) % nReduce
		err := encoders[encoderIdx].Encode(&kv)
		if err != nil {
			panic("worker内部错误")
		}
	}
	return resFiles
}

func Reduce(reducef func(string, []string) string, files []string, taskId int, worker string) []string {
	var intermediate []KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			panic("打开文件失败")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	filename := "/home/mit6824/6824/src/tmp_file/" + worker + "Reduce" + strconv.Itoa(taskId)
	file, _ := os.Create(filename)
	defer file.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		values := []string{intermediate[i].Value}
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return []string{filename}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	waitTime := 1
	worker := "Woker" + strconv.Itoa(os.Getpid())
	for {
		message := WorkerMessage{}
		_ = call("Coordinator.GetTask", "来点工作吧", &message)
		switch message.TaskType {
		case "Done":
			os.Exit(0)
		case "Map":
			resFiles := Map(mapf, message.Task, message.TaskId, message.NReduce, worker)
			ok := false
			commitMessage := CommitMessage{message.TaskId, resFiles}
			call("Coordinator.CommitTask", commitMessage, &ok)
		case "Reduce":
			resFiles := Reduce(reducef, message.Task, message.TaskId, worker)
			ok := false
			commitMessage := CommitMessage{message.TaskId, resFiles}
			call("Coordinator.CommitTask", commitMessage, &ok)
		case "Wait":
			if waitTime > 4 {
				waitTime = 1
			}
			time.Sleep((1 << waitTime) * time.Second)
			waitTime++
			continue
		}
		time.Sleep(2 * time.Second)
		waitTime = 1
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

	fmt.Println(err)
	return false
}

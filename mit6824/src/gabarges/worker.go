package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
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

func createIntermediateFiles(num1, num2 int) {
	base := "mr"
	for i := 0; i < num1; i++ {
		for j := 0; j < num2; j++ {
			file, err := os.Create(fmt.Sprintf("../main/%s-%d-%d", base, i, j))
			defer file.Close()
			if err != nil {
				panic(err)
			}
		}
	}
}

func Maper(filenames []string, f func(string, string) []KeyValue, wg *sync.WaitGroup, mapIdx, numOfReducers int) {
	defer wg.Done()
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Open file failed")
		}
		contents, err := io.ReadAll(file)
		if err != nil {
			log.Fatal("Read file failed")
		}
		file.Close()
		KeyValues := f(filename, string(contents))
		intermediate = append(intermediate, KeyValues...)
	}
	sort.Sort(ByKey(intermediate))

	encs := make([]*json.Encoder, numOfReducers)
	for i := 0; i < numOfReducers; i++ {
		file, err := os.Open(fmt.Sprintf("../main/mr-%d-%d", mapIdx, i))
		defer file.Close()
		if err != nil {
			fmt.Println("Open file failed")
		}
		encs[i] = json.NewEncoder(file)
	}
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % numOfReducers
		if err := encs[idx].Encode(&kv); err != nil {
			panic(err)
		}
	}
}

func Reducer(reducef func(string, []string) string, reducerIdx, numOfMapers int, wg *sync.WaitGroup) {
	defer wg.Done()
	decs := make([]*json.Decoder, numOfMapers)
	intermediate := []KeyValue{}
	for i := 0; i < numOfMapers; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, reducerIdx))
		defer file.Close()
		if err != nil {
			panic(err)
		}
		decs[i] = json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decs[i].Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for i := 0; i < len(intermediate); i++ {
		values := []string{}
		values = append(values, intermediate[i].Value)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
			values = append(values, intermediate[j].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%s %s\n", intermediate[i].Key, output)
		i = j
	}

}

func GetFileList(files []string, num int) [][]string {
	filesLists := make([][]string, num)
	for i := 0; i < num; i++ {
		filesLists[i] = make([]string, 0)
	}
	for _, file := range files {
		idx := ihash(file) % num
		filesLists[idx] = append(filesLists[idx], file)
	}
	return filesLists
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerconfig := WorkerConfig{}
	if ok := call("GetWorkerConfig", "", &workerconfig); !ok {
		panic("Get WorkerConfig failed")
	}
	//TODO revover from panic
	var wg sync.WaitGroup
	wg.Add(workerconfig.NumOfMapers)
	filesLists := GetFileList(workerconfig.Files, workerconfig.NumOfMapers)
	createIntermediateFiles(workerconfig.NumOfMapers, workerconfig.NumOfReducers)
	for i := 0; i < workerconfig.NumOfMapers; i++ {
		go Maper(filesLists[i], mapf, &wg, i, workerconfig.NumOfReducers)
	}
	var wg2 sync.WaitGroup
	wg2.Add(workerconfig.NumOfReducers)
	for i := 0; i < workerconfig.NumOfReducers; i++ {
		go Reducer(reducef, i, workerconfig.NumOfMapers, &wg2)
	}
	wg2.Wait()
	// Your worker implementation here.
	CallExample()
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

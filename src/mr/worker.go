package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, taskResp *GetTaskResp) {
	// Open original files
	filePtr, err := os.Open(taskResp.Filename)
	if err != nil {
		log.Fatalf("Map cannot open taskId=%v, filename=%v", taskResp.Id, taskResp.Filename)
	}
	content, err := ioutil.ReadAll(filePtr)
	if err != nil {
		log.Fatalf("Map cannot read %v", taskResp.Filename)
	}
	filePtr.Close()

	// Core task execution
	kva := mapf(taskResp.Filename, string(content))

	kvaPartitions := make([][]KeyValue, taskResp.NReduce)
	intermediatePtrs := []*os.File{}
	encoders := []*json.Encoder{}
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("map Getwd error, %v", err)
	}

	// Create files and encoders (nReduce)
	for i := 0; i < taskResp.NReduce; i++ {
		intFilename := "mr-" + strconv.Itoa(taskResp.Id) + "-" + strconv.Itoa(i)
		tempPtr, err := ioutil.TempFile(pwd, intFilename)
		if err != nil {
			log.Fatalf("create intermediate file error: err=%v", err)
		}
		enc := json.NewEncoder(tempPtr)
		intermediatePtrs = append(intermediatePtrs, tempPtr)
		encoders = append(encoders, enc)
	}

	// Hash kv pairs into slice
	for _, kv := range kva {
		pid := ihash(kv.Key) % taskResp.NReduce
		kvaPartitions[pid] = append(kvaPartitions[pid], kv)
	}

	// Write kvs into intermediate files and then close them
	for pid := 0; pid < taskResp.NReduce; pid++ {
		err := encoders[pid].Encode(kvaPartitions[pid])
		if err != nil {
			log.Fatalf("Map encode error: kv_pid=%v, err=%v", pid, err)
		}
		intFilename := "mr-" + strconv.Itoa(taskResp.Id) + "-" + strconv.Itoa(pid)
		err = intermediatePtrs[pid].Close()
		if err != nil {
			log.Fatalf("Map close file error: err=%v", err)
		}

		err = os.Rename(intermediatePtrs[pid].Name(), pwd+"/"+intFilename)
		if err != nil {
			log.Fatalf("Map rename file error: old filename=%v, new filename=%v err=%v",
				intermediatePtrs[pid].Name(), pwd+"/"+intFilename, err)
		}
	}
}

func doReduceTask(reducef func(string, []string) string, taskResp *GetTaskResp) {
	// Read all intermediate file mr-X-{id} into vector
	var kvsForR []KeyValue
	for mid := 0; mid < taskResp.NMap; mid++ {
		intFilename := "mr-" + strconv.Itoa(mid) + "-" + strconv.Itoa(taskResp.Id)
		intFile, err := os.Open(intFilename)
		if err != nil {
			log.Fatalf("Worker open intermediate file error, filename=%v, err=%v", intFilename, err)
		}
		var kvsInThisFile []KeyValue
		dec := json.NewDecoder(intFile)
		err = dec.Decode(&kvsInThisFile)
		if err != nil {
			log.Fatalf("Worker decode intermediate file error, filename=%v, err=%v", intFilename, err)
		}
		kvsForR = append(kvsForR, kvsInThisFile...)
		err = intFile.Close()
		if err != nil {
			log.Fatalf("Worker close intermediate file error, filename=%v, err=%v", intFilename, err)
		}

	}

	// Sort all kv pairs
	sort.Sort(ByKey(kvsForR))

	outFilename := "mr-out-" + strconv.Itoa(taskResp.Id)
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("reduce Getwd error, %v", err)
	}
	outFile, err := ioutil.TempFile(pwd, outFilename)
	// outFile, err := os.Create(outFilename)
	if err != nil {
		log.Fatalf("Reduce create out file error, filename=%v, err=%v", outFilename, err)
	}

	// Call reducef once for each distinct Key
	for i := 0; i < len(kvsForR); {
		j := i + 1
		for j < len(kvsForR) && kvsForR[i].Key == kvsForR[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvsForR[k].Value)
		}
		output := reducef(kvsForR[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kvsForR[i].Key, output)
		i = j
	}
	err = outFile.Close()
	if err != nil {
		log.Fatalf("Reduce close output file error, filename=%v, err=%v", outFilename, err)
	}

	err = os.Rename(outFile.Name(), pwd+"/"+outFilename)
	if err != nil {
		log.Fatalf("Reduce rename out file error: old filename=%v, new filename=%v err=%v",
			outFile.Name(), pwd+"/"+outFilename, err)
	}
}

// Get task from master, with task info in resp
func GetTaskFromMaster() *GetTaskResp {
	req := GetTaskReq{}
	resp := GetTaskResp{}
	call("Master.GetTask", &req, &resp)
	return &resp
}

func SendCompleteMsg(taskType, id int) {
	req := CompleteNoticeReq{Id: id, TaskType: taskType}
	call("Master.CompleteNotice", &req, nil)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// fmt.Println("====== mr/worker.go:Worker begin to work =======")

	for {
		taskResp := GetTaskFromMaster()

		if taskResp.TaskType == MapTask {
			doMapTask(mapf, taskResp)
			SendCompleteMsg(MapTask, taskResp.Id)
		} else if taskResp.TaskType == ReduceTask {
			doReduceTask(reducef, taskResp)
			SendCompleteMsg(ReduceTask, taskResp.Id)
		} else {
			break
		}
	}

	// fmt.Println("====== mr/worker.go:Worker exit =======")
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, req interface{}, resp interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, req, resp)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	MapTask = iota
	Reserve1
	Reserve2
	ReduceTask
	AllDone
)

const (
	Pending = iota
	Processing
	Finished
)

type MRTask struct {
	Id       int
	Filename string
	Status   int32
	TaskType int
}

func (t *MRTask) String() string {
	return "<" + strconv.Itoa(t.Id) + ", " + t.Filename + ", " + strconv.Itoa(int(t.Status)) + ", " + strconv.Itoa(t.TaskType) + ">"
}

type Master struct {
	// Your definitions here.
	mapTasks        []*MRTask
	reduceTasks     []*MRTask
	mapFinishNum    int32
	reduceFinishNum int32
	nReduce         int
	nMap            int

	timeout int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(req *GetTaskReq, resp *GetTaskResp) error {
	if req == nil {
		log.Fatal("GetTask req is nil")
	}
	if resp == nil {
		log.Fatal("GetTask resp is nil")
	}

	// Map tasks not all finished (may be all assigned)
	// In a new round, possible cases are:
	//  1. Remaining pening map tasks are all finished, break the loop
	//  2. Not finished, still waiting...
	//  3. A map task failed and returned Pending state, pick it
	//  4. 3 was picked by other worker, still waiting...
	//
	// Synchronize PRC, worker might keep waiting for several tens of seconds
	for atomic.LoadInt32(&m.mapFinishNum) < int32(m.nMap) {
		for i := 0; i < m.nMap; i++ {
			if atomic.CompareAndSwapInt32(&(m.mapTasks[i].Status), Pending, Processing) {
				resp.Filename = m.mapTasks[i].Filename
				resp.Id = i
				resp.NMap = m.nMap
				resp.NReduce = m.nReduce
				resp.TaskType = MapTask
				log.Println("Master send map task: " + m.mapTasks[i].String())

				// The task, if hasn't finished in timeout seconds,
				//  should be regarded as worker process failure.
				go func(mid int) {
					time.Sleep(time.Duration(m.timeout) * time.Second)
					if atomic.CompareAndSwapInt32(&(m.mapTasks[mid].Status), Processing, Pending) {
						log.Printf("Map task %v timeout\n", mid)
					}
				}(i)

				return nil
			}
		}
		// When reach here, map tasks are all assigned, but some are not finished yet
		log.Println("map all assigned")
		time.Sleep(time.Duration(3) * time.Second)
	}

	if atomic.LoadInt32(&m.mapFinishNum) != int32(m.nMap) {
		log.Fatalf("Core error, mapFinishNum=%v", m.mapFinishNum)
	}

	// Assign a reduce task
	for atomic.LoadInt32(&m.reduceFinishNum) < int32(m.nReduce) {
		for i := 0; i < m.nReduce; i++ {
			if atomic.CompareAndSwapInt32(&(m.reduceTasks[i].Status), Pending, Processing) {
				resp.Id = i
				resp.NMap = m.nMap
				resp.NReduce = m.nReduce
				resp.TaskType = ReduceTask
				log.Printf("Master send reduce task %v", i)

				go func(rid int) {
					time.Sleep(time.Duration(m.timeout) * time.Second)
					if atomic.CompareAndSwapInt32(&(m.reduceTasks[rid].Status), Processing, Pending) {
						log.Printf("Reduce task %v timeout\n", rid)
					}
				}(i)

				return nil
			}
		}
		// When reach here, reduce tasks are all assigned, but some are not finished yet
		time.Sleep(time.Duration(3) * time.Second)
	}

	log.Println("reduce all finished")
	resp.TaskType = AllDone
	return nil
}

// Finish notice sent by worker
func (m *Master) FinishNotice(req *FinishNoticeReq, resp *FinishNoticeResp) error {
	if req == nil {
		log.Fatal("finish notice req is nil")
	}

	if req.TaskType == MapTask {
		if atomic.CompareAndSwapInt32(&(m.mapTasks[req.Id].Status), Processing, Finished) {
			atomic.AddInt32(&m.mapFinishNum, 1)
		}
	} else if req.TaskType == ReduceTask {
		if atomic.CompareAndSwapInt32(&(m.reduceTasks[req.Id].Status), Processing, Finished) {
			atomic.AddInt32(&m.reduceFinishNum, 1)
		}
	} else {
		log.Printf("Duplicated finish set on task %v\n", req.Id)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	if atomic.LoadInt32(&m.reduceFinishNum) == int32(m.nReduce) {
		return true
	}
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce: nReduce,
		nMap:    0,
		timeout: 10,
	}

	// Your code here.
	// Create map tasks of num nMap (= file num)
	for _, f := range files {
		log.Println("file: " + f)
		task := &MRTask{
			Id:       m.nMap,
			Filename: f,
			Status:   Pending,
			TaskType: MapTask,
		}

		m.mapTasks = append(m.mapTasks, task)
		m.nMap++
	}

	// Create reduce tasks (=nReduce)
	for i := 0; i < m.nReduce; i++ {
		task := &MRTask{
			Id:       i,
			Status:   Pending,
			TaskType: ReduceTask,
		}
		m.reduceTasks = append(m.reduceTasks, task)
	}

	m.server()
	return &m
}

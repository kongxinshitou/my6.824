package mr

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// TaskImplement

type TaskMessage struct {
	task  []string //任务的文件列表
	state int      //任务的状态0 表示未分配 1表示正在运行中 2 表示已经完成
}

var state2WorkType = map[int]string{
	-1: "Wait",
	0:  "Map",
	1:  "Reduce",
	2:  "Done",
}

type TaskInfo struct {
	taskStack []int         //用于保存任务的编号
	taskTable []TaskMessage //用于保存每个任务的详细信息
	state     int
	nReduce   int

	stateLock sync.RWMutex
	tableLock sync.RWMutex
	stackLock sync.RWMutex
}

type TaskScheduler interface {
	AllocateTask() (int, []string, int, error)
	RecycleTask(int)
	TaskHealth(int)
	CreateTask([][]string)
	Check()
	Commit(CommitMessage) error
}

// 不做优雅推出,在worker那里直接停止
// 这个函数不是并发安全的,要加锁
func (t *TaskInfo) AllocateTask() (int, []string, int, error) {
	//ReadLock of stateLock
	t.stateLock.RLock()
	defer t.stateLock.RUnlock()
	t.stackLock.Lock()
	defer t.stackLock.Unlock()
	t.tableLock.Lock()
	defer t.tableLock.Unlock()

	//Lock of stackLock
	state := t.state
	if state == 2 {
		return -1, nil, state, nil
	}
	//正处于map状态中,并且所有的map任务都分配完,返回让worker等待的命令
	if len(t.taskStack) <= t.nReduce && state == 0 {
		return -1, nil, -1, nil
	}
	//处于reduce状态中, 并且所有的reduce任务已经分配完, 返回让worker等待的命令
	if len(t.taskStack) == 0 {
		return -1, nil, -1, nil
	}
	taskIdx := t.taskStack[len(t.taskStack)-1]
	t.taskStack = t.taskStack[0 : len(t.taskStack)-1]

	t.taskTable[taskIdx].state = 1
	go t.TaskHealth(taskIdx)
	return taskIdx, t.taskTable[taskIdx].task, state, nil
}

func (t *TaskInfo) RecycleTask(taskIdx int) {
	t.stackLock.Lock()
	defer t.stackLock.Unlock()
	t.tableLock.Lock()
	defer t.tableLock.Unlock()
	t.taskTable[taskIdx].state = 0
	t.taskStack = append(t.taskStack, taskIdx)
}

func (t *TaskInfo) CreateTask(files []string) {
	numFiles := len(files)
	for i := 0; i < t.nReduce; i++ {
		t.taskTable = append(t.taskTable, TaskMessage{
			task:  nil,
			state: 0,
		})
		t.taskStack = append(t.taskStack, i)
	}
	for i := t.nReduce; i < numFiles+t.nReduce; i++ {
		t.taskTable = append(t.taskTable, TaskMessage{
			task:  []string{files[i-t.nReduce]},
			state: 0,
		})
		t.taskStack = append(t.taskStack, i)
	}
	fmt.Println("任务已经处理加载完成")
}

func (t *TaskInfo) TaskHealth(taskIdx int) {
	time.Sleep(time.Second * 20)
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()
	if t.taskTable[taskIdx].state == 1 {
		go t.RecycleTask(taskIdx)
	}

}

func (t *TaskInfo) Commit(task CommitMessage) error {
	//判断任务状态, 如果任务状态正在运行则改写任务状态, 其他情况下不做处理. 对taskTable做了读写两种操作, 上写锁
	t.tableLock.Lock()
	defer t.tableLock.Unlock()
	if t.taskTable[task.TaskIdx].state == 1 {
		t.taskTable[task.TaskIdx].state = 2
	} else {
		return fmt.Errorf("提交任务失败")
	}
	if task.TaskIdx >= t.nReduce {
		for _, file := range task.CommitFiles {
			taskLabel := int(file[len(file)-1] - '0')
			t.taskTable[taskLabel].task = append(t.taskTable[taskLabel].task, file)
		}
		go t.Check()
		return nil
	}

	srcFile, err := os.Open(task.CommitFiles[0])
	defer srcFile.Close()
	if err != nil {
		panic("打开文件失败")
	}
	desFile, err := os.Create("mr-out-" + strconv.Itoa(task.TaskIdx))
	if err != nil {
		panic("创建文件失败")
	}
	defer desFile.Close()
	_, err = io.Copy(desFile, srcFile)
	if err != nil {
		panic("写入文件失败")
	}
	if err != nil {
		panic("服务器")
	}
	go t.Check()
	return nil
}

func (t *TaskInfo) Check() {
	t.stateLock.Lock()
	defer t.stateLock.Unlock()
	t.tableLock.RLock()
	defer t.tableLock.RUnlock()
	for i := len(t.taskTable) - 1; i >= t.nReduce; i-- {
		if t.taskTable[i].state != 2 {
			t.state = 0
			return
		}
	}
	for i := t.nReduce; i >= 0; i-- {
		if t.taskTable[i].state != 2 {
			t.state = 1
			return
		}
	}
	t.state = 2
	return
}

type Coordinator struct {
	// Your definitions here.
	taskInfo *TaskInfo
}
type Master interface {
	TaskScheduler
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Init(files []string, nReduce int) {
	c.taskInfo = &TaskInfo{
		taskTable: []TaskMessage{},
	}
	c.taskInfo.nReduce = nReduce
	c.taskInfo.CreateTask(files)
}

func (c *Coordinator) GetTask(worker string, message *WorkerMessage) error {
	//c.workerInfo.CreateWorker(worker)
	taskIdx, task, state, err := c.taskInfo.AllocateTask()
	if err != nil {
		return err
	}
	message.TaskId = taskIdx
	message.Task = task
	message.NReduce = c.taskInfo.nReduce
	message.TaskType = state2WorkType[state]
	return err
}

func (c *Coordinator) CommitTask(task CommitMessage, ok *bool) error {
	var err error
	c.taskInfo.Commit(task)
	if err != nil {
		*ok = false
		return err
	}
	*ok = true

	return err
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
	c.taskInfo.stateLock.RLock()
	if c.taskInfo.state == 2 {
		ret = true
	}
	c.taskInfo.stateLock.RUnlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.Init(files, nReduce)
	c.server()
	return &c
}

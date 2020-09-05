package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus struct {
	status    int
	workerId  int
	startTime time.Time
}

type Master struct {
	// Your definitions here.

	files     []string
	nReduce   int
	taskPhase TaskPhase

	taskStatus []TaskStatus
	mu         sync.Mutex
	done       bool

	workerSeq   int
	taskChannel chan Task
}

type TaskPhase int

const (
	MAP_PHASE    TaskPhase = 0
	REDUCE_PHASE TaskPhase = 1
)

type Task struct {
	Filename string
	NReduce  int
	NMap     int
	Seq      int
	Phase    TaskPhase
	IsAlive  bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskChannel
	reply.Task = &task

	if task.IsAlive {
		m.registerTask(args, task)
	}
	log.Println("Getting a Task for assigning it to worker")
	return nil
}

func (m *Master) ReportTask(args *TaskReport, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Println("Task report in master")
	if m.taskPhase != args.Phase || args.WorkerId != m.taskStatus[args.Seq].workerId {
		return nil
	}

	if args.Done {
		m.taskStatus[args.Seq].status = FINISH
	} else {
		m.taskStatus[args.Seq].status = ERRORED
	}

	go m.schedule()
	return nil
}

func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	return nil
}

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Started listening for socket mr-socket")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) initMapTask() {
	m.taskPhase = MAP_PHASE
	m.taskStatus = make([]TaskStatus, len(m.files))
}

const (
	MAPTASK_TIMEOUT = time.Second * 30
)

const (
	READY   = 0
	QUEUED  = 1
	RUNNING = 2
	FINISH  = 3
	ERRORED = 4
)

func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(MAPTASK_TIMEOUT)
	}
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	allFinish := true
	for index, task := range m.taskStatus {
		switch task.status {
		case READY:
			allFinish = false
			m.taskChannel <- m.getTask(index)
			m.taskStatus[index].status = QUEUED

		case QUEUED:
			allFinish = false
		case RUNNING:
			allFinish = false
			if time.Now().Sub(task.startTime) > MAPTASK_TIMEOUT {
				m.taskStatus[index].status = QUEUED
				m.taskChannel <- m.getTask(index)
			}
		case FINISH:
		case ERRORED:
			allFinish = false
			m.taskStatus[index].status = QUEUED
			m.taskChannel <- m.getTask(index)
		default:
			log.Panic("Error while scheduling a Task")
		}
	}

	if allFinish {
		if m.taskPhase == MAP_PHASE {
			m.initReduceTask()
		} else {
			m.done = true
			// stop the socket
			os.Remove("mr-socket")
		}
	}

}

func (m *Master) getTask(index int) Task {

	task := Task{
		Filename: "",
		NReduce:  m.nReduce,
		NMap:     len(m.files),
		Seq:      index,
		Phase:    m.taskPhase,
		IsAlive:  true,
	}
	log.Printf("m: %+v, Task Seq: %d, len files: %d, len tasks: %d\n", m, index, len(m.files), len(m.taskStatus))

	if task.Phase == MAP_PHASE {
		task.Filename = m.files[index]
	}
	return task
}

func (m *Master) initReduceTask() {
	log.Println("Starting reduce Phase")
	m.taskPhase = REDUCE_PHASE
	m.taskStatus = make([]TaskStatus, m.nReduce)
}

func (m *Master) registerTask(args *TaskArgs, task Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		log.Panic("The Task Phase reported by worker does not match with master Task Phase assignment to worker")
	}

	m.taskStatus[task.Seq].status = RUNNING
	m.taskStatus[task.Seq].workerId = args.WorkerId
	m.taskStatus[task.Seq].startTime = time.Now()

}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files

	if nReduce > len(files) {
		m.taskChannel = make(chan Task, nReduce)
	} else {
		m.taskChannel = make(chan Task, len(m.files))
	}

	log.Println("Initializing map Task")
	m.initMapTask()
	go m.tickSchedule()

	m.server()
	log.Println("Master process is started")

	return &m
}

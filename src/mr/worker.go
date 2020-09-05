package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef

	// register worker with the master
	w.register()

	// executes the task (either map or reduce)
	w.run()
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func (w *worker) run() {
	// the for loop keeps the worker alive until all the tasks are completed
	for {
		task := w.requestTask()
		if !task.IsAlive {
			log.Println("The received Task is not alive")
			return
		}
		w.doTask(task)
		time.Sleep(time.Second * 10)
	}
}

// request one task from the master using RPC
func (w *worker) requestTask() Task {

	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		log.Println("Failed to get the Task")

		// could not find the master process
		// possible improvements:
		//		1. Add a retry with a delay, it could be due to network issue
		//		2. Send graceful termination from `master` to all the registered
		//		   `worker`(s) when all the tasks are completed
		os.Exit(1)
	}
	log.Printf("Worker Task: %+v\n", reply.Task)
	return *reply.Task
}

// register worker with the master and get the worker id
func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterArgs{}

	if ok := call("Master.RegisterWorker", args, reply); !ok {
		log.Fatal("Register of worker failed")
	}
	w.id = reply.WorkerId
}

func (w *worker) doTask(t Task) {

	switch t.Phase {
	case MAP_PHASE:
		w.doMapTask(t)
	case REDUCE_PHASE:
		w.doReduceTask(t)
	default:
		log.Panic(fmt.Sprint("Unknown Task Phase %v", t.Phase))
	}
}

func (w *worker) doMapTask(t Task) {

	contents, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	keyValues := w.mapf(t.Filename, string(contents))
	reduces := make([][]KeyValue, t.NReduce)

	for _, kv := range keyValues {
		index := ihash(kv.Key) % t.NReduce
		reduces[index] = append(reduces[index], kv)
	}

	for index, l := range reduces {
		filename := reduceName(t.Seq, index)
		f, err := os.Create(filename)
		if err != nil {
			w.reportTask(t, false, err)
		}

		encoding := json.NewEncoder(f)
		for _, keyvalue := range l {
			if err := encoding.Encode(&keyvalue); err != nil {
				w.reportTask(t, false, err)
			}
		}

		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

func reduceName(mapIndex, reduceIndex int) string {
	return fmt.Sprintf("mapIndex-%d-reduceIndex-%d", mapIndex, reduceIndex)
}

func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for i := 0; i < t.NMap; i++ {
		filename := reduceName(i, t.Seq)

		file, error := os.Open(filename)
		if error != nil {
			w.reportTask(t, false, error)
			return
		}

		decode := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decode.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}
	w.reportTask(t, true, nil)
}

func mergeName(reduceIndex int) string {
	return fmt.Sprintf("reduce-output-%d", reduceIndex)
}

func (w *worker) reportTask(t Task, done bool, err error) {

	if err != nil {
		log.Printf("%v\n", err)
	}

	args := TaskReport{
		Done:     done,
		Seq:      t.Seq,
		Phase:    t.Phase,
		WorkerId: w.id,
	}

	reply := TaskReply{}

	if ok := call("Master.ReportTask", &args, &reply); !ok {
		log.Println("Error in Task reporting")
	}
}

type RegisterArgs struct {
	WorkerId int
}

//
// example function to show how to make an RPC call to the master.
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
	log.Printf("reply.Y %v\n\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Fatal(err)
	return false
}

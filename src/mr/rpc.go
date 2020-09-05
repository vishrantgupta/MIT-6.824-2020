package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
}

type TaskReport struct {
	Done     bool
	Seq      int
	Phase    TaskPhase
	WorkerId int
}

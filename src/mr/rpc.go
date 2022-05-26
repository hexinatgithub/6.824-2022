package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

// Add your RPC definitions here.

type Type int

const (
	ExitType = iota + 1
	MapType
	ReduceType
	StandByType
)

var ts = map[int]string{
	1: "Exit",
	2: "Map",
	3: "Reduce",
	4: "StandBy",
}

func (t Type) String() string {
	return ts[int(t)]
}

type Ask struct{}

type Task struct {
	Type    Type
	Files   []string
	NReduce int
	// map or reduce task number
	// Id is 0 means exit
	Id int
}

func (t *Task) String() string {
	switch t.Type {
	case MapType:
		return fmt.Sprintf("Task{TaskId: %d, Type: %s, Files: %v, NReduce: %d}", t.Id, t.Type, t.Files, t.NReduce)
	case ReduceType:
		return fmt.Sprintf("Task{TaskId: %d, Type: %s, Files: %v}", t.Id, t.Type, t.Files)
	default:
		return fmt.Sprintf("Task{TaskId: %d, Type: %s}", t.Id, t.Type)
	}
}

type Report struct {
	Type  Type
	Id    int
	Files []string
}

func (ts *Report) String() string {
	switch ts.Type {
	case MapType:
		return fmt.Sprintf("Task{TaskId: %d, Type: %s, Intermediate: %v} finished", ts.Id, ts.Type, ts.Files)
	case ReduceType:
		return fmt.Sprintf("Task{TaskId: %d, Type: %s, Output: %v} finished", ts.Id, ts.Type, ts.Files)
	default:
		return fmt.Sprintf("Task{TaskId: %d, Type: %s} unknown", ts.Id, ts.Type)
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

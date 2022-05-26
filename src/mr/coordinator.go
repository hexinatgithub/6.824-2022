package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type State int

const (
	MapState State = iota
	ReduceState
	DoneState
)

const (
	timeout uint64 = 10
)

type tracker interface {
	// getTask return Task if current has one,
	// return nil if current task is all given to
	// worker
	getTask() *Task
	// finish report the id worker's job is finish
	finish(id int)
	done() bool
	tick()
	output(output []string)
}

type mapInProgressItem struct {
	index int
	files []string
	tick  uint64
}

type mapTaskTracker struct {
	nReduce  int
	files    []string
	todo     []int
	complete int
	// inProgress is map task id map to files
	inProgress map[int]*mapInProgressItem
	intr       intermediate
	timeout    uint64
}

func makeMapTaskTracker(
	nReduce int, timeout uint64,
	intr intermediate, files []string,
) *mapTaskTracker {
	todo := make([]int, len(files))
	for i := range files {
		todo[i] = i + 1
	}

	return &mapTaskTracker{
		nReduce:    nReduce,
		files:      files,
		todo:       todo,
		inProgress: make(map[int]*mapInProgressItem),
		intr:       intr,
		timeout:    timeout,
	}
}

func (t *mapTaskTracker) getTask() *Task {
	if len(t.todo) == 0 {
		return nil
	}

	id := t.todo[0]
	t.todo = t.todo[1:]

	if len(t.todo) == 0 {
		t.todo = nil
	}

	t.inProgress[id] = &mapInProgressItem{
		files: []string{t.files[id-1]},
		index: id,
	}
	task := &Task{
		Type:    MapType,
		Files:   []string{t.files[id-1]},
		NReduce: t.nReduce,
		Id:      id,
	}
	return task
}

func (t *mapTaskTracker) finish(id int) {
	if _, ok := t.inProgress[id]; ok {
		t.complete++
		delete(t.inProgress, id)
	}
}

func (t *mapTaskTracker) done() bool {
	return t.complete == len(t.files)
}

func (t *mapTaskTracker) tick() {
	var failed []int

	for i := range t.inProgress {
		item := t.inProgress[i]
		item.tick++
		if item.tick >= t.timeout {
			failed = append(failed, i)
			log.Printf("worker %d map task timeout, revoke %v\n",
				i, item.files)
		}
	}

	t.todo = append(t.todo, failed...)
	for _, i := range failed {
		delete(t.inProgress, i)
	}
}

func (t *mapTaskTracker) output(output []string) {
	t.intr.add(output)
}

type reduceInProgressItem struct {
	tick uint64
}

type reduceTaskTracker struct {
	total    int
	todo     []int
	complete int

	intermediate intermediate
	// inProgress is reduce task id map to item
	inProgress map[int]*reduceInProgressItem
	timeout    uint64
	result     []string
}

func makeReduceTaskTracker(
	nReduce int, timeout uint64,
	intr intermediate,
) *reduceTaskTracker {
	todo := make([]int, nReduce)
	for i := range todo {
		todo[i] = i + 1
	}

	return &reduceTaskTracker{
		total:        nReduce,
		todo:         todo,
		intermediate: intr,
		inProgress:   make(map[int]*reduceInProgressItem),
		timeout:      timeout,
	}
}

func (t *reduceTaskTracker) getTask() *Task {
	if len(t.todo) == 0 {
		return nil
	}

	id := t.todo[0]
	t.todo = t.todo[1:]

	if len(t.todo) == 0 {
		t.todo = nil
	}

	t.inProgress[id] = &reduceInProgressItem{}
	task := &Task{
		Type:  ReduceType,
		Files: t.intermediate.index(id),
		Id:    id,
	}
	return task
}

func (t *reduceTaskTracker) finish(id int) {
	if _, ok := t.inProgress[id]; ok {
		t.complete++
		delete(t.inProgress, id)
	}
}

func (t *reduceTaskTracker) done() bool {
	return t.complete == t.total
}

func (t *reduceTaskTracker) tick() {
	var failed []int
	for i := range t.inProgress {
		item := t.inProgress[i]
		item.tick++
		if item.tick >= t.timeout {
			failed = append(failed, i)
			log.Printf("reduce task %d timeout, revoke\n", i)
		}
	}

	t.todo = append(t.todo, failed...)
	for _, i := range failed {
		delete(t.inProgress, i)
	}
}

func (t *reduceTaskTracker) output(filenames []string) {
	t.result = append(t.result, filenames...)
}

type askWithResult struct {
	ask    *Ask
	result chan *Task
}

type reportWithResult struct {
	report *Report
	result chan *Task
}

type indexer interface {
	index(id uint64) []string
}

type intermediate map[int][]string

func (i intermediate) index(id int) []string {
	return i[id]
}

func (intr intermediate) add(filenames []string) {
	for i := range filenames {
		filename := filenames[i]
		ss := strings.Split(filename, "-")
		if len(ss) != 3 {
			panic("intermediate file format")
		}
		reduceId, err := strconv.Atoi(ss[2])
		if err != nil {
			panic("intermediate file reduceId")
		}
		intr[reduceId] = append(intr[reduceId], filename)
	}
}

type Coordinator struct {
	nReduce int
	state   State

	input        []string
	intermediate intermediate
	trk          tracker

	ask    chan askWithResult
	report chan reportWithResult
	done   chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) run() {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case tr := <-c.ask:
			task := c.trk.getTask()
			if task == nil {
				task = &Task{
					Type: StandByType,
				}
			}
			tr.result <- task
			log.Printf("assign %s to worker\n", task)
		case rr := <-c.report:
			c.trk.finish(rr.report.Id)
			log.Println(rr.report)

			if c.state == MapState {
				c.trk.output(rr.report.Files)
			}

			if c.trk.done() {
				if c.state == MapState {
					c.trk = makeReduceTaskTracker(c.nReduce, timeout, c.intermediate)
					c.state = ReduceState
					log.Println("coordinator go to reduce phase")
				} else if c.state == ReduceState {
					c.state = DoneState
					close(c.done)
					ticker.Stop()

					task := &Task{
						Id:   rr.report.Id,
						Type: ExitType,
					}
					rr.result <- task
					log.Println("coordinator done")
					return
				} else {
					log.Panic("unknown state")
				}
			}

			task := c.trk.getTask()
			if task == nil {
				task = &Task{
					Type: StandByType,
				}
			}
			rr.result <- task
			log.Printf("assign %s to worker\n", task)
		case <-ticker.C:
			c.trk.tick()
		}
	}
}

func (c *Coordinator) GetTask(args *Ask, reply *Task) error {
	ar := askWithResult{
		ask:    args,
		result: make(chan *Task),
	}
	c.ask <- ar
	*reply = *<-ar.result
	close(ar.result)
	return nil
}

func (c *Coordinator) Report(args *Report, reply *Task) error {
	rr := reportWithResult{
		report: args,
		result: make(chan *Task),
	}
	c.report <- rr
	*reply = *<-rr.result
	close(rr.result)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	<-c.done
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.state = MapState
	c.input = files
	c.intermediate = make(intermediate, nReduce)
	c.trk = makeMapTaskTracker(nReduce, timeout, c.intermediate, files)
	c.ask = make(chan askWithResult)
	c.report = make(chan reportWithResult)
	c.done = make(chan struct{})

	c.server()
	go c.run()
	return &c
}

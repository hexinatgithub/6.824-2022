package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var (
		task *Task
		err  error
		id   int
	)

	for {
		// task != nil means pre-task is done,
		// but coordinator given an other task to do.
		if task == nil {
			task, err = getTask()
			if err != nil {
				log.Fatal("get task")
				return
			}
		}

		id = task.Id
		log.Printf("worker get %s\n", task)

		switch task.Type {
		case MapType:
			for _, file := range task.Files {
				coder := jsonCoder{}
				writer := makeIntermediateFileReadWriter(&coder)
				kva := mapf(file, rawcontent(file))

				for i := range kva {
					kv := kva[i]
					redId := ihash(kv.Key)%task.NReduce + 1
					intermediate := fmt.Sprintf("mr-%d-%d", id, redId)
					writer.append(intermediate, &kv)
				}

				writer.done(true)

				rept := Report{
					Type:  MapType,
					Id:    id,
					Files: writer.files(),
				}
				task, err = report(rept)
				if err != nil {
					log.Fatal("report map")
					return
				}
				log.Println(&rept)
			}
		case ReduceType:
			coder := jsonCoder{}
			ms := makeMergeSort(&coder, task.Files)
			ms.sort()
			ms.reader.done(false)

			intermediate := ms.aggregate
			output := fmt.Sprintf("mr-out-%d", id)
			writer := makeOutputWriter(output)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				content := fmt.Sprintf("%v %v\n", intermediate[i].Key, output)
				writer.append(content)

				i = j
			}

			writer.done()

			rept := Report{
				Type:  ReduceType,
				Id:    id,
				Files: []string{output},
			}
			task, err = report(rept)
			if err != nil {
				log.Fatal("report reduce status")
				return
			}
			log.Println(&rept)
		case ExitType:
			return
		default:
			task = nil
			time.Sleep(time.Millisecond * 500)
		}
	}
}

type mergeSort struct {
	aggregate []KeyValue
	reader    *intermediateFileReadWriter
	filenames []string
}

func (a *mergeSort) sort() {
	var kva []KeyValue
	for _, filename := range a.filenames {
		kva = append(kva, a.reader.read(filename)...)
	}
	sort.Sort(ByKey(kva))
	a.aggregate = kva
}

func makeMergeSort(coder coder, filenames []string) *mergeSort {
	return &mergeSort{
		reader:    makeIntermediateFileReadWriter(coder),
		filenames: filenames,
	}
}

func rawcontent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

type coder interface {
	encode(file *os.File, value interface{}) error
	decode(file *os.File, value interface{}) error
}

type jsonCoder struct {
	encoder map[*os.File]*json.Encoder
	decoder map[*os.File]*json.Decoder
}

func (c *jsonCoder) encode(file *os.File, value interface{}) error {
	if c.encoder == nil {
		c.encoder = make(map[*os.File]*json.Encoder)
	}
	encoder, ok := c.encoder[file]
	if !ok {
		encoder = json.NewEncoder(file)
		c.encoder[file] = encoder
	}
	return encoder.Encode(value)
}

func (c *jsonCoder) decode(file *os.File, value interface{}) error {
	if c.decoder == nil {
		c.decoder = make(map[*os.File]*json.Decoder)
	}
	decoder, ok := c.decoder[file]
	if !ok {
		decoder = json.NewDecoder(file)
		c.decoder[file] = decoder
	}
	return decoder.Decode(value)
}

type intermediateFileReadWriter struct {
	set   map[string]*os.File
	coder coder
}

func (i *intermediateFileReadWriter) files() []string {
	var files []string
	for filename := range i.set {
		files = append(files, filename)
	}
	return files
}

func (i *intermediateFileReadWriter) done(write bool) {
	for filename := range i.set {
		if write {
			os.Rename(i.set[filename].Name(), filename)
		}
		i.set[filename].Close()
		i.set[filename] = nil
	}
}

func (i *intermediateFileReadWriter) append(filename string, kv *KeyValue) {
	var (
		file *os.File
		ok   bool
		err  error
	)

	if file, ok = i.set[filename]; !ok {
		file, err = os.CreateTemp("", filename+"-*")
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		i.set[filename] = file
	}

	if err := i.coder.encode(file, kv); err != nil {
		log.Fatalf("cannot encode KeyValue")
	}
}

func (i *intermediateFileReadWriter) read(filename string) []KeyValue {
	var (
		kva  []KeyValue
		file *os.File
		ok   bool
		err  error
	)

	if file, ok = i.set[filename]; !ok {
		file, err = os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %s", filename)
		}
		i.set[filename] = file
	}

	for {
		var kv KeyValue
		if err := i.coder.decode(file, &kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func makeIntermediateFileReadWriter(coder coder) *intermediateFileReadWriter {
	return &intermediateFileReadWriter{
		set:   make(map[string]*os.File),
		coder: coder,
	}
}

type outputWriter struct {
	file     *os.File
	filename string
}

func (o *outputWriter) append(content string) {
	_, err := fmt.Fprintf(o.file, content)
	if err != nil {
		log.Fatalf("cannot append %v", o.file.Name())
	}
}

func (o *outputWriter) done() {
	os.Rename(o.file.Name(), o.filename)
	o.file.Close()
	o.file = nil
}

func makeOutputWriter(filename string) *outputWriter {
	file, err := os.CreateTemp("", filename+"-*")
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	return &outputWriter{
		file:     file,
		filename: filename,
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func getTask() (*Task, error) {

	// declare an argument structure.
	args := Ask{}

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply, nil
	}
	return nil, errors.New("get task failed!")
}

func report(status Report) (*Task, error) {
	args := status
	reply := Task{}

	ok := call("Coordinator.Report", &args, &reply)
	if ok {
		return &reply, nil
	}
	return nil, errors.New("report failed!")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

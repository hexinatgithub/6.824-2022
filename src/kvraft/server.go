package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dGet       logTopic = "Get"
	dPutAppend logTopic = "PutAppend"
	dApply     logTopic = "Apply"
	dSnap      logTopic = "Snap"
	dRestore   logTopic = "Restore"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

type Action int

const (
	ActGet Action = iota
	ActPut
	ActAppend
)

var (
	ats = map[Action]string{
		ActGet:    "Get",
		ActPut:    "Put",
		ActAppend: "Append",
	}
	sta = map[string]Action{
		"Get":    ActGet,
		"Put":    ActPut,
		"Append": ActAppend,
	}
)

func (act Action) String() string {
	return ats[act]
}

func action(op string) Action {
	return sta[op]
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	Action Action

	ClientId  int64
	RequestId int64
}

func (o Op) String() string {
	return fmt.Sprintf(`Op{Key: "%s", Value: "%s", Action: "%s", ClientId: %d, RequestId: %d}`,
		o.Key, o.Value, o.Action, o.ClientId, o.RequestId)
}

type commitWait struct {
	index  int
	term   int
	op     Op
	result chan Result
}

func (c commitWait) String() string {
	return fmt.Sprintf(`commitWait{term: %d, op: %s}`, c.term, c.op)
}

type Result struct {
	Err   Err
	Value string
}

func (r Result) String() string {
	if r.Value == "" {
		return fmt.Sprintf(`result{Err: "%s"}`, r.Err)
	}
	return fmt.Sprintf(`result{Err: "%s", Value: "%s"}`, r.Err, r.Value)
}

type Track struct {
	RequestId int64
	Result    Result
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	wait      map[int]commitWait
	stopCh    chan struct{}

	state       map[string]string
	session     map[int64]Track
	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Action:    ActGet,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	wait, wrongLeader := kv.commitOp(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		Debug(dGet, "%s is not leader, %s is ignored", kv, args)
		return
	}

	Debug(dGet, "%s handle %s, start commit %d Log, wait for result", kv, args, wait.index)
	result := <-wait.result
	reply.Value = result.Value
	reply.Err = result.Err
	Debug(dGet, "%s reply %s with %s", kv, args, result)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Action:    action(args.Op),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	wait, wrongLeader := kv.commitOp(op)
	if wrongLeader {
		reply.Err = ErrWrongLeader
		Debug(dPutAppend, "%s is not leader, %s is ignored", kv, args)
		return
	}

	Debug(dPutAppend, "%s handle %s, start commit %d Log, wait for result", kv, args, wait.index)
	result := <-wait.result
	reply.Err = result.Err
	Debug(dPutAppend, "%s reply %s with %s", kv, args, result)
}

func (kv *KVServer) commitOp(op Op) (*commitWait, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.killed() {
		return nil, true
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, true
	}
	wait := commitWait{
		index:  index,
		term:   term,
		op:     op,
		result: make(chan Result, 1),
	}
	kv.wait[index] = wait
	kv.trySnap()
	return &wait, false
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.applyCommand(&applyMsg)
			} else {
				kv.applySnapshot(&applyMsg)
			}
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *KVServer) gc() {
	interval := time.Millisecond * 500
	for {
		select {
		case <-time.After(interval):
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				var di []int
				term, _ := kv.rf.GetState()
				res := Result{Err: ErrWrongLeader}
				for i := range kv.wait {
					if kv.wait[i].term != term {
						kv.wait[i].result <- res
						close(kv.wait[i].result)
						di = append(di, i)
					}
				}
				for i := range di {
					delete(kv.wait, di[i])
				}
				kv.trySnap()
			}()
		case <-kv.stopCh:
			kv.mu.Lock()
			for i := range kv.wait {
				close(kv.wait[i].result)
			}
			kv.wait = nil
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *KVServer) applyCommand(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applyMsg.Command.(Op)
	wait, waitOk := kv.wait[applyMsg.CommandIndex]
	trk, trkOk := kv.session[op.ClientId]

	if trkOk && trk.RequestId == op.RequestId {
		if waitOk {
			wait.result <- trk.Result
			Debug(dApply, "%s wait request is duplicate, send saved %s to %s", kv, trk.Result, wait)
			close(wait.result)
			delete(kv.wait, applyMsg.CommandIndex)
		} else {
			Debug(dApply, "%s %s is duplicated", kv, op)
		}
		return
	}

	var res Result
	switch op.Action {
	case ActGet:
		value, ok := kv.state[op.Key]
		if ok {
			res.Err = OK
			res.Value = value
		} else {
			res.Err = ErrNoKey
		}
	case ActPut:
		kv.state[op.Key] = op.Value
		res.Err = OK
	case ActAppend:
		kv.state[op.Key] = kv.state[op.Key] + op.Value
		res.Err = OK
	}
	kv.lastApplied = applyMsg.CommandIndex
	Debug(dApply, "%s apply %d Log %s", kv, applyMsg.CommandIndex, op)
	kv.session[op.ClientId] = Track{
		RequestId: op.RequestId,
		Result:    res,
	}
	Debug(dApply, "%s set client[%d] session's requestId to %d", kv, op.ClientId, op.RequestId)

	if !waitOk {
		// do nothing
	} else if term, _ := kv.rf.GetState(); wait.op != op || wait.term != term {
		res = Result{Err: ErrWrongLeader}
		for i := range kv.wait {
			kv.wait[i].result <- res
			close(kv.wait[i].result)
		}
		Debug(dApply, "%s lose leadership before apply this Log, reply all wait request %s", kv, res.Err)
		kv.wait = make(map[int]commitWait)
	} else {
		wait.result <- res
		Debug(dApply, "%s send %s to %s", kv, res, wait)
		close(wait.result)
		delete(kv.wait, applyMsg.CommandIndex)
	}
	kv.trySnap()
}

func (kv *KVServer) applySnapshot(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.restore(applyMsg.Snapshot)
	Debug(dSnap, "%s apply snapshot at %d", kv, kv.lastApplied)
}

func (kv *KVServer) trySnap() {
	if kv.maxraftstate != -1 {
		left := kv.maxraftstate - kv.persister.RaftStateSize()
		thre := int(float64(kv.maxraftstate) * 0.1)
		if left <= thre {
			kv.snapshot()
		}
	}
}

func (kv *KVServer) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.session)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(kv.lastApplied, data)
	Debug(dSnap, "%s take a snapshot at %d", kv, kv.lastApplied)
}

func (kv *KVServer) restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.state) != nil ||
		d.Decode(&kv.session) != nil ||
		d.Decode(&kv.lastApplied) != nil {
		panic("readPersist")
	}
	Debug(dRestore, "%s restore from image, lastApplied %d", kv, kv.lastApplied)
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("S%d", kv.me)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.wait = make(map[int]commitWait)
	kv.stopCh = make(chan struct{})
	kv.state = make(map[string]string)
	kv.session = make(map[int64]Track)
	kv.restore(persister.ReadSnapshot())
	go kv.apply()
	go kv.gc()

	return kv
}
